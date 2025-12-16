## Persistent Store Sketch

- **Directory Layout**
  - Root data dir (e.g., `/var/lib/s3ish/data`).
  - Buckets as subdirs: `/data/{bucket}/`.
  - Objects content-addressed to avoid duplication and ease cleanup: `/data/{bucket}/{prefix_shard}/{content_hash}` where `prefix_shard` is a short hash prefix to limit files per dir.
  - Metadata DB (sled/rocksdb/sqlite) under `/var/lib/s3ish/meta` holding:
    - Object table: bucket, key, size, etag, content_type, user metadata, last_modified, storage_hash (points to data), shard layout, parity info, part list for multipart.
    - Bucket table: name, creation_time.
    - Multipart table: upload_id, bucket, key, part refs (hash + size), initiated_at.
  - Tmp staging dir for atomic writes: `/data/.tmp/{uuid}`; write then fsync + rename to hash path.

- **Erasure Coding**
  - Store each object as k data shards + m parity shards. Each shard is a file: `/data/{bucket}/shards/{hash}/{idx}.shard`.
  - Metadata DB records k/m, block size, and list of shard paths.
  - Reads reconstruct missing shards on the fly (start with tolerance of 1–m failures).
  - Multipart: each part separately encoded; completion concatenates parts and re-encodes final object (or store parts + manifest and stream join on read to avoid re-encoding).

- **Write Path (PutObject/Copy)**
  1) Stream request body to tmp shard files using chunked RS encode: read chunk -> distribute across k shards, compute parity, write shards (sync or batched).
  2) fsync tmp files, rename to final shard paths, update metadata transactionally.
  3) Optional background dedup: if `storage_hash` already exists, hardlink to existing shards to save space.

- **Read Path (Get/Head/Copy)**
  - Read shard manifest from metadata; open shard files.
  - Stream decode: read aligned blocks from all shards, reconstruct if a shard missing/corrupt.
  - If ETag is MD5 of full object, store/precompute in metadata (optionally multipart ETag format).

- **Bucket Mapping**
  - Bucket dir per bucket; shards under `shards/` subdir to separate from per-bucket config/logs.
  - Shard subdirs by hash prefix to keep dir sizes sane.
  - Metadata DB stores bucket existence; on bucket delete, verify no object entries and remove dir.

- **Performance Considerations**
  - I/O: buffered async I/O (tokio::fs) with preallocated shard files to reduce fragmentation.
  - Batch fsync: group fsyncs per write batch; configurable durability (sync per put vs background).
  - Zero-copy: for GET, consider `sendfile`/`io_uring` to stream shards directly; decode path may still need userspace.
  - Parallelism: read/write shards in parallel; small pool of tasks per request.
  - Caching: in-memory metadata cache (moka/dashmap); optional shard cache for hot objects.
  - Shard layout tuning: k/m and block size aligned to filesystem blocks (e.g., 4KiB–1MiB).
  - GC/compaction: refcount shard hashes; GC unreferenced shard files periodically.
  - Backpressure: limit concurrent PUT/GET with semaphores; cap open files.

- **io_uring**
  - Helpful for many concurrent small I/O ops and batched fsync; can reduce syscalls/context switches.
  - Biggest wins in shard writes (parallel) and shard reads; consider `tokio-uring`/`rio` with fallback to tokio::fs.
  - For large sequential transfers, buffered async I/O may suffice; io_uring shines with concurrency and batching.

- **Failure Handling**
  - Temp write + rename for atomicity.
  - Detect partial writes via manifest checksum and shard size validation.
  - Repair: if shard corrupt/missing and parity available, reconstruct and optionally heal to disk.

- **Testing Strategy**
  - Keep full in-memory backend with EC simulation as reference.
  - Add a disk-backed backend writing to a temp dir for tests, validating shard layout, manifest correctness, and reconstruction on simulated shard loss.

## Things to consider (Horizontal Scaling)

- **Topology & Namespacing**
  - Introduce a cluster namespace: unique cluster ID, node IDs, consistent hashing ring (or shard map) to assign buckets/objects to nodes; start with bucket-level placement for simplicity.

- **Metadata Layer**
  - Central/replicated metadata to track bucket→node mapping, object manifests, multipart state.
    - Option: external consensus KV (etcd/Consul/ZooKeeper) or embedded Raft group over sled/rocksdb.
  - Strong consistency for bucket creation/manifests; leader election and fencing to avoid split-brain.

- **Data Placement & Replication**
  - Erasure across nodes (k data + m parity on distinct nodes). Consistent hashing to select nodes; retry/exclude unavailable.
  - Manifests record shard checksums and node IDs; support healing/rebalance.

- **Routing Layer**
  - Stateless gateways authenticate, resolve bucket→nodes from shard map, and proxy/redirect (HTTP 307) to appropriate data nodes. Cache shard map with TTL and invalidate on changes.

- **Replication & Consistency**
  - Writes: quorum write/fsync to k+m shards, then commit metadata.
  - Reads: quorum or any-available with checksum verification and repair on detect.
  - Metadata strong via Raft/etcd; data eventual with background healing.

- **Failure Handling & Healing**
  - Detect node failure via heartbeats; mark shards degraded; rebuild missing shards to new nodes using parity; update manifests.
  - Rebalance on node add/remove; throttle migrations.
  - Bitrot detection via per-shard checksums; repair on mismatch.

- **Scaling Metadata**
  - Partition metadata (bucket-level) across multiple Raft groups or a scalable store; cache hot metadata at gateways with watch/notify invalidation.

- **Networking & API**
  - LB in front of gateways; gateways use mTLS/grpc/http2 to storage nodes. Support S3-style redirects for placement hints; expose health endpoints.

- **Operational Concerns**
  - Observability: tracing, metrics per node/shard, replication lag, heal rates. Rolling upgrades with shard-map freezes/draining; per-node backpressure limits; security (mTLS, auth between components, audit logs).

- **io_uring & Performance**
  - Keep node-local I/O optimized (chunked streaming, async I/O); io_uring helps with high-concurrency shard fanout and batched fsync; batch replication writes; pipeline streams between nodes to avoid buffering.

- **Transitional Plan**
  - Phase 1: bucket-level shard map + gateway proxy, single replica to prove routing.
  - Phase 2: enable k+m placement with quorum write/read and healing.
  - Phase 3: scale metadata into partitioned/replicated store; add rebalancing and repair tooling.
