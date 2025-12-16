# Storage Engine Plan: Log-Authoritative Tiered Storage

## Vision

This document outlines the plan for implementing a **log-authoritative tiered storage engine** that receives log entries from an external consensus orchestrator (e.g., Raft) and maintains a hot local tier with asynchronous S3 cold tier shipping.

### Core Principle

**The log (maintained by external orchestrator) is the source of truth.**
**The orchestrator's low-water mark becomes the S3 high-water mark.**

### Ordering Mechanism

The system uses a **monotonically increasing comparable number** to order log entries. This can be:
- **LSN (Log Sequence Number)**: Traditional sequential log numbering (1, 2, 3, ...)
- **Timestamp**: Microsecond or nanosecond precision timestamps
- **HLC (Hybrid Logical Clock)**: Combines physical time with logical counter
- **Any comparable u64/i64**: As long as it's monotonically increasing

Throughout this document, we use the term **"sequence number"** or **"seq"** to refer to this ordering mechanism, which is provided by the external orchestrator.

## Architecture Overview

```
   External Orchestrator (Raft/Consensus Layer)
                |
                | Log Entries (seq-ordered)
                | + Low-Water Mark (LWM)
                v
   ┌────────────────────────────────────┐
   │    Storage Engine (This Layer)     │
   │                                    │
   │  ┌──────────────────────────────┐  │
   │  │  Write-Ahead Log (WAL)       │  │
   │  │  - Redo log for durability   │  │
   │  │  - Undo log for rollback     │  │
   │  └───────────┬──────────────────┘  │
   │              |                     │
   │              v                     │
   │  ┌──────────────────────────────┐  │
   │  │  ARIES Storage Engine        │  │
   │  │  - Buffer pool               │  │
   │  │  - Page cache                │  │
   │  │  - Lock manager              │  │
   │  │  - Hot tier (~10% working)   │  │
   │  └───────────┬──────────────────┘  │
   │              |                     │
   └──────────────┼─────────────────────┘
                  |
                  | Async segment shipping
                  | (background thread)
                  v
   ┌────────────────────────────────────┐
   │            S3 Cold Tier            │
   │  - Immutable log segments          │
   │  - Compacted data files            │
   │  - Checkpoint snapshots            │
   └────────────────────────────────────┘
```

## Key Invariant

The storage engine maintains a single critical invariant:

```
Orchestrator LWM ≥ S3 HWM
```

**Where:**
- **Orchestrator LWM** (Low-Water Mark): Minimum sequence number applied on all replicas (provided by external orchestrator)
- **S3 HWM** (High-Water Mark): Maximum sequence number shipped to S3 (maintained by storage engine)

**Sequence Number Timeline:**
```
Sequence number timeline (e.g., timestamp, LSN, HLC)
│
├── ≤ S3_HWM   → safely persisted in S3, can be purged from hot tier
├── ≤ Orch_LWM → applied everywhere, safe to ship to S3
└── > Orch_LWM → may exist only in hot storage, not yet durable across replicas
```

## Interface: External Orchestrator to Storage Engine

The storage engine exposes an interface for the external orchestrator:

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Apply a log entry at the given sequence number
    /// The orchestrator ensures sequence numbers are monotonically increasing
    /// (this could be LSN, timestamp, HLC, or any comparable u64)
    async fn apply_log_entry(&self, seq: SeqNum, entry: LogEntry) -> Result<(), StorageError>;

    /// Notify the storage engine of the current low-water mark
    /// This allows the engine to:
    /// 1. Ship segments ≤ LWM to S3
    /// 2. Purge data from hot tier once in S3
    async fn update_low_water_mark(&self, lwm: SeqNum) -> Result<(), StorageError>;

    /// Read data at a given sequence number (may read from hot or cold tier)
    async fn read(&self, bucket: &str, key: &str, seq: SeqNum)
        -> Result<ObjectData, StorageError>;

    /// Get the current S3 high-water mark
    async fn get_s3_high_water_mark(&self) -> SeqNum;

    /// Force checkpoint creation (for recovery)
    async fn create_checkpoint(&self) -> Result<CheckpointId, StorageError>;
}
```

```rust
pub enum LogEntry {
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    PutObject { bucket: String, key: String, data: Bytes, metadata: ObjectMetadata },
    DeleteObject { bucket: String, key: String },
    CopyObject { src_bucket: String, src_key: String, dest_bucket: String, dest_key: String },
}

/// Monotonically increasing sequence number for ordering
/// Can be LSN, timestamp, HLC, or any comparable u64
pub type SeqNum = u64;
pub type CheckpointId = u64;
```

## Components

### 1. Write-Ahead Log (WAL)

The WAL provides durability for operations before they're committed to the main storage:

**Responsibilities:**
- Persist log entries to disk before acknowledging
- Support redo operations on crash recovery
- Support undo operations for transaction rollback
- Segment management (seal, rotate, archive)

**Structure:**
```
wal/
  ├── current.log          # Active segment being written
  ├── segment-00001.log    # Sealed segment (LSN 1-10000)
  ├── segment-00002.log    # Sealed segment (LSN 10001-20000)
  └── ...
```

**WAL Entry Format:**
```rust
struct WALEntry {
    seq: SeqNum,                   // Monotonically increasing sequence number
    entry_type: LogEntryType,
    before_image: Option<Bytes>,  // For undo
    after_image: Bytes,            // For redo
    checksum: u32,
}
```

### 2. ARIES Storage Engine

Implements the classic ARIES algorithm with three key principles:

**Write-Ahead Logging:**
- Every update is logged before being applied
- Log must be flushed to disk before page is written
- Ensures durability and recovery

**Redo Operations:**
- Replay log forward from last checkpoint
- Apply all committed operations
- Idempotent: can replay multiple times safely

**Undo Operations:**
- Rollback uncommitted transactions
- Use before-images from WAL
- Generate compensation log records (CLRs)

**Buffer Pool:**
```rust
struct BufferPool {
    pages: HashMap<PageId, Page>,
    lru: LruCache<PageId>,
    dirty_pages: HashSet<PageId>,
    max_memory: usize,  // ~10% of total data
}
```

**Page Structure:**
```rust
struct Page {
    page_id: PageId,
    seq: SeqNum,           // Last sequence number that modified this page
    data: Vec<u8>,
    dirty: bool,
    pin_count: usize,
}
```

### 3. S3 Shipping Layer

Background process that asynchronously ships data to S3:

**Responsibilities:**
- Monitor orchestrator LWM updates
- Ship sealed WAL segments to S3
- Ship compacted data files to S3
- Update S3 HWM after successful upload
- Purge hot tier data once in S3

**Shipping Process:**
```rust
async fn background_shipper(engine: Arc<dyn StorageEngine>) {
    loop {
        let orch_lwm = engine.get_orchestrator_lwm().await;
        let s3_hwm = engine.get_s3_high_water_mark().await;

        // Find segments between s3_hwm and orch_lwm
        let segments = engine.find_sealed_segments(s3_hwm, orch_lwm).await;

        for segment in segments {
            // Upload segment to S3
            upload_to_s3(&segment).await?;

            // Update S3 HWM
            engine.set_s3_high_water_mark(segment.max_lsn).await?;

            // Purge segment from hot tier
            engine.purge_segment(segment.id).await?;
        }

        sleep(Duration::from_secs(10)).await;
    }
}
```

**S3 Object Types:**

1. **WAL Segments:**
   ```
   s3://bucket/wal/segment-{:010}.log
   ```
   Immutable sealed log segments.

2. **Compacted Data:**
   ```
   s3://bucket/data/{bucket}/{key}/v{:010}.dat
   ```
   Compacted object data with version history.

3. **Checkpoints:**
   ```
   s3://bucket/checkpoints/checkpoint-{:010}.ckpt
   ```
   Point-in-time snapshots for faster recovery.

### 4. Hot/Cold Tier Management

The engine maintains a hot tier with ~10% of the working set:

**Hot Tier (Local):**
- Recent objects (accessed within last N minutes)
- Objects above S3 HWM
- Active buffer pool pages
- Current WAL segment

**Cold Tier (S3):**
- Historical data ≤ S3 HWM
- Sealed WAL segments
- Compacted data files
- Checkpoints

**Eviction Policy:**
```rust
async fn evict_cold_data(&self) {
    let s3_hwm = self.get_s3_high_water_mark().await;

    // Find pages with seq ≤ s3_hwm and not recently accessed
    let evict_candidates = self.buffer_pool
        .pages
        .iter()
        .filter(|(_, page)| {
            page.seq <= s3_hwm &&
            page.last_access < now() - EVICTION_THRESHOLD
        })
        .collect();

    for page in evict_candidates {
        // Flush if dirty
        if page.dirty {
            self.flush_page(page).await?;
        }

        // Remove from buffer pool
        self.buffer_pool.evict(page.page_id).await?;
    }
}
```

**Read Path with Tiering:**
```rust
async fn read(&self, bucket: &str, key: &str, seq: SeqNum) -> Result<ObjectData, StorageError> {
    // Try hot tier first
    if let Some(data) = self.buffer_pool.get(bucket, key, seq).await? {
        return Ok(data);
    }

    // Fetch from S3 cold tier
    let data = self.fetch_from_s3(bucket, key, seq).await?;

    // Optionally cache in hot tier
    self.buffer_pool.insert(bucket, key, seq, data.clone()).await?;

    Ok(data)
}
```

## Failure and Recovery

### Crash Recovery

On startup, the storage engine performs ARIES recovery:

```rust
async fn recover(&self) -> Result<(), StorageError> {
    // Phase 1: Analysis
    let checkpoint = self.load_latest_checkpoint().await?;
    let dirty_pages = checkpoint.dirty_page_table.clone();

    // Phase 2: Redo
    // Replay log from checkpoint sequence number to end
    let min_seq = dirty_pages.values().min();
    self.replay_redo(min_seq, None).await?;

    // Phase 3: Undo
    // Rollback uncommitted transactions
    self.rollback_uncommitted().await?;

    Ok(())
}
```

### Disk Loss Recovery

If the local disk is lost:

1. Fetch latest checkpoint from S3
2. Load checkpoint state
3. Replay WAL segments from S3 starting after checkpoint
4. Contact orchestrator for any log entries > S3 HWM
5. Resume normal operation

```rust
async fn recover_from_s3(&self, orchestrator: &dyn Orchestrator) -> Result<(), StorageError> {
    // Load latest checkpoint
    let checkpoint = self.fetch_latest_checkpoint_from_s3().await?;
    self.load_checkpoint(checkpoint).await?;

    // Replay WAL from S3
    let s3_hwm = self.get_s3_high_water_mark().await;
    let segments = self.fetch_wal_segments_from_s3(checkpoint.seq, s3_hwm).await?;
    for segment in segments {
        self.replay_segment(segment).await?;
    }

    // Get missing log entries from orchestrator
    let orch_lwm = orchestrator.get_low_water_mark().await?;
    if orch_lwm > s3_hwm {
        let entries = orchestrator.fetch_log_entries(s3_hwm + 1, orch_lwm).await?;
        for entry in entries {
            self.apply_log_entry(entry.seq, entry.data).await?;
        }
    }

    Ok(())
}
```

### S3 Shipping Lag

S3 shipping lag is explicitly allowed and bounded:

**Guarantees:**
- Data is durable once orchestrator LWM advances (replicated across nodes)
- S3 HWM may lag behind orchestrator LWM
- Gap is bounded by shipping interval (default: 10 seconds)
- Reads work correctly regardless of lag (read from hot tier if not in S3)

**Monitoring:**
```rust
// Expose metrics
metrics::gauge!("storage.s3_shipping_lag_bytes", s3_lag_bytes);
metrics::gauge!("storage.s3_shipping_lag_seconds", s3_lag_seconds);
metrics::gauge!("storage.orchestrator_lwm", orch_lwm);
metrics::gauge!("storage.s3_hwm", s3_hwm);
```

## Implementation Phases

### Phase 1: WAL Foundation
- [ ] Implement WAL writer with segment rotation
- [ ] Add sequence number tracking and monotonic ordering validation
- [ ] Implement WAL reader for recovery
- [ ] Add checksum verification

### Phase 2: ARIES Engine
- [ ] Implement buffer pool with LRU eviction
- [ ] Add redo log replay
- [ ] Add undo log support with CLRs
- [ ] Implement checkpoint creation
- [ ] Add recovery algorithm (analysis/redo/undo)

### Phase 3: S3 Integration
- [ ] Implement S3 segment shipping
- [ ] Add S3 HWM tracking
- [ ] Implement object fetching from S3
- [ ] Add compaction logic
- [ ] Implement checkpoint storage in S3

### Phase 4: Orchestrator Integration
- [ ] Define StorageEngine trait
- [ ] Implement apply_log_entry with sequence number ordering
- [ ] Implement update_low_water_mark
- [ ] Add orchestrator LWM monitoring
- [ ] Implement read path with sequence number support

### Phase 5: Hot/Cold Tiering
- [ ] Implement eviction policy
- [ ] Add hot tier size limits (~10% working set)
- [ ] Implement on-demand S3 fetching
- [ ] Add prefetching for sequential access
- [ ] Optimize page cache hit rate

### Phase 6: Production Readiness
- [ ] Add comprehensive metrics
- [ ] Implement background compaction
- [ ] Add S3 shipping lag monitoring
- [ ] Implement garbage collection for old segments
- [ ] Add operational tools (inspect WAL, verify checksums)

## Configuration

```toml
[storage_engine]
# WAL configuration
wal_segment_size = "100MB"
wal_sync_mode = "fsync"  # fsync, fdatasync, none
wal_compression = "zstd"

# Buffer pool configuration
buffer_pool_size = "1GB"  # ~10% of working set
eviction_policy = "lru"
page_size = "8KB"

# S3 shipping configuration
s3_bucket = "my-storage-engine"
s3_prefix = "data/"
shipping_interval_secs = 10
shipping_batch_size = 100
s3_compression = "zstd"

# Checkpoint configuration
checkpoint_interval_secs = 300
checkpoint_max_age_secs = 3600

# Compaction configuration
compaction_enabled = true
compaction_interval_secs = 3600
compaction_size_threshold = "100MB"
```

## Metrics

Key metrics to expose:

```rust
// Sequence number tracking
storage_orchestrator_lwm          // Current orchestrator low-water mark
storage_s3_hwm                    // Current S3 high-water mark
storage_current_seq               // Current sequence number being applied

// S3 shipping
storage_s3_shipping_lag_bytes
storage_s3_shipping_lag_seconds
storage_s3_shipped_segments_total
storage_s3_shipping_errors_total

// Hot tier
storage_buffer_pool_hit_rate
storage_buffer_pool_size_bytes
storage_buffer_pool_evictions_total
storage_hot_tier_size_bytes

// WAL
storage_wal_write_duration_seconds
storage_wal_segments_total
storage_wal_size_bytes

// Recovery
storage_recovery_duration_seconds
storage_checkpoint_creation_duration_seconds
```

## Testing Strategy

### Unit Tests
- WAL segment writing and reading
- Buffer pool eviction policies
- Sequence number ordering and monotonicity validation
- Checksum verification

### Integration Tests
- End-to-end apply → ship → recover flow
- Crash recovery scenarios
- Disk loss recovery from S3
- Concurrent reads during shipping

### Chaos Tests
- Random crashes during WAL writes
- S3 upload failures
- Network partitions
- Disk full scenarios

### Performance Tests
- Write throughput (ops/sec)
- Read latency (p50, p99, p999)
- Recovery time (checkpoint size vs time)
- S3 shipping throughput

## Future Enhancements

### Parallel S3 Shipping
Ship multiple segments concurrently to reduce lag:
```rust
async fn parallel_shipper(&self, concurrency: usize) {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    // Ship up to N segments in parallel
}
```

### Smart Prefetching
Predict access patterns and prefetch from S3:
```rust
async fn prefetch_sequential(&self, bucket: &str, key_prefix: &str) {
    // Detect sequential scan, fetch next pages from S3
}
```

### Compression Tiers
Different compression for hot vs cold:
- Hot tier: LZ4 (fast decompression)
- Cold tier: ZSTD level 9 (high compression ratio)

### Multi-Region S3
Replicate to multiple S3 regions for disaster recovery:
```rust
s3_regions = ["us-east-1", "eu-west-1"]
s3_replication_mode = "async"  # or "sync"
```

## Summary

This storage engine design provides:

1. **Log-authoritative architecture** - Orchestrator log is source of truth
2. **ARIES-based durability** - WAL with redo/undo for crash recovery
3. **Tiered storage** - Hot local tier (~10%) + cold S3 tier
4. **Asynchronous shipping** - No write latency penalty for S3
5. **Bounded lag** - S3 HWM always ≤ orchestrator LWM
6. **Deterministic recovery** - Replay from checkpoints + WAL segments

The storage engine is a **materialized view** of the log maintained by the external orchestrator. S3 becomes a derived, monotonic cold tier rather than an authoritative cache target.
