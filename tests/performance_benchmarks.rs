// Copyright PingCAP Inc. 2025.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use bytes::Bytes;
use s3ish::config::Config;
use s3ish::storage::file_storage::FileStorage;
use s3ish::storage::in_memory::InMemoryStorage;
use s3ish::storage::StorageBackend;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::task::JoinSet;

/// Performance test configuration
struct PerfConfig {
    name: &'static str,
    object_size: usize,
    duration_secs: u64,
    concurrent_operations: usize,
}

/// Performance test results
#[derive(Debug)]
struct PerfResults {
    total_duration: Duration,
    operations_per_sec: f64,
    throughput_mbps: f64,
    avg_latency_ms: f64,
    p50_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
}

impl PerfResults {
    fn from_durations(durations: &[Duration], total_bytes: usize, total_duration: Duration) -> Self {
        let mut sorted_durations = durations.to_vec();
        sorted_durations.sort();

        let operations_per_sec = durations.len() as f64 / total_duration.as_secs_f64();
        let throughput_mbps = (total_bytes as f64 / 1_000_000.0) / total_duration.as_secs_f64();
        let avg_latency_ms =
            durations.iter().map(|d| d.as_secs_f64() * 1000.0).sum::<f64>() / durations.len() as f64;

        let p50_latency_ms = sorted_durations[durations.len() / 2].as_secs_f64() * 1000.0;
        let p95_latency_ms = sorted_durations[durations.len() * 95 / 100].as_secs_f64() * 1000.0;
        let p99_latency_ms = sorted_durations[durations.len() * 99 / 100].as_secs_f64() * 1000.0;

        PerfResults {
            total_duration,
            operations_per_sec,
            throughput_mbps,
            avg_latency_ms,
            p50_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
        }
    }

    fn print(&self, test_name: &str) {
        println!("\n{}", "=".repeat(80));
        println!("Performance Test: {}", test_name);
        println!("{}", "=".repeat(80));
        println!("Total Duration:       {:.2}s", self.total_duration.as_secs_f64());
        println!("Operations/sec:       {:.2}", self.operations_per_sec);
        println!("Throughput:           {:.2} MB/s", self.throughput_mbps);
        println!("Average Latency:      {:.2} ms", self.avg_latency_ms);
        println!("P50 Latency:          {:.2} ms", self.p50_latency_ms);
        println!("P95 Latency:          {:.2} ms", self.p95_latency_ms);
        println!("P99 Latency:          {:.2} ms", self.p99_latency_ms);
        println!("{}", "=".repeat(80));
    }
}

/// Load performance test configuration from TOML file
fn load_perf_config() -> Config {
    let config_path = "tests/perf-test-config.toml";
    Config::from_path(config_path).unwrap_or_else(|e| {
        eprintln!("Warning: Failed to load {}: {}", config_path, e);
        eprintln!("Using default configuration");
        Config {
            listen_addr: "127.0.0.1:9999".to_string(),
            auth_file: "./creds.txt".to_string(),
            region: "us-east-1".to_string(),
            request_id_prefix: "req-".to_string(),
            storage: Default::default(),
            lifecycle: Default::default(),
            test: Default::default(),
        }
    })
}

/// Create test data of specified size
fn create_test_data(size: usize) -> Bytes {
    let pattern = b"0123456789abcdef";
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push(pattern[i % pattern.len()]);
    }
    Bytes::from(data)
}

/// Benchmark PUT operations - runs for specified duration
async fn benchmark_put_operations<S: StorageBackend>(
    storage: Arc<S>,
    config: &PerfConfig,
) -> PerfResults {
    println!("\n  Starting PUT benchmark ({}s)...", config.duration_secs);
    let bucket = "perf-test-bucket";
    storage.create_bucket(bucket).await.unwrap();

    let test_data = create_test_data(config.object_size);
    let mut durations = Vec::new();
    let test_duration = Duration::from_secs(config.duration_secs);

    let start = Instant::now();

    if config.concurrent_operations == 1 {
        // Sequential operations - run until duration expires
        let mut i = 0;
        while start.elapsed() < test_duration {
            let key = format!("object-{}", i);
            let op_start = Instant::now();

            storage
                .put_object(
                    bucket,
                    &key,
                    test_data.clone(),
                    "application/octet-stream",
                    Default::default(),
                    None,
                    None,
                )
                .await
                .unwrap();

            durations.push(op_start.elapsed());
            i += 1;
        }
    } else {
        // Concurrent operations - all tasks run until duration expires
        let mut set = JoinSet::new();

        for task_id in 0..config.concurrent_operations {
            let storage = storage.clone();
            let test_data = test_data.clone();
            let bucket = bucket.to_string();
            let task_start = Instant::now();

            set.spawn(async move {
                let mut task_durations = Vec::new();
                let mut i = 0;
                while task_start.elapsed() < test_duration {
                    let key = format!("object-task{}-{}", task_id, i);
                    let op_start = Instant::now();

                    storage
                        .put_object(
                            &bucket,
                            &key,
                            test_data.clone(),
                            "application/octet-stream",
                            Default::default(),
                            None,
                            None,
                        )
                        .await
                        .unwrap();

                    task_durations.push(op_start.elapsed());
                    i += 1;
                }
                task_durations
            });
        }

        while let Some(result) = set.join_next().await {
            durations.extend(result.unwrap());
        }
    }

    let total_duration = start.elapsed();
    let total_bytes = config.object_size * durations.len();

    PerfResults::from_durations(&durations, total_bytes, total_duration)
}

/// Benchmark GET operations - runs for specified duration
async fn benchmark_get_operations<S: StorageBackend>(
    storage: Arc<S>,
    config: &PerfConfig,
) -> PerfResults {
    println!("\n  Starting GET benchmark ({}s)...", config.duration_secs);
    let bucket = "perf-test-bucket";
    storage.create_bucket(bucket).await.unwrap();

    // Pre-populate a fixed number of objects (100) - we'll read them in round-robin
    println!("    Pre-populating 100 objects...");
    let num_prepopulated = 100;
    let test_data = create_test_data(config.object_size);
    for i in 0..num_prepopulated {
        let key = format!("object-{}", i);
        storage
            .put_object(
                bucket,
                &key,
                test_data.clone(),
                "application/octet-stream",
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap();
    }
    println!("    Pre-population complete, starting GET operations...");

    let mut durations = Vec::new();
    let test_duration = Duration::from_secs(config.duration_secs);
    let start = Instant::now();

    if config.concurrent_operations == 1 {
        // Sequential operations - run until duration expires
        let mut i = 0;
        while start.elapsed() < test_duration {
            let key = format!("object-{}", i % num_prepopulated);
            let op_start = Instant::now();

            let (data, _) = storage.get_object(bucket, &key).await.unwrap();
            assert_eq!(data.len(), config.object_size);

            durations.push(op_start.elapsed());
            i += 1;
        }
    } else {
        // Concurrent operations - all tasks run until duration expires
        let mut set = JoinSet::new();
        let concurrent_ops = config.concurrent_operations;

        for task_id in 0..concurrent_ops {
            let storage = storage.clone();
            let bucket = bucket.to_string();
            let expected_size = config.object_size;
            let task_start = Instant::now();

            set.spawn(async move {
                let mut task_durations = Vec::new();
                let mut i = 0;
                while task_start.elapsed() < test_duration {
                    let key = format!("object-{}", (task_id + i * concurrent_ops) % num_prepopulated);
                    let op_start = Instant::now();

                    let (data, _) = storage.get_object(&bucket, &key).await.unwrap();
                    assert_eq!(data.len(), expected_size);

                    task_durations.push(op_start.elapsed());
                    i += 1;
                }
                task_durations
            });
        }

        while let Some(result) = set.join_next().await {
            durations.extend(result.unwrap());
        }
    }

    let total_duration = start.elapsed();
    let total_bytes = config.object_size * durations.len();

    PerfResults::from_durations(&durations, total_bytes, total_duration)
}

#[tokio::test]
#[ignore] // Run with: cargo test --test performance_benchmarks -- --ignored --nocapture
async fn perf_inmemory_small_objects_sequential() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "InMemory - Small Objects (1KB) - Sequential",
        object_size: 1024,
        duration_secs: 10,
        concurrent_operations: 1,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}

#[tokio::test]
#[ignore]
async fn perf_inmemory_medium_objects_sequential() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "InMemory - Medium Objects (1MB) - Sequential",
        object_size: 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 1,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}

#[tokio::test]
#[ignore]
async fn perf_inmemory_large_objects_sequential() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "InMemory - Large Objects (10MB) - Sequential",
        object_size: 10 * 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 1,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}

#[tokio::test]
#[ignore]
async fn perf_inmemory_concurrent_puts() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "InMemory - Medium Objects (1MB) - Concurrent PUT (10 tasks)",
        object_size: 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 10,
    };

    let results = benchmark_put_operations(storage, &config).await;
    results.print(config.name);
}

#[tokio::test]
#[ignore]
async fn perf_inmemory_concurrent_gets() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "InMemory - Medium Objects (1MB) - Concurrent GET (10 tasks)",
        object_size: 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 10,
    };

    let results = benchmark_get_operations(storage, &config).await;
    results.print(config.name);
}

#[tokio::test]
#[ignore]
async fn perf_file_small_objects_sequential() {
    let cfg = load_perf_config();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        FileStorage::new_multi_drive(
            vec![temp_dir.path().to_path_buf()],
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    let config = PerfConfig {
        name: "FileStorage - Small Objects (1KB) - Sequential",
        object_size: 1024,
        duration_secs: 10,
        concurrent_operations: 1,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}

#[tokio::test]
#[ignore]
async fn perf_file_medium_objects_sequential() {
    let cfg = load_perf_config();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        FileStorage::new_multi_drive(
            vec![temp_dir.path().to_path_buf()],
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    let config = PerfConfig {
        name: "FileStorage - Medium Objects (1MB) - Sequential",
        object_size: 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 1,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}

#[tokio::test]
#[ignore]
async fn perf_file_large_objects_sequential() {
    let cfg = load_perf_config();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        FileStorage::new_multi_drive(
            vec![temp_dir.path().to_path_buf()],
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    let config = PerfConfig {
        name: "FileStorage - Large Objects (10MB) - Sequential",
        object_size: 10 * 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 1,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}

#[tokio::test]
#[ignore]
async fn perf_file_concurrent_puts() {
    let cfg = load_perf_config();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        FileStorage::new_multi_drive(
            vec![temp_dir.path().to_path_buf()],
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    let config = PerfConfig {
        name: "FileStorage - Medium Objects (1MB) - Concurrent PUT (10 tasks)",
        object_size: 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 10,
    };

    let results = benchmark_put_operations(storage, &config).await;
    results.print(config.name);
}

#[tokio::test]
#[ignore]
async fn perf_file_concurrent_gets() {
    let cfg = load_perf_config();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        FileStorage::new_multi_drive(
            vec![temp_dir.path().to_path_buf()],
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    let config = PerfConfig {
        name: "FileStorage - Medium Objects (1MB) - Concurrent GET (10 tasks)",
        object_size: 1024 * 1024,
        duration_secs: 10,
        concurrent_operations: 10,
    };

    let results = benchmark_get_operations(storage, &config).await;
    results.print(config.name);
}

#[tokio::test]
#[ignore]
async fn perf_comparison_storage_backends() {
    println!("\n{}", "=".repeat(80));
    println!("STORAGE BACKEND COMPARISON");
    println!("{}", "=".repeat(80));

    // Load configuration from TOML file
    let cfg = load_perf_config();

    println!("\nTest Configuration:");
    println!("  Duration:      {} seconds", cfg.test.duration_secs);
    println!("  Object size:   {} bytes ({} MB)", cfg.test.object_size, cfg.test.object_size / (1024 * 1024));

    println!("\nStorage Configuration:");
    let drives: Vec<std::path::PathBuf> = cfg.storage.effective_drives()
        .into_iter()
        .map(|s| s.into())
        .collect();
    println!("  Drives:        {} drive(s)", drives.len());
    for (i, drive) in drives.iter().enumerate() {
        println!("    Drive {}: {:?}", i, drive);
    }

    println!("\nErasure Coding Configuration:");
    println!("  Data blocks:   {}", cfg.storage.erasure.data_blocks);
    println!("  Parity blocks: {}", cfg.storage.erasure.parity_blocks);
    println!("  Block size:    {} bytes ({} MB)",
        cfg.storage.erasure.block_size,
        cfg.storage.erasure.block_size / (1024 * 1024));

    let config = PerfConfig {
        name: "Comparison Test",
        object_size: cfg.test.object_size,
        duration_secs: cfg.test.duration_secs,
        concurrent_operations: 1,
    };

    // InMemory
    println!("\n{}", "-".repeat(80));
    println!("Testing InMemoryStorage");
    println!("{}", "-".repeat(80));
    let inmem_storage = Arc::new(InMemoryStorage::new());
    let inmem_put = benchmark_put_operations(inmem_storage.clone(), &config).await;
    let inmem_get = benchmark_get_operations(inmem_storage, &config).await;

    // FileStorage with config-based drives and erasure coding
    println!("\n{}", "-".repeat(80));
    println!("Testing FileStorage");
    println!("{}", "-".repeat(80));
    println!("Initializing FileStorage with {} drives...", drives.len());
    let file_storage = Arc::new(
        FileStorage::new_multi_drive(
            drives.clone(),
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    println!("FileStorage initialized (using {} drives)", file_storage.num_drives());
    let file_put = benchmark_put_operations(file_storage.clone(), &config).await;
    let file_get = benchmark_get_operations(file_storage, &config).await;

    println!("\nPUT Operations:");
    println!("  InMemory:    {:.2} ops/s, {:.2} MB/s, {:.2} ms avg latency",
        inmem_put.operations_per_sec, inmem_put.throughput_mbps, inmem_put.avg_latency_ms);
    println!("  FileStorage: {:.2} ops/s, {:.2} MB/s, {:.2} ms avg latency",
        file_put.operations_per_sec, file_put.throughput_mbps, file_put.avg_latency_ms);

    println!("\nGET Operations:");
    println!("  InMemory:    {:.2} ops/s, {:.2} MB/s, {:.2} ms avg latency",
        inmem_get.operations_per_sec, inmem_get.throughput_mbps, inmem_get.avg_latency_ms);
    println!("  FileStorage: {:.2} ops/s, {:.2} MB/s, {:.2} ms avg latency",
        file_get.operations_per_sec, file_get.throughput_mbps, file_get.avg_latency_ms);

    println!("{}", "=".repeat(80));
}

#[tokio::test]
#[ignore]
async fn perf_filestorage_put_only() {
    println!("\n{}", "=".repeat(80));
    println!("FILESTORAGE PUT PROFILING TEST");
    println!("{}", "=".repeat(80));

    let cfg = load_perf_config();
    let drives: Vec<std::path::PathBuf> = cfg.storage.effective_drives()
        .into_iter()
        .map(|s| s.into())
        .collect();

    println!("\nStorage Configuration:");
    println!("  Drives:        {} drive(s)", drives.len());
    for (i, drive) in drives.iter().enumerate() {
        println!("    Drive {}: {:?}", i, drive);
    }
    println!("  Data blocks:   {}", cfg.storage.erasure.data_blocks);
    println!("  Parity blocks: {}", cfg.storage.erasure.parity_blocks);

    let config = PerfConfig {
        name: "FileStorage PUT Only",
        object_size: cfg.test.object_size,
        duration_secs: cfg.test.duration_secs,
        concurrent_operations: 1,
    };

    let file_storage = Arc::new(
        FileStorage::new_multi_drive(
            drives,
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );

    println!("\nRunning PUT benchmark for {} seconds...", config.duration_secs);
    let file_put = benchmark_put_operations(file_storage, &config).await;

    println!("\nFileStorage PUT Results:");
    println!("  Operations/sec: {:.2}", file_put.operations_per_sec);
    println!("  Throughput:     {:.2} MB/s", file_put.throughput_mbps);
    println!("  Avg Latency:    {:.2} ms", file_put.avg_latency_ms);
    println!("  P50 Latency:    {:.2} ms", file_put.p50_latency_ms);
    println!("  P95 Latency:    {:.2} ms", file_put.p95_latency_ms);
    println!("  P99 Latency:    {:.2} ms", file_put.p99_latency_ms);
    println!("{}", "=".repeat(80));
}

/// GET-only benchmark - prepopulates data, then ONLY measures GET operations
#[tokio::test]
#[ignore]
async fn perf_filestorage_get_only() {
    println!("\n{}", "=".repeat(80));
    println!("FILESTORAGE GET-ONLY TEST");
    println!("{}", "=".repeat(80));

    let cfg = load_perf_config();
    let drives: Vec<std::path::PathBuf> = cfg.storage.effective_drives()
        .into_iter()
        .map(|s| s.into())
        .collect();

    println!("\nStorage Configuration:");
    println!("  Drives:        {} drive(s)", drives.len());
    for (i, drive) in drives.iter().enumerate() {
        println!("    Drive {}: {:?}", i, drive);
    }
    println!("  Data blocks:   {}", cfg.storage.erasure.data_blocks);
    println!("  Parity blocks: {}", cfg.storage.erasure.parity_blocks);

    let config = PerfConfig {
        name: "FileStorage GET Only",
        object_size: cfg.test.object_size,
        duration_secs: cfg.test.duration_secs,
        concurrent_operations: 1,
    };

    let file_storage = Arc::new(
        FileStorage::new_multi_drive(
            drives,
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );

    println!("\n=== PHASE 1: Pre-populating 100 objects ===");
    let bucket = "perf-get-test";
    file_storage.create_bucket(bucket).await.unwrap();

    let test_data = create_test_data(config.object_size);
    for i in 0..100 {
        let key = format!("object-{}", i);
        file_storage.put_object(
            bucket,
            &key,
            test_data.clone(),
            "application/octet-stream",
            Default::default(),
            None,
            None,
        ).await.unwrap();
    }

    println!("Pre-population complete. Waiting 5 seconds for any background writes to settle...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    println!("\n=== PHASE 2: Running GET-only benchmark for {} seconds ===", config.duration_secs);
    println!("Monitor iostat NOW - there should be NO writes, only reads!");

    let file_get = benchmark_get_operations(file_storage, &config).await;

    println!("\nFileStorage GET-Only Results:");
    println!("  Operations/sec: {:.2}", file_get.operations_per_sec);
    println!("  Throughput:     {:.2} MB/s", file_get.throughput_mbps);
    println!("  Avg Latency:    {:.2} ms", file_get.avg_latency_ms);
    println!("  P50 Latency:    {:.2} ms", file_get.p50_latency_ms);
    println!("  P95 Latency:    {:.2} ms", file_get.p95_latency_ms);
    println!("  P99 Latency:    {:.2} ms", file_get.p99_latency_ms);
    println!("{}", "=".repeat(80));
}

/// Benchmark CPU-only overhead (no disk I/O) - measures encoding, hashing, buffering
async fn benchmark_cpu_only_encoding(config: &PerfConfig, erasure_config: &s3ish::config::ErasureConfig) -> PerfResults {
    println!("\n  Starting CPU-only encoding benchmark ({}s)...", config.duration_secs);

    // Note: We manually implement erasure encoding below instead of using Erasure struct
    // to avoid any disk I/O overhead in the measurement

    let test_data = create_test_data(config.object_size);
    let mut durations = Vec::new();
    let test_duration = Duration::from_secs(config.duration_secs);
    let start = Instant::now();

    while start.elapsed() < test_duration {
        let op_start = Instant::now();

        // Do all the CPU work without disk I/O
        let mut hasher = blake3::Hasher::new();

        for stripe_start in (0..test_data.len()).step_by(erasure_config.block_size * erasure_config.data_blocks) {
            let mut parity_block = vec![0u8; erasure_config.block_size];

            for data_idx in 0..erasure_config.data_blocks {
                let offset = stripe_start + data_idx * erasure_config.block_size;
                if offset >= test_data.len() {
                    break;
                }
                let end = std::cmp::min(offset + erasure_config.block_size, test_data.len());
                let slice = &test_data[offset..end];

                // Hash the data
                hasher.update(slice);

                // Compute parity
                for (i, b) in slice.iter().enumerate() {
                    parity_block[i] ^= b;
                }
            }
        }

        let _etag = hasher.finalize().to_hex();

        durations.push(op_start.elapsed());
    }

    let total_duration = start.elapsed();
    let total_bytes = config.object_size * durations.len();

    PerfResults::from_durations(&durations, total_bytes, total_duration)
}

#[tokio::test]
#[ignore]
async fn perf_cpu_only_vs_disk() {
    println!("\n{}", "=".repeat(80));
    println!("CPU-ONLY vs DISK PERFORMANCE COMPARISON");
    println!("{}", "=".repeat(80));

    let cfg = load_perf_config();
    let drives: Vec<std::path::PathBuf> = cfg.storage.effective_drives()
        .into_iter()
        .map(|s| s.into())
        .collect();

    println!("\nConfiguration:");
    println!("  Duration:      {} seconds", cfg.test.duration_secs);
    println!("  Object size:   {} bytes ({} MB)", cfg.test.object_size, cfg.test.object_size / (1024 * 1024));
    println!("  Data blocks:   {}", cfg.storage.erasure.data_blocks);
    println!("  Parity blocks: {}", cfg.storage.erasure.parity_blocks);
    println!("  Drives:        {}", drives.len());

    let config = PerfConfig {
        name: "CPU vs Disk",
        object_size: cfg.test.object_size,
        duration_secs: cfg.test.duration_secs,
        concurrent_operations: 1,
    };

    // CPU-only benchmark
    println!("\n{}", "-".repeat(80));
    println!("Testing CPU-only (no disk I/O)");
    println!("{}", "-".repeat(80));
    let cpu_results = benchmark_cpu_only_encoding(&config, &cfg.storage.erasure).await;

    // FileStorage with disk
    println!("\n{}", "-".repeat(80));
    println!("Testing FileStorage (with disk I/O)");
    println!("{}", "-".repeat(80));
    let file_storage = Arc::new(
        FileStorage::new_multi_drive(
            drives,
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
    let disk_results = benchmark_put_operations(file_storage, &config).await;

    println!("\n{}", "=".repeat(80));
    println!("RESULTS:");
    println!("{}", "=".repeat(80));
    println!("\nCPU-only (encoding + hashing, no disk):");
    println!("  Throughput:     {:.2} MB/s", cpu_results.throughput_mbps);
    println!("  Avg Latency:    {:.2} ms", cpu_results.avg_latency_ms);

    println!("\nFileStorage (with disk I/O):");
    println!("  Throughput:     {:.2} MB/s", disk_results.throughput_mbps);
    println!("  Avg Latency:    {:.2} ms", disk_results.avg_latency_ms);

    let cpu_overhead_pct = (cpu_results.avg_latency_ms / disk_results.avg_latency_ms) * 100.0;
    let disk_overhead_pct = 100.0 - cpu_overhead_pct;

    println!("\nBottleneck Analysis:");
    println!("  CPU overhead:   {:.1}% of total time", cpu_overhead_pct);
    println!("  Disk I/O:       {:.1}% of total time", disk_overhead_pct);
    println!("  Max theoretical throughput (CPU limit): {:.2} MB/s", cpu_results.throughput_mbps);
    println!("{}", "=".repeat(80));
}

#[tokio::test]
#[ignore]
async fn perf_stress_test_high_concurrency() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "Stress Test - High Concurrency (50 concurrent tasks)",
        object_size: 512 * 1024, // 512KB
        duration_secs: 10,
        concurrent_operations: 50,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}
