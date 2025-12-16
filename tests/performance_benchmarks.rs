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
    num_operations: usize,
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

/// Benchmark PUT operations
async fn benchmark_put_operations<S: StorageBackend>(
    storage: Arc<S>,
    config: &PerfConfig,
) -> PerfResults {
    let bucket = "perf-test-bucket";
    storage.create_bucket(bucket).await.unwrap();

    let test_data = create_test_data(config.object_size);
    let mut durations = Vec::new();

    let start = Instant::now();

    if config.concurrent_operations == 1 {
        // Sequential operations
        for i in 0..config.num_operations {
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
        }
    } else {
        // Concurrent operations
        let mut set = JoinSet::new();
        let ops_per_task = config.num_operations / config.concurrent_operations;

        for task_id in 0..config.concurrent_operations {
            let storage = storage.clone();
            let test_data = test_data.clone();
            let bucket = bucket.to_string();

            set.spawn(async move {
                let mut task_durations = Vec::new();
                for i in 0..ops_per_task {
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

/// Benchmark GET operations
async fn benchmark_get_operations<S: StorageBackend>(
    storage: Arc<S>,
    config: &PerfConfig,
) -> PerfResults {
    let bucket = "perf-test-bucket";
    storage.create_bucket(bucket).await.unwrap();

    // Pre-populate objects
    let test_data = create_test_data(config.object_size);
    for i in 0..config.num_operations {
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

    let mut durations = Vec::new();
    let start = Instant::now();

    if config.concurrent_operations == 1 {
        // Sequential operations
        for i in 0..config.num_operations {
            let key = format!("object-{}", i);
            let op_start = Instant::now();

            let (data, _) = storage.get_object(bucket, &key).await.unwrap();
            assert_eq!(data.len(), config.object_size);

            durations.push(op_start.elapsed());
        }
    } else {
        // Concurrent operations
        let mut set = JoinSet::new();
        let ops_per_task = config.num_operations / config.concurrent_operations;

        for task_id in 0..config.concurrent_operations {
            let storage = storage.clone();
            let bucket = bucket.to_string();
            let expected_size = config.object_size;

            set.spawn(async move {
                let mut task_durations = Vec::new();
                for i in 0..ops_per_task {
                    let key = format!("object-{}", task_id * ops_per_task + i);
                    let op_start = Instant::now();

                    let (data, _) = storage.get_object(&bucket, &key).await.unwrap();
                    assert_eq!(data.len(), expected_size);

                    task_durations.push(op_start.elapsed());
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
        num_operations: 1000,
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
        num_operations: 100,
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
        num_operations: 20,
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
        num_operations: 100,
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
        num_operations: 100,
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
        num_operations: 1000,
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
        num_operations: 100,
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
        num_operations: 20,
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
        num_operations: 100,
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
        num_operations: 100,
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

    println!("\nErasure Coding Configuration:");
    println!("  Data blocks:   {}", cfg.storage.erasure.data_blocks);
    println!("  Parity blocks: {}", cfg.storage.erasure.parity_blocks);
    println!("  Block size:    {} bytes ({} MB)",
        cfg.storage.erasure.block_size,
        cfg.storage.erasure.block_size / (1024 * 1024));

    let config = PerfConfig {
        name: "Comparison Test",
        object_size: 1024 * 1024, // 1MB
        num_operations: 50,
        concurrent_operations: 1,
    };

    // InMemory
    let inmem_storage = Arc::new(InMemoryStorage::new());
    let inmem_put = benchmark_put_operations(inmem_storage.clone(), &config).await;
    let inmem_get = benchmark_get_operations(inmem_storage, &config).await;

    // FileStorage with config-based erasure coding
    let temp_dir = TempDir::new().unwrap();
    let file_storage = Arc::new(
        FileStorage::new_multi_drive(
            vec![temp_dir.path().to_path_buf()],
            cfg.storage.erasure.data_blocks,
            cfg.storage.erasure.parity_blocks,
            cfg.storage.erasure.block_size,
        )
        .await
        .unwrap()
    );
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
async fn perf_stress_test_high_concurrency() {
    let storage = Arc::new(InMemoryStorage::new());
    let config = PerfConfig {
        name: "Stress Test - High Concurrency (50 concurrent tasks)",
        object_size: 512 * 1024, // 512KB
        num_operations: 500,
        concurrent_operations: 50,
    };

    let put_results = benchmark_put_operations(storage.clone(), &config).await;
    put_results.print(&format!("{} - PUT", config.name));

    let get_results = benchmark_get_operations(storage, &config).await;
    get_results.print(&format!("{} - GET", config.name));
}
