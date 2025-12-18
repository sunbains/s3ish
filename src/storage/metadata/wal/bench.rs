// Copyright PingCAP Inc. 2025.

//! Performance tests for WAL implementation

use super::*;
use super::types::{BlockHeader, Crc32};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Simulated metadata operation types
#[derive(Debug, Clone)]
pub enum MetadataOp {
    CreateObject { bucket: String, key: String, size: u64 },
    UpdateObject { bucket: String, key: String, metadata: String },
    DeleteObject { bucket: String, key: String },
    CreateBucket { bucket: String },
}

impl MetadataOp {
    /// Serialize operation to bytes (simplified format)
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            MetadataOp::CreateObject { bucket, key, size } => {
                format!("CREATE|{}|{}|{}", bucket, key, size).into_bytes()
            }
            MetadataOp::UpdateObject { bucket, key, metadata } => {
                format!("UPDATE|{}|{}|{}", bucket, key, metadata).into_bytes()
            }
            MetadataOp::DeleteObject { bucket, key } => {
                format!("DELETE|{}|{}", bucket, key).into_bytes()
            }
            MetadataOp::CreateBucket { bucket } => {
                format!("BUCKET|{}", bucket).into_bytes()
            }
        }
    }
}

/// Performance metrics
#[derive(Debug, Clone)]
pub struct PerfMetrics {
    pub ops_count: u64,
    pub bytes_written: u64,
    pub duration_micros: u64,
    pub min_latency_micros: u64,
    pub max_latency_micros: u64,
    pub p50_latency_micros: u64,
    pub p95_latency_micros: u64,
    pub p99_latency_micros: u64,
}

impl PerfMetrics {
    pub fn throughput_ops_per_sec(&self) -> f64 {
        (self.ops_count as f64) / (self.duration_micros as f64 / 1_000_000.0)
    }

    pub fn throughput_mb_per_sec(&self) -> f64 {
        (self.bytes_written as f64 / 1_048_576.0) / (self.duration_micros as f64 / 1_000_000.0)
    }

    pub fn avg_latency_micros(&self) -> f64 {
        self.duration_micros as f64 / self.ops_count as f64
    }
}

/// Mock write callback that tracks performance
struct MockWriter {
    write_count: AtomicU64,
    total_bytes: AtomicU64,
    write_latencies: Mutex<Vec<u64>>,
}

impl MockWriter {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            write_count: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            write_latencies: Mutex::new(Vec::new()),
        })
    }

    fn stats(&self) -> (u64, u64) {
        (
            self.write_count.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
        )
    }
}

/// WAL performance test harness
pub struct WalBench {
    log: Log,
    writer: Arc<MockWriter>,
    latencies: Vec<u64>,
}

impl WalBench {
    pub fn new(config: LogConfig) -> Self {
        let mut log = Log::new(config);
        log.initialize(0);

        let writer = MockWriter::new();
        let writer_clone = writer.clone();

        log.set_write_callback(Arc::new(move |headers, data, crcs| {
            let start = Instant::now();

            // Simulate write cost (calculate total bytes)
            let mut total = 0;
            for header in headers {
                if header.data_len > 0 {
                    total += std::mem::size_of::<BlockHeader>();
                    total += header.data_len as usize;
                    total += std::mem::size_of::<Crc32>();
                } else {
                    break;
                }
            }

            // Track metrics
            writer_clone.write_count.fetch_add(1, Ordering::Relaxed);
            writer_clone.total_bytes.fetch_add(total as u64, Ordering::Relaxed);

            let elapsed = start.elapsed().as_micros() as u64;
            writer_clone.write_latencies.lock().unwrap().push(elapsed);

            Ok(total)
        }));

        Self {
            log,
            writer,
            latencies: Vec::new(),
        }
    }

    /// Run a workload and return metrics
    pub fn run_workload(&mut self, ops: &[MetadataOp], flush_interval: usize) -> PerfMetrics {
        self.latencies.clear();
        let start = Instant::now();

        let mut ops_count = 0u64;
        let mut bytes_written = 0u64;

        for (i, op) in ops.iter().enumerate() {
            let data = op.to_bytes();
            let data_len = data.len() as u64;

            // Check if we have space, flush if needed
            if !self.log.has_space(data.len()) {
                if let Err(e) = self.log.flush() {
                    eprintln!("Flush failed: {}", e);
                    break;
                }
            }

            let append_start = Instant::now();
            match self.log.append(&data) {
                Ok(_slot) => {
                    let latency = append_start.elapsed().as_micros() as u64;
                    self.latencies.push(latency);
                    ops_count += 1;
                    bytes_written += data_len;
                }
                Err(e) => {
                    eprintln!("Append failed: {}", e);
                    // Try flush and retry once
                    if let Err(flush_err) = self.log.flush() {
                        eprintln!("Flush after error failed: {}", flush_err);
                        break;
                    }
                    // Retry append
                    match self.log.append(&data) {
                        Ok(_slot) => {
                            let latency = append_start.elapsed().as_micros() as u64;
                            self.latencies.push(latency);
                            ops_count += 1;
                            bytes_written += data_len;
                        }
                        Err(e2) => {
                            eprintln!("Retry append failed: {}", e2);
                            break;
                        }
                    }
                }
            }

            // Periodic flush
            if (i + 1) % flush_interval == 0 {
                if let Err(e) = self.log.flush() {
                    eprintln!("Flush failed: {}", e);
                    break;
                }
            }
        }

        // Final flush
        let _ = self.log.flush();

        let duration = start.elapsed();

        // Calculate latency percentiles
        self.latencies.sort_unstable();
        let min_latency = *self.latencies.first().unwrap_or(&0);
        let max_latency = *self.latencies.last().unwrap_or(&0);
        let p50_latency = self.percentile(50);
        let p95_latency = self.percentile(95);
        let p99_latency = self.percentile(99);

        PerfMetrics {
            ops_count,
            bytes_written,
            duration_micros: duration.as_micros() as u64,
            min_latency_micros: min_latency,
            max_latency_micros: max_latency,
            p50_latency_micros: p50_latency,
            p95_latency_micros: p95_latency,
            p99_latency_micros: p99_latency,
        }
    }

    fn percentile(&self, p: u8) -> u64 {
        if self.latencies.is_empty() {
            return 0;
        }
        let idx = ((p as f64 / 100.0) * self.latencies.len() as f64) as usize;
        self.latencies[idx.min(self.latencies.len() - 1)]
    }

    pub fn writer_stats(&self) -> (u64, u64) {
        self.writer.stats()
    }
}

/// Generate workload for testing
pub fn generate_workload(count: usize, avg_size: usize) -> Vec<MetadataOp> {
    let mut ops = Vec::with_capacity(count);

    for i in 0..count {
        let op = match i % 4 {
            0 => MetadataOp::CreateObject {
                bucket: format!("bucket-{}", i % 100),
                key: format!("object-{}", i),
                size: avg_size as u64,
            },
            1 => MetadataOp::UpdateObject {
                bucket: format!("bucket-{}", i % 100),
                key: format!("object-{}", i),
                metadata: "x".repeat(avg_size / 2),
            },
            2 => MetadataOp::DeleteObject {
                bucket: format!("bucket-{}", i % 100),
                key: format!("object-{}", i - 1),
            },
            _ => MetadataOp::CreateBucket {
                bucket: format!("bucket-{}", i),
            },
        };
        ops.push(op);
    }

    ops
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_perf_small_operations() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 8192,  // 8K blocks * 4KB = 32MB per buffer
                block_size: 4096,
            },
            buffer_pool_size: 5,  // 5 buffers for better throughput
            checkpoint_interval_secs: 0,  // Disabled for benchmark
        };

        let mut bench = WalBench::new(config);
        let workload = generate_workload(10000, 128); // 10K ops, 128B each
        let metrics = bench.run_workload(&workload, 500); // Flush every 500 ops

        println!("\n=== Small Operations (128B) ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Throughput: {:.2} MB/sec", metrics.throughput_mb_per_sec());
        println!("Avg latency: {:.2} µs", metrics.avg_latency_micros());
        println!("P50 latency: {} µs", metrics.p50_latency_micros);
        println!("P95 latency: {} µs", metrics.p95_latency_micros);
        println!("P99 latency: {} µs", metrics.p99_latency_micros);

        assert!(metrics.ops_count >= 10000);
        assert!(metrics.throughput_ops_per_sec() > 10000.0); // At least 10K ops/sec
    }

    #[test]
    fn test_wal_perf_medium_operations() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 8192,
                block_size: 4096,
            },
            buffer_pool_size: 5,
            checkpoint_interval_secs: 0,
        };

        let mut bench = WalBench::new(config);
        let workload = generate_workload(5000, 512); // 5K ops, 512B each
        let metrics = bench.run_workload(&workload, 250); // Flush every 250 ops

        println!("\n=== Medium Operations (512B) ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Throughput: {:.2} MB/sec", metrics.throughput_mb_per_sec());
        println!("Avg latency: {:.2} µs", metrics.avg_latency_micros());
        println!("P50 latency: {} µs", metrics.p50_latency_micros);
        println!("P95 latency: {} µs", metrics.p95_latency_micros);
        println!("P99 latency: {} µs", metrics.p99_latency_micros);

        assert!(metrics.ops_count >= 5000);
        assert!(metrics.throughput_ops_per_sec() > 5000.0);
    }

    #[test]
    fn test_wal_perf_large_operations() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 8192,
                block_size: 4096,
            },
            buffer_pool_size: 5,
            checkpoint_interval_secs: 0,
        };

        let mut bench = WalBench::new(config);
        let workload = generate_workload(2000, 2048); // 2K ops, 2KB each
        let metrics = bench.run_workload(&workload, 100); // Flush every 100 ops

        println!("\n=== Large Operations (2KB) ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Throughput: {:.2} MB/sec", metrics.throughput_mb_per_sec());
        println!("Avg latency: {:.2} µs", metrics.avg_latency_micros());
        println!("P50 latency: {} µs", metrics.p50_latency_micros);
        println!("P95 latency: {} µs", metrics.p95_latency_micros);
        println!("P99 latency: {} µs", metrics.p99_latency_micros);

        assert!(metrics.ops_count >= 2000);
        assert!(metrics.throughput_ops_per_sec() > 2000.0);
    }

    #[test]
    fn test_wal_perf_buffer_rotation() {
        // Small buffer to force frequent rotation
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 64, // Very small buffer
                block_size: 4096,
            },
            buffer_pool_size: 3,
            checkpoint_interval_secs: 0,
        };

        let mut bench = WalBench::new(config);
        let workload = generate_workload(1000, 256);
        let metrics = bench.run_workload(&workload, 100);

        println!("\n=== Buffer Rotation Test ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Avg latency: {:.2} µs", metrics.avg_latency_micros());

        let (write_count, _) = bench.writer_stats();
        println!("Buffer flushes: {}", write_count);

        assert!(metrics.ops_count >= 1000);
        assert!(write_count > 5); // Should have multiple rotations
    }

    #[test]
    fn test_wal_perf_high_frequency_flush() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 4096,
                block_size: 4096,
            },
            buffer_pool_size: 5,
            checkpoint_interval_secs: 0,
        };

        let mut bench = WalBench::new(config);
        let workload = generate_workload(5000, 128);
        let metrics = bench.run_workload(&workload, 50); // Flush every 50 ops (high frequency)

        println!("\n=== High Frequency Flush ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Avg latency: {:.2} µs", metrics.avg_latency_micros());
        println!("P99 latency: {} µs", metrics.p99_latency_micros);

        let (write_count, _) = bench.writer_stats();
        println!("Flushes: {}", write_count);

        assert!(metrics.ops_count >= 5000);
        assert!(write_count >= 100); // At least 5000/50 = 100 flushes
    }

    #[test]
    fn test_wal_perf_mixed_sizes() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 8192,
                block_size: 4096,
            },
            buffer_pool_size: 5,
            checkpoint_interval_secs: 0,
        };

        let mut bench = WalBench::new(config);

        // Generate mixed workload with varying sizes
        let mut workload = Vec::new();
        for i in 0..3000 {
            let size = match i % 5 {
                0 => 64,   // Small
                1 => 128,  // Small-medium
                2 => 512,  // Medium
                3 => 1024, // Large
                _ => 2048, // Very large
            };
            workload.push(MetadataOp::CreateObject {
                bucket: format!("bucket-{}", i % 50),
                key: format!("key-{}", i),
                size: size as u64,
            });
        }

        let metrics = bench.run_workload(&workload, 300);

        println!("\n=== Mixed Sizes ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Throughput: {:.2} MB/sec", metrics.throughput_mb_per_sec());
        println!("Latency stats:");
        println!("  Min: {} µs", metrics.min_latency_micros);
        println!("  P50: {} µs", metrics.p50_latency_micros);
        println!("  P95: {} µs", metrics.p95_latency_micros);
        println!("  P99: {} µs", metrics.p99_latency_micros);
        println!("  Max: {} µs", metrics.max_latency_micros);

        assert!(metrics.ops_count >= 3000);
    }

    #[test]
    fn test_wal_perf_sustained_load() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 16384,  // 16K blocks for sustained load
                block_size: 4096,
            },
            buffer_pool_size: 6,  // More buffers for sustained throughput
            checkpoint_interval_secs: 0,
        };

        let mut bench = WalBench::new(config);
        let workload = generate_workload(20000, 256); // 20K ops
        let metrics = bench.run_workload(&workload, 500);

        println!("\n=== Sustained Load (20K ops) ===");
        println!("Operations: {}", metrics.ops_count);
        println!("Duration: {:.2} sec", metrics.duration_micros as f64 / 1_000_000.0);
        println!("Throughput: {:.2} ops/sec", metrics.throughput_ops_per_sec());
        println!("Throughput: {:.2} MB/sec", metrics.throughput_mb_per_sec());
        println!("Total bytes: {:.2} MB", metrics.bytes_written as f64 / 1_048_576.0);
        println!("Avg latency: {:.2} µs", metrics.avg_latency_micros());
        println!("P95 latency: {} µs", metrics.p95_latency_micros);
        println!("P99 latency: {} µs", metrics.p99_latency_micros);

        let (write_count, total_bytes) = bench.writer_stats();
        println!("Write calls: {}", write_count);
        println!("Bytes written to storage: {:.2} MB", total_bytes as f64 / 1_048_576.0);

        assert!(metrics.ops_count >= 20000);
        assert!(metrics.throughput_ops_per_sec() > 10000.0);
    }
}
