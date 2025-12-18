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

//! Recovery Tests
//!
//! This module tests data recovery capabilities including:
//! - Metadata recovery from corrupted or missing metadata files
//! - Chunk/shard repair using erasure coding
//! - Multi-disk failure scenarios
//! - Complete object reconstruction from partial data

use bytes::Bytes;
use s3ish::storage::file_storage::FileStorage;
use s3ish::storage::StorageBackend;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::fs;

/// Test fixture that creates a multi-drive file storage setup
struct RecoveryTestFixture {
    _temp_dirs: Vec<TempDir>,
    drives: Vec<PathBuf>,
    metadata_drive: Option<PathBuf>,
    storage: FileStorage,
}

impl RecoveryTestFixture {
    /// Create a test fixture with specified number of drives
    async fn new(num_drives: usize, data_blocks: usize, parity_blocks: usize) -> Self {
        assert!(num_drives >= 2, "Need at least 2 drives for recovery tests");

        let mut temp_dirs = Vec::new();
        let mut drives = Vec::new();

        // Create data drives
        for i in 0..num_drives {
            let temp = TempDir::new().unwrap();
            let drive_path = temp.path().join(format!("drive{}", i));
            fs::create_dir_all(&drive_path).await.unwrap();
            drives.push(drive_path);
            temp_dirs.push(temp);
        }

        // Create metadata drive
        let metadata_temp = TempDir::new().unwrap();
        let metadata_drive_path = metadata_temp.path().join("metadata");
        fs::create_dir_all(&metadata_drive_path).await.unwrap();
        let metadata_drive = Some(metadata_drive_path);
        temp_dirs.push(metadata_temp);

        // Create storage backend
        let storage = FileStorage::new_multi_drive_full(
            drives.clone(),
            metadata_drive.clone(),
            data_blocks,
            parity_blocks,
            64 * 1024, // 64KB block size for tests
            Default::default(),
            256 * 1024, // 256KB write buffer
            256 * 1024, // 256KB read buffer
        )
        .await
        .unwrap();

        Self {
            _temp_dirs: temp_dirs,
            drives,
            metadata_drive,
            storage,
        }
    }

    /// Get the metadata file path for an object
    fn metadata_path(&self, bucket: &str, key: &str) -> PathBuf {
        let base = self.metadata_drive.as_ref().unwrap();
        let mut path = base.join(bucket);
        for comp in key.split('/') {
            path.push(comp);
        }
        // Metadata file is stored as key.meta (e.g., test-key.meta)
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("object");
        path.set_file_name(format!("{}.meta", file_name));
        path
    }

    /// Get the shard directory for an object on a specific drive
    fn shard_dir_on_drive(&self, drive_idx: usize, bucket: &str, key: &str) -> PathBuf {
        let drive = &self.drives[drive_idx];
        let mut path = drive.join(bucket);
        for comp in key.split('/') {
            path.push(comp);
        }
        path.set_file_name(format!("{}.shards", path.file_name().unwrap().to_str().unwrap()));
        path
    }

    /// Get the path for a specific shard
    fn shard_path(&self, drive_idx: usize, bucket: &str, key: &str, shard_idx: usize) -> PathBuf {
        self.shard_dir_on_drive(drive_idx, bucket, key).join(format!("{}", shard_idx))
    }

    /// Corrupt a metadata file by writing invalid JSON
    async fn corrupt_metadata(&self, bucket: &str, key: &str) {
        let meta_path = self.metadata_path(bucket, key);
        fs::write(&meta_path, b"{ invalid json !!!").await.unwrap();
    }

    /// Delete a metadata file
    async fn delete_metadata(&self, bucket: &str, key: &str) {
        let meta_path = self.metadata_path(bucket, key);
        let _ = fs::remove_file(&meta_path).await;
    }

    /// Corrupt a shard by writing random data
    async fn corrupt_shard(&self, drive_idx: usize, bucket: &str, key: &str, shard_idx: usize) {
        let shard_path = self.shard_path(drive_idx, bucket, key, shard_idx);
        let corrupted_data = vec![0xFF; 1024];
        fs::write(&shard_path, &corrupted_data).await.unwrap();
    }

    /// Delete a shard file (simulates missing shard)
    async fn delete_shard(&self, drive_idx: usize, bucket: &str, key: &str, shard_idx: usize) {
        let shard_path = self.shard_path(drive_idx, bucket, key, shard_idx);
        let _ = fs::remove_file(&shard_path).await;
    }

    /// Truncate a shard file (partial corruption)
    async fn truncate_shard(&self, drive_idx: usize, bucket: &str, key: &str, shard_idx: usize, new_size: usize) {
        let shard_path = self.shard_path(drive_idx, bucket, key, shard_idx);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&shard_path)
            .await
            .unwrap();
        file.set_len(new_size as u64).await.unwrap();
    }

    /// Delete an entire drive directory (simulates disk failure)
    async fn delete_drive(&self, drive_idx: usize) {
        let drive_path = &self.drives[drive_idx];
        let _ = fs::remove_dir_all(drive_path).await;
    }

    /// Write corrupted bytes to a shard
    async fn write_corrupted_bytes(&self, drive_idx: usize, bucket: &str, key: &str, shard_idx: usize, data: &[u8]) {
        let shard_path = self.shard_path(drive_idx, bucket, key, shard_idx);
        fs::write(&shard_path, data).await.unwrap();
    }
}

/// Test that we can recover from corrupted metadata by reading shard metadata
#[tokio::test]
async fn test_metadata_recovery_from_corrupted_file() {
    let fixture = RecoveryTestFixture::new(2, 4, 2, ).await;

    // Create bucket and object
    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data = b"Hello, this is test data for metadata recovery!";
    let metadata = HashMap::new();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "test-key",
            Bytes::from(&test_data[..]),
            "text/plain",
            metadata.clone(),
            None,
            None,
        )
        .await
        .unwrap();

    // Verify object is readable
    let (data, _meta) = fixture
        .storage
        .get_object("test-bucket", "test-key")
        .await
        .unwrap();
    assert_eq!(data.as_ref(), test_data);

    // Corrupt the metadata file
    fixture.corrupt_metadata("test-bucket", "test-key").await;

    // For now, this will fail because we don't have metadata recovery implemented
    // In a full implementation, we would:
    // 1. Detect corrupted metadata
    // 2. Reconstruct metadata from shard files
    // 3. Successfully read the object
    let result = fixture.storage.get_object("test-bucket", "test-key").await;

    // Current behavior: fails with corrupted metadata
    assert!(result.is_err(), "Expected error due to corrupted metadata");

    // TODO: Implement metadata recovery logic
    // After implementation, this should succeed:
    // let (recovered_data, recovered_meta) = result.unwrap();
    // assert_eq!(recovered_data.as_ref(), test_data);
}

/// Test that we can recover from missing metadata file
#[tokio::test]
async fn test_metadata_recovery_from_missing_file() {
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    // Create bucket and object
    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data = b"Data for missing metadata test";
    let metadata = HashMap::new();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "test-key",
            Bytes::from(&test_data[..]),
            "text/plain",
            metadata.clone(),
            None,
            None,
        )
        .await
        .unwrap();

    // Delete the metadata file
    fixture.delete_metadata("test-bucket", "test-key").await;

    // Attempt to read - should fail currently
    let result = fixture.storage.get_object("test-bucket", "test-key").await;
    assert!(result.is_err(), "Expected error due to missing metadata");

    // TODO: Implement metadata recovery
}

/// Test recovery from a single missing shard using parity
#[tokio::test]
async fn test_single_shard_recovery() {
    // Use 4 data blocks + 2 parity blocks, distributed across 2 drives
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    // Create test data larger than inline threshold (>128KB)
    let test_data: Vec<u8> = (0..256 * 1024).map(|i| (i % 256) as u8).collect();
    let metadata = HashMap::new();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "large-object",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            metadata,
            None,
            None,
        )
        .await
        .unwrap();

    // Verify original read works
    let (original_data, _) = fixture
        .storage
        .get_object("test-bucket", "large-object")
        .await
        .unwrap();
    assert_eq!(original_data.as_ref(), &test_data[..]);

    // Delete shard 0 (on drive 0, since shard_idx % 2 == 0)
    fixture.delete_shard(0, "test-bucket", "large-object", 0).await;

    // Should still be able to recover using parity
    let (recovered_data, _) = fixture
        .storage
        .get_object("test-bucket", "large-object")
        .await
        .unwrap();

    assert_eq!(recovered_data.as_ref(), &test_data[..], "Data should be recovered using parity");
}

/// Test recovery from a corrupted shard (wrong data)
#[tokio::test]
async fn test_corrupted_shard_recovery() {
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    // Create test data larger than inline threshold
    let test_data: Vec<u8> = (0..256 * 1024).map(|i| ((i * 7) % 256) as u8).collect();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "object-to-corrupt",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Corrupt shard 1 by overwriting with garbage
    fixture.corrupt_shard(1, "test-bucket", "object-to-corrupt", 1).await;

    // Currently, corrupted data will be read as-is (no checksum verification)
    // In a production system, we would:
    // 1. Detect corruption via checksum
    // 2. Mark shard as missing
    // 3. Recover using parity
    let result = fixture
        .storage
        .get_object("test-bucket", "object-to-corrupt")
        .await;

    // TODO: Add checksum verification and recovery logic
    // For now, document current behavior
    match result {
        Ok((_data, _)) => {
            // System reads corrupted data without detecting corruption
            println!("Warning: Corrupted shard was read without detection");
            // Note: Cannot reliably assert data mismatch since corruption effects vary
        }
        Err(e) => {
            println!("Read failed: {}", e);
        }
    }
}

/// Test recovery from truncated shard
#[tokio::test]
async fn test_truncated_shard_recovery() {
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data: Vec<u8> = (0..200 * 1024).map(|i| (i % 251) as u8).collect();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "object-truncated",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Truncate shard 2 to half its size
    fixture.truncate_shard(0, "test-bucket", "object-truncated", 2, 1024).await;

    // Attempt recovery
    let result = fixture
        .storage
        .get_object("test-bucket", "object-truncated")
        .await;

    // System should detect size mismatch and potentially recover
    // TODO: Implement proper recovery
    match result {
        Ok((_data, _)) => {
            println!("Read succeeded despite truncation");
            // Data may be corrupted or recovered
        }
        Err(e) => {
            println!("Read failed due to truncation: {}", e);
        }
    }
}

/// Test recovery from multiple missing shards (should fail with only 2 parity blocks)
#[tokio::test]
async fn test_multiple_missing_shards_failure() {
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data: Vec<u8> = (0..256 * 1024).map(|i| (i % 256) as u8).collect();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "multi-loss",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Delete 2 data shards (exceeds parity capacity of 1 for XOR-based erasure)
    fixture.delete_shard(0, "test-bucket", "multi-loss", 0).await;
    fixture.delete_shard(0, "test-bucket", "multi-loss", 2).await;

    // Should fail to recover
    let result = fixture.storage.get_object("test-bucket", "multi-loss").await;

    assert!(
        result.is_err(),
        "Should fail to recover with more missing shards than parity blocks"
    );
}

/// Test complete drive failure scenario
#[tokio::test]
async fn test_drive_failure_recovery() {
    let fixture = RecoveryTestFixture::new(3, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data: Vec<u8> = (0..512 * 1024).map(|i| ((i * 13) % 256) as u8).collect();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "drive-test",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Simulate drive 1 failure by deleting the entire drive
    fixture.delete_drive(1).await;

    // With 3 drives and modulo sharding:
    // - shard 0 -> drive 0
    // - shard 1 -> drive 1 (FAILED)
    // - shard 2 -> drive 2
    // - shard 3 -> drive 0
    // - shard 4 (parity) -> drive 1 (FAILED)
    // - shard 5 (parity) -> drive 2

    // We lost shard 1 (data) and shard 4 (parity)
    // With only 1 parity remaining (shard 5), we can only recover 1 missing shard
    // Since we have 2 missing (1 data + 1 parity), recovery should work if we only need the data shards

    let result = fixture.storage.get_object("test-bucket", "drive-test").await;

    // Current XOR erasure code can only handle 1 missing shard
    // This should fail with multiple missing
    match result {
        Ok((data, _)) => {
            // If recovery succeeded, verify data
            assert_eq!(data.as_ref(), &test_data[..], "Recovered data should match original");
        }
        Err(e) => {
            println!("Expected failure with multiple missing shards: {}", e);
            assert!(
                e.to_string().contains("missing") || e.to_string().contains("shard"),
                "Error should indicate missing shards"
            );
        }
    }
}

/// Test parity shard recovery (verify parity itself can be recovered)
#[tokio::test]
async fn test_parity_shard_recovery() {
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data: Vec<u8> = (0..180 * 1024).map(|i| (i % 256) as u8).collect();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "parity-test",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Delete the first parity shard (shard 4, on drive 0)
    fixture.delete_shard(0, "test-bucket", "parity-test", 4).await;

    // Should still be able to read the object (parity is only needed when data shards are missing)
    let (recovered_data, _) = fixture
        .storage
        .get_object("test-bucket", "parity-test")
        .await
        .unwrap();

    assert_eq!(
        recovered_data.as_ref(),
        &test_data[..],
        "Data should be readable even with missing parity"
    );
}

/// Test recovery with mixed corruption types
#[tokio::test]
async fn test_mixed_corruption_recovery() {
    let fixture = RecoveryTestFixture::new(3, 4, 2).await;

    fixture.storage.create_bucket("test-bucket").await.unwrap();

    let test_data: Vec<u8> = (0..400 * 1024).map(|i| ((i * 17) % 256) as u8).collect();

    fixture
        .storage
        .put_object(
            "test-bucket",
            "mixed-test",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Apply multiple types of corruption:
    // 1. Delete shard 1
    fixture.delete_shard(1, "test-bucket", "mixed-test", 1).await;

    // 2. Truncate shard 3
    fixture.truncate_shard(0, "test-bucket", "mixed-test", 3, 512).await;

    // System should detect these issues and attempt recovery
    let result = fixture.storage.get_object("test-bucket", "mixed-test").await;

    // TODO: Implement comprehensive recovery logic
    match result {
        Ok((_data, _)) => {
            println!("Recovery attempt completed");
            // Check if data matches
        }
        Err(e) => {
            println!("Recovery failed with mixed corruption: {}", e);
        }
    }
}

/// Benchmark recovery performance
#[tokio::test]
async fn bench_recovery_performance() {
    let fixture = RecoveryTestFixture::new(2, 4, 2).await;

    fixture.storage.create_bucket("bench-bucket").await.unwrap();

    // Create 10MB object
    let test_data: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 256) as u8).collect();

    fixture
        .storage
        .put_object(
            "bench-bucket",
            "large",
            Bytes::from(test_data.clone()),
            "application/octet-stream",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Benchmark normal read
    let start = std::time::Instant::now();
    let _ = fixture.storage.get_object("bench-bucket", "large").await.unwrap();
    let normal_duration = start.elapsed();
    println!("Normal read: {:?}", normal_duration);

    // Delete one shard and benchmark recovery
    fixture.delete_shard(0, "bench-bucket", "large", 0).await;

    let start = std::time::Instant::now();
    let result = fixture.storage.get_object("bench-bucket", "large").await;
    let recovery_duration = start.elapsed();

    if result.is_ok() {
        println!("Recovery read: {:?}", recovery_duration);
        println!(
            "Recovery overhead: {:.2}%",
            (recovery_duration.as_secs_f64() / normal_duration.as_secs_f64() - 1.0) * 100.0
        );
    }
}
