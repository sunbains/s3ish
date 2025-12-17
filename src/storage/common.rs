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

use crate::storage::StorageError;
use bytes::Bytes;

pub fn validate_bucket(bucket: &str) -> Result<(), StorageError> {
    if bucket.is_empty() {
        return Err(StorageError::InvalidInput(
            "bucket must be non-empty".into(),
        ));
    }
    Ok(())
}

pub fn validate_key(key: &str) -> Result<(), StorageError> {
    if key.is_empty() {
        return Err(StorageError::InvalidInput("key must be non-empty".into()));
    }
    if key.contains("..") {
        return Err(StorageError::InvalidInput("key cannot contain ..".into()));
    }
    Ok(())
}

pub fn compute_etag(data: &Bytes) -> String {
    // Use BLAKE3 for hardware-accelerated hashing (10-15x faster than MD5)
    // BLAKE3 uses SIMD, AVX-512, and other CPU extensions for maximum performance
    blake3::hash(data).to_hex().to_string()
}
