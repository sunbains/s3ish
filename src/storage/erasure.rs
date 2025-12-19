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

use crate::observability::metrics;
use crate::storage::StorageError;

/// Very small XOR-based erasure helper that simulates data+parity shards.
/// Supports reconstruction of a single missing shard when parity is present.
#[derive(Debug, Clone, Copy)]
pub struct Erasure {
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub block_size: usize,
}

impl Erasure {
    pub fn new(
        data_blocks: usize,
        parity_blocks: usize,
        block_size: usize,
    ) -> Result<Self, StorageError> {
        if data_blocks == 0 {
            return Err(StorageError::InvalidInput("data_blocks must be > 0".into()));
        }
        Ok(Self {
            data_blocks,
            parity_blocks,
            block_size: block_size.max(1),
        })
    }

    pub fn encode(&self, data: &[u8]) -> Result<(Vec<Vec<u8>>, usize), StorageError> {
        let start_time = std::time::Instant::now();

        if data.is_empty() {
            return Err(StorageError::InvalidInput("data cannot be empty".into()));
        }
        let shard_len = data.len().div_ceil(self.data_blocks).max(1);
        let total_shards = self.data_blocks + self.parity_blocks.max(1);
        let mut shards = vec![vec![0u8; shard_len]; total_shards];

        for (i, shard) in shards.iter_mut().enumerate().take(self.data_blocks) {
            let start = i * shard_len;
            if start < data.len() {
                let end = std::cmp::min(start + shard_len, data.len());
                shard[..end - start].copy_from_slice(&data[start..end]);
            }
        }

        // parity shard = XOR of all data shards
        let parity_idx = self.data_blocks;
        let (data_shards, parity_shards) = shards.split_at_mut(parity_idx);
        let parity = &mut parity_shards[0];
        for shard in data_shards.iter() {
            for (j, b) in shard.iter().enumerate() {
                parity[j] ^= b;
            }
        }

        // Record metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_erasure_encode(self.data_blocks, self.parity_blocks, duration);
        metrics::increment_erasure_bytes("encode", data.len() as u64);

        tracing::debug!(
            data_blocks = self.data_blocks,
            parity_blocks = self.parity_blocks,
            input_bytes = data.len(),
            duration_ms = duration * 1000.0,
            "Erasure encoding completed"
        );

        Ok((shards, data.len()))
    }

    pub fn decode(
        &self,
        shards: &mut [Option<Vec<u8>>],
        orig_len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let start_time = std::time::Instant::now();

        if shards.len() < self.data_blocks + 1 {
            return Err(StorageError::InvalidInput("not enough shards".into()));
        }
        let missing = shards.iter().filter(|s| s.is_none()).count();
        if missing > self.parity_blocks.max(1) {
            return Err(StorageError::Internal("too many missing shards".into()));
        }

        // If a data shard is missing, reconstruct using parity.
        if missing > 0 {
            // only support single missing shard for now
            if missing > 1 {
                return Err(StorageError::Internal(
                    "unsupported missing shard count".into(),
                ));
            }
            let missing_idx = shards
                .iter()
                .enumerate()
                .find_map(|(i, s)| if s.is_none() { Some(i) } else { None })
                .unwrap();

            let shard_len = shards
                .iter()
                .filter_map(|s| s.as_ref().map(|v| v.len()))
                .max()
                .unwrap_or(self.block_size);

            let mut recovered = vec![0u8; shard_len];
            for (i, shard_opt) in shards.iter().enumerate() {
                if i == missing_idx {
                    continue;
                }
                if let Some(shard) = shard_opt {
                    for (j, b) in shard.iter().enumerate() {
                        recovered[j] ^= b;
                    }
                }
            }
            shards[missing_idx] = Some(recovered);
        }

        let mut out = Vec::with_capacity(orig_len);
        for shard_opt in shards.iter().take(self.data_blocks) {
            let shard = shard_opt
                .as_ref()
                .ok_or_else(|| StorageError::Internal("missing data shard".into()))?;
            out.extend_from_slice(shard);
        }
        out.truncate(orig_len);

        // Record metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_erasure_decode(self.data_blocks, self.parity_blocks, duration);
        metrics::increment_erasure_bytes("decode", orig_len as u64);

        tracing::debug!(
            data_blocks = self.data_blocks,
            parity_blocks = self.parity_blocks,
            output_bytes = orig_len,
            missing_shards = missing,
            duration_ms = duration * 1000.0,
            "Erasure decoding completed"
        );

        Ok(out)
    }
}
