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

//! QUIC protocol handler for S3 operations
//!
//! Protocol format (compatible with /home/sunny/dev/quic-rust):
//! - 4 bytes: length of JSON header (big-endian u32)
//! - N bytes: JSON header (ChunkHeader)
//! - Remaining bytes: actual data

use anyhow::{self, Result};
use serde::{Deserialize, Serialize};

/// Type of frame being sent over a QUIC stream
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FrameType {
    /// Binary payload containing file contents (upload to server)
    UploadChunk,
    /// Binary payload coming from the server for a download
    DownloadChunk,
    /// Control frame to ask the server to start sending a file
    DownloadRequest,
}

/// Header placed in front of every stream payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub file_name: String,
    pub offset: u64,
    pub total_size: u64,
    pub chunk_len: u64,
    pub frame: FrameType,
}

impl ChunkHeader {
    /// Encode header as: [4-byte length][JSON body]
    pub fn encode(&self) -> Result<Vec<u8>> {
        let body = serde_json::to_vec(self)?;
        let mut framed = Vec::with_capacity(4 + body.len());
        let len: u32 = body
            .len()
            .try_into()
            .map_err(|_| anyhow::anyhow!("header too large"))?;
        framed.extend_from_slice(&len.to_be_bytes());
        framed.extend_from_slice(&body);
        Ok(framed)
    }

    /// Decode header from bytes, returns (header, bytes_consumed)
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 4 {
            anyhow::bail!("truncated header");
        }

        let len = u32::from_be_bytes(bytes[0..4].try_into()?);
        let end = 4 + len as usize;

        if bytes.len() < end {
            anyhow::bail!("header length {} exceeds payload {}", len, bytes.len());
        }

        let header: ChunkHeader = serde_json::from_slice(&bytes[4..end])?;
        Ok((header, end))
    }
}
