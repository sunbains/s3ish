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

//! Lock-free metrics using atomics
//!
//! All metrics are updated using atomic operations, eliminating the need
//! for locks entirely.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free metrics counters
#[derive(Debug, Default)]
pub struct Metrics {
    // Request counters
    pub put_requests: AtomicU64,
    pub get_requests: AtomicU64,
    pub head_requests: AtomicU64,
    pub delete_requests: AtomicU64,
    pub list_requests: AtomicU64,
    pub copy_requests: AtomicU64,
    pub multipart_requests: AtomicU64,

    // Bucket operations
    pub create_bucket_requests: AtomicU64,
    pub delete_bucket_requests: AtomicU64,
    pub list_buckets_requests: AtomicU64,

    // Data transfer
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,

    // Errors
    pub errors_total: AtomicU64,
    pub errors_4xx: AtomicU64,
    pub errors_5xx: AtomicU64,

    // Actor health
    pub messages_sent: AtomicU64,
    pub messages_received: AtomicU64,
    pub messages_timeout: AtomicU64,
}

impl Metrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    // Request counters
    pub fn inc_put(&self) {
        self.put_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_get(&self) {
        self.get_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_head(&self) {
        self.head_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_delete(&self) {
        self.delete_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_list(&self) {
        self.list_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_copy(&self) {
        self.copy_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_multipart(&self) {
        self.multipart_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_create_bucket(&self) {
        self.create_bucket_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_delete_bucket(&self) {
        self.delete_bucket_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_list_buckets(&self) {
        self.list_buckets_requests.fetch_add(1, Ordering::Relaxed);
    }

    // Data transfer
    pub fn add_bytes_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_bytes_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }

    // Errors
    pub fn inc_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_error_4xx(&self) {
        self.errors_4xx.fetch_add(1, Ordering::Relaxed);
        self.inc_error();
    }

    pub fn inc_error_5xx(&self) {
        self.errors_5xx.fetch_add(1, Ordering::Relaxed);
        self.inc_error();
    }

    // Actor health
    pub fn inc_message_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_message_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_message_timeout(&self) {
        self.messages_timeout.fetch_add(1, Ordering::Relaxed);
    }

    // Readers for metrics endpoint
    pub fn get_put_requests(&self) -> u64 {
        self.put_requests.load(Ordering::Relaxed)
    }

    pub fn get_get_requests(&self) -> u64 {
        self.get_requests.load(Ordering::Relaxed)
    }

    pub fn get_bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::Relaxed)
    }

    pub fn get_bytes_out(&self) -> u64 {
        self.bytes_out.load(Ordering::Relaxed)
    }

    pub fn get_errors_total(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }

    pub fn get_message_queue_depth(&self) -> i64 {
        let sent = self.messages_sent.load(Ordering::Relaxed) as i64;
        let received = self.messages_received.load(Ordering::Relaxed) as i64;
        sent - received
    }
}
