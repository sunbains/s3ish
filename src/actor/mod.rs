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

//! Actor-based architecture for s3ish
//!
//! This module implements an actor model where all shared state is eliminated
//! in favor of message passing through lock-free channels. Actors process
//! messages sequentially, avoiding lock contention entirely.

pub mod backend;
pub mod fs_store;
pub mod messages;
pub mod metrics;

pub use backend::ActorStorageBackend;
pub use fs_store::{FsStoreActor, FsStoreReader};
pub use messages::{FsCommand, ObjectHeaders};
pub use metrics::Metrics;
