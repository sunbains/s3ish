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

pub mod auth;
pub mod config;
pub mod handler;
pub mod observability;
pub mod pb;
pub mod s3_http;
pub mod server;
pub mod service;
pub mod storage;

pub use auth::{AuthContext, Authenticator};
pub use handler::BaseHandler;
pub use s3_http::S3HttpHandler;
pub use storage::{ObjectMetadata, StorageBackend};
