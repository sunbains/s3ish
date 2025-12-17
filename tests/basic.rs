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

use s3ish::auth::file_auth::FileAuthenticator;
use s3ish::auth::Authenticator;
use s3ish::storage::in_memory::InMemoryStorage;
use s3ish::storage::StorageBackend;
use std::collections::HashMap;
use tonic::Request;

#[tokio::test]
async fn in_memory_put_get_roundtrip() {
    let storage = InMemoryStorage::new();
    storage.create_bucket("b1").await.unwrap();

    let meta = storage
        .put_object(
            "b1",
            "k1",
            bytes::Bytes::from_static(b"hello"),
            "text/plain",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(meta.size, 5);

    let (data, meta2) = storage.get_object("b1", "k1").await.unwrap();
    assert_eq!(&data[..], b"hello");
    assert_eq!(meta.etag, meta2.etag);
}

#[tokio::test]
async fn file_auth_accepts_valid_creds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("creds.txt");
    std::fs::write(
        &path, "a:b
",
    )
    .unwrap();

    let auth = FileAuthenticator::new(path).await.unwrap();

    let mut req = Request::new(());
    req.metadata_mut()
        .insert("x-access-key", "a".parse().unwrap());
    req.metadata_mut()
        .insert("x-secret-key", "b".parse().unwrap());

    let ctx = auth.authenticate(&req).await.unwrap();
    assert_eq!(ctx.access_key, "a");
}
