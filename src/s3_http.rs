use crate::auth::AuthError;
use crate::handler::BaseHandler;
use crate::storage::StorageError;
use axum::{
    body::Body,
    extract::{OriginalUri, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use hex;
use hmac::{Hmac, Mac};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

// AWS SigV4 encoding: leave unreserved + slash unencoded.
const AWS_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');

/// S3 HTTP handler wrapping BaseHandler
#[derive(Clone)]
pub struct S3HttpHandler {
    state: S3State,
}

#[derive(Clone)]
struct S3State {
    handler: Arc<BaseHandler>,
    renderer: Arc<dyn ResponseRenderer>,
    context: ResponseContext,
    multipart_uploads: Arc<Mutex<HashMap<String, MultipartUpload>>>,
}

#[derive(Clone)]
pub struct ResponseContext {
    region: String,
    request_id_prefix: String,
}

impl ResponseContext {
    pub fn new(region: String, request_id_prefix: String) -> Self {
        Self {
            region,
            request_id_prefix,
        }
    }

    pub fn request_id(&self) -> String {
        format!("{}{}", self.request_id_prefix, Uuid::new_v4().simple())
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}

impl Default for ResponseContext {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            request_id_prefix: "req-".to_string(),
        }
    }
}

static GLOBAL_RESPONSE_CONTEXT: OnceLock<ResponseContext> = OnceLock::new();

fn global_response_context() -> &'static ResponseContext {
    GLOBAL_RESPONSE_CONTEXT.get_or_init(ResponseContext::default)
}

impl S3State {
    async fn storage_buckets(&self) -> Result<Vec<BucketEntry>, S3Error> {
        let buckets = self.handler.storage.list_buckets().await?;
        Ok(buckets
            .into_iter()
            .map(|name| BucketEntry {
                name,
                creation_date_unix_secs: None,
            })
            .collect())
    }

    async fn bucket_exists(&self, bucket: &str) -> Result<bool, S3Error> {
        match self.handler.storage.list_objects(bucket, "", 1).await {
            Ok(_) => Ok(true),
            Err(StorageError::BucketNotFound(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn request_id(&self) -> String {
        self.context.request_id()
    }

    async fn init_multipart(&self, bucket: &str, key: &str) -> Result<String, S3Error> {
        if !self.bucket_exists(bucket).await? {
            return Err(S3Error::BucketNotFound(bucket.to_string()));
        }
        let upload_id = Uuid::new_v4().simple().to_string();
        let mut map = self.multipart_uploads.lock().await;
        map.insert(
            upload_id.clone(),
            MultipartUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                parts: HashMap::new(),
            },
        );
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> Result<String, S3Error> {
        let mut map = self.multipart_uploads.lock().await;
        let entry = map
            .get_mut(upload_id)
            .ok_or_else(|| S3Error::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
        if entry.bucket != bucket || entry.key != key {
            return Err(S3Error::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        let etag = format!("{:x}", md5::compute(body));
        entry.parts.insert(
            part_number,
            MultipartPart {
                etag: etag.clone(),
                data: Bytes::copy_from_slice(body),
            },
        );
        Ok(etag)
    }

    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        requested_parts: Option<Vec<(u32, String)>>,
    ) -> Result<String, S3Error> {
        let mut map = self.multipart_uploads.lock().await;
        let entry = map.remove(upload_id).ok_or_else(|| S3Error::NoSuchUpload {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;
        if entry.bucket != bucket || entry.key != key {
            return Err(S3Error::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let parts: Vec<(u32, MultipartPart)> = if let Some(req_parts) = requested_parts {
            let mut out = Vec::new();
            for (num, etag) in req_parts {
                match entry.parts.get(&num) {
                    Some(p) if normalize_etag(&p.etag) == normalize_etag(&etag) => {
                        out.push((num, p.clone()))
                    }
                    _ => {
                        return Err(S3Error::InvalidPart(format!(
                            "invalid part {} for upload {}",
                            num, upload_id
                        )))
                    }
                }
            }
            out
        } else {
            entry.parts.into_iter().collect()
        };

        if parts.is_empty() {
            return Err(S3Error::InvalidPart(
                "no parts provided for completion".to_string(),
            ));
        }

        let mut ordered = parts;
        ordered.sort_by_key(|(n, _)| *n);
        let mut buf = BytesMut::new();
        for (_, part) in ordered {
            buf.extend_from_slice(&part.data);
        }

        let meta = self
            .handler
            .storage
            .put_object(
                bucket,
                key,
                buf.freeze(),
                "application/octet-stream",
                HashMap::new(),
            )
            .await?;
        Ok(meta.etag)
    }

    async fn abort_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), S3Error> {
        let mut map = self.multipart_uploads.lock().await;
        match map.remove(upload_id) {
            Some(entry) if entry.bucket == bucket && entry.key == key => Ok(()),
            _ => Err(S3Error::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }),
        }
    }
}

impl S3HttpHandler {
    pub fn new(handler: BaseHandler) -> Self {
        Self::with_renderer_and_context(
            handler,
            Arc::new(XmlResponseRenderer),
            ResponseContext::default(),
        )
    }

    pub fn new_with_context(handler: BaseHandler, ctx: ResponseContext) -> Self {
        Self::with_renderer_and_context(handler, Arc::new(XmlResponseRenderer), ctx)
    }

    pub fn with_renderer_and_context(
        handler: BaseHandler,
        renderer: Arc<dyn ResponseRenderer>,
        ctx: ResponseContext,
    ) -> Self {
        let global_ctx = GLOBAL_RESPONSE_CONTEXT.get_or_init(|| ctx.clone());
        let state = S3State {
            handler: Arc::new(handler),
            renderer,
            context: global_ctx.clone(),
            multipart_uploads: Arc::new(Mutex::new(HashMap::new())),
        };
        Self { state }
    }

    /// Create the router for S3 HTTP API
    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(list_buckets))
            .route(
                "/:bucket",
                get(list_objects)
                    .put(create_bucket)
                    .delete(delete_bucket)
                    .post(delete_objects)
                    .head(head_bucket),
            )
            .route("/:bucket/", get(list_objects))
            .route(
                "/:bucket/*key",
                post(post_object)
                    .put(put_object)
                    .patch(copy_object)
                    .get(get_object)
                    .delete(delete_object)
                    .head(head_object),
            )
            .with_state(self.state)
    }

    /// Authenticate a request using SigV4 (preferred) or the legacy x-access-key headers.
    async fn authenticate(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        payload: &[u8],
    ) -> Result<(), S3Error> {
        // SigV4: Authorization header present
        if let Some(authz) = headers.get("authorization") {
            let authz = authz.to_str().map_err(|_| S3Error::InvalidCredentials)?;
            if authz.starts_with("AWS4-HMAC-SHA256") {
                self.verify_sigv4(method, uri, headers, payload, authz)
                    .await?;
                return Ok(());
            }
        }

        // Legacy header-based auth (for simple testing or gRPC parity)
        let access_key = headers
            .get("x-amz-access-key")
            .or_else(|| headers.get("x-access-key"))
            .ok_or(S3Error::MissingCredentials)?
            .to_str()
            .map_err(|_| S3Error::InvalidCredentials)?;

        let secret_key = headers
            .get("x-amz-secret-key")
            .or_else(|| headers.get("x-secret-key"))
            .ok_or(S3Error::MissingCredentials)?
            .to_str()
            .map_err(|_| S3Error::InvalidCredentials)?;

        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("x-access-key", access_key.parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", secret_key.parse().unwrap());

        self.state
            .handler
            .auth
            .authenticate(&req)
            .await
            .map(|_| ())
            .map_err(|e| match e {
                AuthError::MissingCredentials => S3Error::MissingCredentials,
                AuthError::InvalidCredentials => S3Error::InvalidCredentials,
                AuthError::Internal(msg) => S3Error::Internal(msg),
            })
    }

    async fn verify_sigv4(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        payload: &[u8],
        auth_header: &str,
    ) -> Result<(), S3Error> {
        let parsed = parse_sigv4_authorization(auth_header)?;

        let amz_date = headers
            .get("x-amz-date")
            .ok_or(S3Error::MissingCredentials)?
            .to_str()
            .map_err(|_| S3Error::InvalidCredentials)?;

        if !amz_date.starts_with(&parsed.date) {
            return Err(S3Error::InvalidCredentials);
        }

        let payload_header = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok());
        let computed_payload_hash = sha256_hex(payload);
        let payload_hash = match payload_header {
            Some("UNSIGNED-PAYLOAD") => "UNSIGNED-PAYLOAD".to_string(),
            Some(v) => {
                if v != computed_payload_hash {
                    return Err(S3Error::InvalidCredentials);
                }
                v.to_string()
            }
            None => computed_payload_hash,
        };

        let canonical_request =
            build_canonical_request(method, uri, headers, &parsed.signed_headers, &payload_hash)?;
        let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
            amz_date, parsed.date, parsed.region, parsed.service, canonical_request_hash
        );

        let secret = self
            .state
            .handler
            .auth
            .secret_for(&parsed.access_key)
            .await
            .map_err(|_| S3Error::InvalidCredentials)?;
        let sig = sign_string(&secret, &parsed, &string_to_sign)?;
        let computed_signature = hex::encode(sig);

        if computed_signature != parsed.signature {
            return Err(S3Error::InvalidCredentials);
        }

        Ok(())
    }
}

#[derive(Debug)]
enum S3Error {
    MissingCredentials,
    InvalidCredentials,
    BucketNotFound(String),
    BucketNotEmpty(String),
    BucketAlreadyExists(String),
    ObjectNotFound { bucket: String, key: String },
    NoSuchUpload { bucket: String, key: String },
    InvalidLocationConstraint { expected: String, provided: String },
    InvalidInput(String),
    InvalidPart(String),
    InvalidRange(u64),
    PreconditionFailed(String),
    Internal(String),
}

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const REQUEST_ID_HEADER: &str = "x-amz-request-id";
const REQUEST_ID_2_HEADER: &str = "x-amz-id-2";
const BUCKET_REGION_HEADER: &str = "x-amz-bucket-region";

#[derive(Clone)]
pub struct BucketEntry {
    name: String,
    creation_date_unix_secs: Option<i64>,
}

#[derive(Clone)]
pub struct ObjectEntry {
    key: String,
    last_modified_unix_secs: i64,
    etag: String,
    size: u64,
}

#[derive(Debug)]
struct MultipartUpload {
    bucket: String,
    key: String,
    parts: HashMap<u32, MultipartPart>,
}

#[derive(Debug, Clone)]
struct MultipartPart {
    etag: String,
    data: Bytes,
}

#[derive(Debug, Deserialize)]
struct DeleteQuery {
    delete: Option<String>,
}

pub trait ResponseRenderer: Send + Sync {
    fn list_buckets(&self, buckets: &[BucketEntry]) -> Response;
    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        objects: &[ObjectEntry],
    ) -> Response;
}

pub struct XmlResponseRenderer;

impl ResponseRenderer for XmlResponseRenderer {
    fn list_buckets(&self, buckets: &[BucketEntry]) -> Response {
        let mut body = String::new();
        body.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
        body.push_str(&format!(
            "<ListAllMyBucketsResult xmlns=\"{}\"><Buckets>",
            XMLNS
        ));
        for bucket in buckets {
            body.push_str("<Bucket>");
            body.push_str(&format!("<Name>{}</Name>", bucket.name));
            let ts = bucket.creation_date_unix_secs.unwrap_or(0);
            body.push_str(&format!(
                "<CreationDate>{}</CreationDate>",
                format_rfc3339(ts)
            ));
            body.push_str("</Bucket>");
        }
        body.push_str("</Buckets></ListAllMyBucketsResult>");

        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/xml")],
            body,
        )
            .into_response()
    }

    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        objects: &[ObjectEntry],
    ) -> Response {
        let mut body = String::new();
        body.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
        body.push_str(&format!("<ListBucketResult xmlns=\"{}\">", XMLNS));
        body.push_str(&format!("<Name>{}</Name>", bucket));
        body.push_str(&format!("<Prefix>{}</Prefix>", prefix));
        body.push_str("<Marker></Marker>");
        body.push_str(&format!("<MaxKeys>{}</MaxKeys>", max_keys));
        body.push_str(&format!("<KeyCount>{}</KeyCount>", objects.len()));
        body.push_str("<IsTruncated>false</IsTruncated>");
        for meta in objects {
            body.push_str("<Contents>");
            body.push_str(&format!("<Key>{}</Key>", meta.key));
            body.push_str(&format!(
                "<LastModified>{}</LastModified>",
                format_rfc3339(meta.last_modified_unix_secs)
            ));
            body.push_str(&format!("<ETag>\"{}\"</ETag>", meta.etag));
            body.push_str(&format!("<Size>{}</Size>", meta.size));
            body.push_str("<StorageClass>STANDARD</StorageClass>");
            body.push_str("</Contents>");
        }
        body.push_str("</ListBucketResult>");

        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/xml")],
            body,
        )
            .into_response()
    }
}

impl From<StorageError> for S3Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::BucketNotFound(b) => S3Error::BucketNotFound(b),
            StorageError::BucketNotEmpty(b) => S3Error::BucketNotEmpty(b),
            StorageError::ObjectNotFound { bucket, key } => S3Error::ObjectNotFound { bucket, key },
            StorageError::InvalidInput(msg) => S3Error::InvalidInput(msg),
            StorageError::Internal(msg) => S3Error::Internal(msg),
        }
    }
}

#[derive(Debug)]
struct SigV4Authorization {
    access_key: String,
    date: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn hmac_sign(key: &[u8], data: &[u8]) -> Result<Vec<u8>, S3Error> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| S3Error::InvalidCredentials)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn sign_string(
    secret: &str,
    auth: &SigV4Authorization,
    string_to_sign: &str,
) -> Result<Vec<u8>, S3Error> {
    let k_date = hmac_sign(format!("AWS4{}", secret).as_bytes(), auth.date.as_bytes())?;
    let k_region = hmac_sign(&k_date, auth.region.as_bytes())?;
    let k_service = hmac_sign(&k_region, auth.service.as_bytes())?;
    let k_signing = hmac_sign(&k_service, b"aws4_request")?;
    hmac_sign(&k_signing, string_to_sign.as_bytes())
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonical_uri(path: &str) -> String {
    if path == "/" || path.is_empty() {
        return "/".to_string();
    }

    let mut out = String::new();
    for (i, segment) in path.split('/').enumerate() {
        if i > 0 {
            out.push('/');
        }
        out.push_str(&utf8_percent_encode(segment, AWS_ENCODE_SET).to_string());
    }
    if path.ends_with('/') && !out.ends_with('/') {
        out.push('/');
    }
    if !path.starts_with('/') {
        out.insert(0, '/');
    }
    out
}

fn canonical_query(uri: &Uri) -> String {
    let Some(query) = uri.query() else {
        return String::new();
    };

    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                utf8_percent_encode(name, AWS_ENCODE_SET).to_string(),
                utf8_percent_encode(value, AWS_ENCODE_SET).to_string(),
            )
        })
        .collect();

    pairs.sort_by(|a, b| {
        let key_cmp = a.0.cmp(&b.0);
        if key_cmp == std::cmp::Ordering::Equal {
            a.1.cmp(&b.1)
        } else {
            key_cmp
        }
    });

    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &HeaderMap, signed_headers: &[String]) -> Result<String, S3Error> {
    let mut out = String::new();
    for name in signed_headers {
        let header_name = name
            .parse::<header::HeaderName>()
            .map_err(|_| S3Error::InvalidCredentials)?;
        let value = headers
            .get(&header_name)
            .ok_or(S3Error::InvalidCredentials)?
            .to_str()
            .map_err(|_| S3Error::InvalidCredentials)?;
        let normalized = normalize_header_value(value);
        out.push_str(&format!("{}:{}\n", name, normalized));
    }
    Ok(out)
}

fn build_canonical_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    signed_headers: &[String],
    payload_hash: &str,
) -> Result<String, S3Error> {
    let canonical_uri = canonical_uri(uri.path());
    let canonical_query = canonical_query(uri);
    let canonical_headers = canonical_headers(headers, signed_headers)?;
    let signed_headers_str = signed_headers.join(";");

    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers_str,
        payload_hash
    ))
}

fn format_rfc3339(ts: i64) -> String {
    let dt = DateTime::<Utc>::from_timestamp(ts, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn format_http_date(ts: i64) -> String {
    let dt = Utc
        .timestamp_opt(ts, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn parse_create_bucket_configuration(body: &[u8], expected_region: &str) -> Result<(), S3Error> {
    let text = std::str::from_utf8(body)
        .map_err(|_| S3Error::InvalidInput("Invalid XML encoding".to_string()))?;
    if text.trim().is_empty() {
        return Ok(());
    }
    // Very small parser: ensure the body contains the expected root element; ignore LocationConstraint.
    let has_root = text.contains("<CreateBucketConfiguration")
        && text.contains("</CreateBucketConfiguration>");
    if !has_root {
        return Err(S3Error::InvalidInput(
            "Invalid CreateBucketConfiguration".to_string(),
        ));
    }

    if let Some(region) = extract_location_constraint(text) {
        if region != expected_region {
            return Err(S3Error::InvalidLocationConstraint {
                expected: expected_region.to_string(),
                provided: region,
            });
        }
    }
    Ok(())
}

fn extract_location_constraint(xml: &str) -> Option<String> {
    let start_tag = "<LocationConstraint>";
    let end_tag = "</LocationConstraint>";
    let start = xml.find(start_tag)? + start_tag.len();
    let end = xml[start..].find(end_tag)? + start;
    Some(xml[start..end].trim().to_string())
}

fn parse_complete_multipart_body(body: &[u8]) -> Result<Vec<(u32, String)>, S3Error> {
    let text = std::str::from_utf8(body)
        .map_err(|_| S3Error::InvalidInput("Invalid XML encoding".to_string()))?;
    let mut out = Vec::new();
    let mut start = 0;
    while let Some(idx) = text[start..].find("<Part>") {
        let part_start = start + idx + "<Part>".len();
        let part_end_rel = text[part_start..]
            .find("</Part>")
            .ok_or_else(|| S3Error::InvalidPart("Malformed CompleteMultipartUpload".to_string()))?;
        let part_xml = &text[part_start..part_start + part_end_rel];

        let pn = extract_between(part_xml, "<PartNumber>", "</PartNumber>")
            .ok_or_else(|| S3Error::InvalidPart("Missing PartNumber".to_string()))?;
        let etag = extract_between(part_xml, "<ETag>", "</ETag>")
            .ok_or_else(|| S3Error::InvalidPart("Missing ETag".to_string()))?;
        let pn_num: u32 = pn
            .trim()
            .parse()
            .map_err(|_| S3Error::InvalidPart("Invalid PartNumber".to_string()))?;
        out.push((pn_num, normalize_etag(&etag)));
        start = part_start + part_end_rel + "</Part>".len();
    }

    if out.is_empty() {
        return Err(S3Error::InvalidPart(
            "No parts provided for completion".to_string(),
        ));
    }
    Ok(out)
}

fn parse_multi_delete_body(body: &[u8]) -> Result<(Vec<String>, bool), S3Error> {
    let text = std::str::from_utf8(body)
        .map_err(|_| S3Error::InvalidInput("Invalid XML encoding".to_string()))?;
    let quiet = text.to_lowercase().contains("<quiet>true</quiet>");

    let mut keys = Vec::new();
    let mut start = 0;
    while let Some(idx) = text[start..].find("<Key>") {
        let key_start = start + idx + "<Key>".len();
        if let Some(end_rel) = text[key_start..].find("</Key>") {
            let end = key_start + end_rel;
            keys.push(text[key_start..end].to_string());
            start = end + "</Key>".len();
        } else {
            break;
        }
    }

    if keys.is_empty() {
        return Err(S3Error::InvalidInput(
            "No keys provided for multi-delete".to_string(),
        ));
    }

    Ok((keys, quiet))
}

fn parse_copy_source(raw: &str) -> Option<(String, String)> {
    let trimmed = raw.trim().trim_start_matches('/');
    let no_query = trimmed.split('?').next().unwrap_or(trimmed);
    let mut parts = no_query.splitn(2, '/');
    let bucket = parts.next()?.trim();
    let key = parts.next()?.trim();
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    let bucket_decoded = percent_decode_str(bucket).decode_utf8().ok()?;
    let key_decoded = percent_decode_str(key).decode_utf8().ok()?;
    Some((bucket_decoded.to_string(), key_decoded.to_string()))
}

fn generate_request_id() -> String {
    global_response_context().request_id()
}

fn normalize_etag(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

fn extract_between(text: &str, start: &str, end: &str) -> Option<String> {
    let s = text.find(start)? + start.len();
    let e = text[s..].find(end)? + s;
    Some(text[s..e].to_string())
}

fn parse_range(header: &str, total: u64) -> Result<(u64, u64), S3Error> {
    // Only support single range: bytes=start-end
    if !header.starts_with("bytes=") {
        return Err(S3Error::InvalidRange(total));
    }
    let spec = &header[6..];
    let (start, end_opt) = spec.split_once('-').ok_or(S3Error::InvalidRange(total))?;

    if start.is_empty() {
        // suffix range: bytes=-N
        let suffix: u64 = end_opt.parse().map_err(|_| S3Error::InvalidRange(total))?;
        if suffix == 0 {
            return Err(S3Error::InvalidRange(total));
        }
        let start_pos = total.saturating_sub(suffix);
        return Ok((start_pos, total.saturating_sub(1)));
    }

    let start_num: u64 = start.parse().map_err(|_| S3Error::InvalidRange(total))?;
    let end_num: u64 = if end_opt.is_empty() {
        total.saturating_sub(1)
    } else {
        end_opt.parse().map_err(|_| S3Error::InvalidRange(total))?
    };
    if start_num >= total {
        return Err(S3Error::InvalidRange(total));
    }
    Ok((start_num, end_num))
}

fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (name, value) in headers {
        if let Some(name_str) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(val_str) = value.to_str() {
                out.insert(name_str.to_string(), val_str.to_string());
            }
        }
    }
    out
}

fn apply_metadata_headers(headers: &mut HeaderMap, metadata: &HashMap<String, String>) {
    for (k, v) in metadata {
        if let Ok(name) = header::HeaderName::from_bytes(format!("x-amz-meta-{}", k).as_bytes()) {
            if let Ok(val) = HeaderValue::from_str(v) {
                let _ = headers.insert(name, val);
            }
        }
    }
}

fn attach_request_ids(mut resp: Response, request_id: &str) -> Response {
    let headers = resp.headers_mut();
    let _ = headers.insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id).unwrap_or_else(|_| HeaderValue::from_static("0000")),
    );
    let _ = headers.insert(
        REQUEST_ID_2_HEADER,
        HeaderValue::from_str(request_id).unwrap_or_else(|_| HeaderValue::from_static("0000")),
    );
    resp
}

fn attach_common_headers(mut resp: Response, state: &S3State) -> Response {
    let request_id = state.request_id();
    resp = attach_request_ids(resp, &request_id);
    let _ = resp.headers_mut().insert(
        BUCKET_REGION_HEADER,
        HeaderValue::from_str(state.context.region())
            .unwrap_or_else(|_| HeaderValue::from_static("us-east-1")),
    );
    resp
}

fn attach_error_headers(mut resp: Response) -> Response {
    let ctx = global_response_context();
    let request_id = ctx.request_id();
    resp = attach_request_ids(resp, &request_id);
    let _ = resp.headers_mut().insert(
        BUCKET_REGION_HEADER,
        HeaderValue::from_str(ctx.region())
            .unwrap_or_else(|_| HeaderValue::from_static("us-east-1")),
    );
    resp
}

fn parse_sigv4_authorization(value: &str) -> Result<SigV4Authorization, S3Error> {
    let (scheme, rest) = value.split_once(' ').ok_or(S3Error::InvalidCredentials)?;
    if scheme != "AWS4-HMAC-SHA256" {
        return Err(S3Error::InvalidCredentials);
    }

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in rest.split(',') {
        let part = part.trim();
        let (key, val) = part.split_once('=').ok_or(S3Error::InvalidCredentials)?;
        match key {
            "Credential" => credential = Some(val.to_string()),
            "SignedHeaders" => {
                let headers = val
                    .split(';')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_lowercase())
                    .collect::<Vec<_>>();
                signed_headers = Some(headers);
            }
            "Signature" => signature = Some(val.to_lowercase()),
            _ => {}
        }
    }

    let cred = credential.ok_or(S3Error::MissingCredentials)?;
    let sig = signature.ok_or(S3Error::MissingCredentials)?;
    let signed_headers = signed_headers.ok_or(S3Error::MissingCredentials)?;

    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(S3Error::InvalidCredentials);
    }

    Ok(SigV4Authorization {
        access_key: parts[0].to_string(),
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
        signed_headers,
        signature: sig,
    })
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let request_id = generate_request_id();
        let (status, code, message, resource): (StatusCode, &str, String, String) = match self {
            S3Error::MissingCredentials => (
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "Missing credentials".to_string(),
                "".to_string(),
            ),
            S3Error::InvalidCredentials => (
                StatusCode::FORBIDDEN,
                "SignatureDoesNotMatch",
                "The request signature we calculated does not match the signature you provided."
                    .to_string(),
                "".to_string(),
            ),
            S3Error::BucketNotFound(b) => (
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                format!("The specified bucket does not exist: {}", b),
                format!("/{}", b),
            ),
            S3Error::BucketNotEmpty(b) => (
                StatusCode::CONFLICT,
                "BucketNotEmpty",
                format!("The bucket you tried to delete is not empty: {}", b),
                format!("/{}", b),
            ),
            S3Error::BucketAlreadyExists(b) => (
                StatusCode::CONFLICT,
                "BucketAlreadyExists",
                format!("The requested bucket name is not available: {}", b),
                format!("/{}", b),
            ),
            S3Error::ObjectNotFound { bucket, key } => (
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.".to_string(),
                format!("/{}/{}", bucket, key),
            ),
            S3Error::NoSuchUpload { bucket, key } => (
                StatusCode::NOT_FOUND,
                "NoSuchUpload",
                "The specified multipart upload does not exist.".to_string(),
                format!("/{}/{}", bucket, key),
            ),
            S3Error::InvalidLocationConstraint { expected, provided } => (
                StatusCode::BAD_REQUEST,
                "InvalidLocationConstraint",
                format!(
                    "The specified location-constraint '{}' is not valid for region '{}'",
                    provided, expected
                ),
                "".to_string(),
            ),
            S3Error::InvalidInput(msg) => (
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                msg,
                "".to_string(),
            ),
            S3Error::InvalidPart(msg) => {
                (StatusCode::BAD_REQUEST, "InvalidPart", msg, "".to_string())
            }
            S3Error::InvalidRange(total) => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                "InvalidRange",
                "The requested range cannot be satisfied.".to_string(),
                format!("bytes=*/{}", total),
            ),
            S3Error::PreconditionFailed(msg) => (
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                msg,
                "".to_string(),
            ),
            S3Error::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                msg,
                "".to_string(),
            ),
        };

        let host_id = format!("{}.host", request_id);
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>{}</RequestId><HostId>{}</HostId></Error>"#,
            code, message, resource, request_id, host_id
        );

        let resp: Response =
            (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response();
        attach_error_headers(resp)
    }
}

/// PUT /{bucket} - Create bucket
async fn create_bucket(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::PUT, &uri, &headers, &[])
        .await?;

    if !body.is_empty() {
        parse_create_bucket_configuration(&body, state.context.region())?;
    }

    let created = state.handler.storage.create_bucket(&bucket).await?;
    if !created {
        return Err(S3Error::BucketAlreadyExists(bucket));
    }
    let resp: Response = (
        StatusCode::OK,
        [(header::LOCATION, format!("/{}", bucket))],
        Body::empty(),
    )
        .into_response();
    Ok(attach_common_headers(resp, &state))
}

/// DELETE /{bucket} - Delete bucket
async fn delete_bucket(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::DELETE, &uri, &headers, &[])
        .await?;

    let deleted = state.handler.storage.delete_bucket(&bucket).await?;
    if !deleted {
        return Err(S3Error::BucketNotFound(bucket));
    }
    let resp = (StatusCode::NO_CONTENT, Body::empty()).into_response();
    Ok(attach_common_headers(resp, &state))
}

/// HEAD /{bucket} - Head bucket
async fn head_bucket(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::HEAD, &uri, &headers, &[])
        .await?;

    let exists = state.bucket_exists(&bucket).await?;
    if !exists {
        return Err(S3Error::BucketNotFound(bucket));
    }
    let resp = (
        StatusCode::OK,
        [
            (header::CONTENT_LENGTH, "0".to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
        ],
        Body::empty(),
    )
        .into_response();
    Ok(attach_common_headers(resp, &state))
}

/// GET / - List buckets
async fn list_buckets(
    State(state): State<S3State>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::GET, &uri, &headers, &[])
        .await?;

    let buckets = state.storage_buckets().await?;
    let resp = state.renderer.list_buckets(&buckets);
    Ok(attach_common_headers(resp, &state))
}

/// PUT /{bucket}/{key} - Put object
async fn put_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(mq): Query<MultipartQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::PUT, &uri, &headers, &body)
        .await?;

    if headers.contains_key("x-amz-copy-source") {
        return copy_object_internal(&state, &bucket, &key, &headers).await;
    }

    if let Some(upload_id) = mq.upload_id {
        let part_number = mq
            .part_number
            .ok_or_else(|| S3Error::InvalidInput("partNumber is required".to_string()))?;
        let etag = state
            .upload_part(&bucket, &key, &upload_id, part_number, &body)
            .await?;
        let resp: Response = (
            StatusCode::OK,
            [(header::ETAG, format!("\"{}\"", etag))],
            Body::empty(),
        )
            .into_response();
        return Ok(attach_common_headers(resp, &state));
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    let metadata = extract_user_metadata(&headers);
    let meta = state
        .handler
        .storage
        .put_object(&bucket, &key, body, content_type, metadata)
        .await?;

    let resp: Response = (
        StatusCode::OK,
        [(header::ETAG, format!("\"{}\"", meta.etag))],
        Body::empty(),
    )
        .into_response();
    Ok(attach_common_headers(resp, &state))
}

/// PATCH /{bucket}/{key} - Copy object
async fn copy_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::PATCH, &uri, &headers, &[])
        .await?;

    copy_object_internal(&state, &bucket, &key, &headers).await
}

async fn copy_object_internal(
    state: &S3State,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let copy_headers = copy_headers_from_map(headers);
    let raw_source = copy_headers
        .copy_source
        .ok_or_else(|| S3Error::InvalidInput("x-amz-copy-source header is required".to_string()))?;
    let (src_bucket, src_key) = parse_copy_source(&raw_source).ok_or_else(|| {
        S3Error::InvalidInput("Invalid x-amz-copy-source header format".to_string())
    })?;

    if !state.bucket_exists(bucket).await? {
        return Err(S3Error::BucketNotFound(bucket.to_string()));
    }

    let src_meta = state
        .handler
        .storage
        .head_object(&src_bucket, &src_key)
        .await?;
    let metadata_directive = copy_headers
        .metadata_directive
        .as_deref()
        .unwrap_or("COPY")
        .to_ascii_uppercase();
    if metadata_directive != "COPY" && metadata_directive != "REPLACE" {
        return Err(S3Error::InvalidInput(
            "Unsupported x-amz-metadata-directive value".to_string(),
        ));
    }
    let (final_content_type, final_metadata) = if metadata_directive == "REPLACE" {
        let md = extract_user_metadata(headers);
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(&src_meta.content_type)
            .to_string();
        (content_type, md)
    } else {
        (src_meta.content_type.clone(), src_meta.metadata.clone())
    };

    let dest_meta = match state.handler.storage.head_object(bucket, key).await {
        Ok(meta) => Some(meta),
        Err(StorageError::ObjectNotFound { .. }) => None,
        Err(e) => return Err(e.into()),
    };

    if let Some(expect) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        match dest_meta.as_ref() {
            Some(existing) if normalize_etag(expect) == normalize_etag(&existing.etag) => {}
            _ => {
                return Err(S3Error::PreconditionFailed(
                    "If-Match condition failed".to_string(),
                ))
            }
        }
    }

    if let Some(disallow) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        if disallow == "*" {
            if dest_meta.is_some() {
                return Err(S3Error::PreconditionFailed(
                    "If-None-Match condition failed".to_string(),
                ));
            }
        } else if let Some(existing) = dest_meta.as_ref() {
            if normalize_etag(disallow) == normalize_etag(&existing.etag) {
                return Err(S3Error::PreconditionFailed(
                    "If-None-Match condition failed".to_string(),
                ));
            }
        }
    }

    if let Some(expect) = copy_headers.copy_source_if_match.as_deref() {
        if normalize_etag(expect) != normalize_etag(&src_meta.etag) {
            return Err(S3Error::PreconditionFailed(
                "x-amz-copy-source-if-match condition failed".to_string(),
            ));
        }
    }
    if let Some(disallow) = copy_headers.copy_source_if_none_match.as_deref() {
        if normalize_etag(disallow) == normalize_etag(&src_meta.etag) {
            return Err(S3Error::PreconditionFailed(
                "x-amz-copy-source-if-none-match condition failed".to_string(),
            ));
        }
    }

    let dest_meta = state
        .handler
        .storage
        .copy_object(
            &src_bucket,
            &src_key,
            bucket,
            key,
            &final_content_type,
            final_metadata,
        )
        .await?;

    let resp: Response = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult xmlns="{XMLNS}"><LastModified>{}</LastModified><ETag>"{}"</ETag></CopyObjectResult>"#,
            format_rfc3339(dest_meta.last_modified_unix_secs),
            dest_meta.etag
        ),
    )
        .into_response();
    Ok(attach_common_headers(resp, state))
}

/// POST /{bucket}/{key}?uploads or ?uploadId= - Multipart initiate/complete
async fn post_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(mq): Query<MultipartQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::POST, &uri, &headers, &body)
        .await?;

    if mq.uploads.is_some() {
        let upload_id = state.init_multipart(&bucket, &key).await?;
        let resp: Response = (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/xml")],
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="{XMLNS}"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>"#,
                bucket, key, upload_id
            ),
        )
            .into_response();
        return Ok(attach_common_headers(resp, &state));
    }

    if let Some(upload_id) = mq.upload_id {
        let requested_parts = if body.is_empty() {
            None
        } else {
            Some(parse_complete_multipart_body(&body)?)
        };
        let etag = state
            .complete_upload(&bucket, &key, &upload_id, requested_parts)
            .await?;
        let resp: Response = (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/xml")],
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="{XMLNS}"><Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>"{}"</ETag></CompleteMultipartUploadResult>"#,
                bucket, key, bucket, key, etag
            ),
        )
            .into_response();
        return Ok(attach_common_headers(resp, &state));
    }

    Err(S3Error::InvalidInput(
        "Unsupported multipart POST operation".to_string(),
    ))
}

/// GET /{bucket}/{key} - Get object
async fn get_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::GET, &uri, &headers, &[])
        .await?;

    let (data, meta) = state.handler.storage.get_object(&bucket, &key).await?;

    // Conditional GET: If-None-Match
    if let Some(if_none) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        if normalize_etag(if_none) == normalize_etag(&meta.etag) {
            let mut resp: Response = (
                StatusCode::NOT_MODIFIED,
                [
                    (header::ETAG, format!("\"{}\"", meta.etag)),
                    (
                        header::LAST_MODIFIED,
                        format_http_date(meta.last_modified_unix_secs),
                    ),
                ],
                Body::empty(),
            )
                .into_response();
            apply_metadata_headers(resp.headers_mut(), &meta.metadata);
            return Ok(attach_common_headers(resp, &state));
        }
    }

    if let Some(range_header) = headers.get(header::RANGE) {
        let range_str = range_header
            .to_str()
            .map_err(|_| S3Error::InvalidRange(meta.size))?;
        let (start, end) = parse_range(range_str, meta.size)?;
        let end_inclusive = end.min(meta.size.saturating_sub(1));
        if start > end_inclusive {
            return Err(S3Error::InvalidRange(meta.size));
        }
        let slice = data.slice(start as usize..=end_inclusive as usize);
        let mut resp: Response = (
            StatusCode::PARTIAL_CONTENT,
            [
                (header::CONTENT_TYPE, meta.content_type.clone()),
                (header::ETAG, format!("\"{}\"", meta.etag)),
                (header::CONTENT_LENGTH, slice.len().to_string()),
                (
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end_inclusive, meta.size),
                ),
                (header::ACCEPT_RANGES, "bytes".to_string()),
                (
                    header::LAST_MODIFIED,
                    format_http_date(meta.last_modified_unix_secs),
                ),
            ],
            Body::from(slice),
        )
            .into_response();
        apply_metadata_headers(resp.headers_mut(), &meta.metadata);
        return Ok(attach_common_headers(resp, &state));
    }

    let mut resp: Response = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, meta.content_type.clone()),
            (header::ETAG, format!("\"{}\"", meta.etag)),
            (header::CONTENT_LENGTH, meta.size.to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
            (
                header::LAST_MODIFIED,
                format_http_date(meta.last_modified_unix_secs),
            ),
        ],
        Body::from(data),
    )
        .into_response();
    apply_metadata_headers(resp.headers_mut(), &meta.metadata);
    Ok(attach_common_headers(resp, &state))
}

/// POST /{bucket}?delete - Delete multiple objects
async fn delete_objects(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    Query(query): Query<DeleteQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::POST, &uri, &headers, &body)
        .await?;

    if query.delete.is_none() {
        return Err(S3Error::InvalidInput(
            "The delete query parameter is required".to_string(),
        ));
    }

    if !state.bucket_exists(&bucket).await? {
        return Err(S3Error::BucketNotFound(bucket.clone()));
    }

    let (keys, quiet) = parse_multi_delete_body(&body)?;
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for key in keys {
        match state.handler.storage.delete_object(&bucket, &key).await {
            Ok(_) => {
                if !quiet {
                    deleted.push(key);
                }
            }
            Err(StorageError::ObjectNotFound { .. }) => {
                if !quiet {
                    deleted.push(key);
                }
            }
            Err(e) => {
                errors.push((key, e));
            }
        }
    }

    let mut body = String::new();
    body.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    body.push_str(&format!(r#"<DeleteResult xmlns="{}">"#, XMLNS));
    for key in deleted {
        body.push_str("<Deleted>");
        body.push_str(&format!("<Key>{}</Key>", key));
        body.push_str("</Deleted>");
    }
    for (key, err) in errors {
        body.push_str("<Error>");
        body.push_str(&format!("<Key>{}</Key>", key));
        body.push_str("<Code>InternalError</Code>");
        body.push_str(&format!("<Message>{}</Message>", err));
        body.push_str("</Error>");
    }
    body.push_str("</DeleteResult>");

    let resp: Response = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response();
    Ok(attach_common_headers(resp, &state))
}

/// HEAD /{bucket}/{key} - Head object
async fn head_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::HEAD, &uri, &headers, &[])
        .await?;

    let (_data, meta) = state.handler.storage.get_object(&bucket, &key).await?;

    if let Some(range_header) = headers.get(header::RANGE) {
        let range_str = range_header
            .to_str()
            .map_err(|_| S3Error::InvalidRange(meta.size))?;
        let (start, end) = parse_range(range_str, meta.size)?;
        let end_inclusive = end.min(meta.size.saturating_sub(1));
        if start > end_inclusive {
            return Err(S3Error::InvalidRange(meta.size));
        }
        let mut resp: Response = (
            StatusCode::PARTIAL_CONTENT,
            [
                (header::CONTENT_TYPE, meta.content_type.clone()),
                (header::ETAG, format!("\"{}\"", meta.etag)),
                (
                    header::CONTENT_LENGTH,
                    (end_inclusive - start + 1).to_string(),
                ),
                (
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end_inclusive, meta.size),
                ),
                (header::ACCEPT_RANGES, "bytes".to_string()),
                (
                    header::LAST_MODIFIED,
                    format_http_date(meta.last_modified_unix_secs),
                ),
            ],
            Body::empty(),
        )
            .into_response();
        apply_metadata_headers(resp.headers_mut(), &meta.metadata);
        return Ok(attach_common_headers(resp, &state));
    }

    let mut resp: Response = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, meta.content_type.clone()),
            (header::ETAG, format!("\"{}\"", meta.etag)),
            (header::CONTENT_LENGTH, meta.size.to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
            (
                header::LAST_MODIFIED,
                format_http_date(meta.last_modified_unix_secs),
            ),
        ],
        Body::empty(),
    )
        .into_response();
    apply_metadata_headers(resp.headers_mut(), &meta.metadata);
    Ok(attach_common_headers(resp, &state))
}

/// DELETE /{bucket}/{key} - Delete object
async fn delete_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(mq): Query<MultipartQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::DELETE, &uri, &headers, &[])
        .await?;

    if let Some(upload_id) = mq.upload_id {
        state.abort_upload(&bucket, &key, &upload_id).await?;
        let resp = (StatusCode::NO_CONTENT, Body::empty()).into_response();
        return Ok(attach_common_headers(resp, &state));
    }

    let deleted = state.handler.storage.delete_object(&bucket, &key).await?;
    if !deleted {
        return Err(S3Error::ObjectNotFound { bucket, key });
    }
    let resp = (StatusCode::NO_CONTENT, Body::empty()).into_response();
    Ok(attach_common_headers(resp, &state))
}

#[derive(Deserialize)]
struct ListObjectsParams {
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    #[serde(rename = "max-keys")]
    max_keys: Option<u32>,
}

#[derive(Deserialize, Default)]
struct MultipartQuery {
    #[serde(default)]
    uploads: Option<String>,
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    part_number: Option<u32>,
}

#[derive(Deserialize, Default)]
struct CopyHeaders {
    #[serde(rename = "x-amz-copy-source")]
    copy_source: Option<String>,
    #[serde(rename = "x-amz-copy-source-if-match")]
    copy_source_if_match: Option<String>,
    #[serde(rename = "x-amz-copy-source-if-none-match")]
    copy_source_if_none_match: Option<String>,
    #[serde(rename = "x-amz-metadata-directive")]
    metadata_directive: Option<String>,
}

fn copy_headers_from_map(headers: &HeaderMap) -> CopyHeaders {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    CopyHeaders {
        copy_source: get("x-amz-copy-source"),
        copy_source_if_match: get("x-amz-copy-source-if-match"),
        copy_source_if_none_match: get("x-amz-copy-source-if-none-match"),
        metadata_directive: get("x-amz-metadata-directive"),
    }
}

/// GET /{bucket}/ - List objects
async fn list_objects(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsParams>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        state: state.clone(),
    };
    s3_handler
        .authenticate(&Method::GET, &uri, &headers, &[])
        .await?;

    let limit = params.max_keys.unwrap_or(1000) as usize;
    let objects = state
        .handler
        .storage
        .list_objects(&bucket, &params.prefix, limit)
        .await?;

    let entries: Vec<ObjectEntry> = objects
        .into_iter()
        .map(|(key, meta)| ObjectEntry {
            key,
            last_modified_unix_secs: meta.last_modified_unix_secs,
            etag: meta.etag,
            size: meta.size,
        })
        .collect();

    let resp = state
        .renderer
        .list_objects(&bucket, &params.prefix, limit, &entries);
    Ok(attach_common_headers(resp, &state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthContext, AuthError, Authenticator};
    use crate::storage::in_memory::InMemoryStorage;
    use async_trait::async_trait;
    use axum::http::{HeaderMap, Method, Request, Uri};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct MockAuthenticator {
        should_fail: bool,
    }

    #[async_trait]
    impl Authenticator for MockAuthenticator {
        async fn authenticate(&self, req: &tonic::Request<()>) -> Result<AuthContext, AuthError> {
            if self.should_fail {
                return Err(AuthError::InvalidCredentials);
            }
            if req.metadata().get("x-access-key").is_none() {
                return Err(AuthError::MissingCredentials);
            }
            Ok(AuthContext {
                access_key: "test-user".to_string(),
            })
        }

        async fn secret_for(&self, access_key: &str) -> Result<String, AuthError> {
            if self.should_fail {
                return Err(AuthError::InvalidCredentials);
            }
            if access_key == "test-user" {
                Ok("test-pass".to_string())
            } else {
                Err(AuthError::InvalidCredentials)
            }
        }
    }

    fn create_test_handler(auth_should_fail: bool) -> S3HttpHandler {
        let auth: Arc<dyn Authenticator> = Arc::new(MockAuthenticator {
            should_fail: auth_should_fail,
        });
        let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(InMemoryStorage::new());
        let base_handler = BaseHandler::new(auth, storage);
        S3HttpHandler::new(base_handler)
    }

    fn build_sigv4_headers(method: Method, uri: &Uri, body: &[u8]) -> HeaderMap {
        let payload_hash = sha256_hex(body);
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:9000".parse().unwrap());
        headers.insert("x-amz-date", "20240716T000000Z".parse().unwrap());
        headers.insert("x-amz-content-sha256", payload_hash.parse().unwrap());

        let signed_headers = vec![
            "host".to_string(),
            "x-amz-content-sha256".to_string(),
            "x-amz-date".to_string(),
        ];
        let canonical_request =
            build_canonical_request(&method, uri, &headers, &signed_headers, &payload_hash)
                .unwrap();
        let canonical_hash = sha256_hex(canonical_request.as_bytes());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
            "20240716T000000Z", "20240716", "us-east-1", "s3", canonical_hash
        );
        let auth_meta = SigV4Authorization {
            access_key: "test-user".to_string(),
            date: "20240716".to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            signed_headers,
            signature: String::new(),
        };
        let signature = hex::encode(sign_string("test-pass", &auth_meta, &string_to_sign).unwrap());
        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential=test-user/20240716/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
            signature
        );
        headers.insert("authorization", auth_header.parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/test-bucket"
        );
        assert!(response.headers().get("x-amz-request-id").is_some());
    }

    #[tokio::test]
    async fn test_create_bucket_unauthorized() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>AccessDenied</Code>"));
        assert!(headers.get(BUCKET_REGION_HEADER).is_some());
    }

    #[tokio::test]
    async fn test_create_bucket_conflict() {
        let handler = create_test_handler(false);
        let app = handler.router();

        // First creation
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/dupe-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Second creation should conflict
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/dupe-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>BucketAlreadyExists</Code>"));
    }

    #[tokio::test]
    async fn test_create_bucket_with_config() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let cfg_body = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>"#;
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cfg-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "application/xml")
                    .body(Body::from(cfg_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/cfg-bucket"
        );
    }

    #[tokio::test]
    async fn test_create_bucket_with_invalid_config() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cfg-invalid")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "application/xml")
                    .body(Body::from("<Bad></Bad>"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>InvalidArgument</Code>"));
    }

    #[tokio::test]
    async fn test_create_bucket_with_wrong_region() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let cfg_body = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>"#;
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/wrong-region")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "application/xml")
                    .body(Body::from(cfg_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>InvalidLocationConstraint</Code>"));
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket first
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Put object
        let app2 = handler.clone().router();
        let response = app2
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket/test-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("test data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Get object
        let app3 = handler.router();
        let response = app3
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/test-bucket/test-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain"
        );
    }

    #[tokio::test]
    async fn test_copy_object() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and seed object
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-bucket/source.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("copy me"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Copy
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-bucket/dest.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-bucket/source.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<CopyObjectResult"));

        // Fetch destination
        let get_resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/copy-bucket/dest.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let data = get_resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(data, Bytes::from("copy me"));
    }

    #[tokio::test]
    async fn test_copy_object_put_path() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and seed object
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-bucket-put")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-bucket-put/source.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("copy put"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Copy via PUT with header
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-bucket-put/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-bucket-put/source.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify content
        let get_resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/copy-bucket-put/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let data = get_resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(data, Bytes::from("copy put"));
    }

    #[tokio::test]
    async fn test_copy_object_metadata_directive_replace_changes_content_type() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-meta")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-meta/src.json")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-meta/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-meta/src.json")
                    .header("x-amz-metadata-directive", "REPLACE")
                    .header("content-type", "text/plain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let get_resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/copy-meta/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            get_resp
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "text/plain"
        );
    }

    #[tokio::test]
    async fn test_copy_object_invalid_metadata_directive() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-meta-invalid")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-meta-invalid/src.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-meta-invalid/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-meta-invalid/src.txt")
                    .header("x-amz-metadata-directive", "UNKNOWN")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>InvalidArgument</Code>"));
    }

    #[tokio::test]
    async fn test_put_get_with_user_metadata() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and put object with metadata
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/meta-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/meta-bucket/meta.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .header("x-amz-meta-color", "blue")
                    .header("x-amz-meta-Flag", "yes")
                    .body(Body::from("meta data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // GET should include metadata headers
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/meta-bucket/meta.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.headers().get("x-amz-meta-color").unwrap(), "blue");
        assert_eq!(get_resp.headers().get("x-amz-meta-flag").unwrap(), "yes");

        // HEAD should also include metadata
        let head_resp = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/meta-bucket/meta.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_resp.headers().get("x-amz-meta-color").unwrap(), "blue");
        assert_eq!(head_resp.headers().get("x-amz-meta-flag").unwrap(), "yes");
    }

    #[tokio::test]
    async fn test_copy_object_destination_bucket_not_found() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create source bucket and object only
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-no-dest-src")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-no-dest-src/file.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("src"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/missing-dest/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-no-dest-src/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>NoSuchBucket</Code>"));
    }

    #[tokio::test]
    async fn test_copy_object_missing_header_and_bad_source() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create destination bucket only
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-errors")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Missing header should be bad request
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-errors/dst")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Malformed header should be bad request
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-errors/dst2")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "justbucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Missing source bucket should be not found
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-errors/dst3")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/no-such/src.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_copy_object_metadata_copy_and_replace() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and source object with metadata
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-meta-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-meta-bucket/src.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .header("x-amz-meta-src", "keep")
                    .body(Body::from("src meta"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // COPY should keep metadata
        let copy_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-meta-bucket/dst-copy.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-meta-bucket/src.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(copy_resp.status(), StatusCode::OK);
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/copy-meta-bucket/dst-copy.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.headers().get("x-amz-meta-src").unwrap(), "keep");

        // REPLACE should use new metadata only
        let replace_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-meta-bucket/dst-replace.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-meta-bucket/src.txt")
                    .header("x-amz-metadata-directive", "REPLACE")
                    .header("x-amz-meta-new", "yes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replace_resp.status(), StatusCode::OK);
        let get_replace = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/copy-meta-bucket/dst-replace.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(get_replace.headers().get("x-amz-meta-src").is_none());
        assert_eq!(get_replace.headers().get("x-amz-meta-new").unwrap(), "yes");
    }

    #[tokio::test]
    async fn test_copy_object_if_match_fails() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and seed object
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let put_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond/src.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("cond data"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let etag = put_resp
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-cond/dst.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "copy-cond/src.txt")
                    .header("x-amz-copy-source-if-match", etag + "extra")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>PreconditionFailed</Code>"));
    }

    #[tokio::test]
    async fn test_copy_object_if_none_match_star_fails_when_dest_exists() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket, source, and existing destination
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-none-match")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-none-match/source.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("source"))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-none-match/dest.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("existing"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-none-match/dest.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-none-match/source.txt")
                    .header(header::IF_NONE_MATCH, "*")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>PreconditionFailed</Code>"));
    }

    #[tokio::test]
    async fn test_copy_object_if_match_destination_mismatch() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket, source, and dest with known etag
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-dest-match")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-dest-match/source.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("src"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let dest_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-dest-match/dest.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("dest"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let dest_etag = dest_resp
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/copy-dest-match/dest.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("x-amz-copy-source", "/copy-dest-match/source.txt")
                    .header(header::IF_MATCH, dest_etag + "bad")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>PreconditionFailed</Code>"));
    }

    #[tokio::test]
    async fn test_head_object() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and object
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket/head-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("head data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/head-bucket/head-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain"
        );
        assert!(response.headers().get(header::ETAG).is_some());
    }

    #[tokio::test]
    async fn test_list_buckets_xml() {
        let handler = create_test_handler(false);
        let app = handler.router();

        // Create two buckets
        for bucket in ["a-bucket", "b-bucket"] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/{}", bucket))
                        .header("x-access-key", "test-user")
                        .header("x-secret-key", "test-pass")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<ListAllMyBucketsResult"));
        assert!(body_str.contains("<Name>a-bucket</Name>"));
        assert!(body_str.contains("<Name>b-bucket</Name>"));
    }

    #[tokio::test]
    async fn test_list_objects_xml() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/xml-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Put object
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/xml-bucket/obj.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/xml-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<ListBucketResult"));
        assert!(body_str.contains("<Key>obj.txt</Key>"));
        assert!(body_str.contains("<KeyCount>1</KeyCount>"));
        assert!(body_str.contains("<ETag>\""));
    }

    #[tokio::test]
    async fn test_head_bucket() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/head-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
        assert_eq!(
            response
                .headers()
                .get(BUCKET_REGION_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "us-east-1"
        );
    }

    #[tokio::test]
    async fn test_head_bucket_not_found() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/missing-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_found() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/nope")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>NoSuchBucket</Code>"));
    }

    #[tokio::test]
    async fn test_delete_object_not_found() {
        let handler = create_test_handler(false);
        let app = handler.router();

        // Create bucket
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/del-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/del-bucket/missing")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>NoSuchKey</Code>"));
    }

    #[tokio::test]
    async fn test_sigv4_put_and_get_object() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket with SigV4
        let bucket_uri: Uri = "/sigv4-bucket".parse().unwrap();
        let bucket_headers = build_sigv4_headers(Method::PUT, &bucket_uri, b"");
        let mut builder = Request::builder().method("PUT").uri(bucket_uri.clone());
        for (name, value) in bucket_headers.iter() {
            builder = builder.header(name, value);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Put object with SigV4
        let object_body = b"hello sigv4";
        let object_uri: Uri = "/sigv4-bucket/signed-key".parse().unwrap();
        let object_headers = build_sigv4_headers(Method::PUT, &object_uri, object_body);
        let mut put_builder = Request::builder().method("PUT").uri(object_uri.clone());
        for (name, value) in object_headers.iter() {
            put_builder = put_builder.header(name, value);
        }
        let response = app
            .clone()
            .oneshot(
                put_builder
                    .body(Body::from(object_body.as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get object with SigV4
        let get_headers = build_sigv4_headers(Method::GET, &object_uri, b"");
        let mut get_builder = Request::builder().method("GET").uri(object_uri.clone());
        for (name, value) in get_headers.iter() {
            get_builder = get_builder.header(name, value);
        }
        let response = app
            .oneshot(get_builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], object_body);
    }

    #[tokio::test]
    async fn test_sigv4_rejects_bad_signature() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let bucket_uri: Uri = "/sigv4-fail".parse().unwrap();
        let mut headers = build_sigv4_headers(Method::PUT, &bucket_uri, b"");
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test-user/20240716/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=deadbeef".parse().unwrap(),
        );

        let mut builder = Request::builder().method("PUT").uri(bucket_uri);
        for (name, value) in headers.iter() {
            builder = builder.header(name, value);
        }

        let response = app
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    fn extract_between(text: &str, start: &str, end: &str) -> Option<String> {
        let s = text.find(start)? + start.len();
        let e = text[s..].find(end)? + s;
        Some(text[s..e].to_string())
    }

    #[tokio::test]
    async fn test_multipart_initiate_upload_complete() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mp-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate
        let init_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mp-bucket/mp-key?uploads")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(init_resp.status(), StatusCode::OK);
        let init_body = init_resp.into_body().collect().await.unwrap().to_bytes();
        let init_str = std::str::from_utf8(&init_body).unwrap();
        let upload_id =
            extract_between(init_str, "<UploadId>", "</UploadId>").expect("upload id missing");

        // Upload part
        let upload_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mp-bucket/mp-key?uploadId={}&partNumber=1",
                        upload_id
                    ))
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::from("part-one"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload_resp.status(), StatusCode::OK);
        assert!(upload_resp.headers().get(header::ETAG).is_some());

        // Complete
        let complete_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mp-bucket/mp-key?uploadId={}", upload_id))
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete_resp.status(), StatusCode::OK);
        let body = complete_resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<CompleteMultipartUploadResult"));
        assert!(body_str.contains("<ETag>\""));

        // Fetch combined object
        let get_resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mp-bucket/mp-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let data = get_resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&data[..], b"part-one");
    }

    #[tokio::test]
    async fn test_multipart_abort() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/abort-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate
        let init_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/abort-bucket/obj?uploads")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let init_body = init_resp.into_body().collect().await.unwrap().to_bytes();
        let upload_id = extract_between(
            std::str::from_utf8(&init_body).unwrap(),
            "<UploadId>",
            "</UploadId>",
        )
        .unwrap();

        // Abort
        let abort_resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/abort-bucket/obj?uploadId={}", upload_id))
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(abort_resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_multipart_missing_upload() {
        let handler = create_test_handler(false);
        let app = handler.router();

        // Create bucket
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/missing-mp")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/missing-mp/key?uploadId=none")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>NoSuchUpload</Code>"));
    }

    #[tokio::test]
    async fn test_multipart_invalid_part_on_complete() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and initiate upload
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/invalid-part-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let init_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/invalid-part-bucket/key?uploads")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let init_body = init_resp.into_body().collect().await.unwrap().to_bytes();
        let upload_id = extract_between(
            std::str::from_utf8(&init_body).unwrap(),
            "<UploadId>",
            "</UploadId>",
        )
        .unwrap();

        // Complete referencing a part that was never uploaded
        let complete_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/invalid-part-bucket/key?uploadId={}",
                        upload_id
                    ))
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::from(
                        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part></CompleteMultipartUpload>",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(complete_resp.status(), StatusCode::BAD_REQUEST);
        let body = complete_resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Code>InvalidPart</Code>"));
    }

    #[tokio::test]
    async fn test_multi_delete_objects() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket and seed objects
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mdel")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        for key in ["a.txt", "b.txt"] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/mdel/{}", key))
                        .header("x-access-key", "test-user")
                        .header("x-secret-key", "test-pass")
                        .body(Body::from("payload"))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let delete_body = r#"<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mdel?delete")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::from(delete_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("<Deleted><Key>a.txt</Key></Deleted>"));
        assert!(body_str.contains("<Deleted><Key>b.txt</Key></Deleted>"));

        // Objects should be gone
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mdel/a.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_multi_delete_quiet() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mdel-quiet")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mdel-quiet/file.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let delete_body =
            r#"<Delete><Quiet>true</Quiet><Object><Key>file.txt</Key></Object></Delete>"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mdel-quiet?delete")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::from(delete_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(!body_str.contains("<Deleted>"));

        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mdel-quiet/file.txt")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
    }
}
