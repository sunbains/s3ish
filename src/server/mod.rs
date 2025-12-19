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

mod quic_protocol;
mod tcp_sendfile;
mod quinn_noprotection;
mod quinn_server;

use crate::handler::BaseHandler;
use crate::pb::object_store_server::ObjectStoreServer;
use crate::s3_http::{ResponseContext, S3HttpHandler};
use crate::service::ObjectStoreService;
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    async fn serve(&self, addr: SocketAddr)
        -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// gRPC connection manager (tonic) that hosts the ObjectStore service.
#[derive(Clone)]
pub struct GrpcConnectionManager {
    svc: ObjectStoreService,
}

impl GrpcConnectionManager {
    pub fn new(handler: BaseHandler) -> Self {
        Self {
            svc: ObjectStoreService::new(handler),
        }
    }
}

#[async_trait]
impl ConnectionManager for GrpcConnectionManager {
    async fn serve(
        &self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting gRPC server on {}", addr);
        Server::builder()
            .add_service(ObjectStoreServer::new(self.svc.clone()))
            .serve(addr)
            .await?;
        Ok(())
    }
}

/// S3 HTTP connection manager using Axum.
#[derive(Clone)]
pub struct S3HttpConnectionManager {
    handler: S3HttpHandler,
}

impl S3HttpConnectionManager {
    pub fn new(handler: BaseHandler, ctx: ResponseContext) -> Self {
        Self {
            handler: S3HttpHandler::new_with_context(handler, ctx),
        }
    }
}

#[async_trait]
impl ConnectionManager for S3HttpConnectionManager {
    async fn serve(
        &self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting S3 HTTP server on {}", addr);
        let app = self.handler.clone().router();

        // Bind TCP listener with optimized settings
        use socket2::{Socket, Domain, Type, Protocol};

        // Create socket
        let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

        // Enable TCP_NODELAY to disable Nagle's algorithm (reduces latency)
        socket.set_nodelay(true)?;

        // Set large send/receive buffers (16MB each) for high throughput
        socket.set_recv_buffer_size(16 * 1024 * 1024)?;
        socket.set_send_buffer_size(16 * 1024 * 1024)?;

        // Enable reuse address
        socket.set_reuse_address(true)?;

        // Bind and listen
        socket.bind(&addr.into())?;
        socket.listen(1024)?;

        // Convert to tokio listener
        let listener = tokio::net::TcpListener::from_std(socket.into())?;

        tracing::info!("TCP optimizations enabled: TCP_NODELAY=true, buffers=16MB");

        axum::serve(listener, app).await?;
        Ok(())
    }
}

/// QUIC connection manager using tokio-quiche
pub struct QuicConnectionManager {
    handler: Arc<BaseHandler>,
    cert_path: String,
    key_path: String,
    benchmark_mode: bool,
}

impl QuicConnectionManager {
    pub fn new(handler: BaseHandler, _ctx: ResponseContext, benchmark_mode: bool) -> Result<Self, Box<dyn std::error::Error>> {
        // Generate self-signed certificate for testing
        let (cert_path, key_path) = generate_self_signed_cert()?;

        if benchmark_mode {
            tracing::warn!("BENCHMARK MODE: Storage writes disabled for maximum throughput testing");
        }

        Ok(Self {
            handler: Arc::new(handler),
            cert_path,
            key_path,
            benchmark_mode,
        })
    }
}

/// TCP connection manager using sendfile() for zero-copy transfers
pub struct TcpConnectionManager {
    handler: Arc<BaseHandler>,
}

impl TcpConnectionManager {
    pub fn new(handler: BaseHandler) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }
}

#[async_trait]
impl ConnectionManager for TcpConnectionManager {
    async fn serve(
        &self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let server = tcp_sendfile::TcpSendfileServer::new((*self.handler).clone());
        server.serve(addr).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Quinn QUIC connection manager with optional no-encryption mode
pub struct QuinnConnectionManager {
    handler: Arc<BaseHandler>,
    benchmark_mode: bool,
    no_encryption: bool,
}

impl QuinnConnectionManager {
    pub fn new(handler: BaseHandler, benchmark_mode: bool, no_encryption: bool) -> Self {
        Self {
            handler: Arc::new(handler),
            benchmark_mode,
            no_encryption,
        }
    }
}

#[async_trait]
impl ConnectionManager for QuinnConnectionManager {
    async fn serve(
        &self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let server = quinn_server::QuinnServer::new(
            (*self.handler).clone(),
            self.benchmark_mode,
            self.no_encryption,
        );
        server.serve(addr).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Generate a self-signed certificate for testing
fn generate_self_signed_cert() -> Result<(String, String), Box<dyn std::error::Error>> {
    let cert_pem = "/tmp/s3ish_quic_cert.pem".to_string();
    let key_pem = "/tmp/s3ish_quic_key.pem".to_string();

    // Check if certs already exist
    if std::path::Path::new(&cert_pem).exists() && std::path::Path::new(&key_pem).exists() {
        tracing::info!("Using existing QUIC certificate");
        return Ok((cert_pem, key_pem));
    }

    // Generate using openssl
    let output = std::process::Command::new("openssl")
        .args(&[
            "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", &key_pem,
            "-out", &cert_pem,
            "-days", "365",
            "-subj", "/CN=localhost"
        ])
        .output();

    match output {
        Ok(out) if out.status.success() => {
            tracing::info!("Generated self-signed certificate for QUIC at {}", cert_pem);
            Ok((cert_pem, key_pem))
        }
        _ => {
            Err("Failed to generate certificate. Run: sudo apt install openssl".into())
        }
    }
}

#[async_trait]
impl ConnectionManager for QuicConnectionManager {
    async fn serve(
        &self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio_quiche::settings::{CertificateKind, ConnectionParams, Hooks, QuicSettings, TlsCertificatePaths};
        use tokio_quiche::quic::SimpleConnectionIdGenerator;
        use tokio_quiche::metrics::DefaultMetrics;
        use futures_util::StreamExt;

        tracing::info!("Starting QUIC server on {}", addr);

        // Bind UDP socket
        let udp = tokio::net::UdpSocket::bind(addr).await?;

        // Set large UDP socket buffers for high throughput (16MB each)
        // Default is only 212KB which severely limits throughput
        use socket2::SockRef;
        let sock_ref = SockRef::from(&udp);
        sock_ref.set_recv_buffer_size(16 * 1024 * 1024)?; // 16MB receive buffer
        sock_ref.set_send_buffer_size(16 * 1024 * 1024)?; // 16MB send buffer

        let actual_recv = sock_ref.recv_buffer_size()?;
        let actual_send = sock_ref.send_buffer_size()?;
        tracing::info!("UDP socket buffers: recv={}MB, send={}MB",
                      actual_recv / 1024 / 1024, actual_send / 1024 / 1024);

        // Configure QUIC settings optimized for file transfer
        let mut settings = QuicSettings::default();
        settings.alpn = vec![b"file-transfer".to_vec()]; // Match client ALPN
        settings.initial_max_streams_bidi = 100;
        settings.initial_max_streams_uni = 100;
        settings.initial_max_stream_data_bidi_local = 8_000_000; // 8MB per stream (increased from 4MB)
        settings.initial_max_stream_data_bidi_remote = 8_000_000;
        settings.initial_max_stream_data_uni = 8_000_000;
        settings.initial_max_data = 100_000_000; // 100MB total (increased from 40MB)
        settings.max_idle_timeout = Some(Duration::from_secs(30));
        settings.cc_algorithm = "bbr".to_string(); // Use string, not enum

        // TLS certificate paths
        let tls_cert = TlsCertificatePaths {
            cert: &self.cert_path,
            private_key: &self.key_path,
            kind: CertificateKind::X509,
        };

        // Create connection parameters
        let params = ConnectionParams::new_server(settings, tls_cert, Hooks::default());

        tracing::info!(
            "QUIC config: ALPN=file-transfer, BBR CC, 100MB window (8MB/stream), cert={}",
            self.cert_path
        );

        // Start listening for connections
        let mut listeners = tokio_quiche::listen(
            [udp],
            params,
            SimpleConnectionIdGenerator,
            DefaultMetrics,
        )?;

        let accept_stream = &mut listeners[0];

        // Accept connections and start application
        while let Some(conn_result) = accept_stream.next().await {
            match conn_result {
                Ok(conn) => {
                    let app = S3QuicApplication::new(self.handler.clone(), self.benchmark_mode);
                    conn.start(app);
                    tracing::info!("QUIC connection started");
                }
                Err(e) => {
                    tracing::warn!("Failed to accept QUIC connection: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// S3 file transfer application over QUIC
struct S3QuicApplication {
    storage: Arc<BaseHandler>,
    inbound_streams: HashMap<u64, bytes::BytesMut>,
    rx_buffer: Vec<u8>,
    tx_buffer: Vec<u8>,
    // Track partial files being uploaded (using BytesMut for zero-copy)
    uploads: HashMap<String, bytes::BytesMut>,
    benchmark_mode: bool,
}

impl S3QuicApplication {
    fn new(storage: Arc<BaseHandler>, benchmark_mode: bool) -> Self {
        Self {
            storage,
            inbound_streams: HashMap::new(),
            rx_buffer: vec![0; 64 * 1024], // 64KB read buffer (original size)
            tx_buffer: vec![0; 1500], // MTU-sized write buffer
            uploads: HashMap::new(),
            benchmark_mode,
        }
    }

    fn drive_reads(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        let loop_start = std::time::Instant::now();
        let mut recv_time = std::time::Duration::ZERO;
        let mut copy_time = std::time::Duration::ZERO;
        let mut process_time = std::time::Duration::ZERO;
        let mut bytes_read = 0usize;
        let mut chunks_processed = 0usize;
        let mut iterations = 0usize;

        // AGGRESSIVE BATCHING: Drain ALL available data before yielding
        // Keep looping until no more data available across all streams
        loop {
            let mut made_progress = false;
            iterations += 1;

            // Process all readable streams in this iteration
            for stream_id in qconn.readable() {
                loop {
                    let recv_start = std::time::Instant::now();
                    match qconn.stream_recv(stream_id, &mut self.rx_buffer) {
                        Ok((len, fin)) => {
                            recv_time += recv_start.elapsed();
                            bytes_read += len;
                            made_progress = true;

                            // Accumulate data for this stream
                            let copy_start = std::time::Instant::now();
                            let buf = self.inbound_streams.entry(stream_id).or_default();
                            buf.extend_from_slice(&self.rx_buffer[..len]);
                            copy_time += copy_start.elapsed();

                            // If stream finished, process the complete request
                            if fin {
                                if let Some(data) = self.inbound_streams.remove(&stream_id) {
                                    let process_start = std::time::Instant::now();
                                    self.handle_stream(qconn, stream_id, data)?;
                                    process_time += process_start.elapsed();
                                    chunks_processed += 1;
                                }
                                break;
                            }
                        }
                        Err(tokio_quiche::quiche::Error::Done) => break,
                        Err(e) => return Err(Box::new(e)),
                    }
                }
            }

            // Exit if no more data available across any stream
            if !made_progress {
                break;
            }
        }

        let total_time = loop_start.elapsed();
        if bytes_read > 0 {
            tracing::debug!(
                "drive_reads: {} bytes in {:?} over {} iterations (recv:{:?}, copy:{:?}, process:{:?}, chunks:{})",
                bytes_read, total_time, iterations, recv_time, copy_time, process_time, chunks_processed
            );
        }

        Ok(())
    }

    fn handle_stream(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
        stream_id: u64,
        data: bytes::BytesMut,
    ) -> tokio_quiche::QuicResult<()> {
        // Parse protocol header
        let (header, consumed) = match quic_protocol::ChunkHeader::decode(&data) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!("Failed to decode header on stream {}: {}", stream_id, e);
                return Ok(());
            }
        };

        let body = &data[consumed..];

        tracing::debug!(
            "QUIC stream {}: {:?} file={} offset={} len={}",
            stream_id,
            header.frame,
            header.file_name,
            header.offset,
            body.len()
        );

        match header.frame {
            quic_protocol::FrameType::UploadChunk => {
                self.handle_upload_chunk(qconn, stream_id, header, body)?;
            }
            quic_protocol::FrameType::DownloadRequest => {
                self.handle_download_request(qconn, stream_id, header)?;
            }
            _ => {
                tracing::warn!("Unexpected frame type: {:?}", header.frame);
            }
        }

        Ok(())
    }

    fn handle_upload_chunk(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
        stream_id: u64,
        header: quic_protocol::ChunkHeader,
        body: &[u8],
    ) -> tokio_quiche::QuicResult<()> {
        // Accumulate file data with pre-allocation on first chunk (using BytesMut for zero-copy)
        let file_data = self.uploads.entry(header.file_name.clone()).or_insert_with(|| {
            // Pre-allocate full file size to avoid repeated allocations
            let mut buf = bytes::BytesMut::with_capacity(header.total_size as usize);
            buf.resize(header.total_size as usize, 0);
            buf
        });

        // Ensure we have enough capacity (handles out-of-order chunks)
        if file_data.len() < (header.offset + header.chunk_len) as usize {
            file_data.resize((header.offset + header.chunk_len) as usize, 0);
        }

        // Write chunk at correct offset
        let start = header.offset as usize;
        let end = start + body.len();
        file_data[start..end].copy_from_slice(body);

        tracing::debug!(
            "Upload: {} chunk at offset {} ({}/{})",
            header.file_name,
            header.offset,
            header.offset + header.chunk_len,
            header.total_size
        );

        // If this is the last chunk, write to storage
        if header.offset + header.chunk_len >= header.total_size {
            if let Some(complete_data) = self.uploads.remove(&header.file_name) {
                tracing::info!(
                    "Upload complete: {} ({} bytes)",
                    header.file_name,
                    complete_data.len()
                );

                if !self.benchmark_mode {
                    // Write to storage using blocking task
                    let storage = self.storage.clone();
                    let file_name = header.file_name.clone();
                    // Zero-copy conversion: BytesMut::freeze() -> Bytes (no memcpy)
                    let data_bytes = complete_data.freeze();

                    // Spawn blocking task to write to storage
                    tokio::spawn(async move {
                        match storage
                            .storage
                            .put_object(
                                "quic-uploads",
                                &file_name,
                                data_bytes,
                                "application/octet-stream",
                                std::collections::HashMap::new(),
                                None,
                                None,
                                None,
                            )
                            .await
                        {
                            Ok(meta) => {
                                tracing::info!("Stored {} (etag: {})", file_name, meta.etag);
                            }
                            Err(e) => {
                                tracing::error!("Failed to store {}: {}", file_name, e);
                            }
                        }
                    });
                } else {
                    tracing::debug!("Benchmark mode: skipped storage write for {}", header.file_name);
                }

                // Send acknowledgment back to client
                let ack_header = quic_protocol::ChunkHeader {
                    file_name: header.file_name,
                    offset: 0,
                    total_size: header.total_size,
                    chunk_len: 0,
                    frame: quic_protocol::FrameType::UploadChunk,
                };

                if let Ok(encoded) = ack_header.encode() {
                    let _ = qconn.stream_send(stream_id, &encoded, true);
                }
            }
        }

        Ok(())
    }

    fn handle_download_request(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
        _stream_id: u64,
        header: quic_protocol::ChunkHeader,
    ) -> tokio_quiche::QuicResult<()> {
        tracing::info!("Download request: {}", header.file_name);

        // Fetch from storage and send back
        let storage = self.storage.clone();
        let file_name = header.file_name.clone();
        let peer_addr = format!("{:?}", qconn.trace_id());

        // Spawn task to fetch and send
        tokio::spawn(async move {
            match storage.storage.get_object("quic-uploads", &file_name).await {
                Ok((data, meta)) => {
                    tracing::info!(
                        "Sending {} to {} ({} bytes)",
                        file_name,
                        peer_addr,
                        data.len()
                    );

                    // TODO: Send chunks back to client on new streams
                    // This requires access to the connection which isn't available here
                    // For now, log success
                    tracing::info!("File {} ready for download (size: {})", file_name, meta.size);
                }
                Err(e) => {
                    tracing::error!("Failed to fetch {}: {}", file_name, e);
                }
            }
        });

        Ok(())
    }
}

impl tokio_quiche::ApplicationOverQuic for S3QuicApplication {
    fn on_conn_established(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
        handshake_info: &tokio_quiche::quic::HandshakeInfo,
    ) -> tokio_quiche::QuicResult<()> {
        tracing::info!(
            "QUIC connection established (handshake took {:?}, trace_id: {})",
            handshake_info.elapsed(),
            qconn.trace_id()
        );
        Ok(())
    }

    fn should_act(&self) -> bool {
        true
    }

    fn buffer(&mut self) -> &mut [u8] {
        &mut self.tx_buffer
    }

    fn wait_for_data(
        &mut self,
        _qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> impl std::future::Future<Output = tokio_quiche::QuicResult<()>> + Send {
        async {
            // Don't sleep - let tokio-quiche use epoll for event notification
            // Polling with sleep() was causing excessive context switches
            std::future::ready(Ok(())).await
        }
    }

    fn process_reads(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        self.drive_reads(qconn)
    }

    fn process_writes(
        &mut self,
        _qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }

    fn on_conn_close<M: tokio_quiche::metrics::Metrics>(
        &mut self,
        qconn: &mut tokio_quiche::quic::QuicheConnection,
        _metrics: &M,
        connection_result: &tokio_quiche::QuicResult<()>,
    ) {
        match connection_result {
            Ok(_) => tracing::info!("QUIC connection closed (trace_id: {})", qconn.trace_id()),
            Err(e) => tracing::warn!("QUIC connection error (trace_id: {}): {:?}", qconn.trace_id(), e),
        }
    }
}
