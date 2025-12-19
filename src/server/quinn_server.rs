// Quinn QUIC server with optional no-encryption mode for benchmarking
use crate::handler::BaseHandler;
use bytes::BytesMut;
use quinn::{Connection, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use super::quinn_noprotection::NoProtectionServerConfig;

pub struct QuinnServer {
    handler: Arc<BaseHandler>,
    benchmark_mode: bool,
    no_encryption: bool,
}

impl QuinnServer {
    pub fn new(handler: BaseHandler, benchmark_mode: bool, no_encryption: bool) -> Self {
        if benchmark_mode {
            tracing::warn!("BENCHMARK MODE: Storage writes disabled");
        }
        if no_encryption {
            tracing::warn!("NO ENCRYPTION MODE: Packet encryption disabled for benchmarking");
        }

        Self {
            handler: Arc::new(handler),
            benchmark_mode,
            no_encryption,
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        // Initialize crypto provider
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Generate self-signed certificate
        let (cert, key) = generate_self_signed_cert_quinn()?;

        // Create rustls config with ALPN
        let mut tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        tls_config.alpn_protocols = vec![b"file-transfer".to_vec()];

        // Wrap with NoProtection if requested
        let mut server_config = if self.no_encryption {
            tracing::info!("Using NoProtection mode - encryption disabled for benchmarking");
            ServerConfig::with_crypto(Arc::new(
                NoProtectionServerConfig::new(Arc::new(tls_config))
            ))
        } else {
            ServerConfig::with_crypto(Arc::new(
                quinn_proto::crypto::rustls::QuicServerConfig::try_from(tls_config)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?
            ))
        };

        // Configure transport parameters for high throughput
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_concurrent_bidi_streams(100u32.into());
        transport_config.max_concurrent_uni_streams(100u32.into());
        transport_config.stream_receive_window(quinn_proto::VarInt::from_u32(8_000_000)); // 8MB per stream
        transport_config.receive_window(quinn_proto::VarInt::from_u64(100_000_000).unwrap()); // 100MB total
        transport_config.send_window(100_000_000u64); // 100MB send window

        server_config.transport = Arc::new(transport_config);

        // Create endpoint
        let endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        tracing::info!(
            "Quinn QUIC server listening on {} (encryption: {})",
            addr,
            if self.no_encryption { "DISABLED" } else { "enabled" }
        );

        // Accept connections
        while let Some(connecting) = endpoint.accept().await {
            tracing::info!("Incoming connection from {}", connecting.remote_address());
            let handler = self.handler.clone();
            let benchmark_mode = self.benchmark_mode;

            tokio::spawn(async move {
                tracing::debug!("Starting TLS handshake...");
                match connecting.await {
                    Ok(connection) => {
                        tracing::info!("Connection established from {}", connection.remote_address());
                        if let Err(e) = handle_connection(connection, handler, benchmark_mode).await {
                            tracing::error!("Connection error: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Connection handshake failed: {:?}", e);
                    }
                }
            });
        }

        Ok(())
    }
}

async fn handle_connection(
    connection: Connection,
    handler: Arc<BaseHandler>,
    benchmark_mode: bool,
) -> anyhow::Result<()> {
    let mut stream_buffers: HashMap<u64, BytesMut> = HashMap::new();
    let start = Instant::now();
    let mut total_bytes = 0u64;

    // Accept bidirectional streams
    loop {
        tokio::select! {
            stream_result = connection.accept_bi() => {
                match stream_result {
                    Ok((mut send, mut recv)) => {
                        let stream_id = recv.id().index();

                        // Read all data from this stream
                        let mut buffer = BytesMut::new();
                        loop {
                            match recv.read_chunk(64 * 1024, true).await? {
                                Some(chunk) => {
                                    total_bytes += chunk.bytes.len() as u64;
                                    buffer.extend_from_slice(&chunk.bytes);
                                }
                                None => break,
                            }
                        }

                        // For benchmarking, just acknowledge receipt
                        if !benchmark_mode {
                            // TODO: Parse protocol and store object
                            // For now, just acknowledge
                        }

                        // Send response
                        send.write_all(b"OK").await?;
                        send.finish()?;
                    }
                    Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                        break;
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput_mbps = if elapsed.as_secs_f64() > 0.0 {
        (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    } else {
        0.0
    };

    tracing::info!(
        "Connection closed: {} bytes in {:?} ({:.2} MiB/s)",
        total_bytes, elapsed, throughput_mbps
    );

    Ok(())
}

fn generate_self_signed_cert_quinn() -> std::io::Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
    let cert = CertificateDer::from(cert.cert);

    Ok((cert, key))
}
