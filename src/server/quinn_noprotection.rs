// No-encryption crypto layer for Quinn QUIC benchmarking
// Adapted from https://github.com/quinn-rs/quinn/blob/main/perf/src/noprotection.rs
// This disables packet encryption/decryption for performance testing

use quinn_proto::{crypto, transport_parameters, ConnectError, ConnectionId, TransportError, Side};
use std::sync::Arc;

/// Client configuration wrapper that disables encryption
pub struct NoProtectionClientConfig {
    inner: Arc<rustls::ClientConfig>,
}

impl NoProtectionClientConfig {
    pub fn new(inner: Arc<rustls::ClientConfig>) -> Self {
        Self { inner }
    }
}

impl crypto::ClientConfig for NoProtectionClientConfig {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        server_name: &str,
        params: &transport_parameters::TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, ConnectError> {
        let quic_config = Arc::new(
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(self.inner.clone())
                .map_err(|_| ConnectError::UnsupportedVersion)?
        );
        let tls = quic_config.start_session(version, server_name, params)?;

        Ok(Box::new(NoProtectionSession::new(tls)))
    }
}

/// Server configuration wrapper that disables encryption
pub struct NoProtectionServerConfig {
    inner: Arc<rustls::ServerConfig>,
}

impl NoProtectionServerConfig {
    pub fn new(inner: Arc<rustls::ServerConfig>) -> Self {
        Self { inner }
    }
}

impl crypto::ServerConfig for NoProtectionServerConfig {
    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<crypto::Keys, crypto::UnsupportedVersion> {
        let quic_config = quinn_proto::crypto::rustls::QuicServerConfig::try_from(self.inner.clone())
            .map_err(|_| crypto::UnsupportedVersion)?;
        let mut keys = quic_config.initial_keys(version, dst_cid)?;

        // Wrap with NoProtection
        keys.packet.local = Box::new(NoProtectionPacketKey {
            inner: keys.packet.local,
        });
        keys.packet.remote = Box::new(NoProtectionPacketKey {
            inner: keys.packet.remote,
        });

        Ok(keys)
    }

    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        let quic_config = quinn_proto::crypto::rustls::QuicServerConfig::try_from(self.inner.clone())
            .expect("Failed to create QuicServerConfig");
        quic_config.retry_tag(version, orig_dst_cid, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &transport_parameters::TransportParameters,
    ) -> Box<dyn crypto::Session> {
        let quic_config = Arc::new(
            quinn_proto::crypto::rustls::QuicServerConfig::try_from(self.inner.clone())
                .expect("Failed to create QuicServerConfig")
        );
        let tls = quic_config.start_session(version, params);

        Box::new(NoProtectionSession::new(tls))
    }
}

/// Session wrapper that forwards calls except encryption/decryption
struct NoProtectionSession {
    inner: Box<dyn crypto::Session>,
}

impl NoProtectionSession {
    fn new(inner: Box<dyn crypto::Session>) -> Self {
        Self { inner }
    }
}

impl crypto::Session for NoProtectionSession {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: Side) -> crypto::Keys {
        let mut keys = self.inner.initial_keys(dst_cid, side);
        keys.packet.local = Box::new(NoProtectionPacketKey {
            inner: keys.packet.local,
        });
        keys.packet.remote = Box::new(NoProtectionPacketKey {
            inner: keys.packet.remote,
        });
        keys
    }

    fn handshake_data(&self) -> Option<Box<dyn std::any::Any>> {
        self.inner.handshake_data()
    }

    fn peer_identity(&self) -> Option<Box<dyn std::any::Any>> {
        self.inner.peer_identity()
    }

    fn early_crypto(&self) -> Option<(Box<dyn crypto::HeaderKey>, Box<dyn crypto::PacketKey>)> {
        self.inner.early_crypto().map(|(hk, pk)| {
            (
                hk,
                Box::new(NoProtectionPacketKey { inner: pk }) as Box<dyn crypto::PacketKey>,
            )
        })
    }

    fn early_data_accepted(&self) -> Option<bool> {
        self.inner.early_data_accepted()
    }

    fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    fn read_handshake(&mut self, buf: &[u8]) -> Result<bool, TransportError> {
        self.inner.read_handshake(buf)
    }

    fn transport_parameters(&self) -> Result<Option<transport_parameters::TransportParameters>, TransportError> {
        self.inner.transport_parameters()
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<crypto::Keys> {
        self.inner.write_handshake(buf).map(|mut keys| {
            keys.packet.local = Box::new(NoProtectionPacketKey {
                inner: keys.packet.local,
            });
            keys.packet.remote = Box::new(NoProtectionPacketKey {
                inner: keys.packet.remote,
            });
            keys
        })
    }

    fn next_1rtt_keys(&mut self) -> Option<crypto::KeyPair<Box<dyn crypto::PacketKey>>> {
        self.inner.next_1rtt_keys().map(|mut keys| {
            keys.local = Box::new(NoProtectionPacketKey {
                inner: keys.local,
            });
            keys.remote = Box::new(NoProtectionPacketKey {
                inner: keys.remote,
            });
            keys
        })
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.inner.is_valid_retry(orig_dst_cid, header, payload)
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), crypto::ExportKeyingMaterialError> {
        self.inner
            .export_keying_material(output, label, context)
    }
}

/// Packet key wrapper that disables actual encryption/decryption
struct NoProtectionPacketKey {
    inner: Box<dyn crypto::PacketKey>,
}

impl crypto::PacketKey for NoProtectionPacketKey {
    fn encrypt(&self, packet: u64, buf: &mut [u8], header_len: usize) {
        // Instead of encrypting, just fill tag with a constant value
        let tag_start = buf.len() - self.tag_len();
        buf[tag_start..].fill(42);
    }

    fn decrypt(
        &self,
        packet: u64,
        header: &[u8],
        payload: &mut bytes::BytesMut,
    ) -> Result<(), crypto::CryptoError> {
        // Instead of decrypting, just truncate the tag
        let tag_len = self.tag_len();
        if payload.len() < tag_len {
            return Err(crypto::CryptoError);
        }
        payload.truncate(payload.len() - tag_len);
        Ok(())
    }

    fn tag_len(&self) -> usize {
        self.inner.tag_len()
    }

    fn confidentiality_limit(&self) -> u64 {
        self.inner.confidentiality_limit()
    }

    fn integrity_limit(&self) -> u64 {
        self.inner.integrity_limit()
    }
}
