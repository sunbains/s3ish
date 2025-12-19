// TCP server using sendfile() for zero-copy file transfers
// Based on ~/dev/sendfile implementation for baseline performance comparison

use crate::handler::BaseHandler;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::raw::c_int;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener as TokioTcpListener;

type OffT = i64;
type SizeT = usize;
type SSizeT = isize;

const REQUEST_BYTE: u8 = 1;
const SENDFILE_CHUNK: usize = 8 * 1024 * 1024; // 8MB chunks

unsafe extern "C" {
    fn sendfile(out_fd: c_int, in_fd: c_int, offset: *mut OffT, count: SizeT) -> SSizeT;
}

pub struct TcpSendfileServer {
    handler: Arc<BaseHandler>,
}

impl TcpSendfileServer {
    pub fn new(handler: BaseHandler) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    pub async fn serve(self, addr: std::net::SocketAddr) -> io::Result<()> {
        // Use std::net::TcpListener for sendfile() compatibility
        // We need the raw file descriptor for sendfile()
        let std_listener = std::net::TcpListener::bind(addr)?;
        std_listener.set_nonblocking(true)?;

        let listener = TokioTcpListener::from_std(std_listener)?;
        tracing::info!("TCP sendfile server listening on {}", addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let handler = self.handler.clone();

            // Spawn a task to handle this connection
            tokio::task::spawn_blocking(move || {
                if let Err(e) = Self::handle_connection(stream, peer_addr, handler) {
                    tracing::error!("Connection error from {}: {}", peer_addr, e);
                }
            });
        }
    }

    fn handle_connection(
        stream: tokio::net::TcpStream,
        peer_addr: std::net::SocketAddr,
        handler: Arc<BaseHandler>,
    ) -> io::Result<()> {
        // Convert tokio TcpStream to std for sendfile() support
        let std_stream = stream.into_std()?;
        std_stream.set_nodelay(true)?;

        tracing::debug!("Client {} connected", peer_addr);

        // Read request byte
        let mut req = [0u8; 1];
        (&std_stream).read_exact(&mut req)?;

        if req[0] != REQUEST_BYTE {
            tracing::warn!("Unexpected request byte {:#x} from {}", req[0], peer_addr);
            return Ok(());
        }

        // For now, send a test file
        // TODO: Integrate with storage backend to serve actual objects
        let test_file_path = "/tmp/test_10mb.bin";
        let file = std::fs::File::open(test_file_path)?;
        let file_len = file.metadata()?.len();
        let fd_in = file.as_raw_fd();

        // Send file length header
        (&std_stream).write_all(&file_len.to_be_bytes())?;

        // Use sendfile() for zero-copy transfer
        let mut offset: OffT = 0;
        let mut bytes_sent: u64 = 0;
        let started = Instant::now();

        while bytes_sent < file_len {
            let remaining = file_len - bytes_sent;
            let to_send = remaining.min(SENDFILE_CHUNK as u64) as SizeT;

            match unsafe { sendfile(std_stream.as_raw_fd(), fd_in, &mut offset, to_send) } {
                -1 => {
                    let err = io::Error::last_os_error();
                    // Retry on EINTR/EAGAIN
                    if err.kind() == io::ErrorKind::Interrupted
                        || err.kind() == io::ErrorKind::WouldBlock
                    {
                        continue;
                    }
                    return Err(err);
                }
                0 => break, // EOF
                n => bytes_sent += n as u64,
            }
        }

        let elapsed = started.elapsed();
        let throughput_mbps = if elapsed.as_secs_f64() > 0.0 {
            (bytes_sent as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
        } else {
            0.0
        };

        tracing::info!(
            "Sent {} bytes to {} in {:?} ({:.2} MiB/s)",
            bytes_sent, peer_addr, elapsed, throughput_mbps
        );

        let _ = std_stream.shutdown(Shutdown::Write);
        Ok(())
    }
}
