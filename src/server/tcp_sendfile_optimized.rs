// Optimized TCP server using sendfile() with reduced overhead
// Optimizations:
// 1. Pre-open file to avoid open() syscall overhead
// 2. Use TCP_CORK to batch header + data
// 3. Remove per-request metadata() call
// 4. Minimize allocations and runtime overhead
// 5. Use SO_SNDBUF tuning

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::raw::c_int;
use std::sync::Arc;
use std::time::Instant;

type OffT = i64;
type SizeT = usize;
type SSizeT = isize;

const REQUEST_BYTE: u8 = 1;
const SENDFILE_CHUNK: usize = 8 * 1024 * 1024; // 8MB chunks
const TCP_CORK: c_int = 3;
const SO_SNDBUF: c_int = 7;
const SOL_SOCKET: c_int = 1;
const IPPROTO_TCP: c_int = 6;

unsafe extern "C" {
    fn sendfile(out_fd: c_int, in_fd: c_int, offset: *mut OffT, count: SizeT) -> SSizeT;
    fn setsockopt(
        socket: c_int,
        level: c_int,
        name: c_int,
        value: *const std::os::raw::c_void,
        option_len: u32,
    ) -> c_int;
}

pub struct OptimizedTcpSendfileServer {
    test_file_path: String,
    file_fd: c_int,
    file_len: u64,
    file_len_bytes: [u8; 8],
}

impl OptimizedTcpSendfileServer {
    pub fn new(test_file_path: String) -> io::Result<Self> {
        // Pre-open file and cache metadata
        let file = std::fs::File::open(&test_file_path)?;
        let file_len = file.metadata()?.len();
        let file_fd = file.as_raw_fd();
        let file_len_bytes = file_len.to_be_bytes();

        // Keep file open by leaking it
        std::mem::forget(file);

        Ok(Self {
            test_file_path,
            file_fd,
            file_len,
            file_len_bytes,
        })
    }

    pub fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(false)?;

        eprintln!("Optimized TCP sendfile server listening on {}", addr);

        loop {
            let (stream, peer_addr) = listener.accept()?;

            let server = self.clone();
            // Use std::thread for minimal overhead (no Tokio scheduler)
            std::thread::spawn(move || {
                if let Err(e) = server.handle_connection_optimized(stream, peer_addr) {
                    eprintln!("Connection error from {}: {}", peer_addr, e);
                }
            });
        }
    }

    fn handle_connection_optimized(
        &self,
        stream: TcpStream,
        _peer_addr: std::net::SocketAddr,
    ) -> io::Result<()> {
        // Set TCP options for optimal performance
        stream.set_nodelay(true)?;

        // Increase send buffer size
        let sndbuf_size: c_int = 256 * 1024; // 256KB
        unsafe {
            setsockopt(
                stream.as_raw_fd(),
                SOL_SOCKET,
                SO_SNDBUF,
                &sndbuf_size as *const _ as *const std::os::raw::c_void,
                std::mem::size_of::<c_int>() as u32,
            );
        }

        // Enable TCP_CORK to batch header + data
        let cork_on: c_int = 1;
        unsafe {
            setsockopt(
                stream.as_raw_fd(),
                IPPROTO_TCP,
                TCP_CORK,
                &cork_on as *const _ as *const std::os::raw::c_void,
                std::mem::size_of::<c_int>() as u32,
            );
        }

        // Read request byte
        let mut req = [0u8; 1];
        (&stream).read_exact(&mut req)?;

        if req[0] != REQUEST_BYTE {
            return Ok(());
        }

        // Send file length header (uses pre-computed bytes)
        (&stream).write_all(&self.file_len_bytes)?;

        // Use sendfile() for zero-copy transfer
        let mut offset: OffT = 0;
        let mut bytes_sent: u64 = 0;
        let started = Instant::now();

        while bytes_sent < self.file_len {
            let remaining = self.file_len - bytes_sent;
            let to_send = remaining.min(SENDFILE_CHUNK as u64) as SizeT;

            match unsafe { sendfile(stream.as_raw_fd(), self.file_fd, &mut offset, to_send) } {
                -1 => {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted
                        || err.kind() == io::ErrorKind::WouldBlock
                    {
                        continue;
                    }
                    return Err(err);
                }
                0 => break,
                n => bytes_sent += n as u64,
            }
        }

        // Disable TCP_CORK to flush
        let cork_off: c_int = 0;
        unsafe {
            setsockopt(
                stream.as_raw_fd(),
                IPPROTO_TCP,
                TCP_CORK,
                &cork_off as *const _ as *const std::os::raw::c_void,
                std::mem::size_of::<c_int>() as u32,
            );
        }

        let elapsed = started.elapsed();
        let throughput_mbps = if elapsed.as_secs_f64() > 0.0 {
            (bytes_sent as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
        } else {
            0.0
        };

        // Only log if trace level (minimal overhead in production)
        if log::log_enabled!(log::Level::Trace) {
            eprintln!(
                "Sent {} bytes in {:?} ({:.2} MiB/s)",
                bytes_sent, elapsed, throughput_mbps
            );
        }

        let _ = stream.shutdown(Shutdown::Write);
        Ok(())
    }
}
