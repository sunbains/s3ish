// Standalone optimized TCP sendfile benchmark
// Run with: cargo run --release --example tcp_optimized_benchmark

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
const SENDFILE_CHUNK: usize = 8 * 1024 * 1024;
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

struct OptimizedServer {
    file_fd: c_int,
    file_len: u64,
    file_len_bytes: [u8; 8],
}

impl OptimizedServer {
    fn new(path: &str) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let file_len = file.metadata()?.len();
        let file_fd = file.as_raw_fd();
        let file_len_bytes = file_len.to_be_bytes();
        std::mem::forget(file);

        Ok(Self {
            file_fd,
            file_len,
            file_len_bytes,
        })
    }

    fn handle_connection(&self, stream: TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;

        // Tune socket buffers
        let sndbuf: c_int = 256 * 1024;
        unsafe {
            setsockopt(
                stream.as_raw_fd(),
                SOL_SOCKET,
                SO_SNDBUF,
                &sndbuf as *const _ as *const std::os::raw::c_void,
                4,
            );
        }

        // Enable TCP_CORK
        let cork_on: c_int = 1;
        unsafe {
            setsockopt(
                stream.as_raw_fd(),
                IPPROTO_TCP,
                TCP_CORK,
                &cork_on as *const _ as *const std::os::raw::c_void,
                4,
            );
        }

        // Read request
        let mut req = [0u8; 1];
        (&stream).read_exact(&mut req)?;
        if req[0] != REQUEST_BYTE {
            return Ok(());
        }

        // Send header
        (&stream).write_all(&self.file_len_bytes)?;

        // Sendfile loop
        let mut offset: OffT = 0;
        let mut bytes_sent: u64 = 0;

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

        // Disable TCP_CORK
        let cork_off: c_int = 0;
        unsafe {
            setsockopt(
                stream.as_raw_fd(),
                IPPROTO_TCP,
                TCP_CORK,
                &cork_off as *const _ as *const std::os::raw::c_void,
                4,
            );
        }

        let _ = stream.shutdown(Shutdown::Write);
        Ok(())
    }
}

fn main() -> io::Result<()> {
    let server = Arc::new(OptimizedServer::new("/tmp/test_10mb.bin")?);
    let listener = TcpListener::bind("0.0.0.0:9000")?;

    println!("Optimized TCP sendfile server listening on 0.0.0.0:9000");
    println!("Optimizations enabled:");
    println!("  - Pre-opened file (no open() syscall overhead)");
    println!("  - TCP_CORK (batches header + data)");
    println!("  - SO_SNDBUF tuning (256KB)");
    println!("  - std::thread (no Tokio scheduler overhead)");
    println!();

    loop {
        let (stream, _) = listener.accept()?;
        let server = server.clone();

        std::thread::spawn(move || {
            if let Err(e) = server.handle_connection(stream) {
                eprintln!("Error: {}", e);
            }
        });
    }
}
