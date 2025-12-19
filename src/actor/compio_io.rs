// Compio I/O service - one runtime per disk

use crossbeam_channel::{Sender, Receiver, unbounded};
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;
use std::sync::Arc;

/// I/O operation types
pub enum IoOperation {
    Read { path: PathBuf },
    Write { path: PathBuf, data: bytes::Bytes },
}

/// I/O request with response channel
pub struct IoRequest {
    pub operation: IoOperation,
    pub response_tx: oneshot::Sender<Result<Vec<u8>, std::io::Error>>,
}

/// Response from I/O operation
pub type IoResponse = Result<Vec<u8>, std::io::Error>;

/// Compio I/O service with one runtime per disk
#[derive(Clone)]
pub struct CompioIoService {
    disk_senders: Arc<Vec<Sender<IoRequest>>>,
}

impl CompioIoService {
    /// Create new compio I/O service with runtimes for each disk
    pub fn new(num_disks: usize) -> Self {
        let mut disk_senders = Vec::new();

        for disk_id in 0..num_disks {
            let (tx, rx) = unbounded();
            disk_senders.push(tx);

            // Spawn dedicated thread for this disk's compio runtime
            std::thread::Builder::new()
                .name(format!("compio-disk-{}", disk_id))
                .spawn(move || {
                    Self::run_compio_runtime(disk_id, rx);
                })
                .expect("Failed to spawn compio thread");
        }

        Self {
            disk_senders: Arc::new(disk_senders),
        }
    }

    /// Run compio runtime on dedicated thread
    fn run_compio_runtime(disk_id: usize, rx: Receiver<IoRequest>) {
        tracing::info!("Starting compio runtime for disk {}", disk_id);

        // Create compio runtime ONCE for this disk
        let runtime = compio::runtime::Runtime::new()
            .expect("Failed to create compio runtime");

        // Run forever processing requests
        runtime.block_on(async move {
            loop {
                match rx.recv() {
                    Ok(req) => {
                        let result = Self::process_request(req.operation).await;
                        // Send response back (ignore if receiver dropped)
                        let _ = req.response_tx.send(result);
                    }
                    Err(_) => {
                        tracing::info!("Compio runtime {} shutting down", disk_id);
                        break;
                    }
                }
            }
        });
    }

    /// Process I/O request using compio
    async fn process_request(operation: IoOperation) -> IoResponse {
        use compio_io::AsyncReadAtExt;
        use compio_io::AsyncWriteAtExt;

        match operation {
            IoOperation::Read { path } => {
                // Open file with compio
                let file = compio_fs::File::open(&path).await?;

                // Read entire file
                let buf = Vec::with_capacity(4096);
                let buf_result = file.read_to_end_at(buf, 0).await;
                buf_result.0.map(|_| buf_result.1)
            }
            IoOperation::Write { path, data } => {
                // Create file with compio
                let mut file = compio_fs::File::create(&path).await?;

                // Write data (convert Bytes to Vec for compio)
                let data_vec = data.to_vec();
                let buf_result = file.write_all_at(data_vec, 0).await;
                buf_result.0.map(|_| Vec::new())  // Empty response for write
            }
        }
    }

    /// Send read request to appropriate disk
    pub async fn read(&self, disk_id: usize, path: &Path) -> IoResponse {
        let (tx, rx) = oneshot::channel();
        let req = IoRequest {
            operation: IoOperation::Read {
                path: path.to_path_buf(),
            },
            response_tx: tx,
        };

        self.disk_senders[disk_id]
            .send(req)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "IO service closed"))?;

        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Response channel closed"))?
    }

    /// Send write request to appropriate disk
    pub async fn write(&self, disk_id: usize, path: &Path, data: bytes::Bytes) -> IoResponse {
        let (tx, rx) = oneshot::channel();
        let req = IoRequest {
            operation: IoOperation::Write {
                path: path.to_path_buf(),
                data,
            },
            response_tx: tx,
        };

        self.disk_senders[disk_id]
            .send(req)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "IO service closed"))?;

        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Response channel closed"))?
    }

    /// Fire-and-forget write: submit to io_uring without waiting for completion
    /// Returns immediately after submitting the request to the compio runtime
    /// Used for eventual consistency where we don't need to wait for writes
    pub fn write_nowait(&self, disk_id: usize, path: PathBuf, data: bytes::Bytes) -> Result<(), std::io::Error> {
        // Create dummy response channel - compio will send result but we don't wait
        let (tx, _rx) = oneshot::channel();
        let req = IoRequest {
            operation: IoOperation::Write {
                path,
                data,
            },
            response_tx: tx,
        };

        // Send request to compio runtime (non-blocking channel send)
        self.disk_senders[disk_id]
            .send(req)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "IO service closed"))?;

        Ok(())
    }
}
