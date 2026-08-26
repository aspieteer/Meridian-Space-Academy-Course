// Once a connection is established, below are some examples of handling
// the connection, including Reading frame, Spliting a socket into ReaderHalf and WriterHalf,
// whether they're referenced or owned, you can use them Either on the same task, such as within
// a single select! or a sequential pair in the former case, Or sent to different tasks in the
// latter case, and wrapping socket onto a BufWriter to reduce the overhead for every write_all
// syscall.

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::Duration,
};

#[derive(Debug)]
pub struct TelemetryFrame {
    station_id: Bytes,
    payload: Vec<u8>,
}

impl TelemetryFrame {
    pub fn id(&self) -> Bytes {
        self.station_id.slice(..)
    }

    pub fn payload(&self) -> Option<&str> {
        let res = str::from_utf8(self.payload.as_slice());
        res.ok()
    }
}

// NOTE:
// 1.
// read_exact is the right primitive for fixed-size framing (like Meridian's 4-byte length prefix).
// It guarantees the buffer is fully populated before returning, handling the case where
// the underlying read returns fewer bytes than requested.
// 2.
// EOF handling: read() returning Ok(0) means the remote has closed the write half of the connection.
// Any subsequent read() will also return Ok(0). When you see this, exit the read loop —
// continuing to call read() on a closed stream creates a 100% CPU spin loop.
pub(crate) async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Option<Vec<u8>>> {
    // u32 as the message header for indicating payload length.
    let mut len_buf = [0u8; 4];

    // read_exact returns Err(UnexpectedEof) if the connection closes mid-header.
    match stream.read_exact(&mut len_buf).await {
        Ok(_n) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Clean EOF at frame boundary — connection closed normally.
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let length = u32::from_be_bytes(len_buf) as usize;
    if length > 65_536 {
        anyhow::bail!("frame too large: {length} bytes");
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;

    Ok(Some(payload))
}

// NOTE:
// Use TcpStream::split() (reference) when both read and write stay in one task.
// Use TcpStream::into_split() (value) when they need to move to separate tasks.
pub(crate) async fn bidirectional_handler(stream: TcpStream) -> anyhow::Result<()> {
    // into_split: value split — each half can move to separate tasks.
    let (mut reader, mut writer) = stream.into_split();

    // Write task: sends periodic heartbeats.
    let write_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if writer.write_all(b"HEARTBEAT\n").await.is_err() {
                break;
            }
        }
    });

    // Read task: processes incoming frames.
    let mut buf = vec![0u8; 4096];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        } // EOF
        tracing::debug!(bytes = n, "frame received");
    }

    // write_task.abort();

    Ok(())
}

// NOTE:
// 1.
// Each write_all call is a syscall.
// For a protocol that sends many small writes (header bytes, then payload bytes), the overhead accumulates.
// Wrapping the write half in tokio::io::BufWriter buffers writes and flushes them in larger batches:
// 2.
// Always call flush() after writing a complete logical unit (a frame, a response).
// If you return from the handler without flushing,
// buffered data is silently dropped when the BufWriter drops.
pub(crate) async fn write_framed(stream: TcpStream, payload: &[u8]) -> anyhow::Result<()> {
    // BufWriter with 8KB internal buffer — flushes when full or on explicit flush().
    let mut writer = BufWriter::new(stream);

    // These two writes go to the internal buffer, not to the socket.
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;

    // flush() pushes the buffered bytes to the socket in one syscall.
    writer.flush().await?;

    Ok(())
}

async fn connection_handler(
    mut stream: TcpStream,
    station_id: Bytes,
    frame_tx: mpsc::Sender<TelemetryFrame>,
    mut shutdown: watch::Receiver<bool>,
) {
    tracing::info!(station = ?station_id, "session started");

    loop {
        tokio::select! {
            // Bias toward reading to complete in-progress frames.
            biased;

            frame = tokio::time::timeout(Duration::from_secs(60), read_frame(&mut stream)) => {
                match frame {
                    // Session timeout — ground station went silent.
                    Err(_elapsed) => {
                        tracing::warn!(station = ?station_id, "session timeout");
                        break;
                    }
                    Ok(Ok(Some(payload))) => {
                        if frame_tx.send(TelemetryFrame { station_id: station_id.clone(), payload }).await.is_err() {
                            tracing::warn!("Aggregator shut down");
                            break;
                        }
                    }
                    Ok(Ok(None)) => {
                        tracing::info!(station = ?station_id, "connection closed by peer");
                        break;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(station = ?station_id, "read error: {e}");
                        break;
                    }
                }
            }

            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    // Send a clean close to the peer.
    let _ = stream.shutdown().await;
    tracing::info!(station = ?station_id, "session ended");
}

// Review:
// About gracefully shutdowning Accept loop,
// using the watch channel implemented in Module 3:
pub async fn run_tcp_server(
    bind_addr: &str,
    frame_tx: mpsc::Sender<TelemetryFrame>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!("ground station server listening on {bind_addr}");
    let mut conn_id = 0_usize;
    let mut sd = shutdown.clone();

    loop {
        tokio::select! {
            accept = listener.accept() => {
            match accept {
                Ok((socket, addr)) => {
                    tracing::info!(%addr, "connection accepted");
                    conn_id += 1;
                    let station_id = Bytes::from(format!("gs-{conn_id}@{addr}"));
                    tokio::spawn(
                        connection_handler(socket, station_id, frame_tx.clone(), shutdown.clone())
                    );
                },
                Err(e) => {
                    tracing::warn!("accept error: {e}");
                    // Continue — transient errors are normal.
                },
            }
            }
            Ok(()) = sd.changed() => {
                if *sd.borrow() {
                    tracing::info!("accept loop shutting down");
                    break;
                }
            }
        }
    }

    tracing::info!("accept loop exited");
    Ok(())
}
