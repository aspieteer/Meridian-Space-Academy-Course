use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{broadcast, watch},
};

// Define Frame carried by tasks
#[derive(Debug, Clone)]
pub struct Frame {
    station_id: String,
    payload: Vec<u8>,
}

impl Frame {
    pub fn new(station_id: String, payload: Vec<u8>) -> Self {
        Self {
            station_id,
            payload,
        }
    }
}

//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                      Payload Length (u32 BE)                  |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                   Payload (variable length)                   |
// |                          ...                                  |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// The Frame Format, consisting the starting 4 bytes header in big-endian, which indicates the payload length,
// followed by the payload with the exact **length**.
pub async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut len_buffer = [0_u8; 4];
    stream.read_exact(&mut len_buffer).await?;
    let len = u32::from_be_bytes(len_buffer) as usize;

    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;

    Ok(payload)
}

pub async fn handle_connection(
    mut stream: TcpStream,
    station_id: String,
    frame_tx: broadcast::Sender<Frame>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tracing::info!(station = station_id, "connection established");

    loop {
        tokio::select! {
            // Bias toward reading frames to minimize partial-frame cancellation.
            // It'll poll the futures in top-to-bottom order.
            biased;

            result = read_frame(&mut stream) => {
                match result {
                    Ok(payload) => {
                        let frame = Frame::new(station_id.clone(), payload);

                        // Broadcast errors mean all receivers dropped — broker is shutting down.
                        if frame_tx.send(frame).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        // EOF or read error — connection dropped.
                        tracing::info!(station = %station_id, "connection closed: {e}");
                        break;
                    },
                }
            }

            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!(station = %station_id, "shutdown - completing current frame then exiting");
                    // The biased select ensures we finish the in-progress read if one was started.
                    // On the next iteration, the shutdown branch will win again and we break.
                    break;
                }
            }
        }
    }
}

pub fn spawn_handler(
    id: usize,
    mut frame_rx: broadcast::Receiver<Frame>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match frame_rx.recv().await {
                Ok(frame) => {
                    tracing::info!(handler = id, station = %frame.station_id, bytes = frame.payload.len(), "frame received");
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(handler = id, missed = n, "handler fell behind - lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!(handler = id, "broadcast channel closed, handler exiting");
                    break;
                }
            }
        }
    })
}
