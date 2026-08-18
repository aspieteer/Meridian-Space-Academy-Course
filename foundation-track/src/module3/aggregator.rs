use std::collections::VecDeque;

use tokio::sync::{mpsc, oneshot};

const MAX_BUFFER: usize = 1000;

#[derive(Debug)]
pub struct TelemetryFrame {
    satellite_id: u32,
    sequence: u64,
    payload: Vec<u8>,
}

impl TelemetryFrame {
    pub fn new(satellite_id: u32, sequence: u64, payload: Vec<u8>) -> Self {
        Self {
            satellite_id,
            sequence,
            payload,
        }
    }
}

pub enum AggregatorMsg {
    /// A new frame from an uplink session.
    Frame(TelemetryFrame),
    /// Request: how many frames are buffered?
    Depth { reply: oneshot::Sender<usize> },
    /// Drain the buffer and return all frames.
    Drain {
        reply: oneshot::Sender<Vec<TelemetryFrame>>,
    },
}

pub async fn run_aggregator(mut rx: mpsc::Receiver<AggregatorMsg>) {
    let mut buffer = VecDeque::with_capacity(MAX_BUFFER);

    while let Some(msg) = rx.recv().await {
        match msg {
            AggregatorMsg::Frame(frame) => {
                if buffer.len() >= MAX_BUFFER {
                    tracing::warn!(
                        satellite_id = frame.satellite_id,
                        "buffer full - dropping oldest frame"
                    );
                    let _ = buffer.pop_front();
                }
                buffer.push_back(frame);
            }
            AggregatorMsg::Depth { reply } => {
                if reply.send(buffer.len()).is_err() {
                    tracing::error!("oneshot channel error");
                }
            }
            AggregatorMsg::Drain { reply } => {
                let frames = buffer.drain(..).collect::<Vec<_>>();
                if reply.send(frames).is_err() {
                    tracing::error!("oneshot channel error");
                }
            }
        }
    }

    tracing::info!("aggregator: all senders dropped, shutting down");
}

/// A typed handle to the aggregator actor.
/// Hides the channel internals from callers.
#[derive(Clone)]
pub struct AggregatorHandle {
    tx: mpsc::Sender<AggregatorMsg>,
}

impl AggregatorHandle {
    pub fn spawn(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        // Spawn a thread running background task
        tokio::spawn(run_aggregator(rx));
        Self { tx }
    }

    pub async fn send_frame(&self, frame: TelemetryFrame) -> anyhow::Result<()> {
        self.tx
            .send(AggregatorMsg::Frame(frame))
            .await
            .map_err(|_| anyhow::anyhow!("aggregator has shut down"))
    }

    pub async fn depth(&self) -> anyhow::Result<usize> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(AggregatorMsg::Depth { reply: reply_tx })
            .await
            .map_err(|_| anyhow::anyhow!("aggregator has shut down"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("aggregator dropped reply"))
    }

    pub async fn drain(&self) -> anyhow::Result<Vec<TelemetryFrame>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(AggregatorMsg::Drain { reply: reply_tx })
            .await
            .map_err(|_| anyhow::anyhow!("aggregator has shut down"))?;

        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("aggregator dropped reply"))
    }
}
