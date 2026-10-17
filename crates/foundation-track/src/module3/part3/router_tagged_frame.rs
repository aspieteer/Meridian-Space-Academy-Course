use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct TaggedFrame {
    source_id: u32,
    payload: Bytes,
}

impl TaggedFrame {
    pub fn source_id(&self) -> u32 {
        self.source_id
    }

    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }
}

pub enum RouterMsg {
    /// Register a new uplink feed.
    AddFeed {
        source_id: u32,
        feed: mpsc::Receiver<Bytes>,
    },
    /// Remove an uplink feed (session ended).
    RemoveFeed { source_id: u32 },
}

pub async fn router_actor(mut ctrl: mpsc::Receiver<RouterMsg>, out: mpsc::Sender<TaggedFrame>) {
    // Tokio's mpsc doesn't provide a built-in multi-receiver select,
    // so we use a secondary MPSC where all feeds forward their frames.
    let (internal_tx, mut internal_rx) = mpsc::channel(512);
    let mut feed_handles = HashMap::new();

    loop {
        tokio::select! {
            // Control messages: add or remove feeds.
            Some(msg) = ctrl.recv() => {
                match msg {
                    RouterMsg::AddFeed { source_id, mut feed } => {
                        let forward_tx = internal_tx.clone();
                        let handle = tokio::spawn(async move {
                            while let Some(payload) = feed.recv().await {
                                if forward_tx.send(TaggedFrame { source_id, payload }).await.is_err() {
                                    break; // Router shut down.
                                }
                            }
                            tracing::debug!(source_id, "feed task exiting");
                        });
                        feed_handles.insert(source_id, handle);
                    }
                    RouterMsg::RemoveFeed { source_id } => {
                        if let Some(handle) = feed_handles.remove(&source_id) {
                            handle.abort(); // Feed task no longer needed.
                        }
                    }
                }
            }
            // Frames from all registered feeds, already fan-in'ed via internal channel.
            Some(frame) = internal_rx.recv() => {
                if out.send(frame).await.is_err() {
                    break; // Downstream consumer has shut down.
                }
            }
            else => break,
        }
    }
}
