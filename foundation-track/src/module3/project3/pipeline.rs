// ### **Pipeline Architexture**
//
// [Uplink 0..48]  ──┐
//                   ├─► [Router Actor] ─► [Priority Fan-In] ─► [Frame Processor]
// [Replay 0..2]   ──┘         │                                        │
//                             └──────────────────────────────► [Broadcast: all frames]
//                                                                      │
//                                                             [Dashboard] [Archive]
//
// [watch: shutdown] ──────────────────────────────────────► All tasks
// [watch: stats]    ◄─────────────────────────── Frame Processor (updates atomically)

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{broadcast, mpsc, watch};

use super::{
    frame::{Frame, FramePriority},
    source::SourceMsg,
};

#[derive(Debug)]
pub struct PipelineStats {
    frames_processed: AtomicU64,
    emergency_count: AtomicU64,
}

impl PipelineStats {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            frames_processed: AtomicU64::new(0),
            emergency_count: AtomicU64::new(0),
        })
    }

    pub(crate) fn increment_frames_processed(&self) {
        self.frames_processed.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn increment_emergency_count(&self) {
        self.emergency_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn frames_processed(&self) -> u64 {
        self.frames_processed.load(Ordering::Relaxed)
    }
    pub fn emergency_count(&self) -> u64 {
        self.emergency_count.load(Ordering::Relaxed)
    }
}

pub async fn router_source(
    mut ctrl: mpsc::Receiver<SourceMsg>,
    mut shutdown_rx: watch::Receiver<bool>,
    emergency_out: mpsc::Sender<Frame>,
    routine_out: mpsc::Sender<Frame>,
) {
    let (internal_tx, mut internal_rx) = mpsc::channel(512);
    let mut feed_handles = HashMap::new();

    loop {
        tokio::select! {
            // Control messages: add or remove sources.
            Some(rtr_msg) = ctrl.recv() => {
                match rtr_msg {
                    SourceMsg::AddSource { source_id, source_kind, mut feed } => {
                        let forward_tx = internal_tx.clone();
                        let kind_clone = source_kind.clone();
                        let handle = tokio::spawn(async move {
                            while let Some(frame) = feed.recv().await {
                                if forward_tx.send(frame).await.is_err() {
                                    tracing::warn!("Router has shut down");
                                    break;
                                }
                            }
                            tracing::debug!(id=source_id, kind=?kind_clone.clone(), "feed task exiting");
                        });

                        feed_handles.insert((source_id, source_kind), handle);
                    },
                    SourceMsg::RemoveSource { source_id, source_kind } => {
                        if let Some(handle) = feed_handles.remove(&(source_id, source_kind)) {
                            handle.abort(); // Feed task no longer needed.
                        }
                    }
                }
            }
            // Frames from all registered feeds, already fan-in'ed via internal channel.
            Some(frame) = internal_rx.recv() => {
                match frame.priority() {
                    FramePriority::Emergency => {
                        match emergency_out.send(frame).await {
                            Ok(()) => {
                                tracing::info!("Routing Emergency frame ~ ");
                            },
                            Err(_) => break, // Downstream consumer has shut down.
                        }
                    }
                    FramePriority::Routine => {
                        match routine_out.send(frame).await {
                            Ok(()) => {
                                tracing::info!("Routing Routine frame ~ ");
                            },
                            Err(_) => break, // Downstream consumer has shut down.
                        }
                    }
                }
            }
            Ok(()) = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    drop(internal_tx);
                    for (_, h) in feed_handles.drain() { h.await.unwrap(); }
                    break;
                }
            }
            else => break,
        }
    }
}

pub async fn priority_fan_in(
    mut emergency_rx: mpsc::Receiver<Frame>,
    mut routine_rx: mpsc::Receiver<Frame>,
    out: mpsc::Sender<Frame>,
) {
    loop {
        tokio::select! {
            biased;

            Some(frame) = emergency_rx.recv() => {
                if out.send(frame).await.is_err() {
                    tracing::warn!("routing channel receiver has shut down");
                    break;
                }
            }

            Some(frame) = routine_rx.recv() => {
                if out.send(frame).await.is_err() {
                    tracing::warn!("routing channel receiver has shut down");
                    break;
                }
            }


            else => break,
        }
    }
}

pub async fn processer_fan_out(
    stats: Arc<PipelineStats>,
    mut process_rx: mpsc::Receiver<Frame>,
    stats_tx: watch::Sender<Arc<PipelineStats>>,
    brdcast_tx: broadcast::Sender<Frame>,
) {
    while let Some(frame) = process_rx.recv().await {
        let is_emerg = matches!(frame.priority(), FramePriority::Emergency);
        tracing::info!(
            source = frame.source().0,
            seq = frame.sequence(),
            priority = if is_emerg { "EMERGENCY" } else { "Routine" },
            "processed"
        );

        stats.increment_frames_processed();
        if is_emerg {
            stats.increment_emergency_count();
        }

        let _ = stats_tx.send(stats.clone());
        let _ = brdcast_tx.send(frame);
    }
}

pub async fn archive_consumer(mut brdcast_rx: broadcast::Receiver<Frame>) {
    let mut count = 0_u64;
    loop {
        match brdcast_rx.recv().await {
            Ok(_) => count += 1,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(missed = n, "archive lagged");
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::info!(total = count, "archive done");
                break;
            }
        }
    }
}
