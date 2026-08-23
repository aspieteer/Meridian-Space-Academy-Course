use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{broadcast, mpsc, watch};

use crate::module3::project3::{
    pipeline::{
        PipelineStats, archive_consumer, priority_fan_in, processer_fan_out, router_source,
    },
    source::{Source, SourceKind, SourceMsg},
};

#[derive(Debug, Clone)]
pub struct TelemetrySystem {
    shared: Arc<Shared>,
    // Recording stats on the running pipeline
    stats: Arc<PipelineStats>,
}

#[derive(Debug)]
pub struct Shared {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    // Separate different source_kind
    sources_uplink: HashMap<u32, Source>,
    sources_archiv_replay: HashMap<u32, Source>,
    // TODO: expirations ...
}

// ===== impl TelemetrySystem =====

impl Default for TelemetrySystem {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetrySystem {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                sources_uplink: HashMap::new(),
                sources_archiv_replay: HashMap::new(),
            }),
        });
        let stats = PipelineStats::new();

        Self { shared, stats }
    }

    pub fn run(
        &self,
        ctrl_rx: mpsc::Receiver<SourceMsg>,
        shutdown_tx: watch::Sender<bool>,
    ) -> watch::Receiver<Arc<PipelineStats>> {
        let stats = self.stats.clone();
        let shutdown_rx_router = shutdown_tx.subscribe();

        let (stats_tx, stats_rx) = watch::channel(stats.clone());
        let (brdcast_tx, _) = broadcast::channel(128);
        let (emerg_tx, emerg_rx) = mpsc::channel(32);
        let (routine_tx, routine_rx) = mpsc::channel(128);
        let (process_tx, process_rx) = mpsc::channel(64); // only this
        // amout 64 is specified.

        // Start pipeline tasks.
        tokio::spawn(router_source(
            ctrl_rx,
            shutdown_rx_router,
            emerg_tx,
            routine_tx,
        ));
        tokio::spawn(priority_fan_in(emerg_rx, routine_rx, process_tx));
        tokio::spawn(processer_fan_out(
            stats.clone(),
            process_rx,
            stats_tx,
            brdcast_tx.clone(),
        ));
        tokio::spawn(archive_consumer(brdcast_tx.subscribe()));

        stats_rx
    }

    pub fn stats(&self) -> Arc<PipelineStats> {
        self.stats.clone()
    }

    pub fn len(&self) -> usize {
        let state = self.shared.state.lock().unwrap();

        state.sources_uplink.len() + state.sources_archiv_replay.len()
    }

    pub fn is_empty(&self) -> bool {
        let state = self.shared.state.lock().unwrap();

        state.sources_uplink.is_empty() && state.sources_archiv_replay.is_empty()
    }

    pub fn add_source(
        &self,
        source_id: u32,
        source_kind: SourceKind,
        ctrl_tx: mpsc::Sender<SourceMsg>,
    ) -> Option<Source> {
        let mut state = self.shared.state.lock().unwrap();

        if !state.sources_uplink.contains_key(&source_id) && state.sources_uplink.len() >= 48 {
            tracing::warn!(
                source_id,
                ?source_kind,
                length = state.sources_uplink.len(),
                "Up to 48 Live Uplink sources limitation, adding more sources are discarded"
            );
            return None;
        }
        if !state.sources_archiv_replay.contains_key(&source_id)
            && state.sources_archiv_replay.len() >= 2
        {
            tracing::warn!(
                source_id,
                ?source_kind,
                length = state.sources_uplink.len(),
                "Up to 2 Archived Replay sources limitation, adding more sources are discarded"
            );
            return None;
        }

        let (feed_tx, feed_rx) = mpsc::channel(16);
        let source = Source::new(source_id, source_kind.clone(), feed_tx);

        match source_kind {
            SourceKind::LiveUplink => {
                // TODO: Adding furhter replacement if inserting one existed.
                state.sources_uplink.insert(source_id, source.clone());
            }
            SourceKind::ArchivedReplay => {
                state
                    .sources_archiv_replay
                    .insert(source_id, source.clone());
            }
        }

        drop(state);

        let source_msg = SourceMsg::AddSource {
            source_id,
            source_kind,
            feed: feed_rx,
        };

        // TODO:
        if ctrl_tx.try_send(source_msg).is_err() {
            return None;
        }

        tracing::info!(source = ?source.source(), "Adding source");
        Some(source)
    }
}
