use std::fmt;
use std::hash::Hash;

use bytes::Bytes;
use tokio::sync::mpsc;

use super::frame::FramePriority;

use super::frame::Frame;

#[derive(Debug, Clone)]
pub struct Source {
    source_id: u32,
    source_kind: SourceKind,
    feed: Option<mpsc::Sender<Frame>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SourceKind {
    LiveUplink,
    ArchivedReplay,
}

impl Source {
    pub(crate) fn new(
        source_id: u32,
        source_kind: SourceKind,
        feed_tx: mpsc::Sender<Frame>,
    ) -> Self {
        Self {
            source_id,
            source_kind,
            feed: Some(feed_tx),
        }
    }

    pub(crate) fn source(&self) -> (u32, &SourceKind) {
        (self.source_id, &self.source_kind)
    }

    pub(crate) fn take_feed(&mut self) -> Option<mpsc::Sender<Frame>> {
        self.feed.take()
    }

    pub async fn feeding(&self, priority: FramePriority, payload: Bytes) {
        // TODO: Further sequence hashing actually needs each source
        // holds its own hasher, to be honest.
        let frame = Frame::new(self.source_id, self.source_kind.clone(), priority, payload);

        let _ = self
            .feed
            .as_ref()
            .expect("Invalid Source without feed channel")
            .send(frame)
            .await;
    }
}

// ===== SourceMsg =====

pub enum SourceMsg {
    AddSource {
        source_id: u32,
        source_kind: SourceKind,
        feed: tokio::sync::mpsc::Receiver<Frame>,
    },
    RemoveSource {
        source_id: u32,
        source_kind: SourceKind,
    },
}

impl fmt::Debug for SourceMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddSource {
                source_id,
                source_kind,
                ..
            } => f
                .debug_struct("AddSource")
                .field("source_id", source_id)
                .field("source_kind", source_kind)
                .finish(),
            Self::RemoveSource {
                source_id,
                source_kind,
            } => f
                .debug_struct("RemoveSource")
                .field("source_id", source_id)
                .field("source_kind", source_kind)
                .finish(),
        }
    }
}
