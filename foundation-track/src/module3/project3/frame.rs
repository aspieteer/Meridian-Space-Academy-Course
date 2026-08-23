use bytes::Bytes;

use super::source::SourceKind;

#[derive(Debug, Clone)]
pub struct Frame {
    source_id: u32,
    source_kind: SourceKind,
    priority: FramePriority,
    sequence: u64,
    payload: Bytes,
}

#[derive(Debug, Clone)]
pub enum FramePriority {
    Emergency, // SAFE_MODE, ABORT commands
    Routine,   // Standard telemetry
}

impl Frame {
    pub fn new(
        source_id: u32,
        source_kind: SourceKind,
        priority: FramePriority,
        payload: Bytes,
    ) -> Self {
        let id = u64::from(source_id);
        let property = u64::from(source_kind.clone() as u8 + priority.clone() as u8);
        let sequence = u64::wrapping_add(id, property);

        Self {
            source_id,
            source_kind,
            priority,
            sequence,
            payload,
        }
    }

    pub(crate) fn source(&self) -> (u32, &SourceKind) {
        (self.source_id, &self.source_kind)
    }

    pub(crate) fn priority(&self) -> &FramePriority {
        &self.priority
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}
