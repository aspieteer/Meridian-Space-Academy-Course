use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum FeedKind {
    LiveUplink { satellite_id: u32 },
    ArchivedReplay { mission_id: Bytes },
}

#[derive(Debug)]
pub struct TaggedFrame {
    source: FeedKind,
    sequence: u64,
    payload: Bytes,
}

impl TaggedFrame {
    pub fn source(&self) -> &FeedKind {
        &self.source
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }
}

pub async fn live_uplink(sat_id: u32, tx: mpsc::Sender<TaggedFrame>) {
    for seq in 0..3_u64 {
        let mut payload = sat_id.to_le_bytes().to_vec();
        payload.extend(vec![0u8; 28]);
        debug_assert!(payload.len() == 32);
        let payload = Bytes::from(payload);
        let _ = tx
            .send(TaggedFrame {
                source: FeedKind::LiveUplink {
                    satellite_id: sat_id,
                },
                sequence: seq,
                payload,
            })
            .await;
    }
}

pub async fn replay_feed(mission: Bytes, tx: mpsc::Sender<TaggedFrame>) {
    for seq in 0..2_u64 {
        let _ = tx
            .send(TaggedFrame {
                source: FeedKind::ArchivedReplay {
                    mission_id: mission.clone(),
                },
                sequence: seq,
                payload: Bytes::from(vec![0xAA; 128]),
            })
            .await;
    }
}
