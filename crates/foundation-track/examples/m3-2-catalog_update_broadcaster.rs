use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use tokio::sync::{broadcast, watch};

#[derive(Debug, Clone)]
struct TleUpdate {
    batch_id: u32,
    records: Arc<Vec<Bytes>>,
}

impl TleUpdate {
    fn get_first_record(&self) -> &str {
        assert!(!self.records.is_empty());
        let byt = self.records.first().unwrap();
        let byt = byt.iter().as_slice();
        // SAFETY: in fact, we should use unsafe version to check whether &[u8] slice is valid
        // or not, the code here just for demonstration, so ...
        str::from_utf8(byt).unwrap()
    }
}

async fn session_task(
    satellite_id: u32,
    mut tle_rx: broadcast::Receiver<TleUpdate>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let mut shutdown = shutdown_rx.clone();

    loop {
        tokio::select! {
            result = tle_rx.recv() => {
                match result {
                    Ok(update) => {
                        tracing::info!(satellite_id, batch = update.batch_id, records = update.records.len(), content = update.get_first_record(), "TLE update applied");
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(satellite_id, missed = n, "TLE lag - resyncing");
                    },
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("shutdown signalled");
                    break;
                }
            }
        }
    }

    tracing::info!(satellite_id, "session task exiting");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_thread_ids(true).init();

    let (tle_tx, _) = broadcast::channel(32);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn 5 sessions, each with its own broadcast receiver.
    for sat_id in 0..5_u32 {
        let tle_rx = tle_tx.subscribe();
        let shutdown = shutdown_rx.clone();
        tokio::spawn(session_task(sat_id, tle_rx, shutdown));
    }

    // Publish a TLE update to all sessions.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = tle_tx.send(TleUpdate {
        batch_id: 42,
        records: Arc::new(vec![Bytes::from("1 25544U..."); 100]),
    });

    // Trigger shutdown.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(true);

    tokio::time::sleep(Duration::from_millis(50)).await;
}
