use std::sync::Arc;

use tokio::{
    sync::{broadcast, watch},
    time::Duration,
};

// ===== broadcast example =====

async fn session_loop(mut rx: broadcast::Receiver<Vec<u8>>) {
    loop {
        match rx.recv().await {
            Ok(frame) => {
                // Normal path.
                process_update(frame).await;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // Receiver fell behind — n messages were lost from this receiver's view.
                // Log the gap and continue; the next recv will succeed.
                tracing::warn!(
                    missed = n,
                    "session fell behind broadcast - requesting resync"
                );
                request_catalog_resync().await;
            }
            Err(broadcast::error::RecvError::Closed) => {
                // All senders dropped — broadcast channel is done.
                tracing::info!("broadcast channel closed, session exiting");
                break;
            }
        }
    }
}

async fn process_update(_frame: Vec<u8>) {}
async fn request_catalog_resync() {}

// ===== watch example =====

#[derive(Debug)]
struct ControlPlaneConfig {
    max_frame_size: usize,
    session_timeout_secs: u64,
}

async fn uplink_session(
    satellite_id: u32,
    mut config_rx: watch::Receiver<Arc<ControlPlaneConfig>>,
) {
    loop {
        // Read current config — no lock, no await.
        let config = config_rx.borrow().clone();

        tokio::select! {
            // Process frames using current config.
            _ = tokio::time::sleep(Duration::from_secs(config.session_timeout_secs)) => {
                tracing::warn!(satellite_id, "session timeout");
                break;
            }
            // React to config changes mid-session.
            Ok(()) = config_rx.changed() => {
                let new_config = config_rx.borrow().clone();
                tracing::info!(satellite_id, max_frame = new_config.max_frame_size, "config reloaded");
                // Loop continues with new config.
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let initial = Arc::new(ControlPlaneConfig {
        max_frame_size: 65536,
        session_timeout_secs: 600,
    });

    let (config_tx, config_rx) = watch::channel(Arc::clone(&initial));

    // Spawn a few sessions.
    for sat_id in 0..5u32 {
        let rx = config_rx.clone();
        tokio::spawn(uplink_session(sat_id, rx));
    }

    // Simulate a config reload.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = config_tx.send(Arc::new(ControlPlaneConfig {
        max_frame_size: 32768,
        session_timeout_secs: 300,
    }));

    tokio::time::sleep(Duration::from_millis(50)).await;
}
