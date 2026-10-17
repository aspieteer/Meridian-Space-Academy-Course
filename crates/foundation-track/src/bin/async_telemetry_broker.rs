use std::time::Duration;

use foundation_track::module1::project1::{Frame, handle_connection, spawn_handler};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let bind_addr = "0.0.0.0:7777";
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!("meridian broker is listening on {bind_addr}");

    // broadcast channel for frame tranferring
    let (frame_tx, _) = broadcast::channel::<Frame>(256);
    // watch channel listening for shutdown signal
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn 5 downstream handlers.
    let handler_handles: Vec<tokio::task::JoinHandle<()>> = (0..5)
        .map(|idx| spawn_handler(idx, frame_tx.subscribe()))
        .collect();

    // Ctrl-C handler.
    let shutdown_tx_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
        tracing::info!("ctrl-c received - initiating graceful shutdown");
        let _ = shutdown_tx_ctrlc.send(true);
    });

    let mut session_handles = Vec::new();
    let mut conn_id = 0_usize;

    loop {
        // Stop accepting new connections once shutdown is signalled.
        if *shutdown_rx.borrow() {
            break;
        }

        tokio::select! {
            conn = listener.accept() => {
                match conn {
                    Ok((sock, addr)) => {
                        conn_id += 1;
                        let station_id = format!("gs-{conn_id}@{addr}");
                        let handle = tokio::spawn(handle_connection(sock, station_id, frame_tx.clone(),shutdown_rx.clone()));
                        session_handles.push(handle);
                    },
                    Err(e) => tracing::warn!("accept error: {e}"),
                }
            }
            _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
            }

        }
    }

    tracing::info!(
        "draining {} active sessions (10s deadline)",
        session_handles.len()
    );

    // Drop the broadcast sender so downstream handlers see Closed after drain.
    drop(frame_tx);

    let drain_result = tokio::time::timeout(Duration::from_secs(10), async {
        for h in handler_handles {
            let _ = h.await;
        }

        for h in session_handles {
            let _ = h.await;
        }
    })
    .await;

    if drain_result.is_err() {
        tracing::warn!("drain deadline exceeded - forcing exit");
    } else {
        tracing::info!("all tasks drained cleanly");
    }

    Ok(())
}
