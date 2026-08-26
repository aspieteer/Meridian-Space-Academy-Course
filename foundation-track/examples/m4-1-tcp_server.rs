use std::time::Duration;

use foundation_track::module4::connection_processing::{TelemetryFrame, run_tcp_server};
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let (frame_tx, mut frame_rx) = mpsc::channel::<TelemetryFrame>(256);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Frame consumer.
    tokio::spawn(async move {
        while let Some(frame) = frame_rx.recv().await {
            tracing::info!(station = ?frame.id(), bytes = frame.payload().expect("Invalid payload string"), "frame received");
        }
    });

    // Shutdown after 2 seconds for demo purposes.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = shutdown_tx.send(true);
    });

    run_tcp_server("0.0.0.0:7777", frame_tx, shutdown_rx).await
}
