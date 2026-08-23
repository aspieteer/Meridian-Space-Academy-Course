use std::time::Duration;

use bytes::Bytes;
use foundation_track::module3::project3::{
    frame::FramePriority, source::SourceKind, telemetry_system::TelemetrySystem,
};
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let (shutdown_tx, _) = watch::channel(false);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
    let tel_system = TelemetrySystem::new();

    let mut stats_rx = tel_system.run(ctrl_rx, shutdown_tx.clone());

    let mut source_handles = vec![];
    for i in 0..5_u32 {
        let system_clone = tel_system.clone();
        let ctrl_tx_clone = ctrl_tx.clone();

        source_handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(45 * (i + 1) as u64)).await;
            let source = system_clone.add_source(i, SourceKind::LiveUplink, ctrl_tx_clone);
            if let Some(source) = source {
                source
                    .feeding(FramePriority::Routine, Bytes::from("ARTEMIS-IV"))
                    .await;
            }
        }));
    }

    drop(ctrl_tx);

    // Stats monitor.
    tokio::spawn(async move {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stats_rx.changed().await.ok();
            let s = stats_rx.borrow().clone();
            tracing::info!(
                "stats: processed={} emergency={}",
                s.frames_processed(),
                s.emergency_count()
            );
        }
    });

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        shutdown_tx.send(true).unwrap();
        tracing::info!(
            "Shutdown signal fired, the pipeline is gonna stop and all sources gonna release"
        );
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;
    println!(
        "Telemetry System:\nTotal Length: {:?}\n{:?}",
        tel_system.len(),
        tel_system
    );
}
