use std::time::Duration;

use bytes::Bytes;
use rand::{Rng, RngExt, SeedableRng, rngs::SmallRng};
use tokio::sync::{mpsc, watch};

use foundation_track::module3::project3::{
    frame::FramePriority, source::SourceKind, telemetry_system::TelemetrySystem,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
    let tel_system = TelemetrySystem::new();

    let mut stats_rx = tel_system.run(ctrl_rx, shutdown_tx.clone());

    let system_clone = tel_system.clone();
    let ctrl_tx_clone = ctrl_tx.clone();

    tokio::spawn(async move {
        loop {
            let ctrl_tx_clone = ctrl_tx_clone.clone();
            let mut rng = SmallRng::from_rng(&mut rand::rng());
            let i = rng.random_range(0..100) as u32;
            let j = rng.random_range(0..100);
            let kind = match j {
                0..96 => SourceKind::LiveUplink,
                96..100 => SourceKind::ArchivedReplay,
                _ => unreachable!(),
            };
            tokio::time::sleep(Duration::from_millis(200)).await;

            let source = system_clone.add_source(i, kind, ctrl_tx_clone);
            if let Some(source) = source {
                let mut data = [0u8; 10];
                rand::rng().fill_bytes(&mut data);
                source
                    .feeding(FramePriority::Routine, Bytes::from(data.to_vec()))
                    .await;
            }
        }
    });

    drop(ctrl_tx);

    // Stats monitor.
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if !*shutdown_rx.borrow() {
                stats_rx.changed().await.ok();
                let s = stats_rx.borrow().clone();
                tracing::info!(
                    "stats: processed={} emergency={}",
                    s.frames_processed(),
                    s.emergency_count()
                );
            } else {
                break;
            }
        }
    });

    let system_clone = tel_system.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        shutdown_tx.send(true).unwrap();
        // println!(
        //     "Telemetry System:\nTotal Length: {:?}\n{:?}",
        //     system_clone.len(),
        //     system_clone
        // );
        system_clone.shutdown_all_sources_feeds();
        tracing::warn!(
            "Shutdown signal fired, the pipeline is gonna stop and all sources gonna release"
        );
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
}
