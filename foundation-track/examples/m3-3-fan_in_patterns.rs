use std::time::Duration;

use foundation_track::module3::{
    router_tagged_frame::{RouterMsg, router_actor},
    tagged_frame::{live_uplink, replay_feed},
};
use tokio::sync::mpsc;

use bytes::Bytes;

// ===== select!-Based Priority Fan-In =====

async fn priority_aggregator(mut high: mpsc::Receiver<Bytes>, mut low: mpsc::Receiver<Bytes>) {
    loop {
        tokio::select! {
            // biased: always check high-priority first.
        // Without biased, both channels are polled in random order —
        // low-priority frames could be dispatched before high-priority ones
        // if both are ready simultaneously.
            biased;

            Some(frame) = high.recv() => {
                println!("HIGH: {} bytes", frame.len());
            }
            Some(frame) = low.recv() => {
                println!("LOW: {} bytes", frame.len());
            }
            else => break,
        }
    }
}

#[tokio::main]
async fn main() {
    // ===== select high priority msgs over low priority ones =====

    let (high_tx, high_rx) = mpsc::channel(64);
    let (low_tx, low_rx) = mpsc::channel(256);

    // High-priority: SAFE_MODE and emergency commands.
    tokio::spawn(async move {
        for _ in 0..3 {
            high_tx.send(Bytes::from(vec![0xFF; 8])).await.unwrap(); // emergency frame
        }
    });

    // Low-priority: housekeeping telemetry.
    tokio::spawn(async move {
        for _ in 0..5 {
            low_tx.send(Bytes::from(vec![0x00; 64])).await.unwrap();
        }
    });

    priority_aggregator(high_rx, low_rx).await;

    // ===== Tagged msg with Source Identity =====

    let (tx, mut rx) = mpsc::channel(128);

    for sat_id in 0..5_u32 {
        tokio::spawn(live_uplink(sat_id, tx.clone()));
    }
    tokio::spawn(replay_feed(Bytes::from("ARTEMIS-IV"), tx.clone()));
    drop(tx);

    while let Some(frame) = rx.recv().await {
        match frame.source() {
            foundation_track::module3::tagged_frame::FeedKind::LiveUplink { satellite_id } => {
                println!(
                    "live satellite {satellite_id} seq {}: {} bytes",
                    frame.sequence(),
                    frame.payload().len()
                );
            }
            foundation_track::module3::tagged_frame::FeedKind::ArchivedReplay { mission_id } => {
                println!(
                    "replay {mission_id:?} seq {}: {} bytes",
                    frame.sequence(),
                    frame.payload().len()
                );
            }
        }
    }

    // ===== router actor pattern =====

    let (ctrl_tx, ctrl_rx) = mpsc::channel(8);
    let (out_tx, mut out_rx) = mpsc::channel(256);

    tokio::spawn(router_actor(ctrl_rx, out_tx));

    // Register two satellite feeds dynamically.
    for sat_id in [25544_u32, 48274] {
        let (feed_tx, feed_rx) = mpsc::channel(32);
        let _ = ctrl_tx
            .send(RouterMsg::AddFeed {
                source_id: sat_id,
                feed: feed_rx,
            })
            .await;

        tokio::spawn(async move {
            for i in 0..3_u8 {
                let _ = feed_tx.send(Bytes::from(vec![i; 16])).await;
            }
        });
    }

    drop(ctrl_tx);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut count = 0;

    while let Ok(frame) = tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await {
        if let Some(f) = frame {
            println!("satellite {}: {} bytes", f.source_id(), f.payload().len());
            count += 1;
        } else {
            break;
        }
    }
    println!("total frames: {count}");
}
