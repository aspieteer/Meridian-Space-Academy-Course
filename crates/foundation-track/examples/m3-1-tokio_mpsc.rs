use tokio::sync::mpsc;

async fn multiple_uplink_send(satellite_id: u32, tx: &mpsc::Sender<(u32, Vec<u8>)>) {
    // Each uplink session gets its own cloned sender.
    let tx = tx.clone();
    tokio::spawn(async move {
        for seq in 0_u8..3 {
            let mut frame = satellite_id.to_ne_bytes().to_vec();
            frame.push(seq);
            // Yields if channel is full — backpressure in action.
            tx.send((satellite_id, frame))
                .await
                .expect("aggregator task dropped");
        }
    });
}

async fn forward_or_drop(tx: &mpsc::Sender<Vec<u8>>, frame: Vec<u8>) {
    match tx.try_send(frame) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(frame)) => {
            // Aggregator is falling behind — record the drop and continue.
            // In production: increment a metrics counter here.
            tracing::warn!(bytes = frame.len(), "frame dropped: aggregator full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => tracing::error!("aggregator task has exited"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let (tx1, mut rx1) = mpsc::channel(64);

    for satellite_id in 0..8_u32 {
        multiple_uplink_send(satellite_id, &tx1).await;
    }

    // Drop the original sender so the channel closes when all
    // spawned tasks finish. Without this drop, rx.recv() never
    // returns None — the original sender keeps the channel alive.
    drop(tx1);

    while let Some((sat, frame)) = rx1.recv().await {
        println!("sat {sat}: {:?}", frame);
    }

    let (tx2, mut rx2) = mpsc::channel(8);
    // Demonstrate try_send behaviour
    for i in 0u8..12 {
        forward_or_drop(&tx2, vec![i]).await;
    }
    drop(tx2);
    let mut count = 0;
    while rx2.recv().await.is_some() {
        count += 1;
    }
    println!("received {count} frame (8 max due to bound)");
}
