use foundation_track::module3::aggregator::{AggregatorHandle, TelemetryFrame};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let aggr = AggregatorHandle::spawn(128);

    // Simulate 24 concurrent uplink sessions each sending 50 frames.
    let tasks = (0..24_u32)
        .map(|sat_id| {
            let aggr = aggr.clone();
            tokio::spawn(async move {
                for seq in 0..50_u64 {
                    // `?` inside the task: exit early with THIS task's error
                    aggr.send_frame(TelemetryFrame::new(
                        sat_id,
                        seq,
                        sat_id.to_le_bytes().to_vec(),
                    ))
                    .await?;
                }
                Ok::<(), anyhow::Error>(())
            })
        })
        .collect::<Vec<_>>();

    // Await every handle: this is where errors travel back to main.
    for t in tasks {
        t.await??; // JoinError (panic/cancel) OR the task's own error
    }

    // NOTE: Cuz MAX_BUFFER_SIZE is 1000, the 24 * 50 = 1200 frames sent here
    // would trigger warnings on popping the oldest frames dozens of times.
    println!("buffered: {}", aggr.depth().await?);

    let frames = aggr.drain().await?;
    println!("drained {} frames\nFrames:", frames.len());
    for frame in frames {
        println!("{:?}", frame);
    }

    Ok(())
}
