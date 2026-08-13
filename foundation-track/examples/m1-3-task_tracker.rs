use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    time::{Duration, sleep},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

async fn uplink_session(station_id: u32, token: CancellationToken) -> tokio::io::Result<()> {
    let filename = format!("f00{}.txt", station_id);
    let mut file = BufWriter::new(File::create(&filename).await.unwrap());
    loop {
        tokio::select! {
            // cancelled() is just a future — it composes naturally with select!.
            _ = token.cancelled() => {
                // Run async cleanup here before returning.
                // This is the key advantage over .abort(): we choose when to stop
                // and can flush state, send final messages, close connections.
                tracing::info!(station_id, "session received cancellation - draining");
                flush_pending_frames(&mut file).await?;
                break;
            }
            res = process_next_frame(&mut file, station_id) => {
                // Normal frame processing continues until token is cancelled.
                res?;
            }
        }
    }

    Ok(())
}

async fn flush_pending_frames<W>(buf: &mut BufWriter<W>) -> tokio::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    buf.flush().await?;
    sleep(Duration::from_millis(10)).await; // placeholder

    Ok(())
}

async fn process_next_frame<W>(buf: &mut BufWriter<W>, id: u32) -> tokio::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    tracing::debug!(id = id, "processing");
    let content = format!("processing id={}\n", id);
    buf.write_all(content.as_bytes()).await?;
    sleep(Duration::from_millis(u64::from(id + 1) * 50)).await; // placeholder

    Ok(())
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let tracker = TaskTracker::new();
    let token = CancellationToken::new();

    for station_id in 0..12_u32 {
        let t = token.clone();
        tracker.spawn(uplink_session(station_id, t));
    }

    // Signal that no more tasks will be spawned.
    // wait() will not resolve until close() has been called.
    tracker.close();

    // Trigger shutdown.
    sleep(Duration::from_millis(150)).await;
    token.cancel();

    // Block until all 12 sessions finish their cleanup.
    tracker.wait().await;

    tracing::info!("all sessions drained");

    Ok(())
}
