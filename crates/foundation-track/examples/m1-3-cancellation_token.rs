use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;

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
    tracing_subscriber::FmtSubscriber::builder()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let token = CancellationToken::new();

    let handles: Vec<_> = (0..4)
        .map(|id| {
            // Clone the token for each task — all clones share the same cancellation.
            let t = token.clone();
            tokio::spawn(uplink_session(id, t))
        })
        .collect();

    // Simulate running for a short time then shutting down.
    sleep(Duration::from_millis(150)).await;

    // Cancel all sessions simultaneously with one call.
    token.cancel();

    for handle in handles {
        match handle.await {
            // Task completed, session succeeded.
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(%e, "session returned I/O error"),
            Err(e) if e.is_panic() => tracing::error!(%e, "session task panicked"),
            Err(e) => tracing::error!(%e, "session task aborted"),
        }
    }

    tracing::info!("all sessions shut down");

    Ok(())
}
