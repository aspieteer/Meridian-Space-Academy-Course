// NOTE: tokio::join! is appropriate when the futures are independent and you need both results.
// If you only need the first result and want to cancel the loser, use tokio::select!.
// If the futures have no data dependency and need to run across multiple threads simultaneously,
// tokio::spawn each and join the handles.

use tokio::net::TcpStream;

async fn fetch_frame(station_id: &str) -> anyhow::Result<Vec<u8>> {
    // Simplified: in production this reads from a persistent connection pool.
    let mut _stream = TcpStream::connect(format!("{station_id}:7777")).await?;
    // ... read frame bytes ...
    Ok(vec![]) // placeholder
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // CONCURRENT — total latency ≈ max(latency_a, latency_b)
    // Both futures are polled in the same task; no new OS threads are created.
    let (frame_a, frame_b) = tokio::join!(fetch_frame("gs-atacama"), fetch_frame("gs-svalbard"));

    // Both results are available here; handle errors independently.
    match (frame_a, frame_b) {
        (Ok(a), Ok(b)) => {
            println!(
                "Received {} + {} bytes from ground stations",
                a.len(),
                b.len()
            );
        }
        (Err(e), _) | (_, Err(e)) => eprintln!("Ground station fetch failed: {e}"),
    }

    Ok(())
}
