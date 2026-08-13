/// Parse and validate a TLE record from a raw string.
/// TLE parsing is synchronous and O(n) with input length.
/// On a 100KB batch, this can take several milliseconds.
fn parse_tle_batch_blocking(raw: String) -> anyhow::Result<Vec<String>> {
    // Synchronous parsing — no I/O, but CPU-bound for large inputs.
    raw.lines()
        .filter(|l| l.starts_with("1 ") || l.starts_with("2 "))
        .map(|l| Ok(l.to_string()))
        .collect()
}

async fn ingest_tle_update(raw_batch: String) -> anyhow::Result<Vec<String>> {
    // Moving raw_batch into spawn_blocking satisfies the 'static bound.
    // The closure executes on a blocking thread; we await the JoinHandle.
    let records = tokio::task::spawn_blocking(move || parse_tle_batch_blocking(raw_batch))
        .await
        .map_err(|e| anyhow::anyhow!("TLE parser panicked: {e}"))??;

    Ok(records)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let raw = "1 25544U 98067A   21275.52500000  .00001234  00000-0  12345-4 0  9999\n\
               2 25544  51.6400 337.6640 0007417  62.6000 297.5200 15.48889583300000\n"
        .to_string();

    let records = ingest_tle_update(raw).await?;
    println!("Parsed {} TLE records", records.len());

    Ok(())
}
