use std::time::Duration;

// Simulates a synchronous vendor library call.
// In production: calls into the C FFI wrapper.
fn validate_tle_sync(line1: &str, line2: &str) -> Result<(), String> {
    // Vendor library does checksum + orbital element bounds checking.
    // Blocks for 2–15ms depending on record complexity.
    std::thread::sleep(Duration::from_millis(5)); // placeholder
    if line1.starts_with("1 ") && line2.starts_with("2 ") {
        Ok(())
    } else {
        Err(format!("malformed TLE: {line1}"))
    }
}

async fn validate_tle_async(line1: String, line2: String) -> Result<(), String> {
    // Move strings into the blocking closure.
    // spawn_blocking runs on the dedicated blocking thread pool —
    // async worker threads are not touched.
    tokio::task::spawn_blocking(move || validate_tle_sync(&line1, &line2))
        .await
        .map_err(|e| format!("validator panicked: {e}"))?
}

#[tokio::main]
async fn main() {
    // All 48 sessions can submit validation concurrently.
    // Each runs on the blocking pool; none stall the async workers.
    let tasks = (0..6)
        .map(|idx| {
            tokio::spawn(validate_tle_async(
                format!(
                    "1 {:05}U 98067A   21275.52  .00001234  00000-0  12345-4 0  999{idx}",
                    idx
                ),
                format!(
                    "2 {:05}  51.6400 337.6640 0007417  62.6000 297.5200 15.4888958300000{idx}",
                    idx
                ),
            ))
        })
        .collect::<Vec<_>>();

    for (idx, t) in tasks.into_iter().enumerate() {
        match t.await.unwrap() {
            Ok(()) => println!("record {idx}: valid"),
            Err(e) => println!("record {idx}: {e}"),
        }
    }
}
