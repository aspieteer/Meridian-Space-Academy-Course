use foundation_track::module1::frame_validation::FrameValidationFuture;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    let (tx, rx) = oneshot::channel();

    // Simulate the CRC validator running on a blocking thread pool.
    tokio::spawn(async move {
        // In production: tokio::task::spawn_blocking(|| compute_crc(...)).await
        // Here we just send a valid result immediately.

        // Simulate some processing needed for computing crc.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = tx.send(true);
    });

    let validation = FrameValidationFuture::new(rx);
    match validation.await {
        Ok(()) => println!("Frame header valid - forwarding to telemetry pipeline"),
        Err(e) => eprintln!("Frame rejected: {e}"),
    }
}
