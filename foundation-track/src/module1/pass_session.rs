use std::time::Duration;

use tokio::sync::oneshot;

#[derive(Debug)]
pub struct PassSession {
    satellite_id: u32,
    ground_station: String,
}

impl Drop for PassSession {
    fn drop(&mut self) {
        // Synchronous state flush — no async.
        // In production, push final state to a lock-free ring buffer
        // that a background writer drains to persistent storage.
        tracing::info!(
            satellite_id = self.satellite_id,
            ground_station = self.ground_station,
            "PassSession dropped - flushing state synchronously"
        );
    }
}

impl PassSession {
    fn new(satellite_id: u32, ground_station: String) -> Self {
        Self {
            satellite_id,
            ground_station,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!(satellite_id = self.satellite_id, "pass session started");
        // Simulate frame processing loop.
        // In production: read frames from TcpStream, validate, forward.
        for frame_num in 0_u32..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tracing::info!(frame = frame_num, "frame processed");
        }

        Ok(())
    }
}

pub async fn manage_pass(
    satellite_id: u32,
    ground_station: String,
    pass_duration: Duration,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let mut session = PassSession::new(satellite_id, ground_station);

    // Race: session completion, pass duration timeout, or shutdown signal.
    tokio::select! {
        result = tokio::time::timeout(pass_duration, session.run()) => {
            match result {
                Ok(Ok(())) => tracing::info!(satellite_id, "pass completed normally"),
                Ok(Err(e)) => tracing::warn!(satellite_id, "session run error: {e}"),
                Err(_) => tracing::warn!(satellite_id, "pass duration exceeded - session timed out"),
            }
        }
        _ = &mut shutdown_rx => {
            // PassSession::drop runs here, flushing state before the task exits.
            tracing::warn!(satellite_id, "pass cancelled: shutdown received");
        }
    }
    Ok(())
}
