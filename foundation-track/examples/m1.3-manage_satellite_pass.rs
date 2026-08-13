use std::time::Duration;

use module_1::pass_session::manage_pass;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let handle = tokio::spawn(manage_pass(
        25544,
        "gs-svalbard".to_string(),
        Duration::from_secs(30),
        shutdown_rx,
    ));

    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = shutdown_tx.send(());

    match handle.await {
        Ok(Ok(())) => tracing::info!("task completed"),
        Ok(Err(e)) => tracing::warn!("task error: {e}"),
        Err(e) if e.is_cancelled() => tracing::warn!("task was aborted externally"),
        Err(e) => tracing::warn!("task panicked: {e}"),
    }

    Ok(())
}
