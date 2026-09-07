use foundation_track::module4::project4::gs_server::{http_task, tcp_task};
use tokio::{net::TcpListener, sync::watch};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let (sys_shutdown_tx, _) = watch::channel(false);

    // TASK 1: Raw TCP Stream Server
    const ADDR: &str = "127.0.0.1:9100";
    let tcp_listener = TcpListener::bind(ADDR).await?;
    tracing::info!(addr = ADDR, "TCP listener bound");

    let tcp_task = tokio::spawn(tcp_task(tcp_listener, sys_shutdown_tx.subscribe()));

    // TASK 2: HTTP Server
    const API_BASE_URL: &str = "127.0.0.1:9101";
    let http_listener = TcpListener::bind(API_BASE_URL).await?;
    tracing::info!(addr = API_BASE_URL, "HTTP listener bound");

    let http_task = tokio::spawn(http_task(http_listener, sys_shutdown_tx.subscribe()));

    // Broadcast shutdown on Ctrl-C so accept loops and connection tasks can drain.
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received, broadcasting shutdown");
            let _ = sys_shutdown_tx.send(true);
        }
    });

    let _ = tokio::join!(tcp_task, http_task);
    tracing::info!("all server tasks finished, exiting");

    Ok(())
}
