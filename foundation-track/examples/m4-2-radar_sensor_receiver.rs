use foundation_track::module4::radar_detection::UdpSysGuard;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const MAX_MSG_CAP: usize = 128;
    tracing_subscriber::fmt::init();

    let bind_addr = "0.0.0.0:9099";
    let socket = UdpSocket::bind(bind_addr).await?;

    let sys_guard = UdpSysGuard::new(socket, MAX_MSG_CAP);

    // 1. ANCHOR — main parks here. Program runs until Ctrl+C.
    tokio::signal::ctrl_c().await?;
    tracing::info!("ctrl-c received — shutting down");

    // 2. CLOSE - the main system guard invokes its shutdown routine.
    sys_guard.close().await?;

    Ok(())
}
