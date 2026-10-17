use foundation_track::module5::project5::{PORT, server};
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::try_init()?;

    let listener = TcpListener::bind(&format!("127.0.0.1:{PORT}")).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}
