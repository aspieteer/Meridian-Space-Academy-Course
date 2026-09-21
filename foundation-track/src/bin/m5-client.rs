use foundation_track::module5::project5::{PORT, client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().init();

    client::run(&format!("127.0.0.1:{PORT}"), 256, tokio::signal::ctrl_c()).await?;

    Ok(())
}
