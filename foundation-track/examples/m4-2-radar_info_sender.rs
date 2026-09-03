use std::time::Duration;

use rand::{Rng, SeedableRng, rngs::SmallRng};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bind_addr = "0.0.0.0:9098";
    let socket = UdpSocket::bind(bind_addr).await?;

    let remote_addr = "0.0.0.0:9099";
    socket.connect(remote_addr).await?;

    tokio::time::timeout(Duration::from_secs(5), async move {
        let mut rng = SmallRng::from_rng(&mut rand::rng());
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut buf = [0u8; 24];
            rng.fill_bytes(&mut buf);
            if let Err(e) = socket.send(&buf[..]).await {
                return Err(anyhow::anyhow!("send error: {e}"));
            }
        }
    })
    .await??;

    Ok(())
}
