use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use bytes::Bytes;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, ToSocketAddrs},
    sync::broadcast,
};

use super::{frame::Frame, shutdown::Shutdown};

use super::{
    config::{IdPool, PoolId},
    connection::Connection,
};

pub struct Client {
    connection: Connection,
    station_id: PoolId,
    shutdown: Shutdown,
}

pub async fn run<T: ToSocketAddrs + fmt::Debug>(
    addr: T,
    max_conns: usize,
    shutdown: impl Future,
) -> anyhow::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);

    let id_pool = IdPool::new(max_conns);

    let client = Client::connect(addr, &id_pool, notify_shutdown.subscribe()).await?;

    if let Some(mut client) = client {
        tokio::select! {
            res = client.sending() => {
                if let Err(e) = res {
                    tracing::error!(cause = %e, "failed to send frames");
                }
            }

            _ = shutdown => {
                tracing::info!(client_id = %client.station_id.get(), "client shutting down");
            }
        }
    }

    drop(notify_shutdown);

    Ok(())
}

impl Client {
    pub async fn connect<T: ToSocketAddrs + fmt::Debug>(
        addr: T,
        id_pool: &Arc<IdPool>,
        mut notify_shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<Option<Client>> {
        let mut attempt = 0u32;
        let start = Instant::now();

        loop {
            if start.elapsed() > Duration::from_secs(180) {
                return Err(anyhow!("3-min reconnection window exceeded"));
            }

            match TcpStream::connect(&addr).await {
                Ok(socket) => {
                    let station_id = id_pool.select_id();
                    tracing::info!(station_id = %station_id.get(), "station connected to server: [{:?}]", &addr);
                    let connection = Connection::new(socket);
                    return Ok(Some(Client {
                        connection,
                        station_id,
                        shutdown: Shutdown::new(notify_shutdown),
                    }));
                }
                Err(e) => {
                    tracing::warn!(attempt = %attempt, "failed to connect: {e}");
                }
            }

            attempt += 1;
            let delay = backoff_delay_plus_jitter(attempt);
            tracing::info!("try reconnecting in {delay:?}");
            // TODO: replace with select!
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = notify_shutdown.recv() => {
                    return Ok(None);
                }
            }
        }
    }

    pub async fn sending(&mut self) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 256;

        let mut rng = fastrand::Rng::new();
        let station_id = self.station_id.get();
        let mut batch: Vec<Bytes> = Vec::with_capacity(BATCH_SIZE);

        loop {
            batch.clear();
            Frame::extend_batch(&mut rng, station_id, 256, &mut batch, BATCH_SIZE);

            tokio::select! {
                res = self.connection.write_frames(&batch) => {
                    if let Err(e) = res {
                        return Err(anyhow::anyhow!("failed to send frames: {e}"));
                    }
                }
                _ = self.shutdown.recv() => break,
            }
        }

        if !self.connection.stream().buffer().is_empty() {
            self.connection.stream_mut().flush().await?;
        }

        Ok(())
    }
}

// ===== utils =====

fn backoff_delay_plus_jitter(attempt: u32) -> Duration {
    use std::time::SystemTime;

    Duration::from_secs(1u64 << attempt.min(5))
        + Duration::from_millis(u64::from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_millis()
                % 1000,
        ))
}
