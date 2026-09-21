use std::sync::Arc;

use anyhow::Context;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc},
};
use tracing::instrument;

use super::{connection::Connection, frame, shutdown::Shutdown};

const MAX_CONNECTIONS: usize = 256;

#[derive(Debug)]
struct Listener {
    listener: TcpListener,

    limit_connections: Arc<Semaphore>,

    // The reason why this broadcast sender is defined here is because
    // we need to instantiate a Handler when listener accepts a connection.
    notify_shutdown: broadcast::Sender<()>,

    // Use as a part of graceful shutdonw process to wait for all client connections
    // to complete processing, dropping the senders, then the receiver in run() function
    // could yield to its result.
    shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(e) = res {
                tracing::error!(cause = %e, "failed to accept");
            }
        }
        _ = shutdown => {
            tracing::info!("shutting down");
        }
    }

    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);

    // Drop first initialized `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("accepting inbound connections");

        loop {
            // `acquire_owned()` returns `Err` when the semaphore has been
            // closed. We don't ever close the semaphore, so `unwrap()` is safe.
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await;

            if let Some(socket) = socket {
                let mut handler = Handler {
                    connection: Connection::new(socket),
                    shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                    _shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                };

                tokio::spawn(async move {
                    if let Err(e) = handler.run().await {
                        tracing::error!(cause = ?e, "connection error");
                    }

                    drop(permit);
                });
            }
        }
    }

    async fn accept(&mut self) -> Option<TcpStream> {
        match self.listener.accept().await {
            Ok((socket, addr)) => {
                tracing::info!("connection from {}", addr);
                Some(socket)
            }
            Err(e) => {
                tracing::warn!("connection failed: {e}");
                None
            }
        }
    }
}

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut batch = Vec::with_capacity(1024);

        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let (header, _) = frame.split_into_header_and_payload();
            if header.validate() {
                batch.push(header);
            }

            if batch.len() >= 1000 {
                let full_batch = std::mem::take(&mut batch);

                let processing = tokio::task::spawn_blocking(move || {
                    let mut indices = frame::deduplicate_indices(&full_batch);
                    frame::sort_indices_by_timestamp(&mut indices, &full_batch);
                    indices
                })
                .await
                .context("process batch")?;

                tracing::info!(result_count = %processing.len(), "filtered in one batch");
                batch.clear();
            }
        }

        Ok(())
    }
}
