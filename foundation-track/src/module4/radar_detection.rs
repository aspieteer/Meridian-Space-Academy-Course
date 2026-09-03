use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct RadarDetection {
    sensor_id: u32,
    azimuth_deg: f32,
    elevation_deg: f32,
    range_km: f32,
    timestamp_ms: u64,
}

impl RadarDetection {
    fn new_with_parse(buf: &[u8], addr: SocketAddr) -> Option<Self> {
        // Wire format: 4-byte sensor_id | 4-byte azimuth (f32 BE) |
        //              4-byte elevation (f32 BE) | 4-byte range (f32 BE) |
        //              8-byte timestamp (u64 BE)
        if buf.len() < 24 {
            return None; // Malformed datagram - discard silently.
        }

        let sensor_id = u32::from_be_bytes(buf[0..4].try_into().ok()?);

        let azimuth_deg = f32::from_be_bytes(buf[4..8].try_into().ok()?);
        let elevation_deg = f32::from_be_bytes(buf[8..12].try_into().ok()?);
        let range_km = f32::from_be_bytes(buf[12..16].try_into().ok()?);
        let timestamp_ms = u64::from_be_bytes(buf[16..24].try_into().ok()?);

        tracing::debug!(%addr, sensor_id, "detection received");

        Some(Self {
            sensor_id,
            azimuth_deg,
            elevation_deg,
            range_km,
            timestamp_ms,
        })
    }
}

pub struct UdpSysGuard {
    system: Option<UdpSys>,
    transmit_handle: Option<JoinHandle<anyhow::Result<()>>>,
    recv_handle: Option<JoinHandle<()>>,
}

impl UdpSysGuard {
    pub fn new(socket: UdpSocket, bound: usize) -> Self {
        let (system, transmit_handle, recv_handle) = UdpSys::new(socket, bound);

        Self {
            system: Some(system),
            transmit_handle: Some(transmit_handle),
            recv_handle: Some(recv_handle),
        }
    }

    /// Graceful shutdown: signal, then actually wait for workers.
    pub async fn close(mut self) -> anyhow::Result<()> {
        self.system
            .as_ref()
            .expect("retrieved system field after taken")
            .signal_shutdown();

        // Transmit task observes the token and returns.
        self.transmit_handle
            .take()
            .expect("no transmit handle running")
            .await??;

        // Our Arc<Shared> must go before RecvEnd can see the channel close.
        drop(self.system.take());

        self.recv_handle
            .take()
            .expect("no recv handle running")
            .await?;

        Ok(())
    }
}

impl Drop for UdpSysGuard {
    fn drop(&mut self) {
        if let Some(system) = &self.system {
            system.signal_shutdown();
        }
    }
}

pub struct UdpSys {
    shared: Arc<Shared>,
}

pub struct Shared {
    socket: UdpSocket,
    tx: mpsc::Sender<RadarDetection>,
    token: CancellationToken,
}

impl UdpSys {
    pub fn new(
        socket: UdpSocket,
        bound: usize,
    ) -> (UdpSys, JoinHandle<anyhow::Result<()>>, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(bound);
        let shared = Arc::new(Shared {
            socket,
            tx,
            token: CancellationToken::new(),
        });

        let transmit_handle = tokio::spawn(transmit_from_udp(shared.clone()));
        let recv_handle = tokio::spawn(RecvEnd::new(rx));

        (UdpSys { shared }, transmit_handle, recv_handle)
    }

    pub fn signal_shutdown(&self) {
        if self.shared.token.is_cancelled() {
            tracing::debug!(
                "the shutdown signal should be triggered only once during the system running"
            );
            return;
        }

        self.shared.token.cancel();
    }
}

pub async fn transmit_from_udp(shared: Arc<Shared>) -> anyhow::Result<()> {
    let mut buf = [0u8; 1472]; // Stay under MTU to avoid fragmentation. (1500 MTU -
    // 20 IP header - 8 UDP header)

    loop {
        tokio::select! {
            biased;

            _ = shared.token.cancelled() => {
                tracing::info!("radar transfer shutting down");
                break;
            }

            recv = shared.socket.recv_from(&mut buf) => {
                match recv {
                    Ok((n , addr)) => {
                        if let Some(detection) = RadarDetection::new_with_parse(&buf[0..n], addr) {
                            // Non-blocking — drop if pipeline is full rather than
                            // blocking the receive loop. A queued radar sweep is
                            // useless by the time it clears the backlog.
                            if shared.tx.try_send(detection).is_err() {
                                tracing::warn!("detection pipeline full — datagram dropped");
                            }
                        }
                    },
                    Err(e) => {
                        tracing::warn!("recv error: {e}");
                        // UDP recv errors are typically transient — continue.
                    },
                }
            }
        }
    }

    Ok(())
}

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct RecvEnd {
        rx: mpsc::Receiver<RadarDetection>,
    }
}

impl RecvEnd {
    pub(crate) fn new(rx: mpsc::Receiver<RadarDetection>) -> Self {
        Self { rx }
    }
}

impl Future for RecvEnd {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        loop {
            match ready!(this.rx.poll_recv(cx)) {
                Some(det) => {
                    tracing::info!(
                        sensor = det.sensor_id,
                        az = det.azimuth_deg,
                        el = det.elevation_deg,
                        range = det.range_km,
                        "detection processed"
                    );
                }
                None => {
                    tracing::warn!("sender closed");
                    return Poll::Ready(());
                }
            }
        }
    }
}
