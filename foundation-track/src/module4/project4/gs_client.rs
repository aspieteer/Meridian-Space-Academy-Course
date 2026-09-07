use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{mpsc, watch},
};

use super::{frame::Frame, tle_record::TleRecord};

pub struct ClientGuard {
    gs_client: GroundStationClient,
    tle_tx: watch::Sender<Option<TleRecord>>,
}

pub struct GroundStationClient {
    inner: Inner,
    start_at: std::time::Instant,
    state: watch::Sender<SessionState>,
}

#[derive(Clone)]
struct Inner {
    client: Client,
    config: ClientConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SessionState {
    Connecting { attempt: u32 },
    Connected { since: std::time::Instant },
    Reconnecting { attempt: u32 },
    Disconnecting,
    Failed { reason: String },
    Stopped,
}

#[derive(Clone)]
pub struct ClientConfig {
    station_id: String,
    addr: String,
    norad_id: u32,
    api_base_url: String,
    tle_refresh_interval: Duration,
}

// ===== impl ClientGuard =====

impl ClientGuard {
    pub fn new(
        config: &ClientConfig,
        overall_timeout_secs: u64,
        conn_timeout_secs: u64,
        sys_shutdown: watch::Receiver<bool>,
    ) -> Self {
        let (state_tx, state_rx) = watch::channel(SessionState::init());
        let client = Client::builder()
            .timeout(Duration::from_secs(overall_timeout_secs))
            .connect_timeout(Duration::from_secs(conn_timeout_secs))
            // The TLE server speaks cleartext HTTP/2 (h2c) — start every
            // connection with the h2 preface instead of HTTP/1.1.
            .http2_prior_knowledge()
            .build()
            .expect("failed to build HTTP client");
        let inner = Inner {
            client,
            config: config.clone(),
        };
        let start_at = Instant::now();

        let (tle_tx, _) = watch::channel(None);

        tokio::spawn(state_monitor(state_rx, tle_tx.subscribe(), sys_shutdown));

        Self {
            gs_client: GroundStationClient {
                inner,
                start_at,
                state: state_tx,
            },
            tle_tx,
        }
    }

    pub async fn run(&self, shutdown_rx: watch::Receiver<bool>) {
        let (frame_tx, frame_rx) = mpsc::channel(256);

        tokio::spawn(frame_receiver(frame_rx));

        self.gs_client
            .run_client(&self.tle_tx, &frame_tx, shutdown_rx)
            .await;
    }
}

// ===== impl GroundStationClient =====

impl GroundStationClient {
    async fn run_session(
        &self,
        mut stream: TcpStream,
        tle_tx: &watch::Sender<Option<TleRecord>>,
        frame_tx: &mpsc::Sender<Frame>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        // Kick off TLE refresh task for this session.
        let (session_shutdown_tx, mut session_shutdown_rx) = watch::channel(false);

        let tle_refresh = {
            let client = self.inner.clone();
            let tle_tx = tle_tx.clone();

            tokio::spawn(async move {
                // interval() ticks immediately on the first call, so the first
                // fetch happens at session start instead of one full period late.
                // Delay (not the default Burst) keeps one fetch per period even
                // when a slow fetch (timeout + retries) overruns a tick.
                let mut ticker = tokio::time::interval(client.config.tle_refresh_interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            match client.fetch_tle().await {
                                Ok(tle) => {
                                    let _ = tle_tx.send(Some(tle));
                                },
                                Err(e) => tracing::warn!("TLE refresh failed: {e}"),
                            }
                        }
                        Ok(()) = session_shutdown_rx.changed() => {
                                if *session_shutdown_rx.borrow() { break; }
                        }
                    }
                }
            })
        };

        loop {
            tokio::select! {
                biased;
                frame = tokio::time::timeout(Duration::from_secs(60), Frame::read_frame(&mut stream)) => {
                    match frame {
                        Err(_) => {
                            tracing::warn!(station = %self.inner.config.station_id, "session timeout");
                            break;
                        }
                        Ok(Ok(Some(payload))) => {
                            let tle = tle_tx.subscribe().borrow().clone();
                            let frame = Frame::new(&self.inner.config.station_id, tle, payload);
                            // Instantly send the frame with non-blocking path
                            if frame_tx.try_send(frame).is_err() {
                                tracing::warn!(station = %self.inner.config.station_id, "frame dropped: pipeline full");
                            }
                        },
                        Ok(Ok(None)) => {
                            tracing::info!(station = %self.inner.config.station_id, "peer closed connection");
                            break;
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(station = %self.inner.config.station_id, "read error: {e}");
                            break;

                        }
                    }
                }
                Ok(()) = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!(station = %self.inner.config.station_id, "shutdown — sending GOODBYE");
                        let payload = b"GOODBYE";
                        let len = (payload.len() as u32).to_be_bytes();
                        let _ = stream.write_all(&len).await;
                        let _ = stream.write_all(payload).await;
                        let _ = stream.flush().await;
                        let _ = stream.shutdown().await;
                        break;
                    }
                }
            }
        }

        // Send session shutdown signal
        let _ = session_shutdown_tx.send(true);
        // Do not drop the fetching task mid-term.
        let _ = tle_refresh.await;
    }

    async fn run_client(
        &self,
        tle_tx: &watch::Sender<Option<TleRecord>>,
        frame_tx: &mpsc::Sender<Frame>,
        shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut attempt = 0u32;

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if self.start_at.elapsed() > Duration::from_secs(300) {
                // Change state to Failed
                let _ = self.state.send(SessionState::Failed {
                    reason: "5-minute reconnect window exceeded".into(),
                });
                return;
            }

            // Connecting
            let _ = self.state.send(SessionState::Connecting { attempt });
            match TcpStream::connect(&self.inner.config.addr).await {
                Ok(stream) => {
                    attempt = 0;
                    // Connected
                    let _ = self.state.send(SessionState::Connected {
                        since: self.start_at,
                    });
                    tracing::info!(station = %self.inner.config.station_id, "connected to {}", self.inner.config.addr);
                    // Run session
                    self.run_session(stream, tle_tx, frame_tx, shutdown_rx.clone())
                        .await;
                    // After one session, check the shutdown signal to be able to early exit.
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    tracing::info!(station = %self.inner.config.station_id, "session ended, will reconnect");
                }
                Err(e) => {
                    tracing::warn!(station = %self.inner.config.station_id, attempt, "connection failed: {e}")
                }
            }

            attempt += 1;
            let delay = backoff_delay(attempt);
            let _ = self.state.send(SessionState::Reconnecting { attempt });
            tracing::info!(station = %self.inner.config.station_id, "reconnecting in {delay:?}");
            tokio::time::sleep(delay).await;
        }
    }
}
// ===== impl Inner =====

impl Inner {
    /// Fetch a single TLE record with up to 3 retry attempts.
    async fn fetch_tle(&self) -> anyhow::Result<TleRecord> {
        let url = format!("{}/tle/{}", self.config.api_base_url, self.config.norad_id);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            match self.client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(resp.json::<TleRecord>().await?);
                }
                Ok(resp) if resp.status().is_server_error() && attempt < 3 => {
                    tracing::warn!(
                        norad_id = &self.config.norad_id,
                        attempt,
                        "TLE fetch server error, retrying"
                    );
                    tokio::time::sleep(backoff_delay(attempt)).await;
                }
                Ok(resp) => anyhow::bail!("TLE fetch: HTTP {}", resp.status()),
                Err(e) if (e.is_connect() || e.is_timeout()) && attempt < 3 => {
                    tracing::warn!(
                        norad_id = &self.config.norad_id,
                        attempt,
                        "TLE fetch network error, retrying"
                    );
                    tokio::time::sleep(backoff_delay(attempt)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

// ===== impl ClientConfig =====

impl ClientConfig {
    pub fn new(
        station_id: &str,
        addr: &str,
        norad_id: u32,
        api_base_url: &str,
        tle_refresh_interval_secs: u32,
    ) -> Self {
        Self {
            station_id: station_id.to_string(),
            addr: addr.to_string(),
            norad_id,
            api_base_url: api_base_url.to_string(),
            tle_refresh_interval: Duration::from_secs(u64::from(tle_refresh_interval_secs)),
        }
    }
}

// ===== impl SessionState =====

impl SessionState {
    fn init() -> Self {
        Self::Stopped
    }
}

// ===== background monitor =====

async fn frame_receiver(mut frame_rx: mpsc::Receiver<Frame>) {
    while let Some(frame) = frame_rx.recv().await {
        tracing::info!(frame = ?frame, "downstream received frame");
    }
}

async fn state_monitor(
    mut state_rx: watch::Receiver<SessionState>,
    mut tle_rx: watch::Receiver<Option<TleRecord>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            Ok(()) = state_rx.changed() => {
                tracing::info!(state = ?*state_rx.borrow(), "session state changed");
            }
            Ok(()) = tle_rx.changed() => {
                tracing::info!(record = ?*tle_rx.borrow(), "fetched tle record");
            }
            // Update interval
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            // Overall shutdown signal from System
            Ok(()) = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { break; }
                }

        }
    }
}

// ===== utils =====

fn backoff_delay(attempt: u32) -> Duration {
    use std::time::SystemTime;

    // Exponential backoff: 1s, 2s, 4s, 8s, capped at 30s.
    // Add jitter to avoid thundering herd.

    let base = Duration::from_secs(1u64 << attempt.min(5));
    let jitter_ms = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis())
        % 1000;

    base + Duration::from_millis(u64::from(jitter_ms))
}
