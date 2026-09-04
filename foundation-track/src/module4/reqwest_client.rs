use std::time::Duration;

use anyhow::Context;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

// ===== Build Client =====

pub struct MissionApiClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl MissionApiClient {
    pub fn new(base_url: String, api_key: String) -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(4)
            .user_agent("meridian-control-plane/1.0")
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            client,
            base_url,
            api_key,
        })
    }

    /// Fetch a single TLE record with up to 3 retry attempts.
    pub async fn get_tle(&self, norad_id: u32) -> anyhow::Result<TleRecord> {
        let url = format!("{}/tle/{norad_id}", self.base_url);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let response = self
                .client
                .get(&url)
                .header("X-API-Key", &self.api_key)
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    return resp
                        .json::<TleRecord>()
                        .await
                        .context("failed to parse TLE response");
                }
                Ok(resp) if resp.status().is_server_error() && attempt < 3 => {
                    tracing::warn!(norad_id, attempt, status = %resp.status(), "retrying");
                    tokio::time::sleep(backoff_delay(attempt)).await;
                }
                Ok(resp) => {
                    anyhow::bail!("TLE fetch failed: HTTP {}", resp.status());
                }
                Err(e) if (e.is_connect() || e.is_timeout()) && attempt < 3 => {
                    tracing::warn!(norad_id, attempt, "network error: {e}, retrying");
                    tokio::time::sleep(backoff_delay(attempt)).await;
                }
                Err(e) => return Err(e).context("TLE fetch network error"),
            }
        }
    }

    /// Post a conjunction report to the mission operations endpoint.
    pub async fn post_conjunction(&self, report: &ConjunctionReport) -> anyhow::Result<()> {
        let _resp = self
            .client
            .post(format!("{}/conjunctions", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(report)
            .send()
            .await
            .context("failed to send conjunction report")?
            .error_for_status()
            .context("conjunction report rejected")?;

        Ok(())
    }

    /// Fetch all active TLEs in a specified altitude band (batch request).
    pub async fn get_tle_batch(&self, min_km: u32, max_km: u32) -> anyhow::Result<Vec<TleRecord>> {
        self.client
            .get(format!("{}/tle/batch", self.base_url))
            .header("X-API-Key", &self.api_key)
            .query(&[("min_alt_km", min_km), ("max_alt_km", max_km)])
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<TleRecord>>()
            .await
            .context("failed to parse TLE batch response")
    }
}

// ===== Example Code below =====

#[allow(dead_code)]
pub fn build_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        // Overall request timeout: connection + headers + body. (all phases)
        .timeout(Duration::from_secs(30))
        // How long to wait for the TCP connection to establish.
        .connect_timeout(Duration::from_secs(5))
        // Keep connections alive for reuse — avoids TCP handshake per request.
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(10)
        // User-Agent header for all requests.
        .user_agent("meridian-control-plane/1.0")
        .build()?)
}

// ===== GET =====

#[derive(Clone, Debug, Deserialize)]
pub struct TleRecord {
    norad_id: u32,
    name: String,
    line1: String,
    line2: String,
    epoch: String,
}

impl TleRecord {
    pub fn get_name(&self) -> &str {
        &self.name
    }
}

pub async fn fetch_tle(client: &Client, norad_id: u32) -> anyhow::Result<TleRecord> {
    let url = format!("https://api.meridian.internal/tle/{norad_id}");
    let response = client
        .get(&url)
        .header("X-API-Key", "mission-control-key")
        .send()
        .await?;

    // error_for_status() converts 4xx/5xx responses into Err.
    // Without this, a 404 or 500 is not an error — you receive the body.
    let response = response.error_for_status()?;

    let record = response.json().await?;

    Ok(record)
}

// ===== POST =====

#[derive(Debug, Serialize)]
pub struct ConjunctionReport {
    obj_a_id: u32,
    obj_b_id: u32,
    tca_unix: f64,
    miss_distance_km: f64,
    probability: f64,
}

impl ConjunctionReport {
    pub const fn new(
        obj_a_id: u32,
        obj_b_id: u32,
        tca_unix: f64,
        miss_distance_km: f64,
        probability: f64,
    ) -> Self {
        Self {
            obj_a_id,
            obj_b_id,
            tca_unix,
            miss_distance_km,
            probability,
        }
    }
}

pub async fn post_alert(client: &Client, alert: &ConjunctionReport) -> anyhow::Result<()> {
    client
        .post("https://api.meridian.internal/alerts")
        .json(alert)
        // For large payloads that should be streamed rather than buffered in memory, use
        // .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

// ===== RETRY =====

pub async fn fetch_with_retry(
    client: &Client,
    url: &str,
    max_attempts: u32,
) -> anyhow::Result<String> {
    let mut attempt = 0_u32;

    loop {
        attempt += 1;
        let result = client.get(url).send().await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                return Ok(resp.text().await?);
            }
            Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS => {
                // Respect Retry-After header if present, otherwise backoff.
                let retry_after = resp
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let delay = if retry_after > 0 {
                    Duration::from_secs(retry_after)
                } else {
                    backoff_delay(attempt)
                };
                // log
                tracing::warn!(attempt, url, ?delay, "rate limited - backing off");
                if attempt >= max_attempts {
                    anyhow::bail!("rate limit exhausted");
                }
                // sleep for some delayed time
                tokio::time::sleep(delay).await;
            }
            Ok(resp) if resp.status().is_server_error() => {
                tracing::warn!(attempt, url, status = %resp.status(), "server error");
                if attempt >= max_attempts {
                    anyhow::bail!("server error after {max_attempts} attempts");
                }
                // retry after sleep time elapsed
                tokio::time::sleep(backoff_delay(attempt)).await;
            }
            Ok(resp) => {
                // 4xx client errors (except 429) are not retryable.
                anyhow::bail!("request failed: HTTP {}", resp.status());
            }
            Err(e) if e.is_connect() || e.is_timeout() => {
                tracing::warn!(attempt, url, "network error: {e}");
                if attempt >= max_attempts {
                    return Err(e.into());
                }

                tokio::time::sleep(backoff_delay(attempt)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn backoff_delay(attempt: u32) -> Duration {
    // Exponential backoff: 1s, 2s, 4s, 8s, capped at 30s.
    // Add jitter to avoid thundering herd.
    use std::time::SystemTime;

    let base = Duration::from_secs(1u64 << attempt.min(3));
    let jitter_ms = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis())
        % 1000;

    base + Duration::from_millis(jitter_ms as u64)
}
