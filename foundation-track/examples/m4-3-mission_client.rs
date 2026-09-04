use std::{sync::Arc, time::Duration};

use foundation_track::module4::reqwest_client::{ConjunctionReport, MissionApiClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let api_client = MissionApiClient::new(
        "https://api.meridian.internal".to_string(),
        "mission-control-key".to_string(),
    )?;

    // Periodic TLE refresh loop.
    let api_ref = Arc::new(api_client);
    let refresh_api = Arc::clone(&api_ref);

    tokio::spawn(async move {
        loop {
            match refresh_api.get_tle(25544).await {
                Ok(tle) => tracing::info!(name = %tle.get_name(), "TLE refreshed"),
                Err(e) => tracing::error!("TLE refresh failed: {e}"),
            }
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
    });

    // Post a conjunction report.
    api_ref
        .post_conjunction(&ConjunctionReport::new(
            25544,
            48274,
            1_735_000_000.0,
            0.8,
            0.003,
        ))
        .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}
