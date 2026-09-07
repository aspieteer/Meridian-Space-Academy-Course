use foundation_track::module4::project4::gs_client::{ClientConfig, ClientGuard};
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    // TODO:
    // Overall Config for the Client,
    // can be imported from env.
    const ADDR: &str = "127.0.0.1:9100";
    const API_BASE_URL: &str = "http://127.0.0.1:9101";
    const TLE_REFRESH_INTERVAL_SECS: u32 = 30;
    let station_id = "25544";
    let norad_id = 0u32;
    let overall_timeout_secs = 15;
    let conn_timeout_secs = 3;

    let (sys_shutdown_tx, sys_shutdown_rx) = watch::channel(false);

    let config = ClientConfig::new(
        station_id,
        ADDR,
        norad_id,
        API_BASE_URL,
        TLE_REFRESH_INTERVAL_SECS,
    );

    let gs_client = ClientGuard::new(
        &config,
        overall_timeout_secs,
        conn_timeout_secs,
        sys_shutdown_rx.clone(),
    );

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = sys_shutdown_tx.send(true);
        }
    });

    gs_client.run(sys_shutdown_rx).await;
}
