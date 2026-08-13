use std::sync::LazyLock;

use tokio::runtime::{Builder, Runtime};
use tracing_subscriber::FmtSubscriber;

// Ingress runtime: tuned for concurrent I/O — one worker per core,
// minimal blocking threads since real blocking work routes to the
// housekeeping runtime.
static INGRESS_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .max_blocking_threads(4)
        .thread_name("meridian-ingress")
        .thread_stack_size(2 * 1024 * 1024)
        .on_thread_start(|| tracing::debug!("ingress worker started"))
        .enable_all()
        .build()
        .expect("failed to build ingress runtime")
});

// Housekeeping runtime: fewer workers, more blocking threads for
// catalog refreshes and frame archival.
static HOUSEKEEPING_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(32)
        .thread_name("meridian-housekeeping")
        .enable_all()
        .build()
        .expect("failed to build housekeeping runtime")
});

async fn handle_uplink_session() {
    // This runs on an ingress worker thread.
    // Long-running I/O awaits are fine here.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    tracing::info!("uplink session processed");
}

async fn refresh_tle_catalog() {
    // CPU + blocking I/O — route to spawn_blocking so we do not
    // park an ingress worker for the duration of the refresh.
    tokio::task::spawn_blocking(|| {
        // Synchronous HTTP fetch + file write; blocks for ~200ms.
        tracing::info!("TLE catalog refreshed");
    })
    .await
    .expect("TLE refresh panicked");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_thread_names(true)
        .with_thread_ids(true)
        .pretty()
        .with_max_level(tracing::Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // Ingress and housekeeping run in separate thread pools.
    // A TLE refresh spike cannot starve active uplink sessions.
    std::thread::spawn(move || {
        HOUSEKEEPING_RUNTIME.block_on(async {
            loop {
                refresh_tle_catalog().await;
                tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            }
        });
    });

    INGRESS_RUNTIME.block_on(async {
        // In production: bind TCP listener, accept connections,
        // spawn handle_uplink_session per connection.
        for _ in 0..48 {
            INGRESS_RUNTIME.spawn(handle_uplink_session());
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    });

    Ok(())
}
