// [`tokio::select`] works for racing futures
//
// [`tokio::select!`] polls multiple futures concurrently and completes with
// the first one that becomes ready, cancelling the others. It is the right tool for:
//
// - Racing a task against a timeout
// - Racing a task against a shutdown signal
// - Implementing priority receive patterns on multiple channels

use tokio::{
    sync::{mpsc, oneshot},
    time::Duration,
};

async fn session_with_shutdown(
    session: impl Future<Output = ()>,
    mut shutdown: oneshot::Receiver<()>,
) {
    tokio::select! {
        _ = session => {
            tracing::info!("session completed normally");
        }
        _ = &mut shutdown => {
            // Shutdown signal received — session future is cancelled here.
            // RAII cleanup in the session's Drop runs.
            tracing::info!("session cancelled: shutdown signal received");
        }
    }
}

// [`tokio::select!`] is most often used inside a loop. Two patterns come up constantly in production systems.
//
// Multi-channel drain with `else`: when a session task needs to drain
// from multiple upstream channels until all are closed:

fn process_frame(frame: Vec<u8>, source: &str) {
    let s = String::from_utf8(frame);
    match s {
        Ok(s) => {
            println!("From [{}] - {}", source, s);
            tracing::debug!(bytes = s.len(), source, "frame processed");
        }
        Err(e) => eprintln!("failed to convert u8 vector to string: {}", e),
    }
}

async fn drain_uplinks(
    mut primary: mpsc::Receiver<Vec<u8>>,
    mut redundant: mpsc::Receiver<Vec<u8>>,
) {
    loop {
        tokio::select! {
            // select! randomly picks which ready branch to check first —
            // this prevents the redundant channel from always being starved
            // if the primary is consistently busy.
            Some(frame) = primary.recv() => {
                process_frame(frame, "primary");
            }
            Some(frame) = redundant.recv() => {
                process_frame(frame, "redundant");
            }
            // else fires when ALL patterns fail — both channels returned None,
            // meaning both are closed. This is the clean exit condition.
            else => {
                tracing::info!("all uplink channels closed - drain complete");
                break;
            }
        }
    }
}

// Branch preconditions: the `, if condition` syntax disables a branch before [`tokio::select!`] evaluates it.
// This is essential when polling a pinned future by reference inside a loop —
// once the future completes, the branch must be disabled or
// the next iteration will attempt to poll an already-resolved future, causing a panic:

async fn catalog_refresh() -> Vec<u8> {
    tokio::time::sleep(Duration::from_millis(100)).await;
    vec![0u8; 128]
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<String>(8);

    let refresher = catalog_refresh();
    let mut refresher = std::pin::pin!(refresher);
    let mut refresh_done = false;

    for _ in 0..5 {
        tokio::select! {
            // Branch is disabled once refresh_done = true.
            // Without this precondition: panic on second iteration.
            result = &mut refresher, if !refresh_done => {
                println!("catalog refreshed: {} bytes", result.len());
                refresh_done = true;
            }
            Some(cmd) = rx.recv() => {
                println!("command: {cmd}");
            }
            else => break,
        }
    }
}
