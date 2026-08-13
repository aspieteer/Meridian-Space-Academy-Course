use std::time::Duration;

use tokio::sync::mpsc;

async fn fetch_tle_update() -> Vec<u8> {
    // Simulate a slow catalog fetch — ~200ms in production.
    tokio::time::sleep(Duration::from_millis(200)).await;
    vec![0u8; 64] // placeholder TLE payload
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(8);

    // Spawn a sender to simulate incoming commands.
    tokio::spawn(async move {
        for cmd in ["REPOINT", "STATUS", "OPERATION", "RESET"] {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = tx.send(cmd.to_string()).await;
        }
    });

    // Create the future ONCE, outside the loop.
    let tle_fetch = fetch_tle_update();
    // NOTE: This is the key point of the example shown.
    // Pin it to the stack so we can poll it by reference (&mut tle_fetch).
    tokio::pin!(tle_fetch);

    let mut tle_done = false;

    loop {
        tokio::select! {
            // Poll the same future instance each iteration.
            // Without tokio::pin!, each iteration would call fetch_tle_update()
            // again, creating a brand-new future and discarding all progress.
            tle = &mut tle_fetch, if !tle_done => {
                println!("TLE update received: {} bytes", tle.len());
                tle_done = true;
            }
            Some(cmd) = rx.recv() => {
                println!("command received: {cmd}");
                if cmd == "RESET" {
                    break;
                }
            }
            else => break,
        }
    }
}
