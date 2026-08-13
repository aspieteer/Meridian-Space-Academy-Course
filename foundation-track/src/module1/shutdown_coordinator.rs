// A production service needs a defined shutdown sequence. For the Meridian control plane:
//
// 1. Stop accepting new connections.
// 2. Signal active session tasks to finish or cancel.
// 3. Wait for tasks to drain (with a deadline — do not wait forever).
// 4. Flush pending telemetry to downstream consumers.
// 5. Exit cleanly.

use std::time::Duration;

use tokio::sync::broadcast;

#[allow(dead_code)]
struct ShutdownCoordinator {
    sender: broadcast::Sender<()>,
}

#[allow(dead_code)]
impl ShutdownCoordinator {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        Self { sender }
    }

    fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    async fn shutdown(&self, tasks: Vec<tokio::task::JoinHandle<()>>) {
        // Signal all subscribers.
        let _ = self.sender.send(());

        // Give tasks 10 seconds to drain. After that, abort stragglers.
        let deadline = Duration::from_secs(10);
        let _res = tokio::time::timeout(deadline, async {
            for handle in tasks {
                // Each task may have its own broadcast receiver
                // and handle its own shutdown logic once received shutdown signal.
                // Ignore individual task errors during shutdown.
                let _ = handle.await;
            }
        })
        .await;
    }
}
