use tokio::sync::broadcast;

/// Listens for the server shutdown signal.
///
/// Actually, this struct is used for sync and async examine for Handler.
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// the type for sync check at the outside while loop,
    /// it serves as an inspecting point when a series of handling job is done,
    /// avoiding dropping the Handler while it's still working.
    is_shutdown: bool,

    /// The receiver half of broadcast,
    /// serves as an async check inside the Handler working flow.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Self {
        Self {
            is_shutdown: false,
            notify,
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub(crate) async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        self.is_shutdown = true;
    }
}
