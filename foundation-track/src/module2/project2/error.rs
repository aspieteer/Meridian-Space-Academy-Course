use std::fmt;

// ===== Error =====

pub struct QueueShutdown {
    _p: (),
}

impl fmt::Debug for QueueShutdown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueShutdown").finish()
    }
}

impl QueueShutdown {
    pub(crate) fn new() -> Self {
        Self { _p: () }
    }
}
