use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default, Debug)]
pub struct Metrics {
    commands_pushed: AtomicU64,
    commands_dispatched: AtomicU64,
    safe_mode_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            commands_pushed: AtomicU64::new(0),
            commands_dispatched: AtomicU64::new(0),
            safe_mode_count: AtomicU64::new(0),
        })
    }

    pub(crate) fn increment_push(&self) {
        self.commands_pushed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_dispatch(&self) {
        self.commands_dispatched.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_safe_mode_count(&self) {
        self.safe_mode_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn load_pushed(&self) -> u64 {
        self.commands_pushed.load(Ordering::Relaxed)
    }
    pub fn load_dispatched(&self) -> u64 {
        self.commands_dispatched.load(Ordering::Relaxed)
    }
    pub fn load_safe_mode_count(&self) -> u64 {
        self.safe_mode_count.load(Ordering::Relaxed)
    }
}
