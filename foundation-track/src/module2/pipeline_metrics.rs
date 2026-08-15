use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub struct PipelineMetrics {
    frames_received: AtomicU64,
    frames_dropped: AtomicU64,
    bytes_processed: AtomicU64,
    // Shutdown flag: Release on write, Acquire on read.
    shutdown: AtomicBool,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            frames_received: AtomicU64::new(0),
            frames_dropped: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        })
    }

    pub fn record_frame(&self, bytes: u64) {
        // Relaxed: these counters are for monitoring only.
        // The exact ordering relative to other threads' stores doesn't matter;
        // we only care about the eventual totals.
        self.frames_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_drop(&self) {
        self.frames_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn signal_shutdown(&self) {
        // Release: ensures all frame counts written before this are visible
        // to any thread that reads shutdown with Acquire.
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn should_stop(&self) -> bool {
        // Acquire: establishes happens-before with the Release store above.
        // Any Relaxed loads on frames_received etc. after this call
        // will see all stores from before signal_shutdown().
        self.shutdown.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.frames_received.load(Ordering::Relaxed),
            self.frames_dropped.load(Ordering::Relaxed),
            self.bytes_processed.load(Ordering::Relaxed),
        )
    }
}
