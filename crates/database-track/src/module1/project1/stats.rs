use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative counters for completed fetches and CLOCK replacement work.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub clock_second_chances: u64,
}

impl BufferPoolStats {
    pub fn hit_rate(&self) -> f64 {
        let fetches = self.hits + self.misses;
        if fetches == 0 {
            0.0
        } else {
            self.hits as f64 / fetches as f64
        }
    }
}

/// Cumulative counters, the atomic version.
#[derive(Debug, Default)]
pub struct BufferPoolAtomStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub clock_second_chances: AtomicU64,
}

impl BufferPoolAtomStats {
    /// A relaxed snapshot. Values may come from slightly different moments
    /// while other threads are updating the counters.
    pub fn snapshot(&self) -> BufferPoolStats {
        BufferPoolStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            clock_second_chances: self.clock_second_chances.load(Ordering::Relaxed),
        }
    }
}
