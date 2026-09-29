use std::{
    alloc::{GlobalAlloc, System},
    sync::atomic::{AtomicU64, Ordering},
};

/// Wraps the system allocator and counts every alloc/dealloc.
#[derive(Default)]
pub struct CountingAllocator {
    pub(crate) alloc_count: AtomicU64,
    pub(crate) dealloc_count: AtomicU64,
    pub(crate) alloc_bytes: AtomicU64,
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        self.alloc_bytes
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
}

impl CountingAllocator {
    pub const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            alloc_bytes: AtomicU64::new(0),
        }
    }

    pub fn alloc_count(&self) -> u64 {
        self.alloc_count.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.alloc_count.load(Ordering::Relaxed),
            self.dealloc_count.load(Ordering::Relaxed),
            self.alloc_bytes.load(Ordering::Relaxed),
        )
    }

    pub fn reset_counters(&self) {
        self.alloc_count.store(0, Ordering::Relaxed);
        self.dealloc_count.store(0, Ordering::Relaxed);
        self.alloc_bytes.store(0, Ordering::Relaxed);
    }
}
