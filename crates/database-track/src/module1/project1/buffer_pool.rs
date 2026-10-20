use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering},
    },
};

use super::{PageId, PageStore, error::BufferPoolError};
use crate::module1::{MAX_USAGE_COUNT, PAGE_SIZE};

pub struct BufferPool<S> {
    shared: Arc<BufferPoolShared<S>>,
}

struct BufferPoolShared<S> {
    store: S,
    inner: Mutex<BufferPoolInner>,
}

struct BufferPoolInner {
    frames: Vec<Arc<Frame>>,
    page_table: HashMap<PageId, usize>,
    clock_hand: usize,
    stats: BufferPoolStats,
}

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

/// The content inside Frame is synonymous with Page,
/// but functions more within the BufferPool.
struct Frame {
    page_id: RwLock<Option<PageId>>,
    data: RwLock<Box<[u8; PAGE_SIZE]>>,

    pin_count: AtomicU32,
    usage_count: AtomicU8,
    dirty: AtomicBool,
}

pub struct PageGuard {
    page_id: PageId,
    frame: Arc<Frame>,
}

#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    Normal,
    BulkRead,
}

// ===== impl BufferPool =====

impl<S: PageStore> BufferPool<S> {
    pub fn new(capacity: usize, store: S) -> Self {
        Self {
            shared: Arc::new(BufferPoolShared::new(capacity, store)),
        }
    }

    pub fn stats(&self) -> BufferPoolStats {
        self.shared.inner.lock().unwrap().stats
    }

    pub fn fetch_page(
        &self,
        page_id: PageId,
        mode: AccessMode,
    ) -> Result<PageGuard, BufferPoolError> {
        let mut inner_guard = self.shared.inner.lock().unwrap();

        // 1. Buffer hit
        if let Some(&frame_idx) = inner_guard.page_table.get(&page_id) {
            let frame = Arc::clone(&inner_guard.frames[frame_idx]);
            inner_guard.stats.hits += 1;

            frame.pin_count.fetch_add(1, Ordering::AcqRel);

            self.record_access(&frame, mode);

            return Ok(PageGuard { page_id, frame });
        }

        // 2. Buffer miss: choose frame by clock_hand
        let victim_index = Self::choose_victim(&mut inner_guard)?;
        let victim = Arc::clone(&inner_guard.frames[victim_index]);

        let old_page_id = victim.page_id.read().unwrap().clone();

        // 3. Write page back when victim is dirty
        if let Some(id) = old_page_id.clone()
            && victim.dirty.load(Ordering::Acquire)
        {
            let data = victim.data.read().unwrap();
            let slice = &mut *data.to_vec();

            if let Err(e) = self.shared.store.write_page(&id, slice) {
                victim.pin_count.store(0, Ordering::Release);
                return Err(BufferPoolError::Io(e));
            }

            victim.dirty.store(false, Ordering::Release);
        }

        // 4. Read the page from stored disk into new_data temporary buffer first
        let mut new_data = Box::new([0u8; PAGE_SIZE]);

        if let Err(e) = self.shared.store.read_page(&page_id, &mut *new_data) {
            victim.pin_count.store(0, Ordering::Release);
            return Err(BufferPoolError::Io(e));
        }

        // 5. If the disk read succeeded, replace the victim by new data
        let evicted = old_page_id.is_some();
        if let Some(id) = old_page_id {
            inner_guard.page_table.remove(&id);
        }

        {
            let mut victim_data = victim.data.write().unwrap();
            *victim_data = new_data;
        }

        {
            let mut current_page_id = victim.page_id.write().unwrap();
            *current_page_id = Some(page_id.clone());
        }

        victim.dirty.store(false, Ordering::Release);
        victim.usage_count.store(1, Ordering::Release);

        inner_guard.page_table.insert(page_id.clone(), victim_index);
        inner_guard.stats.misses += 1;
        if evicted {
            inner_guard.stats.evictions += 1;
        }

        Ok(PageGuard {
            page_id,
            frame: victim,
        })
    }

    fn record_access(&self, frame: &Frame, mode: AccessMode) {
        match mode {
            AccessMode::Normal => {
                let _ =
                    frame
                        .usage_count
                        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                            Some(current.saturating_add(1).min(MAX_USAGE_COUNT))
                        });
            }
            AccessMode::BulkRead => {
                let _ =
                    frame
                        .usage_count
                        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                            Some(current.max(1))
                        });
            }
        }
    }

    fn choose_victim(
        inner_guard: &mut MutexGuard<'_, BufferPoolInner>,
    ) -> Result<usize, BufferPoolError> {
        let frame_count = inner_guard.frames.len();

        for _ in 0..=MAX_USAGE_COUNT {
            let mut found_unpinned = false;

            for _ in 0..frame_count {
                let index = inner_guard.clock_hand;
                inner_guard.clock_hand = (inner_guard.clock_hand + 1) % frame_count;

                let frame = &inner_guard.frames[index];

                if frame.pin_count.load(Ordering::Acquire) != 0 {
                    continue;
                }

                found_unpinned = true;

                let usage = frame.usage_count.load(Ordering::Acquire);
                if usage == 0 {
                    frame.pin_count.store(1, Ordering::Release);
                    return Ok(index);
                }

                frame.usage_count.fetch_sub(1, Ordering::AcqRel);
                inner_guard.stats.clock_second_chances += 1;
            }

            if !found_unpinned {
                return Err(BufferPoolError::AllFramesPinned);
            }
        }

        unreachable!("usage count is bounded, whether a victim is found or all frames are pinned.");
    }

    pub fn flush_all(&self) -> Result<(), BufferPoolError> {
        let inner_guard = self.shared.inner.lock().unwrap();

        for frame in &inner_guard.frames {
            let page_id = frame.page_id.read().unwrap();

            let Some(pid) = (*page_id).clone() else {
                continue;
            };

            if !frame.dirty.load(Ordering::Acquire) {
                continue;
            }

            let data = frame.data.read().unwrap();
            let slice = &mut *data.to_vec();

            self.shared
                .store
                .write_page(&pid, slice)
                .map_err(BufferPoolError::Io)?;

            // Unmark dirty while still having the reader lock,
            // to avoid contention of new writer lock.
            frame.dirty.store(false, Ordering::Release);
        }

        Ok(())
    }
}

impl<S> Clone for BufferPool<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

// ===== impl BufferPoolShared =====

impl<S: PageStore> BufferPoolShared<S> {
    fn new(capacity: usize, store: S) -> Self {
        assert!(capacity > 0);

        let frames = (0..capacity).map(|_| Arc::new(Frame::empty())).collect();

        Self {
            store,
            inner: Mutex::new(BufferPoolInner {
                frames,
                page_table: HashMap::with_capacity(capacity),
                clock_hand: 0,
                stats: BufferPoolStats::default(),
            }),
        }
    }
}

// ===== impl Frame =====

impl Frame {
    fn empty() -> Self {
        Self {
            page_id: RwLock::new(None),
            data: RwLock::new(Box::new([0u8; PAGE_SIZE])),
            pin_count: AtomicU32::new(0),
            usage_count: AtomicU8::new(0),
            dirty: AtomicBool::new(false),
        }
    }
}

// ===== impl PageGuard =====

impl PageGuard {
    pub fn page_id(&self) -> &PageId {
        &self.page_id
    }

    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; PAGE_SIZE]) -> R,
    {
        let data = self.frame.data.read().unwrap();
        f(&data)
    }

    pub fn write<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    {
        let mut data = self.frame.data.write().unwrap();

        // Must set dirty after gained the writer lock.
        self.frame.dirty.store(true, Ordering::Release);

        f(&mut data)
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        let previous = self.frame.pin_count.fetch_sub(1, Ordering::AcqRel);

        debug_assert!(previous > 0);
    }
}
