use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering},
    },
};

use super::{PageId, PageStore, error::BufferPoolError};
use crate::module1::{MAX_USAGE_COUNT, PAGE_SIZE};

type FrameIdx = usize;

pub struct BufferPoolV2<S> {
    shared: Arc<BufferPoolV2Shared<S>>,
}

struct BufferPoolV2Shared<S> {
    // Storage that is responsible for read/write pages from/to file system.
    store: S,

    frames: Vec<Arc<Frame>>,

    page_table: RwLock<HashMap<PageId, FrameIdx>>,

    clock_hand: AtomicUsize,
}

struct Frame {
    state: Mutex<FrameState>,
    data: RwLock<Box<[u8; PAGE_SIZE]>>,

    pin_count: AtomicU32,
    usage_count: AtomicU8,
    dirty: AtomicBool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FrameState {
    Empty,
    Valid(PageId),
    Evicting,
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

// ===== impl BufferPoolV2 =====

impl<S: PageStore> BufferPoolV2<S> {
    pub fn new(capacity: usize, store: S) -> Self {
        Self {
            shared: Arc::new(BufferPoolV2Shared::new(capacity, store)),
        }
    }

    pub fn fetch_page(
        &self,
        page_id: PageId,
        mode: AccessMode,
    ) -> Result<PageGuard, BufferPoolError> {
        // Pin while holding the table read lock. An evictor cannot acquire
        // the table write lock between finding the mapping and adding the pin.
        {
            // 1. Buffer hit
            let table_read_lock = self.shared.page_table.read().unwrap();
            if let Some(&frame_idx) = table_read_lock.get(&page_id) {
                return Ok(self.pin_hit(frame_idx, page_id, mode));
            }
        }

        // Recheck after acquiring the write lock: another thread may have
        // loaded this page after our first lookup.
        let mut table_write_lock = self.shared.page_table.write().unwrap();
        if let Some(&frame_idx) = table_write_lock.get(&page_id) {
            return Ok(self.pin_hit(frame_idx, page_id, mode));
        }

        // Keep the write lock through replacement. This prevents a new hit
        // from pinning the chosen victim or seeing an incomplete mapping.
        // 2. Buffer miss
        let victim_index = self.choose_victim()?;
        let victim = Arc::clone(&self.shared.frames[victim_index]);

        let mut victim_state = victim.state.lock().unwrap();
        let previous_state = (*victim_state).clone();

        let old_page_id = match &previous_state {
            FrameState::Empty => None,
            FrameState::Valid(page_id) => Some(page_id.clone()),
            FrameState::Evicting => unreachable!("reserved frame is already evicting"),
        };

        // Do evicting operation
        *victim_state = FrameState::Evicting;

        // 3. Write page back when victim is dirty
        if let Some(old_id) = old_page_id.as_ref()
            && victim.dirty.load(Ordering::Acquire)
        {
            // PageStore::write_page takes a mutable slice, so retain the copy.
            let write_result = {
                let data = victim.data.read().unwrap();
                let mut copy = data.to_vec();
                self.shared.store.write_page(old_id, &mut copy)
            };

            if let Err(e) = write_result {
                *victim_state = previous_state;
                victim.pin_count.fetch_sub(1, Ordering::AcqRel);
                return Err(BufferPoolError::Io(e));
            }

            victim.dirty.store(false, Ordering::Release);
        }

        // 4. Read the page from stored disk into new_data temporary buffer first
        let mut new_data = Box::new([0u8; PAGE_SIZE]);

        // Keep the old frame data intact if the new page cannot be read.
        if let Err(e) = self.shared.store.read_page(&page_id, &mut *new_data) {
            *victim_state = previous_state;
            victim.pin_count.fetch_sub(1, Ordering::AcqRel);
            return Err(BufferPoolError::Io(e));
        }

        // 5. If the disk read above succeeded, replace the victim by new data
        *victim.data.write().unwrap() = new_data;
        // Change victim frame's flags
        victim.dirty.store(false, Ordering::Release);
        victim.usage_count.store(1, Ordering::Release);

        *victim_state = FrameState::Valid(page_id.clone());

        if let Some(old_id) = old_page_id {
            table_write_lock.remove(&old_id);
        }
        table_write_lock.insert(page_id.clone(), victim_index);

        drop(victim_state);

        // The reservation from choose_victim is now owned by this guard.
        Ok(PageGuard {
            page_id,
            frame: victim,
        })
    }

    fn pin_hit(&self, index: FrameIdx, page_id: PageId, mode: AccessMode) -> PageGuard {
        // Caller must hold a page_table read or write lock.
        let frame = Arc::clone(&self.shared.frames[index]);
        frame.pin_count.fetch_add(1, Ordering::AcqRel);
        self.record_access(&frame, mode);

        PageGuard { page_id, frame }
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

    fn choose_victim(&self) -> Result<FrameIdx, BufferPoolError> {
        let frame_count = self.shared.frames.len();

        for _ in 0..=MAX_USAGE_COUNT {
            let mut found_unpinned = false;

            for _ in 0..frame_count {
                let index = self.shared.clock_hand.update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| (current + 1) % frame_count,
                );

                let frame = Arc::clone(&self.shared.frames[index]);
                let reserved = frame
                    .pin_count
                    .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();

                if !reserved {
                    // Get Pinned or Reserved already by other threads
                    continue;
                }

                found_unpinned = true;

                let usage = frame.usage_count.load(Ordering::Acquire);
                if usage == 0 {
                    return Ok(index);
                }

                frame.usage_count.fetch_sub(1, Ordering::AcqRel);
                // If usage > 0, decrement the pin_count to give the clock scan a "second" chance.
                frame.pin_count.fetch_sub(1, Ordering::AcqRel);
            }

            if !found_unpinned {
                return Err(BufferPoolError::AllFramesPinned);
            }
        }

        unreachable!("usage count is bounded, whether a victim is found or all frames are pinned.");
    }

    pub fn flush_all(&self) -> Result<(), BufferPoolError> {
        // Gain a table read lock at the start, before its frame loop.
        // That gives it the same lock order and stops replacement midway through a flush.
        let _table_read_lock = self.shared.page_table.read().unwrap();

        for frame in &self.shared.frames {
            let state = frame.state.lock().unwrap();
            let page_id = match &*state {
                FrameState::Valid(page_id) => Some(page_id.clone()),
                _ => None,
            };
            let Some(page_id) = page_id else {
                continue;
            };

            if !frame.dirty.load(Ordering::Acquire) {
                continue;
            }

            let data = frame.data.read().unwrap();
            let slice = &mut *data.to_vec();

            self.shared
                .store
                .write_page(&page_id, slice)
                .map_err(BufferPoolError::Io)?;

            // Unmark dirty while still having the reader lock,
            // to avoid contention of new writer lock.
            frame.dirty.store(false, Ordering::Release);
        }

        Ok(())
    }
}

impl<S> Clone for BufferPoolV2<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

// ===== impl BufferPoolV2Shared =====

impl<S: PageStore> BufferPoolV2Shared<S> {
    fn new(capacity: usize, store: S) -> Self {
        assert!(capacity > 0);

        let frames = (0..capacity).map(|_| Arc::new(Frame::empty())).collect();

        Self {
            store,
            frames,
            page_table: RwLock::new(HashMap::with_capacity(capacity)),
            clock_hand: AtomicUsize::new(0),
        }
    }
}

// ===== impl Frame =====

impl Frame {
    fn empty() -> Self {
        Self {
            state: Mutex::new(FrameState::Empty),
            data: RwLock::new(Box::new([0u8; PAGE_SIZE])),
            pin_count: AtomicU32::new(0),
            usage_count: AtomicU8::new(0),
            dirty: AtomicBool::new(false),
        }
    }
}

// ===== impl PageGuard =====
// page_id: PageId, frame: Arc<Frame>

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

    pub fn write<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    {
        let mut data = self.frame.data.write().unwrap();
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
