use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use crate::module1::{
    PAGE_SIZE,
    project1::{
        PageId, PageStore,
        error::BufferPoolError,
        stats::{BufferPoolAtomStats, BufferPoolStats},
    },
};

type FrameIdx = usize;

pub struct BufferPoolV3<S> {
    shared: Arc<Shared<S>>,
}

struct Shared<S> {
    store: S,

    frames: Vec<Arc<Frame>>,
    // Extend the page_table functionality, stores Entry state for each PageId respectively.
    shards: Vec<RwLock<HashMap<PageId, Entry>>>,

    clock_hand: AtomicUsize,

    stats: BufferPoolAtomStats,
}

struct Frame {
    state: Mutex<FrameState>,
    data: RwLock<Box<[u8; PAGE_SIZE]>>,

    pin_count: AtomicU32,
    referenced: AtomicBool,
    dirty: AtomicBool,
}

#[derive(Clone)]
enum FrameState {
    Empty,
    Valid(PageId),
    Evicting(Arc<Pending>),
}

enum Entry {
    Ready(FrameIdx),
    Loading(Arc<Pending>),
    Evicting(Arc<Pending>),
}

struct Victim {
    index: FrameIdx,
    old_page_id: Option<PageId>,
}

pub struct PageGuard {
    page_id: PageId,
    frame: Arc<Frame>,
}

#[derive(Clone, Copy)]
pub enum AccessMode {
    Normal,
    BulkRead,
}

// ===== Pending =====

struct Pending {
    done: Mutex<bool>,
    changed: Condvar,
}

impl Pending {
    fn new() -> Self {
        Self {
            done: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    fn wait(&self) {
        let done = self.done.lock().unwrap();
        drop(self.changed.wait_while(done, |done| !*done).unwrap());
    }

    fn finish(&self) {
        *self.done.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

// ===== impl BufferPoolV3 =====

impl<S: PageStore> BufferPoolV3<S> {
    pub fn new(capacity: usize, store: S) -> Self {
        assert!(capacity > 0);

        let shard_count = capacity.min(32).next_power_of_two();
        let shards = (0..shard_count)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();

        let frames = (0..capacity).map(|_| Arc::new(Frame::empty())).collect();

        Self {
            shared: Arc::new(Shared {
                store,
                frames,
                shards,
                clock_hand: AtomicUsize::new(0),
                stats: BufferPoolAtomStats::default(),
            }),
        }
    }

    pub fn stats(&self) -> BufferPoolStats {
        self.shared.stats.snapshot()
    }

    pub fn fetch_page(
        &self,
        page_id: PageId,
        mode: AccessMode,
    ) -> Result<PageGuard, BufferPoolError> {
        let shard_index = self.shard_index(&page_id);

        loop {
            // Pin a ready frame before releasing its shard's read lock.
            // Eviction needs the write lock for that same shard.
            let pending = {
                let table = self.shared.shards[shard_index].read().unwrap();

                match table.get(&page_id) {
                    Some(Entry::Ready(index)) => {
                        let frame = Arc::clone(&self.shared.frames[*index]);
                        frame.pin_count.fetch_add(1, Ordering::AcqRel);
                        self.record_access(&frame, mode);
                        // Hit stats
                        self.shared.stats.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(PageGuard { page_id, frame });
                    }
                    Some(Entry::Loading(pending) | Entry::Evicting(pending)) => {
                        Some(Arc::clone(pending))
                    }
                    None => None,
                }
            };

            if let Some(pending) = pending {
                pending.wait();
                continue;
            }

            // Claim this PageId before I/O. Another thread that misses
            // on the same page will wait for this load.
            let pending = Arc::new(Pending::new());
            let mut table = self.shared.shards[shard_index].write().unwrap();

            if table.contains_key(&page_id) {
                continue;
            }

            table.insert(page_id.clone(), Entry::Loading(Arc::clone(&pending)));
            drop(table);

            return self.load_claimed(page_id, mode, pending);
        }
    }

    fn load_claimed(
        &self,
        page_id: PageId,
        mode: AccessMode,
        pending: Arc<Pending>,
    ) -> Result<PageGuard, BufferPoolError> {
        let victim = loop {
            match self.choose_victim(&pending) {
                Ok(victim) => break victim,
                // A frame being loaded may become available shortly.
                Err(_) if self.wait_for_active_load() => continue,

                Err(e) => {
                    self.shared.shards[self.shard_index(&page_id)]
                        .write()
                        .unwrap()
                        .remove(&page_id);
                    pending.finish();
                    return Err(e);
                }
            }
        };

        let frame = Arc::clone(&self.shared.frames[victim.index]);
        if let Some(old_id) = victim.old_page_id.as_ref()
            && frame.dirty.load(Ordering::Acquire)
        {
            // PageStore::write_page takes a mutable slice. Keep the
            // frame's original bytes intact for error recovery.
            let mut copy = frame.data.read().unwrap().to_vec();

            if let Err(e) = self.shared.store.write_page(old_id, &mut copy) {
                self.restore_failed_load(&page_id, &victim, &pending);
                return Err(BufferPoolError::Io(e));
            }

            frame.dirty.store(false, Ordering::Release);
        }

        let mut new_data = Box::new([0u8; PAGE_SIZE]);

        if let Err(e) = self.shared.store.read_page(&page_id, &mut *new_data) {
            self.restore_failed_load(&page_id, &victim, &pending);
            return Err(BufferPoolError::Io(e));
        }

        // The reservation prevents guards from accessing this frame,
        // so data can be replaced without holding a table lock.
        *frame.data.write().unwrap() = new_data;
        frame.dirty.store(false, Ordering::Release);
        frame
            .referenced
            .store(matches!(mode, AccessMode::Normal), Ordering::Relaxed);

        // No thread can pin the old entry: it is marked Evicting.
        // Remove it before publishing the new Ready entry.
        let evicted = victim.old_page_id.is_some();
        if let Some(old_id) = victim.old_page_id {
            self.shared.shards[self.shard_index(&old_id)]
                .write()
                .unwrap()
                .remove(&old_id);
        }
        {
            let mut table = self.shared.shards[self.shard_index(&page_id)]
                .write()
                .unwrap();
            *frame.state.lock().unwrap() = FrameState::Valid(page_id.clone());
            table.insert(page_id.clone(), Entry::Ready(victim.index));
        }

        pending.finish();

        self.shared.stats.misses.fetch_add(1, Ordering::Relaxed);
        if evicted {
            self.shared.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }

        // The pin reserved by choose_victim now belongs to this guard.
        Ok(PageGuard { page_id, frame })
    }

    fn restore_failed_load(&self, new_id: &PageId, victim: &Victim, pending: &Arc<Pending>) {
        let frame = &self.shared.frames[victim.index];

        if let Some(old_id) = victim.old_page_id.as_ref() {
            let mut table = self.shared.shards[self.shard_index(old_id)]
                .write()
                .unwrap();
            *frame.state.lock().unwrap() = FrameState::Valid(old_id.clone());
            // The entry state of old_id mutated earlier,
            // so we need to catch it, check its state and change it back
            let entry = table
                .get_mut(old_id)
                .expect("evicting old page is missing from the table");
            debug_assert!(
                matches!(&*entry, Entry::Evicting(current) if Arc::ptr_eq(current, pending))
            );
            *entry = Entry::Ready(victim.index);
        } else {
            *frame.state.lock().unwrap() = FrameState::Empty;
        }

        frame.pin_count.fetch_sub(1, Ordering::AcqRel);

        self.shared.shards[self.shard_index(new_id)]
            .write()
            .unwrap()
            .remove(new_id);

        pending.finish();
    }

    fn choose_victim(&self, pending: &Arc<Pending>) -> Result<Victim, BufferPoolError> {
        let frame_count = self.shared.frames.len();

        // First sweep grants a referenced frame a second chance.
        // The second sweep may take an unpinned frame even if another
        // access set its reference bit again.
        for pass in 0..2 {
            for _ in 0..frame_count {
                let index = self.shared.clock_hand.fetch_add(1, Ordering::Relaxed) % frame_count;

                if let Some(victim) = self.try_reserve(index, pending, pass == 1) {
                    return Ok(victim);
                }
            }
        }

        Err(BufferPoolError::AllFramesPinned)
    }

    fn wait_for_active_load(&self) -> bool {
        let active =
            self.shared
                .frames
                .iter()
                .find_map(|frame| match &*frame.state.lock().unwrap() {
                    FrameState::Evicting(pending) => Some(Arc::clone(pending)),
                    _ => None,
                });

        if let Some(pending) = active {
            pending.wait();
            true
        } else {
            false
        }
    }

    fn try_reserve(
        &self,
        index: FrameIdx,
        pending: &Arc<Pending>,
        force_after_one_sweep: bool,
    ) -> Option<Victim> {
        let frame = &self.shared.frames[index];

        // This snapshot identifies the shard to lock. Recheck the
        // state after acquiring that shard lock.
        let snapshot = frame.state.lock().unwrap().clone();

        match snapshot {
            FrameState::Empty => {
                let mut state = frame.state.lock().unwrap();

                if !matches!(*state, FrameState::Empty)
                    || frame
                        .pin_count
                        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                {
                    return None;
                }

                if !force_after_one_sweep && frame.referenced.swap(false, Ordering::Relaxed) {
                    self.shared
                        .stats
                        .clock_second_chances
                        .fetch_add(1, Ordering::Relaxed);
                    frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                    return None;
                }

                *state = FrameState::Evicting(Arc::clone(pending));

                Some(Victim {
                    index,
                    old_page_id: None,
                })
            }
            FrameState::Valid(old_id) => {
                // A hit pins under this shard's read lock. Holding its
                // write lock makes the pin-count check and transition
                // to Evicting one coordinated operation.
                let mut table = self.shared.shards[self.shard_index(&old_id)]
                    .write()
                    .unwrap();
                let mut state = frame.state.lock().unwrap();

                if !matches!(&*state, FrameState::Valid(current) if current == &old_id)
                    || !matches!(table.get(&old_id), Some(Entry::Ready(current)) if *current == index)
                    || frame
                        .pin_count
                        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                {
                    return None;
                }

                if !force_after_one_sweep && frame.referenced.swap(false, Ordering::Relaxed) {
                    self.shared
                        .stats
                        .clock_second_chances
                        .fetch_add(1, Ordering::Relaxed);
                    frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                    return None;
                }

                *state = FrameState::Evicting(Arc::clone(pending));
                table.insert(old_id.clone(), Entry::Evicting(Arc::clone(pending)));

                Some(Victim {
                    index,
                    old_page_id: Some(old_id),
                })
            }
            FrameState::Evicting(_pending) => None,
        }
    }

    fn shard_index(&self, page_id: &PageId) -> usize {
        let mut hasher = DefaultHasher::new();
        page_id.hash(&mut hasher);
        hasher.finish() as usize & (self.shared.shards.len() - 1)
    }

    fn record_access(&self, frame: &Frame, mode: AccessMode) {
        if matches!(mode, AccessMode::Normal) {
            frame.referenced.store(true, Ordering::Relaxed);
        }
    }

    pub fn flush_all(&self) -> Result<(), BufferPoolError> {
        for frame in &self.shared.frames {
            loop {
                let action = {
                    let state = frame.state.lock().unwrap();

                    match &*state {
                        FrameState::Empty => None,
                        FrameState::Valid(page_id) => {
                            // Pin under the state lock so eviction cannot
                            // select this frame during the flush.
                            frame.pin_count.fetch_add(1, Ordering::AcqRel);
                            Some(Ok(page_id.clone()))
                        }
                        FrameState::Evicting(pending) => Some(Err(Arc::clone(pending))),
                    }
                };

                match action {
                    Some(Ok(page_id)) => {
                        // The guard releases the flush pin on every exit,
                        // including an I/O error.
                        let guard = PageGuard {
                            page_id,
                            frame: Arc::clone(frame),
                        };

                        if frame.dirty.load(Ordering::Acquire) {
                            let data = frame.data.read().unwrap();
                            let mut copy = data.to_vec();

                            self.shared
                                .store
                                .write_page(guard.page_id(), &mut copy)
                                .map_err(BufferPoolError::Io)?;

                            // Keep the data read lock until dirty is cleared.
                            // A concurrent writer waits, then marks dirty.
                            frame.dirty.store(false, Ordering::Release);
                        }

                        drop(guard);
                        break;
                    }
                    Some(Err(pending)) => pending.wait(),
                    None => break,
                }
            }
        }
        Ok(())
    }
}

impl<S> Clone for BufferPoolV3<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
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
            referenced: AtomicBool::new(false),
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
        let previous_pin_count = self.frame.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous_pin_count > 0);
    }
}
