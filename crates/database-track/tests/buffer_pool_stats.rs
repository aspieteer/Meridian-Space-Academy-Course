use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use database_track::module1::project1::{
    PAGE_SIZE, PageId, PageStore,
    buffer_pool_v2::{AccessMode as V2Mode, BufferPoolV2},
    buffer_pool_v3::{AccessMode as V3Mode, BufferPoolV3},
};

#[derive(Clone)]
struct MemoryStore {
    shared: Arc<StoreState>,
}

struct StoreState {
    pages: Mutex<HashMap<PageId, Vec<u8>>>,
    fail_next_read: Mutex<Option<PageId>>,
    reads: AtomicUsize,
    delay: Duration,
}

impl MemoryStore {
    fn new(delay: Duration) -> Self {
        Self {
            shared: Arc::new(StoreState {
                pages: Mutex::new(HashMap::new()),
                fail_next_read: Mutex::new(None),
                reads: AtomicUsize::new(0),
                delay,
            }),
        }
    }

    fn fail_once_on(&self, page_id: PageId) {
        *self.shared.fail_next_read.lock().unwrap() = Some(page_id);
    }
}

impl PageStore for MemoryStore {
    fn read_page(&self, page_id: &PageId, dest: &mut [u8]) -> io::Result<()> {
        let should_fail = {
            let mut next = self.shared.fail_next_read.lock().unwrap();
            if next.as_ref() == Some(page_id) {
                next.take();
                true
            } else {
                false
            }
        };
        if should_fail {
            return Err(io::Error::other("injected read error"));
        }

        self.shared.reads.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.shared.delay);
        if let Some(data) = self.shared.pages.lock().unwrap().get(page_id) {
            dest.copy_from_slice(data);
        } else {
            assert_eq!(dest.len(), PAGE_SIZE);
            dest.fill(page_id.page_no() as u8);
        }
        Ok(())
    }

    fn write_page(&self, page_id: &PageId, src: &mut [u8]) -> io::Result<()> {
        self.shared
            .pages
            .lock()
            .unwrap()
            .insert(page_id.clone(), src.to_vec());
        Ok(())
    }
}

#[test]
fn v2_counts_completed_fetches_evictions_and_second_chances() {
    let store = MemoryStore::new(Duration::ZERO);
    let pool = BufferPoolV2::new(1, store.clone());
    let a = PageId::new(1, 1);
    let b = PageId::new(1, 2);
    let c = PageId::new(1, 3);

    assert_eq!(pool.stats().hits + pool.stats().misses, 0);
    pool.fetch_page(a.clone(), V2Mode::Normal).unwrap();
    assert_eq!(pool.stats().misses, 1);
    assert_eq!(pool.stats().evictions, 0);

    pool.fetch_page(a, V2Mode::Normal).unwrap();
    assert_eq!(pool.stats().hits, 1);

    pool.fetch_page(b.clone(), V2Mode::Normal).unwrap();
    let before_error = pool.stats();
    assert_eq!(before_error.misses, 2);
    assert_eq!(before_error.evictions, 1);
    assert_eq!(before_error.clock_second_chances, 2);

    store.fail_once_on(c.clone());
    assert!(pool.fetch_page(c, V2Mode::Normal).is_err());
    assert_eq!(pool.stats().misses, before_error.misses);
    assert_eq!(pool.stats().evictions, before_error.evictions);
    assert_eq!(
        pool.fetch_page(b, V2Mode::Normal).unwrap().read(|d| d[0]),
        2
    );
}

#[test]
fn v3_counts_each_cleared_reference_bit() {
    let pool = BufferPoolV3::new(2, MemoryStore::new(Duration::ZERO));
    pool.fetch_page(PageId::new(1, 1), V3Mode::Normal).unwrap();
    pool.fetch_page(PageId::new(1, 2), V3Mode::Normal).unwrap();

    pool.fetch_page(PageId::new(1, 3), V3Mode::Normal).unwrap();
    let stats = pool.stats();
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.clock_second_chances, 2);
}

#[test]
fn v3_counts_completed_fetches_and_flushes_writes() {
    let store = MemoryStore::new(Duration::ZERO);
    let pool = BufferPoolV3::new(1, store.clone());
    let a = PageId::new(1, 1);
    let b = PageId::new(1, 2);
    let c = PageId::new(1, 3);

    let mut first = pool.fetch_page(a.clone(), V3Mode::Normal).unwrap();
    first.write(|data| data[0] = 99);
    drop(first);
    pool.flush_all().unwrap();
    assert_eq!(store.shared.pages.lock().unwrap().get(&a).unwrap()[0], 99);

    pool.fetch_page(a.clone(), V3Mode::Normal).unwrap();
    pool.fetch_page(b, V3Mode::Normal).unwrap();
    let before_error = pool.stats();
    assert_eq!(before_error.hits, 1);
    assert_eq!(before_error.misses, 2);
    assert_eq!(before_error.evictions, 1);
    assert_eq!(before_error.clock_second_chances, 1);

    store.fail_once_on(c.clone());
    assert!(pool.fetch_page(c, V3Mode::Normal).is_err());
    assert_eq!(pool.stats().misses, before_error.misses);
    assert_eq!(pool.stats().evictions, before_error.evictions);
}

#[test]
fn v3_waiters_share_one_load_and_count_one_miss() {
    let store = MemoryStore::new(Duration::from_millis(10));
    let pool = BufferPoolV3::new(2, store.clone());
    let barrier = Arc::new(Barrier::new(2));

    std::thread::scope(|scope| {
        for _ in 0..2 {
            let pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                let guard = pool.fetch_page(PageId::new(1, 7), V3Mode::Normal).unwrap();
                assert_eq!(guard.read(|data| data[0]), 7);
            });
        }
    });

    assert_eq!(store.shared.reads.load(Ordering::Relaxed), 1);
    assert_eq!(pool.stats().hits, 1);
    assert_eq!(pool.stats().misses, 1);
}
