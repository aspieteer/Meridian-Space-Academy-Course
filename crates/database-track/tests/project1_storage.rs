//! Run with: cargo test -p database-track --test project1_storage -- --nocapture

use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use database_track::module1::project1::{
    PAGE_SIZE, PageId, PageStore,
    buffer_pool::{AccessMode, BufferPool, PageGuard},
    buffer_pool_v2::{AccessMode as V2AccessMode, BufferPoolV2, PageGuard as V2PageGuard},
    buffer_pool_v3::{AccessMode as V3AccessMode, BufferPoolV3, PageGuard as V3PageGuard},
    disk_manager::DiskManager,
    error::BufferPoolError,
    page::Page,
    relation_file::RelationFile,
    stats::BufferPoolStats,
    tle::TleRecord,
};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::{Binomial, Distribution};

const RELATION_ID: u32 = 42;
const PAYLOAD_OFFSET: usize = 128;

struct TempDb(PathBuf);

impl TempDb {
    fn new() -> io::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "project1-storage-test-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fetch(pool: &BufferPool<DiskManager>, id: &PageId) -> io::Result<PageGuard> {
    pool.fetch_page(id.clone(), AccessMode::Normal)
        .map_err(|error| match error {
            BufferPoolError::Io(error) => error,
            BufferPoolError::AllFramesPinned => io::Error::other("all buffer frames are pinned"),
        })
}

fn check_disk(store: &DiskManager, id: &PageId, expected: &[u8]) -> io::Result<()> {
    let mut data = [0u8; PAGE_SIZE];
    store.read_page(id, &mut data)?; // Also verifies the stored checksum.
    assert_eq!(
        &data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + expected.len()],
        expected
    );
    Ok(())
}

fn run(db_dir: &Path) -> io::Result<()> {
    let manager = DiskManager::new(db_dir)?;
    let relation = manager.create_relation(&PageId::new(RELATION_ID, 0))?;
    let mut ids = Vec::new();

    for page_no in 0..3 {
        let id = relation.allocate_page()?;
        assert_eq!(id.page_no(), page_no);

        // Allocation reserves zeroed space. Write a valid page before fetching it.
        let mut data = [0u8; PAGE_SIZE];
        Page::initialize(&mut data, id.page_no());
        data[PAYLOAD_OFFSET] = b'0' + page_no as u8;
        manager.write_page(&id, &mut data)?;
        ids.push(id);
    }
    assert_eq!(relation.page_count()?, 3);
    drop(relation);
    println!("allocated and checksummed 3 pages");

    let pool = BufferPool::new(2, manager);
    let verifier = DiskManager::new(db_dir)?;

    let first = fetch(&pool, &ids[0])?;
    assert_eq!(first.page_id(), &ids[0]);
    first.write(|data| data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + 5].copy_from_slice(b"alpha"));

    let hit = fetch(&pool, &ids[0])?;
    hit.read(|data| assert_eq!(&data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + 5], b"alpha"));
    drop(hit);
    check_disk(&verifier, &ids[0], b"0")?; // Dirty data is still only in the pool.
    println!("buffer hit and dirty in-memory write: ok");

    let second = fetch(&pool, &ids[1])?;
    assert!(matches!(
        pool.fetch_page(ids[2].clone(), AccessMode::BulkRead),
        Err(BufferPoolError::AllFramesPinned)
    ));
    println!("pinned frames cannot be evicted: ok");

    drop(first);
    let third = fetch(&pool, &ids[2])?;
    check_disk(&verifier, &ids[0], b"alpha")?; // Eviction wrote the dirty page.
    third.write(|data| data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + 5].copy_from_slice(b"bravo"));
    drop(second);
    drop(third);
    pool.flush_all().map_err(|error| match error {
        BufferPoolError::Io(error) => error,
        BufferPoolError::AllFramesPinned => io::Error::other("unexpected pinned-frame error"),
    })?;
    check_disk(&verifier, &ids[2], b"bravo")?;
    println!("dirty eviction and flush: ok");

    drop(pool);
    drop(verifier);
    let reopened = DiskManager::new(db_dir)?;
    assert_eq!(reopened.get_relation(&ids[0])?.page_count()?, 3);
    check_disk(&reopened, &ids[0], b"alpha")?;
    check_disk(&reopened, &ids[1], b"1")?;
    check_disk(&reopened, &ids[2], b"bravo")?;
    println!("reopen and checksum-verified reads: ok");

    // Corrupt one body byte and confirm the disk manager rejects the page.
    use std::os::unix::fs::FileExt;
    let path = db_dir
        .join("relations")
        .join(format!("{RELATION_ID}.0.rel"));
    let file = fs::OpenOptions::new().write(true).open(path)?;
    file.write_all_at(
        b"!",
        ids[1].page_no() as u64 * PAGE_SIZE as u64 + PAYLOAD_OFFSET as u64,
    )?;
    let mut data = [0u8; PAGE_SIZE];
    let error = reopened.read_page(&ids[1], &mut data).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    println!("checksum detects corruption: ok");

    Ok(())
}

#[test]
fn disk_and_buffer_pool_smoke() -> io::Result<()> {
    let db = TempDb::new()?;
    run(&db.0)
}

#[test]
fn clock_keeps_hot_page_and_evicts_cold_page() -> io::Result<()> {
    let db = TempDb::new()?;
    let manager = DiskManager::new(&db.0)?;
    let relation = manager.create_relation(&PageId::new(RELATION_ID, 0))?;
    let mut ids = Vec::new();
    for _ in 0..4 {
        let id = relation.allocate_page()?;
        let mut data = [0; PAGE_SIZE];
        Page::initialize(&mut data, id.page_no());
        manager.write_page(&id, &mut data)?;
        ids.push(id);
    }
    drop(relation);

    let pool = BufferPool::new(3, manager);
    for id in &ids[..3] {
        drop(fetch(&pool, id)?);
    }
    for _ in 0..5 {
        drop(fetch(&pool, &ids[0])?);
    }
    drop(fetch(&pool, &ids[3])?);
    let after_eviction = pool.stats();
    assert_eq!(after_eviction.evictions, 1);
    assert!(after_eviction.clock_second_chances >= 4);

    let before_hot = pool.stats();
    drop(fetch(&pool, &ids[0])?);
    assert_eq!(pool.stats().hits, before_hot.hits + 1);

    let before_cold = pool.stats();
    drop(fetch(&pool, &ids[1])?);
    assert_eq!(pool.stats().misses, before_cold.misses + 1);
    Ok(())
}

fn tle(sequence: u32) -> TleRecord {
    TleRecord {
        norad_id: 25_544 + sequence,
        intl_designator: *b"98067A  ",
        epoch: 26_001.0 + sequence as f64 / 1000.0,
        mean_motion: 15.5 + sequence as f64 / 100_000.0,
        eccentricity: 0.0005,
        inclination: 51.64,
        raan: (sequence % 360) as f64,
        arg_perigee: 90.0,
        mean_anomaly: 270.0,
        bstar: 0.00012,
        element_set: (sequence % 1000) as u16,
        rev_number: 10_000 + sequence,
    }
}

trait StressPageGuard {
    fn read_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; PAGE_SIZE]) -> R;

    fn write_page<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> R;
}

impl StressPageGuard for PageGuard {
    fn read_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; PAGE_SIZE]) -> R,
    {
        self.read(f)
    }

    fn write_page<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    {
        self.write(f)
    }
}

impl StressPageGuard for V2PageGuard {
    fn read_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; PAGE_SIZE]) -> R,
    {
        self.read(f)
    }

    fn write_page<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    {
        self.write(f)
    }
}

impl StressPageGuard for V3PageGuard {
    fn read_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; PAGE_SIZE]) -> R,
    {
        self.read(f)
    }

    fn write_page<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    {
        self.write(f)
    }
}

trait StressPool: Sync {
    type Guard: StressPageGuard;

    fn with_store(capacity: usize, store: DiskManager) -> Self;
    fn fetch_normal(&self, page_id: PageId) -> Result<Self::Guard, BufferPoolError>;
    fn flush(&self) -> Result<(), BufferPoolError>;
    fn stats_snapshot(&self) -> BufferPoolStats;
}

impl StressPool for BufferPool<DiskManager> {
    type Guard = PageGuard;

    fn with_store(capacity: usize, store: DiskManager) -> Self {
        Self::new(capacity, store)
    }

    fn fetch_normal(&self, page_id: PageId) -> Result<Self::Guard, BufferPoolError> {
        self.fetch_page(page_id, AccessMode::Normal)
    }

    fn flush(&self) -> Result<(), BufferPoolError> {
        self.flush_all()
    }

    fn stats_snapshot(&self) -> BufferPoolStats {
        self.stats()
    }
}

impl StressPool for BufferPoolV2<DiskManager> {
    type Guard = V2PageGuard;

    fn with_store(capacity: usize, store: DiskManager) -> Self {
        Self::new(capacity, store)
    }

    fn fetch_normal(&self, page_id: PageId) -> Result<Self::Guard, BufferPoolError> {
        self.fetch_page(page_id, V2AccessMode::Normal)
    }

    fn flush(&self) -> Result<(), BufferPoolError> {
        self.flush_all()
    }

    fn stats_snapshot(&self) -> BufferPoolStats {
        self.stats()
    }
}

impl StressPool for BufferPoolV3<DiskManager> {
    type Guard = V3PageGuard;

    fn with_store(capacity: usize, store: DiskManager) -> Self {
        Self::new(capacity, store)
    }

    fn fetch_normal(&self, page_id: PageId) -> Result<Self::Guard, BufferPoolError> {
        self.fetch_page(page_id, V3AccessMode::Normal)
    }

    fn flush(&self) -> Result<(), BufferPoolError> {
        self.flush_all()
    }

    fn stats_snapshot(&self) -> BufferPoolStats {
        self.stats()
    }
}

fn stress_error(error: BufferPoolError) -> io::Error {
    match error {
        BufferPoolError::Io(error) => error,
        BufferPoolError::AllFramesPinned => io::Error::other("all buffer frames are pinned"),
    }
}

fn insert_record_with_overflow<P: StressPool>(
    pool: &P,
    relation: &RelationFile,
    preferred: &PageId,
    overflow_page: &mut Option<PageId>,
    record: &TleRecord,
) -> io::Result<(PageId, u16, usize)> {
    let encoded = record.serialize();

    // An empty page needs a header(17 Bytes) and one slot entry(4 Bytes).
    if encoded.len() > PAGE_SIZE - 17 - 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record is even larger than an empty page",
        ));
    }

    if let Some(slot_id) = try_insert_record(pool, preferred, &encoded)? {
        return Ok((preferred.clone(), slot_id, 0));
    }

    let mut extra_fetches = 0;
    if let Some(id) = overflow_page.as_ref() {
        extra_fetches += 1;
        if let Some(slot_id) = try_insert_record(pool, id, &encoded)? {
            return Ok((id.clone(), slot_id, extra_fetches));
        }
    }

    let id = relation.allocate_page()?;
    let mut bytes = [0u8; PAGE_SIZE];
    Page::initialize(&mut bytes, id.page_no());
    relation.write_page_with_checksum(id.page_no(), &mut bytes)?;

    extra_fetches += 1;
    let slot_id = try_insert_record(pool, &id, &encoded)?
        .ok_or_else(|| io::Error::other("record did not fit newly allocated page"))?;

    *overflow_page = Some(id.clone());

    Ok((id, slot_id, extra_fetches))
}

fn insert_record<P: StressPool>(pool: &P, id: &PageId, record: &TleRecord) -> io::Result<u16> {
    try_insert_record(pool, id, &record.serialize())?
        .ok_or_else(|| io::Error::other("seed page is full"))
}

fn try_insert_record<P: StressPool>(
    pool: &P,
    id: &PageId,
    encoded: &[u8],
) -> io::Result<Option<u16>> {
    let mut guard = pool.fetch_normal(id.clone()).map_err(stress_error)?;

    guard.write_page(|bytes| {
        let mut page = Page::from_bytes(bytes, id.page_no())?;
        let slot_id = page.insert_compacting(encoded);
        if slot_id.is_some() {
            page.copy_to(bytes);
        }
        Ok(slot_id)
    })
}

fn load_record<P: StressPool>(pool: &P, id: &PageId, slot: u16) -> io::Result<TleRecord> {
    let guard = pool.fetch_normal(id.clone()).map_err(stress_error)?;
    guard.read_page(|bytes| {
        let page = Page::from_bytes(bytes, id.page_no())?;
        let record = page
            .get(slot)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slot is empty"))?;
        TleRecord::deserialize(record)
    })
}

fn run_tle_stress<P: StressPool>(pool_name: &str) -> io::Result<()> {
    const PAGES: usize = 1024;
    const CAPACITY: usize = 512;
    const WRITERS: usize = 6;
    const READERS: usize = 12;
    const WRITES_PER_THREAD: usize = 5_000;
    const READS_PER_THREAD: usize = 10_000;

    let db = TempDb::new()?;
    let manager = DiskManager::new(&db.0)?;
    let relation = manager.create_relation(&PageId::new(RELATION_ID, 0))?;
    let mut ids = Vec::with_capacity(PAGES);
    for _ in 0..PAGES {
        let id = relation.allocate_page()?;
        let mut data = [0; PAGE_SIZE];
        Page::initialize(&mut data, id.page_no());
        manager.write_page(&id, &mut data)?;
        ids.push(id);
    }

    // The key is the physical page plus its slot. Seed one stable record per page.
    let pool = P::with_store(CAPACITY, manager);
    let index = Mutex::new(HashMap::<(PageId, u16), TleRecord>::new());
    let mut baseline = Vec::with_capacity(PAGES);

    for (page_index, id) in ids.iter().enumerate() {
        let record = tle(page_index as u32);
        let slot_id = insert_record(&pool, id, &record)?;
        baseline.push((id.clone(), slot_id, record.clone()));
        index.lock().unwrap().insert((id.clone(), slot_id), record);
    }

    let before = pool.stats_snapshot();
    let start = Instant::now();
    let extra_fetches = std::thread::scope(|scope| -> io::Result<usize> {
        let mut workers = Vec::new();
        for writer in 0..WRITERS {
            let ids = &ids;
            let pool = &pool;
            let relation = &relation;
            let index = &index;

            let mut rng = SmallRng::seed_from_u64(0x73_0d_59 + writer as u64);
            workers.push(scope.spawn(move || -> io::Result<usize> {
                let mut overflow_page = None;
                let mut extra_fetches = 0;

                let bin_write = Binomial::new((PAGES - 1) as u64, 0.6).unwrap();
                for operation in 0..WRITES_PER_THREAD {
                    let page_index = bin_write.sample(&mut rng) as usize;
                    let preferred = &ids[page_index];
                    let record = tle(1_000 + (writer * WRITES_PER_THREAD + operation) as u32);
                    let (actual_id, slot_id, extra) = insert_record_with_overflow(
                        pool,
                        relation,
                        preferred,
                        &mut overflow_page,
                        &record,
                    )?;
                    extra_fetches += extra;
                    index.lock().unwrap().insert((actual_id, slot_id), record);
                }
                Ok(extra_fetches)
            }));
        }
        for reader in 0..READERS {
            let baseline = &baseline;
            let pool = &pool;

            let mut rng = SmallRng::seed_from_u64(0x54_4c_45 + reader as u64);
            workers.push(scope.spawn(move || -> io::Result<usize> {
                let bin = Binomial::new((PAGES - 1) as u64, 0.6).unwrap();
                for _ in 0..READS_PER_THREAD {
                    let page_index = bin.sample(&mut rng) as usize;
                    let (id, slot, expected) = &baseline[page_index];
                    assert_eq!(&load_record(pool, id, *slot)?, expected);
                }
                Ok(0)
            }));
        }
        let mut extra_fetches = 0;
        for worker in workers {
            extra_fetches += worker.join().expect("storage worker panicked")?;
        }
        Ok(extra_fetches)
    })?;
    let elapsed = start.elapsed();
    let operations = WRITERS * WRITES_PER_THREAD + READERS * READS_PER_THREAD;
    let after = pool.stats_snapshot();
    let workload = BufferPoolStats {
        hits: after.hits - before.hits,
        misses: after.misses - before.misses,
        evictions: after.evictions - before.evictions,
        clock_second_chances: after.clock_second_chances - before.clock_second_chances,
    };
    let fetches = operations + extra_fetches;
    assert_eq!(workload.hits + workload.misses, fetches as u64);
    assert!(workload.hits > 0);
    assert!(workload.misses > 0);
    assert!(workload.evictions > 0);
    assert!(workload.clock_second_chances > 0);
    eprintln!(
        "{pool_name}: {PAGES} pages / {CAPACITY} frames: {operations} concurrent TLE operations in {elapsed:?} ({:.0} ops/s); fetches={fetches} ({extra_fetches} extra page fetches), hits={}, misses={}, hit rate={:.1}%, evictions={}, CLOCK second chances={}",
        operations as f64 / elapsed.as_secs_f64(),
        workload.hits,
        workload.misses,
        workload.hit_rate() * 100.0,
        workload.evictions,
        workload.clock_second_chances
    );

    pool.flush().map_err(stress_error)?;
    drop(pool);

    let reopened = P::with_store(CAPACITY, DiskManager::new(&db.0)?);
    let index = index.into_inner().unwrap();
    assert_eq!(index.len(), PAGES + WRITERS * WRITES_PER_THREAD);
    for ((id, slot), expected) in &index {
        assert_eq!(&load_record(&reopened, id, *slot)?, expected);
    }
    Ok(())
}

#[test]
fn tle_records_survive_concurrent_io_and_reopen() -> io::Result<()> {
    run_tle_stress::<BufferPool<DiskManager>>("BufferPool")?;
    run_tle_stress::<BufferPoolV2<DiskManager>>("BufferPoolV2")?;
    run_tle_stress::<BufferPoolV3<DiskManager>>("BufferPoolV3")
}
