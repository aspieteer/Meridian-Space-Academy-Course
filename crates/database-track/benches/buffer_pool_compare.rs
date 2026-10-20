//! Compare fetch + read + drop throughput for the two buffer pools.
//!
//! Run with: cargo bench -p database-track --bench buffer_pool_compare
//! The store is synthetic: the memory case isolates pool overhead, while the
//! delayed case shows the effect of holding the pool lock across a slow read.

use std::{
    hint::black_box,
    io, thread,
    time::{Duration, Instant},
};

use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use database_track::module1::project1::{
    PageId, PageStore,
    buffer_pool::{AccessMode as MutexMode, BufferPool},
    buffer_pool_v2::{AccessMode as RwLockMode, BufferPoolV2},
};

const OPS_PER_ITERATION_PER_THREAD: u64 = 128;
const HIT_PAGES_PER_THREAD: usize = 32;
const MISS_PAGES_PER_THREAD: usize = 128;
const MISS_CAPACITY: usize = 64;

#[derive(Clone, Copy)]
struct SyntheticStore {
    read_delay: Duration,
}

impl PageStore for SyntheticStore {
    fn read_page(&self, page_id: &PageId, dest: &mut [u8]) -> io::Result<()> {
        if !self.read_delay.is_zero() {
            thread::sleep(self.read_delay);
        }
        dest.fill(page_id.page_no() as u8);
        Ok(())
    }

    fn write_page(&self, _page_id: &PageId, _src: &mut [u8]) -> io::Result<()> {
        Ok(())
    }
}

trait BenchPool: Sync {
    fn create(capacity: usize, store: SyntheticStore) -> Self;
    fn fetch_byte(&self, page_id: PageId) -> u8;
}

impl BenchPool for BufferPool<SyntheticStore> {
    fn create(capacity: usize, store: SyntheticStore) -> Self {
        Self::new(capacity, store)
    }

    fn fetch_byte(&self, page_id: PageId) -> u8 {
        let guard = self.fetch_page(page_id, MutexMode::Normal).unwrap();
        guard.read(|data| data[0])
    }
}

impl BenchPool for BufferPoolV2<SyntheticStore> {
    fn create(capacity: usize, store: SyntheticStore) -> Self {
        Self::new(capacity, store)
    }

    fn fetch_byte(&self, page_id: PageId) -> u8 {
        let guard = self.fetch_page(page_id, RwLockMode::Normal).unwrap();
        guard.read(|data| data[0])
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Hits,
    Misses,
}

fn page_ids(threads: usize, pages_per_thread: usize) -> Vec<Vec<PageId>> {
    (0..threads)
        .map(|thread_index| {
            (0..pages_per_thread)
                .map(|page_no| PageId::new(thread_index as u32 + 1, page_no as u32))
                .collect()
        })
        .collect()
}

fn measure<P: BenchPool>(pool: &P, ids: &[Vec<PageId>], iterations: u64) -> Duration {
    let start = Instant::now();
    thread::scope(|scope| {
        for pages in ids {
            scope.spawn(move || {
                let mut cursor = 0;
                let mut checksum = 0u64;
                for _ in 0..iterations {
                    for _ in 0..OPS_PER_ITERATION_PER_THREAD {
                        checksum =
                            checksum.wrapping_add(pool.fetch_byte(pages[cursor].clone()) as u64);
                        cursor += 1;
                        if cursor == pages.len() {
                            cursor = 0;
                        }
                    }
                }
                black_box(checksum);
            });
        }
    });
    start.elapsed()
}

fn bench_pool<P: BenchPool>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    threads: usize,
    workload: Workload,
    read_delay: Duration,
) {
    let pages_per_thread = match workload {
        Workload::Hits => HIT_PAGES_PER_THREAD,
        Workload::Misses => MISS_PAGES_PER_THREAD,
    };
    let capacity = match workload {
        Workload::Hits => threads * HIT_PAGES_PER_THREAD,
        Workload::Misses => MISS_CAPACITY,
    };
    let ids = page_ids(threads, pages_per_thread);
    let pool = P::create(capacity, SyntheticStore { read_delay });

    // Warm before timing. Hits keep every page resident; misses cycle through
    // more pages per thread than the entire pool can hold.
    for pages in &ids {
        for id in pages {
            assert_eq!(pool.fetch_byte(id.clone()), id.page_no() as u8);
        }
    }

    group.bench_function(BenchmarkId::new(name, format!("{threads}_threads")), |b| {
        b.iter_custom(|iterations| measure(&pool, &ids, iterations));
    });
}

fn bench_buffer_pools(c: &mut Criterion) {
    for (name, workload, read_delay) in [
        ("cached_reads", Workload::Hits, Duration::ZERO),
        ("memory_misses", Workload::Misses, Duration::ZERO),
        (
            "delayed_misses_200us",
            Workload::Misses,
            Duration::from_micros(200),
        ),
    ] {
        let mut group = c.benchmark_group(name);
        for threads in [1, 4] {
            group.throughput(Throughput::Elements(
                OPS_PER_ITERATION_PER_THREAD * threads as u64,
            ));
            bench_pool::<BufferPool<SyntheticStore>>(
                &mut group,
                "global_mutex",
                threads,
                workload,
                read_delay,
            );
            bench_pool::<BufferPoolV2<SyntheticStore>>(
                &mut group,
                "table_rwlock",
                threads,
                workload,
                read_delay,
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_buffer_pools);
criterion_main!(benches);
