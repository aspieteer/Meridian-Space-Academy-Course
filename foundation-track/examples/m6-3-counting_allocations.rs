use std::sync::atomic::AtomicU64;

use foundation_track::module6::{
    part1::benchmarking::assemble_u128, part3::counting_alloc::CountingAllocator,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator {
    alloc_count: AtomicU64::new(0),
    dealloc_count: AtomicU64::new(0),
    alloc_bytes: AtomicU64::new(0),
};

// --- Frame processor under test ---

struct Processor {
    seen: std::collections::HashSet<u128>,
    indices: Vec<usize>,
}

impl Processor {
    fn new(cap: usize) -> Self {
        Self {
            seen: std::collections::HashSet::with_capacity(cap),
            indices: Vec::with_capacity(cap),
        }
    }

    fn process_batch(&mut self, headers: &[(u64, u32)]) -> usize {
        self.seen.clear();
        self.indices.clear();

        for (i, &key) in headers.iter().enumerate() {
            let combined_key = assemble_u128(key.0, key.1);
            if self.seen.insert(combined_key) {
                self.indices.push(i);
            }
        }

        self.indices.len()
    }
}

fn main() {
    const COUNT: usize = 10_000;
    let headers: Vec<(u64, u32)> = (0..COUNT)
        .map(|i| ((i / 3) as u64, (i % 48) as u32))
        .collect();

    let mut processor = Processor::new(COUNT);

    // Warm up — first batch may allocate as HashSet grows.
    processor.process_batch(&headers);

    // Reset — we only want to count allocations from process_batch.
    ALLOCATOR.reset_counters();

    // Run 100 batches.
    for _ in 0..100 {
        std::hint::black_box(processor.process_batch(std::hint::black_box(&headers)));
    }

    let (allocs, deallocs, bytes) = ALLOCATOR.snapshot();

    // In CI: assert!(allocs == 0, "unexpected allocations in hot path: {allocs}");
    if allocs == 0 {
        println!("PASS: hot path is allocation-free after warm-up");
    } else {
        println!("WARN: {allocs} unexpected allocations detected");
    }

    println!("  allocations:   {allocs}");
    println!("  deallocations: {deallocs}");
    println!("  bytes:         {bytes}");
}
