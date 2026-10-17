use std::{hint::black_box, time::Instant};

use foundation_track::module6::{
    part3::counting_alloc::CountingAllocator,
    project6::{
        header::HeaderExtraction,
        pipeline::{Deduplicator, run_pipeline},
    },
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn main() {
    const BATCH_SIZE: usize = 1_000;
    const BATCHES: u32 = 10_000;
    let headers: Vec<_> = (0..BATCH_SIZE)
        .map(|i| HeaderExtraction::new((i / 3) as u64, (i % 48) as u32, 0u8))
        .collect();
    let mut dedup = Deduplicator::new(BATCH_SIZE);

    // Warm up.
    for _ in 0..10 {
        run_pipeline(&headers, &mut dedup);
    }

    // Throughput measurement.
    ALLOCATOR.reset_counters();
    let start = Instant::now();

    for _ in 0..BATCHES {
        black_box(run_pipeline(black_box(&headers), &mut dedup));
    }
    let elapsed = start.elapsed();
    let fps = (BATCHES as usize * BATCH_SIZE) as f64 / elapsed.as_secs_f64();

    let (allocs, deallocs, alloc_bytes) = ALLOCATOR.snapshot();

    println!(
        "{} BATCHES {:>15} {:>15} {:>15}",
        BATCHES, "allocs", "deallocs", "alloc bytes"
    );
    println!("{}", "-".repeat(65));
    println!(
        "{:>12} {:>15} {:>15} {:>15}",
        "Total", allocs, deallocs, alloc_bytes
    );
    println!(
        "{:>12} {:>15.3} {:>15.3} {:>15.3}",
        "Num/Batch",
        allocs as f64 / BATCHES as f64,
        deallocs as f64 / BATCHES as f64,
        alloc_bytes as f64 / BATCHES as f64
    );

    println!("\nthroughput: {:.0} frames/sec", fps);
    println!("elapsed: {:.2?}", elapsed);
}
