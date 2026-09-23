use std::{hint::black_box, time::Instant};

use foundation_track::module6::benchmarking::{build_test_data, process_batch};

fn main() {
    // Run enough iterations for perf to collect ~1000+ samples.
    // At 99 Hz sampling, we need ~10 seconds of CPU time.
    let (timestamps, headers) = build_test_data(10_000);
    let batches = 5_000;

    let start = Instant::now();
    let mut total = 0_usize;

    for _ in 0..batches {
        total += black_box(process_batch(black_box(&headers), black_box(&timestamps)));
    }
    let elapsed = start.elapsed();

    println!("processed {} batches, {} unique frames", batches, total);
    println!(
        "throughput: {:.0} batches/sec",
        batches as f64 / elapsed.as_secs_f64()
    );
    println!("wall time:  {:.2?}", elapsed);
}
