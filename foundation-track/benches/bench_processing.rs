use criterion::{Criterion, criterion_group, criterion_main};
use foundation_track::module6::part1::benchmarking::processing;

fn bench_processing(c: &mut Criterion) {
    for &n in &[100_usize, 500, 1_000, 5_000, 10_000] {
        // Build test data once — not in the measured loop.
        let headers: Vec<(u64, u32)> = (0..n).map(|i| ((i / 3) as u64, (i % 48) as u32)).collect();
        let timestamps: Vec<u64> = (0..n).map(|i| (n - i) as u64).collect();

        c.bench_function("batch processing", |b| {
            b.iter(|| processing(&headers, &timestamps, 1)) // bench is doing iterations
            // for us, so processing does not need iteration itself
        });
    }
}

criterion_group!(benches, bench_processing);
criterion_main!(benches);
