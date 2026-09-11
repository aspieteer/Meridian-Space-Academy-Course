use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use foundation_track::module5::allocs::{arena_alloc, bump_arena_alloc, global_alloc};

fn bench_allocs(c: &mut Criterion) {
    const FRAMES: usize = 100_000;
    const PAYLOAD_SIZE: usize = 256;

    let mut group = c.benchmark_group("Different allocation approaches");

    group.bench_function("Naive Global Allocation", |b| {
        b.iter(|| global_alloc(FRAMES, PAYLOAD_SIZE))
    });
    group.bench_function("Manual Arena Allocation", |b| {
        b.iter(|| arena_alloc(FRAMES, PAYLOAD_SIZE))
    });
    group.bench_function("Bumpalo Arena Allocation", |b| {
        b.iter(|| bump_arena_alloc(FRAMES, PAYLOAD_SIZE))
    });

    group.finish();
}

criterion_group!(benches, bench_allocs);
criterion_main!(benches);
