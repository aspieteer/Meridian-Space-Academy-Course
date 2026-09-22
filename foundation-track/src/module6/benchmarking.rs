use std::{
    hint::black_box,
    time::{Duration, Instant},
};

pub fn processing(headers: &[(u64, u32)], timestamps: &[u64], iters: u32) -> (Duration, Duration) {
    let dedup_time = time_fn(
        || {
            black_box(deduplicate(black_box(headers)));
        },
        iters,
    );

    let sort_time = time_fn(
        || {
            let mut indices = (0..timestamps.len()).collect::<Vec<_>>();
            sort_by_timestamp(black_box(&mut indices), black_box(timestamps));
            black_box(indices);
        },
        iters,
    );

    (dedup_time, sort_time)
}

fn deduplicate(headers: &[(u64, u32)]) -> Vec<usize> {
    let mut seen = std::collections::HashSet::with_capacity(headers.len());

    headers
        .iter()
        .enumerate()
        .filter_map(|(i, &(seq, sat))| {
            let combined = assemble_u128(seq, sat);
            if seen.insert(combined) { Some(i) } else { None }
        })
        .collect()
}

fn sort_by_timestamp(indices: &mut [usize], timestamps: &[u64]) {
    indices.sort_unstable_by_key(|&i| timestamps[i]);
}

/// Run `iterations` iterations, return median per-iteration duration.
fn time_fn<F: Fn()>(f: F, iters: u32) -> Duration {
    // Warm up — let branch predictor and instruction cache settle.
    for _ in 0..10 {
        f();
    }

    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed() / iters
}

fn assemble_u128(high: u64, low: u32) -> u128 {
    ((high as u128) << 32) | (low as u128)
}

#[allow(unused)]
fn split_back_from_u128(combined: u128) -> (u64, u32) {
    let high = (combined >> 32) as u64;
    let low = combined as u32;

    (high, low)
}
