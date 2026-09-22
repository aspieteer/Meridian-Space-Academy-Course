use foundation_track::module6::benchmarking::processing;

fn main() {
    println!(
        "{:<10} {:>15} {:>15} {:>15}",
        "n_frames", "dedup (µs)", "sort (µs)", "total (µs)"
    );
    println!("{}", "-".repeat(55));

    for &n in &[100_usize, 500, 1_000, 5_000, 10_000] {
        // Build test data once — not in the measured loop.
        let headers: Vec<(u64, u32)> = (0..n).map(|i| ((i / 3) as u64, (i % 48) as u32)).collect();
        let timestamps: Vec<u64> = (0..n).map(|i| (n - i) as u64).collect();

        let (dedup_time, sort_time) = processing(&headers, &timestamps, 10_000);

        println!(
            "{:<10} {:>15.2} {:>15.2} {:>15.2}",
            n,
            dedup_time.as_secs_f64() * 1e6,
            sort_time.as_secs_f64() * 1e6,
            (dedup_time + sort_time).as_secs_f64() * 1e6,
        );
    }
}
