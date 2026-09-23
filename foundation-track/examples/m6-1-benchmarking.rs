use foundation_track::module6::benchmarking::{build_test_data, processing};

fn main() {
    println!(
        "{:<10} {:>15} {:>15} {:>15}",
        "n_frames", "dedup (µs)", "sort (µs)", "total (µs)"
    );
    println!("{}", "-".repeat(55));

    for &n in &[100_usize, 500, 1_000, 5_000, 10_000] {
        // Build test data once — not in the measured loop.
        let (timestamps, headers) = build_test_data(n);

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
