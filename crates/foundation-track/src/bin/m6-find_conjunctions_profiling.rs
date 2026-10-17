// Example: a function with a deliberately inefficient hot path
// to demonstrate profiling workflow.

#[inline(never)]
fn find_conjunction_naive(
    altitudes: &[f64],
    norad_ids: &[u32],
    threshold_km: f64,
) -> Vec<(u32, u32)> {
    debug_assert_eq!(altitudes.len(), norad_ids.len());
    debug_assert!(threshold_km > 0.0);

    let mut alerts = Vec::new();
    let n = altitudes.len();

    for i in 0..(n - 1) {
        for j in (i + 1)..n {
            // This inner loop is O(n²) — will show as wide in a flamegraph.
            // The call to f64::abs() will likely appear as a hot child.
            if (altitudes[i] - altitudes[j]).abs() < threshold_km {
                alerts.push((norad_ids[i], norad_ids[j]));
            }
        }
    }

    alerts
}

fn main() {
    // Simulate workload for profiling.
    let n = 5_000;
    let altitudes: Vec<f64> = (0..n).map(|i| 400.0 + (i as f64) * 0.1).collect();
    let norad_ids: Vec<u32> = (0..n).collect();

    let alerts = find_conjunction_naive(&altitudes, &norad_ids, 2.0);
    println!("{} pairs of conjunction alerts", alerts.len());
}
