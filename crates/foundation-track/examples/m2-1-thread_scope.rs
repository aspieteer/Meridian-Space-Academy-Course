use std::thread;

// The 'static requirement on spawn prevents borrowing stack data.
// thread::scope lifts this restriction: threads spawned within a scope are guaranteed to
// finish before the scope exits, which allows them to borrow data from the enclosing frame.

fn validate_tle_batch(records: &[String]) -> usize {
    let mid = records.len() / 2;
    let (left, right) = records.split_at(mid);

    // Scoped threads can borrow `left` and `right` — no Arc, no clone.
    thread::scope(|s| {
        let left_handle = s.spawn(|| left.iter().filter(|r| r.starts_with("1 ")).count());
        let right_handle = s.spawn(|| right.iter().filter(|r| r.starts_with("1 ")).count());

        // scope blocks here until both threads finish.
        left_handle.join().unwrap() + right_handle.join().unwrap()
    })
}

// `thread::scope` is the right tool for data-parallel CPU work over a borrowed slice —
// exactly the conjunction check pattern in the Meridian pipeline. No heap allocation, no Arc, no 'static constraint.
// The compiler enforces that the borrowed data outlives the scope.

fn main() {
    let records: Vec<String> = (0..100)
        .map(|i| format!("{} {:05}U record", if i % 2 == 0 { "1" } else { "2" }, i))
        .collect();
    println!("{} valid TLE lines", validate_tle_batch(&records));
}
