#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn process_batch_optimised(n: usize) -> Vec<usize> {
    // Pre-allocate with known capacity — no reallocation on push.
    let mut result = Vec::with_capacity(n);
    let mut seen = std::collections::HashSet::with_capacity(n);

    for i in 0..n {
        if seen.insert(i % (n / 2)) {
            // ~50% are unique
            result.push(i);
        }
    }
    result
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let batch = process_batch_optimised(10_000);

    println!("{} unique items", batch.len());
    println!("dhat profile written on drop of _profiler");
}
