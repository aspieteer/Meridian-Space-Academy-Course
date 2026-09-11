use foundation_track::module5::manual_bumparena::BumpArena;

fn main() {
    let mut arena = BumpArena::new(4096);

    // Allocate space for 10 u64 values.
    let buf = arena.alloc(128 * 8, 8).expect("arena exhausted");
    buf[256] = 0xAA;
    println!(
        "allocated {} bytes, used {}/{}",
        buf.len(),
        arena.used(),
        arena.capacity(),
    );

    // Reset — all allocations invalidated, slab reused.
    arena.reset();
    println!("after reset: used {}", arena.used());

    use core::alloc::Layout;

    let a = Layout::from_size_align(1, 3).unwrap_err();
    println!("{}", a);
}
