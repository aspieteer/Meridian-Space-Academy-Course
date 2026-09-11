use bumpalo::Bump;

pub fn global_alloc(frames: usize, payload_size: usize) {
    for _ in 0..frames {
        // Each Vec::new() + push triggers malloc + memcpy.
        let mut v = Vec::with_capacity(payload_size);
        for i in 0..payload_size {
            v.push(i as u8);
        }
        // Drop at end of loop iteration — free() called 100,000 times.
        let _ = v;
    }
}

pub fn arena_alloc(frames: usize, payload_size: usize) {
    // Pre-allocate a slab for the entire batch.
    let mut slab = vec![0u8; frames * payload_size];
    let mut offset = 0;
    for frame_idx in 0..frames {
        let start_byte = offset;
        let end_byte = offset + payload_size;
        for (i, byte) in slab[start_byte..end_byte].iter_mut().enumerate() {
            *byte = (frame_idx ^ i) as u8;
        }
        offset = end_byte;
    }
    // All frames "freed" by resetting offset to 0 — one operation.
    offset = 0;
    let _ = offset;
}

pub fn bump_arena_alloc(frames: usize, payload_size: usize) {
    assert!(payload_size.is_multiple_of(8));

    // Initialize a Bump Arena — one chunk sized for the whole batch.
    let bump = Bump::with_capacity(frames * payload_size);

    // Allocate the entire batch with ONE bump, then fill it through slices —
    // the same vectorizable write pattern as the manual arena.
    // (Element-wise `BumpVec::push` is scalar: a capacity-check branch plus
    // a len update per byte, which LLVM cannot vectorize.)
    let buf = bump.alloc_slice_fill_default::<u8>(frames * payload_size);
    for (frame_idx, chunk) in buf.chunks_mut(payload_size).enumerate() {
        for (i, byte) in chunk.iter_mut().enumerate() {
            *byte = (frame_idx ^ i) as u8;
        }
    }

    let _ = buf;
}
