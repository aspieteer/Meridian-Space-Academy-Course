use std::{cell::RefCell, fmt};

// Illustrating the pattern with a manual approach instead
pub struct BumpArena {
    slab: Vec<u8>,
    offset: usize,
}

impl fmt::Debug for BumpArena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BumpArena")
            .field("slab", &self.slab)
            .field("offset", &self.offset)
            .finish()
    }
}

impl BumpArena {
    pub fn new(capacity: usize) -> Self {
        Self {
            slab: vec![0u8; capacity],
            offset: 0,
        }
    }

    /// Allocate `size` bytes aligned to `align`.
    /// Returns None if the slab is exhausted.
    pub fn alloc(&mut self, size: usize, align: usize) -> Option<&mut [u8]> {
        // Align the current offset up.
        // align variable indicates number of bytes to be aligned.
        let aligned = (self.offset + align - 1) & !(align - 1);
        let end = aligned + size;

        if end > self.slab.len() {
            return None;
        }

        self.offset = end;

        Some(&mut self.slab[aligned..end])
    }

    /// Reset the arena — all previous allocations are invalidated.
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    pub fn used(&self) -> usize {
        self.offset
    }

    pub fn capacity(&self) -> usize {
        self.slab.len()
    }
}

// Thread-local Arenas for Concurrent Processing

const ARENA_CAPACITY: usize = 16 * 1024 * 1024; // 16MiB per thread

thread_local! {
    // Each worker thread has its own private arena.
    // No synchronisation — no atomic operations, no locks.
    static FRAME_ARENA: RefCell<Vec<u8>> = RefCell::new(vec![0u8; ARENA_CAPACITY]);
    static ARENA_OFFSET: RefCell<usize> = const { RefCell::new(0) };
}

pub fn alloc_frame_buffer(size: usize) -> *mut u8 {
    FRAME_ARENA.with(|arena| {
        ARENA_OFFSET.with(|offset| {
            let mut off = offset.borrow_mut();
            let aligned = (*off + 7) & !7; // 8-byte alignment
            let end = aligned + size;

            if end > arena.borrow().len() {
                panic!(
                    "thread-local arena exhausted - increase ARENA_CAPACITY or reduce batch size"
                );
            }

            *off = end;
            arena.as_ptr() as *mut u8
        })
    })
}

pub fn reset_thread_arena() {
    ARENA_OFFSET.with(|offset| *offset.borrow_mut() = 0);
}
