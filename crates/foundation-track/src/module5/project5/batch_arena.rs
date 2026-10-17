use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct BatchArena {
    slab: BytesMut,
    offsets: Vec<usize>,
}

impl BatchArena {
    pub fn new(capacity: usize) -> Self {
        Self {
            slab: BytesMut::with_capacity(capacity),
            offsets: Vec::new(),
        }
    }

    pub fn alloc(&mut self, payload: Bytes) {
        let start = self.offsets.last();
        match start {
            Some(start) => {
                let offset = start + payload.len();
                self.offsets.push(offset);
            }
            None => {
                self.offsets.push(payload.len());
            }
        }
        self.slab.put_slice(&payload[..]);
    }

    pub fn reset(&mut self) {
        self.slab.clear();
        self.offsets.clear();
    }
}

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_arena() {
        let mut arena = BatchArena::new(64);
        arena.alloc(Bytes::from("00000"));
        arena.alloc(Bytes::from("11111"));
        arena.alloc(Bytes::from("2222222222"));

        println!("arena: {:?}", arena);
        println!("arena length: {:?}", arena.slab.len());

        arena.reset();

        println!("arena: {:?}", arena);
        println!("arena length: {:?}", arena.slab.len());
    }
}
