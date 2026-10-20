// ===== BufferPoolError =====

use std::io;

#[derive(Debug)]
pub enum BufferPoolError {
    Io(io::Error),
    AllFramesPinned,
}

impl From<io::Error> for BufferPoolError {
    fn from(src: io::Error) -> Self {
        Self::Io(src)
    }
}
