use std::io;

use crate::module1::config::PAGE_SIZE;

use super::buffer_pool::{BufferPool, lock};

/// RAII handle to a page pinned in the buffer pool.
/// The page is automatically unpinned when this handle is dropped.
pub struct PageHandle {
    pool: BufferPool,
    page_id: u32,
    frame_index: usize,
}

impl PageHandle {
    pub(crate) fn new(pool: BufferPool, page_id: u32, frame_index: usize) -> Self {
        Self {
            pool,
            page_id,
            frame_index,
        }
    }

    pub fn page_id(&self) -> u32 {
        self.page_id
    }

    /// Copy the page bytes out of the pool while keeping this page pinned.
    pub fn read_page(&self) -> io::Result<[u8; PAGE_SIZE]> {
        let state = lock(&self.pool.shared.state)?;
        let frame = &state.frames[self.frame_index];
        self.ensure_current(frame.page_id)?;
        Ok(frame.data)
    }

    /// Replace the page bytes and mark the frame dirty.
    pub fn modify_page(&mut self, data: [u8; PAGE_SIZE]) -> io::Result<()> {
        let mut state = lock(&self.pool.shared.state)?;
        let frame = &mut state.frames[self.frame_index];
        self.ensure_current(frame.page_id)?;
        frame.data = data;
        frame.is_dirty = true;
        Ok(())
    }

    fn ensure_current(&self, frame_page_id: Option<u32>) -> io::Result<()> {
        if frame_page_id == Some(self.page_id) {
            Ok(())
        } else {
            Err(io::Error::other("page handle no longer points to its page"))
        }
    }
}

impl Drop for PageHandle {
    fn drop(&mut self) {
        let Ok(mut state) = self.pool.shared.state.lock() else {
            return;
        };
        let frame = &mut state.frames[self.frame_index];
        if frame.page_id == Some(self.page_id) && frame.pin_count > 0 {
            frame.pin_count -= 1;
        }
    }
}
