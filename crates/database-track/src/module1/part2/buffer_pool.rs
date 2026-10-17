use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::module1::{
    config::PAGE_SIZE,
    part1::page::{PageFile, PageHeader, PageType},
};

use super::page_handle::PageHandle;

/// A fixed-size LRU buffer pool backed by a [`PageFile`].
#[derive(Debug, Clone)]
pub struct BufferPool {
    pub(crate) shared: Arc<Shared>,
}

#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) state: Mutex<State>,
    page_file: Mutex<PageFile>,
}

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) frames: Vec<Frame>,
    page_table: HashMap<u32, usize>,
    /// Most recently used at the front, least recently used at the back.
    lru_list: VecDeque<usize>,
}

impl BufferPool {
    pub fn new(num_frames: usize, page_file: PageFile) -> Self {
        assert!(num_frames > 0, "a buffer pool needs at least one frame");

        let frames = (0..num_frames).map(|_| Frame::new()).collect();
        let lru_list = (0..num_frames).collect();

        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    frames,
                    page_table: HashMap::new(),
                    lru_list,
                }),
                page_file: Mutex::new(page_file),
            }),
        }
    }

    /// Allocate and initialize a page, then return it pinned in the pool.
    pub fn allocate_page(&self, page_type: PageType) -> io::Result<PageHandle> {
        let page_id = {
            let mut page_file = lock(&self.shared.page_file)?;
            let page_id = page_file.allocate_page()?;
            let mut data = [0_u8; PAGE_SIZE];
            PageHeader::new(page_id, page_type).serialize(&mut data);
            page_file.write_page_with_checksum(page_id, &mut data)?;
            page_id
        };

        self.fetch_page(page_id)
    }

    /// Fetch and pin a page. Dropping the returned handle unpins it.
    pub fn fetch_page(&self, page_id: u32) -> io::Result<PageHandle> {
        let frame_index = self.pin_page(page_id)?;
        Ok(PageHandle::new(self.clone(), page_id, frame_index))
    }

    /// Return the number of pages in the backing file.
    pub fn page_count(&self) -> io::Result<u32> {
        lock(&self.shared.page_file)?.page_count()
    }

    /// Write every dirty frame to the backing page file.
    pub fn flush_all(&self) -> io::Result<()> {
        let mut state = lock(&self.shared.state)?;
        let mut page_file = lock(&self.shared.page_file)?;

        for frame in &mut state.frames {
            if let Some(page_id) = frame.page_id
                && frame.is_dirty
            {
                page_file.write_page_with_checksum(page_id, &mut frame.data)?;
                frame.is_dirty = false;
            }
        }

        Ok(())
    }

    fn pin_page(&self, page_id: u32) -> io::Result<usize> {
        let mut state = lock(&self.shared.state)?;

        if let Some(&frame_index) = state.page_table.get(&page_id) {
            state.frames[frame_index].pin_count += 1;
            state.move_to_front(frame_index);
            return Ok(frame_index);
        }

        let frame_index = state.find_evict_target()?;
        let mut page_file = lock(&self.shared.page_file)?;

        if let Some(old_page_id) = state.frames[frame_index].page_id {
            if state.frames[frame_index].is_dirty {
                page_file
                    .write_page_with_checksum(old_page_id, &mut state.frames[frame_index].data)?;
            }
            state.page_table.remove(&old_page_id);
        }

        page_file.read_and_verify_page(page_id, &mut state.frames[frame_index].data)?;
        state.frames[frame_index].page_id = Some(page_id);
        state.frames[frame_index].pin_count = 1;
        state.frames[frame_index].is_dirty = false;
        state.page_table.insert(page_id, frame_index);
        state.move_to_front(frame_index);

        Ok(frame_index)
    }
}

impl State {
    fn find_evict_target(&self) -> io::Result<usize> {
        self.lru_list
            .iter()
            .rev()
            .copied()
            .find(|&frame_index| self.frames[frame_index].pin_count == 0)
            .ok_or_else(|| io::Error::other("buffer pool exhausted: all frames are pinned"))
    }

    fn move_to_front(&mut self, frame_index: usize) {
        self.lru_list.retain(|&index| index != frame_index);
        self.lru_list.push_front(frame_index);
    }
}

/// One in-memory slot managed by the buffer pool.
#[derive(Debug)]
pub(crate) struct Frame {
    pub(crate) page_id: Option<u32>,
    pub(crate) data: [u8; PAGE_SIZE],
    pub(crate) pin_count: u32,
    pub(crate) is_dirty: bool,
}

impl Frame {
    fn new() -> Self {
        Self {
            page_id: None,
            data: [0_u8; PAGE_SIZE],
            pin_count: 0,
            is_dirty: false,
        }
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> io::Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| io::Error::other("buffer pool lock was poisoned"))
}
