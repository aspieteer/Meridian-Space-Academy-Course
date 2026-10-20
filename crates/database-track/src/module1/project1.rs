//! [page_id, slot/record_id] as page indexing.
//! And the buffer pool leverage CLOCK eviction policy.

use std::io;

pub mod buffer_pool;
pub mod buffer_pool_v2;
pub mod disk_manager;
pub mod error;
pub mod page;
pub mod relation_file;
pub mod tle;

// PageNo alias
pub type PageNo = u32;
pub const PAGE_SIZE: usize = crate::module1::PAGE_SIZE;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageId {
    relation_id: u32,
    page_no: PageNo,
}

impl PageId {
    pub fn new(relation_id: u32, page_no: PageNo) -> Self {
        Self {
            relation_id,
            page_no,
        }
    }

    pub fn page_no(&self) -> PageNo {
        self.page_no
    }
}

pub trait PageStore: Send + Sync + 'static {
    fn read_page(&self, page_id: &PageId, dest: &mut [u8]) -> io::Result<()>;

    fn write_page(&self, page_id: &PageId, src: &mut [u8]) -> io::Result<()>;
}
