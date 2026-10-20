use std::{fs::File, io, os::unix::fs::FileExt, path::Path, sync::Mutex};

use super::{PageId, PageNo};
use crate::module1::PAGE_HEADER_SIZE;

#[derive(Debug)]
pub struct RelationFile {
    relation_id: u32,
    file: File,
    page_size: usize,

    // Avoid page no. contention when allocating one page.
    alloc_lock: Mutex<()>,
}

impl RelationFile {
    pub fn open<P: AsRef<Path>>(relation_id: u32, path: P, page_size: usize) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&path)?;

        Ok(Self {
            relation_id,
            file,
            page_size,
            alloc_lock: Mutex::new(()),
        })
    }

    pub fn page_count(&self) -> io::Result<u32> {
        let file_len = self.file.metadata()?.len();

        if file_len % self.page_size as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "database file length is not aligned to the configured page size",
            ));
        }

        u32::try_from(file_len / self.page_size as u64)
            .map_err(|_| io::Error::other("database file contains too many pages"))
    }

    pub fn read_and_verify_page(&self, page_no: PageNo, buf: &mut [u8]) -> io::Result<()> {
        self.read_page(page_no, buf)?;

        let stored = u32::from_le_bytes(buf[13..17].try_into().expect("slice -> array failed"));
        let computed = compute_checksum(buf);

        if stored != computed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "page {} checksum mismatch: stored={:#010x}, computed={:#010x}",
                    page_no, stored, computed
                ),
            ));
        }

        Ok(())
    }

    // read page based upon the page_id as offset
    fn read_page(&self, page_no: PageNo, buf: &mut [u8]) -> io::Result<()> {
        assert_eq!(buf.len(), self.page_size);

        let offset = u64::from(page_no)
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "page offset overflow"))?;

        // Use read_at api cuz Shared File Cursor has potential concurrency issues.
        self.file.read_exact_at(buf, offset)
    }

    pub fn write_page_with_checksum(&self, page_no: PageNo, buf: &mut [u8]) -> io::Result<()> {
        let computed = compute_checksum(buf);
        buf[13..17].copy_from_slice(&computed.to_le_bytes());

        self.write_page(page_no, buf)
    }

    fn write_page(&self, page_no: PageNo, buf: &[u8]) -> io::Result<()> {
        assert_eq!(buf.len(), self.page_size);

        let offset = u64::from(page_no)
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "page offset overflow"))?;

        self.file.write_all_at(buf, offset)
    }

    pub fn allocate_page(&self) -> io::Result<PageId> {
        let _alloc_guard = self.alloc_lock.lock().unwrap();

        let file_len = self.file.metadata()?.len();
        let new_page_no = self.page_count()?;
        let zeroed = vec![0u8; self.page_size];

        self.file.write_all_at(&zeroed, file_len)?;

        Ok(PageId {
            relation_id: self.relation_id,
            page_no: new_page_no,
        })
    }
}

// ===== util =====

// CRC32 checksum of the page body (everything after the checksum field).
// We zero the checksum field before computing so the checksum is
// deterministic regardless of the previous checksum value.
fn compute_checksum(page_buf: &[u8]) -> u32 {
    // Checksum covers bytes 17..PAGE_SIZE (the body).
    // The header's checksum field (bytes 13..17) is excluded from the
    // computation — it stores the result.
    let body = &page_buf[PAGE_HEADER_SIZE..];
    crc32fast::hash(body)
}
