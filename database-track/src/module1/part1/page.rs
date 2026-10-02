use std::{
    fs::File,
    io::{self, Read, Seek, Write},
    path::Path,
};

use crate::module1::config::{MAGIC, PAGE_HEADER_SIZE};

/// Page types in the Orbital Object Registry.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    FileHeader = 0,
    Data = 1,
    Index = 2,
    Free = 3,
    Overflow = 4,
}

/// Fixed-size page header. Sits at byte 0 of every page.
#[derive(Debug)]
pub struct PageHeader {
    magic: [u8; 4],
    page_id: u32,
    page_type: PageType,
    record_count: u16,
    free_space_offset: u16,
    checksum: u32,
}

impl PageHeader {
    pub fn new(page_id: u32, page_type: PageType) -> Self {
        Self {
            magic: MAGIC,
            page_id,
            page_type,
            record_count: 0,
            // Free space starts immediately after the header
            free_space_offset: PAGE_HEADER_SIZE as u16,
            checksum: 0,
        }
    }

    pub fn serialize(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.page_id.to_le_bytes());
        buf[8] = self.page_type as u8;
        buf[9..11].copy_from_slice(&self.record_count.to_le_bytes());
        buf[11..13].copy_from_slice(&self.free_space_offset.to_le_bytes());
        buf[13..17].copy_from_slice(&self.checksum.to_le_bytes());
    }

    pub fn deserialize(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < 17 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid page bytes - not enough size even for page header",
            ));
        }
        if buf[0..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid page magic bytes - not an OOR page",
            ));
        }

        Ok(Self {
            magic: MAGIC,
            page_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            page_type: match buf[8] {
                0 => PageType::FileHeader,
                1 => PageType::Data,
                2 => PageType::Index,
                3 => PageType::Free,
                4 => PageType::Overflow,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown page type discriminant",
                    ));
                }
            },
            record_count: u16::from_le_bytes(buf[9..11].try_into().unwrap()),
            free_space_offset: u16::from_le_bytes(buf[11..13].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[13..17].try_into().unwrap()),
        })
    }
}

// ===== PageFile =====

/// Low-level page I/O against the database file.
#[derive(Debug)]
pub struct PageFile {
    file: File,
    page_size: usize,
}

impl PageFile {
    pub fn open<P: AsRef<Path>>(path: P, page_size: usize) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)?;
        Ok(Self { file, page_size })
    }

    /// Return the number of complete pages currently stored in the file.
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

    /// Read a page from disk into the provided buffer,
    /// meanwhile compare the checksum it stores for validation.
    /// The buffer must be exactly `page_size` bytes.
    pub fn read_and_verify_page(&mut self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        self.read_page(page_id, buf)?;

        let stored = u32::from_le_bytes(buf[13..17].try_into().expect("slice -> array failed"));
        let computed = compute_checksum(buf);

        if stored != computed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "page {} checksum mismatch: stored={:#010x}, computed={:#010x}",
                    page_id, stored, computed
                ),
            ));
        }

        Ok(())
    }

    fn read_page(&mut self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        assert_eq!(buf.len(), self.page_size);

        let offset = u64::from(page_id) * self.page_size as u64;
        self.file.seek(io::SeekFrom::Start(offset))?;
        self.file.read_exact(buf)
    }

    /// Write a page buffer with its checksum to disk at the correct offset.
    pub fn write_page_with_checksum(&mut self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        let checksum = compute_checksum(buf);
        buf[13..17].copy_from_slice(&checksum.to_le_bytes());

        self.write_page(page_id, buf)
    }

    fn write_page(&mut self, page_id: u32, buf: &[u8]) -> io::Result<()> {
        assert_eq!(buf.len(), self.page_size);

        let offset = u64::from(page_id) * self.page_size as u64;
        self.file.seek(io::SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        // Note: we do NOT fsync here. Durability is the WAL's job (Module 4).
        // Calling fsync on every page write would destroy throughput —
        // a single fsync costs 1-10ms on SSD, 10-30ms on spinning disk.
        Ok(())
    }

    /// Allocate a new page at the end of the file. Returns the new page ID.
    pub fn allocate_page(&mut self) -> io::Result<u32> {
        let file_len = self.file.seek(io::SeekFrom::End(0))?;
        let page_id = (file_len / self.page_size as u64) as u32;
        let zeroed = vec![0_u8; self.page_size];
        self.file.write_all(&zeroed)?;

        Ok(page_id)
    }
}

// ===== util =====

/// CRC32 checksum of the page body (everything after the checksum field).
/// We zero the checksum field before computing so the checksum is
/// deterministic regardless of the previous checksum value.
pub(crate) fn compute_checksum(page_buf: &[u8]) -> u32 {
    // Checksum covers bytes 17..PAGE_SIZE (the body).
    // The header's checksum field (bytes 13..17) is excluded from the
    // computation — it stores the result.
    let body = &page_buf[PAGE_HEADER_SIZE..];
    crc32fast::hash(body)
}
