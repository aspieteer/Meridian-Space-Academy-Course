use std::io;

use super::PageNo;
use crate::module1::{MAGIC, PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_SIZE, TOMBSTONE};

// This file implements a slotted page structure.

// ===== Slotted Page =====

pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_no: PageNo) -> Self {
        let mut page = Self {
            data: [0u8; PAGE_SIZE],
        };

        let header = PageHeader::new(page_no, PageType::Data);
        header.serialize(&mut page.data);

        page
    }

    pub fn initialize(page: &mut [u8; PAGE_SIZE], page_no: PageNo) {
        let header = PageHeader::new(page_no, PageType::Data);
        header.serialize(page);
    }

    pub fn from_bytes(data: &[u8; PAGE_SIZE], expected_page_no: PageNo) -> io::Result<Self> {
        let header = PageHeader::deserialize(data)?;
        let lower = PAGE_HEADER_SIZE + header.record_count as usize * SLOT_SIZE;
        let upper = header.free_space_offset as usize;
        if header.page_no != expected_page_no
            || header.page_type != PageType::Data
            || lower > upper
            || upper > PAGE_SIZE
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid data page header",
            ));
        }
        let page = Self { data: *data };
        for slot_id in 0..header.record_count {
            let (offset, length) = page.read_slot(slot_id);
            if offset != TOMBSTONE
                && ((offset as usize) < upper || (offset as usize + length as usize) > PAGE_SIZE)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid slot bounds",
                ));
            }
        }
        Ok(page)
    }

    pub fn copy_to(&self, dest: &mut [u8; PAGE_SIZE]) {
        dest.copy_from_slice(&self.data);
    }

    pub fn record_count(&self) -> u16 {
        u16::from_le_bytes(self.data[9..11].try_into().unwrap())
    }

    pub fn set_record_count(&mut self, count: u16) {
        self.data[9..11].copy_from_slice(&count.to_le_bytes());
    }

    pub fn record_region_offset(&self) -> u16 {
        u16::from_le_bytes(self.data[11..13].try_into().unwrap())
    }

    pub fn set_record_region_offset(&mut self, offset: u16) {
        self.data[11..13].copy_from_slice(&offset.to_le_bytes());
    }

    /// Read a slot directory entry.
    /// Return its offset and length.
    pub fn read_slot(&self, slot_id: u16) -> (u16, u16) {
        let base = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;

        let offset = u16::from_le_bytes(self.data[base..base + 2].try_into().unwrap());
        let length = u16::from_le_bytes(self.data[base + 2..base + 4].try_into().unwrap());

        (offset, length)
    }

    pub fn write_slot(&mut self, slot_id: u16, offset: u16, length: u16) {
        let base = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;

        self.data[base..base + 2].copy_from_slice(&offset.to_le_bytes());
        self.data[base + 2..base + 4].copy_from_slice(&length.to_le_bytes());
    }

    /// Free space available for new records + slot entries.
    pub fn free_space(&self) -> usize {
        let record_count = self.record_count();

        let upper = self.record_region_offset() as usize;
        let lower = PAGE_HEADER_SIZE + record_count as usize * SLOT_SIZE;

        upper.saturating_sub(lower)
    }

    /// Insert a record. Returns the slot ID on success.
    pub fn insert(&mut self, record: &[u8]) -> Option<u16> {
        if record.len() > u16::MAX as usize {
            return None;
        }
        let count = self.record_count();
        let reused_slot = (0..count).find(|&i| self.read_slot(i).0 == TOMBSTONE);
        let needed = record.len() + if reused_slot.is_some() { 0 } else { SLOT_SIZE };
        if self.free_space() < needed {
            return None; // Page full — caller should try compaction or new page
        }

        // Find a tombstoned slot to reuse, or allocate a new one.
        let slot_id = if let Some(id) = reused_slot {
            id
        } else {
            self.set_record_count(count + 1);
            count
        };

        // Place the record at the end of the record region
        let new_offset = self.record_region_offset() - record.len() as u16;
        let start = new_offset as usize;
        let end = start + record.len();
        self.data[start..end].copy_from_slice(record);

        // Update the slot directory
        self.write_slot(slot_id, new_offset, record.len() as u16);
        self.set_record_region_offset(new_offset);

        Some(slot_id)
    }

    /// Look up a record by slot ID. Returns None if the slot is
    /// tombstoned or out of range.
    pub fn get(&self, slot_id: u16) -> Option<&[u8]> {
        if slot_id >= self.record_count() {
            return None;
        }
        let (offset, length) = self.read_slot(slot_id);
        if offset == TOMBSTONE {
            return None; // Deleted record
        }

        let start = offset as usize;
        let end = start + length as usize;

        Some(&self.data[start..end])
    }

    /// Delete a record by tombstoning its slot entry.
    pub fn delete(&mut self, slot_id: u16) -> bool {
        if slot_id >= self.record_count() {
            return false;
        }

        let (offset, _) = self.read_slot(slot_id);
        if offset == TOMBSTONE {
            return false; // Already deleted
        }

        self.write_slot(slot_id, TOMBSTONE, 0);
        true
    }

    /// Compact the page: slide all live records to the end,
    /// eliminating gaps from deleted records.
    pub fn compact(&mut self) {
        let record_count = self.record_count();

        // Collect live records: (slot_id, data_copied)
        let mut live_records: Vec<(u16, Vec<u8>)> = Vec::new();
        for slot_id in 0..record_count {
            let (offset, length) = self.read_slot(slot_id);
            if offset != TOMBSTONE {
                let start = offset as usize;
                let end = start + length as usize;
                live_records.push((slot_id, self.data[start..end].to_vec()));
            }
        }

        // Rewrite records contiguously from the end of the page
        let mut cursor = PAGE_SIZE as u16;
        live_records.into_iter().for_each(|(slot_id, record)| {
            cursor -= record.len() as u16;
            let start = cursor as usize;
            let end = start + record.len();
            self.data[start..end].copy_from_slice(&record);
            self.write_slot(slot_id, cursor, record.len() as u16);
        });

        self.set_record_region_offset(cursor);
    }
}

// ===== Page Header =====

#[derive(Debug)]
pub struct PageHeader {
    magic: [u8; 4],
    page_no: PageNo,
    page_type: PageType,
    record_count: u16,
    free_space_offset: u16,
    checksum: u32,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PageType {
    FileHeader = 0,
    Data = 1,
    Index = 2,
    Free = 3,
    Overflow = 4,
}

impl PageHeader {
    pub fn new(page_no: u32, page_type: PageType) -> Self {
        Self {
            magic: MAGIC,
            page_no,
            page_type,
            record_count: 0,
            free_space_offset: PAGE_SIZE as u16,
            checksum: 0,
        }
    }

    pub(crate) fn serialize(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.page_no.to_le_bytes());
        buf[8] = self.page_type as u8;
        buf[9..11].copy_from_slice(&self.record_count.to_le_bytes());
        buf[11..13].copy_from_slice(&self.free_space_offset.to_le_bytes());
        buf[13..17].copy_from_slice(&self.checksum.to_le_bytes());
    }

    pub(crate) fn deserialize(buf: &[u8]) -> io::Result<Self> {
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
            page_no: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
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
