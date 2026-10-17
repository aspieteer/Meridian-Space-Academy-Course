use crate::module1::{
    PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_SIZE, TOMBSTONE,
    part1::page::{PageHeader, PageType},
};

/// A slotted page that stores variable-length records.
pub struct SlottedPage {
    data: [u8; PAGE_SIZE],
}

impl SlottedPage {
    pub fn new(page_id: u32) -> Self {
        let mut page = Self {
            data: [0u8; PAGE_SIZE],
        };

        let header = PageHeader::new(page_id, PageType::Data);
        header.serialize(&mut page.data);

        page
    }

    /// Number of slots (including tombstoned ones).
    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.data[9..11].try_into().unwrap())
    }

    pub fn set_slot_count(&mut self, count: u16) {
        self.data[9..11].copy_from_slice(&count.to_le_bytes());
    }

    /// Byte offset where the record region begins (records grow downward).
    pub fn record_region_start(&self) -> u16 {
        u16::from_le_bytes(self.data[11..13].try_into().unwrap())
    }

    pub fn set_record_region_start(&mut self, offset: u16) {
        self.data[11..13].copy_from_slice(&offset.to_le_bytes());
    }

    /// Read a slot directory entry.
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
        let slot_dir_end = PAGE_HEADER_SIZE + self.slot_count() as usize * SLOT_SIZE;
        let rec_start = self.record_region_start() as usize;

        rec_start.saturating_sub(slot_dir_end)
    }

    /// Find a tombstoned slot to reuse, or allocate a new one.
    pub fn find_free_slot(&mut self) -> u16 {
        let count = self.slot_count();
        for i in 0..count {
            let (offset, _) = self.read_slot(i);
            if offset == TOMBSTONE {
                return i;
            }
        }
        // No tombstoned slots — extend the directory
        self.set_slot_count(count + 1);
        count
    }

    /// Insert a record. Returns the slot ID on success.
    pub fn insert(&mut self, record: &[u8]) -> Option<u16> {
        let needed = record.len() + SLOT_SIZE; // record bytes + new slot entry
        if self.free_space() < needed {
            return None; // Page full — caller should try compaction or new page
        }

        // Find a tombstoned slot to reuse, or allocate a new one
        let slot_id = self.find_free_slot();

        // Place the record at the end of the record region
        let new_offset = self.record_region_start() - record.len() as u16;
        let start = new_offset as usize;
        let end = start + record.len();
        self.data[start..end].copy_from_slice(record);

        // Update the slot directory
        self.write_slot(slot_id, new_offset, record.len() as u16);
        self.set_record_region_start(new_offset);

        Some(slot_id)
    }

    /// Look up a record by slot ID. Returns None if the slot is
    /// tombstoned or out of range.
    pub fn get(&self, slot_id: u16) -> Option<&[u8]> {
        if self.slot_count() <= slot_id {
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
        if self.slot_count() <= slot_id {
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
        let slot_count = self.slot_count();

        // Collect live records: (slot_id, data_copied)
        let mut live_records: Vec<(u16, Vec<u8>)> = Vec::new();
        for slot_id in 0..slot_count {
            let (offset, length) = self.read_slot(slot_id);
            if offset != TOMBSTONE {
                let start = offset as usize;
                let end = start + length as usize;
                live_records.push((slot_id, self.data[start..end].to_vec()));
            }
        }

        // Rewrite records contiguously from the end of the page
        let mut curosr = PAGE_SIZE as u16;
        live_records.into_iter().for_each(|(slot_id, record)| {
            curosr -= record.len() as u16;
            let start = curosr as usize;
            let end = start + record.len();
            self.data[start..end].copy_from_slice(&record);
            self.write_slot(slot_id, curosr, record.len() as u16);
        });

        self.set_record_region_start(curosr);
    }
}
