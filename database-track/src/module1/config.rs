pub(crate) const PAGE_SIZE: usize = 4096;
pub(crate) const PAGE_HEADER_SIZE: usize = 17;
pub(crate) const SLOT_SIZE: usize = 4; // 2 bytes offset + 2 bytes length

pub(crate) const MAGIC: [u8; 4] = [0x4F, 0x4F, 0x52, 0x31]; // "OOR1"
pub(crate) const TOMBSTONE: u16 = 0xFFFF;
