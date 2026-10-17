// Page settings
pub(crate) const PAGE_SIZE: usize = 4096;
pub(crate) const PAGE_HEADER_SIZE: usize = 17;
// Relation File Size
const SEGMENT_SIZE: u32 = 1024 * 1024 * 1024; // 1 GiB
pub(crate) const PAGES_PER_FILE: u32 = SEGMENT_SIZE / PAGE_SIZE as u32;
// Magic Bytes
pub(crate) const MAGIC: [u8; 4] = [0x4F, 0x4F, 0x52, 0x31]; // "OOR1"
// Slot indexing inside Page
pub(crate) const SLOT_SIZE: usize = 4; // 2 bytes offset + 2 bytes length
// TOMBSTONE flag indicating deleted slots that overwrites the offset location
pub(crate) const TOMBSTONE: u16 = 0xFFFF;
// Max usage count used for CLOCK hand scanning
pub(crate) const MAX_USAGE_COUNT: u8 = 5;
