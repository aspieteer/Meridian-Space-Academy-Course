pub struct HeaderExtraction {
    pub(crate) sequence: u64,
    pub(crate) satellite_id: u32,
    pub(crate) flag: u8,
}

impl HeaderExtraction {
    pub fn new(sequence: u64, satellite_id: u32, flag: u8) -> Self {
        Self {
            sequence,
            satellite_id,
            flag,
        }
    }
}
