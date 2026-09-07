use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct TleRecord {
    norad_id: u32,
    name: String,
    line1: String,
    line2: String,
}
