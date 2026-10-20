pub mod part1;
pub mod part2;
pub mod part3;
pub mod project1;

mod config;
pub(crate) use config::{
    MAGIC, MAX_USAGE_COUNT, PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_SIZE, TOMBSTONE,
};
