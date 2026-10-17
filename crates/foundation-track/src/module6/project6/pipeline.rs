use std::collections::HashSet;

use super::header::HeaderExtraction;

pub struct Deduplicator {
    seen: HashSet<u128>,
    indices: Vec<usize>,
}

// ===== Pipeline stages =====

#[inline(never)]
fn validate(headers: &[HeaderExtraction]) -> Vec<u128> {
    headers
        .iter()
        .filter(|h| h.flag & 0x80 == 0)
        .map(|h| assemble_u128(h.sequence, h.satellite_id))
        .collect()
}

impl Deduplicator {
    pub fn new(cap: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(cap),
            indices: Vec::with_capacity(cap),
        }
    }

    #[inline(never)]
    fn process(&mut self, valid: &[u128]) -> &[usize] {
        self.seen.clear();
        self.indices.clear();

        for (i, &key) in valid.iter().enumerate() {
            if self.seen.insert(key) {
                self.indices.push(i);
            }
        }

        &self.indices
    }
}

#[inline(never)]
fn forward(valid: &[u128], unique: &[usize]) -> usize {
    unique
        .iter()
        .map(|&i| {
            let (seq, _) = split_back_from_u128(valid[i]);
            seq as usize
        })
        .sum()
}

pub fn run_pipeline(headers: &[HeaderExtraction], dedup: &mut Deduplicator) -> usize {
    let valid = validate(headers);
    let unique = dedup.process(&valid);
    forward(&valid, unique)
}

// ===== utils =====

pub fn assemble_u128(high: u64, low: u32) -> u128 {
    ((high as u128) << 32) | (low as u128)
}

fn split_back_from_u128(combined: u128) -> (u64, u32) {
    let high = (combined >> 32) as u64;
    let low = combined as u32;

    (high, low)
}
