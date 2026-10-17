/// Simulates a frame batch processor using a bump-style pre-allocated pool.
/// Each frame's payload is drawn from the batch buffer.
/// When the batch is complete, the buffer is reset — no individual frees.
pub struct FrameBatchProcessor {
    /// Pre-allocated buffer for all frame payloads in one batch.
    payload_pool: Vec<u8>,
    pool_offset: usize,
    batch_size: usize,
    frames_in_batch: usize,
}

impl FrameBatchProcessor {
    pub fn new(batch_size: usize, max_payload_per_frame: usize) -> Self {
        Self {
            payload_pool: vec![0u8; batch_size * max_payload_per_frame],
            pool_offset: 0,
            batch_size,
            frames_in_batch: 0,
        }
    }

    /// Claim space for a frame payload from the pool.
    /// Returns a mutable slice for the caller to fill.
    pub fn claim_payload_slot(&mut self, size: usize) -> Option<&mut [u8]> {
        if self.frames_in_batch >= self.batch_size {
            return None; // Batch full
        }

        let end = self.pool_offset + size;
        if end > self.payload_pool.len() {
            return None; // Pool exhausted
        }

        let slot = &mut self.payload_pool[self.pool_offset..end];
        self.pool_offset = end;
        self.frames_in_batch += 1;

        Some(slot)
    }

    /// Process the current batch and reset for the next one.
    /// All payload slots are implicitly freed — no individual deallocation.
    pub fn flush_and_reset(&mut self) -> usize {
        let processed = self.frames_in_batch;
        self.pool_offset = 0;
        self.frames_in_batch = 0;

        processed
    }
}
