use criterion::{Criterion, criterion_group, criterion_main};
use foundation_track::module5::frame_batch::FrameBatchProcessor;

fn frame_batch_cycle() -> usize {
    let mut processor = FrameBatchProcessor::new(1000, 1024);

    // Simulate processing 100,000 frames in batches of 1,000.
    let mut total = 0;
    for _batch in 0..100 {
        for _frame in 0..1000 {
            // Claim a 256-byte payload slot — no malloc.
            if let Some(slot) = processor.claim_payload_slot(256) {
                slot[0] = 0xAA; // Simulate writing frame data.
            }
        }

        total += processor.flush_and_reset();
    }

    total
}

fn bench_batch_processor(c: &mut Criterion) {
    c.bench_function("batch processor", |b| b.iter(frame_batch_cycle));
}

criterion_group!(benches, bench_batch_processor);
criterion_main!(benches);
