use std::mem::{align_of, size_of};

/// A telemetry frame header optimized for sequential scanning.
/// Fields ordered by alignment (descending) to minimize padding.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
struct TelemetryHeader {
    timestamp_ms: u64,  // 8 bytes — largest alignment first
    sequence: u64,      // 8 bytes
    satellite_id: u32,  // 4 bytes
    byte_count: u32,    // 4 bytes
    flags: u8,          // 1 byte
    station_id: u8,     // 1 byte
    _reserved: [u8; 2], // 2 bytes explicit pad — documented intent
}

// Lock in the expected size at compile time.
// If a future change causes unexpected padding, this fails to compile.
const _SIZE_CHECK: () = assert!(size_of::<TelemetryHeader>() == 32);
const _ALIGN_CHECK: () = assert!(align_of::<TelemetryHeader>() == 8);

/// A per-uplink session counter, cache-line aligned to prevent false sharing.
/// 48 sessions each updating their own counter never contend on a shared line.
#[allow(dead_code)]
#[repr(align(64))]
struct SessionCounter {
    frames_received: u64,
    bytes_received: u64,
    frames_dropped: u64,
    _pad: [u8; 40], // Pad to fill 64-byte cache line: 3×8 + 40 = 64.
}

const _COUNTER_ALIGN: () = assert!(align_of::<SessionCounter>() == 64);
const _COUNTER_SIZE: () = assert!(size_of::<SessionCounter>() == 64);

fn main() {
    println!("TelemetryHeader: {} bytes", size_of::<TelemetryHeader>());
    println!(
        "SessionCounter:  {} bytes (cache-line aligned)",
        size_of::<SessionCounter>()
    );

    // Verify that an array of counters places each on its own cache line.
    let counters: Vec<SessionCounter> = (0..5)
        .map(|_| SessionCounter {
            frames_received: 0,
            bytes_received: 0,
            frames_dropped: 0,
            _pad: [0; 40],
        })
        .collect();

    // Each counter is 64 bytes and 64-byte aligned — no false sharing.
    for (i, c) in counters.iter().enumerate() {
        let addr = c as *const _ as usize;
        println!(
            "counter[{i}] at 0x{addr:x} (aligned: {})",
            addr.is_multiple_of(64)
        );
    }
}
