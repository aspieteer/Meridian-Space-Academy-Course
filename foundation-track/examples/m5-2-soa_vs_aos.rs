use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

/// Simplified O(n) altitude band screening (not the full O(n²) conjunction check).
/// Finds objects in a dangerous altitude band. SoA makes this trivially parallel
/// and cache-friendly: the working set is just Vec<f64>.
fn screen_altitude_band(
    altitudes_km: &[f64],
    norad_ids: &[u32],
    min_km: f64,
    max_km: f64,
) -> Vec<u32> {
    assert_eq!(altitudes_km.len(), norad_ids.len());

    // Sequential: all altitudes fit in one contiguous slice.
    // Hardware prefetcher maximises cache utilisation.
    altitudes_km
        .par_iter()
        .zip(norad_ids.par_iter())
        .filter_map(|(&alt, &id)| {
            if alt >= min_km && alt <= max_km {
                Some(id)
            } else {
                None
            }
        })
        .collect()
}

/// Transitioning an existing AoS codebase to SoA does not require a full rewrite.
/// Extract the hot fields into a companion SoA structure, index both by the same key
// Existing AoS type — not changed, other code still uses it.
#[allow(dead_code)]
#[derive(Clone, Debug)]
struct TelemetryFrame {
    satellite_id: u32,
    sequence: u64,
    timestamp_ms: u64,
    station_id: u8,
    payload: Vec<u8>,
}

// New SoA hot path for bulk sequence-number deduplication.
// Built from the AoS data; kept in sync on insert.
struct FrameSequenceIndex {
    satellite_ids: Vec<u32>,
    sequences: Vec<u64>,
}

impl FrameSequenceIndex {
    fn from_frames(frames: &[TelemetryFrame]) -> Self {
        Self {
            satellite_ids: frames.iter().map(|f| f.satellite_id).collect(),
            sequences: frames.iter().map(|f| f.sequence).collect(),
        }
    }

    /// Find all duplicate (satellite_id, sequence) pairs — O(n) scan,
    /// cache-friendly because both vecs are small and contiguous.
    fn find_duplicates(&self) -> Vec<usize> {
        let mut seen = std::collections::HashSet::new();

        self.satellite_ids
            .iter()
            .zip(self.sequences.iter())
            .enumerate()
            .filter_map(|(i, (&id, &seq))| {
                if !seen.insert((id, seq)) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }
}

fn main() {
    // Simulate a 10,000-object catalog.
    let altitudes_km: Vec<f64> = (0..10_000u32)
        .into_par_iter()
        .map(|i| 350.0 + (i as f64) * 0.05)
        .collect();
    let norad_ids: Vec<u32> = (0..10_000u32).collect();

    // Screen for objects in the 400–450 km band (high debris density).
    const MIN_KM: f64 = 400.0;
    const MAX_KM: f64 = 450.0;
    let alerts = screen_altitude_band(&altitudes_km, &norad_ids, MIN_KM, MAX_KM);
    println!("{} objects in 400–450km band", alerts.len());

    // Verify the working set is contiguous and predictable:
    let working_set_bytes = altitudes_km.len() * std::mem::size_of::<f64>();
    println!("working set: {} KB", working_set_bytes / 1024);

    // Scan for duplicate Frames
    let frames: Vec<TelemetryFrame> = (0..100u32)
        .map(|i| TelemetryFrame {
            satellite_id: i % 10,
            sequence: (i / 10) as u64 % 9,
            timestamp_ms: 1_700_000_000 + i as u64,
            station_id: (1 + i as u8) % u8::MAX,
            payload: vec![i as u8; 128],
        })
        .collect();

    let index = FrameSequenceIndex::from_frames(&frames);
    let dups = index.find_duplicates();
    println!("{} duplicate frames found:\n{:?}", dups.len(), dups);
}
