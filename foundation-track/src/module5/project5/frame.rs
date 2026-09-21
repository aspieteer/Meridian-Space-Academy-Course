use std::{collections::HashSet, fmt, io::Cursor, num::NonZeroU16, string::FromUtf8Error};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fastrand::Rng;

#[derive(Debug)]
pub struct Frame {
    header: FrameHeader,
    payload: Bytes,
}

#[derive(Debug)]
pub struct FrameHeader {
    timestamp_ms: u64,
    sequence: u64,
    satellite_id: u32,
    byte_count: Option<NonZeroU16>,
    station_id: u8,
    flags: u8,
}

const _SIZE: () = assert!(std::mem::size_of::<FrameHeader>() == 24);
const _ALIGN: () = assert!(std::mem::align_of::<FrameHeader>() == 8);

const ALNUM: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const FLAGS: [u8; 20] = [
    0x80_u8, 0x81, 0x8f, 0x70, 0x78, 0x7a, 0x50, 0x39, 0x10, 0x11, 0x15, 0x1d, 0x1f, 0x00, 0x01,
    0x03, 0x07, 0x08, 0x0b, 0x0f,
];

// ===== impl Frame =====

impl Frame {
    pub fn create(rng: &mut Rng, station_id: u8, max_payload: u16) -> Bytes {
        Self::create_at(rng, station_id, max_payload, current_timestamp_ms())
    }

    /// Like [`Frame::create`], but with an explicit timestamp, so batch
    /// generation can hoist the clock read out of the per-frame hot path.
    pub fn create_at(rng: &mut Rng, station_id: u8, max_payload: u16, timestamp_ms: u64) -> Bytes {
        // Full-range sequence: the dedup key space (satellite_id, sequence)
        // must not collapse at high frame rates.
        let sequence = rng.u64(0..1 << 15);
        let satellite_id = rng.u32(0..80);
        let flags: u8 = rng.choice(FLAGS).unwrap();
        let length = rng.u16(1..max_payload); // guarantee that length > 0

        let mut bytes = BytesMut::with_capacity(24 + length as usize); // exact size
        bytes.put_u64(timestamp_ms);
        bytes.put_u64(sequence);
        bytes.put_u32(satellite_id);
        bytes.put_u16(length);
        bytes.put_u8(station_id);
        bytes.put_u8(flags);

        // Fill the payload region *before* publishing it. `bytes[24..]` is
        // still empty here (len == 24), so index the spare capacity instead.
        let payload = {
            let spare = &mut bytes.spare_capacity_mut()[..length as usize];
            // SAFETY: `spare` covers `length` bytes of reserved capacity. Every
            // byte is written below (fill + remap) before `set_len` publishes
            // the region, and it is never read while uninitialized.
            unsafe { &mut *(spare as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]) }
        };
        rng.fill(payload);
        for b in payload.iter_mut() {
            *b = ALNUM[(*b % 62) as usize];
        }
        // SAFETY: bytes 24..24+length are now fully initialized, and
        // capacity == 24 + length.
        unsafe {
            bytes.set_len(24 + length as usize);
        }

        bytes.freeze()
    }

    /// Append `n` freshly generated frames to `batch`, sharing a single
    /// timestamp sample across the whole batch.
    pub fn extend_batch(
        rng: &mut Rng,
        station_id: u8,
        max_payload: u16,
        batch: &mut Vec<Bytes>,
        n: usize,
    ) {
        let timestamp_ms = current_timestamp_ms();
        batch.extend((0..n).map(|_| Self::create_at(rng, station_id, max_payload, timestamp_ms)));
    }

    pub fn split_into_header_and_payload(self) -> (FrameHeader, Bytes) {
        (self.header, self.payload)
    }

    pub fn from_bytes(src: Bytes) -> Option<Self> {
        Self::try_from(src).ok()
    }
}

impl TryFrom<Bytes> for Frame {
    type Error = Box<dyn std::error::Error>;

    fn try_from(src: Bytes) -> Result<Self, Self::Error> {
        let mut cursor = Cursor::new(&src[..]);

        try_extract_frame(&mut cursor).map_err(Into::into)
    }
}

pub(crate) fn try_extract_frame(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
    if src.remaining() < 24 {
        return Err(Error::Incomplete);
    }

    let timestamp_ms = src.get_u64();
    let sequence = src.get_u64();
    let satellite_id = src.get_u32();
    let byte_count = src.get_u16();
    let station_id = src.get_u8();
    let flags = src.get_u8();

    let byte_count = if byte_count == 0 {
        None
    } else {
        NonZeroU16::new(byte_count)
    };

    let payload = Bytes::copy_from_slice(get_payload(src, &byte_count)?);

    let frame = Frame {
        header: FrameHeader::new(
            timestamp_ms,
            sequence,
            satellite_id,
            byte_count,
            station_id,
            flags,
        ),
        payload,
    };

    Ok(frame)
}

/// Get the payload by the count or line ending
fn get_payload<'a>(
    src: &mut Cursor<&'a [u8]>,
    payload_length: &Option<NonZeroU16>,
) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    if let Some(length) = payload_length {
        let length = u64::from(length.get());
        if src.remaining() < length as usize {
            return Err(Error::Incomplete);
        }

        let end = start as u64 + length;
        src.set_position(end);
        return Ok(&src.get_ref()[start..end as usize]);
    }
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the \n
            src.set_position((i + 2) as u64);
            // Return the line
            return Ok(&src.get_ref()[start..i]);
        }
        // if src.get_ref()[i] == b'\0' {
        //     // We found a c-style terminator, update the position to be *after* the \0
        //     src.set_position((i + 1) as u64);
        //     // Return the line
        //     return Ok(&src.get_ref()[start..i]);
        // }
    }

    Err(Error::Incomplete)
}

// ===== impl FrameHeader =====

/// This is where the processor pipeline works on
impl FrameHeader {
    fn new(
        timestamp_ms: u64,
        sequence: u64,
        satellite_id: u32,
        byte_count: Option<NonZeroU16>,
        station_id: u8,
        flags: u8,
    ) -> Self {
        Self {
            timestamp_ms,
            sequence,
            satellite_id,
            byte_count,
            station_id,
            flags,
        }
    }

    pub(crate) fn validate(&self) -> bool {
        self.flags & 0x80 == 0 // Simulate CRC: high bit = error flag
    }
}

// Pipeline

pub fn deduplicate_indices(headers: &[FrameHeader]) -> Vec<usize> {
    // Initialize a HashSet for deduplication
    let mut seen = HashSet::with_capacity(headers.len());

    headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| {
            if seen.insert((h.satellite_id, h.sequence)) {
                Some(i)
            } else {
                None
            }
        })
        .collect()
}

pub fn sort_indices_by_timestamp(indices: &mut [usize], headers: &[FrameHeader]) {
    indices.sort_unstable_by_key(|&i| headers[i].timestamp_ms);
}

// ===== utils ====

fn current_timestamp_ms() -> u64 {
    use jiff::Timestamp;

    Timestamp::now()
        .duration_since(Timestamp::UNIX_EPOCH)
        .as_millis() as u64
}

// ===== Frame Error =====

#[derive(Debug)]
pub(crate) enum Error {
    /// Not enough data is available to parse a message
    Incomplete,
    /// Invalid message encoding
    Other(super::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(f),
            Error::Other(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(src: String) -> Self {
        Self::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Self {
        Self::Other(src.into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_frame_parsing() -> Result<(), Error> {
        let mut bytes: Vec<u8> = vec![0u8; 24];
        bytes.extend(vec![70, 70, 70, 70, 70, 70, 70, b'\r', b'\n']);
        bytes.extend(vec![2u8; 20]);
        bytes.push(0u8);
        bytes.extend(vec![5u8; 3]);
        bytes.extend(vec![50, 50, 60, 60, 70]);

        let mut c = Cursor::new(bytes.as_slice());
        while c.has_remaining() {
            let res = try_extract_frame(&mut c)?;
            println!("{:?}", res);
        }

        Ok(())
    }

    #[test]
    fn test_frame_incomplete() {
        let b1 = vec![0u8; 23];
        let mut c1 = Cursor::new(b1.as_slice());
        let res1 = try_extract_frame(&mut c1);
        assert!(matches!(res1, Err(Error::Incomplete)));

        let b2 = vec![0u8; 36];
        let mut c2 = Cursor::new(b2.as_slice());
        let res2 = try_extract_frame(&mut c2);
        assert!(matches!(res2, Err(Error::Incomplete)));

        let b3 = vec![1u8; 36];
        let mut c3 = Cursor::new(b3.as_slice());
        let res3 = try_extract_frame(&mut c3);
        assert!(matches!(res3, Err(Error::Incomplete)));
    }

    #[test]
    fn create_and_extract() {
        let mut rng = fastrand::Rng::new();
        for _ in 0..3 {
            let b = Frame::create(&mut rng, 0, 256);
            // wire layout: 24-byte header (byte_count at offset 20..22), then payload
            let declared_len = u16::from_be_bytes([b[20], b[21]]) as usize;
            assert_eq!(b.len(), 24 + declared_len, "frame length must match header");
            assert!(
                b[24..].iter().all(u8::is_ascii_alphanumeric),
                "payload must be initialized alphanumeric data, not garbage"
            );

            let mut cursor = Cursor::new(&b[..]);
            let frame = try_extract_frame(&mut cursor).expect("frame must parse");
            assert_eq!(frame.payload.len(), declared_len);
        }
    }

    #[test]
    fn batch_frames_share_one_timestamp() {
        let mut rng = fastrand::Rng::new();
        let mut batch = Vec::new();
        Frame::extend_batch(&mut rng, 1, 256, &mut batch, 8);

        assert_eq!(batch.len(), 8);
        let first_ts = u64::from_be_bytes(batch[0][0..8].try_into().unwrap());
        assert!(
            batch
                .iter()
                .all(|f| u64::from_be_bytes(f[0..8].try_into().unwrap()) == first_ts),
            "all frames in a batch share the sampled timestamp"
        );
    }
}
