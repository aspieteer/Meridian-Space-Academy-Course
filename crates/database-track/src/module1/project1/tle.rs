use std::io;

/// A Two-Line Element record for a tracked orbital object.
#[derive(Debug, Clone, PartialEq)]
pub struct TleRecord {
    /// NORAD catalog number (unique object ID, e.g., 25544 for ISS)
    pub norad_id: u32,
    /// International designator (e.g., "98067A")
    pub intl_designator: [u8; 8],
    /// Epoch year + fractional day (e.g., 24045.5 = Feb 14 2024, 12:00 UTC)
    pub epoch: f64,
    /// Mean motion (revolutions per day)
    pub mean_motion: f64,
    /// Eccentricity (dimensionless, 0–1)
    pub eccentricity: f64,
    /// Inclination (degrees)
    pub inclination: f64,
    /// Right ascension of ascending node (degrees)
    pub raan: f64,
    /// Argument of perigee (degrees)
    pub arg_perigee: f64,
    /// Mean anomaly (degrees)
    pub mean_anomaly: f64,
    /// Drag term (B* coefficient)
    pub bstar: f64,
    /// Element set number (for provenance tracking)
    pub element_set: u16,
    /// Revolution number at epoch
    pub rev_number: u32,
}

impl TleRecord {
    pub const SERIALIZED_SIZE: usize = 82;

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(82);

        buf.extend_from_slice(&self.norad_id.to_le_bytes());
        buf.extend_from_slice(&self.intl_designator);
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        buf.extend_from_slice(&self.mean_motion.to_le_bytes());
        buf.extend_from_slice(&self.eccentricity.to_le_bytes());
        buf.extend_from_slice(&self.inclination.to_le_bytes());
        buf.extend_from_slice(&self.raan.to_le_bytes());
        buf.extend_from_slice(&self.arg_perigee.to_le_bytes());
        buf.extend_from_slice(&self.mean_anomaly.to_le_bytes());
        buf.extend_from_slice(&self.bstar.to_le_bytes());
        buf.extend_from_slice(&self.element_set.to_le_bytes());
        buf.extend_from_slice(&self.rev_number.to_le_bytes());

        buf
    }

    pub fn deserialize(buf: &[u8]) -> io::Result<Self> {
        if buf.len() != Self::SERIALIZED_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid TLE record length",
            ));
        }

        fn take<const N: usize>(buf: &[u8], cursor: &mut usize) -> [u8; N] {
            let bytes = buf[*cursor..*cursor + N].try_into().unwrap();
            *cursor += N;
            bytes
        }

        let mut cursor = 0;
        Ok(Self {
            norad_id: u32::from_le_bytes(take(buf, &mut cursor)),
            intl_designator: take(buf, &mut cursor),
            epoch: f64::from_le_bytes(take(buf, &mut cursor)),
            mean_motion: f64::from_le_bytes(take(buf, &mut cursor)),
            eccentricity: f64::from_le_bytes(take(buf, &mut cursor)),
            inclination: f64::from_le_bytes(take(buf, &mut cursor)),
            raan: f64::from_le_bytes(take(buf, &mut cursor)),
            arg_perigee: f64::from_le_bytes(take(buf, &mut cursor)),
            mean_anomaly: f64::from_le_bytes(take(buf, &mut cursor)),
            bstar: f64::from_le_bytes(take(buf, &mut cursor)),
            element_set: u16::from_le_bytes(take(buf, &mut cursor)),
            rev_number: u32::from_le_bytes(take(buf, &mut cursor)),
        })
    }
}
