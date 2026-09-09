use std::fmt;

use rand::{Rng, RngExt, SeedableRng, rngs::SmallRng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::tle_record::TleRecord;

pub struct Frame {
    station_id: String,
    tle: Option<TleRecord>,
    payload: Vec<u8>,
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = str::from_utf8(&self.payload).ok();

        f.debug_struct("Frame")
            .field("station_id", &self.station_id)
            .field("tle", &self.tle)
            .field("payload", &format_args!("{}", payload.unwrap_or("")))
            .field("raw_info", &self.payload)
            .finish()
    }
}

impl Frame {
    pub(crate) fn new(station_id: &str, tle: Option<TleRecord>, payload: Vec<u8>) -> Self {
        Self {
            station_id: station_id.to_owned(),
            tle,
            payload,
        }
    }
}

pub(crate) async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Option<Vec<u8>>> {
    // For 4-byte big-endian u32 length header
    let mut len_buf = [0u8; 4];

    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 65_536 {
        anyhow::bail!("frame too large: {len}");
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    Ok(Some(buf))
}

pub(crate) async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> anyhow::Result<()> {
    let len = (payload.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;

    Ok(())
}

pub(crate) fn create_frame() -> Vec<u8> {
    let mut rng = SmallRng::from_rng(&mut rand::rng());

    let len = rng.random_range::<u32, _>(..20);

    let mut buf = len.to_be_bytes().to_vec();

    let mut content_buf = vec![0u8; len as usize];
    rng.fill_bytes(&mut content_buf);

    buf.extend(content_buf);

    buf
}
