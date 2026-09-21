use std::io::Cursor;

use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use super::frame::{self, Frame};

#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`,
    // which provides write level buffering.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`.
    /// Read and write buffers are initialized.
    pub fn new(socket: TcpStream) -> Self {
        Self {
            // 64KiB write buffer: a full 256-frame batch (~39KiB avg) fits
            // without an intermediate socket write before the explicit flush.
            stream: BufWriter::with_capacity(64 * 1024, socket),
            // FrameHeader 24B + payload(vec![u8; 256] = 32B approximately, 60 Bytes reserved for a frame),
            // set 60KiB read buffer initially.
            buffer: BytesMut::with_capacity(60 * 1024),
        }
    }

    pub(crate) fn stream(&self) -> &BufWriter<TcpStream> {
        &self.stream
    }

    pub(crate) fn stream_mut(&mut self) -> &mut BufWriter<TcpStream> {
        &mut self.stream
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // `0` indicates "end of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow::anyhow!("connection reset by peer"));
                }
            }
        }
    }

    fn parse_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

        match frame::try_extract_frame(&mut buf) {
            Ok(frame) => {
                let len = buf.position() as usize;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Write a batch of frames into the buffered stream, flushing once at the end.
    ///
    /// `write_all` on a `BufWriter` only touches the socket when the internal
    /// buffer fills, so a whole batch costs ~1 flush syscall instead of one
    /// write+flush per frame.
    pub(crate) async fn write_frames(&mut self, frames: &[Bytes]) -> anyhow::Result<()> {
        for frame in frames {
            self.stream.write_all(&frame[..]).await?;
        }
        // trace!, not info!: at max throughput this fires thousands of times
        // per second and the fmt layer would become the bottleneck.
        tracing::trace!(frames = frames.len(), "flushing batch");
        self.stream.flush().await.map_err(Into::into)
    }

    // pub(crate) async fn write_frame(&mut self, frame_bytes: Bytes) -> anyhow::Result<()> {
    //     self.write_frames(std::slice::from_ref(&frame_bytes)).await
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn loopback() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn read_frame_drains_consumed_bytes() {
        let (mut raw, server) = loopback().await;
        let mut conn = Connection::new(server);

        let mut rng = fastrand::Rng::new();
        let f1 = Frame::create(&mut rng, 1, 256);
        let f2 = Frame::create(&mut rng, 1, 256);

        let mut wire = Vec::with_capacity(f1.len() + f2.len());
        wire.extend_from_slice(&f1[..]);
        wire.extend_from_slice(&f2[..]);
        raw.write_all(&wire).await.unwrap();

        let (_, p1) = conn
            .read_frame()
            .await
            .unwrap()
            .unwrap()
            .split_into_header_and_payload();
        let (_, p2) = conn
            .read_frame()
            .await
            .unwrap()
            .unwrap()
            .split_into_header_and_payload();

        assert_ne!(p1, p2, "second read must not re-parse the first frame");
        assert!(conn.buffer.is_empty(), "consumed bytes must be drained");
    }

    #[tokio::test]
    async fn write_frames_sends_entire_batch() {
        let (client, server) = loopback().await;
        let mut writer = Connection::new(client);
        let mut reader = Connection::new(server);

        let mut rng = fastrand::Rng::new();
        let frames: Vec<Bytes> = (0..3).map(|_| Frame::create(&mut rng, 7, 256)).collect();
        writer.write_frames(&frames).await.unwrap();

        for expected in &frames {
            let frame = reader.read_frame().await.unwrap().unwrap();
            let (_, payload) = frame.split_into_header_and_payload();
            // wire layout: 24-byte header, then payload
            assert_eq!(&payload[..], &expected[24..]);
        }
    }
}
