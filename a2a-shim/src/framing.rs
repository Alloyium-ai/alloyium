//! Section A.2 frame codec.
//!
//! Frames are encoded as a little-endian u32 length covering the one-byte type
//! plus payload. The maximum frame length is 16 MiB.

use std::io::{self, ErrorKind};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Mcp = 0x01,
    Ctrl = 0x02,
}

impl FrameType {
    fn from_byte(byte: u8) -> io::Result<Self> {
        match byte {
            0x01 => Ok(Self::Mcp),
            0x02 => Ok(Self::Ctrl),
            _ => Err(invalid_data("bad_type")),
        }
    }

    fn to_byte(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub payload: Vec<u8>,
}

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Frame> {
    let mut len_buf = [0_u8; 4];
    r.read_exact(&mut len_buf).await?;

    let len = u32::from_le_bytes(len_buf) as usize;
    if len < 1 {
        return Err(invalid_data("bad_length"));
    }
    if len > MAX_FRAME {
        return Err(invalid_data("frame_too_large"));
    }

    let mut type_buf = [0_u8; 1];
    r.read_exact(&mut type_buf).await?;
    let frame_type = FrameType::from_byte(type_buf[0])?;

    let mut payload = vec![0_u8; len - 1];
    r.read_exact(&mut payload).await?;

    Ok(Frame {
        frame_type,
        payload,
    })
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    t: FrameType,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > MAX_FRAME - 1 {
        return Err(invalid_data("frame_too_large"));
    }

    let len = (payload.len() + 1) as u32;
    w.write_all(&len.to_le_bytes()).await?;
    w.write_all(&[t.to_byte()]).await?;
    w.write_all(payload).await?;
    w.flush().await
}

/// Write a single frame whose body is streamed in `chunk_size` socket-writes.
/// This is ONE contiguous length-prefixed frame on the wire (§A-v3 (A): one MCP
/// message = one frame, ≤16MiB); the chunking is purely cooperative yielding for
/// large payloads and does NOT split the message into multiple frames (which
/// would desync a contiguous-length receiver).
pub async fn write_frame_chunked<W: AsyncWrite + Unpin>(
    w: &mut W,
    t: FrameType,
    payload: &[u8],
    chunk_size: usize,
) -> io::Result<()> {
    if payload.len() > MAX_FRAME - 1 {
        return Err(invalid_data("frame_too_large"));
    }

    let len = (payload.len() + 1) as u32;
    w.write_all(&len.to_le_bytes()).await?;
    w.write_all(&[t.to_byte()]).await?;

    let step = chunk_size.max(1);
    for chunk in payload.chunks(step) {
        w.write_all(chunk).await?;
    }

    w.flush().await
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        write_frame(&mut writer, FrameType::Ctrl, br#"{"t":"ping"}"#)
            .await
            .unwrap();

        let frame = read_frame(&mut reader).await.unwrap();
        assert_eq!(frame.frame_type, FrameType::Ctrl);
        assert_eq!(frame.payload, br#"{"t":"ping"}"#);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_len_less_than_one() {
        let bytes = 0_u32.to_le_bytes();
        let mut input = bytes.as_slice();

        let err = read_frame(&mut input).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "bad_length");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_oversize() {
        let bytes = ((MAX_FRAME as u32) + 1).to_le_bytes();
        let mut input = bytes.as_slice();

        let err = read_frame(&mut input).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "frame_too_large");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_unknown_type() {
        let bytes = [1, 0, 0, 0, 0xff];
        let mut input = bytes.as_slice();

        let err = read_frame(&mut input).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "bad_type");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_rejects_oversize_payload() {
        let mut writer = tokio::io::sink();
        let payload = vec![0_u8; MAX_FRAME];

        let err = write_frame(&mut writer, FrameType::Mcp, &payload)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "frame_too_large");
    }
}
