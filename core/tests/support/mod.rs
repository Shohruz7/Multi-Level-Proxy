//! Shared harness for the differential tests: our engine playing the server
//! against a real `h2` client (ADR 0003).
//!
//! `h2` keeps its `frame`, `codec`, and `hpack` modules private (not exposed
//! even under its `unstable` feature), so we cannot call its encoder or decoder
//! on a lone frame or header block. Instead we oracle at the *connection
//! boundary*: run a real `h2` client against a scripted server built from our
//! own [`FrameCodec`], and assert against the bytes it produces and accepts.
//!
//! That is a stronger claim than a self-round-trip. The bytes come from a mature
//! implementation, and the session only progresses at all if the frames *we*
//! synthesize are ones `h2` accepts — so one passing session covers both
//! directions.

// Each integration test binary compiles this module separately and uses a
// different subset of it.
#![allow(dead_code)]

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use h2proxy_core::conn::PREFACE;
use h2proxy_core::frame::{FRAME_HEADER_LEN, Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};

/// Generous enough that a slow machine never flakes, short enough that a
/// deadlocked session fails the run rather than hanging it.
pub const TIMEOUT: Duration = Duration::from_secs(10);

/// The server side of the connection, speaking our own codec. Every frame the
/// `h2` client sends is captured together with the exact octets it wrote.
pub struct Oracle {
    io: DuplexStream,
    buf: BytesMut,
    codec: FrameCodec,
}

impl Oracle {
    pub fn new(io: DuplexStream) -> Self {
        Oracle {
            io,
            buf: BytesMut::new(),
            codec: FrameCodec::new(MAX_ALLOWED_FRAME_SIZE),
        }
    }

    /// Read and verify the 24-octet client connection preface (§3.4).
    pub async fn read_preface(&mut self) {
        while self.buf.len() < PREFACE.len() {
            let n = self.io.read_buf(&mut self.buf).await.expect("read preface");
            assert!(n > 0, "client closed before sending the preface");
        }
        assert_eq!(
            &self.buf[..PREFACE.len()],
            &PREFACE[..],
            "unexpected connection preface",
        );
        let _ = self.buf.split_to(PREFACE.len());
    }

    /// The next frame from the client, with h2's exact wire bytes alongside it.
    /// `None` once the client closes.
    pub async fn next_frame(&mut self) -> Option<(Frame, Bytes)> {
        loop {
            if let Some(total) = buffered_frame_len(&self.buf)
                && self.buf.len() >= total
            {
                let raw = self.buf.split_to(total).freeze();

                // Decode from a copy so `raw` stays exactly what h2 wrote.
                let mut copy = BytesMut::from(&raw[..]);
                let frame = self
                    .codec
                    .decode(&mut copy)
                    .expect("our codec rejected a frame h2 considers valid")
                    .expect("h2 sent a frame type we discard");
                assert!(
                    copy.is_empty(),
                    "decoding {:?} left {} octets unconsumed",
                    frame.kind(),
                    copy.len(),
                );
                return Some((frame, raw));
            }
            let n = self.io.read_buf(&mut self.buf).await.expect("read");
            if n == 0 {
                return None;
            }
        }
    }

    pub async fn send(&mut self, frame: &Frame) {
        let mut out = BytesMut::new();
        self.codec.encode(frame, &mut out).expect("encode");
        self.io.write_all(&out).await.expect("write");
        self.io.flush().await.expect("flush");
    }
}

/// Total length of the frame at the front of `buf`, once its header is present.
pub fn buffered_frame_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < FRAME_HEADER_LEN {
        return None;
    }
    let payload_len = u32::from_be_bytes([0, buf[0], buf[1], buf[2]]) as usize;
    Some(FRAME_HEADER_LEN + payload_len)
}

/// The frame-level guarantee: decoding h2's octets and re-encoding them must
/// reproduce those octets exactly.
///
/// Note that no such claim is possible one layer up, in HPACK: an encoder is
/// free to choose among representations, so `hpack_differential.rs` asserts
/// semantic equality instead.
pub fn assert_byte_exact(frame: &Frame, raw: &Bytes) {
    let mut reencoded = BytesMut::new();
    FrameCodec::new(MAX_ALLOWED_FRAME_SIZE)
        .encode(frame, &mut reencoded)
        .expect("re-encode");
    assert_eq!(
        &reencoded[..],
        &raw[..],
        "re-encoding {:?} did not reproduce h2's bytes\n  ours: {:02x?}\n  h2's: {:02x?}",
        frame.kind(),
        &reencoded[..],
        &raw[..],
    );
}
