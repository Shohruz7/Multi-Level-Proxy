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

use h2proxy_core::conn::{PREFACE, Settings};
use h2proxy_core::frame::{Decoded, FRAME_HEADER_LEN, Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};
use h2proxy_core::hpack::{Header, HpackDecoder, HpackEncoder};
use h2proxy_core::stream::StreamId;
use tokio::net::TcpStream;

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

/// A header field from string literals, which is most of what a test writes.
pub fn header(name: &'static str, value: &str) -> Header {
    Header::new(
        Bytes::from_static(name.as_bytes()),
        Bytes::copy_from_slice(value.as_bytes()),
    )
}

// ---------------------------------------------------------------------------
// A raw peer, so a test can say exactly what goes on the wire and when.
// ---------------------------------------------------------------------------

/// One end of a connection, speaking our own codec directly. Used as the client
/// against our server engine, and as the backend against our upstream engine —
/// the two roles differ only in who sends the preface.
pub struct RawPeer {
    io: TcpStream,
    buf: BytesMut,
    codec: FrameCodec,
    enc: HpackEncoder,
    dec: HpackDecoder,
}

impl RawPeer {
    pub fn new(io: TcpStream) -> Self {
        RawPeer {
            io,
            buf: BytesMut::new(),
            codec: FrameCodec::new(MAX_ALLOWED_FRAME_SIZE),
            enc: HpackEncoder::new(4096),
            dec: HpackDecoder::new(4096, None),
        }
    }

    pub async fn send(&mut self, frame: &Frame) {
        let mut out = BytesMut::new();
        self.codec.encode(frame, &mut out).expect("encode");
        self.io.write_all(&out).await.expect("write");
    }

    pub async fn send_raw(&mut self, bytes: &[u8]) {
        self.io.write_all(bytes).await.expect("write raw");
    }

    /// The next frame, or `None` once the peer closes.
    pub async fn next(&mut self) -> Option<Frame> {
        loop {
            match self.codec.decode_any(&mut self.buf) {
                Ok(Some(Decoded::Frame(frame))) => return Some(frame),
                Ok(Some(Decoded::Ignored { .. })) => continue,
                Ok(None) => {}
                Err(e) => panic!("peer sent something we cannot decode: {e}"),
            }
            let n = self.io.read_buf(&mut self.buf).await.expect("read");
            if n == 0 {
                return None;
            }
        }
    }

    /// The next frame matching `want`, skipping the connection housekeeping
    /// (SETTINGS, their acks, WINDOW_UPDATEs) that may legally interleave.
    pub async fn next_matching(&mut self, want: impl Fn(&Frame) -> bool) -> Option<Frame> {
        while let Some(frame) = self.next().await {
            if want(&frame) {
                return Some(frame);
            }
        }
        None
    }

    /// Client role: preface, SETTINGS, and the peer's SETTINGS ack.
    pub async fn client_handshake(&mut self) {
        self.send_raw(PREFACE).await;
        self.send(&Settings::default().to_frame()).await;
        self.next_matching(|f| matches!(f, Frame::Settings { ack: true, .. }))
            .await
            .expect("our SETTINGS acknowledged");
    }

    /// Server role: read the preface and SETTINGS, answer with ours plus an ack.
    pub async fn server_handshake(&mut self) {
        while self.buf.len() < PREFACE.len() {
            let n = self.io.read_buf(&mut self.buf).await.expect("read preface");
            assert!(n > 0, "peer closed before the preface");
        }
        assert_eq!(&self.buf[..PREFACE.len()], &PREFACE[..], "preface");
        let _ = self.buf.split_to(PREFACE.len());
        self.send(&Settings::default().to_frame()).await;
        self.send(&Frame::Settings {
            ack: true,
            params: Vec::new(),
        })
        .await;
    }

    pub async fn send_headers(&mut self, stream_id: u32, fields: &[Header], end_stream: bool) {
        let mut block = BytesMut::new();
        self.enc.encode(fields, &mut block);
        self.send(&Frame::Headers {
            stream_id: StreamId::new(stream_id),
            block: block.freeze(),
            end_stream,
            end_headers: true,
        })
        .await;
    }

    /// The HPACK encoder, for a test that has to build a header block by hand
    /// (a CONTINUATION flood, say) rather than send a whole HEADERS.
    pub fn encoder(&mut self) -> &mut HpackEncoder {
        &mut self.enc
    }

    pub fn decode(&mut self, block: &Bytes) -> Vec<Header> {
        self.dec.decode(block).expect("decode header block")
    }
}
