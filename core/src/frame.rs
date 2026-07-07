//! Framing layer: raw connection bytes ⇄ typed HTTP/2 frames.
//!
//! Owns the 9-octet frame header, the `Frame` enum (DATA, HEADERS, SETTINGS,
//! WINDOW_UPDATE, RST_STREAM, PING, GOAWAY, CONTINUATION), and the streaming
//! codec with its reassembly buffer — the parser only advances on a complete
//! frame, so partial frames across TCP segment boundaries never consume input
//! (RFC 9113 §4, §6; design doc §3.2).
//!
//! Per-type size/placement validation lives here (which lengths are legal,
//! which stream IDs, which flags); *semantic* stream rules live in
//! [`crate::stream`]. Header block fragments pass through opaque — HPACK is
//! deliberately above this layer, in [`crate::hpack`], because the proxy
//! re-encodes headers with separate table state per side.
//!
//! Implemented in week 3; every frame type is round-tripped against the `h2`
//! crate (encode ours → decode theirs, and vice versa) and fuzzed. SETTINGS is
//! implemented now so the week-2 differential harness has one passing case.

use bytes::{BufMut, Bytes, BytesMut};

use crate::conn::ErrorCode;
use crate::stream::StreamId;

/// Every frame begins with a fixed 9-octet header (RFC 9113 §4.1).
pub const FRAME_HEADER_LEN: usize = 9;

/// The frame types the proxy handles (RFC 9113 §6). PRIORITY and PUSH_PROMISE
/// are not among them (no dependency tree; push disabled), so they — and any
/// future/unknown type code — decode as [`FrameType::Unknown`], which §4.1
/// requires be ignored rather than rejected.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameType {
    Data,
    Headers,
    RstStream,
    Settings,
    Ping,
    GoAway,
    WindowUpdate,
    Continuation,
    Unknown(u8),
}

impl FrameType {
    pub const fn from_u8(v: u8) -> FrameType {
        match v {
            0x0 => FrameType::Data,
            0x1 => FrameType::Headers,
            0x3 => FrameType::RstStream,
            0x4 => FrameType::Settings,
            0x6 => FrameType::Ping,
            0x7 => FrameType::GoAway,
            0x8 => FrameType::WindowUpdate,
            0x9 => FrameType::Continuation,
            other => FrameType::Unknown(other),
        }
    }

    pub const fn as_u8(self) -> u8 {
        match self {
            FrameType::Data => 0x0,
            FrameType::Headers => 0x1,
            FrameType::RstStream => 0x3,
            FrameType::Settings => 0x4,
            FrameType::Ping => 0x6,
            FrameType::GoAway => 0x7,
            FrameType::WindowUpdate => 0x8,
            FrameType::Continuation => 0x9,
            FrameType::Unknown(other) => other,
        }
    }
}

/// The frame flags byte (RFC 9113 §4.1). Which bits are meaningful depends on
/// the frame type; the same bit value carries different names in different
/// frames (e.g. `0x1` is END_STREAM on DATA/HEADERS but ACK on SETTINGS/PING).
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Flags(pub u8);

impl Flags {
    pub const END_STREAM: Flags = Flags(0x1);
    pub const ACK: Flags = Flags(0x1);
    pub const END_HEADERS: Flags = Flags(0x4);
    pub const PADDED: Flags = Flags(0x8);
    pub const PRIORITY: Flags = Flags(0x20);

    pub const fn empty() -> Flags {
        Flags(0)
    }

    /// True if every bit set in `other` is also set here.
    pub const fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }
}

/// The parsed 9-octet frame header, before the payload is interpreted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameHeader {
    /// Payload length; a 24-bit field on the wire, so at most `2^24 - 1`.
    pub length: u32,
    pub kind: FrameType,
    pub flags: Flags,
    pub stream_id: StreamId,
}

impl FrameHeader {
    /// Parse a header from at least [`FRAME_HEADER_LEN`] octets.
    fn parse(b: &[u8]) -> FrameHeader {
        FrameHeader {
            length: u32::from_be_bytes([0, b[0], b[1], b[2]]),
            kind: FrameType::from_u8(b[3]),
            flags: Flags(b[4]),
            stream_id: StreamId::new(u32::from_be_bytes([b[5], b[6], b[7], b[8]])),
        }
    }

    /// Serialize the header into `out`.
    fn write(&self, out: &mut BytesMut) {
        let len = self.length.to_be_bytes();
        out.put_u8(len[1]);
        out.put_u8(len[2]);
        out.put_u8(len[3]);
        out.put_u8(self.kind.as_u8());
        out.put_u8(self.flags.0);
        // High bit is reserved and sent as zero.
        out.put_u32(self.stream_id.get() & 0x7fff_ffff);
    }
}

/// A typed HTTP/2 frame. Header block fragments (`HEADERS`/`CONTINUATION`) stay
/// opaque `Bytes` here; HPACK decoding happens a layer up ([`crate::hpack`]).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Frame {
    Data {
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    Headers {
        stream_id: StreamId,
        block: Bytes,
        end_stream: bool,
        end_headers: bool,
    },
    Settings {
        ack: bool,
        /// Raw id/value pairs, kept unmerged so an unknown identifier survives a
        /// re-encode byte-for-byte (the differential harness relies on this).
        params: Vec<(u16, u32)>,
    },
    WindowUpdate {
        stream_id: StreamId,
        increment: u32,
    },
    RstStream {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    Ping {
        data: [u8; 8],
        ack: bool,
    },
    GoAway {
        last_stream_id: StreamId,
        error_code: ErrorCode,
        debug_data: Bytes,
    },
    Continuation {
        stream_id: StreamId,
        block: Bytes,
        end_headers: bool,
    },
}

/// Framing errors. Each maps to the connection-level [`ErrorCode`] the RFC
/// requires (§4.2, §4.3, §6), surfaced via [`FrameError::code`].
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame length {len} exceeds SETTINGS_MAX_FRAME_SIZE {max}")]
    Oversized { len: u32, max: u32 },
    #[error("{kind:?} frame has an invalid length ({len})")]
    BadLength { kind: FrameType, len: u32 },
    #[error("{0:?} frame arrived on an invalid stream")]
    BadStreamId(FrameType),
}

impl FrameError {
    /// The connection error code this framing violation must be reported with.
    pub const fn code(&self) -> ErrorCode {
        match self {
            FrameError::Oversized { .. } | FrameError::BadLength { .. } => {
                ErrorCode::FrameSizeError
            }
            FrameError::BadStreamId(_) => ErrorCode::ProtocolError,
        }
    }
}

/// The framing seam: bytes ⇄ typed frames (design doc §3.2).
///
/// Holds the peer-negotiated `MAX_FRAME_SIZE` so oversized frames are rejected
/// during decode. [`FrameCodec::decode`] is streaming: it pulls one whole frame
/// off the front of a reassembly buffer and consumes nothing when the buffer
/// holds only a partial frame.
pub struct FrameCodec {
    max_frame_size: u32,
}

impl FrameCodec {
    /// A codec bounded by `max_frame_size` (the value this endpoint advertised
    /// in SETTINGS; defaults to 16,384 before the handshake — RFC 9113 §6.5.2).
    pub const fn new(max_frame_size: u32) -> Self {
        FrameCodec { max_frame_size }
    }

    /// Try to decode one frame from the front of `buf`.
    ///
    /// Returns `Ok(None)` — consuming nothing — when `buf` does not yet hold a
    /// complete frame, so the caller can read more bytes and retry. On success
    /// the frame's octets are removed from `buf`.
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        if buf.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }
        let header = FrameHeader::parse(&buf[..FRAME_HEADER_LEN]);
        if header.length > self.max_frame_size {
            return Err(FrameError::Oversized {
                len: header.length,
                max: self.max_frame_size,
            });
        }
        let total = FRAME_HEADER_LEN + header.length as usize;
        if buf.len() < total {
            return Ok(None);
        }
        // A whole frame is present: consume it, then split header from payload.
        let mut frame = buf.split_to(total);
        let payload = frame.split_off(FRAME_HEADER_LEN).freeze();
        Frame::from_parts(header, payload).map(Some)
    }

    /// Serialize `frame` onto `out`.
    pub fn encode(&mut self, frame: &Frame, out: &mut BytesMut) -> Result<(), FrameError> {
        frame.write(out)
    }
}

impl Frame {
    /// Interpret a header + opaque payload as a typed frame. Only SETTINGS is
    /// implemented for week 2; the rest land in week 3.
    fn from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        match header.kind {
            FrameType::Settings => Self::settings_from_parts(header, payload),
            FrameType::Data => todo!("DATA decode — week 3"),
            FrameType::Headers => todo!("HEADERS decode — week 3"),
            FrameType::RstStream => todo!("RST_STREAM decode — week 3"),
            FrameType::Ping => todo!("PING decode — week 3"),
            FrameType::GoAway => todo!("GOAWAY decode — week 3"),
            FrameType::WindowUpdate => todo!("WINDOW_UPDATE decode — week 3"),
            FrameType::Continuation => todo!("CONTINUATION decode — week 3"),
            FrameType::Unknown(_) => todo!("discard unknown frame types — week 3"),
        }
    }

    /// Parse a SETTINGS payload (RFC 9113 §6.5): stream 0 only, ACK carries an
    /// empty payload, otherwise a sequence of 6-octet id/value pairs.
    fn settings_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if !header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::Settings));
        }
        if header.flags.contains(Flags::ACK) {
            if !payload.is_empty() {
                return Err(FrameError::BadLength {
                    kind: FrameType::Settings,
                    len: payload.len() as u32,
                });
            }
            return Ok(Frame::Settings {
                ack: true,
                params: Vec::new(),
            });
        }
        if !payload.len().is_multiple_of(6) {
            return Err(FrameError::BadLength {
                kind: FrameType::Settings,
                len: payload.len() as u32,
            });
        }
        let mut params = Vec::with_capacity(payload.len() / 6);
        for chunk in payload.chunks_exact(6) {
            let id = u16::from_be_bytes([chunk[0], chunk[1]]);
            let value = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
            params.push((id, value));
        }
        Ok(Frame::Settings { ack: false, params })
    }

    /// Serialize this frame (header + payload) onto `out`. SETTINGS only for
    /// week 2.
    fn write(&self, out: &mut BytesMut) -> Result<(), FrameError> {
        match self {
            Frame::Settings { ack, params } => {
                let flags = if *ack { Flags::ACK } else { Flags::empty() };
                FrameHeader {
                    length: (params.len() * 6) as u32,
                    kind: FrameType::Settings,
                    flags,
                    stream_id: StreamId::CONNECTION,
                }
                .write(out);
                for (id, value) in params {
                    out.put_u16(*id);
                    out.put_u32(*value);
                }
                Ok(())
            }
            _ => todo!("frame encode for non-SETTINGS types — week 3"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trips_through_our_codec() {
        let frame = Frame::Settings {
            ack: false,
            params: vec![
                (crate::conn::setting_id::MAX_CONCURRENT_STREAMS, 128),
                (crate::conn::setting_id::INITIAL_WINDOW_SIZE, 1 << 20),
                (crate::conn::setting_id::ENABLE_PUSH, 0),
            ],
        };

        let mut codec = FrameCodec::new(16_384);
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).expect("encode");

        // Header (9) + three 6-octet entries.
        assert_eq!(buf.len(), FRAME_HEADER_LEN + 3 * 6);

        let decoded = codec.decode(&mut buf).expect("decode").expect("a frame");
        assert_eq!(decoded, frame);
        assert!(buf.is_empty(), "the whole frame should be consumed");
    }

    #[test]
    fn settings_ack_has_empty_payload() {
        let frame = Frame::Settings {
            ack: true,
            params: Vec::new(),
        };
        let mut codec = FrameCodec::new(16_384);
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).expect("encode");
        assert_eq!(buf.len(), FRAME_HEADER_LEN);
        assert_eq!(codec.decode(&mut buf).expect("decode"), Some(frame));
    }

    #[test]
    fn decode_waits_for_a_complete_frame() {
        let frame = Frame::Settings {
            ack: false,
            params: vec![(crate::conn::setting_id::MAX_FRAME_SIZE, 16_384)],
        };
        let mut codec = FrameCodec::new(16_384);
        let mut full = BytesMut::new();
        codec.encode(&frame, &mut full).expect("encode");

        // Feed one byte short of the whole frame: nothing decodes, nothing is
        // consumed.
        let mut partial = full.clone();
        let last = partial.split_off(full.len() - 1);
        assert_eq!(codec.decode(&mut partial).expect("decode"), None);
        assert_eq!(partial.len(), full.len() - 1, "partial input is untouched");

        // Supplying the final byte completes the frame.
        partial.unsplit(last);
        assert_eq!(codec.decode(&mut partial).expect("decode"), Some(frame));
    }
}
