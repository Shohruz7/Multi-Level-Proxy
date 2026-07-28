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
//! Two things are normalized away on decode, because the proxy models neither:
//! **padding** (validated, then stripped — §6.1) and the deprecated **priority
//! fields** on HEADERS (skipped — §6.3). The encoder emits neither, so
//! `decode(encode(f)) == f` holds for every frame this module builds.

use bytes::{BufMut, Bytes, BytesMut};

use crate::conn::ErrorCode;
use crate::stream::StreamId;

/// Every frame begins with a fixed 9-octet header (RFC 9113 §4.1).
pub const FRAME_HEADER_LEN: usize = 9;

/// The largest payload the header's 24-bit length field can express (§4.1),
/// which is also the ceiling on `SETTINGS_MAX_FRAME_SIZE` (§6.5.2).
pub const MAX_ALLOWED_FRAME_SIZE: u32 = (1 << 24) - 1;

/// The initial and minimum `SETTINGS_MAX_FRAME_SIZE` (§6.5.2). In force until
/// the peer's SETTINGS raises it.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;

/// The optional priority block on a HEADERS frame: a 1-bit exclusive flag plus
/// a 31-bit stream dependency, then an 8-bit weight (§6.2).
const PRIORITY_FIELD_LEN: usize = 5;

/// PUSH_PROMISE's type code (§6.6). Deliberately *not* a [`FrameType`] variant:
/// the proxy advertises `ENABLE_PUSH = 0`, so receiving one is a protocol
/// violation (§8.4) rather than a frame worth modeling.
const PUSH_PROMISE_TYPE: u8 = 0x5;

/// The frame types the proxy handles (RFC 9113 §6). PRIORITY and PUSH_PROMISE
/// are not among them (no dependency tree; push disabled), so they — and any
/// future/unknown type code — decode as [`FrameType::Unknown`], which §4.1
/// requires be ignored rather than rejected.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
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

    /// Set `other`'s bits when `cond` holds — how the encoder assembles a flags
    /// byte from a frame's booleans.
    pub const fn set_if(self, cond: bool, other: Flags) -> Flags {
        if cond { Flags(self.0 | other.0) } else { self }
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

impl Frame {
    /// The frame type this variant serializes as.
    pub const fn kind(&self) -> FrameType {
        match self {
            Frame::Data { .. } => FrameType::Data,
            Frame::Headers { .. } => FrameType::Headers,
            Frame::Settings { .. } => FrameType::Settings,
            Frame::WindowUpdate { .. } => FrameType::WindowUpdate,
            Frame::RstStream { .. } => FrameType::RstStream,
            Frame::Ping { .. } => FrameType::Ping,
            Frame::GoAway { .. } => FrameType::GoAway,
            Frame::Continuation { .. } => FrameType::Continuation,
        }
    }

    /// The stream this frame is addressed to; [`StreamId::CONNECTION`] for the
    /// connection-control frames (SETTINGS, PING, GOAWAY).
    pub const fn stream_id(&self) -> StreamId {
        match self {
            Frame::Data { stream_id, .. }
            | Frame::Headers { stream_id, .. }
            | Frame::WindowUpdate { stream_id, .. }
            | Frame::RstStream { stream_id, .. }
            | Frame::Continuation { stream_id, .. } => *stream_id,
            Frame::Settings { .. } | Frame::Ping { .. } | Frame::GoAway { .. } => {
                StreamId::CONNECTION
            }
        }
    }
}

/// Framing errors. Each maps to the connection-level [`ErrorCode`] the RFC
/// requires (§4.2, §4.3, §6), surfaced via [`FrameError::code`].
///
/// Everything here is *connection*-scoped by design (ADR 0008): a malformed
/// frame means the byte stream can no longer be trusted, so the connection dies
/// with GOAWAY. Violations that the RFC scopes to a single stream — a zero
/// WINDOW_UPDATE increment, say — are the connection layer's to raise, because
/// only it knows the stream's state.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame length {len} exceeds SETTINGS_MAX_FRAME_SIZE {max}")]
    Oversized { len: u32, max: u32 },
    #[error("{kind:?} frame has an invalid length ({len})")]
    BadLength { kind: FrameType, len: u32 },
    #[error("{0:?} frame arrived on an invalid stream")]
    BadStreamId(FrameType),
    #[error("{kind:?} frame padding is at least as long as its payload")]
    PaddingOverflow { kind: FrameType },
    #[error("received PUSH_PROMISE, but this endpoint advertises ENABLE_PUSH = 0")]
    UnexpectedPushPromise,
}

impl FrameError {
    /// The connection error code this framing violation must be reported with.
    pub const fn code(&self) -> ErrorCode {
        match self {
            FrameError::Oversized { .. } | FrameError::BadLength { .. } => {
                ErrorCode::FrameSizeError
            }
            FrameError::BadStreamId(_)
            | FrameError::PaddingOverflow { .. }
            | FrameError::UnexpectedPushPromise => ErrorCode::ProtocolError,
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

    /// Raise the accepted frame size after our SETTINGS are acknowledged.
    pub const fn set_max_frame_size(&mut self, max_frame_size: u32) {
        self.max_frame_size = max_frame_size;
    }

    /// Try to decode one frame from the front of `buf`.
    ///
    /// Returns `Ok(None)` — consuming nothing — when `buf` does not yet hold a
    /// complete frame, so the caller can read more bytes and retry. On success
    /// the frame's octets are removed from `buf`.
    ///
    /// Frames of unknown type are consumed and discarded here rather than
    /// surfaced, as §4.1 requires; the loop then continues to the next frame, so
    /// a caller never sees them.
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        loop {
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
            match Frame::from_parts(header, payload)? {
                Some(frame) => return Ok(Some(frame)),
                // An ignorable type (§4.1): already consumed, so look at the
                // next frame rather than reporting "need more bytes".
                None => continue,
            }
        }
    }

    /// Serialize `frame` onto `out`.
    pub fn encode(&mut self, frame: &Frame, out: &mut BytesMut) -> Result<(), FrameError> {
        frame.write(out)
    }
}

impl Frame {
    /// Interpret a header + opaque payload as a typed frame.
    ///
    /// `Ok(None)` means "a well-formed frame of a type we ignore" (§4.1) — it
    /// has been consumed and should be skipped.
    fn from_parts(header: FrameHeader, payload: Bytes) -> Result<Option<Frame>, FrameError> {
        match header.kind {
            FrameType::Data => Self::data_from_parts(header, payload).map(Some),
            FrameType::Headers => Self::headers_from_parts(header, payload).map(Some),
            FrameType::RstStream => Self::rst_stream_from_parts(header, payload).map(Some),
            FrameType::Settings => Self::settings_from_parts(header, payload).map(Some),
            FrameType::Ping => Self::ping_from_parts(header, payload).map(Some),
            FrameType::GoAway => Self::go_away_from_parts(header, payload).map(Some),
            FrameType::WindowUpdate => Self::window_update_from_parts(header, payload).map(Some),
            FrameType::Continuation => Self::continuation_from_parts(header, payload).map(Some),
            // Push is disabled, so a PUSH_PROMISE is a violation to report, not
            // a frame to ignore (§8.4).
            FrameType::Unknown(PUSH_PROMISE_TYPE) => Err(FrameError::UnexpectedPushPromise),
            // Everything else — including the deprecated PRIORITY (0x2) — must
            // be discarded rather than rejected (§4.1).
            FrameType::Unknown(_) => Ok(None),
        }
    }

    /// Validate and strip the optional padding shared by DATA and HEADERS
    /// (§6.1): with PADDED set, the first payload octet gives the number of
    /// trailing padding octets, and both it and the padding are removed.
    fn strip_padding(kind: FrameType, flags: Flags, payload: Bytes) -> Result<Bytes, FrameError> {
        if !flags.contains(Flags::PADDED) {
            return Ok(payload);
        }
        // Once PADDED is set the Pad Length octet is mandatory frame data, so a
        // payload too short to hold it is a size error (§4.2).
        if payload.is_empty() {
            return Err(FrameError::BadLength { kind, len: 0 });
        }
        let pad_len = payload[0] as usize;
        // "If the length of the padding is the length of the frame payload or
        // greater, the recipient MUST treat this as a connection error of type
        // PROTOCOL_ERROR" (§6.1).
        if pad_len >= payload.len() {
            return Err(FrameError::PaddingOverflow { kind });
        }
        Ok(payload.slice(1..payload.len() - pad_len))
    }

    /// Parse a DATA payload (§6.1): any length, never on stream 0, padding
    /// stripped.
    fn data_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::Data));
        }
        let data = Self::strip_padding(FrameType::Data, header.flags, payload)?;
        Ok(Frame::Data {
            stream_id: header.stream_id,
            data,
            end_stream: header.flags.contains(Flags::END_STREAM),
        })
    }

    /// Parse a HEADERS payload (§6.2): never on stream 0; padding stripped and
    /// the deprecated priority block skipped, leaving the opaque header block.
    fn headers_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::Headers));
        }
        let mut block = Self::strip_padding(FrameType::Headers, header.flags, payload)?;
        // The priority fields are read only to be discarded: RFC 9113 deprecates
        // the priority scheme and the proxy models no dependency tree.
        if header.flags.contains(Flags::PRIORITY) {
            if block.len() < PRIORITY_FIELD_LEN {
                return Err(FrameError::BadLength {
                    kind: FrameType::Headers,
                    len: block.len() as u32,
                });
            }
            block = block.slice(PRIORITY_FIELD_LEN..);
        }
        Ok(Frame::Headers {
            stream_id: header.stream_id,
            block,
            end_stream: header.flags.contains(Flags::END_STREAM),
            end_headers: header.flags.contains(Flags::END_HEADERS),
        })
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

    /// Parse a RST_STREAM payload (§6.4): exactly 4 octets of error code, never
    /// on stream 0.
    fn rst_stream_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::RstStream));
        }
        let code = Self::error_code(FrameType::RstStream, &payload)?;
        Ok(Frame::RstStream {
            stream_id: header.stream_id,
            error_code: code,
        })
    }

    /// Parse a PING payload (§6.7): exactly 8 opaque octets, stream 0 only.
    fn ping_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if !header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::Ping));
        }
        if payload.len() != 8 {
            return Err(FrameError::BadLength {
                kind: FrameType::Ping,
                len: payload.len() as u32,
            });
        }
        let mut data = [0u8; 8];
        data.copy_from_slice(&payload);
        Ok(Frame::Ping {
            data,
            ack: header.flags.contains(Flags::ACK),
        })
    }

    /// Parse a GOAWAY payload (§6.8): last-stream-id + error code, then
    /// arbitrary debug data; stream 0 only.
    fn go_away_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if !header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::GoAway));
        }
        if payload.len() < 8 {
            return Err(FrameError::BadLength {
                kind: FrameType::GoAway,
                len: payload.len() as u32,
            });
        }
        let last_stream_id = StreamId::new(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]));
        let error_code = Self::error_code(FrameType::GoAway, &payload[4..8])?;
        Ok(Frame::GoAway {
            last_stream_id,
            error_code,
            debug_data: payload.slice(8..),
        })
    }

    /// Parse a WINDOW_UPDATE payload (§6.9): exactly 4 octets, a 31-bit
    /// increment. Legal on any stream, including 0 (the connection window).
    ///
    /// A zero increment is *not* rejected here: §6.9 makes it a stream error on
    /// a stream but a connection error on stream 0, and choosing between those
    /// needs stream state this layer does not have. [`crate::conn`] enforces it.
    fn window_update_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if payload.len() != 4 {
            return Err(FrameError::BadLength {
                kind: FrameType::WindowUpdate,
                len: payload.len() as u32,
            });
        }
        let increment =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
        Ok(Frame::WindowUpdate {
            stream_id: header.stream_id,
            increment,
        })
    }

    /// Parse a CONTINUATION payload (§6.10): an opaque header-block fragment of
    /// any length, never on stream 0. That it may only follow an unterminated
    /// header block is a sequencing rule, enforced in [`crate::conn`].
    fn continuation_from_parts(header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        if header.stream_id.is_connection() {
            return Err(FrameError::BadStreamId(FrameType::Continuation));
        }
        Ok(Frame::Continuation {
            stream_id: header.stream_id,
            block: payload,
            end_headers: header.flags.contains(Flags::END_HEADERS),
        })
    }

    /// Read the 4-octet error code shared by RST_STREAM and GOAWAY. Unknown
    /// codes normalize to `INTERNAL_ERROR`, which §7 explicitly permits — so a
    /// re-encode of an unrecognized code is deliberately *not* byte-identical.
    fn error_code(kind: FrameType, bytes: &[u8]) -> Result<ErrorCode, FrameError> {
        if bytes.len() != 4 {
            return Err(FrameError::BadLength {
                kind,
                len: bytes.len() as u32,
            });
        }
        Ok(ErrorCode::from_u32(u32::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ])))
    }

    /// Write a frame header, refusing a payload the 24-bit length field cannot
    /// express. Per-peer `MAX_FRAME_SIZE` enforcement on the send path lands in
    /// week 5, when the connection tracks both endpoints' settings.
    fn write_header(
        kind: FrameType,
        flags: Flags,
        stream_id: StreamId,
        payload_len: usize,
        out: &mut BytesMut,
    ) -> Result<(), FrameError> {
        if payload_len > MAX_ALLOWED_FRAME_SIZE as usize {
            return Err(FrameError::Oversized {
                len: u32::try_from(payload_len).unwrap_or(u32::MAX),
                max: MAX_ALLOWED_FRAME_SIZE,
            });
        }
        FrameHeader {
            length: payload_len as u32,
            kind,
            flags,
            stream_id,
        }
        .write(out);
        Ok(())
    }

    /// Serialize this frame (header + payload) onto `out`.
    ///
    /// The encoder never emits padding or priority fields, so every frame it
    /// writes decodes back to an identical value.
    fn write(&self, out: &mut BytesMut) -> Result<(), FrameError> {
        match self {
            Frame::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let flags = Flags::empty().set_if(*end_stream, Flags::END_STREAM);
                Self::write_header(FrameType::Data, flags, *stream_id, data.len(), out)?;
                out.put_slice(data);
                Ok(())
            }
            Frame::Headers {
                stream_id,
                block,
                end_stream,
                end_headers,
            } => {
                let flags = Flags::empty()
                    .set_if(*end_stream, Flags::END_STREAM)
                    .set_if(*end_headers, Flags::END_HEADERS);
                Self::write_header(FrameType::Headers, flags, *stream_id, block.len(), out)?;
                out.put_slice(block);
                Ok(())
            }
            Frame::Settings { ack, params } => {
                let flags = Flags::empty().set_if(*ack, Flags::ACK);
                Self::write_header(
                    FrameType::Settings,
                    flags,
                    StreamId::CONNECTION,
                    params.len() * 6,
                    out,
                )?;
                for (id, value) in params {
                    out.put_u16(*id);
                    out.put_u32(*value);
                }
                Ok(())
            }
            Frame::WindowUpdate {
                stream_id,
                increment,
            } => {
                Self::write_header(FrameType::WindowUpdate, Flags::empty(), *stream_id, 4, out)?;
                // The high bit is reserved and sent as zero (§6.9).
                out.put_u32(*increment & 0x7fff_ffff);
                Ok(())
            }
            Frame::RstStream {
                stream_id,
                error_code,
            } => {
                Self::write_header(FrameType::RstStream, Flags::empty(), *stream_id, 4, out)?;
                out.put_u32(error_code.as_u32());
                Ok(())
            }
            Frame::Ping { data, ack } => {
                let flags = Flags::empty().set_if(*ack, Flags::ACK);
                Self::write_header(FrameType::Ping, flags, StreamId::CONNECTION, 8, out)?;
                out.put_slice(data);
                Ok(())
            }
            Frame::GoAway {
                last_stream_id,
                error_code,
                debug_data,
            } => {
                Self::write_header(
                    FrameType::GoAway,
                    Flags::empty(),
                    StreamId::CONNECTION,
                    8 + debug_data.len(),
                    out,
                )?;
                out.put_u32(last_stream_id.get() & 0x7fff_ffff);
                out.put_u32(error_code.as_u32());
                out.put_slice(debug_data);
                Ok(())
            }
            Frame::Continuation {
                stream_id,
                block,
                end_headers,
            } => {
                let flags = Flags::empty().set_if(*end_headers, Flags::END_HEADERS);
                Self::write_header(FrameType::Continuation, flags, *stream_id, block.len(), out)?;
                out.put_slice(block);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode `frame`, decode it back, and assert both the value and full
    /// consumption of the buffer — the invariant every frame type must hold.
    fn assert_round_trips(frame: Frame) {
        let mut codec = FrameCodec::new(MAX_ALLOWED_FRAME_SIZE);
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).expect("encode");
        let decoded = codec.decode(&mut buf).expect("decode").expect("a frame");
        assert_eq!(decoded, frame);
        assert!(buf.is_empty(), "the whole frame should be consumed");
    }

    /// Build the raw octets of a frame with an arbitrary (possibly illegal)
    /// header, for the malformed-input tests.
    fn raw(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        let len = (payload.len() as u32).to_be_bytes();
        buf.put_u8(len[1]);
        buf.put_u8(len[2]);
        buf.put_u8(len[3]);
        buf.put_u8(kind);
        buf.put_u8(flags);
        buf.put_u32(stream_id);
        buf.put_slice(payload);
        buf
    }

    fn decode_raw(buf: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        FrameCodec::new(MAX_ALLOWED_FRAME_SIZE).decode(buf)
    }

    const STREAM_1: StreamId = StreamId::new(1);

    // ---- round trips, one per frame type ---------------------------------

    #[test]
    fn data_round_trips() {
        assert_round_trips(Frame::Data {
            stream_id: STREAM_1,
            data: Bytes::from_static(b"hello world"),
            end_stream: true,
        });
        // An empty DATA frame is legal and is how a body is terminated.
        assert_round_trips(Frame::Data {
            stream_id: STREAM_1,
            data: Bytes::new(),
            end_stream: true,
        });
    }

    #[test]
    fn headers_round_trips() {
        for (end_stream, end_headers) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            assert_round_trips(Frame::Headers {
                stream_id: STREAM_1,
                block: Bytes::from_static(b"\x82\x86\x84"),
                end_stream,
                end_headers,
            });
        }
    }

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
    fn rst_stream_round_trips() {
        assert_round_trips(Frame::RstStream {
            stream_id: STREAM_1,
            error_code: ErrorCode::Cancel,
        });
    }

    #[test]
    fn ping_round_trips() {
        for ack in [false, true] {
            assert_round_trips(Frame::Ping {
                data: *b"\x01\x02\x03\x04\x05\x06\x07\x08",
                ack,
            });
        }
    }

    #[test]
    fn go_away_round_trips() {
        assert_round_trips(Frame::GoAway {
            last_stream_id: StreamId::new(7),
            error_code: ErrorCode::ProtocolError,
            debug_data: Bytes::from_static(b"bad frame on stream 9"),
        });
        // Debug data is optional: an 8-octet payload is the minimum.
        assert_round_trips(Frame::GoAway {
            last_stream_id: StreamId::CONNECTION,
            error_code: ErrorCode::NoError,
            debug_data: Bytes::new(),
        });
    }

    #[test]
    fn window_update_round_trips() {
        assert_round_trips(Frame::WindowUpdate {
            stream_id: STREAM_1,
            increment: 65_535,
        });
        // Stream 0 carries the connection-level window.
        assert_round_trips(Frame::WindowUpdate {
            stream_id: StreamId::CONNECTION,
            increment: 1 << 20,
        });
    }

    #[test]
    fn continuation_round_trips() {
        assert_round_trips(Frame::Continuation {
            stream_id: STREAM_1,
            block: Bytes::from_static(b"\x40\x0a"),
            end_headers: true,
        });
    }

    // ---- streaming / reassembly -------------------------------------------

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

    #[test]
    fn decode_drains_frames_back_to_back() {
        let mut codec = FrameCodec::new(16_384);
        let mut buf = BytesMut::new();
        let ping = Frame::Ping {
            data: [9; 8],
            ack: false,
        };
        let rst = Frame::RstStream {
            stream_id: STREAM_1,
            error_code: ErrorCode::RefusedStream,
        };
        codec.encode(&ping, &mut buf).expect("encode");
        codec.encode(&rst, &mut buf).expect("encode");

        assert_eq!(codec.decode(&mut buf).expect("decode"), Some(ping));
        assert_eq!(codec.decode(&mut buf).expect("decode"), Some(rst));
        assert_eq!(codec.decode(&mut buf).expect("decode"), None);
    }

    #[test]
    fn oversized_frames_are_rejected_before_buffering_the_payload() {
        // A header claiming 100 octets against a 64-octet limit must fail
        // immediately, without waiting for the payload to arrive.
        let mut buf = raw(FrameType::Data.as_u8(), 0, 1, &[]);
        buf[2] = 100;
        let err = FrameCodec::new(64).decode(&mut buf).expect_err("oversized");
        assert_eq!(err, FrameError::Oversized { len: 100, max: 64 });
        assert_eq!(err.code(), ErrorCode::FrameSizeError);
    }

    // ---- padding and priority ---------------------------------------------

    #[test]
    fn padded_data_is_stripped() {
        // PADDED: pad length 3, payload "hi", then three zero octets.
        let mut buf = raw(
            FrameType::Data.as_u8(),
            Flags::PADDED.0,
            1,
            b"\x03hi\x00\x00\x00",
        );
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::Data {
                stream_id: STREAM_1,
                data: Bytes::from_static(b"hi"),
                end_stream: false,
            }
        );
    }

    #[test]
    fn padding_at_least_as_long_as_the_payload_is_a_protocol_error() {
        // Pad length 4 with only 4 payload octets total: the padding would eat
        // its own length octet (§6.1).
        let mut buf = raw(FrameType::Data.as_u8(), Flags::PADDED.0, 1, b"\x04abc");
        let err = decode_raw(&mut buf).expect_err("padding overflow");
        assert_eq!(
            err,
            FrameError::PaddingOverflow {
                kind: FrameType::Data
            }
        );
        assert_eq!(err.code(), ErrorCode::ProtocolError);
    }

    #[test]
    fn padded_flag_with_an_empty_payload_is_a_size_error() {
        let mut buf = raw(FrameType::Data.as_u8(), Flags::PADDED.0, 1, b"");
        let err = decode_raw(&mut buf).expect_err("missing pad length octet");
        assert_eq!(err.code(), ErrorCode::FrameSizeError);
    }

    #[test]
    fn headers_priority_fields_are_skipped() {
        // PRIORITY: 4 octets of stream dependency + 1 weight, then the block.
        let mut buf = raw(
            FrameType::Headers.as_u8(),
            Flags::PRIORITY.0 | Flags::END_HEADERS.0,
            1,
            b"\x00\x00\x00\x03\x10\x82\x86",
        );
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::Headers {
                stream_id: STREAM_1,
                block: Bytes::from_static(b"\x82\x86"),
                end_stream: false,
                end_headers: true,
            }
        );
    }

    #[test]
    fn headers_too_short_for_its_priority_block_is_a_size_error() {
        let mut buf = raw(
            FrameType::Headers.as_u8(),
            Flags::PRIORITY.0,
            1,
            b"\x00\x00",
        );
        let err = decode_raw(&mut buf).expect_err("truncated priority block");
        assert_eq!(err.code(), ErrorCode::FrameSizeError);
    }

    #[test]
    fn padded_and_prioritized_headers_strip_both() {
        // Pad length 2, priority block, "\x82", then two padding octets.
        let mut buf = raw(
            FrameType::Headers.as_u8(),
            Flags::PADDED.0 | Flags::PRIORITY.0,
            1,
            b"\x02\x00\x00\x00\x03\x10\x82\x00\x00",
        );
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::Headers {
                stream_id: STREAM_1,
                block: Bytes::from_static(b"\x82"),
                end_stream: false,
                end_headers: false,
            }
        );
    }

    // ---- length and stream-id validation -----------------------------------

    #[test]
    fn fixed_length_frames_reject_wrong_lengths() {
        // RST_STREAM, PING and WINDOW_UPDATE all have exact payload lengths.
        for (kind, stream_id, payload) in [
            (FrameType::RstStream, 1u32, &b"\x00\x00"[..]),
            (FrameType::Ping, 0, &b"\x00\x00\x00"[..]),
            (FrameType::WindowUpdate, 1, &b"\x00\x00\x00\x00\x00"[..]),
            // GOAWAY needs at least 8 octets.
            (FrameType::GoAway, 0, &b"\x00\x00\x00"[..]),
        ] {
            let mut buf = raw(kind.as_u8(), 0, stream_id, payload);
            let err = decode_raw(&mut buf).unwrap_err();
            assert_eq!(
                err,
                FrameError::BadLength {
                    kind,
                    len: payload.len() as u32
                },
                "{kind:?} accepted a {}-octet payload",
                payload.len(),
            );
            assert_eq!(err.code(), ErrorCode::FrameSizeError);
        }
    }

    #[test]
    fn settings_ack_with_a_payload_is_rejected() {
        let mut buf = raw(
            FrameType::Settings.as_u8(),
            Flags::ACK.0,
            0,
            b"\x00\x03\x00\x00\x00\x64",
        );
        let err = decode_raw(&mut buf).expect_err("ACK must be empty");
        assert_eq!(err.code(), ErrorCode::FrameSizeError);
    }

    #[test]
    fn settings_payload_must_be_a_multiple_of_six() {
        let mut buf = raw(FrameType::Settings.as_u8(), 0, 0, b"\x00\x03\x00\x00\x00");
        let err = decode_raw(&mut buf).expect_err("truncated settings entry");
        assert_eq!(err.code(), ErrorCode::FrameSizeError);
    }

    #[test]
    fn stream_addressed_frames_reject_stream_zero() {
        for kind in [
            FrameType::Data,
            FrameType::Headers,
            FrameType::RstStream,
            FrameType::Continuation,
        ] {
            // A 4-octet payload keeps RST_STREAM's length legal so the stream-id
            // check is what fails.
            let mut buf = raw(kind.as_u8(), 0, 0, b"\x00\x00\x00\x00");
            let err = decode_raw(&mut buf).unwrap_err();
            assert_eq!(
                err,
                FrameError::BadStreamId(kind),
                "{kind:?} allowed stream 0"
            );
            assert_eq!(err.code(), ErrorCode::ProtocolError);
        }
    }

    #[test]
    fn connection_frames_reject_nonzero_streams() {
        for (kind, payload) in [
            (FrameType::Settings, &b""[..]),
            (FrameType::Ping, &b"\x00\x00\x00\x00\x00\x00\x00\x00"[..]),
            (FrameType::GoAway, &b"\x00\x00\x00\x00\x00\x00\x00\x00"[..]),
        ] {
            let mut buf = raw(kind.as_u8(), 0, 1, payload);
            let err = decode_raw(&mut buf).unwrap_err();
            assert_eq!(
                err,
                FrameError::BadStreamId(kind),
                "{kind:?} allowed stream 1"
            );
            assert_eq!(err.code(), ErrorCode::ProtocolError);
        }
    }

    #[test]
    fn window_update_is_legal_on_any_stream() {
        // Unlike the other stream-addressed frames, stream 0 is meaningful here:
        // it is the connection-level window.
        let mut buf = raw(FrameType::WindowUpdate.as_u8(), 0, 0, b"\x00\x00\x01\x00");
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::WindowUpdate {
                stream_id: StreamId::CONNECTION,
                increment: 256,
            }
        );
    }

    #[test]
    fn window_update_masks_the_reserved_bit() {
        // The high bit is reserved; a peer that sets it must not inflate the
        // increment (§6.9).
        let mut buf = raw(FrameType::WindowUpdate.as_u8(), 0, 1, b"\x80\x00\x00\x01");
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::WindowUpdate {
                stream_id: STREAM_1,
                increment: 1,
            }
        );
    }

    #[test]
    fn zero_window_update_decodes_for_the_connection_layer_to_judge() {
        // §6.9 scopes this violation to a stream or the connection depending on
        // state the framing layer does not have, so it is not an error here.
        let mut buf = raw(FrameType::WindowUpdate.as_u8(), 0, 1, b"\x00\x00\x00\x00");
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::WindowUpdate {
                stream_id: STREAM_1,
                increment: 0,
            }
        );
    }

    // ---- unknown types and PUSH_PROMISE ------------------------------------

    #[test]
    fn unknown_frame_types_are_discarded() {
        let mut buf = raw(0x2a, 0xff, 3, b"whatever this is");
        // The unknown frame is consumed; with nothing behind it the codec
        // reports "need more bytes" rather than surfacing it.
        assert_eq!(decode_raw(&mut buf).expect("decode"), None);
        assert!(buf.is_empty(), "the unknown frame should be consumed");
    }

    #[test]
    fn a_frame_after_an_unknown_type_still_decodes() {
        let mut buf = raw(0x2a, 0, 3, b"ignore me");
        let ping = Frame::Ping {
            data: [7; 8],
            ack: true,
        };
        FrameCodec::new(16_384)
            .encode(&ping, &mut buf)
            .expect("encode");
        assert_eq!(decode_raw(&mut buf).expect("decode"), Some(ping));
        assert!(buf.is_empty());
    }

    #[test]
    fn deprecated_priority_frames_are_discarded() {
        // PRIORITY (0x2) is deprecated by RFC 9113; we ignore it like any other
        // unrecognized type rather than modeling a dependency tree.
        let mut buf = raw(0x2, 0, 1, b"\x00\x00\x00\x00\x10");
        assert_eq!(decode_raw(&mut buf).expect("decode"), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn push_promise_is_rejected() {
        let mut buf = raw(PUSH_PROMISE_TYPE, 0, 1, b"\x00\x00\x00\x02");
        let err = decode_raw(&mut buf).expect_err("push is disabled");
        assert_eq!(err, FrameError::UnexpectedPushPromise);
        assert_eq!(err.code(), ErrorCode::ProtocolError);
    }

    // ---- error-code normalization -------------------------------------------

    #[test]
    fn unknown_error_codes_normalize_to_internal_error() {
        // §7 permits treating an unrecognized code as INTERNAL_ERROR, so this
        // one decode is deliberately not byte-preserving.
        let mut buf = raw(FrameType::RstStream.as_u8(), 0, 1, b"\xff\xff\xff\xff");
        let frame = decode_raw(&mut buf).expect("decode").expect("a frame");
        assert_eq!(
            frame,
            Frame::RstStream {
                stream_id: STREAM_1,
                error_code: ErrorCode::InternalError,
            }
        );
    }

    // ---- encode-side guards ---------------------------------------------------

    #[test]
    fn encoding_refuses_a_payload_the_length_field_cannot_hold() {
        let too_big = Frame::Data {
            stream_id: STREAM_1,
            data: Bytes::from(vec![0u8; MAX_ALLOWED_FRAME_SIZE as usize + 1]),
            end_stream: false,
        };
        let err = FrameCodec::new(MAX_ALLOWED_FRAME_SIZE)
            .encode(&too_big, &mut BytesMut::new())
            .expect_err("payload exceeds the 24-bit length field");
        assert_eq!(err.code(), ErrorCode::FrameSizeError);
    }

    #[test]
    fn kind_and_stream_id_accessors_agree_with_the_wire() {
        let frame = Frame::Headers {
            stream_id: STREAM_1,
            block: Bytes::new(),
            end_stream: false,
            end_headers: true,
        };
        assert_eq!(frame.kind(), FrameType::Headers);
        assert_eq!(frame.stream_id(), STREAM_1);
        assert_eq!(
            Frame::Ping {
                data: [0; 8],
                ack: false
            }
            .stream_id(),
            StreamId::CONNECTION,
        );
    }
}
