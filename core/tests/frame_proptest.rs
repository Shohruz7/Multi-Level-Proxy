//! Property tests for the frame codec.
//!
//! Two invariants, over randomly generated frames of every type:
//!
//! 1. **encode → decode is the identity**, and consumes exactly the frame's
//!    octets — the guarantee the whole engine reads and writes against.
//! 2. **decode never panics**, on any input at all. That mirrors the fuzz
//!    target as a cheap check that runs on every `cargo test`.
//!
//! Two normalizations are deliberate and therefore excluded from the
//! generators (both are covered by unit tests in `frame.rs` instead):
//! unrecognized error codes collapse to `INTERNAL_ERROR` (RFC 9113 §7 permits
//! it), and padding/priority fields are stripped on decode and never re-emitted.

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;

use h2proxy_core::conn::ErrorCode;
use h2proxy_core::frame::{Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};
use h2proxy_core::stream::StreamId;

/// Payloads stay modest so the suite is fast; the size limits themselves are
/// unit-tested at their exact boundaries.
const MAX_PAYLOAD: usize = 512;

/// Any stream id a frame may legally be *addressed* to, excluding 0.
fn stream_id() -> impl Strategy<Value = StreamId> {
    (1u32..=0x7fff_ffff).prop_map(StreamId::new)
}

/// Every error code with a defined wire value (§7). Unknown codes are excluded
/// because decoding normalizes them, so they are not round-trippable by design.
fn error_code() -> impl Strategy<Value = ErrorCode> {
    prop::sample::select(vec![
        ErrorCode::NoError,
        ErrorCode::ProtocolError,
        ErrorCode::InternalError,
        ErrorCode::FlowControlError,
        ErrorCode::SettingsTimeout,
        ErrorCode::StreamClosed,
        ErrorCode::FrameSizeError,
        ErrorCode::RefusedStream,
        ErrorCode::Cancel,
        ErrorCode::CompressionError,
        ErrorCode::ConnectError,
        ErrorCode::EnhanceYourCalm,
        ErrorCode::InadequateSecurity,
        ErrorCode::Http11Required,
    ])
}

fn payload() -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..MAX_PAYLOAD).prop_map(Bytes::from)
}

/// Up to 31 arbitrary id/value pairs — one shy of the 32 that would still fit
/// well under the default 16,384-octet max frame size.
fn settings_params() -> impl Strategy<Value = Vec<(u16, u32)>> {
    proptest::collection::vec((any::<u16>(), any::<u32>()), 0..32)
}

fn settings_frame() -> impl Strategy<Value = Frame> {
    (any::<bool>(), settings_params()).prop_map(|(ack, params)| Frame::Settings {
        ack,
        // A SETTINGS ACK carries no parameters on the wire.
        params: if ack { Vec::new() } else { params },
    })
}

fn data_frame() -> impl Strategy<Value = Frame> {
    (stream_id(), payload(), any::<bool>()).prop_map(|(stream_id, data, end_stream)| Frame::Data {
        stream_id,
        data,
        end_stream,
    })
}

fn headers_frame() -> impl Strategy<Value = Frame> {
    (stream_id(), payload(), any::<bool>(), any::<bool>()).prop_map(
        |(stream_id, block, end_stream, end_headers)| Frame::Headers {
            stream_id,
            block,
            end_stream,
            end_headers,
        },
    )
}

fn continuation_frame() -> impl Strategy<Value = Frame> {
    (stream_id(), payload(), any::<bool>()).prop_map(|(stream_id, block, end_headers)| {
        Frame::Continuation {
            stream_id,
            block,
            end_headers,
        }
    })
}

fn rst_stream_frame() -> impl Strategy<Value = Frame> {
    (stream_id(), error_code()).prop_map(|(stream_id, error_code)| Frame::RstStream {
        stream_id,
        error_code,
    })
}

fn ping_frame() -> impl Strategy<Value = Frame> {
    (any::<[u8; 8]>(), any::<bool>()).prop_map(|(data, ack)| Frame::Ping { data, ack })
}

fn go_away_frame() -> impl Strategy<Value = Frame> {
    // Unlike the stream-addressed frames, GOAWAY's last-stream-id may be 0.
    (0u32..=0x7fff_ffff, error_code(), payload()).prop_map(|(last, error_code, debug_data)| {
        Frame::GoAway {
            last_stream_id: StreamId::new(last),
            error_code,
            debug_data,
        }
    })
}

fn window_update_frame() -> impl Strategy<Value = Frame> {
    // Legal on any stream, including 0 (the connection window), and the
    // increment is a 31-bit field.
    (0u32..=0x7fff_ffff, 0u32..=0x7fff_ffff).prop_map(|(stream, increment)| Frame::WindowUpdate {
        stream_id: StreamId::new(stream),
        increment,
    })
}

fn any_frame() -> impl Strategy<Value = Frame> {
    prop_oneof![
        data_frame(),
        headers_frame(),
        continuation_frame(),
        settings_frame(),
        rst_stream_frame(),
        ping_frame(),
        go_away_frame(),
        window_update_frame(),
    ]
}

fn codec() -> FrameCodec {
    FrameCodec::new(MAX_ALLOWED_FRAME_SIZE)
}

proptest! {
    /// Every frame type: what we write is what we read back, exactly.
    #[test]
    fn frames_round_trip(frame in any_frame()) {
        let mut codec = codec();
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).expect("encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("decode")
            .expect("a complete frame");
        prop_assert_eq!(decoded, frame);
        prop_assert!(buf.is_empty(), "the whole frame should be consumed");
    }

    /// Frames arrive back-to-back on a real connection, so decoding a stream of
    /// them must yield the same sequence — this is what proves the reassembly
    /// buffer advances by exactly one frame at a time.
    #[test]
    fn a_stream_of_frames_decodes_in_order(frames in proptest::collection::vec(any_frame(), 0..16)) {
        let mut codec = codec();
        let mut buf = BytesMut::new();
        for frame in &frames {
            codec.encode(frame, &mut buf).expect("encode");
        }

        let mut decoded = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).expect("decode") {
            decoded.push(frame);
        }
        prop_assert_eq!(decoded, frames);
        prop_assert!(buf.is_empty(), "every frame should be consumed");
    }

    /// Feeding a frame one octet at a time must never mis-parse: the decoder
    /// reports "need more" until the final octet arrives, and consumes nothing
    /// in the meantime (RFC 9113 §3.2 — frames split across TCP segments).
    #[test]
    fn a_frame_split_across_reads_decodes_only_when_complete(frame in any_frame()) {
        let mut codec = codec();
        let mut whole = BytesMut::new();
        codec.encode(&frame, &mut whole).expect("encode");

        let mut buf = BytesMut::new();
        for (i, byte) in whole.iter().enumerate() {
            buf.extend_from_slice(&[*byte]);
            if i + 1 < whole.len() {
                let before = buf.len();
                prop_assert_eq!(codec.decode(&mut buf).expect("decode"), None);
                prop_assert_eq!(buf.len(), before, "a partial frame must not be consumed");
            }
        }
        prop_assert_eq!(codec.decode(&mut buf).expect("decode"), Some(frame));
        prop_assert!(buf.is_empty());
    }

    /// The parser faces hostile input on every connection: malformed frames must
    /// come back as `Err`, short ones as `Ok(None)`, and neither may panic.
    #[test]
    fn decoding_arbitrary_bytes_never_panics(
        data in proptest::collection::vec(any::<u8>(), 0..2048)
    ) {
        let mut buf = BytesMut::from(&data[..]);
        let _ = codec().decode(&mut buf);
    }
}
