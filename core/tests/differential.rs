//! Differential tests: our frame codec against the `h2` crate as oracle
//! (ADR 0003).
//!
//! `h2` keeps its `frame`/`codec` modules private (not exposed even under its
//! `unstable` feature), so we cannot call its encoder/decoder on a lone frame.
//! Instead we oracle at the *connection boundary*: drive `h2::client::handshake`
//! over an in-memory duplex and capture the real connection preface + initial
//! SETTINGS frame it writes on the wire, then round-trip those bytes through our
//! [`FrameCodec`]. That proves our SETTINGS codec is byte-exact against a mature
//! implementation's real output — not just self-consistent.
//!
//! This is the scaffolding the week-3 frame types plug into as they land.

use bytes::BytesMut;
use tokio::io::AsyncReadExt;

use h2proxy_core::conn::PREFACE;
use h2proxy_core::frame::{FRAME_HEADER_LEN, Frame, FrameCodec};

/// Capture the SETTINGS frame an `h2` client emits at handshake (preface
/// stripped). We do not drive the handshake future to completion — after
/// writing preface + SETTINGS it blocks on our (never-sent) server SETTINGS, but
/// the bytes we care about are already on the wire.
async fn h2_client_initial_settings() -> Vec<u8> {
    let (client_io, mut test_io) = tokio::io::duplex(16 * 1024);

    // Drive a real h2 client. The handshake writes the preface + initial
    // SETTINGS, and the `Connection` future is what actually flushes them and
    // holds the write half open. We keep both handles alive and never send
    // server SETTINGS, so nothing resolves — we just read what the client put on
    // the wire.
    tokio::spawn(async move {
        if let Ok((send_request, connection)) = h2::client::handshake(client_io).await {
            let _send_request = send_request;
            let _ = connection.await;
        }
    });

    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(total) = preface_plus_first_frame_len(&buf)
            && buf.len() >= total
        {
            buf.truncate(total);
            break;
        }
        let n = test_io.read(&mut chunk).await.expect("read from h2 client");
        assert!(n > 0, "h2 client closed before sending its SETTINGS");
        buf.extend_from_slice(&chunk[..n]);
    }

    assert_eq!(
        &buf[..PREFACE.len()],
        &PREFACE[..],
        "unexpected connection preface from h2",
    );
    buf[PREFACE.len()..].to_vec()
}

/// Once `buf` holds the preface plus the first frame's 9-octet header, return
/// the total length of preface + that frame; otherwise `None`.
fn preface_plus_first_frame_len(buf: &[u8]) -> Option<usize> {
    let header_start = PREFACE.len();
    if buf.len() < header_start + FRAME_HEADER_LEN {
        return None;
    }
    let len = &buf[header_start..header_start + 3];
    let payload_len = u32::from_be_bytes([0, len[0], len[1], len[2]]) as usize;
    Some(header_start + FRAME_HEADER_LEN + payload_len)
}

#[tokio::test]
async fn our_codec_round_trips_h2s_real_settings() {
    let settings_bytes = h2_client_initial_settings().await;

    // Our decoder accepts h2's real SETTINGS frame and consumes exactly it.
    let mut codec = FrameCodec::new(16_384);
    let mut buf = BytesMut::from(&settings_bytes[..]);
    let frame = codec
        .decode(&mut buf)
        .expect("decode h2's SETTINGS")
        .expect("a complete frame");
    assert!(buf.is_empty(), "the whole frame should be consumed");
    assert!(
        matches!(frame, Frame::Settings { ack: false, .. }),
        "expected a non-ACK SETTINGS frame, got {frame:?}",
    );

    // Re-encoding it reproduces h2's bytes exactly — the byte-level guarantee.
    let mut reencoded = BytesMut::new();
    codec.encode(&frame, &mut reencoded).expect("re-encode");
    assert_eq!(
        &reencoded[..],
        &settings_bytes[..],
        "our SETTINGS encoding is not byte-identical to h2's",
    );
}
