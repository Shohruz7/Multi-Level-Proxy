//! Differential tests: our frame codec against the `h2` crate as oracle
//! (ADR 0003).
//!
//! Every frame an `h2` client puts on the wire must decode with our codec and
//! **re-encode to byte-identical octets**. The harness that makes that possible
//! — our engine scripted as the server — lives in [`support`], and is shared
//! with `hpack_differential.rs`, which makes the analogous (but necessarily
//! weaker) claim one layer up.

mod support;

use std::collections::BTreeSet;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, DuplexStream};

use h2proxy_core::conn::{PREFACE, Settings};
use h2proxy_core::frame::{FRAME_HEADER_LEN, Frame, FrameCodec, FrameType};
use h2proxy_core::hpack::{Header, HpackEncoder};
use h2proxy_core::stream::StreamId;

use support::{Oracle, TIMEOUT, assert_byte_exact};

/// The client's advertised `MAX_FRAME_SIZE` (it sends the protocol default), so
/// the scripted server must chunk its response body to fit.
const PEER_MAX_FRAME_SIZE: usize = 16_384;

// ---------------------------------------------------------------------------
// The original, narrowest case: h2's initial SETTINGS frame.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The full session: every frame type, byte-exact.
// ---------------------------------------------------------------------------

/// The client half of the scripted session: exercise every frame type an h2
/// client can be made to emit.
async fn h2_client_script(io: DuplexStream) {
    let (send_request, mut connection) = h2::client::Builder::new()
        // Raising the connection window makes h2 send a WINDOW_UPDATE for
        // stream 0 immediately after the preface.
        .initial_connection_window_size(1 << 20)
        .handshake::<_, Bytes>(io)
        .await
        .expect("h2 handshake");

    // Must be taken before the connection future moves into its own task.
    let mut ping_pong = connection.ping_pong().expect("ping_pong handle");
    let driver = tokio::spawn(connection);

    let mut send_request = send_request.ready().await.expect("client ready");

    // HEADERS + CONTINUATION: a header value larger than one frame forces h2 to
    // split the block. DATA: the request body. (It only has to beat
    // MAX_FRAME_SIZE; staying well under the 64 KiB MAX_HEADER_LIST_SIZE we
    // advertise keeps this about framing rather than the §6 bomb guard.)
    let oversized = "a".repeat(32_000);
    let request = http::Request::builder()
        .method("POST")
        .uri("https://example.com/upload")
        .header("x-oversized", oversized)
        .body(())
        .expect("build request");
    let (response, mut body) = send_request
        .send_request(request, false)
        .expect("send_request");
    body.send_data(Bytes::from_static(b"hello upstream"), true)
        .expect("send_data");

    // Draining the response body and releasing its capacity is what makes h2
    // emit stream-level WINDOW_UPDATEs.
    let response = response.await.expect("response headers");
    let mut recv = response.into_body();
    while let Some(chunk) = recv.data().await {
        let chunk = chunk.expect("response body chunk");
        recv.flow_control()
            .release_capacity(chunk.len())
            .expect("release capacity");
    }

    // PING: h2's own keepalive, which our server must answer with an ACK.
    ping_pong
        .ping(h2::Ping::opaque())
        .await
        .expect("ping round-trip");

    // RST_STREAM: open a second stream and cancel it.
    let mut send_request = send_request.ready().await.expect("client ready again");
    let request = http::Request::builder()
        .uri("https://example.com/cancelled")
        .body(())
        .expect("build request");
    let (response, mut stream) = send_request
        .send_request(request, false)
        .expect("send_request");
    stream.send_reset(h2::Reason::CANCEL);
    drop(response);
    drop(send_request);

    // The server now provokes a connection error so h2 emits a real GOAWAY (see
    // the RST_STREAM arm of the session loop), which ends the driver with that
    // error. An `h2` *client* has no graceful-shutdown API — only the server
    // does — so this is the one way to get its GOAWAY bytes.
    let _ = driver.await;
}

#[tokio::test]
async fn every_frame_type_round_trips_byte_exactly_against_h2() {
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let client = tokio::spawn(h2_client_script(client_io));

    let session = async {
        let mut oracle = Oracle::new(server_io);
        let mut encoder = HpackEncoder::new(Settings::default().header_table_size as usize);

        // Our half of the preface, then theirs.
        oracle.send(&Settings::server().to_frame()).await;
        oracle.read_preface().await;

        let mut seen: BTreeSet<FrameType> = BTreeSet::new();
        while let Some((frame, raw)) = oracle.next_frame().await {
            // The assertion this whole test exists for.
            assert_byte_exact(&frame, &raw);
            seen.insert(frame.kind());

            match &frame {
                Frame::Settings { ack: false, .. } => {
                    oracle
                        .send(&Frame::Settings {
                            ack: true,
                            params: Vec::new(),
                        })
                        .await;
                }
                Frame::Ping { data, ack: false } => {
                    oracle
                        .send(&Frame::Ping {
                            data: *data,
                            ack: true,
                        })
                        .await;
                }
                // The request is complete: answer it so the client has a body to
                // read and capacity to release.
                Frame::Data {
                    stream_id,
                    end_stream: true,
                    ..
                } => {
                    // The response headers, compressed by our own encoder — if
                    // h2 could not decode them the session would not get as far
                    // as the WINDOW_UPDATEs this arm exists to elicit.
                    let mut block = BytesMut::new();
                    encoder.encode(&[Header::new(":status", "200")], &mut block);
                    oracle
                        .send(&Frame::Headers {
                            stream_id: *stream_id,
                            block: block.freeze(),
                            end_stream: false,
                            end_headers: true,
                        })
                        .await;
                    // Fill the client's 65,535-octet stream window so releasing
                    // it provokes a WINDOW_UPDATE, chunked to the client's
                    // advertised MAX_FRAME_SIZE.
                    let body = vec![b'x'; 60_000];
                    let mut offset = 0;
                    while offset < body.len() {
                        let end = (offset + PEER_MAX_FRAME_SIZE).min(body.len());
                        oracle
                            .send(&Frame::Data {
                                stream_id: *stream_id,
                                data: Bytes::copy_from_slice(&body[offset..end]),
                                end_stream: end == body.len(),
                            })
                            .await;
                        offset = end;
                    }
                }
                // The client has said everything it will say on its own. To see
                // a real GOAWAY we have to earn one: DATA on stream 0 is a
                // connection error (§6.1) that h2 must report with GOAWAY.
                Frame::RstStream { .. } => {
                    oracle
                        .send(&Frame::Data {
                            stream_id: StreamId::CONNECTION,
                            data: Bytes::from_static(b"!"),
                            end_stream: false,
                        })
                        .await;
                }
                Frame::GoAway { .. } => break,
                _ => {}
            }
        }
        seen
    };

    let seen = tokio::time::timeout(TIMEOUT, session)
        .await
        .expect("the differential session did not finish in time");

    for kind in [
        FrameType::Settings,
        FrameType::WindowUpdate,
        FrameType::Headers,
        FrameType::Continuation,
        FrameType::Data,
        FrameType::Ping,
        FrameType::RstStream,
        FrameType::GoAway,
    ] {
        assert!(
            seen.contains(&kind),
            "the session never produced a {kind:?} frame to check; saw {seen:?}",
        );
    }

    tokio::time::timeout(TIMEOUT, client)
        .await
        .expect("the h2 client did not finish")
        .expect("the h2 client panicked");
}
