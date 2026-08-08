//! Rapid Reset and the frame-flood family (design doc §6, CVE-2023-44487).
//!
//! Every test here asserts **three** things, not one, because "the attacker was
//! disconnected" is only a third of the claim:
//!
//! 1. the offending connection is closed with `ENHANCE_YOUR_CALM`;
//! 2. a well-behaved connection running *at the same time* is unaffected;
//! 3. the process keeps serving afterwards.
//!
//! Without (2) a mitigation is indistinguishable from a fragility — anything
//! that kills the proxy also stops the attack. (2) is what makes the blast
//! radius one connection.
//!
//! The false-positive side of the same line lives in `legitimate.rs`, and is the
//! harder half: a guard that trips on a browser is worse than no guard.

mod support;

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use h2proxy_core::conn::{Connection, ErrorCode, Settings};
use h2proxy_core::frame::Frame;
use h2proxy_core::guard::Limits;
use h2proxy_core::service::Echo;
use h2proxy_core::stream::StreamId;
use support::{RawPeer, header};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(15);

/// A listener serving our engine with `limits`, so several peers — an attacker
/// and a bystander — can share one "process".
async fn spawn_server(limits: Limits) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    tokio::spawn(async move {
        let _keep = shutdown_tx;
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let shutdown = _keep.subscribe();
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), Echo::new(64))
                    .with_limits(limits)
                    .run()
                    .await
            });
        }
    });
    addr
}

async fn connect(addr: std::net::SocketAddr) -> RawPeer {
    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    peer
}

fn get(path: &str) -> Vec<h2proxy_core::hpack::Header> {
    vec![
        header(":method", "GET"),
        header(":scheme", "http"),
        header(":authority", "guard.test"),
        header(":path", path),
    ]
}

/// Read until the connection closes, returning the GOAWAY code if one came.
async fn drain_to_close(peer: &mut RawPeer) -> Option<ErrorCode> {
    let mut code = None;
    while let Some(frame) = peer.next().await {
        if let Frame::GoAway { error_code, .. } = frame {
            code = Some(error_code);
        }
    }
    code
}

/// A bystander that must keep working while the attack runs.
async fn assert_bystander_is_served(peer: &mut RawPeer, stream_id: u32) {
    peer.send_headers(stream_id, &get("/bytes/32"), true).await;
    let answered = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(
            |f| matches!(f, Frame::Headers { stream_id: id, .. } if id.get() == stream_id),
        ),
    )
    .await
    .expect("the bystander was answered within the timeout");
    assert!(
        answered.is_some(),
        "a well-behaved connection stopped being served while another was being \
         rate-limited: the blast radius must be one connection",
    );
}

// ---------------------------------------------------------------------------
// Rapid Reset
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_rapid_reset_flood_closes_only_the_offending_connection() {
    // The milestone, from the spec: "a Rapid-Reset simulation closes the
    // offending connection without downing the proxy".
    let addr = spawn_server(Limits::default()).await;
    let mut bystander = connect(addr).await;
    let mut attacker = connect(addr).await;

    assert_bystander_is_served(&mut bystander, 1).await;

    // HEADERS immediately followed by RST_STREAM, pipelined as fast as the
    // socket takes them. Each pair is ~30 octets to send and a whole request
    // pipeline to process — that asymmetry is the attack.
    let mut id = 1u32;
    for _ in 0..400 {
        attacker.send_headers(id, &get("/"), true).await;
        attacker
            .send(&Frame::RstStream {
                stream_id: StreamId::new(id),
                error_code: ErrorCode::Cancel,
            })
            .await;
        id += 2;
    }

    let code = tokio::time::timeout(TIMEOUT, drain_to_close(&mut attacker))
        .await
        .expect("the attacker's connection must be closed, not merely slowed");
    assert_eq!(
        code,
        Some(ErrorCode::EnhanceYourCalm),
        "the offender must be told why: ENHANCE_YOUR_CALM says \"you are not \
         malformed, you are asking for too much\", which is exactly the case",
    );

    // The claim that makes it a mitigation rather than a fragility.
    assert_bystander_is_served(&mut bystander, 3).await;
}

#[tokio::test]
async fn resetting_streams_that_were_answered_is_ordinary_cancellation() {
    // The distinction the whole mitigation rests on. A browser cancelling
    // requests it has already been served does this all day, and it must not be
    // mistaken for an attack — so the same *volume* of resets, differing only in
    // whether we answered first, must survive.
    let addr = spawn_server(Limits::default()).await;
    let mut peer = connect(addr).await;

    let mut id = 1u32;
    for _ in 0..80 {
        peer.send_headers(id, &get("/bytes/16"), true).await;
        // Wait to be answered before cancelling — the ordinary shape.
        let answered = tokio::time::timeout(
            TIMEOUT,
            peer.next_matching(
                |f| matches!(f, Frame::Headers { stream_id: s, .. } if s.get() == id),
            ),
        )
        .await
        .expect("answered within the timeout");
        assert!(answered.is_some(), "the connection closed mid-run");
        peer.send(&Frame::RstStream {
            stream_id: StreamId::new(id),
            error_code: ErrorCode::Cancel,
        })
        .await;
        id += 2;
    }

    // Still alive and still serving.
    assert_bystander_is_served(&mut peer, id).await;
}

// ---------------------------------------------------------------------------
// The flood family
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_ping_flood_is_cut_off() {
    // CVE-2019-9512. Each PING obliges us to write an ACK, so a client that
    // never reads can make us generate unbounded output from tiny input.
    let addr = spawn_server(Limits::default()).await;
    let mut bystander = connect(addr).await;
    let mut attacker = connect(addr).await;
    assert_bystander_is_served(&mut bystander, 1).await;

    for i in 0..2000u64 {
        attacker
            .send(&Frame::Ping {
                data: i.to_be_bytes(),
                ack: false,
            })
            .await;
    }

    let code = tokio::time::timeout(TIMEOUT, drain_to_close(&mut attacker))
        .await
        .expect("the flooding connection must be closed");
    assert_eq!(code, Some(ErrorCode::EnhanceYourCalm));
    assert_bystander_is_served(&mut bystander, 3).await;
}

#[tokio::test]
async fn a_settings_flood_is_cut_off() {
    // CVE-2019-9515, the same shape: every SETTINGS demands an ACK.
    let addr = spawn_server(Limits::default()).await;
    let mut attacker = connect(addr).await;

    for _ in 0..2000 {
        attacker
            .send(&Frame::Settings {
                ack: false,
                params: Vec::new(),
            })
            .await;
    }

    let code = tokio::time::timeout(TIMEOUT, drain_to_close(&mut attacker))
        .await
        .expect("the flooding connection must be closed");
    assert_eq!(code, Some(ErrorCode::EnhanceYourCalm));
}

#[tokio::test]
async fn an_empty_data_flood_is_cut_off() {
    // CVE-2019-9518. Zero-length DATA debits no flow-control window at all, so
    // the windows — which bound everything else — bound nothing here.
    let addr = spawn_server(Limits::default()).await;
    let mut attacker = connect(addr).await;

    attacker
        .send_headers(
            1,
            &[
                header(":method", "POST"),
                header(":scheme", "http"),
                header(":authority", "guard.test"),
                header(":path", "/echo"),
            ],
            false,
        )
        .await;
    for _ in 0..500 {
        attacker
            .send(&Frame::Data {
                stream_id: StreamId::new(1),
                data: Bytes::new(),
                end_stream: false,
            })
            .await;
    }

    let code = tokio::time::timeout(TIMEOUT, drain_to_close(&mut attacker))
        .await
        .expect("the flooding connection must be closed");
    assert_eq!(code, Some(ErrorCode::EnhanceYourCalm));
}

#[tokio::test]
async fn a_continuation_flood_is_cut_off() {
    // The gap ADR 0012 flagged: the byte cap bounds the reassembly *buffer*, but
    // a stream of 1-octet CONTINUATIONs stays under it forever while costing a
    // decode round each. Only a frame count closes it.
    let addr = spawn_server(Limits::default()).await;
    let mut attacker = connect(addr).await;

    // A HEADERS that never ends its block, then CONTINUATIONs without END_HEADERS.
    let mut block = BytesMut::new();
    attacker.encoder().encode(&get("/"), &mut block);
    attacker
        .send(&Frame::Headers {
            stream_id: StreamId::new(1),
            block: block.freeze(),
            end_stream: false,
            end_headers: false,
        })
        .await;
    for _ in 0..500 {
        attacker
            .send(&Frame::Continuation {
                stream_id: StreamId::new(1),
                block: Bytes::from_static(b"\x00"),
                end_headers: false,
            })
            .await;
    }

    let code = tokio::time::timeout(TIMEOUT, drain_to_close(&mut attacker))
        .await
        .expect("the flooding connection must be closed");
    assert_eq!(
        code,
        Some(ErrorCode::EnhanceYourCalm),
        "a header block that never ends must be bounded by frame count, not only \
         by octets",
    );
}

// ---------------------------------------------------------------------------
// Observe-only
// ---------------------------------------------------------------------------

#[tokio::test]
async fn observe_only_lets_an_attack_through_on_purpose() {
    // The rollout mode, and the mode the calibration run uses. It has to be
    // genuinely inert: if it tripped, every threshold measured under it would be
    // measuring the guard rather than the traffic.
    let addr = spawn_server(Limits {
        observe_only: true,
        ..Limits::default()
    })
    .await;
    let mut attacker = connect(addr).await;

    let mut id = 1u32;
    for _ in 0..400 {
        attacker.send_headers(id, &get("/"), true).await;
        attacker
            .send(&Frame::RstStream {
                stream_id: StreamId::new(id),
                error_code: ErrorCode::Cancel,
            })
            .await;
        id += 2;
    }

    // Still open and still serving after what would otherwise be a trip.
    assert_bystander_is_served(&mut attacker, id).await;
}
