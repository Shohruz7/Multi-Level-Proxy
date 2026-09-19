//! Admission advertised through SETTINGS, not answered with 503s (ADR 0023).
//!
//! The defect these pin is not subtle once measured and was invisible before:
//! a proxy that refuses work more cheaply than it serves it, against a client
//! that keeps a fixed number of requests in flight, converts overload into a
//! refusal storm. Measured, the bounded-queue build produced ~113,000
//! responses/s of which 93% were 503s, at one seventh the throughput of the
//! build without the bound.
//!
//! So the claim under test is that the limit is **advertised** and **enforced**:
//! advertised, so a conforming client waits for a slot instead of re-offering;
//! enforced, so one that ignores the setting is still bounded.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use h2proxy_core::conn::{Connection, ErrorCode, Settings, setting_id};
use h2proxy_core::frame::Frame;
use h2proxy_core::service::Echo;
use h2proxy_core::stream::StreamId;
use support::{RawPeer, header};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(10);

async fn spawn(budget: Arc<AtomicU32>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let (shutdown_tx, _) = broadcast::channel(1);
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let shutdown = shutdown_tx.subscribe();
            let budget = Arc::clone(&budget);
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), Echo::new(16))
                    .with_admission(budget)
                    .run()
                    .await
            });
        }
    });
    addr
}

fn get(path: &str) -> Vec<h2proxy_core::hpack::Header> {
    vec![
        header(":method", "GET"),
        header(":scheme", "http"),
        header(":authority", "admission.test"),
        header(":path", path),
    ]
}

/// Handshake that keeps the server's SETTINGS instead of discarding it.
///
/// `RawPeer::client_handshake` reads forward to the ack, which throws away every
/// frame on the way — including the one under test here. The first SETTINGS is
/// precisely where the budget has to appear, so this test cannot use it.
async fn handshake_capturing_settings(peer: &mut RawPeer, ack: bool) -> Option<u32> {
    peer.send_raw(h2proxy_core::conn::PREFACE).await;
    peer.send(&Settings::default().to_frame()).await;
    let advertised = next_advertised(peer).await;
    tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| matches!(f, Frame::Settings { ack: true, .. })),
    )
    .await
    .expect("our SETTINGS acknowledged");
    // Acknowledging is what licenses the server to hold us to the limit. Whether
    // we do is the variable in these tests, not an incidental of the handshake.
    if ack {
        peer.send(&Frame::Settings {
            ack: true,
            params: Vec::new(),
        })
        .await;
    }
    advertised
}

/// Pull the advertised concurrency out of the next SETTINGS frame that carries
/// it, ignoring acks and any other settings traffic.
async fn next_advertised(peer: &mut RawPeer) -> Option<u32> {
    let frame = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| {
            matches!(f, Frame::Settings { ack: false, params }
                     if params.iter().any(|(id, _)| *id == setting_id::MAX_CONCURRENT_STREAMS))
        }),
    )
    .await
    .expect("a SETTINGS frame within the timeout")?;
    let Frame::Settings { params, .. } = frame else {
        return None;
    };
    params
        .iter()
        .find(|(id, _)| *id == setting_id::MAX_CONCURRENT_STREAMS)
        .map(|(_, v)| *v)
}

#[tokio::test]
async fn the_handshake_advertises_the_budget_it_was_given() {
    // The budget has to reach the *handshake*, not only later frames: a client
    // that opens its streams immediately would otherwise spend its first round
    // trip over-committed, which is exactly the window in which the measured
    // shedding happened.
    let addr = spawn(Arc::new(AtomicU32::new(4))).await;
    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut peer = RawPeer::new(socket);
    assert_eq!(
        handshake_capturing_settings(&mut peer, true).await,
        Some(4),
        "the first SETTINGS must carry the budget, not the compiled-in default",
    );
}

#[tokio::test]
async fn lowering_the_budget_is_pushed_to_an_open_connection() {
    // The whole point of the shared handle: capacity is discovered while
    // connections are already open, so a budget that only applied at handshake
    // would never constrain the connections that are actually loaded.
    let budget = Arc::new(AtomicU32::new(50));
    let addr = spawn(Arc::clone(&budget)).await;
    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut peer = RawPeer::new(socket);
    assert_eq!(
        handshake_capturing_settings(&mut peer, true).await,
        Some(50)
    );

    budget.store(3, Ordering::Relaxed);
    // Give the connection something to wake up for; the re-advertise happens on
    // a pass of the I/O loop, and an idle connection is correctly parked.
    peer.send(&Frame::Ping {
        data: [0; 8],
        ack: false,
    })
    .await;

    assert_eq!(
        next_advertised(&mut peer).await,
        Some(3),
        "a budget that drops must reach the peer, or the peer keeps opening \
         streams it will only be refused",
    );
}

#[tokio::test]
async fn a_client_that_ignores_the_setting_is_still_bounded() {
    // Advertising is for the conforming client; enforcing is for the other one.
    // REFUSED_STREAM rather than a 503, because this is a stream-level protocol
    // answer and costs no response body — and because a 503 is what the
    // measured refusal storm was made of.
    let addr = spawn(Arc::new(AtomicU32::new(2))).await;
    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut peer = RawPeer::new(socket);
    assert_eq!(handshake_capturing_settings(&mut peer, true).await, Some(2));

    // Three at once against a budget of two, none of them ended, so all three
    // want to be live simultaneously.
    for id in [1u32, 3, 5] {
        peer.send_headers(id, &get("/bytes/8"), false).await;
    }

    let refused = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| {
            matches!(f, Frame::RstStream { stream_id, error_code }
                     if stream_id.get() == 5 && *error_code == ErrorCode::RefusedStream)
        }),
    )
    .await
    .expect("the over-budget stream is answered within the timeout");

    assert!(
        refused.is_some(),
        "the third concurrent stream must be refused when the budget is two",
    );
}

#[tokio::test]
async fn raising_the_budget_lets_more_streams_in() {
    // The other direction, and the one that matters for recovery: a proxy that
    // throttles under load and never un-throttles has traded a cliff for a
    // ratchet.
    let budget = Arc::new(AtomicU32::new(2));
    let addr = spawn(Arc::clone(&budget)).await;
    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut peer = RawPeer::new(socket);
    assert_eq!(handshake_capturing_settings(&mut peer, true).await, Some(2));

    budget.store(8, Ordering::Relaxed);
    peer.send(&Frame::Ping {
        data: [1; 8],
        ack: false,
    })
    .await;
    assert_eq!(next_advertised(&mut peer).await, Some(8));

    // Four concurrent, which the old budget would have refused.
    for id in [1u32, 3, 5, 7] {
        peer.send_headers(id, &get("/bytes/8"), true).await;
    }
    let answered = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(
            |f| matches!(f, Frame::Headers { stream_id, .. } if stream_id.get() == 7),
        ),
    )
    .await
    .expect("answered within the timeout");
    assert!(
        answered.is_some(),
        "stream 7 must be served once the budget has risen to eight",
    );
    let _ = StreamId::new(7);
}

#[tokio::test]
async fn a_peer_that_has_not_acked_yet_is_not_refused() {
    // The regression this pins produced 800 client-visible errors on a benchmark
    // containing no attack and no overload: exactly 16 refusals on each of 50
    // connections, every run, regardless of how long the run was.
    //
    // The cause was a race the client cannot win. RFC 9113 makes the initial
    // SETTINGS_MAX_CONCURRENT_STREAMS *unlimited*, so a client is entitled to
    // open streams before our first SETTINGS arrives. Enforcing a budget of two
    // against a client that had already sent twenty is refusing it for not yet
    // having heard a rule.
    //
    // So: same tiny budget, same over-budget burst, but the peer never
    // acknowledges — and nothing may be refused.
    let addr = spawn(Arc::new(AtomicU32::new(2))).await;
    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut peer = RawPeer::new(socket);
    assert_eq!(
        handshake_capturing_settings(&mut peer, false).await,
        Some(2)
    );

    for id in [1u32, 3, 5] {
        peer.send_headers(id, &get("/bytes/8"), true).await;
    }

    // Stream 5 is the one the acking peer had refused. It must be served here.
    let answered = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(
            |f| matches!(f, Frame::Headers { stream_id, .. } if stream_id.get() == 5),
        ),
    )
    .await
    .expect("the third stream is answered within the timeout");
    assert!(
        answered.is_some(),
        "a peer that has not acknowledged our SETTINGS has not been told the \
         limit, and must not be held to it",
    );
}
