//! Active liveness probing, end to end (design doc §5.2).
//!
//! Passive health checking learns from failures, which means it learns nothing
//! from a backend that produces none. A process that accepts TCP, completes the
//! handshake, and then stops answering fails no request — the requests on it
//! *hang*, and a hang is the one failure mode a proxy must never produce. Every
//! test here is about that gap: the black hole, and the two ways of getting it
//! wrong.
//!
//! Getting it wrong in the first direction is not probing, which is where week 7
//! left this. Getting it wrong in the second is probing too eagerly and
//! disconnecting backends that were fine, which costs real requests to defend
//! against nothing — so the false-positive tests here matter at least as much as
//! the detection one.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use h2proxy_core::conn::{Connection, Settings};
use h2proxy_core::frame::{Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};
use h2proxy_core::health::{self, State};
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use support::{RawPeer, header};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(15);

/// Short enough to run in a test, in the same proportion as the shipped default
/// (`ping_idle == ping_timeout`), so what is being tested is the mechanism and
/// not a special case of the arithmetic.
fn fast_probe() -> health::Policy {
    health::Policy {
        // One failure ejects: this suite is about whether the probe reports at
        // all, and five rounds of a 400 ms cycle would only be testing patience.
        eject_after: 1,
        base_backoff: Duration::from_secs(60),
        max_backoff: Duration::from_secs(60),
        ping_idle: Duration::from_millis(200),
        ping_timeout: Duration::from_millis(200),
        idle_timeout: Duration::from_secs(60),
    }
}

/// A backend that answers nothing at all: it completes TCP, sends its SETTINGS
/// so the connection looks established, then reads its socket into the void
/// forever.
///
/// Reading and discarding is what makes it a *black hole* rather than a slow
/// peer. If it stopped reading, our writes would eventually fill the socket
/// buffer and back up, which is a different failure with a different signature.
/// This one looks perfectly healthy from every angle except the one that counts.
async fn spawn_black_hole() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                // Its SETTINGS goes out, so the handshake completes and the
                // connection is a normal member of the pool. Nothing else ever
                // does: no SETTINGS ack, no response, no RST_STREAM, no GOAWAY,
                // and — the part a probe is the only way to see — no PING ack.
                let mut out = BytesMut::new();
                FrameCodec::new(MAX_ALLOWED_FRAME_SIZE)
                    .encode(&Settings::default().to_frame(), &mut out)
                    .expect("encode SETTINGS");
                let _ = socket.write_all(&out).await;
                let mut sink = [0u8; 4096];
                while socket.read(&mut sink).await.unwrap_or(0) > 0 {}
            });
        }
    });
    addr
}

/// A backend that serves, and counts the connections it was given so a test can
/// tell "the probe kept this connection alive" from "the pool quietly opened a
/// new one".
async fn spawn_backend(conns: Arc<AtomicU32>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            conns.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let mut peer = RawPeer::new(socket);
                peer.server_handshake().await;
                // `RawPeer::next` answers PING itself, as §6.7 requires of any
                // endpoint — so an ordinary scripted backend is one that keeps
                // its liveness promise without the script mentioning it.
                while let Some(frame) = peer.next().await {
                    let Frame::Headers { stream_id, .. } = frame else {
                        continue;
                    };
                    peer.send_headers(stream_id.get(), &[header(":status", "200")], false)
                        .await;
                    peer.send(&Frame::Data {
                        stream_id,
                        data: Bytes::from_static(b"ok"),
                        end_stream: true,
                    })
                    .await;
                }
            });
        }
    });
    addr
}

async fn spawn_proxy(
    backends: Vec<SocketAddr>,
    policy: health::Policy,
) -> (TcpStream, Arc<Shared>) {
    let shared = Shared::with_policy(backends.into_iter().map(Backend::new).collect(), 8, policy);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    let accept = Arc::clone(&shared);
    tokio::spawn(async move {
        let keep = shutdown_tx;
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let proxy = Proxy::new(Arc::clone(&accept));
            let shutdown = keep.subscribe();
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), proxy)
                    .run()
                    .await
            });
        }
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    (client, shared)
}

fn request(path: &str) -> Vec<h2proxy_core::hpack::Header> {
    vec![
        header(":method", "GET"),
        header(":scheme", "http"),
        header(":authority", "probe.test"),
        header(":path", path),
    ]
}

/// Wait for `check` to hold, or fail the test. Polling rather than a fixed sleep
/// because the whole point is a deadline that must actually be met.
async fn within<F: Fn() -> bool>(what: &str, check: F) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{what} did not happen within {TIMEOUT:?}");
}

#[tokio::test]
async fn a_silent_backend_is_detected_by_a_probe_and_ejected() {
    // The gap this feature exists to close. Nothing here fails: the request is
    // accepted, the stream opens, and then there is silence. Without a probe the
    // proxy waits forever and so does the client.
    let dead = spawn_black_hole().await;
    let (socket, shared) = spawn_proxy(vec![dead], fast_probe()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    peer.send_headers(1, &request("/"), true).await;

    within("the probe failure", || shared.stats.probe_failures() > 0).await;
    assert!(
        shared.stats.probes() > 0,
        "a probe failure was counted without a probe being sent",
    );
    within("the ejection", || {
        shared.health.state(&Backend::new(dead), Instant::now()) == State::Ejected
    })
    .await;
    assert!(
        shared.health.ejections() > 0,
        "the connection was closed but health was never told, which is the \
         difference between a probe and a socket recycler",
    );
}

#[tokio::test]
async fn the_client_gets_an_answer_rather_than_hanging_on_a_silent_backend() {
    // The same failure from the client's side, which is the only side that
    // matters. A 502 is a bad answer; no answer is a worse one.
    let dead = spawn_black_hole().await;
    let (socket, _shared) = spawn_proxy(vec![dead], fast_probe()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    peer.send_headers(1, &request("/"), true).await;

    let frame = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| {
            matches!(
                f,
                Frame::Headers { stream_id, .. } | Frame::RstStream { stream_id, .. }
                    if stream_id.get() == 1
            )
        }),
    )
    .await
    .expect("the request hung: no probe closed the connection under it")
    .expect("an outcome");
    assert!(
        matches!(frame, Frame::Headers { .. } | Frame::RstStream { .. }),
        "unexpected outcome {frame:?}",
    );
}

#[tokio::test]
async fn an_idle_connection_is_probed_and_kept() {
    // The other half of the claim. A probe that killed idle connections instead
    // of proving them would look identical in the ejection counter and be a
    // regression: every quiet period would cost a reconnect, and — with the
    // report wired up — an ejection of a backend that was never unwell.
    let conns = Arc::new(AtomicU32::new(0));
    let backend = spawn_backend(Arc::clone(&conns)).await;
    let (socket, shared) = spawn_proxy(vec![backend], fast_probe()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    peer.send_headers(1, &request("/"), true).await;
    tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| matches!(f, Frame::Headers { .. })),
    )
    .await
    .expect("a response")
    .expect("a response");
    assert_eq!(conns.load(Ordering::Relaxed), 1);

    // Sit idle across several probe cycles.
    within("some probes", || shared.stats.probes() >= 3).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        shared.stats.probe_failures(),
        0,
        "a backend that answered every probe was declared dead",
    );
    assert_eq!(shared.health.ejections(), 0);

    // And the connection is still the same one: the probes proved it rather
    // than replacing it.
    peer.send_headers(3, &request("/"), true).await;
    tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(
            |f| matches!(f, Frame::Headers { stream_id, .. } if stream_id.get() == 3),
        ),
    )
    .await
    .expect("a response on the pooled connection")
    .expect("a response");
    assert_eq!(
        conns.load(Ordering::Relaxed),
        1,
        "the pool opened a second connection, so the first was not kept alive",
    );
}

#[tokio::test]
async fn a_busy_connection_is_never_probed() {
    // Probing costs a frame and a wake-up, and neither is worth paying for on a
    // connection whose liveness is being demonstrated by every response on it.
    let conns = Arc::new(AtomicU32::new(0));
    let backend = spawn_backend(Arc::clone(&conns)).await;
    let (socket, shared) = spawn_proxy(vec![backend], fast_probe()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    for id in (1..).step_by(2).take(20) {
        peer.send_headers(id, &request("/"), true).await;
        tokio::time::timeout(
            TIMEOUT,
            peer.next_matching(
                move |f| matches!(f, Frame::Headers { stream_id, .. } if stream_id.get() == id),
            ),
        )
        .await
        .expect("a response")
        .expect("a response");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        shared.stats.probes(),
        0,
        "a connection carrying traffic every 50 ms was probed anyway, on a \
         200 ms idle threshold",
    );
    assert_eq!(shared.health.ejections(), 0);
}
