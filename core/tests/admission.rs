//! Bounded admission: what the proxy does when it is offered more than it can
//! carry.
//!
//! The behaviour under test was added after measuring the alternative. With an
//! unbounded wait-for-a-slot queue, offering 30,000 req/s to a proxy whose
//! comfortable rate was lower did not produce errors and did not produce
//! backpressure — it produced 6,400 requests parked inside the proxy and a p99
//! of 274 ms, while the process used 1.1 of 10 cores. Nothing was working hard;
//! the load had simply been converted into latency, and it stayed converted for
//! as long as the overload lasted.
//!
//! A queue with no bound is not a kindness to the client. It is a promise the
//! proxy cannot keep, made silently. The bound turns that into a **503 the
//! client can act on**, and — the part that matters for anyone downstream — it
//! puts a *stated* ceiling on how long a request can wait before it is either
//! served or refused.
//!
//! Two properties are asserted here, and they are the whole contract:
//!
//!   1. past the bound the proxy answers **503**, not 502 and not silence;
//!   2. shedding is **not** recorded against the backend's health, because the
//!      backend was never asked. Getting this wrong would eject a healthy
//!      backend for the crime of being popular.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use h2proxy_core::conn::{Connection, Settings, Tuning, setting_id};
use h2proxy_core::frame::Frame;
use h2proxy_core::health;
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use support::{RawPeer, header};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(10);

/// How many streams the scripted backend admits at once.
const BACKEND_LIMIT: u32 = 4;
/// How many may then wait for one of those slots.
const PENDING: usize = 2;

/// A backend that accepts everything and answers nothing.
///
/// Silence is the point: every stream it is given stays open, so the proxy's
/// concurrency fills and stays full without the test having to win a race
/// against a backend that might answer first.
async fn spawn_silent_backend(seen: Arc<AtomicU32>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut peer = RawPeer::new(socket);
                peer.server_handshake().await;
                // Advertise the real limit. Without this the proxy has no reason
                // to queue anything: an upstream that never states a
                // `MAX_CONCURRENT_STREAMS` is treated as having no limit, and
                // the path under test is never entered.
                peer.send(&Frame::Settings {
                    ack: false,
                    params: vec![(setting_id::MAX_CONCURRENT_STREAMS, BACKEND_LIMIT)],
                })
                .await;
                while let Some(frame) = peer.next().await {
                    if matches!(frame, Frame::Headers { .. }) {
                        seen.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    addr
}

async fn spawn_proxy(backend: SocketAddr) -> (TcpStream, Arc<Shared>) {
    let shared = Shared::with_tuning(
        vec![Backend::new(backend)],
        // One upstream connection, so the capacity under test is the one the
        // test set rather than the pool's willingness to open another.
        1,
        health::Policy::permissive(),
        Tuning {
            max_pending: PENDING,
            ..Tuning::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    let accept = Arc::clone(&shared);
    tokio::spawn(async move {
        let _keep = shutdown_tx;
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let proxy = Proxy::new(Arc::clone(&accept));
            let shutdown = _keep.subscribe();
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

fn get(path: &str) -> Vec<h2proxy_core::hpack::Header> {
    vec![
        header(":method", "GET"),
        header(":scheme", "http"),
        header(":authority", "admission.test"),
        header(":path", path),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_the_queue_bound_the_proxy_sheds_with_503() {
    let seen = Arc::new(AtomicU32::new(0));
    let backend = spawn_silent_backend(Arc::clone(&seen)).await;
    let (socket, shared) = spawn_proxy(backend).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    // Fill the connection: BACKEND_LIMIT in service, PENDING waiting behind
    // them. None of these can be answered — the backend is silent — so the only
    // thing being established is that the proxy accepted them.
    let capacity = BACKEND_LIMIT as usize + PENDING;
    let mut id = 1u32;

    // The first request is what causes the pool to dial the backend at all, so
    // it has to go on its own: until that connection exists there is no
    // `MAX_CONCURRENT_STREAMS` to obey, and a burst sent now would be admitted
    // under the "no stated limit" rule and never reach the queue.
    peer.send_headers(id, &get("/fill"), true).await;
    id += 2;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while shared.stats.upstream_streams() < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the upstream connection never carried the first request",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Let the backend's SETTINGS be read and applied. It is sent immediately
    // after the handshake, but the connection task takes client messages and
    // socket reads from the same select, so "sent" is not "seen".
    tokio::time::sleep(Duration::from_millis(200)).await;

    for _ in 1..capacity {
        peer.send_headers(id, &get("/fill"), true).await;
        id += 2;
    }
    // The queue is entered only once the limit is reached, so give the rest of
    // the fill a moment to land in it rather than in service.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // One more than the proxy can hold. This is the request under test.
    let shed_id = id;
    peer.send_headers(shed_id, &get("/one-too-many"), true)
        .await;

    let frame = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(move |f| match f {
            Frame::Headers { stream_id, .. } | Frame::RstStream { stream_id, .. } => {
                stream_id.get() == shed_id
            }
            _ => false,
        }),
    )
    .await
    .expect("the shed request must be answered, not left to hang — the whole point of a bound")
    .expect("an answer");

    let Frame::Headers { block, .. } = frame else {
        panic!(
            "expected a response head, got a reset: shedding must be a status the client can read"
        );
    };
    let fields = peer.decode(&block);
    let status = fields
        .iter()
        .find(|h| h.name.as_ref() == b":status")
        .map(|h| {
            String::from_utf8_lossy(&h.value)
                .parse::<u16>()
                .unwrap_or(0)
        })
        .unwrap_or(0);

    assert_eq!(
        status, 503,
        "a request refused for capacity is a 503: we had nothing to try it with, \
         and the client may usefully try again. 502 would blame a backend that \
         was never asked",
    );
    assert!(
        shared.stats.shed_total() >= 1,
        "the refusal must be counted — an unobservable shed is indistinguishable \
         from a dropped request",
    );

    // The backend was never asked about the shed request, so nothing may have
    // been recorded against it. This is the assertion that stops a popular
    // backend from being ejected for being popular.
    assert_eq!(
        shared
            .health
            .state(&Backend::new(backend), tokio::time::Instant::now()),
        health::State::Healthy,
        "shedding is our decision about our own capacity, not evidence about a backend",
    );
    assert!(
        seen.load(Ordering::Relaxed) <= capacity as u32,
        "the shed request must never reach the backend; it saw {} of at most {capacity}",
        seen.load(Ordering::Relaxed),
    );
}
