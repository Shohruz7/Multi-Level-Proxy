//! Idempotent retries and outlier ejection, end to end (design doc §5.2, §5.3).
//!
//! These two features exist to make the same class of failure invisible: a
//! backend that goes away should cost latency, not errors. They are tested
//! together because that is how they work — ejection stops sending to the dead
//! one, and the retry rescues the requests that were already in flight when it
//! died.
//!
//! The line being held on retries is narrow and deliberate. A retry is only safe
//! when the request provably never ran, which needs three things at once: an
//! idempotent method, a promise from the backend that it processed nothing
//! (`REFUSED_STREAM`, which §5.1.2 and §6.8 both define that way), and no
//! `:status` already on the client's wire. Anything less and a retry is a second
//! side effect.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use h2proxy_core::conn::{Connection, ErrorCode, Settings};
use h2proxy_core::frame::Frame;
use h2proxy_core::health::{self, State};
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use support::{RawPeer, header};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(15);

/// How a scripted backend answers each request it receives.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Behaviour {
    /// 200 with a short body naming the backend.
    Serve,
    /// RST_STREAM(REFUSED_STREAM) — "I processed nothing", the retryable answer.
    Refuse,
    /// RST_STREAM(INTERNAL_ERROR) — it may well have run; not retryable.
    Fail,
}

/// A backend whose behaviour a test can change while it runs, and which counts
/// the requests it saw.
#[derive(Debug)]
struct Script {
    behaviour: std::sync::Mutex<Behaviour>,
    seen: AtomicU32,
    accepting: AtomicBool,
}

impl Script {
    fn new(behaviour: Behaviour) -> Arc<Self> {
        Arc::new(Script {
            behaviour: std::sync::Mutex::new(behaviour),
            seen: AtomicU32::new(0),
            accepting: AtomicBool::new(true),
        })
    }
    fn set(&self, behaviour: Behaviour) {
        *self.behaviour.lock().expect("script mutex") = behaviour;
    }
    fn get(&self) -> Behaviour {
        *self.behaviour.lock().expect("script mutex")
    }
    fn seen(&self) -> u32 {
        self.seen.load(Ordering::Relaxed)
    }
}

async fn spawn_backend(script: Arc<Script>, tag: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            if !script.accepting.load(Ordering::Relaxed) {
                drop(socket);
                continue;
            }
            let script = Arc::clone(&script);
            tokio::spawn(async move {
                let mut peer = RawPeer::new(socket);
                peer.server_handshake().await;
                while let Some(frame) = peer.next().await {
                    let Frame::Headers { stream_id, .. } = frame else {
                        continue;
                    };
                    script.seen.fetch_add(1, Ordering::Relaxed);
                    match script.get() {
                        Behaviour::Serve => {
                            peer.send_headers(
                                stream_id.get(),
                                &[header(":status", "200"), header("x-backend", tag)],
                                false,
                            )
                            .await;
                            peer.send(&Frame::Data {
                                stream_id,
                                data: Bytes::from_static(b"ok"),
                                end_stream: true,
                            })
                            .await;
                        }
                        Behaviour::Refuse => {
                            peer.send(&Frame::RstStream {
                                stream_id,
                                error_code: ErrorCode::RefusedStream,
                            })
                            .await;
                        }
                        Behaviour::Fail => {
                            peer.send(&Frame::RstStream {
                                stream_id,
                                error_code: ErrorCode::InternalError,
                            })
                            .await;
                        }
                    }
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

fn request(method: &'static str, path: &str) -> Vec<h2proxy_core::hpack::Header> {
    vec![
        header(":method", method),
        header(":scheme", "http"),
        header(":authority", "retry.test"),
        header(":path", path),
    ]
}

/// What the client saw for one stream: a status, or a reset.
#[derive(PartialEq, Eq, Debug)]
enum Outcome {
    Status(u16, Option<Vec<u8>>),
    Reset(ErrorCode),
}

async fn one_request(
    peer: &mut RawPeer,
    id: u32,
    method: &'static str,
    end_stream: bool,
) -> Outcome {
    peer.send_headers(id, &request(method, "/"), end_stream)
        .await;
    let frame = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(move |f| match f {
            Frame::Headers { stream_id, .. } | Frame::RstStream { stream_id, .. } => {
                stream_id.get() == id
            }
            _ => false,
        }),
    )
    .await
    .expect("an outcome within the timeout")
    .expect("an outcome");

    match frame {
        Frame::RstStream { error_code, .. } => Outcome::Reset(error_code),
        Frame::Headers { block, .. } => {
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
            let tag = fields
                .iter()
                .find(|h| h.name.as_ref() == b"x-backend")
                .map(|h| h.value.to_vec());
            Outcome::Status(status, tag)
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refused_idempotent_get_is_retried_onto_another_backend() {
    let refusing = Script::new(Behaviour::Refuse);
    let serving = Script::new(Behaviour::Serve);
    let a = spawn_backend(Arc::clone(&refusing), "refusing").await;
    let b = spawn_backend(Arc::clone(&serving), "serving").await;
    let (socket, shared) = spawn_proxy(vec![a, b], health::Policy::permissive()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    // Whichever backend is picked first, the request must end up served: if it
    // lands on the refusing one, the retry rescues it.
    let mut served = 0;
    for (n, id) in (1..).step_by(2).take(12).enumerate() {
        match one_request(&mut peer, id, "GET", true).await {
            Outcome::Status(200, _) => served += 1,
            other => panic!("request {n} ended as {other:?}, not a 200"),
        }
    }
    assert_eq!(served, 12);
    assert!(
        refusing.seen() > 0,
        "the refusing backend was never chosen, so this proved nothing",
    );
    assert!(
        shared.stats.retries() > 0,
        "requests were served but no retry was counted",
    );
}

#[tokio::test]
async fn a_refused_post_is_not_retried() {
    // The line that matters most. POST is not idempotent: a retry can charge a
    // card twice, and "it probably did not arrive" is not good enough.
    let refusing = Script::new(Behaviour::Refuse);
    let a = spawn_backend(Arc::clone(&refusing), "refusing").await;
    let (socket, shared) = spawn_proxy(vec![a], health::Policy::permissive()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    let outcome = one_request(&mut peer, 1, "POST", true).await;
    assert_eq!(
        outcome,
        Outcome::Reset(ErrorCode::RefusedStream),
        "a refused POST must reach the client as a refusal, not be sent twice",
    );
    assert_eq!(shared.stats.retries(), 0);
    assert_eq!(
        refusing.seen(),
        1,
        "the backend saw the POST more than once",
    );
}

#[tokio::test]
async fn an_internal_error_is_not_retried() {
    // INTERNAL_ERROR carries no promise that nothing was processed, so even for
    // a GET the request may already have had its effect. Only REFUSED_STREAM
    // makes the promise a retry depends on.
    let failing = Script::new(Behaviour::Fail);
    let a = spawn_backend(Arc::clone(&failing), "failing").await;
    let (socket, shared) = spawn_proxy(vec![a], health::Policy::permissive()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    let outcome = one_request(&mut peer, 1, "GET", true).await;
    assert_eq!(outcome, Outcome::Reset(ErrorCode::InternalError));
    assert_eq!(shared.stats.retries(), 0);
    assert_eq!(failing.seen(), 1);
}

#[tokio::test]
async fn a_retry_is_attempted_at_most_once() {
    // Both backends refuse, so the request cannot be rescued. It must give up
    // rather than bounce between them: a retry policy that keeps trying turns
    // one bad deploy into an amplification attack on your own backends.
    let a_script = Script::new(Behaviour::Refuse);
    let b_script = Script::new(Behaviour::Refuse);
    let a = spawn_backend(Arc::clone(&a_script), "a").await;
    let b = spawn_backend(Arc::clone(&b_script), "b").await;
    let (socket, shared) = spawn_proxy(vec![a, b], health::Policy::permissive()).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    let outcome = one_request(&mut peer, 1, "GET", true).await;
    assert_eq!(outcome, Outcome::Reset(ErrorCode::RefusedStream));
    assert_eq!(shared.stats.retries(), 1, "exactly one retry, then stop");
    assert_eq!(
        a_script.seen() + b_script.seen(),
        2,
        "one original attempt and one retry, no more",
    );
}

// ---------------------------------------------------------------------------
// Ejection and recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failing_backend_is_ejected_and_traffic_keeps_flowing() {
    let sick = Script::new(Behaviour::Fail);
    let healthy = Script::new(Behaviour::Serve);
    let a = spawn_backend(Arc::clone(&sick), "sick").await;
    let b = spawn_backend(Arc::clone(&healthy), "healthy").await;
    let policy = health::Policy {
        eject_after: 3,
        base_backoff: Duration::from_millis(400),
        max_backoff: Duration::from_secs(2),
        ..health::Policy::default()
    };
    let (socket, shared) = spawn_proxy(vec![a, b], policy).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    // Drive enough traffic that the sick backend accumulates its failures.
    for id in (1..).step_by(2).take(40) {
        let _ = one_request(&mut peer, id, "GET", true).await;
    }

    let now = Instant::now();
    assert_eq!(
        shared.health.state(&Backend::new(a), now),
        State::Ejected,
        "a backend failing every request must leave rotation",
    );
    assert!(shared.health.ejections() >= 1);

    // With it ejected, everything now succeeds on the survivor.
    let before = sick.seen();
    for id in (81..).step_by(2).take(20) {
        assert!(
            matches!(
                one_request(&mut peer, id, "GET", true).await,
                Outcome::Status(200, _)
            ),
            "traffic must keep flowing on the healthy backend",
        );
    }
    assert_eq!(
        sick.seen(),
        before,
        "the ejected backend kept receiving requests",
    );
}

#[tokio::test]
async fn a_recovered_backend_is_probed_back_by_a_single_request() {
    let flaky = Script::new(Behaviour::Fail);
    let healthy = Script::new(Behaviour::Serve);
    let a = spawn_backend(Arc::clone(&flaky), "flaky").await;
    let b = spawn_backend(Arc::clone(&healthy), "healthy").await;
    let policy = health::Policy {
        eject_after: 3,
        base_backoff: Duration::from_millis(300),
        max_backoff: Duration::from_secs(2),
        ..health::Policy::default()
    };
    let (socket, shared) = spawn_proxy(vec![a, b], policy).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    for id in (1..).step_by(2).take(40) {
        let _ = one_request(&mut peer, id, "GET", true).await;
    }
    assert_eq!(
        shared.health.state(&Backend::new(a), Instant::now()),
        State::Ejected
    );

    // It recovers, and the ejection expires.
    flaky.set(Behaviour::Serve);
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The probe goes out on ordinary traffic, and its success restores the
    // backend without any request being lost to the transition.
    for id in (201..).step_by(2).take(30) {
        assert!(
            matches!(
                one_request(&mut peer, id, "GET", true).await,
                Outcome::Status(200, _)
            ),
            "no request may be lost while a backend is being probed back",
        );
    }
    assert_eq!(
        shared.health.state(&Backend::new(a), Instant::now()),
        State::Healthy,
        "a backend that answers its probe must return to rotation",
    );
}

#[tokio::test]
async fn a_refusing_backend_is_not_ejected_for_being_busy() {
    // REFUSED_STREAM is what a backend says when it is at its concurrency limit
    // or draining gracefully — both correct behaviour. Ejecting for it is how a
    // load spike becomes an outage.
    let busy = Script::new(Behaviour::Refuse);
    let healthy = Script::new(Behaviour::Serve);
    let a = spawn_backend(Arc::clone(&busy), "busy").await;
    let b = spawn_backend(Arc::clone(&healthy), "healthy").await;
    let policy = health::Policy {
        eject_after: 3,
        ..health::Policy::default()
    };
    let (socket, shared) = spawn_proxy(vec![a, b], policy).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    for id in (1..).step_by(2).take(40) {
        let _ = one_request(&mut peer, id, "GET", true).await;
    }

    assert_eq!(
        shared.health.state(&Backend::new(a), Instant::now()),
        State::Healthy,
        "a backend refusing streams is busy, not broken",
    );
    assert_eq!(shared.health.ejections(), 0);
}
