//! Accounting invariants under a mixed workload at scale (plan §9.4).
//!
//! Every other suite in this repo asks whether one behaviour is correct. This
//! one asks whether the bookkeeping *behind* those behaviours still balances
//! after thousands of requests have taken every path through it — because the
//! two worst bugs of the last two weeks were both invisible at the scale a
//! feature test runs at:
//!
//! - week 6's leaked lease was fine at 400 requests and fatal at 20,000. A lease
//!   is dropped on a path no unit test walks, the concurrency slot is never
//!   returned, and the load balancer's view of every backend drifts until
//!   nothing can be checked out at all.
//! - week 7's `debug_assert!` with a side effect passed 192 tests because they
//!   all ran in debug, and cost 14% of throughput in release.
//!
//! What they have in common is that no single request looks wrong. The failure
//! is in a *total*, so the test has to be about totals. The assertions here are
//! deliberately about quantities that must be exactly zero once the traffic
//! stops: leases outstanding, streams open, octets buffered. A number that is
//! merely small is a leak that has not run long enough yet.
//!
//! The workload mixes every ending a stream has: completion, a backend refusal
//! that is retried, a backend error that is not, a client cancel mid-response,
//! and a backend that dies partway through and is ejected. They are run together
//! rather than one at a time on purpose — the interactions are where the
//! accounting goes wrong, and a retry that overlaps an ejection is exactly the
//! case no feature test constructs.
//!
//! A healthy run, for reference: 3,000 streams, 3,607 attempts, 607 retries, one
//! ejection, 2,100 2xx, **zero 5xx** — a backend was killed under live traffic
//! and no client saw it — and a bridge that peaked at 131,074 octets, which is
//! one cancelled response and change against a ceiling of 1 MiB per upstream
//! connection.

mod support;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use h2proxy_core::conn::{Connection, ErrorCode, Settings};
use h2proxy_core::flow::CONNECTION_WINDOW;
use h2proxy_core::frame::Frame;
use h2proxy_core::health;
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use support::{RawPeer, header};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(60);

/// Enough to make a per-request leak visible as a total, and small enough that
/// the suite stays a test rather than a soak (`just soak` is the long version).
const REQUESTS: u32 = 3_000;

/// How many streams the client keeps in flight. Above one, so the accounting is
/// exercised concurrently rather than as a sequence of isolated round trips.
const WINDOW: u32 = 16;

/// A large enough body that the client can cancel *during* it, which is the
/// ending that leaves the most state behind: a lease, an upstream stream, an
/// unreleased flow-control window, and octets in the bridge.
const BIG_BODY: usize = 128 * 1024;

/// What the client asked for, and therefore what should come back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    /// Served normally.
    Ok,
    /// RST_STREAM(REFUSED_STREAM) from the backend: retried elsewhere, and the
    /// client never learns it happened.
    Refused,
    /// RST_STREAM(INTERNAL_ERROR): reaches the client, because the backend may
    /// well have processed it.
    Failed,
    /// A large response the client abandons after the head arrives.
    Cancelled,
}

impl Shape {
    /// A fixed repeating mix rather than a random one. The point of this suite
    /// is a total that must balance, and a run whose composition changes between
    /// invocations makes a failure harder to reproduce than it needs to be.
    fn of(n: u32) -> Shape {
        match n % 10 {
            0..=5 => Shape::Ok,
            6 | 7 => Shape::Refused,
            8 => Shape::Failed,
            _ => Shape::Cancelled,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Shape::Ok => "/ok",
            Shape::Refused => "/refuse",
            Shape::Failed => "/fail",
            Shape::Cancelled => "/big",
        }
    }
}

/// A backend that answers by path, and can be told to stop accepting so a test
/// can kill it mid-run.
#[derive(Debug)]
struct Script {
    /// When false, new connections are dropped on accept — a backend that has
    /// gone away, from the proxy's point of view.
    accepting: AtomicBool,
    /// Set to make live connections hang up mid-stream.
    hang_up: AtomicBool,
    seen: AtomicU32,
}

impl Script {
    fn new() -> Arc<Self> {
        Arc::new(Script {
            accepting: AtomicBool::new(true),
            hang_up: AtomicBool::new(false),
            seen: AtomicU32::new(0),
        })
    }
}

async fn spawn_backend(script: Arc<Script>) -> SocketAddr {
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
                    let Frame::Headers {
                        stream_id, block, ..
                    } = frame
                    else {
                        continue;
                    };
                    if script.hang_up.load(Ordering::Relaxed) {
                        return;
                    }
                    script.seen.fetch_add(1, Ordering::Relaxed);
                    let fields = peer.decode(&block);
                    let path = fields
                        .iter()
                        .find(|h| h.name.as_ref() == b":path")
                        .map(|h| h.value.to_vec())
                        .unwrap_or_default();
                    match path.as_slice() {
                        b"/refuse" => {
                            peer.send(&Frame::RstStream {
                                stream_id,
                                error_code: ErrorCode::RefusedStream,
                            })
                            .await;
                        }
                        b"/fail" => {
                            peer.send(&Frame::RstStream {
                                stream_id,
                                error_code: ErrorCode::InternalError,
                            })
                            .await;
                        }
                        b"/big" => {
                            peer.send_headers(stream_id.get(), &[header(":status", "200")], false)
                                .await;
                            // In 16 KiB frames, so the client can cancel while
                            // the response is still arriving.
                            for _ in 0..BIG_BODY / 16_384 {
                                peer.send(&Frame::Data {
                                    stream_id,
                                    data: Bytes::from_static(&[b'x'; 16_384]),
                                    end_stream: false,
                                })
                                .await;
                            }
                            peer.send(&Frame::Data {
                                stream_id,
                                data: Bytes::new(),
                                end_stream: true,
                            })
                            .await;
                        }
                        _ => {
                            peer.send_headers(stream_id.get(), &[header(":status", "200")], false)
                                .await;
                            peer.send(&Frame::Data {
                                stream_id,
                                data: Bytes::from_static(b"ok"),
                                end_stream: true,
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

/// The shipped guard with one threshold moved, and the reason stated.
///
/// A tenth of these requests is cancelled, and the run drives tens of thousands
/// of requests a second over loopback — so the client cancels at a few thousand
/// per second, three orders of magnitude past anything the calibration run saw
/// from legitimate traffic (1/s). The default `reset_rate` of 20/s is right for
/// that measurement and this workload is not evidence against it: what is being
/// compressed here is *time*, not behaviour.
///
/// Everything else stays at its shipped value, and the driver fails the test on
/// any GOAWAY — so if a different signal trips, this suite still catches it.
/// Worth carrying into week 8: the deployed run is the first traffic that will
/// cancel at volume for real reasons, and `H2PROXYD_RESET_RATE` exists so that
/// re-calibration costs a restart.
fn cancel_heavy_limits() -> h2proxy_core::guard::Limits {
    h2proxy_core::guard::Limits {
        reset_burst: 10_000.0,
        reset_rate: 10_000.0,
        ..h2proxy_core::guard::Limits::default()
    }
}

async fn spawn_proxy(backends: Vec<SocketAddr>) -> (TcpStream, Arc<Shared>, broadcast::Sender<()>) {
    // The shipped defaults, not a permissive policy: an ejection in the middle
    // of the run is one of the interactions being tested.
    let shared = Shared::with_policy(
        backends.into_iter().map(Backend::new).collect(),
        8,
        health::Policy {
            eject_after: 3,
            base_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(1),
            ..health::Policy::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    let accept = Arc::clone(&shared);
    let signal = shutdown_tx.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let proxy = Proxy::new(Arc::clone(&accept));
            let shutdown = signal.subscribe();
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), proxy)
                    .with_limits(cancel_heavy_limits())
                    .run()
                    .await
            });
        }
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    (client, shared, shutdown_tx)
}

fn request(path: &str) -> Vec<h2proxy_core::hpack::Header> {
    vec![
        header(":method", "GET"),
        header(":scheme", "http"),
        header(":authority", "invariants.test"),
        header(":path", path),
    ]
}

/// Drives `REQUESTS` streams through the proxy with `WINDOW` of them in flight,
/// cancelling the ones that asked for a big body, and returns how many finished.
///
/// Deliberately tolerant about *outcomes*: a request that lands on the backend
/// being killed can end as a 502 rather than a 200, and this suite is not about
/// which. It is about every stream reaching an ending, and about the books
/// balancing once they all have.
async fn drive(peer: &mut RawPeer, kill_at: Option<(u32, Arc<Script>)>) -> u32 {
    let mut next = 0u32;
    let mut open: HashMap<u32, Shape> = HashMap::new();
    let mut finished = 0u32;

    loop {
        while open.len() < WINDOW as usize && next < REQUESTS {
            let shape = Shape::of(next);
            let id = next * 2 + 1;
            peer.send_headers(id, &request(shape.path()), true).await;
            open.insert(id, shape);
            next += 1;
            if let Some((at, script)) = &kill_at
                && next == *at
            {
                // Mid-run, with streams in flight on it: the connection dies
                // under requests that have already been sent, which is what
                // produces `Gone`, retries, and an ejection all at once.
                script.accepting.store(false, Ordering::Relaxed);
                script.hang_up.store(true, Ordering::Relaxed);
            }
        }
        if open.is_empty() {
            return finished;
        }

        let frame = tokio::time::timeout(TIMEOUT, peer.next())
            .await
            .expect("the run stalled: a stream never reached an ending")
            .expect("the proxy closed the connection mid-run");

        match frame {
            Frame::Headers {
                stream_id,
                end_stream,
                ..
            } => {
                let id = stream_id.get();
                if open.get(&id) == Some(&Shape::Cancelled) {
                    // Cancel while the body is still arriving. The most state to
                    // unwind, and the ending most likely to strand a lease.
                    peer.send(&Frame::RstStream {
                        stream_id,
                        error_code: ErrorCode::Cancel,
                    })
                    .await;
                    open.remove(&id);
                    finished += 1;
                } else if end_stream && open.remove(&id).is_some() {
                    finished += 1;
                }
            }
            Frame::Data {
                stream_id,
                ref data,
                end_stream,
            } => {
                // Credit the octets back immediately. A real client releases
                // window as it consumes; this one has nothing to consume, and
                // without the release the connection window runs out partway
                // through the first few hundred cancelled downloads and the
                // whole run stalls — which would be a test bug wearing the
                // costume of a proxy bug.
                let n = data.len() as u32;
                if n > 0 {
                    peer.send(&Frame::WindowUpdate {
                        stream_id: h2proxy_core::stream::StreamId::CONNECTION,
                        increment: n,
                    })
                    .await;
                    if open.contains_key(&stream_id.get()) {
                        peer.send(&Frame::WindowUpdate {
                            stream_id,
                            increment: n,
                        })
                        .await;
                    }
                }
                if end_stream && open.remove(&stream_id.get()).is_some() {
                    finished += 1;
                }
            }
            Frame::RstStream { stream_id, .. } => {
                if open.remove(&stream_id.get()).is_some() {
                    finished += 1;
                }
            }
            // Never expected: legitimate traffic, however much of it, must not
            // be mistaken for abuse. If this fires, a guard threshold is wrong
            // and no amount of exempting this test would fix it (design doc §6).
            Frame::GoAway {
                error_code,
                debug_data,
                ..
            } => panic!(
                "the proxy hung up mid-run with GOAWAY({error_code:?}): {}",
                String::from_utf8_lossy(&debug_data),
            ),
            _ => {}
        }
    }
}

/// Poll until `check` holds. The invariants are about a settled system, and the
/// last upstream stream retires a moment after the client's last frame — the
/// alternative to polling is a sleep long enough to be a flake either way.
async fn settles<F: Fn() -> Option<String>>(check: F) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while Instant::now() < deadline {
        match check() {
            None => return,
            Some(why) => last = Some(why),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{}", last.unwrap_or_else(|| "never settled".into()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_books_balance_after_thousands_of_requests_of_every_shape() {
    let a_script = Script::new();
    let b_script = Script::new();
    let a = spawn_backend(Arc::clone(&a_script)).await;
    let b = spawn_backend(Arc::clone(&b_script)).await;
    let (socket, shared, _shutdown) = spawn_proxy(vec![a, b]).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;

    // One backend dies a third of the way in and stays dead: retries, `Gone`,
    // and an ejection all land while the other backend is still serving.
    let finished = drive(&mut peer, Some((REQUESTS / 3, Arc::clone(&a_script)))).await;
    assert_eq!(finished, REQUESTS, "not every stream reached an ending");

    let backends = shared.backends.clone();
    let stats = Arc::clone(&shared.stats);

    // ---- the invariants ---------------------------------------------------

    let pool_shared = Arc::clone(&shared);
    settles(move || {
        let outstanding: usize = pool_shared
            .pool
            .load(&backends)
            .iter()
            .map(|b| b.outstanding)
            .sum();
        (outstanding != 0).then(|| {
            format!(
                "{outstanding} pool leases were never returned — the load balancer's \
                 view of every backend is now permanently wrong (the week-6 bug)",
            )
        })
    })
    .await;

    let s = Arc::clone(&stats);
    settles(move || {
        (s.client_streams() != 0).then(|| {
            format!(
                "{} client streams are still open after every one of them ended; \
                 a route leaked in `Proxy::routes`",
                s.client_streams(),
            )
        })
    })
    .await;

    let s = Arc::clone(&stats);
    settles(move || {
        (s.upstream_streams() != 0).then(|| {
            format!(
                "{} upstream streams are still open; a route leaked in an \
                 `UpstreamConnection`",
                s.upstream_streams(),
            )
        })
    })
    .await;

    let s = Arc::clone(&stats);
    settles(move || {
        (s.buffered() != 0).then(|| {
            format!(
                "the bridge is still holding {} octets with nothing in flight; \
                 response data was recorded and never released (ADR 0016)",
                s.buffered(),
            )
        })
    })
    .await;

    // Cancelled downloads are where the bridge fills up, so this is the shape of
    // traffic the bound in ADR 0016 was written for. The bound is per upstream
    // connection, so the ceiling scales with how many were opened.
    let ceiling = CONNECTION_WINDOW as usize * stats.connects().max(1) as usize;
    assert!(
        stats.peak_buffered() <= ceiling,
        "the bridge peaked at {} octets, past {ceiling} — the backpressure bound \
         does not hold under cancellation",
        stats.peak_buffered(),
    );

    // Every stream was measured exactly once, on the way out. A latency
    // histogram that quietly drops the failures is the one that looks healthiest
    // when things are worst, so this is a correctness claim about the metric.
    assert_eq!(
        stats.latency_count(),
        u64::from(REQUESTS),
        "{} of {REQUESTS} streams were recorded in the latency histogram",
        stats.latency_count(),
    );

    // The mix actually happened: without this, a run where everything 502'd
    // early would satisfy every assertion above.
    assert!(
        stats.retries() > 0,
        "no retry occurred, so the retry path was never exercised",
    );
    assert!(
        shared.health.ejections() > 0,
        "the killed backend was never ejected, so ejection never overlapped the run",
    );
    assert!(
        b_script.seen.load(Ordering::Relaxed) > REQUESTS / 4,
        "the surviving backend served almost nothing; the run proved little",
    );
    assert!(
        stats.responses(2) > u64::from(REQUESTS / 2),
        "fewer than half the requests were answered 2xx with one backend healthy \
         throughout",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_hangs_up_mid_stream_does_not_leak_its_streams() {
    // Found by the soak, not by reasoning: `h2proxy_client_streams_active` read
    // 458 with no load running and nothing in flight. A peer that hangs up sends
    // neither RST_STREAM nor END_STREAM, so the engine calls neither `cancel`
    // nor `finish`, and the streams were counted as active forever. The gauge
    // was not wrong once — it was wrong permanently and increasingly, which is
    // the worst kind of wrong for a number an operator is meant to page on.
    //
    // Nothing leaked but the *numbers*, which is exactly why it survived a week
    // of tests: the leases release themselves when the routes drop, so the proxy
    // kept working perfectly while its instruments drifted.
    let script = Script::new();
    let backend = spawn_backend(Arc::clone(&script)).await;
    let (socket, shared, _shutdown) = spawn_proxy(vec![backend]).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    const OPEN: u32 = 24;
    for n in 0..OPEN {
        // A large response, so these are still in flight when the socket goes.
        peer.send_headers(n * 2 + 1, &request("/big"), true).await;
    }
    // Wait until the proxy has actually taken them on, or the test proves
    // nothing about streams that were never opened.
    settles(|| {
        (shared.stats.client_streams() < OPEN as usize).then(|| "streams never opened".into())
    })
    .await;

    // The hang-up: no GOAWAY, no RST_STREAM, no FIN handshake to speak of.
    drop(peer);

    settles(|| {
        (shared.stats.client_streams() != 0).then(|| {
            format!(
                "{} client streams are still counted as active after the client \
                 vanished; the gauge can now only climb",
                shared.stats.client_streams(),
            )
        })
    })
    .await;
    assert_eq!(
        shared.stats.latency_count(),
        u64::from(OPEN),
        "streams cut by a client hang-up were dropped from the latency \
         histogram; a histogram that omits the abandoned requests looks \
         healthiest exactly when things are worst",
    );
}

/// Per-connection and per-stream state has to stay small, because week 8's
/// concurrency profile multiplies it by ten thousand.
///
/// A `size_of` assertion looks pedantic until a field is added to `Stream` in a
/// week that is not measuring memory, and the regression shows up as RSS in a
/// deployed load test where it is far more expensive to find. The numbers are
/// not sacred — raising one deliberately is a normal change. Raising one by
/// accident is the thing being caught.
#[test]
fn per_connection_and_per_stream_state_stays_small() {
    use std::mem::size_of;

    assert!(
        size_of::<h2proxy_core::guard::Guard>() <= 384,
        "Guard is {} bytes; it is per client connection",
        size_of::<h2proxy_core::guard::Guard>(),
    );
    assert!(
        size_of::<h2proxy_core::stream::Stream>() <= 128,
        "Stream is {} bytes, so 10k concurrent streams cost {} KiB of table alone",
        size_of::<h2proxy_core::stream::Stream>(),
        size_of::<h2proxy_core::stream::Stream>() * 10_000 / 1024,
    );
}
