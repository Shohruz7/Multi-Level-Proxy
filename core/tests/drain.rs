//! Graceful drain, on both legs (RFC 9113 §6.8, design doc §5.3).
//!
//! Two separate claims, and they fail in opposite directions:
//!
//! - **Draining toward clients.** SIGTERM must not cut a response in half. The
//!   connection advertises GOAWAY, keeps serving what it already accepted, and
//!   only then commits an id and closes.
//! - **Draining toward backends.** A GOAWAY *received* is a request to wind
//!   down, not a hang-up. Streams at or below `last_stream_id` were accepted and
//!   are still coming; only what is above it never started. Treating the whole
//!   connection as dead is what made every rolling backend restart produce a
//!   burst of 502s for requests the backend was still willing to answer.
//!
//! Timing here is real rather than paused, because the thing under test is an
//! interaction between two engines over a socket. The policies are driven to
//! near-zero instead, so the tests stay fast without pretending time away.

mod support;

use support::{RawPeer, header};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use h2proxy_core::conn::{Connection, DrainPolicy, ErrorCode, Settings};
use h2proxy_core::frame::Frame;
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use h2proxy_core::service::Echo;
use h2proxy_core::stream::{MAX_LOCAL_STREAM_ID, StreamId};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(15);

/// Our server engine on a socket, with a drain policy and a shutdown handle.
async fn spawn_server(policy: DrainPolicy) -> (RawPeer, broadcast::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        Connection::with_service(socket, shutdown, Settings::server(), Echo::new(64))
            .with_drain_policy(policy)
            .run()
            .await
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    (RawPeer::new(client), shutdown_tx)
}

/// A POST that never ends, so the stream stays live for as long as the test
/// needs it to. `Echo` answers a bodyless request instantly, which would empty
/// the table before the drain had anything to protect.
async fn open_a_lasting_stream(peer: &mut RawPeer, stream_id: u32) {
    peer.send_headers(
        stream_id,
        &[
            header(":method", "POST"),
            header(":scheme", "http"),
            header(":authority", "drain.test"),
            header(":path", "/echo"),
        ],
        false,
    )
    .await;
}

// ---------------------------------------------------------------------------
// Draining toward clients
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shutdown_advertises_a_goaway_before_committing_an_id() {
    let (mut peer, shutdown) = spawn_server(DrainPolicy {
        grace: Duration::from_millis(300),
        deadline: Duration::from_secs(5),
    })
    .await;
    peer.client_handshake().await;
    open_a_lasting_stream(&mut peer, 1).await;

    // Let the request land before the signal, so the drain has a live stream.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(shutdown);

    let advisory = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| matches!(f, Frame::GoAway { .. })),
    )
    .await
    .expect("a GOAWAY within the timeout")
    .expect("a GOAWAY");
    let Frame::GoAway {
        last_stream_id,
        error_code,
        ..
    } = advisory
    else {
        unreachable!()
    };
    assert_eq!(
        last_stream_id.get(),
        MAX_LOCAL_STREAM_ID,
        "the advisory GOAWAY must commit to no id at all: it says \"open no more \
         streams\", and naming a real id here would retroactively refuse whatever \
         the peer already put on the wire",
    );
    assert_eq!(error_code, ErrorCode::NoError, "a drain is not an error");

    // Finish the request so the drain can complete, then expect the committed one.
    peer.send(&Frame::Data {
        stream_id: StreamId::new(1),
        data: Bytes::from_static(b"done"),
        end_stream: true,
    })
    .await;

    let committed = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| matches!(f, Frame::GoAway { .. })),
    )
    .await
    .expect("a second GOAWAY within the timeout")
    .expect("a second GOAWAY");
    let Frame::GoAway { last_stream_id, .. } = committed else {
        unreachable!()
    };
    assert_eq!(
        last_stream_id.get(),
        1,
        "the committed GOAWAY names the highest stream we actually served",
    );
}

#[tokio::test]
async fn a_stream_opened_after_the_commit_is_refused_not_ignored() {
    let (mut peer, shutdown) = spawn_server(DrainPolicy {
        grace: Duration::ZERO,
        deadline: Duration::from_secs(5),
    })
    .await;
    peer.client_handshake().await;
    open_a_lasting_stream(&mut peer, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(shutdown);

    // With no grace, the commit is immediate — wait for the second GOAWAY.
    let mut goaways = 0;
    while goaways < 2 {
        let frame = tokio::time::timeout(TIMEOUT, peer.next())
            .await
            .expect("frames")
            .expect("still open");
        if matches!(frame, Frame::GoAway { .. }) {
            goaways += 1;
        }
    }

    open_a_lasting_stream(&mut peer, 3).await;
    let reset = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| matches!(f, Frame::RstStream { .. })),
    )
    .await
    .expect("a reset within the timeout")
    .expect("a reset");
    let Frame::RstStream {
        stream_id,
        error_code,
    } = reset
    else {
        unreachable!()
    };
    assert_eq!(stream_id.get(), 3);
    assert_eq!(
        error_code,
        ErrorCode::RefusedStream,
        "REFUSED_STREAM promises the request was never processed, which is what \
         makes it safe for the client to send elsewhere — any other code would \
         make a routine deploy look like a failure",
    );
}

#[tokio::test]
async fn the_deadline_closes_a_connection_whose_streams_never_finish() {
    let (mut peer, shutdown) = spawn_server(DrainPolicy {
        grace: Duration::ZERO,
        deadline: Duration::from_millis(300),
    })
    .await;
    peer.client_handshake().await;
    open_a_lasting_stream(&mut peer, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let began = std::time::Instant::now();
    drop(shutdown);

    // The stream is never completed, so only the deadline can end this.
    let closed =
        tokio::time::timeout(TIMEOUT, async { while peer.next().await.is_some() {} }).await;
    assert!(closed.is_ok(), "the deadline must close the connection");
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "closed after {:?}; the deadline is what bounds a client that never \
         finishes, or a single stalled stream would hold the process open until \
         the container runtime SIGKILLs it",
        began.elapsed(),
    );
}

#[tokio::test]
async fn a_parked_connection_does_not_wait_out_a_grace_that_protects_nothing() {
    // The common case at deploy time: hundreds of kept-alive connections sitting
    // idle. A request in flight is at most one round trip old, so a connection
    // quiet for longer than the whole grace provably has none — and making it
    // wait anyway adds the grace to every rolling restart for nothing.
    //
    // Note what this is *not* asserting: a connection that was merely quiet for
    // an instant. That is the case the grace exists for, and
    // `a_busy_connection_keeps_its_whole_grace` holds the other side of the line.
    let grace = Duration::from_millis(200);
    let (mut peer, shutdown) = spawn_server(DrainPolicy {
        grace,
        deadline: Duration::from_secs(30),
    })
    .await;
    peer.client_handshake().await;

    // Park longer than the grace, which is what makes it a keep-alive rather
    // than a connection that might still have something on the wire.
    tokio::time::sleep(grace * 2).await;

    let began = std::time::Instant::now();
    drop(shutdown);
    let closed =
        tokio::time::timeout(TIMEOUT, async { while peer.next().await.is_some() {} }).await;
    assert!(closed.is_ok(), "a parked connection must close");
    assert!(
        began.elapsed() < grace,
        "a connection parked for {:?} still waited {:?} of its {grace:?} grace",
        grace * 2,
        began.elapsed(),
    );
}

#[tokio::test]
async fn a_busy_connection_keeps_its_whole_grace() {
    // The counterpart to the test above, and the harder one. "Idle connections
    // need no grace" is right, but reading `live_count()` on every pass makes an
    // ordinary gap between two requests look like idleness — and a connection
    // serving short requests is empty most instants. Under `h2load -m 20` that
    // collapsed a 2 s grace to 4.8 ms and closed the connection out from under
    // requests the client was still sending. The emptiness that matters is the
    // one at the moment the signal arrives, so it is sampled once.
    let grace = Duration::from_millis(600);
    let (mut peer, shutdown) = spawn_server(DrainPolicy {
        grace,
        deadline: Duration::from_secs(5),
    })
    .await;
    peer.client_handshake().await;

    // One complete request, so the connection has been busy and is now — for
    // this instant — empty, exactly like the gap between two h2load requests.
    peer.send_headers(
        1,
        &[
            header(":method", "GET"),
            header(":scheme", "http"),
            header(":authority", "drain.test"),
            header(":path", "/bytes/16"),
        ],
        true,
    )
    .await;
    tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| {
            matches!(
                f,
                Frame::Data {
                    end_stream: true,
                    ..
                }
            )
        }),
    )
    .await
    .expect("the response within the timeout")
    .expect("a response");

    let began = std::time::Instant::now();
    drop(shutdown);

    // A new request arriving inside the grace must still be served: that is what
    // the grace is *for*.
    tokio::time::sleep(grace / 3).await;
    peer.send_headers(
        3,
        &[
            header(":method", "GET"),
            header(":scheme", "http"),
            header(":authority", "drain.test"),
            header(":path", "/bytes/16"),
        ],
        true,
    )
    .await;

    let answered = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(
            |f| matches!(f, Frame::Headers { stream_id, .. } if stream_id.get() == 3),
        ),
    )
    .await
    .expect("an answer within the timeout");
    assert!(
        answered.is_some(),
        "a request sent {:?} into a {grace:?} grace was dropped; the connection \
         treated a gap between requests as idleness and committed early",
        began.elapsed(),
    );
}

// ---------------------------------------------------------------------------
// Draining toward backends — the mirror, and the regression
// ---------------------------------------------------------------------------

/// A backend that accepts one request, sends GOAWAY naming it, and *then*
/// answers it — which is exactly what a well-behaved server does on its way
/// down, and what §6.8 requires a client to honour.
async fn spawn_draining_backend(answer_after_goaway: bool) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut peer = RawPeer::new(socket);
        peer.server_handshake().await;

        // Wait for the proxy's first request.
        let first = peer
            .next_matching(|f| matches!(f, Frame::Headers { .. }))
            .await
            .expect("a request");
        let Frame::Headers { stream_id, .. } = first else {
            unreachable!()
        };

        // Going away, but this stream is promised.
        peer.send(&Frame::GoAway {
            last_stream_id: stream_id,
            error_code: ErrorCode::NoError,
            debug_data: Bytes::new(),
        })
        .await;

        if answer_after_goaway {
            tokio::time::sleep(Duration::from_millis(150)).await;
            peer.send_headers(stream_id.get(), &[header(":status", "200")], false)
                .await;
            peer.send(&Frame::Data {
                stream_id,
                data: Bytes::from_static(b"promised-and-delivered"),
                end_stream: true,
            })
            .await;
        }

        // Stay up long enough for the proxy to read it all.
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    addr
}

async fn spawn_proxy_to(backends: Vec<SocketAddr>) -> TcpStream {
    let shared = Shared::new(backends.into_iter().map(Backend::new).collect(), 8);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    tokio::spawn(async move {
        let _keep = shutdown_tx;
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let proxy = Proxy::new(Arc::clone(&shared));
            let shutdown = _keep.subscribe();
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), proxy)
                    .run()
                    .await
            });
        }
    });
    TcpStream::connect(addr).await.expect("connect")
}

#[tokio::test]
async fn a_backend_goaway_lets_the_streams_it_promised_finish() {
    // The regression. Before this, a backend GOAWAY returned straight out of the
    // frame loop and `fail_all_routes` answered every live stream with 502 —
    // including the ones at or below `last_stream_id`, which the backend had
    // explicitly promised to complete and was about to. Every rolling restart
    // produced a burst of errors for requests that were fine.
    let backend = spawn_draining_backend(true).await;
    let socket = spawn_proxy_to(vec![backend]).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    peer.send_headers(
        1,
        &[
            header(":method", "GET"),
            header(":scheme", "http"),
            header(":authority", "drain.test"),
            header(":path", "/"),
        ],
        true,
    )
    .await;

    let head = tokio::time::timeout(
        TIMEOUT,
        peer.next_matching(|f| matches!(f, Frame::Headers { .. })),
    )
    .await
    .expect("a response within the timeout")
    .expect("a response");
    let Frame::Headers { block, .. } = head else {
        unreachable!()
    };
    let fields = peer.decode(&block);
    let status = fields
        .iter()
        .find(|h| h.name.as_ref() == b":status")
        .map(|h| h.value.clone())
        .expect(":status");
    assert_eq!(
        status.as_ref(),
        b"200",
        "a stream the backend promised in its GOAWAY must complete, not become a \
         502: §6.8 says everything at or below last_stream_id was accepted",
    );

    let mut body = BytesMut::new();
    while let Some(frame) = tokio::time::timeout(TIMEOUT, peer.next())
        .await
        .expect("frames")
    {
        if let Frame::Data {
            data, end_stream, ..
        } = frame
        {
            body.extend_from_slice(&data);
            if end_stream {
                break;
            }
        }
    }
    assert_eq!(
        &body[..],
        b"promised-and-delivered",
        "the promised response must arrive intact",
    );
}

#[tokio::test]
async fn a_backend_goaway_refuses_only_what_it_never_started() {
    // The other half of §6.8: above `last_stream_id` nothing was processed, so
    // those become REFUSED_STREAM — retryable — rather than 502.
    let backend = spawn_draining_backend(false).await;
    let socket = spawn_proxy_to(vec![backend]).await;

    let mut peer = RawPeer::new(socket);
    peer.client_handshake().await;
    for id in [1, 3, 5] {
        peer.send_headers(
            id,
            &[
                header(":method", "GET"),
                header(":scheme", "http"),
                header(":authority", "drain.test"),
                header(":path", "/"),
            ],
            true,
        )
        .await;
    }

    // Streams the backend never accepted must be answered, one way or another —
    // a hang is the one outcome a proxy may never produce.
    let mut answered = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while answered < 2 {
        let Ok(Some(frame)) = tokio::time::timeout_at(deadline, peer.next()).await else {
            break;
        };
        match frame {
            Frame::RstStream { stream_id, .. } if stream_id.get() > 1 => answered += 1,
            Frame::Headers { stream_id, .. } if stream_id.get() > 1 => answered += 1,
            _ => {}
        }
    }
    assert_eq!(
        answered, 2,
        "both streams above the backend's last_stream_id must be answered rather \
         than left waiting for a backend that said it would never process them",
    );
}
