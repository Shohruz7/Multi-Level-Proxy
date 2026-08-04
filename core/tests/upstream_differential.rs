//! The upstream leg against a real HTTP/2 **server** (ADR 0003, mirrored).
//!
//! Week 3 pointed the `h2` crate at our server side: a mature client only makes
//! progress if the frames we synthesize are ones it accepts, so one passing
//! session covers both directions at once. Week 6 adds a client side, so it gets
//! the same treatment from `h2::server` — the oracle at the other end of the
//! wire.
//!
//! What this proves that a unit test cannot: our preface, SETTINGS, HEADERS
//! encoding, flow-control accounting and END_STREAM placement are all acceptable
//! to an implementation that had no part in writing them.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h2proxy_core::conn::ErrorCode;
use h2proxy_core::hpack::Header;
use h2proxy_core::proxy::ProxyStats;
use h2proxy_core::service::{RequestHead, ServiceEvent};
use h2proxy_core::stream::StreamId;
use h2proxy_core::upstream::{RequestId, ToUpstream, UpstreamConnection, channel};
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(10);

fn request(method: &'static str, path: &'static str) -> RequestHead {
    RequestHead::from_headers(&[
        Header::new(
            Bytes::from_static(b":method"),
            Bytes::from_static(method.as_bytes()),
        ),
        Header::new(Bytes::from_static(b":scheme"), Bytes::from_static(b"http")),
        Header::new(
            Bytes::from_static(b":authority"),
            Bytes::from_static(b"oracle.example"),
        ),
        Header::new(
            Bytes::from_static(b":path"),
            Bytes::from_static(path.as_bytes()),
        ),
    ])
    .expect("well-formed")
}

/// Start our upstream engine on one end of a duplex, handing back the handle to
/// drive it with and the other end to script an `h2::server` on.
fn connect() -> (
    h2proxy_core::upstream::UpstreamHandle,
    tokio::io::DuplexStream,
) {
    let (ours, theirs) = tokio::io::duplex(64 * 1024);
    let (handle, inbox) = channel();
    tokio::spawn(async move {
        UpstreamConnection::new(ours, inbox, Arc::new(ProxyStats::default()), None)
            .run()
            .await
    });
    (handle, theirs)
}

#[tokio::test]
async fn a_real_h2_server_accepts_our_request_and_we_read_its_response() {
    let (ours, theirs) = tokio::io::duplex(64 * 1024);
    let (handle, inbox) = channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let stats = Arc::new(ProxyStats::default());

    let engine = tokio::spawn(async move {
        UpstreamConnection::new(ours, inbox, stats, None)
            .run()
            .await
    });

    // The oracle: a real h2 server, which will reject anything malformed by
    // failing its handshake or erroring on the frame.
    let server = tokio::spawn(async move {
        let mut connection = h2::server::handshake(theirs)
            .await
            .expect("h2 server accepted our preface and SETTINGS");
        let (request, mut respond) = connection
            .accept()
            .await
            .expect("a request arrived")
            .expect("it parsed");

        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(request.uri().path(), "/upload");
        assert_eq!(
            request.uri().authority().map(|a| a.as_str()),
            Some("oracle.example"),
        );

        // Read the request body the engine sends.
        let mut body = request.into_body();
        let mut received = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.expect("a body chunk");
            body.flow_control()
                .release_capacity(chunk.len())
                .expect("release");
            received.extend_from_slice(&chunk);
        }

        let response = http::Response::builder()
            .status(201)
            .header("x-oracle", "yes")
            .body(())
            .expect("response");
        let mut send = respond.send_response(response, false).expect("send head");
        send.send_data(Bytes::from_static(b"pong"), true)
            .expect("send body");

        // Drive the connection to completion.
        let _ = connection.accept().await;
        received
    });

    handle.send(ToUpstream::Request {
        id: RequestId::new(1),
        client_id: StreamId::new(7),
        head: Box::new(request("POST", "/upload")),
        end_stream: false,
        events: events_tx,
    });
    handle.send(ToUpstream::Body {
        id: RequestId::new(1),
        data: Bytes::from_static(b"ping"),
        end_stream: true,
    });

    // The response, translated into the *client's* id space. `BodyAccepted` for
    // the request body may arrive first — the two directions are independent.
    let mut body = Vec::new();
    let mut accepted = 0u32;
    let mut saw_head = false;
    while !saw_head || body.len() < 4 {
        match tokio::time::timeout(TIMEOUT, events_rx.recv())
            .await
            .expect("the oracle answered")
            .expect("an event")
        {
            ServiceEvent::Head { id, response, .. } => {
                assert_eq!(id, StreamId::new(7), "events carry the client's stream id");
                assert_eq!(response.status, 201);
                assert!(
                    response
                        .fields
                        .iter()
                        .any(|f| f.name.as_ref() == b"x-oracle"),
                    "the backend's fields survive the trip",
                );
                saw_head = true;
            }
            ServiceEvent::Data { data, .. } => body.extend_from_slice(&data),
            ServiceEvent::BodyAccepted { n, .. } => accepted += n,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(&body[..], b"pong");
    assert_eq!(
        accepted, 4,
        "the request body was 4 octets, and every one has to be credited back",
    );

    // Close our side first. Both ends wait for the other otherwise — the mirror
    // of the week-5 harness note, and the same deadlock: our engine waits for
    // more work, `h2::server` waits for the client to go away.
    drop(handle);
    let sent = tokio::time::timeout(TIMEOUT, server)
        .await
        .expect("the server finished")
        .expect("no panic");
    assert_eq!(&sent[..], b"ping", "our request body arrived intact");

    let summary = tokio::time::timeout(TIMEOUT, engine)
        .await
        .expect("the engine finished")
        .expect("no panic");
    assert!(summary.handshake_completed);
    assert_eq!(summary.requests_sent, 1);
    assert_eq!(summary.responses_received, 1);
}

#[tokio::test]
async fn many_requests_ride_one_connection() {
    // The coalescing claim at its smallest: twenty independent requests, one
    // socket, correct answers on each.
    const N: u32 = 20;
    let (ours, theirs) = tokio::io::duplex(256 * 1024);
    let (handle, inbox) = channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        UpstreamConnection::new(ours, inbox, Arc::new(ProxyStats::default()), None)
            .run()
            .await
    });

    tokio::spawn(async move {
        let mut connection = h2::server::handshake(theirs).await.expect("handshake");
        while let Some(accepted) = connection.accept().await {
            let (request, mut respond) = accepted.expect("a request");
            // Answer with the path, so each response identifies its request and
            // a crossed id would be visible rather than merely suspected.
            let path = request.uri().path().trim_start_matches('/').to_string();
            tokio::spawn(async move {
                let response = http::Response::builder().status(200).body(()).unwrap();
                let mut send = respond.send_response(response, false).expect("head");
                let _ = send.send_data(Bytes::from(path), true);
            });
        }
    });

    for i in 0..N {
        // Ids from the pool are odd and strictly increasing.
        handle.send(ToUpstream::Request {
            id: RequestId::new(i),
            client_id: StreamId::new(101 + i * 2),
            head: Box::new(request("GET", "/x")),
            end_stream: true,
            events: events_tx.clone(),
        });
    }
    drop(events_tx);

    let mut heads = 0;
    let mut bodies = 0;
    while heads < N || bodies < N {
        let event = tokio::time::timeout(TIMEOUT, events_rx.recv())
            .await
            .expect("progress")
            .expect("an event");
        match event {
            ServiceEvent::Head { id, response, .. } => {
                assert_eq!(response.status, 200);
                assert!(id.get() >= 101, "client ids, not upstream ids");
                heads += 1;
            }
            ServiceEvent::Data { data, .. } => {
                assert_eq!(&data[..], b"x");
                bodies += 1;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_backend_reset_reaches_the_client_as_a_stream_error() {
    let (handle, theirs) = connect();

    tokio::spawn(async move {
        let mut connection = h2::server::handshake(theirs).await.expect("handshake");
        let (_request, mut respond) = connection
            .accept()
            .await
            .expect("a request")
            .expect("parsed");
        respond.send_reset(h2::Reason::REFUSED_STREAM);
        let _ = connection.accept().await;
    });

    let (tx, mut rx) = mpsc::unbounded_channel();
    handle.send(ToUpstream::Request {
        id: RequestId::new(1),
        client_id: StreamId::new(9),
        head: Box::new(request("GET", "/")),
        end_stream: true,
        events: tx,
    });

    let event = tokio::time::timeout(TIMEOUT, rx.recv())
        .await
        .expect("the backend answered")
        .expect("an event");
    match event {
        // A backend refusing one stream must not become a connection error for
        // the client — that is ADR 0008's split holding across the proxy.
        ServiceEvent::Reset { id, code } => {
            assert_eq!(id, StreamId::new(9));
            assert_eq!(code, ErrorCode::RefusedStream);
        }
        other => panic!("expected a reset, got {other:?}"),
    }
}

#[tokio::test]
async fn a_dead_backend_becomes_a_502_rather_than_a_hang() {
    let (ours, theirs) = tokio::io::duplex(4096);
    let (handle, inbox) = channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        UpstreamConnection::new(ours, inbox, Arc::new(ProxyStats::default()), None)
            .run()
            .await
    });

    handle.send(ToUpstream::Request {
        id: RequestId::new(1),
        client_id: StreamId::new(5),
        head: Box::new(request("GET", "/")),
        end_stream: true,
        events: events_tx,
    });
    // The backend vanishes without answering — the failure mode a proxy has to
    // turn into a status rather than a stalled client.
    drop(theirs);

    let event = tokio::time::timeout(TIMEOUT, events_rx.recv())
        .await
        .expect("the client is told something")
        .expect("an event");
    match event {
        ServiceEvent::Head {
            id,
            response,
            end_stream,
        } => {
            assert_eq!(id, StreamId::new(5));
            assert_eq!(response.status, 502);
            assert!(end_stream, "a synthetic error response carries no body");
        }
        other => panic!("expected a 502, got {other:?}"),
    }
}

#[tokio::test]
async fn requests_arriving_out_of_order_still_open_streams_in_order() {
    // The regression test for a real bug. The pool used to hand out the upstream
    // *stream id* at checkout, but §5.1.1 requires ids to increase in the order
    // they reach the wire — and two client connections leasing from the same
    // pooled connection have no ordering between them. A request that leased id
    // 5 could arrive after one that leased id 7, and the backend correctly
    // refused it: real traffic saw sporadic REFUSED_STREAMs under concurrency.
    //
    // Now the id is chosen here, when the HEADERS is queued. Feeding request ids
    // in descending order is the sharpest version of the old failure, and a real
    // `h2` server is the judge: it rejects a non-increasing id outright.
    const N: u32 = 12;
    let (ours, theirs) = tokio::io::duplex(256 * 1024);
    let (handle, inbox) = channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        UpstreamConnection::new(ours, inbox, Arc::new(ProxyStats::default()), None)
            .run()
            .await
    });

    // The server runs until *we* close, so nothing it does can be mistaken for
    // a refusal.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let server_seen = Arc::clone(&seen);
    let server = tokio::spawn(async move {
        let mut connection = h2::server::handshake(theirs).await.expect("handshake");
        while let Some(accepted) = connection.accept().await {
            let (_request, mut respond) = accepted.expect("h2 accepted the stream id");
            server_seen
                .lock()
                .expect("not poisoned")
                .push(respond.stream_id());
            let response = http::Response::builder().status(200).body(()).unwrap();
            let _ = respond.send_response(response, true);
        }
    });

    for i in (0..N).rev() {
        handle.send(ToUpstream::Request {
            id: RequestId::new(i),
            client_id: StreamId::new(1 + i * 2),
            head: Box::new(request("GET", "/")),
            end_stream: true,
            events: events_tx.clone(),
        });
    }
    drop(events_tx);

    let mut answered = 0;
    while answered < N {
        match tokio::time::timeout(TIMEOUT, events_rx.recv())
            .await
            .expect("every request was answered")
            .expect("an event")
        {
            ServiceEvent::Head { response, .. } => {
                assert_eq!(response.status, 200, "no request may be refused");
                answered += 1;
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    drop(handle);
    let _ = tokio::time::timeout(TIMEOUT, server).await;
    let seen = seen.lock().expect("not poisoned");
    let raw: Vec<u32> = seen.iter().map(|id| u32::from(*id)).collect();
    assert_eq!(raw.len(), N as usize, "every request reached the backend");
    assert!(
        raw.windows(2).all(|w| w[0] < w[1]),
        "stream ids reached the wire out of order: {raw:?}",
    );
}
