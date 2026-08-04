//! The milestone: client → proxy → upstream → client, over real sockets.
//!
//! A real `h2` client talks TLS-less h2 to our [`Connection`] running a
//! [`Proxy`]; the proxy opens h2c connections to an `h2::server` backend through
//! the pool. Both ends of the proxy are our own engine, and both peers are the
//! `h2` crate — so a body that arrives intact has been framed, HPACK-encoded,
//! flow-controlled, re-framed and re-encoded correctly twice, in opposite roles.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h2proxy_core::conn::{Connection, Settings};
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(15);

/// A backend that answers `/bytes/<n>` with exactly `n` octets, echoes a POST
/// body back, and can be told to send trailers.
async fn spawn_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let Ok(mut connection) = h2::server::handshake(socket).await else {
                    return;
                };
                while let Some(accepted) = connection.accept().await {
                    let Ok((request, mut respond)) = accepted else {
                        return;
                    };
                    tokio::spawn(async move {
                        let path = request.uri().path().to_string();
                        let wants_trailers = request.headers().contains_key("x-want-trailers");
                        let mut body = request.into_body();

                        let response = http::Response::builder()
                            .status(200)
                            .header("x-backend", "yes")
                            .body(())
                            .expect("response");
                        let mut send = respond.send_response(response, false).expect("head");

                        if let Some(n) = path
                            .strip_prefix("/bytes/")
                            .and_then(|n| n.parse::<usize>().ok())
                        {
                            // Send in chunks so the flow-control path is
                            // actually exercised rather than fitting in one
                            // frame.
                            let chunk = 16 * 1024;
                            let mut left = n;
                            while left > 0 {
                                let take = left.min(chunk);
                                send.reserve_capacity(take);
                                let got = std::future::poll_fn(|cx| send.poll_capacity(cx)).await;
                                let got = match got {
                                    Some(Ok(got)) => got,
                                    _ => return,
                                };
                                let take = take.min(got);
                                if send
                                    .send_data(Bytes::from(vec![b'x'; take]), false)
                                    .is_err()
                                {
                                    return;
                                }
                                left -= take;
                            }
                            let _ = send.send_data(Bytes::new(), !wants_trailers);
                        } else {
                            // Echo whatever the client uploaded.
                            while let Some(chunk) = body.data().await {
                                let Ok(chunk) = chunk else { return };
                                let _ = body.flow_control().release_capacity(chunk.len());
                                if send.send_data(chunk, false).is_err() {
                                    return;
                                }
                            }
                            let _ = send.send_data(Bytes::new(), !wants_trailers);
                        }

                        if wants_trailers {
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("x-checksum", "42".parse().expect("value"));
                            let _ = send.send_trailers(trailers);
                        }
                    });
                }
            });
        }
    });
    addr
}

/// Run the proxy on a socket pair, returning the client end and the shared state
/// (so a test can read the pool gauges).
async fn spawn_proxy(backends: Vec<SocketAddr>) -> (TcpStream, Arc<Shared>, broadcast::Sender<()>) {
    let shared = Shared::new(backends.into_iter().map(Backend::new).collect(), 8);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);

    let accept_shared = Arc::clone(&shared);
    let accept_shutdown = shutdown_tx.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let proxy = Proxy::new(Arc::clone(&accept_shared));
            let shutdown = accept_shutdown.subscribe();
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), proxy)
                    .run()
                    .await
            });
        }
    });

    let client = TcpStream::connect(addr)
        .await
        .expect("connect to the proxy");
    (client, shared, shutdown_tx)
}

/// Drive an `h2` client over `socket`, running `body` with it.
macro_rules! with_client {
    ($socket:expr, |$send:ident| $body:block) => {
        async {
            let (mut $send, connection) = h2::client::handshake($socket).await.expect("handshake");
            // h2's client only makes progress while its connection future is
            // polled, so it gets a task of its own.
            let driver = tokio::spawn(async move {
                let _ = connection.await;
            });
            let out = $body;
            // Dropping the sender is what lets both ends finish: ours waits for
            // the peer to close, and h2's client waits for us.
            drop($send);
            let _ = tokio::time::timeout(TIMEOUT, driver).await;
            out
        }
    };
}

#[tokio::test]
async fn a_request_is_forwarded_and_its_response_comes_back_intact() {
    let backend = spawn_backend().await;
    let (socket, shared, _shutdown) = spawn_proxy(vec![backend]).await;

    let received = tokio::time::timeout(
        TIMEOUT,
        with_client!(socket, |send| {
            let request = http::Request::builder()
                .method("GET")
                .uri("http://proxy.example/bytes/100000")
                .body(())
                .expect("request");
            let (response, _) = send.send_request(request, true).expect("send");
            let response = response.await.expect("a response");
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers().get("x-backend").map(|v| v.as_bytes()),
                Some(&b"yes"[..]),
                "the backend's own fields reach the client",
            );
            let mut body = response.into_body();
            let mut total = 0usize;
            while let Some(chunk) = body.data().await {
                let chunk = chunk.expect("a chunk");
                let _ = body.flow_control().release_capacity(chunk.len());
                total += chunk.len();
            }
            total
        }),
    )
    .await
    .expect("the exchange finished");

    assert_eq!(received, 100_000, "every octet crossed both legs");
    assert_eq!(shared.stats.requests(), 1);
    assert!(
        shared.stats.upstream_connections() >= 1,
        "the pool opened a connection to the backend",
    );
}

#[tokio::test]
async fn a_request_body_is_forwarded_and_echoed_back() {
    let backend = spawn_backend().await;
    let (socket, _shared, _shutdown) = spawn_proxy(vec![backend]).await;

    let echoed = tokio::time::timeout(
        TIMEOUT,
        with_client!(socket, |send| {
            let request = http::Request::builder()
                .method("POST")
                .uri("http://proxy.example/echo")
                .body(())
                .expect("request");
            let (response, mut stream) = send.send_request(request, false).expect("send");
            stream
                .send_data(Bytes::from(vec![b'a'; 40_000]), true)
                .expect("body");

            let response = response.await.expect("a response");
            let mut body = response.into_body();
            let mut total = 0usize;
            while let Some(chunk) = body.data().await {
                let chunk = chunk.expect("a chunk");
                let _ = body.flow_control().release_capacity(chunk.len());
                total += chunk.len();
            }
            total
        }),
    )
    .await
    .expect("the exchange finished");

    assert_eq!(echoed, 40_000, "the request body made it upstream and back");
}

#[tokio::test]
async fn trailers_survive_both_legs() {
    let backend = spawn_backend().await;
    let (socket, _shared, _shutdown) = spawn_proxy(vec![backend]).await;

    let trailers = tokio::time::timeout(
        TIMEOUT,
        with_client!(socket, |send| {
            let request = http::Request::builder()
                .method("GET")
                .uri("http://proxy.example/bytes/1024")
                .header("x-want-trailers", "1")
                .body(())
                .expect("request");
            let (response, _) = send.send_request(request, true).expect("send");
            let response = response.await.expect("a response");
            let mut body = response.into_body();
            while let Some(chunk) = body.data().await {
                let chunk = chunk.expect("a chunk");
                let _ = body.flow_control().release_capacity(chunk.len());
            }
            body.trailers().await.expect("trailers resolved")
        }),
    )
    .await
    .expect("the exchange finished");

    let trailers = trailers.expect("the backend sent trailers");
    assert_eq!(
        trailers.get("x-checksum").map(|v| v.as_bytes()),
        Some(&b"42"[..]),
        "a trailer section has to ride behind the body, not overtake it",
    );
}

#[tokio::test]
async fn many_client_streams_are_all_answered() {
    const N: usize = 50;
    let backend = spawn_backend().await;
    let (socket, shared, _shutdown) = spawn_proxy(vec![backend]).await;

    let sizes = tokio::time::timeout(
        TIMEOUT,
        with_client!(socket, |send| {
            let mut pending = Vec::new();
            for i in 0..N {
                let size = 1000 + i * 10;
                let request = http::Request::builder()
                    .method("GET")
                    .uri(format!("http://proxy.example/bytes/{size}"))
                    .body(())
                    .expect("request");
                let (response, _) = send.send_request(request, true).expect("send");
                pending.push((size, response));
            }
            // Read them concurrently: with a bounded connection window, a
            // sequential reader stalls a correct server (the week-5 note).
            let mut tasks = Vec::new();
            for (size, response) in pending {
                tasks.push(tokio::spawn(async move {
                    let response = response.await.expect("a response");
                    let mut body = response.into_body();
                    let mut total = 0usize;
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk.expect("a chunk");
                        let _ = body.flow_control().release_capacity(chunk.len());
                        total += chunk.len();
                    }
                    (size, total)
                }));
            }
            let mut out = Vec::new();
            for task in tasks {
                out.push(task.await.expect("no panic"));
            }
            out
        }),
    )
    .await
    .expect("every stream finished");

    assert_eq!(sizes.len(), N);
    for (requested, received) in sizes {
        assert_eq!(received, requested, "a stream got the wrong body length");
    }
    assert_eq!(shared.stats.requests(), N as u64);
    // The headline: fifty client streams, and the pool did not open fifty
    // connections to say so.
    assert!(
        shared.stats.upstream_connections() <= 2,
        "expected coalescing, got {} upstream connections",
        shared.stats.upstream_connections(),
    );
}

#[tokio::test]
async fn with_no_backend_reachable_the_client_gets_a_status_not_a_hang() {
    // Port 1 on loopback: nothing listens, and connecting fails fast.
    let (socket, _shared, _shutdown) =
        spawn_proxy(vec![SocketAddr::from(([127, 0, 0, 1], 1))]).await;

    let status = tokio::time::timeout(
        TIMEOUT,
        with_client!(socket, |send| {
            let request = http::Request::builder()
                .uri("http://proxy.example/")
                .body(())
                .expect("request");
            let (response, _) = send.send_request(request, true).expect("send");
            response.await.map(|r| r.status().as_u16())
        }),
    )
    .await
    .expect("the client was answered rather than left waiting");

    assert_eq!(
        status.expect("a response, not a stream error"),
        502,
        "an unreachable backend is a bad gateway, and must be said out loud",
    );
}
