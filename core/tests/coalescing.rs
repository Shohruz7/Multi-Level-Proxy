//! Coalescing and load balancing: many client *connections* collapsing onto few
//! upstream ones (design doc §4.3), and traffic spreading across backends
//! (§5.1).
//!
//! This is the claim the README leads with, and it is only observable across
//! client connections: a single client multiplexing 50 streams proves the stream
//! machine works, but any per-connection proxy would also pass it. Twenty
//! separate client connections landing on one or two upstream connections is the
//! part that needs a shared pool.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use h2proxy_core::conn::{Connection, Settings};
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(30);

/// A backend that counts the TCP connections it accepts and the requests it
/// serves, and identifies itself in a response header.
async fn spawn_backend(name: &'static str) -> (SocketAddr, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let conns = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (task_conns, task_requests) = (Arc::clone(&conns), Arc::clone(&requests));

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            task_conns.fetch_add(1, Ordering::Relaxed);
            let requests = Arc::clone(&task_requests);
            tokio::spawn(async move {
                let Ok(mut connection) = h2::server::handshake(socket).await else {
                    return;
                };
                while let Some(accepted) = connection.accept().await {
                    let Ok((_request, mut respond)) = accepted else {
                        return;
                    };
                    requests.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        let response = http::Response::builder()
                            .status(200)
                            .header("x-served-by", name)
                            .body(())
                            .expect("response");
                        if let Ok(mut send) = respond.send_response(response, false) {
                            let _ = send.send_data(Bytes::from_static(b"ok"), true);
                        }
                    });
                }
            });
        }
    });
    (addr, conns, requests)
}

/// A running proxy: its address, so tests can open many client connections.
async fn spawn_proxy(backends: Vec<SocketAddr>) -> (SocketAddr, Arc<Shared>) {
    let shared = Shared::new(backends.into_iter().map(Backend::new).collect(), 8);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    Box::leak(Box::new(shutdown_tx.clone()));

    let accept_shared = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let proxy = Proxy::new(Arc::clone(&accept_shared));
            let shutdown = shutdown_tx.subscribe();
            tokio::spawn(async move {
                Connection::with_service(socket, shutdown, Settings::server(), proxy)
                    .run()
                    .await
            });
        }
    });
    (addr, shared)
}

/// Open one client connection and run `streams` requests concurrently over it,
/// returning the `x-served-by` value each response carried.
async fn client_session(proxy: SocketAddr, streams: usize) -> Vec<String> {
    let socket = TcpStream::connect(proxy).await.expect("connect");
    let (mut send_request, connection) = h2::client::handshake(socket).await.expect("handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut pending = Vec::new();
    for _ in 0..streams {
        let request = http::Request::builder()
            .uri("http://proxy.example/hello")
            .body(())
            .expect("request");
        let (response, _) = send_request.send_request(request, true).expect("send");
        pending.push(tokio::spawn(async move {
            let response = response.await.expect("a response");
            let served_by = response
                .headers()
                .get("x-served-by")
                .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
                .unwrap_or_default();
            let mut body = response.into_body();
            while let Some(chunk) = body.data().await {
                let chunk = chunk.expect("a chunk");
                let _ = body.flow_control().release_capacity(chunk.len());
            }
            served_by
        }));
    }

    let mut out = Vec::new();
    for task in pending {
        out.push(task.await.expect("no panic"));
    }
    drop(send_request);
    let _ = tokio::time::timeout(Duration::from_secs(5), driver).await;
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_client_connections_collapse_onto_few_upstream_ones() {
    const CLIENTS: usize = 20;
    const STREAMS: usize = 20;

    let (backend, backend_conns, backend_requests) = spawn_backend("one").await;
    let (proxy, shared) = spawn_proxy(vec![backend]).await;

    let sessions: Vec<_> = (0..CLIENTS)
        .map(|_| tokio::spawn(client_session(proxy, STREAMS)))
        .collect();
    for session in sessions {
        let responses = tokio::time::timeout(TIMEOUT, session)
            .await
            .expect("a session finished")
            .expect("no panic");
        assert_eq!(responses.len(), STREAMS);
        assert!(responses.iter().all(|who| who == "one"));
    }

    assert_eq!(
        backend_requests.load(Ordering::Relaxed),
        CLIENTS * STREAMS,
        "every request reached the backend",
    );
    // 400 streams over 20 client connections, and the backend saw a handful of
    // TCP connections. That collapse is the whole thesis; without a shared pool
    // this would be 20.
    let opened = backend_conns.load(Ordering::Relaxed);
    assert!(
        opened <= 5,
        "{CLIENTS} client connections opened {opened} upstream connections; \
         expected them to coalesce",
    );
    assert_eq!(shared.stats.requests(), (CLIENTS * STREAMS) as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traffic_spreads_across_backends() {
    let (first, _, first_requests) = spawn_backend("first").await;
    let (second, _, second_requests) = spawn_backend("second").await;
    let (proxy, _shared) = spawn_proxy(vec![first, second]).await;

    let responses = tokio::time::timeout(TIMEOUT, client_session(proxy, 60))
        .await
        .expect("the session finished");
    assert_eq!(responses.len(), 60);

    let served: HashSet<&str> = responses.iter().map(String::as_str).collect();
    assert_eq!(
        served,
        HashSet::from(["first", "second"]),
        "both backends must see traffic",
    );
    // Least-outstanding-of-two keeps the split roughly even; the assertion is
    // deliberately loose, because the point is that neither backend is starved,
    // not that the balance is exact.
    for (name, count) in [
        ("first", first_requests.load(Ordering::Relaxed)),
        ("second", second_requests.load(Ordering::Relaxed)),
    ] {
        assert!(count >= 10, "{name} only served {count} of 60 requests");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_backend_does_not_take_the_live_one_down_with_it() {
    let (live, _, live_requests) = spawn_backend("live").await;
    // Nothing listens on port 1, so every connect to it fails.
    let dead = SocketAddr::from(([127, 0, 0, 1], 1));
    let (proxy, _shared) = spawn_proxy(vec![live, dead]).await;

    let responses = tokio::time::timeout(TIMEOUT, client_session(proxy, 40))
        .await
        .expect("the session finished");

    // Half the requests land on a backend that is not there. Those get a 502
    // (an empty `x-served-by`); the rest must still be served normally, on the
    // same client connections. Week 7's health checking is what stops the dead
    // backend being chosen at all — this test is the floor below that: a dead
    // backend degrades the service, it does not break it.
    let served = responses.iter().filter(|who| *who == "live").count();
    assert!(
        served >= 10,
        "only {served} of 40 requests were served while one backend was down",
    );
    assert!(live_requests.load(Ordering::Relaxed) >= 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_traffic_does_not_leak_pool_slots() {
    // The regression test for a leak that only shows up under volume. The proxy
    // dropped a stream's pool lease when the client *reset* it, but not when it
    // simply finished — so every completed request kept its concurrency slot
    // forever. A few hundred requests looked perfect; a few thousand filled
    // every pooled connection and the proxy started answering 503 with nothing
    // in the logs to say why.
    //
    // 2,000 requests through a pool allowed 8 connections is comfortably past
    // where the old code broke (it failed at roughly 8 x the backend's
    // concurrency limit).
    const ROUNDS: usize = 20;
    const PER_ROUND: usize = 100;

    let (backend, backend_conns, _requests) = spawn_backend("one").await;
    let (proxy, shared) = spawn_proxy(vec![backend]).await;

    for round in 0..ROUNDS {
        let responses = tokio::time::timeout(TIMEOUT, client_session(proxy, PER_ROUND))
            .await
            .unwrap_or_else(|_| panic!("round {round} finished"));
        assert!(
            responses.iter().all(|who| who == "one"),
            "round {round} saw a failure: the pool ran out of slots",
        );
        // The slots have to come back as streams end, not just at teardown.
        assert_eq!(
            shared.stats.upstream_streams(),
            0,
            "round {round} left streams outstanding after every response arrived",
        );
    }

    assert_eq!(shared.stats.requests(), (ROUNDS * PER_ROUND) as u64);
    let opened = backend_conns.load(Ordering::Relaxed);
    assert!(
        opened <= 4,
        "{} requests opened {opened} upstream connections; slots are leaking",
        ROUNDS * PER_ROUND,
    );
}
