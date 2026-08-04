//! The backpressure bridge (§4.2, ADR 0016) — the week's headline claim, as an
//! executable assertion.
//!
//! The scenario a proxy has to survive: a backend that can produce data far
//! faster than the client will take it. A naive proxy reads as fast as the
//! backend sends and buffers the difference, so its memory tracks the *response
//! size*. This one withholds the upstream's WINDOW_UPDATE until octets have
//! actually reached the client, so the backend runs out of credit and stops —
//! and memory tracks the *window size*, which is a constant.
//!
//! The assertion is on `ProxyStats::peak_buffered` rather than RSS: allocator
//! behaviour is not deterministic and would make the test flaky for reasons that
//! have nothing to do with the protocol. What is measured is exactly the claim —
//! octets received from a backend and not yet delivered to a client.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h2proxy_core::conn::{Connection, Settings};
use h2proxy_core::flow::CONNECTION_WINDOW;
use h2proxy_core::frame::MAX_ALLOWED_FRAME_SIZE;
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(30);

/// Far larger than any window, so a proxy that buffers instead of throttling
/// would have to hold tens of megabytes to pass the delivery check.
const RESPONSE: usize = 64 * 1024 * 1024;

/// A backend that blasts `RESPONSE` octets as fast as its window allows and
/// reports how many it managed to hand to the h2 stack.
async fn spawn_firehose() -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let written = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task_written = Arc::clone(&written);

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let written = Arc::clone(&task_written);
            tokio::spawn(async move {
                let Ok(mut connection) = h2::server::handshake(socket).await else {
                    return;
                };
                while let Some(accepted) = connection.accept().await {
                    let Ok((_request, mut respond)) = accepted else {
                        return;
                    };
                    let written = Arc::clone(&written);
                    tokio::spawn(async move {
                        let response = http::Response::builder().status(200).body(()).unwrap();
                        let Ok(mut send) = respond.send_response(response, false) else {
                            return;
                        };
                        let mut left = RESPONSE;
                        while left > 0 {
                            let want = left.min(MAX_ALLOWED_FRAME_SIZE as usize);
                            send.reserve_capacity(want);
                            // Blocks here once the proxy stops crediting us —
                            // which is the whole point of the test.
                            let Some(Ok(got)) =
                                std::future::poll_fn(|cx| send.poll_capacity(cx)).await
                            else {
                                return;
                            };
                            let take = want.min(got);
                            if send
                                .send_data(Bytes::from(vec![b'x'; take]), false)
                                .is_err()
                            {
                                return;
                            }
                            written.fetch_add(take, std::sync::atomic::Ordering::Relaxed);
                            left -= take;
                        }
                        let _ = send.send_data(Bytes::new(), true);
                    });
                }
            });
        }
    });
    (addr, written)
}

async fn spawn_proxy(backend: std::net::SocketAddr) -> (TcpStream, Arc<Shared>) {
    let shared = Shared::new(vec![Backend::new(backend)], 8);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, _) = broadcast::channel(1);
    // Keep the sender alive for the life of the test.
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

    let client = TcpStream::connect(addr).await.expect("connect");
    (client, shared)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_client_throttles_a_fast_backend_instead_of_filling_memory() {
    let (backend, backend_written) = spawn_firehose().await;
    let (socket, shared) = spawn_proxy(backend).await;

    let (mut send_request, connection) = h2::client::handshake(socket).await.expect("handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = http::Request::builder()
        .uri("http://proxy.example/firehose")
        .body(())
        .expect("request");
    let (response, _) = send_request.send_request(request, true).expect("send");
    let response = tokio::time::timeout(TIMEOUT, response)
        .await
        .expect("a response head")
        .expect("no error");
    let mut body = response.into_body();

    // Read deliberately slowly, releasing capacity a little at a time. This is
    // the "slow client" — and crucially it keeps *reading*, so nothing here is
    // testing TCP backpressure by accident.
    let mut taken = 0usize;
    let target = 4 * 1024 * 1024;
    while taken < target {
        let Some(chunk) = tokio::time::timeout(TIMEOUT, body.data())
            .await
            .expect("the transfer keeps moving")
        else {
            break;
        };
        let chunk = chunk.expect("a chunk");
        taken += chunk.len();
        body.flow_control()
            .release_capacity(chunk.len())
            .expect("release");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let peak = shared.stats.peak_buffered();
    let written = backend_written.load(std::sync::atomic::Ordering::Relaxed);

    // (a) Memory is bounded by the *window*, not the response. One connection
    //     window plus a frame of slack is the honest ceiling: the upstream may
    //     have a full window outstanding, and one more frame may be in flight
    //     when the last release goes out.
    let ceiling = CONNECTION_WINDOW as usize + MAX_ALLOWED_FRAME_SIZE as usize;
    assert!(
        peak <= ceiling,
        "the bridge held {peak} octets; the bound is {ceiling} \
         ({} MiB of response were produced)",
        written / (1024 * 1024),
    );

    // (b) The backend provably *stopped* rather than racing ahead. It cannot get
    //     more than a window beyond what the client has taken, because that
    //     credit is exactly what we withheld.
    assert!(
        written <= taken + ceiling,
        "the backend wrote {written} octets while the client took {taken}: \
         it was not throttled",
    );
    assert!(
        written < RESPONSE,
        "the backend finished the whole {RESPONSE}-octet response, \
         so nothing was ever withheld",
    );

    drop(body);
    drop(send_request);
    let _ = tokio::time::timeout(TIMEOUT, driver).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_stops_reading_entirely_still_bounds_memory() {
    // The harder version: the client reads the head and then never releases a
    // single octet of capacity. A proxy that treats "received" as "delivered"
    // would buffer the entire 64 MiB here.
    let (backend, _written) = spawn_firehose().await;
    let (socket, shared) = spawn_proxy(backend).await;

    let (mut send_request, connection) = h2::client::handshake(socket).await.expect("handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = http::Request::builder()
        .uri("http://proxy.example/firehose")
        .body(())
        .expect("request");
    let (response, _) = send_request.send_request(request, true).expect("send");
    let response = tokio::time::timeout(TIMEOUT, response)
        .await
        .expect("a response head")
        .expect("no error");
    let _body = response.into_body();

    // Let the whole system run flat out for a while against a client that takes
    // nothing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let peak = shared.stats.peak_buffered();
    let ceiling = CONNECTION_WINDOW as usize + MAX_ALLOWED_FRAME_SIZE as usize;
    assert!(
        peak <= ceiling,
        "a client that reads nothing made the bridge hold {peak} octets \
         (bound: {ceiling})",
    );

    drop(send_request);
    let _ = tokio::time::timeout(Duration::from_secs(2), driver).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credit_returns_once_the_client_drains() {
    // The other half of the claim: withholding is not stalling. A client that
    // resumes must get the rest of its response.
    let (backend, _written) = spawn_firehose().await;
    let (socket, _shared) = spawn_proxy(backend).await;

    let (mut send_request, connection) = h2::client::handshake(socket).await.expect("handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = http::Request::builder()
        .uri("http://proxy.example/firehose")
        .body(())
        .expect("request");
    let (response, _) = send_request.send_request(request, true).expect("send");
    let response = tokio::time::timeout(TIMEOUT, response)
        .await
        .expect("a response head")
        .expect("no error");
    let mut body = response.into_body();

    // Stall long enough that every window is certainly exhausted...
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ...then drain at full speed and check the flow restarts.
    let mut taken = 0usize;
    while taken < 8 * 1024 * 1024 {
        let Some(chunk) = tokio::time::timeout(TIMEOUT, body.data())
            .await
            .expect("the transfer resumed after the stall")
        else {
            break;
        };
        let chunk = chunk.expect("a chunk");
        taken += chunk.len();
        body.flow_control()
            .release_capacity(chunk.len())
            .expect("release");
    }
    assert!(
        taken >= 8 * 1024 * 1024,
        "only {taken} octets arrived after the client resumed",
    );

    drop(body);
    drop(send_request);
    let _ = tokio::time::timeout(Duration::from_secs(2), driver).await;
}
