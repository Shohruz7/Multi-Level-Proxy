//! The concurrency half of the week-5 milestone: hundreds of streams live on
//! one connection, each completing correctly, with big responses interleaved
//! against small ones rather than serialized ahead of them.
//!
//! Unlike `differential.rs` and `hpack_differential.rs`, our engine is the
//! *real* server here, not a scripted oracle — [`Connection::run`] drives the
//! whole thing, so a bug anywhere from the frame codec through the stream table
//! to the outbound scheduler shows up as a hung or wrong response. `h2` plays
//! the client because generating 200 well-formed concurrent request streams by
//! hand would be its own source of bugs.
//!
//! Two claims, and the second is the one that is easy to fake:
//!
//! 1. **Every stream completes.** 200 requests, each asking for a different
//!    response length, all arrive intact.
//! 2. **Responses interleave.** A small response issued alongside a 1 MiB one
//!    finishes first. A server that ran streams to completion in arrival order
//!    would pass claim 1 and fail this — which is exactly why the per-visit byte
//!    budget (`flow::SEND_BUDGET`) exists.
//!
//! Both tests read every stream **concurrently**, one task each, and that is
//! load-bearing rather than stylistic. Interleaving and a bounded connection
//! window together mean no single stream is guaranteed to finish before the
//! window runs out; a client that read its streams strictly in sequence could
//! sit on unreleased data for streams it has not reached yet, never release
//! enough to trigger a WINDOW_UPDATE, and stall a server that is behaving
//! correctly. That is a property of interleaved multiplexing, not a defect, and
//! it is why real clients (and `h2load`) consume streams in parallel.

use std::time::Duration;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use h2proxy_core::conn::{Connection, ConnectionSummary};

/// Enough streams to mean "hundreds" without exceeding the 256-stream
/// concurrency limit the server advertises.
const STREAMS: usize = 200;

/// Generous on a loaded machine, short enough that a deadlock fails the run
/// rather than hanging it.
const TIMEOUT: Duration = Duration::from_secs(20);

/// Response length for stream `n`: varied so a mix-up between streams shows up
/// as a wrong byte count rather than passing silently.
fn body_len(n: usize) -> usize {
    64 + n * 37
}

/// A live connection: our engine as the server, plus the signal that ends it.
///
/// The shutdown signal is how these tests stop the server, rather than letting
/// the client close. Both sides otherwise wait for the other: our server runs
/// until the peer closes or sends GOAWAY, and h2's client connection future
/// stays up as long as the server does. Draining deliberately is also the path
/// the daemon actually takes on SIGTERM, so it is the one worth exercising.
struct Server {
    shutdown: broadcast::Sender<()>,
    task: JoinHandle<ConnectionSummary>,
}

impl Server {
    fn start(io: tokio::io::DuplexStream) -> Server {
        let (shutdown, rx) = broadcast::channel::<()>(1);
        Server {
            shutdown,
            task: tokio::spawn(Connection::new(io, rx).run()),
        }
    }

    async fn finish(self) -> ConnectionSummary {
        let _ = self.shutdown.send(());
        tokio::time::timeout(TIMEOUT, self.task)
            .await
            .expect("the connection did not finish")
            .expect("server panicked")
    }
}

#[tokio::test]
async fn two_hundred_concurrent_streams_all_complete() {
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let server = Server::start(server_io);

    let client = tokio::spawn(async move {
        let (mut send_request, connection) = h2::client::handshake(client_io)
            .await
            .expect("h2 handshake");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });

        // Issue every request before awaiting any response. That is what makes
        // them concurrent: 200 streams open at once, which is also what puts
        // MAX_CONCURRENT_STREAMS under real pressure.
        let mut pending = Vec::with_capacity(STREAMS);
        for n in 0..STREAMS {
            let ready = send_request.ready().await.expect("client ready");
            send_request = ready;
            let request = http::Request::builder()
                .uri(format!("https://example.com/bytes/{}", body_len(n)))
                .body(())
                .expect("build request");
            let (response, _stream) = send_request
                .send_request(request, true)
                .expect("send_request");
            pending.push((n, response));
        }

        // One reader task per stream: see the module doc for why this has to be
        // concurrent rather than a loop.
        let readers: Vec<_> = pending
            .into_iter()
            .map(|(n, response)| {
                tokio::spawn(async move {
                    let response = response.await.unwrap_or_else(|e| panic!("stream {n}: {e}"));
                    assert_eq!(response.status(), 200, "stream {n}");
                    let mut body = response.into_body();
                    let mut received = 0usize;
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk.unwrap_or_else(|e| panic!("stream {n} body: {e}"));
                        received += chunk.len();
                        // Releasing capacity as we go is what keeps the client's
                        // own flow-control window open; without it a large
                        // response stalls at 64 KiB.
                        body.flow_control()
                            .release_capacity(chunk.len())
                            .expect("release capacity");
                    }
                    assert_eq!(received, body_len(n), "stream {n}: response length");
                })
            })
            .collect();

        for reader in readers {
            reader.await.expect("reader panicked");
        }

        drop(send_request);
        driver.abort();
    });

    tokio::time::timeout(TIMEOUT, client)
        .await
        .expect("the client did not finish")
        .expect("client panicked");

    let summary = server.finish().await;

    assert_eq!(summary.streams_opened, STREAMS as u64);
    assert_eq!(summary.streams_reset, 0, "no stream should have been reset");
    assert_eq!(
        summary.data_bytes_sent as usize,
        (0..STREAMS).map(body_len).sum::<usize>(),
        "every octet of every response was written",
    );
    assert!(
        summary.peak_concurrent_streams > 1,
        "streams must have overlapped, not run one at a time (peak was {})",
        summary.peak_concurrent_streams,
    );
}

#[tokio::test]
async fn a_small_response_is_not_starved_by_a_large_one() {
    // The per-visit byte budget, as an executable claim. Both streams are opened
    // together and the large one first; if the scheduler ran streams to
    // completion in order, the small one could not finish first.
    const LARGE: usize = 1024 * 1024;
    const SMALL: usize = 4 * 1024;

    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let server = Server::start(server_io);

    let client = tokio::spawn(async move {
        let (mut send_request, connection) = h2::client::handshake(client_io)
            .await
            .expect("h2 handshake");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut responses = Vec::new();
        for len in [LARGE, SMALL] {
            let ready = send_request.ready().await.expect("client ready");
            send_request = ready;
            let request = http::Request::builder()
                .uri(format!("https://example.com/bytes/{len}"))
                .body(())
                .expect("build request");
            let (response, _stream) = send_request
                .send_request(request, true)
                .expect("send_request");
            responses.push((len, response));
        }

        // Read both bodies concurrently and record which one ends first.
        let mut bodies = Vec::new();
        for (len, response) in responses {
            let response = response.await.expect("response headers");
            bodies.push((len, response.into_body()));
        }

        let mut finished = Vec::new();
        let mut counts = vec![0usize; bodies.len()];
        while finished.len() < bodies.len() {
            for (i, (len, body)) in bodies.iter_mut().enumerate() {
                if finished.contains(&i) {
                    continue;
                }
                match body.data().await {
                    Some(chunk) => {
                        let chunk = chunk.expect("body chunk");
                        counts[i] += chunk.len();
                        body.flow_control()
                            .release_capacity(chunk.len())
                            .expect("release capacity");
                    }
                    None => {
                        assert_eq!(counts[i], *len, "body {i}: response length");
                        finished.push(i);
                    }
                }
            }
        }

        drop(send_request);
        driver.abort();
        finished
    });

    let order = tokio::time::timeout(TIMEOUT, client)
        .await
        .expect("the client did not finish")
        .expect("client panicked");

    assert_eq!(
        order.first(),
        Some(&1),
        "the 4 KiB response must finish before the 1 MiB one; \
         it did not, so the scheduler is running streams to completion in order \
         rather than interleaving them",
    );

    let summary = server.finish().await;
    assert_eq!(
        summary.data_bytes_sent as usize,
        LARGE + SMALL,
        "both bodies were written in full",
    );
}
