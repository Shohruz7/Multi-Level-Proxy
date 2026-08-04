//! h2proxyd — the HTTP/2 multiplexing reverse-proxy daemon.
//!
//! The binary owns everything the protocol engine (`h2proxy-core`) must not:
//! sockets, TLS (rustls, ALPN `h2`), configuration, signals, and logging.
//! Errors here are `anyhow` — "log and exit", never "pick a frame to send"
//! (ADR 0008).
//!
//! Current milestone (week 6): terminate TLS 1.3, negotiate ALPN to `h2`, and
//! hand the byte stream to the protocol engine
//! ([`h2proxy_core::conn::Connection`]), which multiplexes streams and answers
//! them from a [`Proxy`] — forwarding each one to a pooled h2c connection to a
//! backend. With no backends configured it falls back to the built-in [`Echo`]
//! responder, which is what keeps the week-5 server (and its `h2load` targets
//! and h2spec run) available without a rebuild.

mod tls;

use std::net::SocketAddr;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use h2proxy_core::conn::{Connection, Settings};
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use h2proxy_core::service::Echo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

/// Listen address; override with the `H2PROXYD_LISTEN` environment variable.
const DEFAULT_LISTEN: &str = "127.0.0.1:8443";

/// Prometheus scrape address; override with `H2PROXYD_METRICS`.
const DEFAULT_METRICS: &str = "127.0.0.1:9090";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    init_metrics();

    let listen: SocketAddr = std::env::var("H2PROXYD_LISTEN")
        .unwrap_or_else(|_| DEFAULT_LISTEN.to_string())
        .parse()
        .context("parsing H2PROXYD_LISTEN as a socket address")?;

    let acceptor = TlsAcceptor::from(tls::server_config()?);

    // With backends configured the daemon is a proxy; without them it is the
    // week-5 server. Keeping both is what lets `h2spec` and the `h2load`
    // baselines run against the engine alone, with no backend in the numbers.
    let backends = upstreams()?;
    let proxy = if backends.is_empty() {
        info!("no H2PROXYD_UPSTREAMS configured; answering from the built-in responder");
        None
    } else {
        info!(
            upstreams = ?backends.iter().map(|b| b.addr).collect::<Vec<_>>(),
            "proxying to backends over h2c",
        );
        Some(Shared::new(backends, max_conns_per_backend()))
    };
    if let Some(shared) = &proxy {
        spawn_stats_sampler(Arc::clone(shared));
    }

    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding TCP listener on {listen}"))?;
    info!(%listen, "h2proxyd listening; offering ALPN \"h2\" over TLS 1.3");

    // A closed broadcast channel is the shutdown signal: every connection task
    // holds a receiver, so dropping the sole sender wakes them all. This is the
    // seam the graceful GOAWAY drain (design doc §5.3) attaches to in week 7.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut conns = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown_signal() => {
                info!("shutdown signal received; no longer accepting connections");
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    // A transient accept error (e.g. fd exhaustion) must not
                    // take down the whole listener.
                    Err(e) => {
                        warn!(error = %e, "accept failed; continuing");
                        continue;
                    }
                };

                let acceptor = acceptor.clone();
                let shutdown = shutdown_tx.subscribe();
                let proxy = proxy.clone();
                conns.spawn(handle_connection(acceptor, stream, peer, shutdown, proxy));
            }
        }
    }

    // Tell in-flight tasks to stop, then wait for the drain to finish.
    drop(shutdown_tx);
    while conns.join_next().await.is_some() {}
    info!("all connections drained; exiting");
    Ok(())
}

/// Terminate TLS for one accepted connection, confirm ALPN negotiated `h2`, and
/// hand the byte stream to the protocol engine ([`Connection`]), which owns it
/// until the peer closes or the drain signal fires.
async fn handle_connection(
    acceptor: TlsAcceptor,
    stream: TcpStream,
    peer: SocketAddr,
    mut shutdown: broadcast::Receiver<()>,
    proxy: Option<Arc<Shared>>,
) {
    // Race the handshake against shutdown so a stalled TLS negotiation can't
    // hold the drain open.
    let tls_stream = tokio::select! {
        biased;
        _ = shutdown.recv() => return,
        accepted = acceptor.accept(stream) => match accepted {
            Ok(s) => s,
            Err(e) => {
                warn!(%peer, error = %e, "TLS handshake failed");
                return;
            }
        }
    };

    // Copy the negotiated ALPN out so the borrow of `tls_stream` ends before it
    // moves into the engine.
    let alpn = tls_stream.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
    match alpn.as_deref() {
        Some(tls::ALPN_H2) => {
            info!(%peer, alpn = "h2", "TLS 1.3 handshake complete; ALPN negotiated h2");
        }
        Some(other) => {
            warn!(
                %peer,
                alpn = %String::from_utf8_lossy(other),
                "handshake complete but peer did not select h2",
            );
            return;
        }
        None => {
            warn!(%peer, "handshake complete but no ALPN protocol was negotiated");
            return;
        }
    }

    // Hand off to the protocol engine: preface + SETTINGS, then the frame loop,
    // the stream table, and the outbound scheduler. What answers the requests is
    // the one thing that changes between the two modes — the engine cannot tell
    // the difference, which is the whole point of the `Service` seam.
    let settings = Settings::server();
    let summary = match proxy {
        Some(shared) => {
            Connection::with_service(tls_stream, shutdown, settings, Proxy::new(shared))
                .run()
                .await
        }
        None => {
            Connection::with_service(
                tls_stream,
                shutdown,
                settings,
                Echo::new(default_body_size()),
            )
            .run()
            .await
        }
    };

    // The engine reports; the binary instruments (so `h2proxy-core` stays free
    // of the metrics dependency).
    if summary.handshake_completed {
        metrics::counter!("h2proxy_handshakes_total").increment(1);
    }
    metrics::counter!("h2proxy_frames_received_total").increment(summary.frames_received);
    metrics::counter!("h2proxy_header_blocks_decoded_total")
        .increment(summary.header_blocks_decoded);
    metrics::counter!("h2proxy_streams_opened_total").increment(summary.streams_opened);
    metrics::counter!("h2proxy_requests_total").increment(summary.requests_dispatched);
    metrics::counter!("h2proxy_streams_reset_total").increment(summary.streams_reset);
    metrics::counter!("h2proxy_data_bytes_sent_total").increment(summary.data_bytes_sent);
    metrics::counter!("h2proxy_flow_control_stalls_total").increment(summary.flow_control_stalls);
    metrics::gauge!("h2proxy_stream_concurrency_max")
        .set(f64::from(summary.peak_concurrent_streams));
    info!(
        %peer,
        handshake = summary.handshake_completed,
        frames = summary.frames_received,
        header_blocks = summary.header_blocks_decoded,
        streams = summary.streams_opened,
        resets = summary.streams_reset,
        peak_streams = summary.peak_concurrent_streams,
        data_bytes = summary.data_bytes_sent,
        stalls = summary.flow_control_stalls,
        "connection closed",
    );
}

/// The backends to proxy to, from `H2PROXYD_UPSTREAMS` — a comma-separated list
/// of `host:port`. Empty (or unset) keeps the built-in responder.
///
/// Static configuration on purpose: a dynamic control plane is a stated non-goal
/// (see the README), and resolving names at startup would hide a backend that
/// moved behind a cache with no TTL anyone chose.
fn upstreams() -> anyhow::Result<Vec<Backend>> {
    let raw = std::env::var("H2PROXYD_UPSTREAMS").unwrap_or_default();
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse::<SocketAddr>()
                .map(Backend::new)
                .with_context(|| format!("parsing upstream {entry:?} as a socket address"))
        })
        .collect()
}

/// How many connections the pool may open to a single backend, via
/// `H2PROXYD_MAX_UPSTREAM_CONNS`.
///
/// A ceiling, not a target: the pool fills one connection before opening
/// another, so a backend advertising a generous `MAX_CONCURRENT_STREAMS` stays
/// at one however many clients arrive. That collapse is the number to watch in
/// `h2proxy_upstream_pool_connections`.
fn max_conns_per_backend() -> usize {
    std::env::var("H2PROXYD_MAX_UPSTREAM_CONNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

/// Publish the proxy's live counters once a second.
///
/// Sampled rather than reported at connection close, because upstream
/// connections are long-lived by design: a pool that is working correctly might
/// not close a connection for hours, and a gauge that only moves on teardown
/// would read zero through an entire load test.
fn spawn_stats_sampler(shared: Arc<Shared>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let stats = &shared.stats;
            metrics::gauge!("h2proxy_upstream_pool_connections")
                .set(stats.upstream_connections() as f64);
            metrics::gauge!("h2proxy_upstream_streams_active").set(stats.upstream_streams() as f64);
            metrics::gauge!("h2proxy_bridge_buffered_bytes").set(stats.buffered() as f64);
            metrics::gauge!("h2proxy_bridge_buffered_bytes_peak").set(stats.peak_buffered() as f64);
            metrics::gauge!("h2proxy_upstream_connects_total").set(stats.connects() as f64);
            metrics::gauge!("h2proxy_upstream_connect_failures_total")
                .set(stats.connect_failures() as f64);
        }
    });
}

/// Body size the built-in responder returns for a request that does not ask for
/// a specific one, via `H2PROXYD_BODY_SIZE`. Mirrors the `backend` crate's
/// `BACKEND_BODY_SIZE`, so a benchmark profile can size both ends the same way
/// without a rebuild.
fn default_body_size() -> usize {
    std::env::var("H2PROXYD_BODY_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024)
}

/// Resolve when the process is asked to stop: SIGTERM (container stop) or
/// SIGINT (Ctrl-C) on Unix, Ctrl-C elsewhere.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Install the Prometheus `/metrics` exporter and seed the RED/gauge series so
/// they exist before there is any traffic (design doc §7). The real values are
/// wired as the engine lands (weeks 5–7); these are the stub hooks. A bind
/// failure (e.g. the port is taken) is logged and skipped rather than fatal —
/// the proxy must still serve without metrics.
fn init_metrics() {
    use std::net::SocketAddr;

    use metrics_exporter_prometheus::PrometheusBuilder;

    let addr: SocketAddr = match std::env::var("H2PROXYD_METRICS")
        .unwrap_or_else(|_| DEFAULT_METRICS.to_string())
        .parse()
    {
        Ok(addr) => addr,
        Err(e) => {
            warn!(error = %e, "invalid H2PROXYD_METRICS; skipping metrics exporter");
            return;
        }
    };

    if let Err(e) = PrometheusBuilder::new().with_http_listener(addr).install() {
        warn!(error = %e, %addr, "could not start metrics exporter; continuing without it");
        return;
    }

    metrics::describe_gauge!("h2proxy_active_streams", "Currently open client streams");
    metrics::describe_counter!("h2proxy_requests_total", "Client requests proxied");
    metrics::describe_gauge!(
        "h2proxy_upstream_pool_connections",
        "Warm upstream connections in the pool"
    );
    metrics::describe_gauge!(
        "h2proxy_upstream_streams_active",
        "Streams currently in flight to backends"
    );
    // The bounded-memory claim, as a number you can watch: response octets held
    // between a backend and a client because the client has not taken them yet.
    // Under a fast-upstream/slow-client mismatch this stays flat at roughly one
    // connection window per upstream connection instead of tracking the
    // response size (§4.2, ADR 0016).
    metrics::describe_gauge!(
        "h2proxy_bridge_buffered_bytes",
        "Response octets received from backends but not yet delivered to clients"
    );
    metrics::describe_gauge!(
        "h2proxy_bridge_buffered_bytes_peak",
        "High-water mark of h2proxy_bridge_buffered_bytes"
    );
    metrics::describe_gauge!(
        "h2proxy_upstream_connects_total",
        "Upstream connections attempted"
    );
    metrics::describe_gauge!(
        "h2proxy_upstream_connect_failures_total",
        "Upstream connections that failed to establish"
    );
    metrics::describe_counter!(
        "h2proxy_handshakes_total",
        "Connections that completed the preface + SETTINGS exchange"
    );
    metrics::describe_counter!(
        "h2proxy_frames_received_total",
        "HTTP/2 frames decoded from clients"
    );
    metrics::describe_counter!(
        "h2proxy_header_blocks_decoded_total",
        "Complete HPACK header blocks decoded from clients"
    );
    metrics::describe_counter!("h2proxy_streams_opened_total", "Client streams opened");
    metrics::describe_counter!(
        "h2proxy_streams_reset_total",
        "Streams aborted with RST_STREAM, in either direction"
    );
    metrics::describe_gauge!(
        "h2proxy_stream_concurrency_max",
        "Most streams live at once on a single connection"
    );
    metrics::describe_counter!(
        "h2proxy_data_bytes_sent_total",
        "DATA payload octets written to clients"
    );
    // The interesting one: a nonzero stall count under load is the observable
    // proof that flow control is doing something rather than being nominally
    // present.
    metrics::describe_counter!(
        "h2proxy_flow_control_stalls_total",
        "Times the outbound scheduler had octets queued but no window to send them in"
    );
    // Seed each series at zero so a scrape returns them before any traffic.
    metrics::gauge!("h2proxy_active_streams").set(0.0);
    metrics::counter!("h2proxy_requests_total").increment(0);
    metrics::gauge!("h2proxy_upstream_pool_connections").set(0.0);
    metrics::gauge!("h2proxy_upstream_streams_active").set(0.0);
    metrics::gauge!("h2proxy_bridge_buffered_bytes").set(0.0);
    metrics::gauge!("h2proxy_bridge_buffered_bytes_peak").set(0.0);
    metrics::gauge!("h2proxy_upstream_connects_total").set(0.0);
    metrics::gauge!("h2proxy_upstream_connect_failures_total").set(0.0);
    metrics::counter!("h2proxy_handshakes_total").increment(0);
    metrics::counter!("h2proxy_frames_received_total").increment(0);
    metrics::counter!("h2proxy_header_blocks_decoded_total").increment(0);
    metrics::counter!("h2proxy_streams_opened_total").increment(0);
    metrics::counter!("h2proxy_streams_reset_total").increment(0);
    metrics::gauge!("h2proxy_stream_concurrency_max").set(0.0);
    metrics::counter!("h2proxy_data_bytes_sent_total").increment(0);
    metrics::counter!("h2proxy_flow_control_stalls_total").increment(0);

    info!(%addr, "metrics exporter listening at /metrics");
}

/// Initialize `tracing` with an env-filter, defaulting to info-level for the
/// daemon. Override with `RUST_LOG` (e.g. `RUST_LOG=h2proxyd=debug`).
fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("h2proxyd=info,warn"));
    fmt().with_env_filter(filter).init();
}
