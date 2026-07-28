//! h2proxyd — the HTTP/2 multiplexing reverse-proxy daemon.
//!
//! The binary owns everything the protocol engine (`h2proxy-core`) must not:
//! sockets, TLS (rustls, ALPN `h2`), configuration, signals, and logging.
//! Errors here are `anyhow` — "log and exit", never "pick a frame to send"
//! (ADR 0008).
//!
//! Current milestone (week 2): terminate TLS 1.3, negotiate ALPN to `h2`, and
//! hand the byte stream to the protocol engine's connection skeleton
//! ([`h2proxy_core::conn::Connection`]). The engine reads the connection and
//! drains cleanly on shutdown or EOF; frame parsing plugs into its reader loop
//! in week 5. The accept loop and graceful-shutdown drain (the §2.2 skeleton)
//! give that engine a stable lifecycle to slot into.

mod tls;

use std::net::SocketAddr;

use anyhow::Context;
use h2proxy_core::conn::Connection;
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
                conns.spawn(handle_connection(acceptor, stream, peer, shutdown));
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
/// hand the byte stream to the protocol engine ([`Connection`]). Week 2 wires
/// the lifecycle; the engine's frame handling lands in week 5.
async fn handle_connection(
    acceptor: TlsAcceptor,
    stream: TcpStream,
    peer: SocketAddr,
    mut shutdown: broadcast::Receiver<()>,
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

    // Hand off to the protocol engine: it runs the preface + SETTINGS handshake
    // and the frame loop. Per-stream dispatch (and therefore responses) lands in
    // week 5, so a request currently establishes and then waits.
    let summary = Connection::new(tls_stream, shutdown).run().await;

    // The engine reports; the binary instruments (so `h2proxy-core` stays free
    // of the metrics dependency).
    if summary.handshake_completed {
        metrics::counter!("h2proxy_handshakes_total").increment(1);
    }
    metrics::counter!("h2proxy_frames_received_total").increment(summary.frames_received);
    info!(
        %peer,
        handshake = summary.handshake_completed,
        frames = summary.frames_received,
        "connection closed",
    );
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
    metrics::describe_counter!(
        "h2proxy_handshakes_total",
        "Connections that completed the preface + SETTINGS exchange"
    );
    metrics::describe_counter!(
        "h2proxy_frames_received_total",
        "HTTP/2 frames decoded from clients"
    );
    // Seed each series at zero so a scrape returns them before any traffic.
    metrics::gauge!("h2proxy_active_streams").set(0.0);
    metrics::counter!("h2proxy_requests_total").increment(0);
    metrics::gauge!("h2proxy_upstream_pool_connections").set(0.0);
    metrics::counter!("h2proxy_handshakes_total").increment(0);
    metrics::counter!("h2proxy_frames_received_total").increment(0);

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
