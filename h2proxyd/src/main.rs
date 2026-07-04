//! h2proxyd — the HTTP/2 multiplexing reverse-proxy daemon.
//!
//! The binary owns everything the protocol engine (`h2proxy-core`) must not:
//! sockets, TLS (rustls, ALPN `h2`), configuration, signals, and logging.
//! Errors here are `anyhow` — "log and exit", never "pick a frame to send"
//! (ADR 0008).
//!
//! Current milestone (week 1): terminate TLS 1.3, negotiate ALPN to `h2`, log
//! the result, and drop the connection. No frame parsing yet — the HTTP/2
//! engine plugs into [`handle_connection`] from week 3. The accept loop and
//! graceful-shutdown drain (week 2's §2.2 skeleton) are already here so the
//! engine has a stable lifecycle to slot into.

mod tls;

use std::net::SocketAddr;

use anyhow::Context;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

/// Listen address; override with the `H2PROXYD_LISTEN` environment variable.
const DEFAULT_LISTEN: &str = "127.0.0.1:8443";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

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
                let mut shutdown = shutdown_tx.subscribe();
                conns.spawn(async move {
                    tokio::select! {
                        _ = handle_connection(acceptor, stream, peer) => {}
                        // Closed channel (Err) or a value both mean "shut down".
                        _ = shutdown.recv() => {}
                    }
                });
            }
        }
    }

    // Tell in-flight tasks to stop, then wait for the drain to finish.
    drop(shutdown_tx);
    while conns.join_next().await.is_some() {}
    info!("all connections drained; exiting");
    Ok(())
}

/// Terminate TLS for one accepted connection and report the negotiated ALPN
/// protocol. Week 1 stops after the handshake; the frame layer takes over here
/// once [`h2proxy_core`] lands.
async fn handle_connection(acceptor: TlsAcceptor, stream: TcpStream, peer: SocketAddr) {
    let tls_stream = match acceptor.accept(stream).await {
        Ok(s) => s,
        Err(e) => {
            warn!(%peer, error = %e, "TLS handshake failed");
            return;
        }
    };

    let (_io, conn) = tls_stream.get_ref();
    match conn.alpn_protocol() {
        Some(tls::ALPN_H2) => {
            info!(%peer, alpn = "h2", "TLS 1.3 handshake complete; ALPN negotiated h2")
        }
        Some(other) => warn!(
            %peer,
            alpn = %String::from_utf8_lossy(other),
            "handshake complete but peer did not select h2",
        ),
        None => warn!(%peer, "handshake complete but no ALPN protocol was negotiated"),
    }

    // No HTTP/2 frame handling yet (week 3). Dropping `tls_stream` closes the
    // connection cleanly.
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

/// Initialize `tracing` with an env-filter, defaulting to info-level for the
/// daemon. Override with `RUST_LOG` (e.g. `RUST_LOG=h2proxyd=debug`).
fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("h2proxyd=info,warn"));
    fmt().with_env_filter(filter).init();
}
