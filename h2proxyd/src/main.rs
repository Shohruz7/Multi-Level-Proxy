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
use h2proxy_core::conn::{Connection, DrainPolicy, Settings};
use h2proxy_core::guard::Limits;
use h2proxy_core::health;
use h2proxy_core::lb::Backend;
use h2proxy_core::proxy::{Proxy, Shared};
use h2proxy_core::service::Echo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::Instant;
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
        Some(Shared::with_policy(
            backends,
            max_conns_per_backend(),
            health_policy(),
        ))
    };
    if let Some(shared) = &proxy {
        spawn_stats_sampler(shared);
    }
    let drain = drain_policy();
    let limits = guard_limits();
    if limits.observe_only {
        info!("abuse guard in OBSERVE-ONLY mode: peaks recorded, nothing enforced");
    }
    info!(
        grace_s = drain.grace.as_secs_f64(),
        deadline_s = drain.deadline.as_secs_f64(),
        "drain policy",
    );

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
                conns.spawn(handle_connection(
                    acceptor, stream, peer, shutdown, proxy, drain, limits,
                ));
            }
        }
    }

    // Tell in-flight tasks to stop, then wait for the drain to finish. Each
    // connection sends its advisory GOAWAY, keeps serving for the grace, commits
    // a final GOAWAY, and closes when its last stream ends (§6.8).
    let started = std::time::Instant::now();
    drop(shutdown_tx);
    while conns.join_next().await.is_some() {}
    info!(seconds = started.elapsed().as_secs_f64(), "clients drained");

    // Now the upstream half. The pool owns the sender for every upstream
    // connection's inbox, so those tasks cannot notice they are finished while
    // anything still holds `Shared` — dropping it here is what closes their
    // inboxes and lets them wind down. The sampler holds only a `Weak`, so it is
    // never the thing keeping them alive.
    let upstreams = proxy.as_ref().map(|shared| Arc::clone(&shared.stats));
    drop(proxy);
    if let Some(stats) = upstreams {
        let deadline = std::time::Instant::now() + UPSTREAM_DRAIN_DEADLINE;
        while stats.upstream_connections() > 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let left = stats.upstream_connections();
        if left > 0 {
            warn!(
                connections = left,
                "upstream connections still open at exit"
            );
        }
    }
    info!(
        seconds = started.elapsed().as_secs_f64(),
        "all connections drained; exiting",
    );
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
    drain: DrainPolicy,
    limits: Limits,
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
            let service = Proxy::new(shared)
                .with_peer(peer.ip())
                .trusting_forwarded_headers(trust_forwarded());
            Connection::with_service(tls_stream, shutdown, settings, service)
                .with_drain_policy(drain)
                .with_limits(limits)
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
            .with_drain_policy(drain)
            .with_limits(limits)
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
fn spawn_stats_sampler(shared: &Arc<Shared>) {
    // A `Weak`, deliberately. A sampler holding a strong reference would keep
    // the pool — and therefore every upstream connection's inbox sender — alive
    // for the life of the process, so the upstream tasks could never observe
    // that their last client had gone and the drain would always hit its
    // deadline instead of finishing.
    let shared = Arc::downgrade(shared);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let Some(shared) = shared.upgrade() else {
                return;
            };
            let stats = &shared.stats;

            // Gauges: quantities that go up and down.
            metrics::gauge!("h2proxy_upstream_pool_connections")
                .set(stats.upstream_connections() as f64);
            metrics::gauge!("h2proxy_upstream_streams_active").set(stats.upstream_streams() as f64);
            metrics::gauge!("h2proxy_client_streams_active").set(stats.client_streams() as f64);
            metrics::gauge!("h2proxy_bridge_buffered_bytes").set(stats.buffered() as f64);
            metrics::gauge!("h2proxy_bridge_buffered_bytes_peak").set(stats.peak_buffered() as f64);
            metrics::gauge!("h2proxy_backends_healthy").set(
                shared
                    .health
                    .healthy_count(&shared.backends, Instant::now()) as f64,
            );

            // Counters: monotonic totals. `absolute` rather than `increment`
            // because the engine owns the running value — this republishes it
            // rather than trying to track deltas, which would drift on every
            // missed tick. These were `gauge!` before, which made a `_total`
            // series a gauge and quietly broke `rate()` for anyone scraping it.
            metrics::counter!("h2proxy_upstream_connects_total").absolute(stats.connects());
            metrics::counter!("h2proxy_upstream_connect_failures_total")
                .absolute(stats.connect_failures());
            metrics::counter!("h2proxy_upstream_requests_total").absolute(stats.requests());
            metrics::counter!("h2proxy_upstream_retries_total").absolute(stats.retries());
            metrics::counter!("h2proxy_backend_ejections_total")
                .absolute(shared.health.ejections());
            for class in 1..=5u16 {
                metrics::counter!("h2proxy_responses_total", "class" => format!("{class}xx"))
                    .absolute(stats.responses(class as usize));
            }

            // The RED histogram. `metrics` has no "publish precomputed buckets"
            // API, so the buckets are emitted as the counters Prometheus expects
            // a histogram to be made of — `_bucket{le=…}`, `_sum`, `_count`.
            // That keeps the `metrics` dependency in the binary, where it has
            // always been, instead of leaking it into the engine for the sake of
            // one histogram.
            let buckets = stats.latency_buckets();
            for (count, bound) in buckets.iter().zip(h2proxy_core::proxy::LATENCY_BUCKETS) {
                let le = if bound.is_infinite() {
                    "+Inf".to_string()
                } else {
                    bound.to_string()
                };
                metrics::counter!("h2proxy_request_duration_seconds_bucket", "le" => le)
                    .absolute(*count);
            }
            metrics::counter!("h2proxy_request_duration_seconds_count")
                .absolute(stats.latency_count());
            metrics::gauge!("h2proxy_request_duration_seconds_sum")
                .set(stats.latency_sum_seconds());
        }
    });
}

/// How the connections wind down on SIGTERM, from `H2PROXYD_DRAIN_GRACE` and
/// `H2PROXYD_DRAIN_DEADLINE` (both in seconds).
///
/// Configurable because the right deadline is a property of the *deployment*,
/// not of the proxy: it has to stay under whatever the container runtime will
/// wait before SIGKILL, and that number belongs to whoever wrote the manifest.
fn drain_policy() -> DrainPolicy {
    let seconds = |name: &str, fallback: Duration| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .map_or(fallback, Duration::from_secs_f64)
    };
    let default = DrainPolicy::default();
    DrainPolicy {
        grace: seconds("H2PROXYD_DRAIN_GRACE", default.grace),
        deadline: seconds("H2PROXYD_DRAIN_DEADLINE", default.deadline),
    }
}

/// How long to wait for upstream connections to finish after the clients have
/// gone. Sized to sit inside the same container-stop budget as the client drain.
const UPSTREAM_DRAIN_DEADLINE: Duration = Duration::from_secs(15);

/// Read a numeric override, keeping the default when it is missing or unusable.
///
/// Deliberately lenient: a typo in an environment variable must not stop a proxy
/// from starting, because the failure mode of "refuses to boot on a bad env var"
/// is an outage during a deploy, while the failure mode of "logs and uses the
/// default" is a warning nobody acted on.
fn env_num<T: std::str::FromStr + PartialOrd + Copy>(name: &str, fallback: T) -> T {
    match std::env::var(name).ok().map(|v| v.parse::<T>()) {
        None => fallback,
        Some(Ok(value)) => value,
        Some(Err(_)) => {
            warn!(var = name, "unparseable value; using the default");
            fallback
        }
    }
}

fn env_secs(name: &str, fallback: Duration) -> Duration {
    let seconds = env_num(name, fallback.as_secs_f64());
    if seconds.is_finite() && seconds >= 0.0 {
        Duration::from_secs_f64(seconds)
    } else {
        fallback
    }
}

/// The abuse-guard thresholds (design doc §6). Every one is overridable, because
/// the right threshold is a property of the traffic and the traffic is not known
/// until it is measured — see `just calibrate`.
fn guard_limits() -> Limits {
    let d = Limits::default();
    Limits {
        reset_burst: env_num("H2PROXYD_RESET_BURST", d.reset_burst),
        reset_rate: env_num("H2PROXYD_RESET_RATE", d.reset_rate),
        unanswered_burst: env_num("H2PROXYD_UNANSWERED_BURST", d.unanswered_burst),
        unanswered_rate: env_num("H2PROXYD_UNANSWERED_RATE", d.unanswered_rate),
        control_burst: env_num("H2PROXYD_CONTROL_BURST", d.control_burst),
        control_rate: env_num("H2PROXYD_CONTROL_RATE", d.control_rate),
        max_consecutive_empty: env_num("H2PROXYD_MAX_EMPTY_FRAMES", d.max_consecutive_empty),
        max_continuations: env_num("H2PROXYD_MAX_CONTINUATIONS", d.max_continuations),
        // The calibration run sets this; so does a cautious first rollout.
        observe_only: std::env::var("H2PROXYD_GUARD_OBSERVE_ONLY").is_ok_and(|v| v != "0"),
    }
}

/// Health-checking and ejection policy (design doc §5.2).
fn health_policy() -> health::Policy {
    let d = health::Policy::default();
    health::Policy {
        eject_after: env_num("H2PROXYD_EJECT_AFTER", d.eject_after),
        base_backoff: env_secs("H2PROXYD_EJECT_BACKOFF", d.base_backoff),
        max_backoff: env_secs("H2PROXYD_EJECT_MAX_BACKOFF", d.max_backoff),
        ping_idle: env_secs("H2PROXYD_PING_IDLE", d.ping_idle),
        ping_timeout: env_secs("H2PROXYD_PING_TIMEOUT", d.ping_timeout),
        idle_timeout: env_secs("H2PROXYD_UPSTREAM_IDLE", d.idle_timeout),
    }
}

/// Whether to append to a client-supplied `x-forwarded-for` rather than replace
/// it. Off unless something trustworthy runs in front — see
/// [`Proxy::trusting_forwarded_headers`].
fn trust_forwarded() -> bool {
    std::env::var("H2PROXYD_TRUST_FORWARDED").is_ok_and(|v| v != "0")
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
    metrics::describe_counter!(
        "h2proxy_upstream_connects_total",
        "Upstream connections attempted"
    );
    metrics::describe_counter!(
        "h2proxy_upstream_connect_failures_total",
        "Upstream connections that failed to establish"
    );
    metrics::describe_gauge!(
        "h2proxy_client_streams_active",
        "Client streams currently being proxied (zero in built-in-responder mode)"
    );
    metrics::describe_counter!(
        "h2proxy_upstream_requests_total",
        "Requests actually forwarded to a backend; the gap from \
         h2proxy_requests_total is what the proxy answered itself (502/503)"
    );
    metrics::describe_counter!(
        "h2proxy_upstream_retries_total",
        "Second attempts made after a retryable upstream failure"
    );
    metrics::describe_counter!(
        "h2proxy_backend_ejections_total",
        "Times a backend was removed from rotation by health checking"
    );
    metrics::describe_gauge!("h2proxy_backends_healthy", "Backends currently in rotation");
    metrics::describe_counter!(
        "h2proxy_responses_total",
        "Responses to clients, by status class"
    );
    // The D and E of RED. Emitted as the counters Prometheus expects a histogram
    // to be made of, because the engine holds the buckets and owns no metrics
    // dependency (design doc §7).
    metrics::describe_counter!(
        "h2proxy_request_duration_seconds_bucket",
        "Cumulative count of proxied requests completing within each latency bound"
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
    metrics::counter!("h2proxy_requests_total").increment(0);
    metrics::gauge!("h2proxy_upstream_pool_connections").set(0.0);
    metrics::gauge!("h2proxy_upstream_streams_active").set(0.0);
    metrics::gauge!("h2proxy_bridge_buffered_bytes").set(0.0);
    metrics::gauge!("h2proxy_bridge_buffered_bytes_peak").set(0.0);
    metrics::counter!("h2proxy_upstream_connects_total").absolute(0);
    metrics::counter!("h2proxy_upstream_connect_failures_total").absolute(0);
    metrics::gauge!("h2proxy_client_streams_active").set(0.0);
    metrics::counter!("h2proxy_upstream_requests_total").absolute(0);
    metrics::counter!("h2proxy_upstream_retries_total").absolute(0);
    metrics::counter!("h2proxy_backend_ejections_total").absolute(0);
    metrics::gauge!("h2proxy_backends_healthy").set(0.0);
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
