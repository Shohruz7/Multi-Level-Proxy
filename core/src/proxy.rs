//! The proxy request path: demux → route → balance → upstream → remux
//! (design doc §2.1, §4).
//!
//! Ties the engine together. A client connection hands it a validated request
//! ([`crate::service::Service::dispatch`]); it picks a backend with
//! [`crate::lb`], leases a slot with [`crate::pool`], and forwards the head,
//! body, and trailers to the [`crate::upstream`] connection task holding that
//! lease. Everything coming back is already addressed in the client's stream-id
//! space, so it goes straight into the client connection's event channel
//! untouched.
//!
//! This module is deliberately thin: **it holds no protocol state.** No windows,
//! no HPACK tables, no stream machine — those belong to the two connection
//! engines, which is why the bridge works out to a few messages rather than a
//! coordination layer. What lives here is one map from client stream to lease,
//! and the header sanitation that has to happen exactly once.
//!
//! # Its half of the bridge (§4.2)
//!
//! Two forwards and nothing else:
//!
//! - [`Service::released`] — the client took `n` response octets → tell the
//!   upstream, which turns it into the WINDOW_UPDATE it was withholding.
//! - [`crate::service::ServiceEvent::BodyAccepted`] — comes back from the
//!   upstream when it has spent its send window, and releases the client's
//!   receive window.
//!
//! Neither side ever waits on the other. Withheld credit, not a blocked task, is
//! what makes memory bounded (ADR 0016).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tracing::debug;

use crate::conn::ErrorCode;
use crate::health::{self, Health};
use crate::hpack::Header;
use crate::lb::{Backend, BackendLoad, LoadBalancer, PowerOfTwoChoices};
use crate::pool::{Lease, Pool};
use crate::service::{Events, RequestHead, Response, Service, ServiceEvent};
use crate::stream::StreamId;
use crate::upstream::ToUpstream;
use tokio::time::Instant;

/// Live counters the daemon samples for its gauges (design doc §7).
///
/// Sampled rather than reported at close, because upstream connections outlive
/// individual requests by design — a summary at teardown would show nothing
/// until the pool churned. The engine still owns no `metrics` dependency: these
/// are plain atomics the binary reads.
#[derive(Debug, Default)]
pub struct ProxyStats {
    upstream_connections: AtomicUsize,
    upstream_streams: AtomicUsize,
    connects: AtomicU64,
    connect_failures: AtomicU64,
    requests: AtomicU64,
    /// Response octets received from backends but not yet confirmed delivered to
    /// clients: what the bridge is holding right now.
    buffered: AtomicUsize,
    /// The high-water mark of the above. **This is the bounded-memory claim as a
    /// number** — the backpressure test asserts against it, and week 8's load
    /// run should show it flat while throughput climbs.
    peak_buffered: AtomicUsize,
    /// Client streams currently being proxied. Replaces the week-2
    /// `h2proxy_active_streams` gauge, which was described and seeded and never
    /// once written to.
    client_streams: AtomicUsize,
    /// Second attempts made after a retryable failure.
    retries: AtomicU64,
    /// Responses by status class, indexed `status / 100 - 1`. The "E" of RED.
    responses: [AtomicU64; 5],
    /// Request latency, as fixed histogram buckets. The "D" of RED.
    ///
    /// Buckets rather than observations because the engine owns no `metrics`
    /// dependency and never will: the daemon republishes these counts as a
    /// Prometheus histogram. Cumulative (each bucket counts everything at or
    /// below its bound), which is the Prometheus convention and saves the
    /// exporter a pass.
    latency: [AtomicU64; LATENCY_BUCKETS.len()],
    latency_count: AtomicU64,
    latency_sum_micros: AtomicU64,
}

/// Upper bounds, in seconds, for the request-latency histogram.
///
/// Sized for the claim being made: the target is a sub-3 ms p99, so the
/// interesting region is 0.1–5 ms and it gets seven of the fourteen buckets. A
/// default exponential ladder would put 3 ms between the 1 ms and 10 ms bounds
/// and make the headline number unresolvable.
pub const LATENCY_BUCKETS: [f64; 14] = [
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.002,
    0.003,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    1.0,
    f64::INFINITY,
];

impl ProxyStats {
    pub fn connect_attempt(&self) {
        self.connects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connect_failure(&self) {
        self.connect_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn open_connection(&self) {
        self.upstream_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn close_connection(&self) {
        // Saturating rather than wrapping: a connect that failed never opened,
        // so the close on its way out must not take the gauge negative.
        let _ = self
            .upstream_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }

    pub fn open_stream(&self) {
        self.upstream_streams.fetch_add(1, Ordering::Relaxed);
    }

    pub fn close_stream(&self) {
        let _ = self
            .upstream_streams
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }

    pub fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    /// `n` octets arrived from a backend and are now the bridge's to hold.
    pub fn buffer(&self, n: u32) {
        let now = self.buffered.fetch_add(n as usize, Ordering::Relaxed) + n as usize;
        let _ = self
            .peak_buffered
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |peak| {
                (now > peak).then_some(now)
            });
    }

    /// `n` octets reached a client (or died with their stream).
    pub fn unbuffer(&self, n: u32) {
        let _ = self
            .buffered
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                Some(held.saturating_sub(n as usize))
            });
    }

    pub fn upstream_connections(&self) -> usize {
        self.upstream_connections.load(Ordering::Relaxed)
    }

    pub fn upstream_streams(&self) -> usize {
        self.upstream_streams.load(Ordering::Relaxed)
    }

    pub fn connects(&self) -> u64 {
        self.connects.load(Ordering::Relaxed)
    }

    pub fn connect_failures(&self) -> u64 {
        self.connect_failures.load(Ordering::Relaxed)
    }

    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn buffered(&self) -> usize {
        self.buffered.load(Ordering::Relaxed)
    }

    pub fn peak_buffered(&self) -> usize {
        self.peak_buffered.load(Ordering::Relaxed)
    }

    pub fn open_client_stream(&self) {
        self.client_streams.fetch_add(1, Ordering::Relaxed);
    }

    pub fn close_client_stream(&self) {
        let _ = self
            .client_streams
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }

    pub fn client_streams(&self) -> usize {
        self.client_streams.load(Ordering::Relaxed)
    }

    pub fn retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    /// A response head went to a client with this status.
    pub fn response(&self, status: u16) {
        let class = (status / 100).clamp(1, 5) as usize - 1;
        self.responses[class].fetch_add(1, Ordering::Relaxed);
    }

    /// Responses in class `n` (1 = 1xx … 5 = 5xx).
    pub fn responses(&self, class: usize) -> u64 {
        self.responses
            .get(class.saturating_sub(1))
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Record one completed request's latency.
    ///
    /// Cumulative buckets, so a scrape is a read rather than a prefix sum. The
    /// loop is over fourteen `f64` comparisons and one `fetch_add` each — a few
    /// nanoseconds against a request that took at least microseconds.
    pub fn observe_latency(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        for (bucket, bound) in self.latency.iter().zip(LATENCY_BUCKETS) {
            if seconds <= bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        self.latency_sum_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    /// The cumulative bucket counts, aligned with [`LATENCY_BUCKETS`].
    pub fn latency_buckets(&self) -> [u64; LATENCY_BUCKETS.len()] {
        std::array::from_fn(|i| self.latency[i].load(Ordering::Relaxed))
    }

    pub fn latency_count(&self) -> u64 {
        self.latency_count.load(Ordering::Relaxed)
    }

    pub fn latency_sum_seconds(&self) -> f64 {
        self.latency_sum_micros.load(Ordering::Relaxed) as f64 / 1e6
    }
}

/// Everything the proxy shares between client connections: the pool, the load
/// balancer, the backend list, and the counters.
#[derive(Debug)]
pub struct Shared {
    pub pool: Pool,
    pub backends: Vec<Backend>,
    pub balancer: Box<dyn LoadBalancer>,
    pub stats: Arc<ProxyStats>,
    /// Which backends are worth sending to (design doc §5.2). Filters the
    /// candidate list before the balancer ever sees it, so health and balancing
    /// stay separate concerns: this decides *eligibility*, `lb` decides *which*.
    pub health: Health,
}

impl Shared {
    /// Build the shared half of the proxy for `backends`.
    pub fn new(backends: Vec<Backend>, max_conns_per_backend: usize) -> Arc<Self> {
        Self::with_policy(backends, max_conns_per_backend, health::Policy::default())
    }

    /// Build with an explicit health policy. The daemon passes its configured
    /// one; tests that are not about health pass
    /// [`health::Policy::permissive`].
    pub fn with_policy(
        backends: Vec<Backend>,
        max_conns_per_backend: usize,
        policy: health::Policy,
    ) -> Arc<Self> {
        let stats = Arc::new(ProxyStats::default());
        Arc::new(Shared {
            pool: Pool::new(Arc::clone(&stats), max_conns_per_backend),
            backends,
            balancer: Box::new(PowerOfTwoChoices::new()),
            stats,
            health: Health::new(policy),
        })
    }

    /// Pick a backend to try, excluding any already attempted for this request.
    ///
    /// Health first, then balance. Filtering before the balancer rather than
    /// inside it keeps `PowerOfTwoChoices` a pure function of its input, which
    /// is what makes its distribution testable without a pool or a socket.
    fn pick(&self, exclude: Option<Backend>, now: Instant) -> Option<Backend> {
        let load = self.pool.load(&self.backends);
        let mut eligible = self.health.eligible(&load, now);
        if let Some(failed) = exclude {
            // A retry must land somewhere else, or it is just the same request
            // to the same broken backend a moment later. If that leaves nothing,
            // fall back to the full list — one more attempt at a backend that
            // might have been unlucky beats no attempt at all.
            let elsewhere: Vec<BackendLoad> = eligible
                .iter()
                .copied()
                .filter(|c| c.backend != failed)
                .collect();
            if !elsewhere.is_empty() {
                eligible = elsewhere;
            }
        }
        let chosen = self.balancer.pick(&eligible)?;
        // Claim the probe slot only now that a backend has really been chosen.
        // A no-op unless `chosen` is half-open.
        self.health.claim_trial(&chosen, now);
        Some(chosen)
    }
}

/// Methods safe to send twice (RFC 9110 §9.2.2).
///
/// `POST` and `PATCH` are absent and must stay absent: a retried POST can charge
/// a card twice, and no amount of "it probably did not arrive" makes that
/// acceptable.
const IDEMPOTENT: [&[u8]; 6] = [b"GET", b"HEAD", b"OPTIONS", b"TRACE", b"DELETE", b"PUT"];

/// One retry, never more.
///
/// A single attempt with a different-backend constraint bounds amplification at
/// 2x and needs no jitter, no budget, and no second tuning knob. A retry budget
/// as a fraction of request rate is the right answer at a scale where one bad
/// backend can double the load on the others; this project is not at that scale,
/// and an unbounded-in-practice retry policy is a classic way to turn a partial
/// failure into a self-inflicted outage.
const MAX_ATTEMPTS: u8 = 2;

/// One attempt at forwarding a request, as `attempt` needs it.
///
/// A struct rather than seven positional parameters, because the two call sites
/// differ in exactly the fields that are easiest to transpose.
struct Attempt {
    head: Box<RequestHead>,
    end_stream: bool,
    /// The copy kept for a possible retry; `None` makes this attempt final.
    spare: Option<Box<RequestHead>>,
    /// A backend not to choose — the one that just failed.
    exclude: Option<Backend>,
    attempts: u8,
    /// The original stream clock and span, when this is a retry.
    carry: Option<(Instant, tracing::Span)>,
}

/// One client stream's upstream leg.
#[derive(Debug)]
struct Route {
    lease: Lease,
    /// Whether the request has ended, so a late body chunk is dropped rather
    /// than sent on a half-closed stream.
    request_done: bool,
    /// Which backend this attempt went to, so a retry can avoid it and so the
    /// outcome can be reported to the right health entry.
    backend: Backend,
    /// The request head, kept **only** when this request could be retried.
    ///
    /// `None` for anything with a body, which is the whole memory argument in
    /// one field: a retryable request is by definition one whose HEADERS carried
    /// END_STREAM, so there is nothing to replay but the head itself. Buffering
    /// bodies to make more requests retryable would reintroduce exactly the
    /// unbounded per-stream buffer that ADR 0016 exists to avoid.
    head: Option<Box<RequestHead>>,
    /// Attempts made so far, including the one in flight.
    attempts: u8,
    /// Whether a `:status` has reached the client. Once it has, no failure can
    /// be replaced by a retry — the response has already begun.
    response_started: bool,
    /// When the client's request arrived, for the RED latency histogram.
    /// Measured from dispatch to the stream ending, so a retry is inside the
    /// number rather than hidden by it — which is the honest way to report it.
    started: Instant,
    /// The tracing span covering this stream's whole journey: demux → LB →
    /// upstream → remux. Debug level, so at the default filter it is never
    /// entered and costs a branch.
    span: tracing::Span,
}

/// The request path, as one client connection sees it (design doc §2.1).
///
/// Cheap to build — a shared pointer and an empty map — because the connection
/// layer makes one per client connection. Everything expensive is in [`Shared`].
#[derive(Debug)]
pub struct Proxy {
    shared: Arc<Shared>,
    events: Option<Events>,
    routes: HashMap<StreamId, Route>,
    /// The client's address, for `x-forwarded-for`. `None` when the caller has
    /// no socket to name — the differential tests run the engine over a duplex
    /// pipe, and inventing an address there would be a lie in a header.
    peer: Option<std::net::IpAddr>,
    /// Whether to believe a client-supplied `x-forwarded-for` and append to it.
    trust_forwarded: bool,
}

impl Proxy {
    pub fn new(shared: Arc<Shared>) -> Self {
        Proxy {
            shared,
            events: None,
            routes: HashMap::new(),
            peer: None,
            trust_forwarded: false,
        }
    }

    /// Name the client, so requests carry `x-forwarded-for`.
    ///
    /// Per client connection, which is why this lives on `Proxy` and not in the
    /// engine: `conn.rs` needs no knowledge of sockets for any of this.
    pub fn with_peer(mut self, peer: std::net::IpAddr) -> Self {
        self.peer = Some(peer);
        self
    }

    /// Append to a client-supplied `x-forwarded-for` instead of replacing it.
    ///
    /// **Off by default, and that is the security-relevant choice.** Appending
    /// means any client can prepend whatever addresses it likes and the backend
    /// cannot tell which entry we observed and which the client invented — so an
    /// allowlist or rate limit keyed on the first entry is trivially forged. The
    /// proxy sits directly behind an NLB (ADR 0005), which adds no such header,
    /// so the peer address *is* the client and overwriting is both correct and
    /// safe. Turn this on only when something trustworthy runs in front.
    pub fn trusting_forwarded_headers(mut self, trust: bool) -> Self {
        self.trust_forwarded = trust;
        self
    }

    fn emit(&self, event: ServiceEvent) {
        if let Some(events) = &self.events {
            let _ = events.send(event);
        }
    }

    /// Answer a request ourselves, without a backend.
    ///
    /// The rule, applied consistently across the proxy: **503 means we had no
    /// backend to try** (none configured, none eligible, no slot), **502 means we
    /// tried one and it let us down** (connect failed, connection died,
    /// malformed response). The distinction is the client's: 503 is worth
    /// retrying in a moment, 502 is not.
    fn answer_locally(&self, id: StreamId, status: u16) {
        self.emit(ServiceEvent::Head {
            id,
            response: Response::status(status),
            end_stream: true,
        });
    }

    /// Strip anything that describes the client hop rather than the request.
    ///
    /// §8.2.2's connection-specific fields are already rejected by
    /// [`RequestHead::from_headers`], so what is left is `te`, which HTTP/2
    /// permits only as `trailers` and which means nothing to a backend we speak
    /// h2 to. The `:authority` is deliberately **kept**: a reverse proxy that
    /// rewrites it silently changes which virtual host the backend serves.
    fn sanitize(head: &mut RequestHead) {
        head.fields.retain(|field| field.name.as_ref() != b"te");
    }

    /// Tell the backend who the client is (`x-forwarded-for`) and how it reached
    /// us (`x-forwarded-proto`).
    ///
    /// RFC 7239's `Forwarded` is the standardised header and is deliberately not
    /// emitted: almost nothing reads it, and sending both means two sources of
    /// truth that can disagree. `x-forwarded-for` is what backends actually
    /// parse.
    fn add_forwarded(&self, head: &mut RequestHead) {
        let Some(peer) = self.peer else {
            return;
        };
        let existing = self
            .trust_forwarded
            .then(|| {
                head.fields
                    .iter()
                    .find(|f| f.name.as_ref() == b"x-forwarded-for")
                    .map(|f| f.value.clone())
            })
            .flatten();

        head.fields.retain(|f| {
            f.name.as_ref() != b"x-forwarded-for" && f.name.as_ref() != b"x-forwarded-proto"
        });

        let value = match existing {
            Some(chain) => format!("{}, {peer}", String::from_utf8_lossy(&chain)),
            None => peer.to_string(),
        };
        head.fields.push(Header::new(
            Bytes::from_static(b"x-forwarded-for"),
            Bytes::from(value.into_bytes()),
        ));
        head.fields.push(Header::new(
            Bytes::from_static(b"x-forwarded-proto"),
            // The client leg is TLS by construction — the daemon terminates it
            // and only hands us an h2-over-TLS stream (ADR 0005, 0017).
            Bytes::from_static(b"https"),
        ));
    }

    /// Send one attempt of a request to a backend.
    ///
    /// Shared by the first dispatch and the retry, because they differ only in
    /// what they carry forward — and a retry path that does not go through the
    /// same code as the original is a retry path that drifts.
    fn attempt(&mut self, id: StreamId, attempt: Attempt) {
        let Attempt {
            head,
            end_stream,
            spare,
            exclude,
            attempts,
            carry,
        } = attempt;
        let now = Instant::now();
        let Some(backend) = self.shared.pick(exclude, now) else {
            debug!(stream = id.get(), "no backend available");
            self.answer_locally(id, 503);
            return;
        };
        let lease = match self.shared.pool.checkout(&backend) {
            Ok(lease) => lease,
            Err(e) => {
                debug!(stream = id.get(), error = %e, "no upstream slot");
                // We never reached a backend, so this says nothing about its
                // health — 503, and no failure recorded.
                self.answer_locally(id, 503);
                return;
            }
        };

        let Some(events) = self.events.clone() else {
            return;
        };
        let sent = lease.send(ToUpstream::Request {
            id: lease.id,
            client_id: id,
            head: head.clone(),
            end_stream,
            events,
        });
        if !sent {
            // The connection died between the checkout and this send. Nothing
            // was written to a backend, so 503 is honest and the client may
            // retry — but the connection dying *is* evidence about the backend.
            self.shared.health.failure(&backend, now);
            self.answer_locally(id, 503);
            return;
        }
        self.shared.stats.request();
        // A retry carries the stream's original clock and span forward: the
        // client is waiting for one request however many attempts it takes, and
        // restarting either would report a latency that never happened.
        let (started, span) = carry.unwrap_or_else(|| {
            self.shared.stats.open_client_stream();
            (
                now,
                tracing::debug_span!("stream", id = id.get(), backend = %backend.addr),
            )
        });
        self.routes.insert(
            id,
            Route {
                lease,
                request_done: end_stream,
                backend,
                head: spare,
                attempts,
                response_started: false,
                started,
                span,
            },
        );
    }

    /// A stream is over, however it ended: close its gauge and record what it
    /// cost.
    ///
    /// Both endings go through here — completion and cancellation — because a
    /// latency histogram that only counts successes is the one that looks
    /// healthiest exactly when things are worst.
    fn finished(&self, route: &Route) {
        self.shared.stats.close_client_stream();
        self.shared.stats.observe_latency(route.started.elapsed());
    }

    /// Try this stream again on a different backend, if it is safe to.
    ///
    /// Returns `true` when a second attempt is on its way, which is the caller's
    /// signal to swallow the failure so the client never learns the first one
    /// happened.
    fn retry(&mut self, id: StreamId) -> bool {
        let Some(route) = self.routes.get(&id) else {
            return false;
        };
        if route.response_started || route.attempts >= MAX_ATTEMPTS || route.head.is_none() {
            return false;
        }

        // Take the route out *before* the new checkout. Dropping it releases the
        // lease, freeing the concurrency slot the balancer counts — otherwise a
        // retry can be refused by the slot its own failed attempt is still
        // holding, which is the shape of the week-6 leaked-lease bug wearing a
        // different hat.
        let route = self.routes.remove(&id).expect("checked above");
        let head = route.head.expect("checked above");
        let failed = route.backend;

        let _entered = route.span.enter();
        debug!(backend = %failed.addr, "retrying on another backend");
        self.shared.stats.retry();
        drop(_entered);

        self.attempt(
            id,
            Attempt {
                head: head.clone(),
                end_stream: true,
                spare: Some(head),
                exclude: Some(failed),
                attempts: route.attempts + 1,
                carry: Some((route.started, route.span)),
            },
        );
        // `attempt` answers locally if it cannot place the request, so either
        // way the client hears something and nothing hangs.
        true
    }
}

impl Service for Proxy {
    fn attach(&mut self, events: Events) {
        self.events = Some(events);
    }

    fn dispatch(&mut self, id: StreamId, mut head: RequestHead, end_stream: bool) {
        Self::sanitize(&mut head);
        self.add_forwarded(&mut head);
        // Kept only if it could ever be replayed — see `Route::head`.
        let retryable = end_stream && IDEMPOTENT.contains(&head.method.as_ref());
        let spare = retryable.then(|| Box::new(head.clone()));
        self.attempt(
            id,
            Attempt {
                head: Box::new(head),
                end_stream,
                spare,
                exclude: None,
                attempts: 1,
                carry: None,
            },
        );
    }

    fn body(&mut self, id: StreamId, data: Bytes, end_stream: bool) {
        let Some(route) = self.routes.get_mut(&id) else {
            // No upstream to take them, so nothing will ever confirm these
            // octets — release the credit here or the client's window shrinks
            // for the rest of the connection.
            let n = data.len() as u32;
            if n > 0 {
                self.emit(ServiceEvent::BodyAccepted { id, n });
            }
            return;
        };
        if route.request_done {
            return;
        }
        route.request_done = end_stream;
        let n = data.len() as u32;
        if !route.lease.send(ToUpstream::Body {
            id: route.lease.id,
            data,
            end_stream,
        }) && n > 0
        {
            self.emit(ServiceEvent::BodyAccepted { id, n });
        }
    }

    fn trailers(&mut self, id: StreamId, fields: Vec<Header>) {
        let Some(route) = self.routes.get_mut(&id) else {
            return;
        };
        route.request_done = true;
        route.lease.send(ToUpstream::Trailers {
            id: route.lease.id,
            fields,
        });
    }

    fn cancel(&mut self, id: StreamId, code: ErrorCode) {
        // Dropping the route drops the lease, which frees the concurrency slot
        // the load balancer counts; the message is what actually resets the
        // upstream stream.
        if let Some(route) = self.routes.remove(&id) {
            route.lease.send(ToUpstream::Cancel {
                id: route.lease.id,
                code,
            });
            self.finished(&route);
        }
    }

    fn finish(&mut self, id: StreamId) {
        // Nothing to tell the upstream — its stream ended too, or it would not
        // have sent the END_STREAM that got us here. All this has to do is drop
        // the lease, and all the lease has to do is not be leaked: every request
        // that completes without this frees no slot, and after a few thousand of
        // them every pooled connection looks full.
        if let Some(route) = self.routes.remove(&id) {
            self.finished(&route);
        }
    }

    fn released(&mut self, id: StreamId, n: u32) {
        let Some(route) = self.routes.get(&id) else {
            return;
        };
        route.lease.send(ToUpstream::Released {
            id: route.lease.id,
            n,
        });
    }

    /// Watch every outcome on its way to the client: report it to health, and
    /// replace it with a retry where that is safe.
    fn intercept(&mut self, event: ServiceEvent) -> Option<ServiceEvent> {
        match &event {
            ServiceEvent::Head { id, response, .. } => {
                if let Some(route) = self.routes.get_mut(id) {
                    let backend = route.backend;
                    // Once a status is on the wire the response has begun, and
                    // no later failure can be retried — there is no way to
                    // un-send a `:status`.
                    route.response_started = true;
                    // A 5xx *from a backend* is a considered answer, not a
                    // transport failure, so it is not evidence the backend is
                    // unreachable. Anything it answers at all proves it is
                    // there, which is what health is asking about.
                    self.shared.health.success(&backend);
                }
                self.shared.stats.response(response.status);
                Some(event)
            }

            ServiceEvent::Gone { id } => {
                let Some(route) = self.routes.get(id) else {
                    return Some(event);
                };
                let backend = route.backend;
                // The connection died rather than the backend answering. This
                // is the one signal that genuinely says "this backend is not
                // reachable", and mistaking it for a response is what left
                // health checking inert: a killed backend produced 18% 5xx with
                // the ejection counter at zero.
                self.shared.health.failure(&backend, Instant::now());
                if self.retry(*id) {
                    return None;
                }
                Some(event)
            }
            ServiceEvent::Reset { id, code } => {
                let Some(route) = self.routes.get(id) else {
                    return Some(event);
                };
                let backend = route.backend;

                // REFUSED_STREAM is a promise that nothing was processed — from
                // §5.1.2, and from the GOAWAY rules for ids above
                // `last_stream_id`. That promise is exactly what makes a retry
                // safe, and it is the only reset code that carries it: CANCEL or
                // INTERNAL_ERROR may well have run the request already.
                if *code == ErrorCode::RefusedStream {
                    // Not a health failure. A backend refusing a stream is
                    // usually one at its concurrency limit or draining
                    // gracefully — both are correct behaviour, and ejecting a
                    // backend for being busy is how a load spike becomes an
                    // outage.
                    if self.retry(*id) {
                        return None;
                    }
                } else {
                    self.shared.health.failure(&backend, Instant::now());
                }
                Some(event)
            }

            _ => Some(event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hpack::Header;

    fn request(path: &'static str) -> RequestHead {
        RequestHead::from_headers(&[
            Header::new(Bytes::from_static(b":method"), Bytes::from_static(b"GET")),
            Header::new(Bytes::from_static(b":scheme"), Bytes::from_static(b"http")),
            Header::new(
                Bytes::from_static(b":authority"),
                Bytes::from_static(b"a.example"),
            ),
            Header::new(
                Bytes::from_static(b":path"),
                Bytes::from_static(path.as_bytes()),
            ),
            Header::new(Bytes::from_static(b"te"), Bytes::from_static(b"trailers")),
            Header::new(Bytes::from_static(b"accept"), Bytes::from_static(b"*/*")),
        ])
        .expect("well-formed")
    }

    #[test]
    fn sanitizing_drops_te_and_keeps_the_authority() {
        let mut head = request("/");
        Proxy::sanitize(&mut head);
        assert!(
            head.fields.iter().all(|f| f.name.as_ref() != b"te"),
            "te describes the client hop and means nothing upstream",
        );
        assert_eq!(
            head.authority.as_deref(),
            Some(&b"a.example"[..]),
            "rewriting the authority would change which vhost the backend serves",
        );
        assert!(head.fields.iter().any(|f| f.name.as_ref() == b"accept"));
    }

    fn forwarded_for(head: &RequestHead) -> Option<String> {
        head.fields
            .iter()
            .find(|f| f.name.as_ref() == b"x-forwarded-for")
            .map(|f| String::from_utf8_lossy(&f.value).into_owned())
    }

    fn proxy_with_peer(trust: bool) -> Proxy {
        Proxy::new(Shared::new(Vec::new(), 1))
            .with_peer(std::net::IpAddr::from([203, 0, 113, 7]))
            .trusting_forwarded_headers(trust)
    }

    #[test]
    fn a_forged_forwarded_for_is_replaced_by_default() {
        // The security-relevant default. Appending to a client-supplied chain
        // lets any client prepend whatever it likes, and the backend then cannot
        // tell which entry we observed from which the client invented — so an
        // allowlist or rate limit keyed on the first entry is trivially forged.
        let mut head = request("/");
        head.fields.push(Header::new(
            Bytes::from_static(b"x-forwarded-for"),
            Bytes::from_static(b"10.0.0.1, 192.168.1.1"),
        ));
        proxy_with_peer(false).add_forwarded(&mut head);
        assert_eq!(
            forwarded_for(&head).as_deref(),
            Some("203.0.113.7"),
            "the client's own claim about who it is must not survive",
        );
        assert_eq!(
            head.fields
                .iter()
                .filter(|f| f.name.as_ref() == b"x-forwarded-for")
                .count(),
            1,
            "exactly one x-forwarded-for, or the backend picks arbitrarily",
        );
    }

    #[test]
    fn a_trusted_chain_is_appended_to() {
        // Only when something trustworthy runs in front and the operator says so.
        let mut head = request("/");
        head.fields.push(Header::new(
            Bytes::from_static(b"x-forwarded-for"),
            Bytes::from_static(b"10.0.0.1"),
        ));
        proxy_with_peer(true).add_forwarded(&mut head);
        assert_eq!(
            forwarded_for(&head).as_deref(),
            Some("10.0.0.1, 203.0.113.7"),
        );
    }

    #[test]
    fn without_a_peer_no_forwarding_header_is_invented() {
        // The engine runs over duplex pipes in the differential tests, where
        // there is no address to name. Making one up would be a lie in a header
        // a backend might act on.
        let mut head = request("/");
        Proxy::new(Shared::new(Vec::new(), 1)).add_forwarded(&mut head);
        assert_eq!(forwarded_for(&head), None);
    }

    #[test]
    fn the_latency_histogram_buckets_cumulatively() {
        let stats = ProxyStats::default();
        stats.observe_latency(Duration::from_micros(400));
        stats.observe_latency(Duration::from_millis(4));
        let buckets = stats.latency_buckets();
        // 0.4 ms falls in the 0.0005 bucket and every one above it; 4 ms starts
        // at 0.005. Cumulative means the last bucket holds everything.
        assert_eq!(buckets[0], 0, "nothing was under 0.1 ms");
        assert_eq!(buckets[2], 1, "0.4 ms is at or under 0.5 ms");
        assert_eq!(buckets[6], 2, "both are at or under 5 ms");
        assert_eq!(buckets[LATENCY_BUCKETS.len() - 1], 2, "+Inf holds all");
        assert_eq!(stats.latency_count(), 2);
    }

    #[tokio::test]
    async fn with_no_backends_a_request_is_answered_rather_than_dropped() {
        let shared = Shared::new(Vec::new(), 1);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut proxy = Proxy::new(shared);
        proxy.attach(tx);
        proxy.dispatch(StreamId::new(1), request("/"), true);

        // A hang is the one failure a proxy must never produce, so "nothing to
        // proxy to" has to become a status.
        let event = rx.try_recv().expect("an answer");
        match event {
            ServiceEvent::Head { response, .. } => assert_eq!(response.status, 503),
            other => panic!("expected a 503, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn body_octets_with_no_route_still_return_their_credit() {
        let shared = Shared::new(Vec::new(), 1);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut proxy = Proxy::new(shared);
        proxy.attach(tx);
        // No dispatch, so no route: the client is still owed its window back, or
        // it would stop being able to send for the life of the connection.
        proxy.body(StreamId::new(1), Bytes::from_static(b"hello"), false);
        let accepted = std::iter::from_fn(|| rx.try_recv().ok())
            .any(|e| matches!(e, ServiceEvent::BodyAccepted { n: 5, .. }));
        assert!(accepted, "unroutable body octets must still release credit");
    }
}
