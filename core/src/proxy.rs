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

use bytes::Bytes;
use tracing::debug;

use crate::conn::ErrorCode;
use crate::hpack::Header;
use crate::lb::{Backend, LoadBalancer, PowerOfTwoChoices};
use crate::pool::{Lease, Pool};
use crate::service::{Events, RequestHead, Response, Service, ServiceEvent};
use crate::stream::StreamId;
use crate::upstream::ToUpstream;

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
}

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
}

/// Everything the proxy shares between client connections: the pool, the load
/// balancer, the backend list, and the counters.
#[derive(Debug)]
pub struct Shared {
    pub pool: Pool,
    pub backends: Vec<Backend>,
    pub balancer: Box<dyn LoadBalancer>,
    pub stats: Arc<ProxyStats>,
}

impl Shared {
    /// Build the shared half of the proxy for `backends`.
    pub fn new(backends: Vec<Backend>, max_conns_per_backend: usize) -> Arc<Self> {
        let stats = Arc::new(ProxyStats::default());
        Arc::new(Shared {
            pool: Pool::new(Arc::clone(&stats), max_conns_per_backend),
            backends,
            balancer: Box::new(PowerOfTwoChoices::new()),
            stats,
        })
    }
}

/// One client stream's upstream leg.
#[derive(Debug)]
struct Route {
    lease: Lease,
    /// Whether the request has ended, so a late body chunk is dropped rather
    /// than sent on a half-closed stream.
    request_done: bool,
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
}

impl Proxy {
    pub fn new(shared: Arc<Shared>) -> Self {
        Proxy {
            shared,
            events: None,
            routes: HashMap::new(),
        }
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
}

impl Service for Proxy {
    fn attach(&mut self, events: Events) {
        self.events = Some(events);
    }

    fn dispatch(&mut self, id: StreamId, mut head: RequestHead, end_stream: bool) {
        let load = self.shared.pool.load(&self.shared.backends);
        let Some(backend) = self.shared.balancer.pick(&load) else {
            debug!(stream = id.get(), "no backend available");
            self.answer_locally(id, 503);
            return;
        };
        let lease = match self.shared.pool.checkout(&backend) {
            Ok(lease) => lease,
            Err(e) => {
                debug!(stream = id.get(), error = %e, "no upstream slot");
                self.answer_locally(id, 503);
                return;
            }
        };

        Self::sanitize(&mut head);
        let Some(events) = self.events.clone() else {
            return;
        };
        let sent = lease.send(ToUpstream::Request {
            id: lease.id,
            client_id: id,
            head: Box::new(head),
            end_stream,
            events,
        });
        if !sent {
            // The connection died between the checkout and this send. Nothing
            // was written to a backend, so 503 is honest and the client may
            // retry.
            self.answer_locally(id, 503);
            return;
        }
        self.shared.stats.request();
        self.routes.insert(
            id,
            Route {
                lease,
                request_done: end_stream,
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
        }
    }

    fn finish(&mut self, id: StreamId) {
        // Nothing to tell the upstream — its stream ended too, or it would not
        // have sent the END_STREAM that got us here. All this has to do is drop
        // the lease, and all the lease has to do is not be leaked: every request
        // that completes without this frees no slot, and after a few thousand of
        // them every pooled connection looks full.
        self.routes.remove(&id);
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
