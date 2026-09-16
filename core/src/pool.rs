//! Upstream connection pool and coalescing (design doc §4.3).
//!
//! Maintains warm HTTP/2 connections per backend, opens more on demand up to
//! the backend's advertised `MAX_CONCURRENT_STREAMS`, and retires them on
//! failure, idle timeout, or stream-id exhaustion. Coalescing is the project
//! thesis made concrete: many client streams — arriving over many *client
//! connections* — ride few upstream ones.
//!
//! # A checkout never waits
//!
//! [`Pool::checkout`] is synchronous and returns a [`Lease`]: a handle plus the
//! upstream stream id this request will use. Two consequences, both deliberate:
//!
//! - **The lease names a *request*, not a stream id.** The upstream stream id is
//!   allocated by the connection task at the moment it sends the HEADERS,
//!   because §5.1.1 requires ids to increase *in the order they go on the wire*
//!   — and two client connections leasing from the same pooled connection have
//!   no ordering between them. Handing out ids here instead produced exactly
//!   that bug: a request leased id 5 could reach the connection task after one
//!   leased id 7, and the backend rightly refused it. The [`crate::upstream`]
//!   task keeps the `RequestId` → stream-id map (design doc §4.3).
//! - **A connection that does not exist yet can still be leased.** The channel
//!   is created before the socket: the record goes into the pool, the connect
//!   runs on its own task, and requests queue in the inbox meanwhile. Making a
//!   checkout await a TCP handshake would block the *client* connection task,
//!   stalling every other stream on it — the exact failure this week is built to
//!   avoid.
//!
//! If the connect fails, the pending requests are answered with 502 rather than
//! left waiting. Nothing hangs.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use tokio::net::TcpStream;
use tracing::{debug, warn};

use crate::lb::{Backend, BackendLoad};
use crate::proxy::ProxyStats;
use crate::stream::MAX_LOCAL_STREAM_ID;
use crate::upstream::{RequestId, ToUpstream, UpstreamHandle};

/// How many concurrent streams to assume a backend allows before its SETTINGS
/// arrives.
///
/// Conservative on purpose: guessing high would pile requests onto a connection
/// that then refuses them, and a REFUSED_STREAM costs a round trip. The real
/// value replaces this as soon as the handshake completes.
const ASSUMED_MAX_CONCURRENT: usize = 100;

/// Stop leasing from a connection this close to the end of the id space, so the
/// last few ids are never handed out in a race.
const ID_EXHAUSTION_MARGIN: u32 = 64;

/// Why a pool checkout failed.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum PoolError {
    #[error("backend {0:?} is unreachable")]
    Unreachable(Backend),
    #[error("no backend is configured")]
    NoBackend,
    #[error("backend {0:?} is at capacity")]
    AtCapacity(Backend),
}

/// One pooled upstream connection, as the pool sees it.
///
/// Everything the hot path reads is an atomic, so a checkout and the load
/// balancer's `outstanding` both run without waiting on the connection task or
/// on each other.
#[derive(Debug)]
pub struct UpstreamRecord {
    handle: UpstreamHandle,
    /// Streams in flight on this connection.
    live: AtomicUsize,
    /// How many requests have been started on this connection. Each one costs
    /// exactly one stream id, so this is also how far into the 31-bit id space
    /// the connection has travelled.
    issued: AtomicU32,
    /// The peer's `MAX_CONCURRENT_STREAMS`, learned at handshake.
    max_concurrent: AtomicUsize,
    /// Set when the connection task has gone or is going away.
    closed: AtomicBool,
    /// Millis since this pool was built, at the last checkout. An atomic rather
    /// than an `Instant` so the hot path stays lock-free and the record stays
    /// `Sync` without a mutex.
    last_used_ms: AtomicU64,
    /// How many requests are waiting for a stream slot on this connection,
    /// published by the connection task.
    ///
    /// This is the pool's only view of whether a full connection is *coping*.
    /// `live` says it is at the backend's stream limit, which is a fact about
    /// the protocol; this says whether the queue behind that limit is draining,
    /// which is the fact about throughput the pool actually needs.
    queued: AtomicUsize,

    // --- observability ------------------------------------------------------
    //
    // These exist because the latency cliff took roughly a dozen experiments to
    // explain, and most of them were spent establishing facts the connection
    // task already knew and had no way to say. Six hypotheses were eliminated by
    // measurement; four of those measurements would have been a glance at a
    // gauge if these had existed.
    //
    // All relaxed, all written by the one task that owns the connection, and all
    // read by a 1 s sampler. Nothing here is control flow - if these numbers are
    // torn or stale, the proxy behaves identically.
    /// Passes through the connection's select loop.
    loop_passes: AtomicU64,
    /// Passes in which the socket-read arm was the one that fired.
    ///
    /// The ratio of this to `loop_passes` is the number the read-starvation
    /// argument was missing. A `biased` select that puts writes and the inbox
    /// ahead of reads starves reads exactly when there is always a write and
    /// always a message - and "exactly then" is a claim about a ratio, which is
    /// why arguing about it went nowhere until it could be read off a gauge.
    loop_reads: AtomicU64,
    /// High-water queue depth since the last sample.
    ///
    /// A high-water mark rather than an instantaneous read, and **reset when
    /// sampled**, matching `ProxyStats::peak_buffered`. A 1 s sampler against a
    /// live value cannot see a 300 ms excursion; it aliases it away, and the
    /// excursion is the whole phenomenon.
    queued_peak: AtomicUsize,
    /// High-water octets waiting in the connection's write buffer, same rule.
    out_peak: AtomicUsize,
}

/// One upstream connection's counters, read at a moment.
///
/// A plain value rather than a borrow of the record: the caller is a metrics
/// sampler holding the pool mutex for as short a time as it can manage, and
/// handing out references would hold it for as long as the caller took to format
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnSnapshot {
    pub live: usize,
    pub queued: usize,
    /// High-water queue depth since the previous snapshot.
    pub queued_peak: usize,
    /// High-water write-buffer occupancy since the previous snapshot, in octets.
    pub out_peak: usize,
    pub loop_passes: u64,
    pub loop_reads: u64,
}

impl ConnSnapshot {
    /// The fraction of loop passes in which this connection read its socket.
    ///
    /// `None` when the connection has not run a pass since the last sample,
    /// which is different from a ratio of zero: an idle connection parked on its
    /// sockets is healthy, and reporting it as 0.0 would make idleness look like
    /// starvation. The distinction matters because the metric exists to find
    /// starvation.
    pub fn read_ratio(&self) -> Option<f64> {
        (self.loop_passes > 0).then(|| self.loop_reads as f64 / self.loop_passes as f64)
    }
}

impl UpstreamRecord {
    fn new(handle: UpstreamHandle, now_ms: u64) -> Self {
        UpstreamRecord {
            handle,
            live: AtomicUsize::new(0),
            issued: AtomicU32::new(0),
            max_concurrent: AtomicUsize::new(ASSUMED_MAX_CONCURRENT),
            closed: AtomicBool::new(false),
            last_used_ms: AtomicU64::new(now_ms),
            queued: AtomicUsize::new(0),
            loop_passes: AtomicU64::new(0),
            loop_reads: AtomicU64::new(0),
            queued_peak: AtomicUsize::new(0),
            out_peak: AtomicUsize::new(0),
        }
    }

    fn usable(&self, now_ms: u64, idle_timeout_ms: u64) -> bool {
        // An idle connection is recycled rather than kept forever. The timeout
        // sits *under* the common 65 s upstream keep-alive so that we close
        // first: racing a backend's own idle close means occasionally handing a
        // request to a socket that is already going away, and the client sees
        // that as a failure rather than as the housekeeping it is.
        let idle_ms = now_ms.saturating_sub(self.last_used_ms.load(Ordering::Relaxed));
        let idle_and_empty = idle_ms >= idle_timeout_ms && self.live.load(Ordering::Relaxed) == 0;
        !idle_and_empty
            && !self.closed.load(Ordering::Relaxed)
            && !self.handle.is_closed()
            // Two ids per request (odd only), so the id space runs out at half
            // the count. A connection that reaches it is retired rather than
            // wrapped: ids are never reused.
            && self.issued.load(Ordering::Relaxed) < (MAX_LOCAL_STREAM_ID / 2) - ID_EXHAUSTION_MARGIN
    }

    fn has_room(&self) -> bool {
        self.live.load(Ordering::Relaxed) < self.max_concurrent.load(Ordering::Relaxed)
    }

    /// Publish this connection's wait-for-a-slot queue depth. Called by the
    /// connection task whenever the queue changes.
    pub fn set_queued(&self, depth: usize) {
        self.queued.store(depth, Ordering::Relaxed);
        self.queued_peak.fetch_max(depth, Ordering::Relaxed);
    }

    /// Read the observability counters, clearing the high-water marks.
    ///
    /// `swap(0)` on the peaks is what makes them "since the last sample" rather
    /// than "since the connection opened". A mark that never resets converges on
    /// the worst moment of the process's life and then never moves again, which
    /// tells you that something once happened and nothing about whether it still
    /// does.
    fn snapshot(&self) -> ConnSnapshot {
        ConnSnapshot {
            live: self.live.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            queued_peak: self.queued_peak.swap(0, Ordering::Relaxed),
            out_peak: self.out_peak.swap(0, Ordering::Relaxed),
            loop_passes: self.loop_passes.swap(0, Ordering::Relaxed),
            loop_reads: self.loop_reads.swap(0, Ordering::Relaxed),
        }
    }

    /// Record one pass of the connection's I/O loop.
    ///
    /// Called by the connection task once per pass, after the select resolves.
    /// Two relaxed increments and two `fetch_max`es on a path that has just done
    /// a syscall, so the cost is not measurable against it.
    pub fn note_pass(&self, read: bool, out_len: usize) {
        self.loop_passes.fetch_add(1, Ordering::Relaxed);
        if read {
            self.loop_reads.fetch_add(1, Ordering::Relaxed);
        }
        self.out_peak.fetch_max(out_len, Ordering::Relaxed);
    }

    fn queued(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Whether this connection's queue has grown past the point where another
    /// connection would help.
    ///
    /// Deliberately not "the queue is non-empty". A connection at the backend's
    /// stream limit *always* has a queue for as long as it is busy — that is
    /// what the limit means — and treating a transient one as distress is the
    /// mistake this predicate exists to avoid.
    fn backed_up(&self, threshold: usize) -> bool {
        self.queued() >= threshold
    }

    /// Whether this connection is already holding every request it agreed to
    /// queue, and would shed the next one.
    ///
    /// Asked of the published queue depth rather than computed from `live` and
    /// `max_concurrent`. Those two are the pool's *belief* about the
    /// connection, and the belief starts as [`ASSUMED_MAX_CONCURRENT`] — a
    /// guess that stands until the backend's SETTINGS arrives and may never be
    /// corrected, because a backend is entitled to advertise no limit at all.
    /// Refusing on the guess means refusing requests a connection would have
    /// been happy to take. The queue depth is not a belief: it is what the
    /// connection task last reported about itself.
    fn saturated(&self, max_pending: usize) -> bool {
        self.queued() >= max_pending
    }

    /// Claim a slot on this connection for one request.
    fn lease(self: &Arc<Self>, now_ms: u64) -> Lease {
        self.last_used_ms.store(now_ms, Ordering::Relaxed);
        let request = self.issued.fetch_add(1, Ordering::Relaxed);
        self.live.fetch_add(1, Ordering::Relaxed);
        Lease {
            record: Arc::clone(self),
            id: RequestId::new(request),
        }
    }

    /// Record the peer's `MAX_CONCURRENT_STREAMS` once its SETTINGS arrives.
    pub fn set_max_concurrent(&self, max: usize) {
        self.max_concurrent.store(max, Ordering::Relaxed);
    }

    /// Take this connection out of rotation while its task keeps running.
    ///
    /// The two are genuinely different states. A connection whose backend has
    /// sent GOAWAY must accept no *new* request — leasing onto it would produce
    /// a request the connection cannot open a stream for — but it is still
    /// serving the streams the backend promised to finish, and it is not gone
    /// until those are done.
    pub fn retire(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}

/// A leased slot on an upstream connection: which connection, and which request
/// on it.
///
/// Dropping a lease frees the concurrency slot, which is what keeps the count
/// the load balancer reads honest. It does **not** cancel the stream —
/// cancelling is a message, because only the connection task may send
/// RST_STREAM.
#[derive(Debug)]
pub struct Lease {
    record: Arc<UpstreamRecord>,
    /// Names this request to the connection task. Not a stream id — see the
    /// module doc for why that distinction is load-bearing.
    pub id: RequestId,
}

impl Lease {
    /// Send a message to the connection this lease is on. `false` if that
    /// connection has gone.
    pub fn send(&self, msg: ToUpstream) -> bool {
        self.record.handle.send(msg)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.record.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Everything known about one backend.
#[derive(Debug, Default)]
struct BackendPool {
    conns: Vec<Arc<UpstreamRecord>>,
}

/// The upstream connection pool.
#[derive(Debug)]
pub struct Pool {
    backends: Mutex<HashMap<Backend, BackendPool>>,
    stats: Arc<ProxyStats>,
    /// How many connections a single backend may have open at once. A ceiling
    /// rather than a target: the pool opens one only when every existing
    /// connection is full, so a healthy backend with a generous
    /// `MAX_CONCURRENT_STREAMS` stays at one.
    max_conns_per_backend: usize,
    /// When this pool was built. Idle ages are millis from here, so the record
    /// can hold a plain `AtomicU64` instead of a lock around an `Instant`.
    epoch: Instant,
    /// How long a connection may sit unused before it is recycled. Cached from
    /// `policy` in the unit the hot path compares in.
    idle_timeout_ms: u64,
    /// Timings for idle recycling and active probing.
    policy: crate::health::Policy,
    /// Where a connection's own verdict on its backend goes.
    ///
    /// The pool is the only layer that knows both halves: which backend a
    /// connection belongs to, and that the connection has just died of an
    /// unanswered probe. A probe with nowhere to report is a socket recycler,
    /// not a health check.
    health: Option<Arc<crate::health::Health>>,
    /// The flow-control sizes to open upstream connections with. The upstream
    /// receive window is the half of the bridge that bounds how much a fast
    /// backend can push into this process, so tuning it without tuning the
    /// client side (or the reverse) moves the memory bound in one direction
    /// only — which is why one struct carries both.
    tuning: crate::conn::Tuning,
}

impl Pool {
    pub fn new(stats: Arc<ProxyStats>, max_conns_per_backend: usize) -> Self {
        Self::with_policy(
            stats,
            max_conns_per_backend,
            crate::health::Policy::default(),
            None,
        )
    }

    pub fn with_policy(
        stats: Arc<ProxyStats>,
        max_conns_per_backend: usize,
        policy: crate::health::Policy,
        health: Option<Arc<crate::health::Health>>,
    ) -> Self {
        Pool {
            backends: Mutex::new(HashMap::new()),
            stats,
            max_conns_per_backend: max_conns_per_backend.max(1),
            epoch: Instant::now(),
            idle_timeout_ms: policy.idle_timeout.as_millis() as u64,
            policy,
            health,
            tuning: crate::conn::Tuning::default(),
        }
    }

    /// Open upstream connections with tuned flow-control sizes. The daemon
    /// passes what it measured; everything else keeps the defaults.
    #[must_use]
    pub fn with_tuning(mut self, tuning: crate::conn::Tuning) -> Self {
        self.tuning = tuning;
        self
    }

    /// The queue depth at which a connection counts as failing to cope.
    ///
    /// Half the shed bound: deep enough that an ordinary burst does not trigger
    /// a new socket, shallow enough that a connection genuinely falling behind
    /// gets help well before it starts refusing requests.
    fn queue_threshold(&self) -> usize {
        (self.tuning.max_pending / 2).max(1)
    }

    /// Millis since this pool was built.
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Lease a slot on a connection to `backend`, opening one if every warm
    /// connection is full.
    pub fn checkout(&self, backend: &Backend) -> Result<Lease, PoolError> {
        let mut backends = self.backends.lock().expect("pool mutex poisoned");
        let pool = backends.entry(*backend).or_default();

        // Drop connections that have died since we last looked. Doing it here
        // rather than on a timer keeps the pool free of a reaper task, and the
        // list is short by construction.
        let now_ms = self.now_ms();
        pool.conns
            .retain(|record| record.usable(now_ms, self.idle_timeout_ms));

        // Prefer the *fullest* connection that still has room. Filling one
        // before opening another is what coalescing means — spreading streams
        // evenly across connections would open the maximum number of them and
        // produce exactly the fan-out the proxy exists to prevent.
        if let Some(record) = pool
            .conns
            .iter()
            .filter(|record| record.has_room())
            .max_by_key(|record| record.live.load(Ordering::Relaxed))
        {
            return Ok(record.lease(now_ms));
        }

        // Every warm connection is at the backend's stream limit. That is *not*
        // by itself a reason to open another, and treating it as one is what
        // made this policy expensive.
        //
        // The limit is a protocol fact, not a throughput signal: a connection
        // holding its 200 streams and retiring them promptly is keeping up, and
        // giving it a sibling does not make it finish sooner. What it does is
        // admit another 200 streams to the same backend, and by Little's law
        // concurrency that buys no throughput is bought entirely with latency.
        //
        // That is the argument. The measurement does **not** currently support
        // it, and this comment says so rather than quoting the run that did.
        //
        // An earlier version of these lines read: "raising the ceiling from 1 to
        // 8 moved p99 from 21 ms to 297 ms while lowering delivered rate". That
        // came from one pair of runs. `bench/confirm.sh` re-ran both policies
        // from this binary — `H2PROXYD_POOL_GROWTH=eager|queue`, six alternating
        // pairs at each of three rates — and could not separate them at any
        // rate (`bench/confirm.csv`):
        //
        //   10,000 req/s  neither policy ever opens a second connection, so the
        //                 arms are identical by construction. They still differ
        //                 by 30% in median p99, which is this machine's noise
        //                 floor and the reason the 14x was never safe to quote.
        //   20,000 req/s  both arms reach the 8-connection ceiling and shed.
        //   30,000 req/s  likewise, and `eager` won 4 of 6 pairs — if anything
        //                 the wrong direction.
        //
        // So the default is kept on the argument above, not on a measured
        // latency win, and the honest scope of the change is narrow: the two
        // policies differ only while a connection is at the backend's stream
        // limit *and* the pool is below its ceiling. Outside that band they are
        // the same policy. What `Queue` reliably does buy is **sockets** — one
        // connection instead of eight at rates the proxy is keeping up with —
        // which is worth having on its own and does not need a latency claim
        // propping it up.
        //
        // The first connection is not a growth decision — there is nothing to
        // reuse and nothing to have an opinion about yet. Past that, `Queue`
        // asks whether the existing connections are failing to cope, which is
        // what a queue that will not drain looks like, and `Eager` asks only
        // whether they are full.
        let first = pool.conns.is_empty();
        let grow = first
            || match self.tuning.growth {
                crate::conn::PoolGrowth::Eager => true,
                crate::conn::PoolGrowth::Queue => {
                    let threshold = self.queue_threshold();
                    !pool.conns.iter().any(|record| !record.backed_up(threshold))
                }
            };
        if pool.conns.len() < self.max_conns_per_backend && grow {
            let record = self.open(*backend, now_ms);
            pool.conns.push(Arc::clone(&record));
            return Ok(record.lease(now_ms));
        }

        // Either the connections are coping, or we are at the ceiling. Lease
        // from the one with the least work waiting and let it **queue** the
        // request until a stream frees up — bounded by `Tuning::max_pending`,
        // past which the connection sheds rather than promising what it cannot
        // deliver.
        //
        // Refusing here instead is what the first version did, and it turned a
        // busy moment into a 503 — including at start-up, before any backend
        // SETTINGS had arrived and the assumed limit was the only thing being
        // enforced. A saturated pool should cost latency, not errors: the
        // backend's real limit is enforced by the connection task, which is the
        // only thing that knows it.
        // Shortest queue first, with load as the tie-break: among connections
        // that are all at the stream limit, the one with least waiting is the
        // one that will start this request soonest.
        let Some(record) = pool
            .conns
            .iter()
            .min_by_key(|record| (record.queued(), record.live.load(Ordering::Relaxed)))
        else {
            return Err(PoolError::Unreachable(*backend));
        };

        // Even the least-loaded connection is holding everything it agreed to
        // hold. Refuse here, in the caller's task, rather than accepting the
        // request and letting the connection task discover the same thing.
        //
        // Where the refusal happens turns out to matter as much as that it
        // happens. Shedding inside the connection task puts the cost of
        // overload on the one task the overload is already starving: every
        // refusal is a message it must receive, match and answer, competing
        // with the requests it is trying to finish. Measured at 40,000 req/s
        // that regime delivered 6,068 req/s, because the connection spent
        // itself saying no. Refusing at checkout spreads the same work across
        // the client connections that caused it, and leaves the upstream task
        // doing nothing but upstream work.
        if record.saturated(self.tuning.max_pending) {
            self.stats.shed();
            return Err(PoolError::AtCapacity(*backend));
        }
        Ok(record.lease(now_ms))
    }

    /// Each backend with its current load — the input the load balancer works
    /// from.
    pub fn load(&self, backends: &[Backend]) -> Vec<BackendLoad> {
        let pools = self.backends.lock().expect("pool mutex poisoned");
        backends
            .iter()
            .map(|backend| BackendLoad {
                backend: *backend,
                outstanding: pools
                    .get(backend)
                    .map(|pool| {
                        pool.conns
                            .iter()
                            .map(|record| record.live.load(Ordering::Relaxed))
                            .sum()
                    })
                    .unwrap_or(0),
            })
            .collect()
    }

    /// Warm connections currently held, for the pool-utilization gauge (§7).
    /// Whether any connection to any backend has a queue that is not draining.
    ///
    /// The distress signal admission control reacts to (ADR 0023). Deliberately
    /// the same predicate the growth policy uses, and for the same reason: a
    /// connection at the backend's stream limit is not in trouble - that is what
    /// the limit means - but one whose wait-for-a-slot queue keeps growing is.
    pub fn backed_up(&self) -> bool {
        let threshold = self.queue_threshold();
        self.backends
            .lock()
            .expect("pool mutex poisoned")
            .values()
            .flat_map(|pool| pool.conns.iter())
            .any(|record| record.backed_up(threshold))
    }

    /// One live sample per upstream connection.
    ///
    /// Sampled from the connections that exist *now*, never accumulated at
    /// connection close. That distinction is not pedantry: it is the bug
    /// `h2proxy_stream_concurrency_max` had, which set its value from a
    /// connection's summary at teardown and therefore read `1` against a proxy
    /// carrying thousands of streams. During a benchmark every connection is
    /// still open, which is the entire point of HTTP/2, so a gauge that only
    /// moves at teardown answers no question about steady state.
    pub fn conn_snapshots(&self) -> Vec<ConnSnapshot> {
        let now_ms = self.now_ms();
        self.backends
            .lock()
            .expect("pool mutex poisoned")
            .values()
            .flat_map(|pool| pool.conns.iter())
            .filter(|record| record.usable(now_ms, self.idle_timeout_ms))
            .map(|record| record.snapshot())
            .collect()
    }

    pub fn connection_count(&self) -> usize {
        let now_ms = self.now_ms();
        self.backends
            .lock()
            .expect("pool mutex poisoned")
            .values()
            .map(|pool| {
                pool.conns
                    .iter()
                    .filter(|c| c.usable(now_ms, self.idle_timeout_ms))
                    .count()
            })
            .sum()
    }

    /// Create the record and start connecting. The record is usable
    /// immediately; the socket catches up.
    fn open(&self, backend: Backend, now_ms: u64) -> Arc<UpstreamRecord> {
        let (handle, inbox) = crate::upstream::channel();
        let record = Arc::new(UpstreamRecord::new(handle, now_ms));
        let stats = Arc::clone(&self.stats);
        let task_record = Arc::clone(&record);
        let health = self.health.clone();
        let (ping_idle, ping_timeout) = (self.policy.ping_idle, self.policy.ping_timeout);
        let tuning = self.tuning;

        self.stats.connect_attempt();
        tokio::spawn(async move {
            match TcpStream::connect(backend.addr).await {
                Ok(socket) => {
                    // Nagle would batch our small control frames behind a round
                    // trip; on a proxy leg that is pure added latency.
                    let _ = socket.set_nodelay(true);
                    stats.open_connection();
                    debug!(backend = %backend.addr, "upstream connection established");
                    let summary = crate::upstream::UpstreamConnection::with_tuning(
                        socket,
                        inbox,
                        Arc::clone(&stats),
                        Some(Arc::clone(&task_record)),
                        tuning,
                    )
                    .with_probe(ping_idle, ping_timeout)
                    .run()
                    .await;
                    if summary.probe_timed_out {
                        // The whole reason the probe exists. A black-holed
                        // backend fails no request — the requests on it hang —
                        // so this is the only report health will ever get, and
                        // an idle connection dying silently would make active
                        // probing a socket recycler with a metric.
                        stats.probe_failure();
                        if let Some(health) = &health {
                            health.failure(&backend, tokio::time::Instant::now());
                        }
                    }
                    debug!(backend = %backend.addr, ?summary, "upstream connection closed");
                }
                Err(e) => {
                    warn!(backend = %backend.addr, error = %e, "upstream connect failed");
                    stats.connect_failure();
                    // The requests already queued behind this connect have to be
                    // answered, or their clients wait forever for a backend that
                    // was never there. The connection task does the same on its
                    // way out; this is the path where it never got to start.
                    crate::upstream::fail_pending(inbox);
                }
            }
            task_record.closed.store(true, Ordering::Relaxed);
        });
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::PoolGrowth;

    fn backend(port: u16) -> Backend {
        Backend::new(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
    }

    fn record() -> Arc<UpstreamRecord> {
        let (handle, rx) = crate::upstream::channel();
        // Keep the receiver alive: a dropped one makes the handle look closed.
        Box::leak(Box::new(rx));
        Arc::new(UpstreamRecord::new(handle, 0))
    }

    #[test]
    fn every_lease_names_a_distinct_request() {
        let record = record();
        let leases: Vec<Lease> = (0..100).map(|_| record.lease(0)).collect();
        let ids: Vec<u32> = leases.iter().map(|l| l.id.get()).collect();
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "request ids must be unique",
        );
        assert_eq!(record.live.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn dropping_a_lease_frees_the_slot() {
        let record = record();
        {
            let _lease = record.lease(0);
            assert_eq!(record.live.load(Ordering::Relaxed), 1);
        }
        assert_eq!(record.live.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_connection_stops_being_usable_near_the_end_of_the_id_space() {
        let record = record();
        assert!(record.usable(0, u64::MAX));
        record
            .issued
            .store(MAX_LOCAL_STREAM_ID / 2, Ordering::Relaxed);
        assert!(
            !record.usable(0, u64::MAX),
            "an exhausted connection must be replaced, not wrapped: ids are never reused",
        );
    }

    #[test]
    fn room_follows_the_backends_advertised_limit() {
        let record = record();
        record.set_max_concurrent(2);
        let _a = record.lease(0);
        let _b = record.lease(0);
        assert!(!record.has_room(), "at the backend's limit");
    }

    #[tokio::test]
    async fn checkout_fills_one_connection_before_opening_another() {
        let stats = Arc::new(ProxyStats::default());
        let pool = Pool::new(Arc::clone(&stats), 4);
        // The backend does not exist; that is fine, since a checkout never waits
        // for the socket. What matters is which connection the leases land on.
        let leases: Vec<Lease> = (0..10)
            .map(|_| pool.checkout(&backend(1)).expect("a lease"))
            .collect();
        assert_eq!(leases.len(), 10);
        assert_eq!(
            pool.connection_count(),
            1,
            "ten streams must coalesce onto one connection",
        );
    }

    #[tokio::test]
    async fn a_full_connection_that_is_keeping_up_does_not_get_a_sibling() {
        // Being at `MAX_CONCURRENT_STREAMS` is a fact about the protocol, not a
        // complaint. A connection retiring streams as fast as it takes them is
        // working perfectly, and giving it a sibling does not make it finish
        // sooner — it admits another connection's worth of streams to the same
        // backend, which by Little's law is latency bought with no throughput.
        let stats = Arc::new(ProxyStats::default());
        let pool = Pool::new(Arc::clone(&stats), 4);
        let first = pool.checkout(&backend(2)).expect("a lease");
        first.record.set_max_concurrent(1);
        first.record.set_queued(0);

        let _second = pool.checkout(&backend(2)).expect("a second lease");
        assert_eq!(
            pool.connection_count(),
            1,
            "a full but draining connection must absorb the request, not spawn a socket",
        );
    }

    #[tokio::test]
    async fn a_backed_up_connection_gets_a_sibling() {
        // The case the ceiling exists for: the queue will not drain, so the
        // request is going to wait either way. Another connection is the only
        // thing that can raise the number of streams this backend will accept.
        let stats = Arc::new(ProxyStats::default());
        let pool = Pool::new(Arc::clone(&stats), 4);
        let first = pool.checkout(&backend(3)).expect("a lease");
        first.record.set_max_concurrent(1);
        // Derived from the bound rather than written as a number: the threshold
        // is half of `MAX_PENDING`, and an earlier version of this test hardcoded
        // 255 against a `MAX_PENDING` of 128. When that constant was re-derived
        // the test started asserting the opposite of what it says.
        first.record.set_queued(crate::conn::MAX_PENDING);

        let _second = pool.checkout(&backend(3)).expect("a second lease");
        assert_eq!(pool.connection_count(), 2);
    }

    #[test]
    fn an_idle_connection_is_recycled_but_a_busy_one_is_not() {
        // The pool doc has promised idle recycling since week 6 without it
        // existing. It matters for a specific reason: the common upstream
        // keep-alive is 65 s, so a pool that holds connections forever
        // eventually hands a request to a socket the backend is closing, and the
        // client sees a failure rather than the housekeeping it is.
        let record = record();
        assert!(
            record.usable(1_000, 60_000),
            "fresh, and inside the timeout"
        );
        assert!(
            record.usable(60_000, 60_000) || record.live.load(Ordering::Relaxed) == 0,
            "sanity",
        );

        // Idle past the timeout with nothing in flight: recycle.
        assert!(!record.usable(60_001, 60_000));

        // Same age, but a stream is live: keep it. Recycling a connection with
        // work on it would abandon that work.
        let _lease = record.lease(0);
        assert!(
            record.usable(60_001, 60_000),
            "a connection with a live stream is not idle, however old",
        );
    }

    #[test]
    fn a_checkout_keeps_a_connection_from_going_idle() {
        let record = record();
        let lease = record.lease(0);
        drop(lease);
        assert!(!record.usable(60_001, 60_000), "idle since ms 0");
        let lease = record.lease(60_000);
        drop(lease);
        assert!(
            record.usable(60_001, 60_000),
            "a checkout must count as use, or a busy pool churns its connections",
        );
    }

    #[tokio::test]
    async fn a_saturated_pool_queues_rather_than_refusing() {
        let stats = Arc::new(ProxyStats::default());
        let pool = Pool::new(Arc::clone(&stats), 1);
        let lease = pool.checkout(&backend(3)).expect("a lease");
        lease.record.set_max_concurrent(1);

        // At the connection ceiling with the only connection full. Refusing here
        // is what the first version did, and it made a busy backend look like a
        // broken one: `h2load -c 50 -m 20` produced thousands of 503s at
        // start-up, before any backend SETTINGS had arrived to replace the
        // assumed limit. The connection task holds the request until a stream
        // frees up, so saturation costs latency instead.
        let queued = pool.checkout(&backend(3)).expect("a lease, not an error");
        assert_eq!(
            pool.connection_count(),
            1,
            "the ceiling still bounds connections",
        );
        drop(queued);
    }

    // ---------------------------------------------------------------------
    // The growth policies, pinned against each other
    // ---------------------------------------------------------------------
    //
    // `PoolGrowth::Eager` is not on any production path — it exists so that
    // `bench/confirm.sh` can measure the default against the policy it replaced,
    // from one binary with one flag different. That makes it exactly the kind of
    // code that rots silently, so these two tests state the difference between
    // the arms rather than merely exercising each. If they ever agree, the A/B
    // in `bench/confirm.csv` is measuring nothing and the artifact is void.

    fn pool_with_growth(growth: PoolGrowth) -> Pool {
        Pool::new(Arc::new(ProxyStats::default()), 4).with_tuning(crate::conn::Tuning {
            growth,
            ..crate::conn::Tuning::default()
        })
    }

    #[tokio::test]
    async fn eager_growth_gives_a_coping_connection_a_sibling() {
        // The same situation as
        // `a_full_connection_that_is_keeping_up_does_not_get_a_sibling`: full at
        // the backend's limit, and draining. The default leaves it alone; the
        // policy it replaced opens a socket, because the only question it asks
        // is whether the connection is full.
        let pool = pool_with_growth(PoolGrowth::Eager);
        let first = pool.checkout(&backend(4)).expect("a lease");
        first.record.set_max_concurrent(1);
        first.record.set_queued(0);

        let _second = pool.checkout(&backend(4)).expect("a second lease");
        assert_eq!(
            pool.connection_count(),
            2,
            "eager growth treats MAX_CONCURRENT_STREAMS as a throughput signal, \
             which is the whole defect being measured",
        );
    }

    #[tokio::test]
    async fn queue_growth_and_eager_growth_disagree_on_the_same_connection() {
        // The arms stated side by side, so that a change to either is a change
        // to a failing test rather than to a silently smaller effect.
        let mut counts = Vec::new();
        for growth in [PoolGrowth::Queue, PoolGrowth::Eager] {
            let pool = pool_with_growth(growth);
            let first = pool.checkout(&backend(5)).expect("a lease");
            first.record.set_max_concurrent(1);
            first.record.set_queued(0);
            let _second = pool.checkout(&backend(5)).expect("a second lease");
            counts.push(pool.connection_count());
        }
        assert_eq!(
            counts,
            vec![1, 2],
            "queue growth must absorb where eager growth expands",
        );
    }

    #[test]
    fn the_default_growth_policy_is_queue() {
        // The one that matters for anything shipped: a stray
        // `H2PROXYD_POOL_GROWTH` in an environment file must be the only way to
        // get the old policy.
        assert_eq!(crate::conn::Tuning::default().growth, PoolGrowth::Queue);
    }
}
