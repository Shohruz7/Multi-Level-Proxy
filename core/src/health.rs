//! Backend health, outlier ejection, and probe-back (design doc §5.2).
//!
//! The load balancer picks among backends; this decides which backends it is
//! allowed to see. Two sources of truth, deliberately:
//!
//! - **Passive.** Every real request is already a health check. Consecutive
//!   failures eject a backend; any success clears the count. This costs nothing
//!   and reacts at the speed of traffic.
//! - **Active.** A backend can accept TCP and then answer nothing — a passive
//!   check on an idle pool never notices, because no request is being sent to
//!   notice with. A PING on an idle pooled connection catches it. The probe is a
//!   protocol frame, so there is no `/healthz` path to agree on with every
//!   backend and no contract to keep in sync.
//!
//! Like [`crate::guard`], this module is pure and clock-injected: every method
//! takes `now`, and there is no timer and no I/O in the file. The connection
//! task owns the clock; this owns the policy.
//!
//! # Probe-back is one request, not a share of traffic
//!
//! When a backend's ejection expires it becomes *half-open*: exactly one
//! in-flight trial request is admitted, and its outcome decides. Letting the
//! full share resume the instant a timer fires is what makes health checking
//! flap — a backend that is still sick gets a burst of traffic, fails it, and is
//! re-ejected, so the cycle repeats forever at the period of the backoff and
//! every cycle costs real requests. One trial costs one request.
//!
//! # Fail open
//!
//! If every backend is ejected, [`Health::eligible`] returns them **all** rather
//! than none. This is a deliberate deviation from "no eligible backend → 503".
//!
//! The reasoning is that ejection is a *guess* — five consecutive failures might
//! mean five broken backends, or it might mean one broken dependency they share,
//! a bad deploy of our own, or a threshold that is simply too tight. If the
//! guess is wrong, refusing everything converts a partial outage into a total
//! one, caused by us. Sending traffic to backends that were probably failing is
//! the strictly better error: at worst it fails the same way it was already
//! failing, and at best it discovers we were wrong. Envoy calls the same idea a
//! panic threshold.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, info};

use crate::lb::{Backend, BackendLoad};

/// When a backend is ejected and how it comes back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Policy {
    /// Consecutive failures before a backend leaves rotation.
    ///
    /// Low enough that a dead backend is out in well under a second at load;
    /// high enough that one blip, or a single 502 from a backend that is
    /// otherwise fine, cannot eject it. Consecutive rather than a ratio because
    /// a ratio needs a window, and a window needs its own tuning.
    pub eject_after: u32,
    /// How long the first ejection lasts. Long enough for a restarting backend
    /// to finish booting, short enough that recovery is not user-visible.
    pub base_backoff: Duration,
    /// The ceiling on repeated ejections. Doubling stops a backend that is still
    /// sick from being retried every `base_backoff` forever.
    pub max_backoff: Duration,
    /// How long a pooled connection may sit idle before it is worth proving it
    /// is still alive.
    pub ping_idle: Duration,
    /// How long to wait for the PING ACK before calling the connection dead.
    /// Together with `ping_idle` this bounds detection of a black-holed backend
    /// at about the sum of the two.
    pub ping_timeout: Duration,
    /// How long a pooled connection may sit unused before it is recycled.
    ///
    /// Under the common 65 s upstream keep-alive, so *we* close first: racing a
    /// backend's own idle close means occasionally handing a request to a socket
    /// that is already going away.
    pub idle_timeout: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            eject_after: 5,
            base_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(30),
            ping_idle: Duration::from_secs(5),
            ping_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

impl Policy {
    /// Never eject. For tests about something else.
    pub fn permissive() -> Self {
        Policy {
            eject_after: u32::MAX,
            ..Policy::default()
        }
    }
}

/// What a backend is currently doing, from our point of view.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    /// In rotation.
    Healthy,
    /// Out of rotation until an instant passes.
    Ejected,
    /// Ejection expired; one trial request is allowed through.
    HalfOpen,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    consecutive_failures: u32,
    /// `Some` while ejected; the instant the ejection expires.
    ejected_until: Option<Instant>,
    /// Whether the single trial request is currently out.
    trial_in_flight: bool,
    /// The backoff the *next* ejection will use.
    backoff: Duration,
    ejections: u64,
}

impl Entry {
    fn new(policy: &Policy) -> Self {
        Entry {
            consecutive_failures: 0,
            ejected_until: None,
            trial_in_flight: false,
            backoff: policy.base_backoff,
            ejections: 0,
        }
    }

    fn state(&self, now: Instant) -> State {
        match self.ejected_until {
            None => State::Healthy,
            Some(until) if now < until => State::Ejected,
            Some(_) => State::HalfOpen,
        }
    }
}

/// Health state for every backend the proxy knows about.
#[derive(Debug)]
pub struct Health {
    backends: Mutex<HashMap<Backend, Entry>>,
    policy: Policy,
    ejections: std::sync::atomic::AtomicU64,
}

impl Health {
    pub fn new(policy: Policy) -> Self {
        Health {
            backends: Mutex::new(HashMap::new()),
            policy,
            ejections: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub const fn policy(&self) -> &Policy {
        &self.policy
    }

    /// A request to `backend` failed in a way that says something about the
    /// backend — a connect failure, a dead connection, a malformed response.
    ///
    /// Deliberately *not* called for a stream the backend reset on its own: a
    /// RST_STREAM is a considered answer about one request, not evidence that
    /// the backend is unwell, and counting it would let one pathological client
    /// eject a healthy backend for everybody.
    pub fn failure(&self, backend: &Backend, now: Instant) {
        let mut backends = self.backends.lock().expect("health mutex poisoned");
        let entry = backends
            .entry(*backend)
            .or_insert_with(|| Entry::new(&self.policy));

        // A failed trial re-ejects immediately, with a longer backoff: the
        // backend has now failed twice over, and the whole point of the single
        // trial is that its verdict is acted on rather than averaged.
        if entry.trial_in_flight {
            entry.trial_in_flight = false;
            entry.backoff = (entry.backoff * 2).min(self.policy.max_backoff);
            entry.ejected_until = Some(now + entry.backoff);
            entry.ejections += 1;
            self.ejections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(
                backend = %backend.addr,
                backoff_s = entry.backoff.as_secs_f64(),
                "probe request failed; re-ejecting",
            );
            return;
        }

        entry.consecutive_failures += 1;
        if entry.consecutive_failures >= self.policy.eject_after && entry.ejected_until.is_none() {
            entry.ejected_until = Some(now + entry.backoff);
            entry.ejections += 1;
            self.ejections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(
                backend = %backend.addr,
                failures = entry.consecutive_failures,
                backoff_s = entry.backoff.as_secs_f64(),
                "ejecting backend",
            );
        }
    }

    /// A request to `backend` succeeded. Clears the failure run and, if this was
    /// the trial, restores the backend to rotation.
    pub fn success(&self, backend: &Backend) {
        let mut backends = self.backends.lock().expect("health mutex poisoned");
        let entry = backends
            .entry(*backend)
            .or_insert_with(|| Entry::new(&self.policy));
        if entry.ejected_until.is_some() {
            info!(backend = %backend.addr, "backend recovered; restoring to rotation");
        }
        entry.consecutive_failures = 0;
        entry.ejected_until = None;
        entry.trial_in_flight = false;
        entry.backoff = self.policy.base_backoff;
    }

    /// Filter `candidates` down to the backends worth sending a request to.
    ///
    /// Admits every healthy backend, plus at most one half-open backend carrying
    /// its single trial. **If that leaves nothing, returns everything** — see the
    /// module doc on failing open.
    /// **Read-only.** Offering a backend is not the same as sending to it — the
    /// balancer still gets to choose — so the single trial is claimed in
    /// [`Health::claim_trial`], after a backend has actually been picked.
    /// Marking it here instead looks equivalent and is not: when the balancer
    /// chose the *other* backend, the flag was set with no probe behind it and
    /// nothing could ever clear it, so the recovering backend stayed half-open
    /// and out of rotation permanently.
    pub fn eligible(&self, candidates: &[BackendLoad], now: Instant) -> Vec<BackendLoad> {
        let backends = self.backends.lock().expect("health mutex poisoned");
        let eligible: Vec<BackendLoad> = candidates
            .iter()
            .copied()
            .filter(|candidate| match backends.get(&candidate.backend) {
                None => true,
                Some(entry) => match entry.state(now) {
                    State::Healthy => true,
                    State::Ejected => false,
                    // Offer it only while no trial is outstanding.
                    State::HalfOpen => !entry.trial_in_flight,
                },
            })
            .collect();

        if eligible.is_empty() && !candidates.is_empty() {
            debug!(
                candidates = candidates.len(),
                "every backend is ejected; failing open rather than refusing traffic",
            );
            return candidates.to_vec();
        }
        eligible
    }

    /// Claim the single probe slot, if `backend` is half-open and free.
    ///
    /// Called once the balancer has actually chosen, so the flag always has a
    /// real request behind it. Two dispatches racing here can produce a second
    /// probe, which is harmless — they are ordinary requests that would have
    /// gone somewhere else — and far better than the alternative of a flag with
    /// no request to clear it.
    pub fn claim_trial(&self, backend: &Backend, now: Instant) {
        let mut backends = self.backends.lock().expect("health mutex poisoned");
        let Some(entry) = backends.get_mut(backend) else {
            return;
        };
        if entry.state(now) == State::HalfOpen && !entry.trial_in_flight {
            entry.trial_in_flight = true;
            debug!(backend = %backend.addr, "probing a recovering backend");
        }
    }

    /// How many backends are currently in rotation, for the gauge (§7).
    pub fn healthy_count(&self, candidates: &[Backend], now: Instant) -> usize {
        let backends = self.backends.lock().expect("health mutex poisoned");
        candidates
            .iter()
            .filter(|backend| {
                backends
                    .get(*backend)
                    .is_none_or(|entry| entry.state(now) == State::Healthy)
            })
            .count()
    }

    /// Total ejections since start, for the counter (§7).
    pub fn ejections(&self) -> u64 {
        self.ejections.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// This backend's state right now. Test and diagnostics only.
    pub fn state(&self, backend: &Backend, now: Instant) -> State {
        self.backends
            .lock()
            .expect("health mutex poisoned")
            .get(backend)
            .map_or(State::Healthy, |entry| entry.state(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(port: u16) -> Backend {
        Backend::new(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
    }

    fn load(b: Backend) -> BackendLoad {
        BackendLoad {
            backend: b,
            outstanding: 0,
        }
    }

    fn policy() -> Policy {
        Policy {
            eject_after: 3,
            base_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(40),
            ..Policy::default()
        }
    }

    #[test]
    fn consecutive_failures_eject_and_a_success_clears_the_run() {
        let h = Health::new(policy());
        let now = Instant::now();
        let b = backend(1);

        h.failure(&b, now);
        h.failure(&b, now);
        assert_eq!(
            h.state(&b, now),
            State::Healthy,
            "two is under the threshold"
        );
        // A success in the middle means the failures were not consecutive, which
        // is the whole point of the word.
        h.success(&b);
        h.failure(&b, now);
        h.failure(&b, now);
        assert_eq!(h.state(&b, now), State::Healthy);
        h.failure(&b, now);
        assert_eq!(h.state(&b, now), State::Ejected);
    }

    #[test]
    fn an_ejected_backend_is_not_offered_to_the_balancer() {
        let h = Health::new(policy());
        let now = Instant::now();
        let (a, b) = (backend(1), backend(2));
        for _ in 0..3 {
            h.failure(&a, now);
        }
        let eligible = h.eligible(&[load(a), load(b)], now);
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].backend, b);
    }

    #[test]
    fn exactly_one_trial_request_probes_a_recovering_backend() {
        // The anti-flap property. When the ejection expires the backend must not
        // get its full share back at once — if it is still sick, that is a burst
        // of failed requests every backoff period, forever.
        let h = Health::new(policy());
        let start = Instant::now();
        let a = backend(1);
        let b = backend(2);
        for _ in 0..3 {
            h.failure(&a, start);
        }
        let later = start + Duration::from_secs(11);
        assert_eq!(h.state(&a, later), State::HalfOpen);

        // Offered once expiry passes...
        let first = h.eligible(&[load(a), load(b)], later);
        assert!(first.iter().any(|c| c.backend == a), "the trial is offered");
        // ...and once the balancer actually chooses it, claimed...
        h.claim_trial(&a, later);
        // ...after which it is offered to nobody else until the outcome lands.
        for _ in 0..50 {
            let next = h.eligible(&[load(a), load(b)], later);
            assert!(
                !next.iter().any(|c| c.backend == a),
                "a second request reached a backend with a trial still in flight",
            );
        }
    }

    #[test]
    fn offering_a_trial_that_the_balancer_declines_does_not_strand_the_backend() {
        // The bug an integration test caught. `eligible` used to claim the trial
        // itself, but offering a backend is not sending to it: when the balancer
        // picked the *other* candidate, the flag was set with no request behind
        // it, and nothing could ever clear it. The backend stayed half-open and
        // out of rotation for good — a recovered backend permanently ejected by
        // its own recovery path.
        let h = Health::new(policy());
        let start = Instant::now();
        let (a, b) = (backend(1), backend(2));
        for _ in 0..3 {
            h.failure(&a, start);
        }
        let later = start + Duration::from_secs(11);

        // Offer repeatedly, but always "choose" the other backend.
        for _ in 0..20 {
            let offered = h.eligible(&[load(a), load(b)], later);
            assert!(
                offered.iter().any(|c| c.backend == a),
                "a half-open backend nobody has probed must keep being offered",
            );
            h.claim_trial(&b, later);
        }

        // Now actually choose it, and let it succeed.
        h.claim_trial(&a, later);
        h.success(&a);
        assert_eq!(h.state(&a, later), State::Healthy);
    }

    #[test]
    fn a_failed_trial_re_ejects_with_a_longer_backoff() {
        let h = Health::new(policy());
        let start = Instant::now();
        let a = backend(1);
        for _ in 0..3 {
            h.failure(&a, start);
        }
        let t1 = start + Duration::from_secs(11);
        h.claim_trial(&a, t1);
        h.failure(&a, t1);

        // Still ejected 10 s later, because the backoff doubled to 20 s.
        assert_eq!(h.state(&a, t1 + Duration::from_secs(15)), State::Ejected);
        assert_eq!(h.state(&a, t1 + Duration::from_secs(21)), State::HalfOpen);
    }

    #[test]
    fn a_successful_trial_restores_the_backend_fully() {
        let h = Health::new(policy());
        let start = Instant::now();
        let a = backend(1);
        for _ in 0..3 {
            h.failure(&a, start);
        }
        let t1 = start + Duration::from_secs(11);
        h.claim_trial(&a, t1);
        h.success(&a);
        assert_eq!(h.state(&a, t1), State::Healthy);
        // ...and the backoff resets, so an unrelated failure later starts over.
        for _ in 0..3 {
            h.failure(&a, t1);
        }
        assert_eq!(h.state(&a, t1 + Duration::from_secs(11)), State::HalfOpen);
    }

    #[test]
    fn the_backoff_stops_doubling_at_its_ceiling() {
        let h = Health::new(policy());
        let mut now = Instant::now();
        let a = backend(1);
        for _ in 0..3 {
            h.failure(&a, now);
        }
        // Fail ten trials in a row; the backoff must not run away.
        for _ in 0..10 {
            now += Duration::from_secs(60);
            h.claim_trial(&a, now);
            h.failure(&a, now);
        }
        assert_eq!(
            h.state(&a, now + Duration::from_secs(41)),
            State::HalfOpen,
            "backoff exceeded max_backoff; a recovered backend would stay out",
        );
    }

    #[test]
    fn everything_ejected_fails_open_rather_than_refusing_traffic() {
        // The deviation from the spec, and the one worth defending: ejection is
        // a guess, and a wrong guess that refuses all traffic turns a partial
        // outage into a total one that we caused.
        let h = Health::new(policy());
        let now = Instant::now();
        let (a, b) = (backend(1), backend(2));
        for _ in 0..3 {
            h.failure(&a, now);
            h.failure(&b, now);
        }
        let eligible = h.eligible(&[load(a), load(b)], now);
        assert_eq!(
            eligible.len(),
            2,
            "with every backend ejected the proxy must still try one, not 503 \
             everything on the strength of its own heuristic",
        );
    }

    #[test]
    fn a_healthy_backend_is_never_ejected_however_long_it_runs() {
        let h = Health::new(policy());
        let mut now = Instant::now();
        let a = backend(1);
        for _ in 0..10_000 {
            now += Duration::from_millis(1);
            h.success(&a);
            assert_eq!(h.state(&a, now), State::Healthy);
        }
    }

    #[test]
    fn an_intermittent_backend_does_not_oscillate_into_permanent_ejection() {
        // A backend failing half its requests is unwell but usable, and the
        // common wrong behaviour is to eject it, restore it, eject it again in a
        // cycle that costs a burst of errors each time. Alternating outcomes
        // never reach `eject_after` consecutive failures, so it stays in.
        let h = Health::new(policy());
        let mut now = Instant::now();
        let a = backend(1);
        for i in 0..1000 {
            now += Duration::from_millis(10);
            if i % 2 == 0 {
                h.failure(&a, now);
            } else {
                h.success(&a);
            }
        }
        assert_eq!(h.state(&a, now), State::Healthy);
        assert_eq!(h.ejections(), 0);
    }

    #[test]
    fn healthy_count_tracks_what_the_gauge_should_show() {
        let h = Health::new(policy());
        let now = Instant::now();
        let (a, b, c) = (backend(1), backend(2), backend(3));
        assert_eq!(h.healthy_count(&[a, b, c], now), 3, "unknown means healthy");
        for _ in 0..3 {
            h.failure(&a, now);
        }
        assert_eq!(h.healthy_count(&[a, b, c], now), 2);
        assert_eq!(h.ejections(), 1);
    }
}
