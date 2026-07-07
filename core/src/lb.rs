//! Load balancing across upstream backends (design doc §5.1, §5.2).
//!
//! Owns the `LoadBalancer` trait — the seam that keeps balancing strategy
//! swappable and unit-testable — and its first implementation:
//! least-outstanding-streams via power-of-two-choices (sample two backends,
//! pick the less loaded; near-optimal balance without global coordination).
//!
//! Week 7 adds the resilience half behind the same trait: active + passive
//! health checking with outlier ejection and probe-back, and conservative
//! idempotent-only retries (§5.3).
//!
//! Implemented in week 6.

use std::net::SocketAddr;

/// An upstream backend the load balancer can select. Identified by its address;
/// per-backend load and health state live inside the [`LoadBalancer`]
/// implementation, not here.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Backend {
    pub addr: SocketAddr,
}

impl Backend {
    pub const fn new(addr: SocketAddr) -> Self {
        Backend { addr }
    }
}

/// The load-balancing seam (design doc §5.1).
///
/// Keeping selection behind a trait lets the week-6 default
/// (least-outstanding-streams via power-of-two-choices) be swapped or unit-
/// tested in isolation, and lets week-7 health checking hide ejected backends
/// without the request path knowing.
pub trait LoadBalancer {
    /// Choose a backend from `candidates` for a new request, or `None` if none
    /// are currently eligible (e.g. all ejected by health checking, §5.2).
    fn pick(&self, candidates: &[Backend]) -> Option<Backend>;
}
