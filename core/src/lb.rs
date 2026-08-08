//! Load balancing across upstream backends (design doc §5.1, §5.2).
//!
//! Owns the [`LoadBalancer`] trait — the seam that keeps balancing strategy
//! swappable and unit-testable — and its default implementation:
//! least-outstanding-streams via **power-of-two-choices**. Sample two backends
//! at random, send the request to whichever has fewer streams in flight.
//!
//! The reason that beats both round-robin and true least-loaded is worth
//! stating, because it is the whole argument for the strategy: round-robin is
//! blind to a backend that has become slow, and true least-loaded requires
//! comparing every backend on every request (and, across several proxies, makes
//! them all pile onto the same "least loaded" instance at the same instant).
//! Two random samples land within a constant factor of optimal while looking at
//! two counters.
//!
//! The resilience half lives beside it rather than inside it: [`crate::health`]
//! filters the candidate list before `pick` ever sees it, so eligibility and
//! balancing stay separable and this stays a pure function of its input.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

/// An upstream backend the load balancer can select. Identified by its address;
/// per-backend load and health state live inside the pool, not here.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Backend {
    pub addr: SocketAddr,
}

impl Backend {
    pub const fn new(addr: SocketAddr) -> Self {
        Backend { addr }
    }
}

/// A backend and how busy it is right now.
///
/// Load is passed in rather than looked up through a trait method so the
/// balancer stays a pure function of its input — which is what makes the
/// distribution testable without a pool, a socket, or a backend.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BackendLoad {
    pub backend: Backend,
    /// Streams currently in flight to this backend across every pooled
    /// connection.
    pub outstanding: usize,
}

/// The load-balancing seam (design doc §5.1).
pub trait LoadBalancer: Send + Sync + std::fmt::Debug {
    /// Choose a backend for a new request, or `None` if the candidate list is
    /// empty. Candidates have already been filtered by [`crate::health`]; note
    /// that it fails *open*, so "every backend is unhealthy" arrives here as the
    /// full list rather than as an empty one.
    fn pick(&self, candidates: &[BackendLoad]) -> Option<Backend>;
}

/// Least-outstanding-streams via power-of-two-choices.
///
/// The RNG is a xorshift over an atomic, which is all this needs: the choice
/// must be *spread*, not unpredictable, and pulling in a dependency to sample
/// two array indices would be poor value. Seeding it explicitly makes the
/// distribution tests deterministic.
#[derive(Debug)]
pub struct PowerOfTwoChoices {
    state: AtomicU64,
}

impl PowerOfTwoChoices {
    pub fn new() -> Self {
        PowerOfTwoChoices::seeded(0x2545_f491_4f6c_dd1d)
    }

    pub const fn seeded(seed: u64) -> Self {
        PowerOfTwoChoices {
            state: AtomicU64::new(seed),
        }
    }

    fn next(&self) -> u64 {
        // xorshift64, updated with a plain load/store pair: two threads racing
        // here get the same value at worst, which costs one slightly less-spread
        // choice and nothing else.
        let mut x = self.state.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state.store(x, Ordering::Relaxed);
        x
    }
}

impl Default for PowerOfTwoChoices {
    fn default() -> Self {
        PowerOfTwoChoices::new()
    }
}

impl LoadBalancer for PowerOfTwoChoices {
    fn pick(&self, candidates: &[BackendLoad]) -> Option<Backend> {
        match candidates.len() {
            0 => None,
            1 => Some(candidates[0].backend),
            n => {
                let first = (self.next() % n as u64) as usize;
                // Sample the *other* n-1 and shift, so the two draws are always
                // distinct. Drawing twice from n and hoping is the version that
                // silently degrades to "least of one" whenever they collide.
                let mut second = (self.next() % (n as u64 - 1)) as usize;
                if second >= first {
                    second += 1;
                }
                let a = candidates[first];
                let b = candidates[second];
                Some(if a.outstanding <= b.outstanding {
                    a.backend
                } else {
                    b.backend
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(port: u16) -> Backend {
        Backend::new(SocketAddr::from(([127, 0, 0, 1], port)))
    }

    fn load(port: u16, outstanding: usize) -> BackendLoad {
        BackendLoad {
            backend: backend(port),
            outstanding,
        }
    }

    #[test]
    fn no_candidates_means_no_pick() {
        assert_eq!(PowerOfTwoChoices::new().pick(&[]), None);
    }

    #[test]
    fn a_single_candidate_is_always_chosen() {
        let lb = PowerOfTwoChoices::new();
        assert_eq!(lb.pick(&[load(8080, 999)]), Some(backend(8080)));
    }

    #[test]
    fn the_two_samples_are_always_distinct() {
        // With two backends the pair is forced, so the *less loaded* one must
        // win every single time — this is the property that catches "sample
        // twice and hope", which would pick the busy one whenever both draws
        // collide on it.
        let lb = PowerOfTwoChoices::seeded(1);
        for _ in 0..200 {
            assert_eq!(
                lb.pick(&[load(8080, 10), load(8081, 0)]),
                Some(backend(8081)),
            );
        }
    }

    #[test]
    fn an_idle_backend_wins_most_of_the_time_in_a_crowd() {
        // The probabilistic half of the claim: with one idle backend among four
        // busy ones, two samples find it whenever either draw lands on it —
        // about 40% of the time, versus 20% for round-robin.
        let lb = PowerOfTwoChoices::seeded(99);
        let candidates = [
            load(8080, 50),
            load(8081, 50),
            load(8082, 0),
            load(8083, 50),
            load(8084, 50),
        ];
        let idle = (0..1000)
            .filter(|_| lb.pick(&candidates) == Some(backend(8082)))
            .count();
        assert!(
            (300..=500).contains(&idle),
            "expected the idle backend around 40% of the time, got {idle}/1000",
        );
    }

    #[test]
    fn equal_load_still_spreads() {
        // Nothing may collapse onto one backend when they are all the same.
        let lb = PowerOfTwoChoices::seeded(7);
        let candidates = [load(8080, 5), load(8081, 5), load(8082, 5)];
        let mut counts = [0usize; 3];
        for _ in 0..900 {
            let picked = lb.pick(&candidates).expect("a backend");
            let index = candidates
                .iter()
                .position(|c| c.backend == picked)
                .expect("one of ours");
            counts[index] += 1;
        }
        for count in counts {
            assert!(count > 150, "lopsided distribution: {counts:?}");
        }
    }
}
