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
