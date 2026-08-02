//! h2proxy-core — a hand-built HTTP/2 protocol engine.
//!
//! Everything protocol-shaped lives in this library so the differential-test
//! harness can target the engine directly, without a socket or a daemon in the
//! way (ADR 0003). The binary crate (`h2proxyd`) owns TLS, sockets, and config.
//!
//! Module map (one module per engine concern; see each module's doc for its
//! contract and the design-doc / RFC sections it implements):
//!
//! - [`frame`] — bytes ⇄ typed frames (RFC 9113 §4, §6)
//! - [`hpack`] — stateful header compression (RFC 7541)
//! - [`stream`] — per-stream state machine (RFC 9113 §5.1)
//! - [`flow`] — connection + stream flow-control windows (RFC 9113 §5.2)
//! - [`conn`] — per-connection task: demux/mux, SETTINGS, errors (§2.2, §5.4)
//! - [`service`] — what answers a stream, and §8.3 request validation
//! - [`pool`] — warm upstream connections and coalescing (design doc §4.3)
//! - [`lb`] — load-balancer trait and strategies (design doc §5.1)
//! - [`proxy`] — the request path tying it together (design doc §2.1, §4)

pub mod conn;
pub mod flow;
pub mod frame;
pub mod hpack;
pub mod lb;
pub mod pool;
pub mod proxy;
pub mod service;
pub mod stream;

#[cfg(test)]
mod tests {
    /// Smoke test so `cargo nextest run` has something to execute from day one
    /// (nextest treats "zero tests found" as failure).
    #[test]
    fn workspace_builds_and_tests_run() {
        assert!(!env!("CARGO_PKG_VERSION").is_empty());
    }
}
