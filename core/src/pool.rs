//! Upstream connection pool and coalescing (design doc §4.3).
//!
//! Maintains warm HTTP/2 connections per backend, opens on demand up to the
//! backend's advertised `MAX_CONCURRENT_STREAMS`, and recycles connections on
//! idle timeout or stream-ID exhaustion. Owns the bidirectional stream-ID
//! remapping per client/upstream connection pair — client stream N and its
//! upstream stream M are independent IDs joined only by the proxy's map.
//!
//! Coalescing is the project thesis made concrete: many client streams ride
//! few upstream connections, so the pool's utilization gauges (§7) are the
//! first thing to look at in any benchmark.
//!
//! Implemented in week 6.
