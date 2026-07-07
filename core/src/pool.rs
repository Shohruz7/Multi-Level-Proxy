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

use crate::lb::Backend;

/// Why a pool checkout failed.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum PoolError {
    #[error("backend {0:?} is unreachable")]
    Unreachable(Backend),
    #[error("backend {0:?} is at its concurrent-stream limit")]
    Exhausted(Backend),
}

/// The upstream-pool seam (design doc §4.3).
///
/// A checkout leases a connection to a backend on which the proxy can open a
/// new upstream stream — opening a fresh connection on demand up to the
/// backend's `MAX_CONCURRENT_STREAMS`, otherwise coalescing onto a warm one.
/// The concrete leased-connection type is defined with the week-6
/// implementation, hence the associated type.
pub trait ConnectionPool {
    /// A handle to a leased upstream connection.
    type Conn;

    /// Lease a connection to `backend`.
    fn checkout(&self, backend: &Backend) -> Result<Self::Conn, PoolError>;

    /// Return a leased connection to the pool for reuse.
    fn checkin(&self, conn: Self::Conn);
}
