//! Flow control: connection-level and per-stream windows (RFC 9113 §5.2).
//!
//! Owns window accounting for both levels simultaneously — every DATA payload
//! octet decrements a stream window *and* the connection window — plus
//! WINDOW_UPDATE bookkeeping and the retroactive `INITIAL_WINDOW_SIZE`
//! adjustment (windows are signed: a SETTINGS decrease can legally drive them
//! negative).
//!
//! This module is the mechanism behind backpressure bridging (design doc
//! §4.2): the proxy withholds the upstream's WINDOW_UPDATE until bytes drain
//! to the client, so the client's window transitively governs the upstream's
//! send rate with bounded proxy memory.
//!
//! Implemented in week 5; the off-by-one hazards only surface under load, so
//! the week-6 slow-client test asserts flat memory, not just green units.

use crate::conn::ErrorCode;

/// The default and maximum flow-control window (RFC 9113 §6.9.1). The initial
/// window is 65,535 octets until raised by SETTINGS; a window may never exceed
/// `2^31 - 1`.
pub const DEFAULT_INITIAL_WINDOW_SIZE: i32 = 65_535;
pub const MAX_WINDOW_SIZE: i32 = i32::MAX;

/// A single flow-control window (RFC 9113 §6.9).
///
/// Signed on purpose: lowering `INITIAL_WINDOW_SIZE` via SETTINGS retroactively
/// shrinks every open stream's window and can drive it negative, at which point
/// the sender must stop until a WINDOW_UPDATE brings it back above zero.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Window(i32);

impl Window {
    /// A window seeded to `initial` available octets.
    pub const fn new(initial: i32) -> Self {
        Window(initial)
    }

    /// Octets currently available to send (may be negative after a SETTINGS
    /// decrease).
    pub const fn available(self) -> i32 {
        self.0
    }

    /// Deduct `len` octets when sending DATA. Errors with `FLOW_CONTROL_ERROR`
    /// if `len` exceeds the available window. Implemented in week 5.
    pub fn consume(&mut self, len: u32) -> Result<(), ErrorCode> {
        let _ = len;
        todo!("flow-control debit — week 5")
    }

    /// Credit the window by a WINDOW_UPDATE increment, rejecting an increment
    /// that would push it past `MAX_WINDOW_SIZE` with `FLOW_CONTROL_ERROR`.
    /// Implemented in week 5.
    pub fn increase(&mut self, increment: u32) -> Result<(), ErrorCode> {
        let _ = increment;
        todo!("flow-control credit — week 5")
    }
}
