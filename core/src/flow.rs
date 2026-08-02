//! Flow control: connection-level and per-stream windows (RFC 9113 §5.2).
//!
//! Owns window accounting for both levels simultaneously — every DATA payload
//! octet decrements a stream window *and* the connection window — plus
//! WINDOW_UPDATE bookkeeping and the retroactive `INITIAL_WINDOW_SIZE`
//! adjustment (windows are signed: a SETTINGS decrease can legally drive them
//! negative).
//!
//! Two directions, two types. [`Window`] is the *send* side: credit the peer
//! gave us, which we spend. [`RecvWindow`] is the *receive* side: credit we gave
//! the peer, which they spend and we replenish — and it deliberately separates
//! "the octets arrived" from "the octets were consumed", because that gap is
//! where backpressure lives (design doc §4.2). Week 6 bridges an upstream to a
//! client by simply not calling [`RecvWindow::release`] until the client drains;
//! no new code path, just a delayed call.
//!
//! **Bounded memory, stated as arithmetic.** We advertise a
//! [`STREAM_INITIAL_WINDOW`] of 256 KiB across up to 256 concurrent streams,
//! which is 64 MiB of *nominal* per-stream credit. Actual in-flight octets are
//! capped by [`CONNECTION_WINDOW`] at 1 MiB regardless of how many streams are
//! open, because every DATA octet debits both levels. The connection window is
//! the memory bound; the stream windows only decide how that budget is shared.
//!
//! The off-by-one hazards here only surface under load, so `flow_control.rs`
//! asserts the blocking behavior end-to-end and `stream_proptest.rs` states the
//! invariant directly: we never emit more octets than the peer has credited.

use crate::conn::ErrorCode;

/// The default and maximum flow-control window (RFC 9113 §6.9.1). The initial
/// window is 65,535 octets until raised by SETTINGS; a window may never exceed
/// `2^31 - 1`.
pub const DEFAULT_INITIAL_WINDOW_SIZE: i32 = 65_535;
pub const MAX_WINDOW_SIZE: i32 = i32::MAX;

/// The connection-level receive window we run with.
///
/// **This cannot be advertised in SETTINGS.** `INITIAL_WINDOW_SIZE` applies only
/// to stream windows; the connection window starts at
/// [`DEFAULT_INITIAL_WINDOW_SIZE`] and moves only via WINDOW_UPDATE (§6.9.1), so
/// the handshake has to send an explicit stream-0 increment of
/// [`CONNECTION_WINDOW_BOOTSTRAP`]. Skipping that is a silent throughput cap
/// rather than a visible failure, which is why `conn.rs` has a test for it.
pub const CONNECTION_WINDOW: i32 = 1024 * 1024;

/// The stream-level receive window we advertise as `INITIAL_WINDOW_SIZE`.
pub const STREAM_INITIAL_WINDOW: i32 = 256 * 1024;

/// The stream-0 WINDOW_UPDATE that lifts the connection window from the
/// protocol default up to [`CONNECTION_WINDOW`], sent once at handshake.
pub const CONNECTION_WINDOW_BOOTSTRAP: u32 =
    (CONNECTION_WINDOW - DEFAULT_INITIAL_WINDOW_SIZE) as u32;

/// The largest run of octets one stream may send before the scheduler rotates
/// to the next (design doc §4.1). One `DEFAULT_MAX_FRAME_SIZE`, so a visit
/// produces at most one DATA frame and a 10 MiB response cannot starve a 1 KiB
/// one.
pub const SEND_BUDGET: usize = 16 * 1024;

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

    /// Octets available to send, floored at zero — what a scheduler wants when
    /// asking "how much may I write right now?".
    pub const fn sendable(self) -> usize {
        if self.0 > 0 { self.0 as usize } else { 0 }
    }

    /// Deduct `len` octets when sending DATA. Errors with `FLOW_CONTROL_ERROR`
    /// if `len` exceeds the available window.
    ///
    /// A negative window makes *every* debit an error, which is exactly the
    /// post-SETTINGS-decrease rule: nothing may be sent until credit arrives.
    pub fn consume(&mut self, len: u32) -> Result<(), ErrorCode> {
        // `len` is at most a frame length (24 bits on the wire), so the cast is
        // lossless; compare before subtracting so we never build the overflow.
        let len = i64::from(len);
        if len > i64::from(self.0) {
            return Err(ErrorCode::FlowControlError);
        }
        self.0 -= len as i32;
        Ok(())
    }

    /// Credit the window by a WINDOW_UPDATE increment, rejecting an increment
    /// that would push it past [`MAX_WINDOW_SIZE`] with `FLOW_CONTROL_ERROR`
    /// (§6.9.1).
    pub fn increase(&mut self, increment: u32) -> Result<(), ErrorCode> {
        let next = i64::from(self.0) + i64::from(increment);
        if next > i64::from(MAX_WINDOW_SIZE) {
            return Err(ErrorCode::FlowControlError);
        }
        self.0 = next as i32;
        Ok(())
    }

    /// Apply the retroactive `INITIAL_WINDOW_SIZE` change from a SETTINGS frame
    /// (§6.9.2): every open stream's window moves by the delta between the old
    /// and new value.
    ///
    /// A decrease may legally drive the window negative — that is the whole
    /// reason it is signed — but an *increase* past `2^31 - 1` is a
    /// `FLOW_CONTROL_ERROR` the peer has to answer for.
    pub fn apply_delta(&mut self, delta: i32) -> Result<(), ErrorCode> {
        let next = i64::from(self.0) + i64::from(delta);
        if next > i64::from(MAX_WINDOW_SIZE) {
            return Err(ErrorCode::FlowControlError);
        }
        // A decrease can go arbitrarily negative in principle, but only by the
        // delta of two legal window sizes, so it stays inside i32.
        self.0 = next.max(i64::from(i32::MIN)) as i32;
        Ok(())
    }
}

/// The receive half of a window: credit we issued, spent by the peer.
///
/// Arrival and consumption are tracked separately. `record` runs the moment
/// DATA is decoded — the octets are on the wire whether we wanted them or not.
/// `release` runs when the octets have actually been handed onward, and only
/// then does a WINDOW_UPDATE become due. Holding a `release` back *is* the
/// backpressure mechanism (§4.2), so the two must not be collapsed into one
/// call however tempting it looks while the responder is synchronous.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecvWindow {
    /// Credit outstanding with the peer.
    window: Window,
    /// Octets consumed but not yet announced back as a WINDOW_UPDATE.
    unreleased: u32,
    /// The window size we replenish back up to.
    initial: i32,
}

impl RecvWindow {
    pub const fn new(initial: i32) -> Self {
        RecvWindow {
            window: Window::new(initial),
            unreleased: 0,
            initial,
        }
    }

    /// Credit still outstanding with the peer.
    pub const fn available(&self) -> i32 {
        self.window.available()
    }

    /// Octets consumed but not yet announced.
    pub const fn unreleased(&self) -> u32 {
        self.unreleased
    }

    /// Account for `len` octets of DATA arriving. `FLOW_CONTROL_ERROR` if the
    /// peer spent credit it did not have — the one flow-control violation we
    /// detect rather than commit.
    pub fn record(&mut self, len: u32) -> Result<(), ErrorCode> {
        self.window.consume(len)
    }

    /// Note that `len` octets have been consumed, returning the WINDOW_UPDATE
    /// increment to send if enough has accumulated to be worth a frame.
    ///
    /// The threshold is half the window: replenishing every octet would put a
    /// WINDOW_UPDATE on the wire per DATA frame, and waiting for the window to
    /// empty entirely would stall the peer for a round trip. Half keeps credit
    /// in flight while the peer is still spending it.
    pub fn release(&mut self, len: u32) -> Option<u32> {
        self.unreleased = self.unreleased.saturating_add(len);
        if i64::from(self.unreleased) * 2 < i64::from(self.initial) {
            return None;
        }
        let increment = std::mem::take(&mut self.unreleased);
        // Restoring credit we previously issued can never exceed the ceiling.
        debug_assert!(self.window.increase(increment).is_ok());
        Some(increment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- the send window ---------------------------------------------------

    #[test]
    fn consuming_spends_available_credit() {
        let mut w = Window::new(100);
        w.consume(40).expect("inside the window");
        assert_eq!(w.available(), 60);
        w.consume(60).expect("exactly the window");
        assert_eq!(w.available(), 0);
        assert_eq!(w.sendable(), 0);
    }

    #[test]
    fn consuming_past_the_window_is_a_flow_control_error() {
        let mut w = Window::new(10);
        assert_eq!(w.consume(11), Err(ErrorCode::FlowControlError));
        // The failed debit must not have moved the window.
        assert_eq!(w.available(), 10);
    }

    #[test]
    fn a_negative_window_blocks_every_send() {
        let mut w = Window::new(100);
        w.apply_delta(-150).expect("a decrease is always legal");
        assert_eq!(w.available(), -50);
        assert_eq!(w.sendable(), 0);
        assert_eq!(w.consume(1), Err(ErrorCode::FlowControlError));

        // ...until a WINDOW_UPDATE lifts it back above zero.
        w.increase(60).expect("credit");
        assert_eq!(w.available(), 10);
        w.consume(10).expect("sendable again");
    }

    #[test]
    fn crediting_past_two_to_the_thirty_first_is_a_flow_control_error() {
        let mut w = Window::new(MAX_WINDOW_SIZE - 1);
        w.increase(1).expect("exactly the ceiling is legal");
        assert_eq!(w.available(), MAX_WINDOW_SIZE);
        assert_eq!(w.increase(1), Err(ErrorCode::FlowControlError));
        assert_eq!(w.available(), MAX_WINDOW_SIZE);
    }

    #[test]
    fn a_settings_increase_past_the_ceiling_is_rejected() {
        let mut w = Window::new(MAX_WINDOW_SIZE);
        assert_eq!(w.apply_delta(1), Err(ErrorCode::FlowControlError));
    }

    #[test]
    fn window_arithmetic_never_wraps() {
        // The u32 increment space is wider than the i32 window, so the naive
        // `self.0 += increment as i32` would wrap here rather than error.
        let mut w = Window::new(0);
        assert_eq!(w.increase(u32::MAX), Err(ErrorCode::FlowControlError));
        assert_eq!(w.available(), 0);
    }

    // ---- the receive window ------------------------------------------------

    #[test]
    fn recording_spends_the_credit_we_issued() {
        let mut r = RecvWindow::new(1000);
        r.record(400).expect("inside the window");
        assert_eq!(r.available(), 600);
    }

    #[test]
    fn a_peer_overspending_its_credit_is_a_flow_control_error() {
        let mut r = RecvWindow::new(100);
        assert_eq!(r.record(101), Err(ErrorCode::FlowControlError));
    }

    #[test]
    fn no_window_update_is_due_below_half_the_window() {
        let mut r = RecvWindow::new(1000);
        r.record(499).expect("credit");
        assert_eq!(r.release(499), None);
        assert_eq!(r.unreleased(), 499);
    }

    #[test]
    fn exactly_one_window_update_is_due_at_half_the_window() {
        let mut r = RecvWindow::new(1000);
        r.record(500).expect("credit");
        assert_eq!(r.release(200), None);
        assert_eq!(r.release(300), Some(500));
        // Announcing it restores the credit and resets the accumulator.
        assert_eq!(r.available(), 1000);
        assert_eq!(r.unreleased(), 0);
        assert_eq!(r.release(1), None);
    }

    #[test]
    fn withholding_release_withholds_the_credit() {
        // The week-6 backpressure hook, as an assertion: data that arrived but
        // was never released leaves the peer's credit spent.
        let mut r = RecvWindow::new(1000);
        r.record(1000).expect("the peer spends its whole window");
        assert_eq!(r.available(), 0);
        // No `release` call, so no increment and no restored credit.
        assert_eq!(r.unreleased(), 0);
        assert_eq!(r.available(), 0);
    }

    #[test]
    fn the_bootstrap_increment_lifts_the_default_to_the_connection_window() {
        let mut w = Window::new(DEFAULT_INITIAL_WINDOW_SIZE);
        w.increase(CONNECTION_WINDOW_BOOTSTRAP).expect("legal");
        assert_eq!(w.available(), CONNECTION_WINDOW);
    }
}
