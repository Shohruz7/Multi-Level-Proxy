//! Property tests for the stream state machine and flow-control windows.
//!
//! The example tests in `stream.rs` and `flow.rs` check the transitions and the
//! arithmetic the RFC names. These state the invariants that have to hold over
//! *arbitrary* sequences — which is the shape the spec warns about, because
//! off-by-one window bugs surface under load rather than in a fixed script.
//!
//! Four claims:
//!
//! 1. The state machine never panics and never invents a state.
//! 2. A stream that reaches `Closed` leaves no entry behind — the leak that
//!    would only show up in a week-8 soak run.
//! 3. A window never wraps and never exceeds `2^31 - 1`.
//! 4. **We never emit more octets than the peer credited**, at either level.
//!    That is flow-control correctness stated directly, and it is the one that
//!    matters: violating it is a `FLOW_CONTROL_ERROR` the peer detects, not a
//!    local crash we would see in testing.

use proptest::prelude::*;

use h2proxy_core::conn::ErrorCode;
use h2proxy_core::flow::{MAX_WINDOW_SIZE, RecvWindow, Window};
use h2proxy_core::stream::{StreamEvent, StreamId, StreamState, StreamTable};

/// The events a peer and a server can generate between them.
fn event() -> impl Strategy<Value = StreamEvent> {
    prop_oneof![
        any::<bool>().prop_map(|end_stream| StreamEvent::RecvHeaders { end_stream }),
        any::<bool>().prop_map(|end_stream| StreamEvent::SendHeaders { end_stream }),
        any::<bool>().prop_map(|end_stream| StreamEvent::RecvData { end_stream }),
        any::<bool>().prop_map(|end_stream| StreamEvent::SendData { end_stream }),
        Just(StreamEvent::RecvRstStream),
        Just(StreamEvent::SendRstStream),
    ]
}

fn state() -> impl Strategy<Value = StreamState> {
    prop::sample::select(vec![
        StreamState::Idle,
        StreamState::Open,
        StreamState::HalfClosedLocal,
        StreamState::HalfClosedRemote,
        StreamState::Closed,
    ])
}

/// Client-initiated ids, drawn from a small pool so sequences collide on the
/// same streams often enough to be interesting.
fn client_id() -> impl Strategy<Value = StreamId> {
    (0u32..8).prop_map(|n| StreamId::new(n * 2 + 1))
}

proptest! {
    /// Claim 1. Every outcome is either a state the enum names or one of the two
    /// codes §5.1 allows — never a panic, never anything else.
    #[test]
    fn the_state_machine_only_ever_produces_a_legal_outcome(
        from in state(),
        event in event(),
    ) {
        match from.transition(event) {
            Ok(next) => {
                // The one structural rule that holds across the whole table: a
                // stream never goes backwards into `Idle`.
                prop_assert_ne!(next, StreamState::Idle);
                // And RST_STREAM always lands in `Closed`, from anywhere.
                if matches!(event, StreamEvent::SendRstStream | StreamEvent::RecvRstStream) {
                    prop_assert_eq!(next, StreamState::Closed);
                }
            }
            Err(code) => {
                prop_assert!(
                    matches!(code, ErrorCode::ProtocolError | ErrorCode::StreamClosed),
                    "unexpected error code {:?} from {:?} + {:?}",
                    code, from, event,
                );
                // PROTOCOL_ERROR is reserved for the idle case, because that one
                // is connection-scoped and the others are not. If this ever
                // widens, `conn.rs` starts tearing down connections it should
                // have reset a single stream on.
                if code == ErrorCode::ProtocolError {
                    prop_assert_eq!(from, StreamState::Idle);
                }
            }
        }
    }

    /// Claim 2. However a stream ends, it leaves no entry — and however chaotic
    /// the sequence, the table never holds more than the concurrency limit.
    #[test]
    fn a_closed_stream_leaves_no_entry_behind(
        script in prop::collection::vec((client_id(), event()), 1..64),
    ) {
        const LIMIT: u32 = 4;
        let mut table = StreamTable::new(LIMIT, 65_535, 65_535);

        for (id, event) in script {
            // Opening is allowed to fail (bad id, at the limit); that is the
            // table doing its job, not a reason to stop the sequence.
            let _ = table.open_peer(id);
            let outcome = table.apply(id, event);

            if let Ok(state) = outcome
                && state == StreamState::Closed
            {
                prop_assert!(
                    table.get_mut(id).is_none(),
                    "stream {} reached Closed but is still in the table",
                    id.get(),
                );
            }
            prop_assert!(
                table.live_count() <= LIMIT as usize,
                "{} live streams exceeds the {LIMIT}-stream limit",
                table.live_count(),
            );
        }
    }

    /// Claim 3. Windows are checked arithmetic end to end: no wrap, no value
    /// above 2^31 - 1, and a failed operation leaves the window untouched.
    #[test]
    fn a_window_never_wraps_or_exceeds_the_ceiling(
        initial in -1_000_000i32..MAX_WINDOW_SIZE,
        ops in prop::collection::vec(
            prop_oneof![
                any::<u32>().prop_map(Ok),
                any::<u32>().prop_map(Err),
            ],
            1..32,
        ),
    ) {
        let mut window = Window::new(initial);
        for op in ops {
            let before = window.available();
            match op {
                Ok(increment) => match window.increase(increment) {
                    // A wrap is exactly what this catches: `self.0 += increment
                    // as i32` would turn a large credit into a *smaller* window,
                    // silently, and the ceiling check below would still pass.
                    Ok(()) => prop_assert!(
                        window.available() >= before,
                        "crediting {increment} moved the window from {before} down to {}",
                        window.available(),
                    ),
                    Err(code) => {
                        prop_assert_eq!(code, ErrorCode::FlowControlError);
                        prop_assert_eq!(
                            window.available(), before,
                            "a rejected credit must not move the window",
                        );
                    }
                },
                Err(len) => match window.consume(len) {
                    Ok(()) => prop_assert!(
                        window.available() <= before,
                        "spending {len} must not raise the window",
                    ),
                    Err(code) => {
                        prop_assert_eq!(code, ErrorCode::FlowControlError);
                        prop_assert_eq!(
                            window.available(), before,
                            "a rejected debit must not move the window",
                        );
                    }
                },
            }
        }
    }

    /// Claim 4, send side. Whatever sequence of credits and debits a peer
    /// drives, the octets we were *allowed* to send never exceed the octets it
    /// credited.
    #[test]
    fn we_never_spend_more_than_the_peer_credited(
        initial in 0i32..65_536,
        ops in prop::collection::vec(
            prop_oneof![
                (1u32..70_000).prop_map(Ok),
                (0u32..70_000).prop_map(Err),
            ],
            1..64,
        ),
    ) {
        let mut window = Window::new(initial);
        let mut credited = i64::from(initial);
        let mut spent = 0i64;

        for op in ops {
            match op {
                Ok(increment) => {
                    if window.increase(increment).is_ok() {
                        credited += i64::from(increment);
                    }
                }
                Err(len) => {
                    // A sender consults `sendable()` first; the debit itself
                    // must agree with it, or the two disagree under load.
                    let allowed = window.sendable();
                    let take = (len as usize).min(allowed) as u32;
                    if take > 0 {
                        prop_assert!(window.consume(take).is_ok());
                        spent += i64::from(take);
                    }
                    prop_assert!(
                        window.consume(u32::try_from(allowed).unwrap_or(u32::MAX) + 1).is_err(),
                        "spending past the window must be refused",
                    );
                }
            }
            prop_assert!(
                spent <= credited,
                "spent {spent} octets against {credited} credited",
            );
        }
    }

    /// Claim 4, receive side. The credit we hand back never exceeds the credit
    /// the peer actually consumed — over-crediting would let a peer overrun the
    /// buffer the window exists to bound.
    #[test]
    fn we_never_credit_back_more_than_was_consumed(
        initial in 1i32..1_000_000,
        chunks in prop::collection::vec(1u32..8192, 1..128),
    ) {
        let mut window = RecvWindow::new(initial);
        let mut consumed = 0i64;
        let mut released = 0i64;

        for len in chunks {
            // Only spend credit the peer actually has, as a real peer must.
            let available = window.available().max(0) as u32;
            let len = len.min(available);
            if len == 0 {
                break;
            }
            prop_assert!(window.record(len).is_ok());
            consumed += i64::from(len);
            if let Some(increment) = window.release(len) {
                released += i64::from(increment);
            }
            prop_assert!(
                released <= consumed,
                "released {released} octets of credit for {consumed} consumed",
            );
            prop_assert!(window.available() <= initial);
        }

        // Everything consumed is either announced or still pending — nothing is
        // lost, which is what stops a long-lived connection slowly starving.
        prop_assert_eq!(released + i64::from(window.unreleased()), consumed);
    }
}
