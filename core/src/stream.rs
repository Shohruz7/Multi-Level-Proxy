//! Per-stream lifecycle: the RFC 9113 §5.1 state machine.
//!
//! Owns `StreamId` and its rules (odd = client-initiated, strictly increasing,
//! never reused), the state transitions idle → open → half-closed → closed
//! plus the RST_STREAM shortcut, and `MAX_CONCURRENT_STREAMS` enforcement.
//! Server push is disabled (`ENABLE_PUSH = 0`), so the reserved states are
//! rejected rather than modeled.
//!
//! Two types, split by what they can know. [`StreamState`] is a pure function of
//! (state, event) and has no idea what a stream id is — so it returns a bare
//! [`ErrorCode`]. [`StreamTable`] owns the ids, the concurrency budget, and the
//! per-stream windows, so it is the one that can name the stream in a
//! [`StreamError`] and the one that decides whether a violation is stream- or
//! connection-scoped.
//!
//! Fair outbound interleaving is a per-stream byte budget
//! ([`crate::flow::SEND_BUDGET`]) applied by the connection's round-robin
//! scheduler; a stream only holds the queue and its windows.

use std::collections::HashMap;
use std::collections::VecDeque;

use bytes::Bytes;

use crate::conn::{ErrorCode, StreamError};
use crate::flow::{RecvWindow, Window};

/// A stream identifier (RFC 9113 §5.1.1).
///
/// The high bit is reserved and MUST be zero on the wire, so only the low 31
/// bits are significant. Stream `0` is the connection control stream; nonzero
/// odd ids are client-initiated and even ids server-initiated.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamId(u32);

impl StreamId {
    /// The connection control stream (SETTINGS, PING, GOAWAY, and
    /// connection-level WINDOW_UPDATE all ride stream 0).
    pub const CONNECTION: StreamId = StreamId(0);

    /// Build an id from a 32-bit wire value, masking off the reserved high bit.
    pub const fn new(raw: u32) -> Self {
        StreamId(raw & 0x7fff_ffff)
    }

    /// The 31-bit numeric value.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Is this the connection control stream (id 0)?
    pub const fn is_connection(self) -> bool {
        self.0 == 0
    }

    /// Client-initiated streams are the nonzero odd ids (§5.1.1).
    pub const fn is_client_initiated(self) -> bool {
        self.0 != 0 && self.0 & 1 == 1
    }

    /// Server-initiated streams are the nonzero even ids. With push disabled the
    /// proxy never opens one toward a client, but upstream connections use them.
    pub const fn is_server_initiated(self) -> bool {
        self.0 != 0 && self.0 & 1 == 0
    }
}

/// The lifecycle states a stream moves through (RFC 9113 §5.1).
///
/// The `reserved` (push) states are intentionally omitted: the proxy sends
/// `ENABLE_PUSH = 0`, so a PUSH_PROMISE is a protocol error rather than a state
/// to model.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum StreamState {
    #[default]
    Idle,
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
}

/// The events that drive a stream between states. `end_stream` carries the
/// END_STREAM flag, which is what collapses `Open` toward `Closed`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StreamEvent {
    SendHeaders { end_stream: bool },
    RecvHeaders { end_stream: bool },
    SendData { end_stream: bool },
    RecvData { end_stream: bool },
    SendRstStream,
    RecvRstStream,
}

impl StreamState {
    /// Has this stream finished, in both directions?
    pub const fn is_closed(self) -> bool {
        matches!(self, StreamState::Closed)
    }

    /// May the peer still send us DATA or HEADERS?
    pub const fn peer_can_send(self) -> bool {
        matches!(self, StreamState::Open | StreamState::HalfClosedLocal)
    }

    /// May we still send DATA or HEADERS?
    pub const fn we_can_send(self) -> bool {
        matches!(self, StreamState::Open | StreamState::HalfClosedRemote)
    }

    /// Apply `event`, returning the next state or the RFC-mandated error code
    /// for an illegal transition.
    ///
    /// Two codes come out of here and they mean different things to the caller:
    ///
    /// - `STREAM_CLOSED` — the stream existed and is finished in the direction
    ///   the event moves. A stream error; RST_STREAM and the connection lives.
    /// - `PROTOCOL_ERROR` — the event landed on an **idle** stream, i.e. the
    ///   peer referenced something that was never opened. §5.1 makes that a
    ///   *connection* error, because it means the two ends disagree about what
    ///   exists and nothing downstream can be trusted. [`StreamTable`] and
    ///   `conn.rs` promote it accordingly.
    pub fn transition(self, event: StreamEvent) -> Result<StreamState, ErrorCode> {
        use StreamEvent::*;
        use StreamState::*;

        match (self, event) {
            // idle: only HEADERS opens a stream. END_STREAM on the opening
            // frame skips `Open` entirely (a bodyless GET does exactly this).
            (Idle, SendHeaders { end_stream }) => {
                Ok(if end_stream { HalfClosedLocal } else { Open })
            }
            (Idle, RecvHeaders { end_stream }) => {
                Ok(if end_stream { HalfClosedRemote } else { Open })
            }
            // Anything else on an idle stream is the connection-scoped case.
            (Idle, _) => Err(ErrorCode::ProtocolError),

            // RST_STREAM is the shortcut from any live state straight to closed,
            // and is idempotent once there (frames cross in flight).
            (_, SendRstStream | RecvRstStream) => Ok(Closed),

            // open: either side may still send; END_STREAM half-closes the
            // sender's direction.
            (Open, SendHeaders { end_stream } | SendData { end_stream }) => {
                Ok(if end_stream { HalfClosedLocal } else { Open })
            }
            (Open, RecvHeaders { end_stream } | RecvData { end_stream }) => {
                Ok(if end_stream { HalfClosedRemote } else { Open })
            }

            // half-closed(local): we said END_STREAM. We may not send again,
            // but we must keep receiving until the peer finishes too.
            (HalfClosedLocal, RecvHeaders { end_stream } | RecvData { end_stream }) => {
                Ok(if end_stream { Closed } else { HalfClosedLocal })
            }
            (HalfClosedLocal, _) => Err(ErrorCode::StreamClosed),

            // half-closed(remote): the peer said END_STREAM. Receiving more from
            // them is the error; we may still finish our response.
            (HalfClosedRemote, SendHeaders { end_stream } | SendData { end_stream }) => {
                Ok(if end_stream { Closed } else { HalfClosedRemote })
            }
            (HalfClosedRemote, _) => Err(ErrorCode::StreamClosed),

            // closed: nothing further is legal, though see the grace period in
            // `StreamTable::lookup` for frames that were already in flight.
            (Closed, _) => Err(ErrorCode::StreamClosed),
        }
    }
}

/// One live stream's state and buffers.
///
/// Only *live* streams get one of these — a stream reaching [`StreamState::
/// Closed`] is dropped from the table entirely (see [`StreamTable`]), so this
/// struct never represents a finished exchange.
#[derive(Debug)]
pub struct Stream {
    pub state: StreamState,
    /// Credit the peer gave us for this stream: how much DATA we may send.
    pub send_window: Window,
    /// Credit we gave the peer for this stream.
    pub recv_window: RecvWindow,
    /// Response body waiting for the scheduler, oldest chunk first.
    pub send_queue: VecDeque<Bytes>,
    /// Whether END_STREAM rides the last chunk in `send_queue`.
    pub send_end_stream: bool,
    /// Whether this stream is already in the connection's round-robin ring.
    /// Keeps a stream from being enqueued twice by two chunks arriving back to
    /// back.
    pub queued: bool,
    /// The body length the request declared via `content-length`, if any, and
    /// how much has actually arrived. §8.1.2.6 makes a mismatch a malformed
    /// message, which cannot be judged until the body ends.
    pub content_length: Option<u64>,
    pub data_received: u64,
}

impl Stream {
    fn new(send_window: i32, recv_window: i32) -> Self {
        Stream {
            state: StreamState::Idle,
            send_window: Window::new(send_window),
            recv_window: RecvWindow::new(recv_window),
            send_queue: VecDeque::new(),
            send_end_stream: false,
            queued: false,
            content_length: None,
            data_received: 0,
        }
    }

    /// Whether the body that arrived matches what `content-length` promised
    /// (§8.1.2.6). Only meaningful once the request body has ended.
    pub fn content_length_matches(&self) -> bool {
        self.content_length
            .is_none_or(|declared| declared == self.data_received)
    }

    /// Octets still queued for this stream.
    pub fn pending_send(&self) -> usize {
        self.send_queue.iter().map(Bytes::len).sum()
    }

    /// Is there anything the scheduler could write for this stream — either body
    /// octets or a bare END_STREAM to close it out?
    pub fn has_pending_send(&self) -> bool {
        !self.send_queue.is_empty() || self.send_end_stream
    }
}

/// Why a peer's attempt to open a stream was rejected.
///
/// The two variants are the ADR-0008 split in miniature, and choosing wrongly
/// between them is user-visible: `Refused` tells a client the request is safe to
/// retry on a new stream, while `Protocol` tears the connection down.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpenRejection {
    /// The id itself is illegal — even (client-initiated must be odd) or not
    /// strictly greater than every id seen. Connection `PROTOCOL_ERROR` (§5.1.1).
    Protocol(&'static str),
    /// `MAX_CONCURRENT_STREAMS` is already met. Stream `REFUSED_STREAM` (§5.1.2)
    /// — deliberately not `PROTOCOL_ERROR`, because the RFC reserves this code
    /// to mean "nothing was processed, retry elsewhere".
    Refused,
}

/// What a frame's stream id refers to.
#[derive(Debug)]
pub enum Lookup<'a> {
    /// A stream that is open or half-closed.
    Live(&'a mut Stream),
    /// An id at or below the highest we have seen, with no live entry: the
    /// stream ran to completion or was reset. Frames may still legitimately
    /// arrive here — they were in flight when we closed it — so §5.1 lets us
    /// ignore them for a period rather than treat them as errors.
    Closed,
    /// An id above everything seen: never opened. A frame for it is a
    /// connection `PROTOCOL_ERROR`.
    Idle,
}

/// Every live stream on one connection, plus the §5.1.1 / §5.1.2 rules that no
/// individual stream can enforce.
///
/// **Closed streams are removed, not tombstoned.** An id at or below
/// `highest_peer_id` with no map entry *is* closed, which is enough to tell
/// "closed" from "idle" with no per-stream storage at all. Keeping tombstones
/// would leak a map entry per request for the life of a connection — invisible
/// in tests, fatal in a week-8 load run.
#[derive(Debug)]
pub struct StreamTable {
    live: HashMap<StreamId, Stream>,
    /// Highest client-initiated id seen, for the ordering rule and for GOAWAY.
    highest_peer_id: StreamId,
    /// How many streams may be open or half-closed at once (§5.1.2).
    max_concurrent: u32,
    /// Initial send window for a new stream — the peer's `INITIAL_WINDOW_SIZE`.
    initial_send_window: i32,
    /// Initial receive window for a new stream — the one we advertise.
    initial_recv_window: i32,
    /// The high-water mark, for the daemon's metrics.
    peak_concurrent: u32,
    opened: u64,
}

impl StreamTable {
    pub fn new(max_concurrent: u32, initial_send_window: i32, initial_recv_window: i32) -> Self {
        StreamTable {
            live: HashMap::new(),
            highest_peer_id: StreamId::CONNECTION,
            max_concurrent,
            initial_send_window,
            initial_recv_window,
            peak_concurrent: 0,
            opened: 0,
        }
    }

    /// Streams currently open or half-closed — exactly what §5.1.2 counts,
    /// because closed streams are not kept.
    pub fn live_count(&self) -> usize {
        self.live.len()
    }

    /// The most streams that were ever live at once.
    pub const fn peak_concurrent(&self) -> u32 {
        self.peak_concurrent
    }

    /// How many streams the peer opened over this connection's lifetime.
    pub const fn opened(&self) -> u64 {
        self.opened
    }

    /// Highest client-initiated id seen — GOAWAY's `last_stream_id` (§6.8).
    pub const fn highest_peer_id(&self) -> StreamId {
        self.highest_peer_id
    }

    /// Admit a new peer-initiated stream, enforcing the id and concurrency
    /// rules. On success the stream exists in the `Idle` state, ready for its
    /// `RecvHeaders` event.
    pub fn open_peer(&mut self, id: StreamId) -> Result<&mut Stream, OpenRejection> {
        if !id.is_client_initiated() {
            return Err(OpenRejection::Protocol(
                "a client may only open odd-numbered streams",
            ));
        }
        // Strictly increasing (§5.1.1). Equality is included: an id we have
        // already seen is either live (a duplicate HEADERS) or implicitly
        // closed, and neither may be reopened.
        if id <= self.highest_peer_id {
            return Err(OpenRejection::Protocol("stream ids must strictly increase"));
        }
        if self.live.len() as u64 >= u64::from(self.max_concurrent) {
            // Note the id anyway: we are about to RST_STREAM it, and a later
            // GOAWAY must not claim we never saw it.
            self.highest_peer_id = id;
            return Err(OpenRejection::Refused);
        }

        self.highest_peer_id = id;
        self.opened += 1;
        let stream = Stream::new(self.initial_send_window, self.initial_recv_window);
        self.live.insert(id, stream);
        // `live` only grows here, so the peak can only move on this path.
        self.peak_concurrent = self.peak_concurrent.max(self.live.len() as u32);
        Ok(self.live.get_mut(&id).expect("just inserted"))
    }

    /// Resolve a frame's stream id against the table.
    pub fn lookup(&mut self, id: StreamId) -> Lookup<'_> {
        // Borrowck: decide the branch before taking the mutable borrow.
        if self.live.contains_key(&id) {
            Lookup::Live(self.live.get_mut(&id).expect("just checked"))
        } else if id <= self.highest_peer_id {
            Lookup::Closed
        } else {
            Lookup::Idle
        }
    }

    pub fn get_mut(&mut self, id: StreamId) -> Option<&mut Stream> {
        self.live.get_mut(&id)
    }

    /// Apply `event` to `id`, retiring the stream if it lands in `Closed`.
    ///
    /// Returns the resulting state. An illegal transition becomes a
    /// [`StreamError`] naming the stream — the caller decides whether the code
    /// warrants RST_STREAM or, for `PROTOCOL_ERROR`, a connection teardown.
    pub fn apply(&mut self, id: StreamId, event: StreamEvent) -> Result<StreamState, StreamError> {
        let Some(stream) = self.live.get_mut(&id) else {
            // No live entry: idle streams are a protocol error, already-closed
            // ones are the in-flight grace period.
            return if id <= self.highest_peer_id {
                Err(StreamError::new(id, ErrorCode::StreamClosed))
            } else {
                Err(StreamError::new(id, ErrorCode::ProtocolError))
            };
        };

        let next = stream
            .state
            .transition(event)
            .map_err(|code| StreamError::new(id, code))?;
        stream.state = next;
        if next.is_closed() {
            self.retire(id);
        }
        Ok(next)
    }

    /// Drop a finished stream. Idempotent — a stream can be retired by
    /// END_STREAM and then again by a crossing RST_STREAM.
    pub fn retire(&mut self, id: StreamId) {
        self.live.remove(&id);
    }

    /// Fan a change in the peer's `INITIAL_WINDOW_SIZE` across every live
    /// stream's send window (§6.9.2).
    ///
    /// This is retroactive by design: the new value applies to streams that were
    /// already open, which is the one place a window can go negative.
    pub fn apply_initial_window_delta(&mut self, delta: i32) -> Result<(), ErrorCode> {
        self.initial_send_window = self.initial_send_window.saturating_add(delta);
        for stream in self.live.values_mut() {
            stream.send_window.apply_delta(delta)?;
        }
        Ok(())
    }

    /// Every live stream, for the scheduler.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&StreamId, &mut Stream)> {
        self.live.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::StreamEvent::*;
    use super::StreamState::*;
    use super::*;

    const STREAM_1: StreamId = StreamId(1);
    const STREAM_3: StreamId = StreamId(3);

    fn table() -> StreamTable {
        StreamTable::new(2, 100, 100)
    }

    /// Open a stream and drive it to `state`.
    fn at(state: StreamState) -> StreamState {
        state
    }

    // ---- the transition table (§5.1) ---------------------------------------

    #[test]
    fn headers_open_a_stream_from_idle() {
        assert_eq!(
            at(Idle).transition(RecvHeaders { end_stream: false }),
            Ok(Open)
        );
        assert_eq!(
            at(Idle).transition(SendHeaders { end_stream: false }),
            Ok(Open)
        );
    }

    #[test]
    fn headers_with_end_stream_skip_open_entirely() {
        // A bodyless GET: one HEADERS frame takes the stream straight to
        // half-closed(remote).
        assert_eq!(
            at(Idle).transition(RecvHeaders { end_stream: true }),
            Ok(HalfClosedRemote)
        );
        assert_eq!(
            at(Idle).transition(SendHeaders { end_stream: true }),
            Ok(HalfClosedLocal)
        );
    }

    #[test]
    fn anything_but_headers_on_an_idle_stream_is_a_protocol_error() {
        // Connection-scoped, not stream-scoped: the peer referenced something
        // that was never opened, so the two ends disagree about what exists.
        for event in [
            RecvData { end_stream: false },
            SendData { end_stream: false },
            RecvRstStream,
            SendRstStream,
        ] {
            assert_eq!(
                at(Idle).transition(event),
                Err(ErrorCode::ProtocolError),
                "idle + {event:?}",
            );
        }
    }

    #[test]
    fn end_stream_half_closes_the_sender_side() {
        assert_eq!(
            at(Open).transition(RecvData { end_stream: true }),
            Ok(HalfClosedRemote)
        );
        assert_eq!(
            at(Open).transition(SendData { end_stream: true }),
            Ok(HalfClosedLocal)
        );
        assert_eq!(
            at(Open).transition(RecvData { end_stream: false }),
            Ok(Open)
        );
    }

    #[test]
    fn a_half_closed_stream_rejects_further_frames_from_the_finished_side() {
        for event in [
            RecvHeaders { end_stream: false },
            RecvData { end_stream: false },
            RecvData { end_stream: true },
        ] {
            assert_eq!(
                at(HalfClosedRemote).transition(event),
                Err(ErrorCode::StreamClosed),
                "half-closed(remote) + {event:?}",
            );
        }
        for event in [
            SendHeaders { end_stream: false },
            SendData { end_stream: false },
            SendData { end_stream: true },
        ] {
            assert_eq!(
                at(HalfClosedLocal).transition(event),
                Err(ErrorCode::StreamClosed),
                "half-closed(local) + {event:?}",
            );
        }
    }

    #[test]
    fn a_half_closed_stream_still_serves_the_live_side() {
        // half-closed(remote) is the normal state of a request being answered.
        assert_eq!(
            at(HalfClosedRemote).transition(SendHeaders { end_stream: false }),
            Ok(HalfClosedRemote)
        );
        assert_eq!(
            at(HalfClosedRemote).transition(SendData { end_stream: true }),
            Ok(Closed)
        );
        assert_eq!(
            at(HalfClosedLocal).transition(RecvData { end_stream: true }),
            Ok(Closed)
        );
    }

    #[test]
    fn rst_stream_closes_from_every_live_state_and_is_idempotent() {
        for state in [Open, HalfClosedLocal, HalfClosedRemote, Closed] {
            assert_eq!(state.transition(RecvRstStream), Ok(Closed), "{state:?}");
            assert_eq!(state.transition(SendRstStream), Ok(Closed), "{state:?}");
        }
    }

    #[test]
    fn a_closed_stream_rejects_everything_but_a_reset() {
        for event in [
            RecvHeaders { end_stream: false },
            RecvData { end_stream: false },
            SendData { end_stream: true },
        ] {
            assert_eq!(
                at(Closed).transition(event),
                Err(ErrorCode::StreamClosed),
                "closed + {event:?}",
            );
        }
    }

    // ---- stream id rules (§5.1.1) ------------------------------------------

    #[test]
    fn a_client_may_only_open_odd_streams() {
        let mut t = table();
        let err = t
            .open_peer(StreamId::new(2))
            .expect_err("even ids are server-initiated");
        assert!(matches!(err, OpenRejection::Protocol(_)));
    }

    #[test]
    fn stream_ids_must_strictly_increase() {
        let mut t = table();
        t.open_peer(STREAM_3).expect("first stream");
        for id in [StreamId::new(1), StreamId::new(3)] {
            let err = t.open_peer(id).expect_err("not above the highest seen");
            assert!(matches!(err, OpenRejection::Protocol(_)), "id {}", id.get());
        }
    }

    #[test]
    fn a_completed_stream_id_can_never_be_reused() {
        let mut t = table();
        t.open_peer(STREAM_1).expect("first stream");
        t.apply(STREAM_1, RecvHeaders { end_stream: true })
            .expect("open it");
        t.apply(STREAM_1, SendHeaders { end_stream: true })
            .expect("and finish it");
        assert_eq!(t.live_count(), 0, "a closed stream leaves no entry");

        let err = t.open_peer(STREAM_1).expect_err("ids are never reused");
        assert!(matches!(err, OpenRejection::Protocol(_)));
    }

    // ---- concurrency (§5.1.2) ----------------------------------------------

    #[test]
    fn exceeding_max_concurrent_streams_refuses_rather_than_kills() {
        let mut t = StreamTable::new(2, 100, 100);
        for id in [1, 3] {
            t.open_peer(StreamId::new(id)).expect("inside the limit");
            t.apply(StreamId::new(id), RecvHeaders { end_stream: true })
                .expect("open");
        }
        // REFUSED_STREAM, not PROTOCOL_ERROR: the RFC reserves this code for
        // "nothing was processed, safe to retry".
        assert_eq!(
            t.open_peer(StreamId::new(5)).err(),
            Some(OpenRejection::Refused)
        );

        // Finishing one frees a slot.
        t.apply(STREAM_1, SendHeaders { end_stream: true })
            .expect("close stream 1");
        t.open_peer(StreamId::new(7)).expect("a slot opened up");
    }

    #[test]
    fn a_refused_stream_still_counts_toward_the_goaway_high_water_mark() {
        let mut t = StreamTable::new(0, 100, 100);
        assert_eq!(t.open_peer(STREAM_1).err(), Some(OpenRejection::Refused));
        // We must RST_STREAM it, so GOAWAY may not later claim it was never
        // seen.
        assert_eq!(t.highest_peer_id(), STREAM_1);
    }

    // ---- lookup: idle vs closed --------------------------------------------

    #[test]
    fn lookup_tells_a_never_opened_stream_from_a_finished_one() {
        let mut t = table();
        t.open_peer(STREAM_1).expect("open");
        t.apply(STREAM_1, RecvHeaders { end_stream: true })
            .expect("live");
        assert!(matches!(t.lookup(STREAM_1), Lookup::Live(_)));

        t.apply(STREAM_1, SendHeaders { end_stream: true })
            .expect("finish");
        // At or below the high-water mark with no entry: closed, and frames for
        // it were probably in flight. Above it: never opened at all.
        assert!(matches!(t.lookup(STREAM_1), Lookup::Closed));
        assert!(matches!(t.lookup(STREAM_3), Lookup::Idle));
    }

    #[test]
    fn a_frame_for_an_unopened_stream_is_a_connection_error() {
        let mut t = table();
        let err = t
            .apply(STREAM_3, RecvData { end_stream: false })
            .expect_err("stream 3 was never opened");
        assert_eq!(err.code, ErrorCode::ProtocolError);
    }

    // ---- retroactive INITIAL_WINDOW_SIZE (§6.9.2) --------------------------

    #[test]
    fn a_settings_change_moves_every_live_streams_send_window() {
        let mut t = StreamTable::new(4, 1000, 1000);
        t.open_peer(STREAM_1).expect("open");
        t.apply(STREAM_1, RecvHeaders { end_stream: true })
            .expect("live");

        t.apply_initial_window_delta(-1500).expect("a decrease");
        assert_eq!(
            t.get_mut(STREAM_1).expect("live").send_window.available(),
            -500,
            "a SETTINGS decrease may legally drive a window negative",
        );

        // And the new value seeds streams opened afterwards.
        t.open_peer(STREAM_3).expect("open");
        assert_eq!(
            t.get_mut(STREAM_3).expect("live").send_window.available(),
            -500,
        );
    }
}
