//! Per-stream lifecycle: the RFC 9113 §5.1 state machine.
//!
//! Owns `StreamId` and its rules (odd = client-initiated, strictly increasing,
//! never reused), the state transitions idle → open → half-closed → closed
//! plus the RST_STREAM shortcut, and `MAX_CONCURRENT_STREAMS` enforcement.
//! Server push is disabled (`ENABLE_PUSH = 0`), so the reserved states are
//! rejected rather than modeled.
//!
//! Illegal transitions map to their RFC-mandated error codes (frames on idle
//! streams → connection `PROTOCOL_ERROR`; frames after END_STREAM → stream
//! `STREAM_CLOSED`), using the error types from [`crate::conn`].
//!
//! Implemented in week 5, together with fair outbound interleaving (per-stream
//! byte budget, design doc §4.1).

use crate::conn::StreamError;

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
    /// Apply `event`, returning the next state or the stream-level error the RFC
    /// mandates for an illegal transition (e.g. DATA after END_STREAM →
    /// `STREAM_CLOSED`). Implemented in week 5.
    pub fn transition(self, event: StreamEvent) -> Result<StreamState, StreamError> {
        let _ = event;
        todo!("stream state machine — week 5")
    }
}
