//! Connection layer: one HTTP/2 connection, owned by one task.
//!
//! Owns the preface + SETTINGS handshake and ack lifecycle (RFC 9113 §3.4,
//! §6.5), the frame loop that demuxes inbound frames onto a table of live
//! streams, the outbound scheduler that interleaves their responses, PING, and
//! GOAWAY handling.
//!
//! Also home of the error model's protocol side (ADR 0008): the
//! connection-error (→ GOAWAY, connection dies) vs stream-error
//! (→ RST_STREAM, connection lives) distinction as types, carrying the RFC
//! §7 error codes they must emit.
//!
//! Week 3 landed the handshake and the connection-control frames (SETTINGS,
//! PING, GOAWAY). Week 4 added the HPACK seam: header blocks are reassembled
//! across HEADERS + CONTINUATION and decoded through a *per-connection* codec,
//! because the dynamic table spans every block the peer sends. Week 5 makes it
//! a server — streams, flow control, and responses. The graceful GOAWAY drain
//! and the Rapid-Reset / flood accounting live in [`crate::guard`] (§6).
//!
//! # One task, three sources of work (ADR 0013, amended by ADR 0015)
//!
//! ADR 0009 planned a reader task, a writer task, and a spawned handler task per
//! stream. Week 5 amended that to **one task per connection**, because a
//! stream's state machine advances on both `Send*` and `Recv*` events:
//! splitting reader from writer would split the one piece of state that has to
//! see both directions, and re-introduce the locking the actor shape exists to
//! avoid. That still holds. What week 6 changes is what the one task waits on:
//!
//! ```text
//! loop {
//!     while let Some(frame) = codec.decode_any(&mut read_buf)? { handle_frame(frame)? }
//!     self.pump_outbound()?;           // queue what the windows allow
//!     if !self.tick().await? { break }  // readable | writable | the responder
//! }
//! ```
//!
//! Two things follow from that shape, and both are week-6 requirements rather
//! than tidying:
//!
//! - **Writes are queued, never awaited.** Frame handlers append to an outbound
//!   buffer and return; only `tick`'s write arm touches the socket, over the
//!   write half of a split stream. Week 5 called `write_all` inline, so a client
//!   that stopped reading parked the whole task — tolerable for a server, not
//!   for a proxy, where it would stall an unrelated upstream leg.
//! - **The responder is a third source of work.** A [`crate::service::Service`]
//!   answers whenever it can rather than inline, so a stream waiting on a
//!   backend costs a table entry and nothing else.
//!
//! When every window is exhausted `pump_outbound` queues nothing and the loop
//! parks in `tick` — which is exactly what "the sender is blocked" means. A
//! WINDOW_UPDATE from the peer or a chunk from the responder wakes it, and the
//! next pass resumes. Reading is never what blocks, so there is no deadlock.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, trace, warn};

use crate::flow::{
    CONNECTION_WINDOW, CONNECTION_WINDOW_BOOTSTRAP, MAX_WINDOW_SIZE, RecvWindow, SEND_BUDGET,
    STREAM_INITIAL_WINDOW, Window,
};
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, Decoded, Frame, FrameCodec, FrameType, MAX_ALLOWED_FRAME_SIZE,
};
use crate::guard::{Guard, Limits, Verdict};
use crate::hpack::{HpackDecoder, HpackEncoder, HpackError};
use crate::service::{Echo, RequestHead, Response, Service, ServiceEvent};
use crate::stream::{
    Lookup, MAX_LOCAL_STREAM_ID, OpenRejection, StreamEvent, StreamId, StreamTable,
};

/// How many encoded octets may sit in the outbound buffer before the scheduler
/// stops adding to it.
///
/// The flow-control windows already bound DATA in flight, but control frames —
/// SETTINGS acks, PING acks, RST_STREAMs, WINDOW_UPDATEs — are not
/// flow-controlled, so a peer that never reads its socket could otherwise grow
/// this without limit. [`crate::guard`]'s flood limits attack the same problem
/// from the other end; this is the backstop that makes a stalled writer cost
/// bounded memory rather than unbounded.
pub const MAX_WRITE_QUEUE: usize = 256 * 1024;

/// How long the last write on a dying connection may take before it is
/// abandoned. See [`Connection::flush`].
const FINAL_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// How a connection winds down when the daemon is asked to stop (§6.8, design
/// doc §5.3).
///
/// Plain data with a `Default`, rather than constants, so the daemon can move
/// the numbers from the environment without a rebuild and the tests can drive
/// them to zero. The library never reads the environment itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DrainPolicy {
    /// How long to keep accepting new streams after the advisory GOAWAY.
    ///
    /// The point of a nonzero grace is the request already on the wire: a client
    /// that put HEADERS in flight microseconds before our GOAWAY has not done
    /// anything wrong, and refusing it turns a clean deploy into a handful of
    /// client-visible errors. One round trip plus reaction time is enough.
    pub grace: Duration,
    /// How long to let in-flight streams finish after the final GOAWAY, before
    /// closing regardless.
    ///
    /// Bounded by what the container runtime will wait: Kubernetes'
    /// `terminationGracePeriodSeconds` and ECS' `StopTimeout` both default to
    /// 30 s, and being SIGKILLed mid-drain is strictly worse than closing
    /// cleanly a moment early.
    pub deadline: Duration,
}

impl Default for DrainPolicy {
    fn default() -> Self {
        DrainPolicy {
            grace: Duration::from_secs(2),
            deadline: Duration::from_secs(30),
        }
    }
}

impl DrainPolicy {
    /// Wind down immediately: no grace, no waiting for in-flight streams. What
    /// the tests use when the drain itself is not what is under test.
    pub const fn immediate() -> Self {
        DrainPolicy {
            grace: Duration::ZERO,
            deadline: Duration::ZERO,
        }
    }
}

/// A drain in progress (§6.8).
///
/// Two GOAWAYs, not one. The first is advisory — `last_stream_id` is the maximum
/// possible id, which commits to nothing and means only "stop opening streams".
/// The second, one `grace` later, names the highest id we will actually serve.
/// Sending only the second is the common shortcut and it is wrong in a specific
/// way: it retroactively refuses whatever the peer had already put on the wire.
#[derive(Clone, Copy, Debug)]
struct Drain {
    /// When the final GOAWAY goes out and new streams stop being accepted.
    commit_at: Instant,
    /// When to close whether or not the stream table has emptied.
    deadline_at: Instant,
    /// The id named in the final GOAWAY, once sent. `Some` means no new stream
    /// will be accepted.
    committed: Option<StreamId>,
}

impl Drain {
    /// The grace exists to protect a request the peer put on the wire before it
    /// could see our GOAWAY, so it can be skipped only when no such request can
    /// exist. That is a statement about *quiet*, not about emptiness:
    ///
    /// - "nothing in flight right now" is true of a busy connection between
    ///   every pair of requests, and using it collapsed a 2 s grace to 4.8 ms
    ///   under `h2load -m 20`;
    /// - "nothing in flight, and nothing for at least a grace period" cannot be
    ///   true of a connection with a request in flight, because a request in
    ///   flight is at most one round trip old.
    ///
    /// So a genuinely parked keep-alive connection still costs nothing at
    /// shutdown, and an active one keeps every millisecond of its grace.
    fn begin(policy: DrainPolicy, now: Instant, quiet_for: Duration) -> Self {
        let grace = if quiet_for >= policy.grace {
            Duration::ZERO
        } else {
            policy.grace - quiet_for
        };
        Drain {
            commit_at: now + grace,
            deadline_at: now + grace + policy.deadline,
            committed: None,
        }
    }

    /// The next instant this drain has something to do.
    fn next_wake(&self) -> Instant {
        match self.committed {
            None => self.commit_at,
            Some(_) => self.deadline_at,
        }
    }
}

/// How many streams a peer may have open or half-closed at once, advertised as
/// `SETTINGS_MAX_CONCURRENT_STREAMS` (§5.1.2). An abuse guard as much as a
/// resource one: unlimited concurrency is what makes Rapid Reset cheap.
pub const MAX_CONCURRENT_STREAMS: u32 = 256;

/// The client connection preface (RFC 9113 §3.4). A client opens every HTTP/2
/// connection by sending these 24 octets, immediately followed by its first
/// SETTINGS frame.
pub const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The largest decoded header list we will accept, advertised as
/// `SETTINGS_MAX_HEADER_LIST_SIZE` — the HPACK-bomb guard (design doc §6).
pub const MAX_HEADER_LIST_SIZE: u32 = 64 * 1024;

/// The largest *compressed* header block we will reassemble across HEADERS +
/// CONTINUATION.
///
/// A separate bound from [`MAX_HEADER_LIST_SIZE`], and a necessary one: the
/// decoded-size limit can only be applied once a block is complete, so without
/// this a peer could stream CONTINUATION frames forever and grow the buffer
/// without ever finishing a block. [`crate::guard`]'s CONTINUATION *count*
/// limit closes the other half: a stream of 1-octet frames stays under this
/// cap forever.
const MAX_HEADER_BLOCK_BYTES: usize = MAX_HEADER_LIST_SIZE as usize;

/// HTTP/2 error codes (RFC 9113 §7), shared by connection errors (GOAWAY) and
/// stream errors (RST_STREAM). The numeric values are the on-wire encoding.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
pub enum ErrorCode {
    NoError = 0x0,
    ProtocolError = 0x1,
    InternalError = 0x2,
    FlowControlError = 0x3,
    SettingsTimeout = 0x4,
    StreamClosed = 0x5,
    FrameSizeError = 0x6,
    RefusedStream = 0x7,
    Cancel = 0x8,
    CompressionError = 0x9,
    ConnectError = 0xa,
    EnhanceYourCalm = 0xb,
    InadequateSecurity = 0xc,
    Http11Required = 0xd,
}

impl ErrorCode {
    /// The on-wire 32-bit value.
    pub const fn as_u32(self) -> u32 {
        self as u32
    }

    /// Decode a wire value. Unknown codes are treated as `INTERNAL_ERROR`, as
    /// RFC 9113 §7 permits ("MAY be treated as being equivalent to
    /// INTERNAL_ERROR").
    pub const fn from_u32(v: u32) -> ErrorCode {
        match v {
            0x0 => ErrorCode::NoError,
            0x1 => ErrorCode::ProtocolError,
            0x2 => ErrorCode::InternalError,
            0x3 => ErrorCode::FlowControlError,
            0x4 => ErrorCode::SettingsTimeout,
            0x5 => ErrorCode::StreamClosed,
            0x6 => ErrorCode::FrameSizeError,
            0x7 => ErrorCode::RefusedStream,
            0x8 => ErrorCode::Cancel,
            0x9 => ErrorCode::CompressionError,
            0xa => ErrorCode::ConnectError,
            0xb => ErrorCode::EnhanceYourCalm,
            0xc => ErrorCode::InadequateSecurity,
            0xd => ErrorCode::Http11Required,
            _ => ErrorCode::InternalError,
        }
    }
}

/// A connection-level error (RFC 9113 §5.4.1): unrecoverable, so the endpoint
/// emits GOAWAY with `code` and closes the connection. `debug` becomes the
/// GOAWAY debug data. (ADR 0008.)
#[derive(Clone, Debug, thiserror::Error)]
#[error("connection error {code:?}: {debug}")]
pub struct ConnectionError {
    pub code: ErrorCode,
    pub debug: String,
}

impl ConnectionError {
    pub fn new(code: ErrorCode, debug: impl Into<String>) -> Self {
        ConnectionError {
            code,
            debug: debug.into(),
        }
    }
}

/// A stream-level error (RFC 9113 §5.4.2): only `stream` is aborted, via
/// RST_STREAM with `code`; the connection survives. (ADR 0008.)
#[derive(Clone, Debug, thiserror::Error)]
#[error("stream {stream:?} error {code:?}")]
pub struct StreamError {
    pub stream: StreamId,
    pub code: ErrorCode,
}

impl StreamError {
    pub fn new(stream: StreamId, code: ErrorCode) -> Self {
        StreamError { stream, code }
    }
}

/// SETTINGS parameter identifiers (RFC 9113 §6.5.2). These are the `u16` keys
/// carried in a SETTINGS frame's payload.
pub mod setting_id {
    pub const HEADER_TABLE_SIZE: u16 = 0x1;
    pub const ENABLE_PUSH: u16 = 0x2;
    pub const MAX_CONCURRENT_STREAMS: u16 = 0x3;
    pub const INITIAL_WINDOW_SIZE: u16 = 0x4;
    pub const MAX_FRAME_SIZE: u16 = 0x5;
    pub const MAX_HEADER_LIST_SIZE: u16 = 0x6;
}

/// The connection SETTINGS in effect (RFC 9113 §6.5.2).
///
/// Only the parameters the proxy uses are modeled; unknown identifiers are
/// ignored on receipt (§6.5.3). `None` on the optional fields means "no limit
/// advertised" — the RFC's unlimited default.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Settings {
    pub header_table_size: u32,
    pub enable_push: bool,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_header_list_size: Option<u32>,
}

impl Default for Settings {
    /// The protocol defaults from RFC 9113 §6.5.2, in force until the peer's
    /// first SETTINGS frame is applied.
    fn default() -> Self {
        Settings {
            header_table_size: 4096,
            enable_push: true,
            max_concurrent_streams: None,
            initial_window_size: 65_535,
            max_frame_size: 16_384,
            max_header_list_size: None,
        }
    }
}

impl Settings {
    /// The settings this proxy advertises: protocol defaults with push turned
    /// off (the stated non-goal — see the README), which also makes any
    /// PUSH_PROMISE we later receive a clean protocol error, plus a bound on
    /// how large a header list we will decode (design doc §6), a concurrency
    /// cap, and a raised per-stream receive window.
    ///
    /// Note what is *not* here: the connection-level receive window. §6.9.1
    /// scopes `INITIAL_WINDOW_SIZE` to streams only, so raising the connection
    /// window takes an explicit WINDOW_UPDATE on stream 0 at handshake — see
    /// [`CONNECTION_WINDOW_BOOTSTRAP`].
    pub fn server() -> Self {
        Settings {
            enable_push: false,
            max_concurrent_streams: Some(MAX_CONCURRENT_STREAMS),
            initial_window_size: STREAM_INITIAL_WINDOW as u32,
            max_header_list_size: Some(MAX_HEADER_LIST_SIZE),
            ..Settings::default()
        }
    }

    /// The settings the proxy advertises on an **upstream** connection, where it
    /// is the client (design doc §4.3).
    ///
    /// Push is refused for the same reason as on the client side, and the
    /// receive window is raised the same way — a backend streaming a large
    /// response is exactly the case the raised window exists for. What differs
    /// is `MAX_CONCURRENT_STREAMS`: as the initiator we have no reason to limit
    /// the peer, who opens nothing.
    pub fn client() -> Self {
        Settings {
            enable_push: false,
            max_concurrent_streams: None,
            initial_window_size: STREAM_INITIAL_WINDOW as u32,
            max_header_list_size: Some(MAX_HEADER_LIST_SIZE),
            ..Settings::default()
        }
    }

    /// Fold a received SETTINGS payload (id/value pairs) into these settings,
    /// validating each parameter's legal range and rejecting violations with
    /// the connection error the RFC mandates (§6.5.2).
    ///
    /// Unknown identifiers are ignored rather than rejected, as §6.5.3
    /// requires — that is what lets the protocol add parameters later.
    pub fn apply(&mut self, params: &[(u16, u32)]) -> Result<(), ConnectionError> {
        for &(id, value) in params {
            match id {
                setting_id::HEADER_TABLE_SIZE => self.header_table_size = value,
                setting_id::ENABLE_PUSH => {
                    self.enable_push = match value {
                        0 => false,
                        1 => true,
                        other => {
                            return Err(ConnectionError::new(
                                ErrorCode::ProtocolError,
                                format!("ENABLE_PUSH must be 0 or 1, got {other}"),
                            ));
                        }
                    };
                }
                setting_id::MAX_CONCURRENT_STREAMS => self.max_concurrent_streams = Some(value),
                setting_id::INITIAL_WINDOW_SIZE => {
                    // A window above 2^31-1 cannot be represented, so §6.5.2
                    // makes it a FLOW_CONTROL_ERROR rather than a protocol one.
                    if value > MAX_WINDOW_SIZE as u32 {
                        return Err(ConnectionError::new(
                            ErrorCode::FlowControlError,
                            format!("INITIAL_WINDOW_SIZE {value} exceeds {MAX_WINDOW_SIZE}"),
                        ));
                    }
                    self.initial_window_size = value;
                }
                setting_id::MAX_FRAME_SIZE => {
                    if !(DEFAULT_MAX_FRAME_SIZE..=MAX_ALLOWED_FRAME_SIZE).contains(&value) {
                        return Err(ConnectionError::new(
                            ErrorCode::ProtocolError,
                            format!(
                                "MAX_FRAME_SIZE {value} outside \
                                 [{DEFAULT_MAX_FRAME_SIZE}, {MAX_ALLOWED_FRAME_SIZE}]"
                            ),
                        ));
                    }
                    self.max_frame_size = value;
                }
                setting_id::MAX_HEADER_LIST_SIZE => self.max_header_list_size = Some(value),
                // Unknown or unsupported identifier: ignore it (§6.5.3).
                _ => {}
            }
        }
        Ok(())
    }

    /// The SETTINGS frame that advertises these values.
    ///
    /// Only parameters that differ from the protocol defaults are sent — plus
    /// `ENABLE_PUSH` always, because "we will never accept a push" is worth
    /// stating explicitly rather than leaving to a default the peer must know.
    pub fn to_frame(&self) -> Frame {
        let defaults = Settings::default();
        let mut params = vec![(setting_id::ENABLE_PUSH, u32::from(self.enable_push))];
        if self.header_table_size != defaults.header_table_size {
            params.push((setting_id::HEADER_TABLE_SIZE, self.header_table_size));
        }
        if let Some(max) = self.max_concurrent_streams {
            params.push((setting_id::MAX_CONCURRENT_STREAMS, max));
        }
        if self.initial_window_size != defaults.initial_window_size {
            params.push((setting_id::INITIAL_WINDOW_SIZE, self.initial_window_size));
        }
        if self.max_frame_size != defaults.max_frame_size {
            params.push((setting_id::MAX_FRAME_SIZE, self.max_frame_size));
        }
        if let Some(max) = self.max_header_list_size {
            params.push((setting_id::MAX_HEADER_LIST_SIZE, max));
        }
        Frame::Settings { ack: false, params }
    }
}

// ---------------------------------------------------------------------------
// Task topology (§2.2) — prototyped in week 2, decided in week 5.
//
// Week 2 sketched `ToStream` / `FromStream` / `Dispatcher` / `STREAM_CHANNEL_BOUND`
// for the reader → per-stream-task → writer pipeline of ADR 0009. Week 5 removed
// them rather than populate them; see ADR 0013 and this module's header for the
// reasoning. Two points survive from the sketch and are worth keeping visible:
//
//   - The channel bound would have counted *messages*, not octets. Sixty-four
//     DATA frames at MAX_FRAME_SIZE is ~1 MiB per stream, so at 256 streams it
//     bounded nothing useful. The real memory bound is the connection receive
//     window (`flow::CONNECTION_WINDOW`), which caps in-flight octets no matter
//     how many streams are open.
//   - Per-stream tasks exist to keep a slow *upstream* from blocking the reader.
//     Week 6 solved that with a third select arm rather than a task per stream:
//     the responder is a source of work, not a thing to wait on.
// ---------------------------------------------------------------------------

/// What a finished connection did, for the daemon's logs and metrics. Keeps the
/// `metrics` crate out of the engine: the binary owns instrumentation, the
/// library just reports.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ConnectionSummary {
    /// Whether the preface + SETTINGS exchange completed (§3.4).
    pub handshake_completed: bool,
    /// Frames successfully decoded from the peer.
    pub frames_received: u64,
    /// Complete header blocks decoded through HPACK — i.e. requests read off
    /// this connection.
    pub header_blocks_decoded: u64,
    /// Streams the peer opened over this connection's lifetime.
    pub streams_opened: u64,
    /// Requests handed to the responder — i.e. proxied, on the proxy path.
    pub requests_dispatched: u64,
    /// Streams that ended in a RST_STREAM, in either direction.
    pub streams_reset: u64,
    /// The most streams live at once — how much of [`MAX_CONCURRENT_STREAMS`]
    /// the peer actually used.
    pub peak_concurrent_streams: u32,
    /// DATA payload octets written to the peer.
    pub data_bytes_sent: u64,
    /// Times the scheduler had queued octets but no window to send them in.
    /// A nonzero count under load is the observable proof that flow control is
    /// doing something.
    pub flow_control_stalls: u64,
    /// Whether this connection wound down through a graceful drain — i.e. it
    /// got as far as committing a GOAWAY id — rather than being closed by the
    /// peer or by an error.
    pub drained: bool,
}

/// Why the reader loop stopped.
enum Stop {
    /// The peer closed, we are draining, or the peer sent GOAWAY. Nothing to
    /// report on the wire.
    Quiet,
    /// A protocol violation: GOAWAY must go out before the socket closes.
    Failed(ConnectionError),
}

/// Whether the reader loop keeps going after a frame.
enum Next {
    Continue,
    Close,
}

/// What one round-robin visit did, which is what decides where the stream sits
/// in the ring for the next lap.
enum Visit {
    /// Octets queued, no window to send them in. Keeps its place at the front.
    Stalled,
    /// Wrote a frame; `more` says whether anything is still queued.
    Wrote { more: bool },
    /// Nothing left to send, or the stream is gone. Leaves the ring.
    Finished,
}

/// One HTTP/2 connection, driven by a single task.
///
/// Week 3 brings the connection up: server SETTINGS, client preface, and the
/// frame loop answering the connection-control frames (SETTINGS/ACK, PING,
/// GOAWAY). Week 5 adds the streams: inbound frames land on a [`StreamTable`],
/// requests are answered by an `S: Service`, and outbound DATA is metered by two
/// levels of flow-control window and interleaved by a round-robin scheduler. The
/// Week 7 added the graceful GOAWAY drain ([`DrainPolicy`]) and the abuse
/// guard ([`crate::guard`]), both inside this same lifecycle.
pub struct Connection<IO, S = Echo> {
    /// The socket, halved so a read future and a write future can be in flight
    /// at once (ADR 0015). Week 5 owned `io` whole and called `write_all` from
    /// the frame loop, which meant a client that stopped reading parked the loop
    /// and stopped *our* reads too. For a server that was tolerable; for a proxy
    /// it would let one stalled client leg stall an unrelated upstream one.
    reader: ReadHalf<IO>,
    writer: WriteHalf<IO>,
    /// Encoded frames waiting for the socket. Every writer in this file appends
    /// here and returns immediately; the loop's write arm is the only thing that
    /// touches the socket, and [`MAX_WRITE_QUEUE`] bounds it.
    out: BytesMut,
    shutdown: broadcast::Receiver<()>,
    read_buf: BytesMut,
    encode_buf: BytesMut,
    codec: FrameCodec,
    /// What we advertised. Our `MAX_FRAME_SIZE` is what bounds the *decoder*:
    /// it is the limit we imposed on the peer.
    local_settings: Settings,
    /// The peer's settings as applied. Bounds what we may send.
    peer_settings: Settings,
    /// Set while a header block is open — a HEADERS or CONTINUATION arrived
    /// without END_HEADERS, so only a CONTINUATION on this stream may follow
    /// (§4.3).
    open_header_block: Option<StreamId>,
    /// The END_STREAM flag of the HEADERS that opened the block being
    /// reassembled. It rides on the first frame but only takes effect once
    /// END_HEADERS arrives, possibly several CONTINUATIONs later.
    open_header_end_stream: bool,
    /// Partial header block, accumulated across CONTINUATION frames. Empty
    /// whenever [`Self::open_header_block`] is `None`.
    header_block: BytesMut,
    /// Inbound HPACK state. One per connection, not per stream: the dynamic
    /// table spans every header block the peer sends, which is why a header
    /// block is indivisible and why a decode failure kills the connection
    /// (design doc §3.3).
    hpack_dec: HpackDecoder,
    /// Outbound HPACK state, bounded by the peer's `HEADER_TABLE_SIZE`.
    hpack_enc: HpackEncoder,
    /// Every live stream, plus the §5.1.1 id rules and the §5.1.2 concurrency
    /// budget.
    streams: StreamTable,
    /// Connection-level credit the peer gave us: the ceiling on DATA in flight
    /// across *all* streams, and therefore the real memory bound.
    conn_send_window: Window,
    /// Connection-level credit we gave the peer.
    conn_recv_window: RecvWindow,
    /// Round-robin ring of streams with something to send. A stream appears at
    /// most once (`Stream::queued` guards that) and rotates to the back after
    /// each visit, whether or not it finished.
    ready: VecDeque<StreamId>,
    /// RST_STREAMs owed to the peer. The state machine is driven from places
    /// that cannot also write (they already hold a borrow of the stream table),
    /// so the frame is queued here and flushed on the way out.
    pending_reset: Vec<StreamError>,
    /// Per-connection abuse accounting (design doc §6). Holds no protocol
    /// state; every trip becomes an `ENHANCE_YOUR_CALM` connection error through
    /// the existing error model, so the blast radius is this connection alone.
    guard: Guard,
    /// When this connection last had a stream open, so a drain can tell a parked
    /// keep-alive connection from one that is merely between requests. Only read
    /// at the moment a drain begins, so keeping it costs one store per request.
    last_stream_at: Instant,
    /// How to wind down when asked to stop.
    drain_policy: DrainPolicy,
    /// Set once the shutdown signal has fired. While this is `Some` the loop
    /// keeps serving — that is the whole difference between a drain and a
    /// teardown — and the shutdown arm of the select is disabled, because a
    /// fired broadcast receiver resolves instantly forever and would spin.
    draining: Option<Drain>,
    /// What answers a request.
    service: S,
    /// Where the responder's answers arrive. Unbounded on purpose — see the
    /// [`crate::service`] module doc for why that does not unbound memory.
    events: mpsc::UnboundedReceiver<ServiceEvent>,
    summary: ConnectionSummary,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Connection<IO, Echo> {
    /// Build a connection over an established (TLS-terminated) byte stream.
    /// `shutdown` fires — as a value or by the sender being dropped — when the
    /// daemon is draining.
    pub fn new(io: IO, shutdown: broadcast::Receiver<()>) -> Self {
        Self::with_settings(io, shutdown, Settings::server())
    }

    /// Build a connection advertising specific settings. Used by tests to drive
    /// the handshake with non-default parameters.
    pub fn with_settings(
        io: IO,
        shutdown: broadcast::Receiver<()>,
        local_settings: Settings,
    ) -> Self {
        Self::with_service(io, shutdown, local_settings, Echo::default())
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin, S: Service> Connection<IO, S> {
    /// Build a connection answered by a specific [`Service`]. Week 6 passes the
    /// proxy here; week 5 uses it for tests that need a scripted responder.
    pub fn with_service(
        io: IO,
        shutdown: broadcast::Receiver<()>,
        local_settings: Settings,
        mut service: S,
    ) -> Self {
        let defaults = Settings::default();
        let (events_tx, events) = mpsc::unbounded_channel();
        service.attach(events_tx);
        let (reader, writer) = tokio::io::split(io);
        Connection {
            reader,
            writer,
            out: BytesMut::with_capacity(16 * 1024),
            shutdown,
            read_buf: BytesMut::with_capacity(16 * 1024),
            encode_buf: BytesMut::with_capacity(1024),
            codec: FrameCodec::new(local_settings.max_frame_size),
            local_settings,
            peer_settings: defaults,
            open_header_block: None,
            open_header_end_stream: false,
            header_block: BytesMut::new(),
            hpack_dec: HpackDecoder::new(
                local_settings.header_table_size as usize,
                local_settings.max_header_list_size.map(|n| n as usize),
            ),
            hpack_enc: HpackEncoder::new(defaults.header_table_size as usize),
            streams: StreamTable::new(
                local_settings
                    .max_concurrent_streams
                    .unwrap_or(MAX_CONCURRENT_STREAMS),
                // Until the peer's SETTINGS arrives, their initial window is the
                // protocol default — not ours. Getting this backwards is a way
                // to overrun a peer's window on the very first response.
                defaults.initial_window_size as i32,
                local_settings.initial_window_size as i32,
            ),
            conn_send_window: Window::new(defaults.initial_window_size as i32),
            conn_recv_window: RecvWindow::new(CONNECTION_WINDOW),
            ready: VecDeque::new(),
            pending_reset: Vec::new(),
            guard: Guard::new(Limits::default(), Instant::now()),
            last_stream_at: Instant::now(),
            drain_policy: DrainPolicy::default(),
            draining: None,
            service,
            events,
            summary: ConnectionSummary::default(),
        }
    }

    /// Set how this connection winds down on shutdown. The daemon passes its
    /// configured policy; tests that are not about draining pass
    /// [`DrainPolicy::immediate`].
    pub fn with_drain_policy(mut self, policy: DrainPolicy) -> Self {
        self.drain_policy = policy;
        self
    }

    /// Set the abuse-guard thresholds (design doc §6). The daemon passes its
    /// configured limits; tests that are not about abuse pass
    /// [`Limits::permissive`].
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.guard = Guard::new(limits, Instant::now());
        self
    }

    /// Turn a guard verdict into the connection error it implies.
    ///
    /// `ENHANCE_YOUR_CALM` is the code every other implementation converged on
    /// for this, and it is the honest one: the peer is not malformed, it is
    /// simply asking for too much. The GOAWAY carries which signal tripped, so
    /// an operator reading the debug data learns something.
    fn check(verdict: Verdict) -> Result<(), ConnectionError> {
        match verdict.tripped() {
            None => Ok(()),
            Some(signal) => Err(ConnectionError::new(
                ErrorCode::EnhanceYourCalm,
                format!("abuse guard tripped: {}", signal.as_str()),
            )),
        }
    }

    /// Run the connection until the peer closes it, a shutdown signal arrives,
    /// or a protocol error forces it down.
    ///
    /// A connection error is reported to the peer as GOAWAY before the socket
    /// closes — the ADR-0008 error model in practice.
    pub async fn run(mut self) -> ConnectionSummary {
        let stop = self.drive().await;
        self.summary.peak_concurrent_streams = self.streams.peak_concurrent();
        self.summary.streams_opened = self.streams.opened();
        if let Stop::Failed(err) = stop {
            warn!(code = ?err.code, reason = %err.debug, "connection error; sending GOAWAY");
            let go_away = Frame::GoAway {
                last_stream_id: self.streams.highest_peer_id(),
                error_code: err.code,
                debug_data: Bytes::from(err.debug.into_bytes()),
            };
            // A failed encode here would be our bug, and there is nothing left
            // to do about it on a connection that is already going down.
            let _ = self.queue_frame(&go_away);
        }
        // Everything queued — the GOAWAY above, a final WINDOW_UPDATE, the tail
        // of a response — is still only in `out`. Flush it before the socket
        // drops, or the peer sees a truncated stream instead of a clean error.
        self.flush().await;
        self.close_politely().await;
        self.summary
    }

    /// Close the write half, then read what the peer already sent until it
    /// closes too.
    ///
    /// Otherwise the last frame the peer sent is often still in the kernel's
    /// receive buffer when the socket drops, and closing a socket with unread
    /// data makes the OS send **RST instead of FIN** — so a peer that had just
    /// been handed a perfectly good GOAWAY reads "connection reset by peer"
    /// instead. h2spec catches this as an intermittent failure on the GOAWAY
    /// tests, which is exactly how it presented.
    async fn close_politely(&mut self) {
        let _ = tokio::time::timeout(FINAL_FLUSH_TIMEOUT, async {
            let _ = self.writer.shutdown().await;
            let mut sink = [0u8; 1024];
            // Read to EOF. Bounded by the timeout, so a peer that neither sends
            // nor closes costs one second, not a task.
            while let Ok(n) = self.reader.read(&mut sink).await {
                if n == 0 {
                    break;
                }
            }
        })
        .await;
    }

    /// The handshake, then the frame loop.
    async fn drive(&mut self) -> Stop {
        // Our half of the connection preface is a SETTINGS frame, sent
        // immediately and before anything is read (§3.4).
        let settings = self.local_settings.to_frame();
        if let Err(e) = self.queue_frame(&settings) {
            return Stop::Failed(e);
        }

        // The connection-level receive window is *not* covered by
        // INITIAL_WINDOW_SIZE (§6.9.1) — SETTINGS can only move stream windows.
        // Raising it takes an explicit stream-0 WINDOW_UPDATE, and forgetting it
        // caps throughput at the 64 KiB default with nothing in the logs to say
        // why. `flow.rs` owns the arithmetic; this is the one place it is sent.
        let bootstrap = Frame::WindowUpdate {
            stream_id: StreamId::CONNECTION,
            increment: CONNECTION_WINDOW_BOOTSTRAP,
        };
        if let Err(e) = self.queue_frame(&bootstrap) {
            return Stop::Failed(e);
        }

        // The client's half opens with 24 fixed octets.
        if !self.fill_at_least(PREFACE.len()).await {
            return Stop::Quiet;
        }
        if self.read_buf[..PREFACE.len()] != PREFACE[..] {
            // Not an HTTP/2 peer at all (a plain HTTP/1.1 request, say). GOAWAY
            // is optional here (§3.4) and meaningless to something that cannot
            // parse it, so just close.
            warn!("client connection preface mismatch; closing");
            return Stop::Quiet;
        }
        let _ = self.read_buf.split_to(PREFACE.len());
        trace!("client preface accepted");

        loop {
            // Drain every frame the buffer already holds before reading again:
            // one read can carry many frames.
            loop {
                let decoded = self.codec.decode_any(&mut self.read_buf);
                match decoded {
                    Ok(Some(Decoded::Ignored { kind, stream_id })) => {
                        // A frame we model no state for still counts against
                        // §4.3: nothing at all may come between a HEADERS and its
                        // CONTINUATION, not even a frame we are about to throw
                        // away. Checking it here is why `decode_any` exists.
                        if let Err(e) = self.reject_if_mid_header_block(kind, stream_id) {
                            return Stop::Failed(e);
                        }
                        trace!(?kind, stream = stream_id.get(), "ignoring frame");
                    }
                    Ok(Some(Decoded::Frame(frame))) => {
                        self.summary.frames_received += 1;
                        match self.handle_frame(frame) {
                            Ok(Next::Continue) => {}
                            Ok(Next::Close) => return Stop::Quiet,
                            Err(e) => return Stop::Failed(e),
                        }
                        // A synchronous responder has already answered by the
                        // time `handle_frame` returns, so taking its events here
                        // keeps a response next to the request that caused it —
                        // the ordering week 5 had for free when responding was a
                        // function call. Costs one `try_recv` on an empty
                        // channel when the responder is a real proxy.
                        if let Err(e) = self.drain_service_events() {
                            return Stop::Failed(e);
                        }
                    }
                    // A partial frame: leave it buffered and read more.
                    Ok(None) => break,
                    Err(e) => {
                        return Stop::Failed(ConnectionError::new(e.code(), e.to_string()));
                    }
                }
            }

            // Queue whatever the flow-control windows now allow. When they allow
            // nothing this is a no-op and we park below — which is exactly what
            // a blocked sender looks like. A WINDOW_UPDATE from the peer or a
            // chunk from the responder is what wakes us, so reading is never
            // what blocks and there is no deadlock.
            if let Err(e) = self.pump_outbound() {
                return Stop::Failed(e);
            }

            match self.tick().await {
                Ok(true) => {}
                Ok(false) => return Stop::Quiet,
                Err(e) => return Stop::Failed(e),
            }
        }
    }

    /// One pass of the connection's I/O: whichever of the socket, the responder,
    /// or the shutdown signal is ready first. `false` means the connection is
    /// finished.
    ///
    /// Three arms rather than week 5's one, and the shape is the week's
    /// structural change. Reading and writing are separate futures over the two
    /// halves of the socket, so a peer that stops reading can no longer park the
    /// task that has to keep serving everyone else; and the responder is a third
    /// source of work that arrives on its own schedule, which is what makes a
    /// slow backend cost one stream instead of the connection.
    async fn tick(&mut self) -> Result<bool, ConnectionError> {
        // The write arm borrows `out` immutably for as long as the select's
        // futures live, so the consumed count comes back out here and the buffer
        // is advanced once they are gone.
        let mut wrote = 0usize;
        let mut event = None;
        let mut begin_drain = false;
        let mut drain_tick = false;

        // Read the drain's next deadline out before the select, so the timer arm
        // does not have to borrow `self`.
        let drain_wake = self.draining.as_ref().map(Drain::next_wake);

        let keep_going = tokio::select! {
            biased;
            // Closed channel (Err) or a value both mean "start draining". Only
            // armed while we are *not* already draining: a broadcast receiver
            // that has fired resolves immediately forever, so leaving this armed
            // would spin the loop at full speed for the whole drain.
            _ = self.shutdown.recv(), if self.draining.is_none() => {
                begin_drain = true;
                true
            }

            // The drain's own clock: the grace expiring, then the deadline.
            // `pending()` when there is no drain, so the arm simply never fires
            // rather than needing a guard that borrows `self`.
            _ = async {
                match drain_wake {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                drain_tick = true;
                true
            }

            // Flush before reading more: the queue is bounded, and leaving it
            // full would stop the scheduler from making progress.
            written = self.writer.write(&self.out), if !self.out.is_empty() => {
                match written {
                    Ok(0) => false,
                    Ok(n) => {
                        wrote = n;
                        true
                    }
                    Err(e) => {
                        debug!(error = %e, "write failed; closing connection");
                        false
                    }
                }
            }

            received = self.events.recv() => {
                match received {
                    Some(received) => {
                        event = Some(received);
                        true
                    }
                    // Every sender is gone, which can only happen once the
                    // responder itself has been dropped.
                    None => false,
                }
            }

            read = self.reader.read_buf(&mut self.read_buf) => {
                match read {
                    Ok(0) => false,  // peer closed the connection
                    Ok(_) => true,
                    Err(e) => {
                        debug!(error = %e, "read failed; closing connection");
                        false
                    }
                }
            }
        };

        self.out.advance(wrote);
        if let Some(event) = event {
            self.handle_service_event(event)?;
            self.drain_service_events()?;
        }
        if !keep_going {
            return Ok(false);
        }
        if begin_drain {
            self.begin_drain()?;
        }
        // Re-checked on every pass, not only on the timer: the last stream
        // finishing is what usually ends a drain, and that happens on a socket
        // wake-up rather than a clock one.
        if self.draining.is_some() {
            return self.advance_drain(drain_tick);
        }
        Ok(true)
    }

    /// Send the advisory GOAWAY and start the drain clock (§6.8).
    ///
    /// `2^31 - 1` is deliberate: it is the largest id that can exist, so it
    /// promises nothing about what we will refuse and says only "open no more
    /// streams". The real id follows one `grace` later, by which time anything
    /// already in flight has arrived and been counted.
    fn begin_drain(&mut self) -> Result<(), ConnectionError> {
        let now = Instant::now();
        // How long this connection has had nothing at all in flight. A live
        // stream means zero: whatever it is doing is current by definition.
        let quiet_for = if self.streams.live_count() > 0 {
            Duration::ZERO
        } else {
            now.saturating_duration_since(self.last_stream_at)
        };
        self.draining = Some(Drain::begin(self.drain_policy, now, quiet_for));
        debug!(
            live = self.streams.live_count(),
            quiet_ms = quiet_for.as_millis() as u64,
            "draining: advertising GOAWAY(NO_ERROR) with no committed id",
        );
        let advisory = Frame::GoAway {
            last_stream_id: StreamId::new(MAX_LOCAL_STREAM_ID),
            error_code: ErrorCode::NoError,
            debug_data: Bytes::new(),
        };
        self.queue_frame(&advisory)
    }

    /// Move the drain along: commit when the grace expires, and finish when the
    /// last stream ends or the deadline arrives. `Ok(false)` closes the loop.
    fn advance_drain(&mut self, timer_fired: bool) -> Result<bool, ConnectionError> {
        let Some(drain) = self.draining else {
            return Ok(true);
        };
        let now = Instant::now();

        if drain.committed.is_none() && (timer_fired || now >= drain.commit_at) {
            let last = self.streams.highest_peer_id();
            debug!(
                last_stream_id = last.get(),
                live = self.streams.live_count(),
                "draining: committing GOAWAY; no new streams accepted",
            );
            self.queue_frame(&Frame::GoAway {
                last_stream_id: last,
                error_code: ErrorCode::NoError,
                debug_data: Bytes::new(),
            })?;
            if let Some(drain) = self.draining.as_mut() {
                drain.committed = Some(last);
            }
            self.summary.drained = true;
        }

        let Some(drain) = self.draining else {
            return Ok(true);
        };
        if drain.committed.is_some() {
            if self.streams.live_count() == 0 {
                trace!("draining: every stream finished");
                return Ok(false);
            }
            if now >= drain.deadline_at {
                warn!(
                    live = self.streams.live_count(),
                    "drain deadline expired; closing with streams still open",
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether the drain has committed an id, i.e. no new stream may open.
    fn drain_committed(&self) -> bool {
        self.draining.is_some_and(|d| d.committed.is_some())
    }

    /// Take everything the responder has produced so far.
    ///
    /// A proxy commonly delivers a head and several chunks together, and one
    /// wake-up's worth of work should not cost several passes around the loop.
    fn drain_service_events(&mut self) -> Result<(), ConnectionError> {
        while let Ok(event) = self.events.try_recv() {
            self.handle_service_event(event)?;
        }
        Ok(())
    }

    /// Act on one thing the responder produced.
    ///
    /// Everything here addresses a *client* stream id, and every event may
    /// arrive for a stream that has since been reset or completed — the
    /// responder found out later than we did, which is normal rather than
    /// exceptional. A missing stream is therefore always a silent drop, never an
    /// error: the RST_STREAM that retired it has already been sent.
    fn handle_service_event(&mut self, event: ServiceEvent) -> Result<(), ConnectionError> {
        // The responder's last look. It may swallow the event — a proxy
        // retrying a refused request does exactly that, so the client never
        // sees the first attempt fail — in which case there is nothing for the
        // connection to do and the stream simply stays open.
        let Some(event) = self.service.intercept(event) else {
            return Ok(());
        };
        match event {
            ServiceEvent::Head {
                id,
                response,
                end_stream,
            } => {
                if self.streams.get_mut(id).is_none() {
                    return Ok(());
                }
                self.send_response(id, response, end_stream)?;
            }
            ServiceEvent::Data {
                id,
                data,
                end_stream,
            } => {
                let Some(stream) = self.streams.get_mut(id) else {
                    return Ok(());
                };
                if !data.is_empty() {
                    stream.send_queue.push_back(data);
                }
                if end_stream {
                    stream.send_end_stream = true;
                }
                self.enqueue_ready(id);
            }
            ServiceEvent::Trailers { id, fields } => {
                if self.streams.get_mut(id).is_none() {
                    return Ok(());
                }
                self.send_trailers(id, &fields)?;
            }
            ServiceEvent::BodyAccepted { id, n } => {
                self.release_recv_window(id, n)?;
            }
            ServiceEvent::Reset { id, code } => {
                if self.streams.get_mut(id).is_some() {
                    self.write_rst_stream(id, code)?;
                }
            }
        }
        Ok(())
    }

    /// Return `n` octets of receive credit at both levels, once whatever the
    /// request body was for has actually taken them (§4.2).
    ///
    /// This is the request-direction half of the backpressure bridge. Week 5
    /// called `release` the instant DATA was decoded, which is correct for a
    /// server that consumes its input immediately and wrong for a proxy: the
    /// client's window has to track the *upstream's* appetite, not ours.
    fn release_recv_window(&mut self, id: StreamId, n: u32) -> Result<(), ConnectionError> {
        if n == 0 {
            return Ok(());
        }
        let stream_increment = match self.streams.get_mut(id) {
            Some(stream) => {
                stream.pending_conn_release = stream.pending_conn_release.saturating_sub(n);
                stream.recv_window.release(n)
            }
            // The stream is gone, so its own window is moot — but the connection
            // window still owes the credit, and `retire` has parked it in
            // `reclaimed` for the drain below.
            None => None,
        };
        if let Some(increment) = stream_increment {
            self.queue_frame(&Frame::WindowUpdate {
                stream_id: id,
                increment,
            })?;
        }
        if self.streams.get_mut(id).is_some()
            && let Some(increment) = self.conn_recv_window.release(n)
        {
            self.queue_frame(&Frame::WindowUpdate {
                stream_id: StreamId::CONNECTION,
                increment,
            })?;
        }
        self.drain_reclaimed()
    }

    /// Hand back connection-level credit that retired streams were still
    /// holding.
    ///
    /// Without this the connection window leaks a little on every transfer the
    /// client abandons mid-body: the octets were recorded, the responder never
    /// got to accept them, and nothing else would ever release them. The symptom
    /// is a connection that grows slower over hours and nothing in the logs.
    fn drain_reclaimed(&mut self) -> Result<(), ConnectionError> {
        let reclaimed = self.streams.take_reclaimed();
        if reclaimed == 0 {
            return Ok(());
        }
        if let Some(increment) = self.conn_recv_window.release(reclaimed) {
            self.queue_frame(&Frame::WindowUpdate {
                stream_id: StreamId::CONNECTION,
                increment,
            })?;
        }
        Ok(())
    }

    /// Push everything queued to the socket, best effort. Used on the way out,
    /// where there is nothing useful to do about a failure.
    ///
    /// **Bounded**, because this is the one write in the file that is not part
    /// of the select loop: a peer whose receive buffer is full and who never
    /// reads again would otherwise pin this task forever, holding its streams,
    /// its leases and the daemon's drain open. A GOAWAY that cannot be delivered
    /// in a second is not going to be delivered.
    async fn flush(&mut self) {
        let _ = tokio::time::timeout(FINAL_FLUSH_TIMEOUT, async {
            while !self.out.is_empty() {
                match self.writer.write(&self.out).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => self.out.advance(n),
                }
            }
            let _ = self.writer.flush().await;
        })
        .await;
    }

    /// Act on one decoded frame.
    ///
    /// Connection-control frames are answered here; stream-addressed frames are
    /// routed onto the stream table, which is where the §5.1 state machine and
    /// the per-stream windows live.
    fn handle_frame(&mut self, frame: Frame) -> Result<Next, ConnectionError> {
        // A header block is atomic: once HEADERS or CONTINUATION arrives without
        // END_HEADERS, nothing but a CONTINUATION on that same stream may follow
        // (§4.3). Interleaving anything else corrupts the shared HPACK state, so
        // it is a connection error even though only one stream looks affected.
        match self.open_header_block {
            Some(open) => {
                let continues_block = matches!(
                    &frame,
                    Frame::Continuation { stream_id, .. } if *stream_id == open
                );
                if !continues_block {
                    return Err(ConnectionError::new(
                        ErrorCode::ProtocolError,
                        format!(
                            "expected CONTINUATION on stream {}, got {:?} on stream {}",
                            open.get(),
                            frame.kind(),
                            frame.stream_id().get(),
                        ),
                    ));
                }
            }
            // The mirror-image rule, and the one easier to miss: a CONTINUATION
            // with no open block continues nothing. Accepting it would feed the
            // shared HPACK decoder a fragment with no context.
            None if matches!(frame, Frame::Continuation { .. }) => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!(
                        "CONTINUATION on stream {} with no header block open",
                        frame.stream_id().get(),
                    ),
                ));
            }
            None => {}
        }

        // PING and SETTINGS are not flow-controlled and each *obliges us to
        // write an ACK*, so a peer can make us do unbounded work with two tiny
        // frame types (§6; CVE-2019-9512 "Ping Flood", CVE-2019-9515 "Settings
        // Flood"). That mandatory reply is the asymmetry, and it is what defines
        // the signal.
        //
        // WINDOW_UPDATE is deliberately **not** counted here, though the first
        // version did count it. It obliges no reply, and its rate is a function
        // of how fast the peer consumes data we chose to send — so it is
        // proportional to legitimate traffic, not to abuse. Metering it made the
        // bounded-memory test fail: a client draining 64 MiB a few KB at a time
        // releases capacity hundreds of times, which is exactly the behaviour
        // the proxy is built to reward. A zero-increment WINDOW_UPDATE, the one
        // genuinely abusive shape, is already a protocol error in
        // `recv_window_update`.
        if matches!(&frame, Frame::Ping { .. } | Frame::Settings { .. }) {
            Self::check(self.guard.on_control_frame(Instant::now()))?;
        }

        match &frame {
            Frame::Settings { ack: false, params } => {
                let previous_window = self.peer_settings.initial_window_size;
                self.peer_settings.apply(params)?;
                // Their HEADER_TABLE_SIZE bounds *our* encoder's dynamic table;
                // the change is announced back as a size update on our next
                // header block (RFC 7541 §6.3).
                self.hpack_enc
                    .set_max_table_size(self.peer_settings.header_table_size as usize);
                // A change to INITIAL_WINDOW_SIZE applies retroactively to every
                // stream already open (§6.9.2), by the *delta* rather than as an
                // assignment — a stream that has already spent credit must not
                // have it silently restored.
                let delta = self.peer_settings.initial_window_size as i64 - previous_window as i64;
                if delta != 0 {
                    self.streams
                        .apply_initial_window_delta(delta as i32)
                        .map_err(|code| {
                            ConnectionError::new(
                                code,
                                "INITIAL_WINDOW_SIZE change overflows a stream window",
                            )
                        })?;
                }
                trace!(?self.peer_settings, "applied peer SETTINGS");
                // Every SETTINGS frame must be acknowledged (§6.5.3). The first
                // one completes the handshake.
                self.write_settings_ack()?;
                self.summary.handshake_completed = true;
            }
            Frame::Settings { ack: true, .. } => {
                // The peer accepted ours. A peer that never acknowledges is
                // not currently treated as a SETTINGS_TIMEOUT — it costs us
                // nothing to keep serving one, and the flood limits already
                // bound the frames it can spend while not answering.
                trace!("peer acknowledged our SETTINGS");
            }
            Frame::Ping { data, ack: false } => {
                let pong = Frame::Ping {
                    data: *data,
                    ack: true,
                };
                self.queue_frame(&pong)?;
            }
            Frame::Ping { ack: true, .. } => {
                // A reply to a keepalive we sent. Nothing to do on the client
                // leg: it is the *upstream* leg that pings to prove a backend is
                // still alive, and it matches the nonce itself.
            }
            Frame::GoAway {
                last_stream_id,
                error_code,
                ..
            } => {
                debug!(
                    last_stream_id = last_stream_id.get(),
                    ?error_code,
                    "peer sent GOAWAY; closing",
                );
                return Ok(Next::Close);
            }
            Frame::WindowUpdate {
                stream_id,
                increment,
            } => {
                self.recv_window_update(*stream_id, *increment)?;
            }
            Frame::Headers {
                stream_id,
                block,
                end_stream,
                end_headers,
            } => {
                if self.open_header_block.is_none() {
                    self.open_header_end_stream = *end_stream;
                }
                self.read_header_block(*stream_id, block, *end_headers)?;
            }
            Frame::Continuation {
                stream_id,
                block,
                end_headers,
            } => {
                self.read_header_block(*stream_id, block, *end_headers)?;
            }
            Frame::Data {
                stream_id,
                data,
                end_stream,
            } => {
                self.recv_data(*stream_id, data, *end_stream)?;
            }
            Frame::RstStream {
                stream_id,
                error_code,
            } => {
                self.recv_rst_stream(*stream_id, *error_code)?;
            }
        }

        // Track whether a header block is still open across this frame.
        self.open_header_block = match &frame {
            Frame::Headers {
                stream_id,
                end_headers,
                ..
            }
            | Frame::Continuation {
                stream_id,
                end_headers,
                ..
            } if !*end_headers => Some(*stream_id),
            Frame::Headers { .. } | Frame::Continuation { .. } => None,
            _ => self.open_header_block,
        };

        Ok(Next::Continue)
    }

    /// §4.3 for the frames the codec discards: an ignorable type is still a
    /// frame, and no frame may sit between a HEADERS and its CONTINUATION.
    fn reject_if_mid_header_block(
        &self,
        kind: FrameType,
        stream_id: StreamId,
    ) -> Result<(), ConnectionError> {
        match self.open_header_block {
            Some(open) => Err(ConnectionError::new(
                ErrorCode::ProtocolError,
                format!(
                    "expected CONTINUATION on stream {}, got {kind:?} on stream {}",
                    open.get(),
                    stream_id.get(),
                ),
            )),
            None => Ok(()),
        }
    }

    /// Take one fragment of a header block, decoding once END_HEADERS arrives.
    ///
    /// A header block is a single HPACK unit however many frames carry it, so
    /// the fragments are reassembled before the decoder sees any of them. The
    /// §4.3 rule that nothing may interleave is already enforced by the caller,
    /// which is what makes plain concatenation correct here.
    fn read_header_block(
        &mut self,
        stream_id: StreamId,
        fragment: &Bytes,
        end_headers: bool,
    ) -> Result<(), ConnectionError> {
        let pending = self.header_block.len();
        if pending + fragment.len() > MAX_HEADER_BLOCK_BYTES {
            self.header_block.clear();
            return Err(ConnectionError::new(
                ErrorCode::EnhanceYourCalm,
                format!("header block exceeds {MAX_HEADER_BLOCK_BYTES} octets before END_HEADERS"),
            ));
        }

        let block = if end_headers && pending == 0 {
            // The common case: one frame carries the whole block, so it decodes
            // straight out of the read buffer with no copy (ADR 0007).
            self.guard.on_header_block_end();
            fragment.clone()
        } else {
            self.header_block.extend_from_slice(fragment);
            if !end_headers {
                // The byte cap above bounds the *buffer*, but not the frame
                // count: a stream of 1-octet CONTINUATIONs stays under it
                // indefinitely while costing a decode round each. ADR 0012
                // flagged this gap; the count is what closes it.
                Self::check(self.guard.on_continuation())?;
                trace!(
                    stream = stream_id.get(),
                    buffered = self.header_block.len(),
                    "header block continues",
                );
                return Ok(());
            }
            self.guard.on_header_block_end();
            self.header_block.split().freeze()
        };

        // Two HPACK failures, two blast radii (the split week 4 built for this).
        // A malformed block poisons the shared dynamic table and is fatal to the
        // connection; a block that merely decodes to *too much* leaves the table
        // sound, because the decoder deliberately ran to the end. That one is a
        // 431 on this stream and nothing more.
        let headers = match self.hpack_dec.decode(&block) {
            Ok(headers) => headers,
            Err(HpackError::HeaderListTooLarge { size, limit }) => {
                debug!(
                    stream = stream_id.get(),
                    size, limit, "header list too large; answering 431",
                );
                self.summary.header_blocks_decoded += 1;
                return self.reject_stream(stream_id, 431);
            }
            Err(e @ HpackError::Compression(_)) => return Err(e.into()),
        };
        self.summary.header_blocks_decoded += 1;
        debug!(
            stream = stream_id.get(),
            fields = headers.len(),
            headers = ?Redacted(&headers),
            "decoded header block",
        );

        let end_stream = self.open_header_end_stream;
        self.recv_request(stream_id, &headers, end_stream)
    }

    /// A complete request has arrived: open the stream, validate the message,
    /// and queue whatever the service answers with.
    fn recv_request(
        &mut self,
        stream_id: StreamId,
        headers: &[crate::hpack::Header],
        end_stream: bool,
    ) -> Result<(), ConnectionError> {
        // A HEADERS on a stream that is already live is a trailer section, not a
        // new request. Week 5 validated and dropped them; week 6 hands them to
        // the responder, which is what lets a proxy carry a backend's trailers
        // through to the client.
        if let Some(stream) = self.streams.get_mut(stream_id) {
            // The state machine decides whether a second field section is legal
            // at all, and it has to be asked *first*. In half-closed(remote) the
            // peer has already sent END_STREAM, so this is `STREAM_CLOSED`
            // (§5.1) — not a malformed trailer section, which is what reading
            // the block before checking the state would conclude.
            //
            // Week 5 never reached this: its responder answered inside the
            // frame handler, so a stream was usually already retired by the time
            // a second HEADERS arrived and the id-reuse rule fired instead. With
            // a real backend the stream is still live, and the difference became
            // visible as an h2spec failure whose outcome depended on backend
            // latency.
            if !stream.state.peer_can_send() {
                return self.write_rst_stream(stream_id, ErrorCode::StreamClosed);
            }

            // §8.1: a trailer section ends the stream. A second HEADERS without
            // END_STREAM is a third field section, which HTTP/2 has no room for.
            if !end_stream {
                return self.write_rst_stream(stream_id, ErrorCode::ProtocolError);
            }
            if let Err(code) = crate::service::validate_trailers(headers) {
                debug!(stream = stream_id.get(), ?code, "malformed trailers");
                return self.write_rst_stream(stream_id, code);
            }
            // The body has ended, so `content-length` can finally be judged —
            // and it must be, before the terminal transition retires the stream
            // and takes the counters with it.
            if let Some(code) = self.request_body_error(stream_id) {
                return self.write_rst_stream(stream_id, code);
            }
            self.service.trailers(stream_id, headers.to_vec());
            return self.apply_stream_event(
                stream_id,
                StreamEvent::RecvHeaders { end_stream },
                "trailers",
            );
        }

        // An id we have seen before, with no live entry, is a stream that
        // finished — not an out-of-order one. §5.1 is specific about the
        // difference: anything after a frame with END_STREAM is `STREAM_CLOSED`,
        // while `PROTOCOL_ERROR` is for ids that violate the *ordering* rule.
        // Letting `open_peer`'s ordering check answer first reports the wrong
        // code, and which one a peer saw depended on whether the response had
        // completed yet — so on the proxy path it varied with backend latency.
        if matches!(self.streams.lookup(stream_id), Lookup::Closed) {
            return Err(ConnectionError::new(
                ErrorCode::StreamClosed,
                format!("HEADERS on stream {}, which is closed", stream_id.get()),
            ));
        }

        // Past the committed GOAWAY id, so this request was never processed.
        // REFUSED_STREAM says exactly that, which is what makes it safe for the
        // client to send elsewhere — the same promise §5.1.2 relies on, and the
        // reason a drain is not visible to a well-behaved client as an error.
        if self.drain_committed() {
            debug!(
                stream = stream_id.get(),
                "refusing a new stream: connection is draining",
            );
            return self.write_rst_stream(stream_id, ErrorCode::RefusedStream);
        }

        match self.streams.open_peer(stream_id) {
            Ok(_) => {}
            Err(OpenRejection::Protocol(why)) => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!("stream {}: {why}", stream_id.get()),
                ));
            }
            Err(OpenRejection::Refused) => {
                // §5.1.2. REFUSED_STREAM specifically, because it promises the
                // client nothing was processed and the request is safe to retry.
                debug!(
                    stream = stream_id.get(),
                    live = self.streams.live_count(),
                    "at MAX_CONCURRENT_STREAMS; refusing",
                );
                return self.write_rst_stream(stream_id, ErrorCode::RefusedStream);
            }
        }
        self.apply_stream_event(
            stream_id,
            StreamEvent::RecvHeaders { end_stream },
            "request",
        )?;

        let request = match RequestHead::from_headers(headers) {
            Ok(request) => request,
            Err(code) => {
                // §8.3: a malformed message is a *stream* error. One bad request
                // must not take down the other streams sharing this connection.
                debug!(stream = stream_id.get(), ?code, "malformed request");
                return self.write_rst_stream(stream_id, code);
            }
        };

        // Remember what the request promised, so the DATA that follows can be
        // held to it (§8.1.2.6).
        match request.content_length() {
            Some(Ok(declared)) => {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.content_length = Some(declared);
                }
            }
            Some(Err(code)) => return self.write_rst_stream(stream_id, code),
            None => {}
        }
        // A bodyless request with a nonzero `content-length` is already wrong;
        // there is no DATA coming to make it right.
        if end_stream && let Some(code) = self.request_body_error(stream_id) {
            return self.write_rst_stream(stream_id, code);
        }

        // Hand it off and move on. Whatever answers — the built-in responder,
        // synchronously, or a backend three network hops away — arrives later as
        // a `ServiceEvent`, and until then this stream costs nothing but a table
        // entry while the other streams on the connection keep being served.
        self.summary.requests_dispatched += 1;
        self.last_stream_at = Instant::now();
        self.service.dispatch(stream_id, request, end_stream);
        Ok(())
    }

    /// Whether the body that arrived contradicts the request's `content-length`
    /// (§8.1.2.6). Call this at the point the request body ends, and *before*
    /// the transition that retires the stream.
    fn request_body_error(&mut self, stream_id: StreamId) -> Option<ErrorCode> {
        let stream = self.streams.get_mut(stream_id)?;
        (!stream.content_length_matches()).then(|| {
            debug!(
                stream = stream_id.get(),
                declared = stream.content_length,
                received = stream.data_received,
                "content-length disagrees with the body",
            );
            ErrorCode::ProtocolError
        })
    }

    /// Encode a response's HEADERS. Body octets arrive separately, as
    /// [`ServiceEvent::Data`].
    fn send_response(
        &mut self,
        stream_id: StreamId,
        response: Response,
        end_stream: bool,
    ) -> Result<(), ConnectionError> {
        let headers = response.to_headers();
        let mut block = BytesMut::new();
        self.hpack_enc.encode(&headers, &mut block);
        self.queue_frame(&Frame::Headers {
            stream_id,
            block: block.freeze(),
            end_stream,
            end_headers: true,
        })?;
        // From here on a reset of this stream is ordinary cancellation rather
        // than the Rapid Reset signature: the client asked, and was answered.
        if let Some(stream) = self.streams.get_mut(stream_id) {
            stream.response_started = true;
        }
        self.apply_stream_event(
            stream_id,
            StreamEvent::SendHeaders { end_stream },
            "response",
        )
    }

    /// Encode a trailer section: a second HEADERS after the body, always with
    /// END_STREAM (§8.1).
    ///
    /// The fields are validated before they go out even though they came from
    /// our own responder — on the proxy path they originated at a backend, and a
    /// backend is just another peer.
    fn send_trailers(
        &mut self,
        stream_id: StreamId,
        fields: &[crate::hpack::Header],
    ) -> Result<(), ConnectionError> {
        if crate::service::validate_trailers(fields).is_err() {
            debug!(
                stream = stream_id.get(),
                "responder produced malformed trailers; ending the stream without them",
            );
            if let Some(stream) = self.streams.get_mut(stream_id) {
                stream.send_end_stream = true;
            }
            self.enqueue_ready(stream_id);
            return Ok(());
        }

        // Trailers may not overtake body octets still waiting on a window, so
        // they queue behind them rather than going out now.
        if let Some(stream) = self.streams.get_mut(stream_id)
            && stream.has_pending_send()
        {
            stream.pending_trailers = Some(fields.to_vec());
            stream.send_end_stream = true;
            self.enqueue_ready(stream_id);
            return Ok(());
        }

        let mut block = BytesMut::new();
        self.hpack_enc.encode(fields, &mut block);
        self.queue_frame(&Frame::Headers {
            stream_id,
            block: block.freeze(),
            end_stream: true,
            end_headers: true,
        })?;
        self.apply_stream_event(
            stream_id,
            StreamEvent::SendHeaders { end_stream: true },
            "trailers",
        )
    }

    /// Answer a request we refuse to process with a bare status and END_STREAM.
    ///
    /// Used for the 431 case, where a response is more useful to the client than
    /// an RST_STREAM: the header list decoded cleanly, it was simply too big, and
    /// the client can act on being told so.
    fn reject_stream(&mut self, stream_id: StreamId, status: u16) -> Result<(), ConnectionError> {
        // The stream still has to exist before it can be answered.
        if self.streams.get_mut(stream_id).is_none() {
            match self.streams.open_peer(stream_id) {
                Ok(_) => {}
                Err(OpenRejection::Protocol(why)) => {
                    return Err(ConnectionError::new(
                        ErrorCode::ProtocolError,
                        format!("stream {}: {why}", stream_id.get()),
                    ));
                }
                Err(OpenRejection::Refused) => {
                    return self.write_rst_stream(stream_id, ErrorCode::RefusedStream);
                }
            }
            self.apply_stream_event(
                stream_id,
                StreamEvent::RecvHeaders { end_stream: true },
                "oversized request",
            )?;
        }
        self.send_response(stream_id, Response::status(status), true)
    }

    /// Account for inbound DATA at both flow-control levels and hand it to the
    /// responder.
    ///
    /// **Nothing is released here.** The credit goes back when the responder
    /// says it has taken the octets ([`ServiceEvent::BodyAccepted`]), which for
    /// a proxy means the upstream had the window for them. That deferral is the
    /// request-direction half of the bridge, and it is why a client cannot push
    /// a slow backend's worth of body into this process's memory.
    fn recv_data(
        &mut self,
        stream_id: StreamId,
        data: &Bytes,
        end_stream: bool,
    ) -> Result<(), ConnectionError> {
        let len = data.len() as u32;

        // An empty DATA that does not end its stream costs a parse and debits no
        // window, so it is unbounded work for free (§6; CVE-2019-9518). One
        // ending a stream is how a body ends and must never count.
        Self::check(self.guard.on_data(data.len(), end_stream))?;

        // The connection window is debited **before** the stream is looked up.
        // Octets that arrive for a stream we have already closed were still sent
        // and still count (§5.1); skipping them here desyncs us from the peer's
        // accounting in a way only a long-running connection would reveal.
        self.conn_recv_window.record(len).map_err(|code| {
            ConnectionError::new(code, "peer exceeded the connection flow-control window")
        })?;

        let overspent = match self.streams.lookup(stream_id) {
            Lookup::Idle => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!("DATA on stream {}, which was never opened", stream_id.get()),
                ));
            }
            // §5.1: DATA for a stream that is finished is a stream error, not
            // something to swallow. The connection-level accounting above has
            // already happened — the octets were sent regardless — and since no
            // responder will ever accept them, the credit goes back now rather
            // than waiting for a `BodyAccepted` that cannot come.
            Lookup::Closed => {
                self.release_conn_window(len)?;
                return self.write_rst_stream(stream_id, ErrorCode::StreamClosed);
            }
            Lookup::Live(stream) => {
                stream.data_received = stream.data_received.saturating_add(u64::from(len));
                stream.pending_conn_release = stream.pending_conn_release.saturating_add(len);
                stream.recv_window.record(len).err()
            }
        };
        if let Some(code) = overspent {
            return self.write_rst_stream(stream_id, code);
        }

        // Judge `content-length` at the moment the body ends, before the
        // terminal transition retires the stream and its counters (§8.1.2.6).
        if end_stream && let Some(code) = self.request_body_error(stream_id) {
            return self.write_rst_stream(stream_id, code);
        }

        self.apply_stream_event(stream_id, StreamEvent::RecvData { end_stream }, "data")?;

        // The responder owes us a `BodyAccepted` for these octets. An empty
        // DATA frame with END_STREAM still goes through: it is how a request
        // body ends, and the responder needs to know.
        if !data.is_empty() || end_stream {
            self.service.body(stream_id, data.clone(), end_stream);
        }
        // A stream retired by that END_STREAM cannot be told anything more, so
        // whatever credit it was holding comes back now.
        self.drain_reclaimed()
    }

    /// Credit the connection-level receive window directly, for octets no stream
    /// will ever account for.
    fn release_conn_window(&mut self, len: u32) -> Result<(), ConnectionError> {
        if let Some(increment) = self.conn_recv_window.release(len) {
            self.queue_frame(&Frame::WindowUpdate {
                stream_id: StreamId::CONNECTION,
                increment,
            })?;
        }
        Ok(())
    }

    /// The peer abandoned a stream.
    fn recv_rst_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), ConnectionError> {
        let answered = match self.streams.lookup(stream_id) {
            Lookup::Idle => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!(
                        "RST_STREAM on stream {}, which was never opened",
                        stream_id.get()
                    ),
                ));
            }
            // A reset crossing our own close. Nothing to undo — and nothing to
            // count either: the stream was already finished, so this cost us
            // nothing and is not evidence of anything.
            Lookup::Closed => return Ok(()),
            Lookup::Live(stream) => stream.response_started,
        };

        // The Rapid Reset accounting (§6, CVE-2023-44487). Whether we had
        // answered is the whole signal: cancelling a stream you have been
        // served is what browsers do all day, while abandoning streams before
        // they can be served is the attack's entire shape.
        Self::check(self.guard.on_reset(answered, Instant::now()))?;

        debug!(stream = stream_id.get(), ?error_code, "peer reset stream");
        self.summary.streams_reset += 1;
        self.streams.retire(stream_id);
        self.ready.retain(|id| *id != stream_id);
        // Tell the responder before anything else: on the proxy path this is
        // what cancels the upstream stream, and a cancel that arrives late is a
        // backend still working on a response nobody wants.
        self.service.cancel(stream_id, error_code);
        self.drain_reclaimed()?;
        Ok(())
    }

    /// Credit a send window (§6.9).
    fn recv_window_update(
        &mut self,
        stream_id: StreamId,
        increment: u32,
    ) -> Result<(), ConnectionError> {
        if increment == 0 {
            // A zero increment is meaningless in both scopes, but the blast
            // radius follows the stream id.
            return if stream_id.is_connection() {
                Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    "WINDOW_UPDATE with a zero increment on stream 0",
                ))
            } else {
                self.write_rst_stream(stream_id, ErrorCode::ProtocolError)
            };
        }

        if stream_id.is_connection() {
            self.conn_send_window.increase(increment).map_err(|code| {
                ConnectionError::new(code, "WINDOW_UPDATE overflows the connection window")
            })?;
            trace!(increment, "connection window credited");
            // Stalled streams are still sitting in the ring with their turn
            // order intact, so there is nothing to re-queue — the next
            // `pump_outbound` lap simply finds budget where it had none.
            return Ok(());
        }

        let overflowed = match self.streams.lookup(stream_id) {
            Lookup::Idle => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!(
                        "WINDOW_UPDATE on stream {}, which was never opened",
                        stream_id.get()
                    ),
                ));
            }
            // Credit for a stream that has finished; harmless.
            Lookup::Closed => return Ok(()),
            Lookup::Live(stream) => stream.send_window.increase(increment).is_err(),
        };
        if overflowed {
            // §6.9.1 scopes a window overflow to the stream when it arrives on
            // one, and to the connection only on stream 0.
            return self.write_rst_stream(stream_id, ErrorCode::FlowControlError);
        }
        self.enqueue_ready(stream_id);
        Ok(())
    }

    /// Drive `event` through the stream's state machine, promoting the one code
    /// that §5.1 scopes to the connection.
    fn apply_stream_event(
        &mut self,
        stream_id: StreamId,
        event: StreamEvent,
        what: &str,
    ) -> Result<(), ConnectionError> {
        match self.streams.apply(stream_id, event) {
            Ok(state) => {
                if state.is_closed() {
                    self.ready.retain(|id| *id != stream_id);
                    // The responder has to hear that this is over, or anything
                    // it holds per stream leaks — for the proxy that is a pool
                    // lease, and leaked leases look exactly like a backend at
                    // its concurrency limit.
                    self.service.finish(stream_id);
                    self.drain_reclaimed()?;
                }
                Ok(())
            }
            // "This stream was never opened" means the two ends disagree about
            // what exists, so nothing after it can be trusted (§5.1).
            Err(e) if e.code == ErrorCode::ProtocolError => Err(ConnectionError::new(
                ErrorCode::ProtocolError,
                format!("{what} on stream {}, which is idle", stream_id.get()),
            )),
            Err(e) => {
                // Everything else is one stream's problem.
                debug!(stream = stream_id.get(), code = ?e.code, "illegal {what}");
                self.streams.retire(stream_id);
                self.ready.retain(|id| *id != stream_id);
                self.pending_reset.push(e);
                Ok(())
            }
        }
    }

    /// Put a stream in the round-robin ring if it has work and is not already
    /// there.
    fn enqueue_ready(&mut self, stream_id: StreamId) {
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return;
        };
        if stream.queued || !stream.has_pending_send() {
            return;
        }
        stream.queued = true;
        self.ready.push_back(stream_id);
    }

    /// Write as much queued DATA as the two windows allow, fairly (design doc
    /// §4.1).
    ///
    /// Round-robin over the ready ring, with a per-visit ceiling of
    /// [`SEND_BUDGET`]. The budget is the point: without it a 10 MiB response
    /// would hold the connection until it finished and starve every small
    /// request behind it. Rotating after each visit whether or not the stream
    /// finished is what makes the interleaving fair rather than merely present.
    ///
    /// **Stalling must not cost a stream its turn.** The ring is rebuilt after
    /// every lap with the stalled streams first, in their original order, and
    /// the streams that were served behind them. The obvious alternative —
    /// rotating everyone to the back as they are visited — quietly starves: a
    /// stream that stalls lands *behind* whoever wrote in the same lap, so the
    /// same stream wins every scarce credit and the others never send a byte.
    /// The `flow_control.rs` two-stream test exists for exactly this.
    fn pump_outbound(&mut self) -> Result<(), ConnectionError> {
        // One lap at a time. A lap that writes nothing means every stream in the
        // ring is out of window, so there is nothing to do until the peer sends
        // a WINDOW_UPDATE — which is what "the sender is blocked" means, and
        // where the connection loop parks.
        while !self.ready.is_empty() {
            // Stop feeding a socket that is not draining. The windows already
            // bound DATA in flight, but only against a peer that reads; against
            // one that does not, this is what keeps the outbound buffer from
            // growing to the size of every response at once (ADR 0015).
            if self.out.len() >= MAX_WRITE_QUEUE {
                break;
            }
            let lap: Vec<StreamId> = self.ready.drain(..).collect();
            let mut stalled: VecDeque<StreamId> = VecDeque::new();
            let mut served: VecDeque<StreamId> = VecDeque::new();
            let mut wrote = false;

            for stream_id in lap {
                match self.visit_ready_stream(stream_id)? {
                    Visit::Stalled => stalled.push_back(stream_id),
                    Visit::Wrote { more } => {
                        wrote = true;
                        if more {
                            served.push_back(stream_id);
                        }
                    }
                    Visit::Finished => {}
                }
            }

            self.ready = stalled;
            self.ready.extend(served);
            for stream_id in self.ready.clone() {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.queued = true;
                }
            }
            // A stream retired mid-lap leaves no entry, so drop it from the ring.
            let streams = &mut self.streams;
            self.ready.retain(|id| streams.get_mut(*id).is_some());

            if !wrote {
                if !self.ready.is_empty() {
                    self.summary.flow_control_stalls += 1;
                }
                break;
            }
        }
        self.flush_pending_resets()
    }

    /// One visit in the round-robin: write at most [`SEND_BUDGET`] octets for
    /// `stream_id`. The caller owns the ring, so this only reports what happened.
    fn visit_ready_stream(&mut self, stream_id: StreamId) -> Result<Visit, ConnectionError> {
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return Ok(Visit::Finished); // retired while queued
        };
        stream.queued = false;

        // The four ceilings on one write: our fair-share budget, the credit this
        // stream has, the credit the whole connection has, and the largest frame
        // the peer will accept.
        let budget = SEND_BUDGET
            .min(stream.send_window.sendable())
            .min(self.conn_send_window.sendable())
            .min(self.peer_settings.max_frame_size as usize);

        let pending = stream.pending_send();
        if pending > 0 && budget == 0 {
            // Queued octets with nowhere to put them: the stall the milestone is
            // about.
            return Ok(Visit::Stalled);
        }

        let chunk = take_from_queue(&mut stream.send_queue, budget);
        let drained = stream.send_queue.is_empty();
        // Trailers own END_STREAM when there are any: a DATA frame that ended
        // the stream would leave nowhere to put them (§8.1).
        let trailers = if drained {
            stream.pending_trailers.take()
        } else {
            None
        };
        let end_stream = drained && stream.send_end_stream && trailers.is_none();
        if chunk.is_empty() && !end_stream && trailers.is_none() {
            return Ok(Visit::Finished);
        }
        // Clear the flag on the visit that actually spends it — not before, or a
        // body spanning several visits never ends. Leaving it set makes
        // `has_pending_send` true forever, which re-queues the stream and emits a
        // second, empty DATA with END_STREAM: a protocol error the peer sees, and
        // one that hides whenever the request itself carried END_STREAM, because
        // then the stream is retired before the second visit can happen.
        if end_stream {
            stream.send_end_stream = false;
        }
        // Taking the trailers hands them the END_STREAM the body did not carry,
        // so the flag has to be cleared here too — leaving it set would make
        // `send_trailers` believe there was still a body ahead of them and queue
        // them right back where they came from.
        if trailers.is_some() {
            stream.send_end_stream = false;
        }

        let len = chunk.len() as u32;
        if len > 0 {
            stream.send_window.consume(len).map_err(|code| {
                ConnectionError::new(code, "internal: overspent a stream window")
            })?;
            self.conn_send_window.consume(len).map_err(|code| {
                ConnectionError::new(code, "internal: overspent the connection window")
            })?;
        }

        if len > 0 || end_stream {
            self.queue_frame(&Frame::Data {
                stream_id,
                data: chunk,
                end_stream,
            })?;
            self.summary.data_bytes_sent += u64::from(len);
            self.apply_stream_event(
                stream_id,
                StreamEvent::SendData { end_stream },
                "response data",
            )?;
        }

        // These octets are now the client's problem, not ours. On the proxy path
        // this is the WINDOW_UPDATE the upstream has been waiting for — the
        // response-direction half of the bridge, and the reason a slow client
        // slows the backend down instead of filling our memory (§4.2).
        if len > 0 {
            self.service.released(stream_id, len);
        }

        if let Some(fields) = trailers {
            self.send_trailers(stream_id, &fields)?;
        }

        let more = self
            .streams
            .get_mut(stream_id)
            .is_some_and(|stream| stream.has_pending_send());
        Ok(Visit::Wrote { more })
    }

    /// Emit the RST_STREAMs queued by [`Self::apply_stream_event`], which cannot
    /// write from inside the state-machine call.
    fn flush_pending_resets(&mut self) -> Result<(), ConnectionError> {
        while let Some(err) = self.pending_reset.pop() {
            self.write_rst_stream(err.stream, err.code)?;
        }
        Ok(())
    }

    /// Abort one stream, leaving the connection up (§5.4.2).
    fn write_rst_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), ConnectionError> {
        debug!(stream = stream_id.get(), ?error_code, "resetting stream");
        self.summary.streams_reset += 1;
        self.streams.retire(stream_id);
        self.ready.retain(|id| *id != stream_id);
        // Our own reset cancels the responder just as the peer's does: whatever
        // was working on this stream should stop, and any receive credit it was
        // holding has to come back.
        self.service.cancel(stream_id, error_code);
        self.drain_reclaimed()?;
        self.queue_frame(&Frame::RstStream {
            stream_id,
            error_code,
        })
    }

    /// Acknowledge the peer's SETTINGS (§6.5.3).
    fn write_settings_ack(&mut self) -> Result<(), ConnectionError> {
        let ack = Frame::Settings {
            ack: true,
            params: Vec::new(),
        };
        self.queue_frame(&ack)
    }

    /// Encode `frame` into the outbound queue.
    ///
    /// **This never blocks.** Week 5 wrote straight to the socket with
    /// `write_all`, which parked the whole connection task whenever the peer
    /// stopped reading; the loop's write arm now owns the socket and this only
    /// appends (ADR 0015). Callers are therefore free to queue a frame from
    /// inside the frame handlers, where awaiting was always awkward.
    ///
    /// A frame *we* built failing to encode is a bug in this crate rather than
    /// anything the peer did, so it surfaces as `INTERNAL_ERROR`.
    fn queue_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        self.encode_buf.clear();
        self.codec
            .encode(frame, &mut self.encode_buf)
            .map_err(|e| {
                ConnectionError::new(
                    ErrorCode::InternalError,
                    format!("could not encode {:?}: {e}", frame.kind()),
                )
            })?;
        self.out.extend_from_slice(&self.encode_buf);
        Ok(())
    }

    /// Read until the buffer holds at least `n` octets. `false` if the peer
    /// closed or the daemon started draining first.
    async fn fill_at_least(&mut self, n: usize) -> bool {
        while self.read_buf.len() < n {
            // `tick` also drains the outbound queue, which matters here: our
            // SETTINGS and the connection-window bootstrap are sitting in it
            // while we wait for the client's preface, and a client that waits
            // for our SETTINGS before sending would otherwise deadlock.
            match self.tick().await {
                Ok(true) => {}
                Ok(false) | Err(_) => return false,
            }
        }
        true
    }
}

/// A header list formatted for a log line, with never-indexed values withheld.
///
/// A peer marks a field never-indexed precisely because its value is a secret
/// worth keeping out of a compression table (RFC 7541 §7.1.3) — writing it to a
/// log instead would defeat the point. The flag is already decoded, so honoring
/// it costs nothing.
struct Redacted<'a>(&'a [crate::hpack::Header]);

impl std::fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for header in self.0 {
            let name = String::from_utf8_lossy(&header.name);
            if header.sensitive {
                list.entry(&format_args!("{name}: <redacted>"));
            } else {
                list.entry(&format_args!(
                    "{name}: {}",
                    String::from_utf8_lossy(&header.value)
                ));
            }
        }
        list.finish()
    }
}

/// Take up to `budget` octets off the front of a send queue.
///
/// Zero-copy in the common case (ADR 0007): a chunk that fits whole is moved out
/// as-is, and one that does not is `split_to`'d, which is a refcount bump rather
/// than a copy. Coalescing across chunks would need an allocation to gain
/// nothing — the peer is just as happy with two frames.
pub(crate) fn take_from_queue(queue: &mut VecDeque<Bytes>, budget: usize) -> Bytes {
    let Some(front) = queue.front_mut() else {
        return Bytes::new();
    };
    if front.len() <= budget {
        queue.pop_front().expect("just peeked")
    } else {
        front.split_to(budget)
    }
}

#[cfg(test)]
mod tests {

    use tokio::io::DuplexStream;

    use super::*;
    use crate::hpack::Header;

    const TIMEOUT: Duration = Duration::from_secs(2);

    // ---- SETTINGS validation (§6.5.2) --------------------------------------

    #[test]
    fn apply_folds_known_parameters() {
        let mut settings = Settings::default();
        settings
            .apply(&[
                (setting_id::HEADER_TABLE_SIZE, 8192),
                (setting_id::ENABLE_PUSH, 0),
                (setting_id::MAX_CONCURRENT_STREAMS, 250),
                (setting_id::INITIAL_WINDOW_SIZE, 1 << 20),
                (setting_id::MAX_FRAME_SIZE, 32_768),
                (setting_id::MAX_HEADER_LIST_SIZE, 16_384),
            ])
            .expect("all values are legal");

        assert_eq!(settings.header_table_size, 8192);
        assert!(!settings.enable_push);
        assert_eq!(settings.max_concurrent_streams, Some(250));
        assert_eq!(settings.initial_window_size, 1 << 20);
        assert_eq!(settings.max_frame_size, 32_768);
        assert_eq!(settings.max_header_list_size, Some(16_384));
    }

    #[test]
    fn apply_ignores_unknown_identifiers() {
        // §6.5.3 — ignoring unknown settings is what lets the protocol grow.
        let mut settings = Settings::default();
        settings
            .apply(&[(0xbeef, 1), (setting_id::MAX_CONCURRENT_STREAMS, 7)])
            .expect("an unknown identifier is not an error");
        assert_eq!(settings.max_concurrent_streams, Some(7));
    }

    #[test]
    fn apply_rejects_a_non_boolean_enable_push() {
        let err = Settings::default()
            .apply(&[(setting_id::ENABLE_PUSH, 2)])
            .expect_err("ENABLE_PUSH must be 0 or 1");
        assert_eq!(err.code, ErrorCode::ProtocolError);
    }

    #[test]
    fn apply_rejects_an_oversized_initial_window() {
        let err = Settings::default()
            .apply(&[(setting_id::INITIAL_WINDOW_SIZE, MAX_WINDOW_SIZE as u32 + 1)])
            .expect_err("window above 2^31-1");
        // §6.5.2 scopes this one to flow control, not protocol.
        assert_eq!(err.code, ErrorCode::FlowControlError);
    }

    #[test]
    fn max_frame_size_bounds_are_enforced() {
        for value in [DEFAULT_MAX_FRAME_SIZE - 1, MAX_ALLOWED_FRAME_SIZE + 1] {
            let err = Settings::default()
                .apply(&[(setting_id::MAX_FRAME_SIZE, value)])
                .expect_err("out-of-range MAX_FRAME_SIZE");
            assert_eq!(err.code, ErrorCode::ProtocolError);
        }
        // Both ends of the range are legal.
        for value in [DEFAULT_MAX_FRAME_SIZE, MAX_ALLOWED_FRAME_SIZE] {
            Settings::default()
                .apply(&[(setting_id::MAX_FRAME_SIZE, value)])
                .expect("boundary values are legal");
        }
    }

    #[test]
    fn the_advertised_settings_turn_push_off() {
        let Frame::Settings { ack, params } = Settings::server().to_frame() else {
            panic!("expected a SETTINGS frame");
        };
        assert!(!ack);
        assert!(
            params.contains(&(setting_id::ENABLE_PUSH, 0)),
            "the server must advertise ENABLE_PUSH = 0, got {params:?}",
        );
    }

    // ---- a test peer speaking our own codec --------------------------------

    /// The client side of a connection, driven with our own codec — enough to
    /// exercise the handshake and the control frames without pulling in `h2`.
    struct TestPeer {
        io: DuplexStream,
        codec: FrameCodec,
        buf: BytesMut,
    }

    impl TestPeer {
        /// Start a `Connection` over a duplex and return its client end plus the
        /// task handle. The shutdown sender is returned so it stays alive.
        fn connect() -> (
            TestPeer,
            broadcast::Sender<()>,
            tokio::task::JoinHandle<ConnectionSummary>,
        ) {
            let (client, server) = tokio::io::duplex(64 * 1024);
            let (tx, rx) = broadcast::channel::<()>(1);
            let handle = tokio::spawn(Connection::new(server, rx).run());
            let peer = TestPeer {
                io: client,
                codec: FrameCodec::new(MAX_ALLOWED_FRAME_SIZE),
                buf: BytesMut::new(),
            };
            (peer, tx, handle)
        }

        async fn send_raw(&mut self, bytes: &[u8]) {
            self.io.write_all(bytes).await.expect("write to the server");
        }

        async fn send(&mut self, frame: &Frame) {
            let mut out = BytesMut::new();
            self.codec.encode(frame, &mut out).expect("encode");
            self.send_raw(&out).await;
        }

        /// The client half of the preface: the magic octets plus our SETTINGS.
        async fn send_preface(&mut self) {
            self.send_raw(PREFACE).await;
            self.send(&Frame::Settings {
                ack: false,
                params: vec![(setting_id::INITIAL_WINDOW_SIZE, 1 << 20)],
            })
            .await;
        }

        /// Send our preface and consume everything the server opens with, so a
        /// test can go straight to the frames it actually cares about.
        ///
        /// Three frames, not two: SETTINGS, then the stream-0 WINDOW_UPDATE that
        /// lifts the connection window off its 64 KiB default (§6.9.1 — SETTINGS
        /// cannot do it), then the ACK of ours.
        async fn complete_handshake(&mut self) {
            self.send_preface().await;
            assert!(matches!(
                self.recv().await,
                Frame::Settings { ack: false, .. }
            ));
            assert!(matches!(
                self.recv().await,
                Frame::WindowUpdate { stream_id, .. } if stream_id.is_connection()
            ));
            assert!(matches!(
                self.recv().await,
                Frame::Settings { ack: true, .. }
            ));
        }

        /// Read one frame, failing the test if none arrives in time.
        async fn recv(&mut self) -> Frame {
            tokio::time::timeout(TIMEOUT, async {
                loop {
                    let decoded = self.codec.decode(&mut self.buf).expect("decode");
                    if let Some(frame) = decoded {
                        return frame;
                    }
                    let n = self.io.read_buf(&mut self.buf).await.expect("read");
                    assert!(n > 0, "server closed the connection unexpectedly");
                }
            })
            .await
            .expect("timed out waiting for a frame")
        }

        /// Read frames until the connection closes, returning what arrived.
        async fn drain(&mut self) -> Vec<Frame> {
            tokio::time::timeout(TIMEOUT, async {
                let mut frames = Vec::new();
                loop {
                    match self.codec.decode(&mut self.buf).expect("decode") {
                        Some(frame) => frames.push(frame),
                        None => {
                            let n = self.io.read_buf(&mut self.buf).await.expect("read");
                            if n == 0 {
                                return frames;
                            }
                        }
                    }
                }
            })
            .await
            .expect("the server never closed the connection")
        }
    }

    // ---- lifecycle ----------------------------------------------------------

    #[tokio::test]
    async fn run_returns_on_shutdown_signal() {
        // Keep the client half alive and never write, so the reader's socket
        // read pends forever — the only way `run` can return is the shutdown
        // branch. That isolates the lifecycle we care about.
        //
        // `immediate()` because this test is about the *lifecycle*, not the
        // drain: with the default policy a connection this young owes a 2 s
        // grace to a request that might be on the wire, and waiting it out here
        // would only be measuring `DrainPolicy::default()`. `drain.rs` covers
        // the timing.
        let (_client, server) = tokio::io::duplex(1024);
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let handle = tokio::spawn(
            Connection::new(server, shutdown_rx)
                .with_drain_policy(DrainPolicy::immediate())
                .run(),
        );
        shutdown_tx.send(()).expect("a live receiver");

        tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("run() did not return after the shutdown signal")
            .expect("the reader task panicked");
    }

    #[tokio::test]
    async fn run_returns_when_peer_closes() {
        // Dropping the client half signals EOF; the reader should notice and
        // exit without any shutdown signal.
        let (client, server) = tokio::io::duplex(1024);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let handle = tokio::spawn(Connection::new(server, shutdown_rx).run());
        drop(client);

        tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("run() did not return after the peer closed")
            .expect("the reader task panicked");
    }

    // ---- the handshake ------------------------------------------------------

    #[tokio::test]
    async fn server_sends_settings_before_reading_anything() {
        // §3.4: the server's preface is its SETTINGS frame, and it must not wait
        // for the client's before sending it.
        let (mut peer, _tx, _handle) = TestPeer::connect();
        assert!(
            matches!(peer.recv().await, Frame::Settings { ack: false, .. }),
            "the server must open with a non-ACK SETTINGS frame",
        );
    }

    #[tokio::test]
    async fn client_settings_are_acknowledged() {
        let (mut peer, _tx, _handle) = TestPeer::connect();
        peer.send_preface().await;

        assert!(matches!(
            peer.recv().await,
            Frame::Settings { ack: false, .. }
        ));
        assert!(matches!(peer.recv().await, Frame::WindowUpdate { .. }));
        assert!(
            matches!(peer.recv().await, Frame::Settings { ack: true, .. }),
            "the server must acknowledge our SETTINGS (§6.5.3)",
        );
    }

    #[tokio::test]
    async fn a_completed_handshake_is_reported() {
        let (mut peer, _tx, handle) = TestPeer::connect();
        peer.complete_handshake().await;
        drop(peer);

        let summary = tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("connection did not finish")
            .expect("task panicked");
        assert!(summary.handshake_completed);
        assert_eq!(summary.frames_received, 1, "one SETTINGS frame");
    }

    #[tokio::test]
    async fn a_bad_preface_closes_without_a_goaway() {
        // An HTTP/1.1 request cannot parse GOAWAY, so §3.4 lets us just close.
        let (mut peer, _tx, handle) = TestPeer::connect();
        peer.send_raw(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n\r\n")
            .await;

        let frames = peer.drain().await;
        assert!(
            frames.iter().all(|f| matches!(
                f,
                Frame::Settings { ack: false, .. } | Frame::WindowUpdate { .. }
            )),
            "expected only our own preface, got {frames:?}",
        );
        let summary = tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("connection did not finish")
            .expect("task panicked");
        assert!(!summary.handshake_completed);
    }

    // ---- control frames -----------------------------------------------------

    #[tokio::test]
    async fn ping_is_answered_with_an_ack_carrying_the_same_payload() {
        let (mut peer, _tx, _handle) = TestPeer::connect();
        peer.complete_handshake().await;

        let payload = *b"keepaliv";
        peer.send(&Frame::Ping {
            data: payload,
            ack: false,
        })
        .await;

        assert_eq!(
            peer.recv().await,
            Frame::Ping {
                data: payload,
                ack: true
            },
            "PING must be echoed back with ACK set (§6.7)",
        );
    }

    #[tokio::test]
    async fn a_peer_goaway_closes_the_connection_quietly() {
        let (mut peer, _tx, handle) = TestPeer::connect();
        peer.complete_handshake().await;

        peer.send(&Frame::GoAway {
            last_stream_id: StreamId::CONNECTION,
            error_code: ErrorCode::NoError,
            debug_data: Bytes::new(),
        })
        .await;

        let trailing = peer.drain().await;
        assert!(
            trailing.is_empty(),
            "responding to a peer's GOAWAY with more frames is wrong, got {trailing:?}",
        );
        tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("connection did not close after GOAWAY")
            .expect("task panicked");
    }

    // ---- protocol errors become GOAWAY (ADR 0008) ---------------------------

    /// Run the handshake, then feed `frames` and return the GOAWAY the server
    /// must answer with.
    async fn goaway_after(frames: &[Frame]) -> Frame {
        let (mut peer, _tx, _handle) = TestPeer::connect();
        peer.complete_handshake().await;
        for frame in frames {
            peer.send(frame).await;
        }
        // Skip past anything stream-scoped the setup provoked on the way — the
        // question here is only which *connection* error comes out.
        loop {
            match peer.recv().await {
                Frame::RstStream { .. } | Frame::Headers { .. } | Frame::Data { .. } => continue,
                frame => return frame,
            }
        }
    }

    #[tokio::test]
    async fn an_illegal_setting_produces_a_goaway() {
        let goaway = goaway_after(&[Frame::Settings {
            ack: false,
            params: vec![(setting_id::ENABLE_PUSH, 3)],
        }])
        .await;
        assert!(
            matches!(
                goaway,
                Frame::GoAway {
                    error_code: ErrorCode::ProtocolError,
                    ..
                }
            ),
            "expected GOAWAY(PROTOCOL_ERROR), got {goaway:?}",
        );
    }

    #[tokio::test]
    async fn a_zero_connection_window_update_produces_a_goaway() {
        let goaway = goaway_after(&[Frame::WindowUpdate {
            stream_id: StreamId::CONNECTION,
            increment: 0,
        }])
        .await;
        assert!(
            matches!(
                goaway,
                Frame::GoAway {
                    error_code: ErrorCode::ProtocolError,
                    ..
                }
            ),
            "a zero increment on stream 0 is a connection error (§6.9), got {goaway:?}",
        );
    }

    #[tokio::test]
    async fn interrupting_a_header_block_produces_a_goaway() {
        // §4.3: nothing may come between HEADERS and its CONTINUATIONs — an
        // interleaved frame would corrupt the connection's shared HPACK state.
        let goaway = goaway_after(&[
            Frame::Headers {
                stream_id: StreamId::new(1),
                block: Bytes::from_static(b"\x82"),
                end_stream: false,
                end_headers: false,
            },
            Frame::Ping {
                data: [0; 8],
                ack: false,
            },
        ])
        .await;
        assert!(
            matches!(
                goaway,
                Frame::GoAway {
                    error_code: ErrorCode::ProtocolError,
                    ..
                }
            ),
            "expected GOAWAY(PROTOCOL_ERROR), got {goaway:?}",
        );
    }

    #[tokio::test]
    async fn a_continuation_may_follow_its_headers() {
        // The other side of the rule above: the legal sequence must survive.
        let (mut peer, _tx, _handle) = TestPeer::connect();
        peer.complete_handshake().await;

        // `:method: GET` + `:scheme: http` here, `:path: /` in the continuation:
        // one request, split so it only validates if the fragments are rejoined.
        peer.send(&Frame::Headers {
            stream_id: StreamId::new(1),
            block: Bytes::from_static(b"\x82\x86"),
            end_stream: true,
            end_headers: false,
        })
        .await;
        peer.send(&Frame::Continuation {
            stream_id: StreamId::new(1),
            block: Bytes::from_static(b"\x84"),
            end_headers: true,
        })
        .await;
        // A PING after the completed block still gets its ACK, which it only can
        // if the connection is healthy.
        peer.send(&Frame::Ping {
            data: [1; 8],
            ack: false,
        })
        .await;

        let mut saw_response = false;
        loop {
            match peer.recv().await {
                Frame::Headers { stream_id, .. } => {
                    assert_eq!(stream_id, StreamId::new(1));
                    saw_response = true;
                }
                Frame::Data { .. } => {}
                frame => {
                    assert_eq!(
                        frame,
                        Frame::Ping {
                            data: [1; 8],
                            ack: true
                        },
                    );
                    break;
                }
            }
        }
        assert!(saw_response, "the rejoined block was a complete request");
    }

    #[tokio::test]
    async fn a_goaway_reports_the_highest_stream_seen() {
        let goaway = goaway_after(&[
            Frame::Headers {
                stream_id: StreamId::new(3),
                block: Bytes::new(),
                end_stream: true,
                end_headers: true,
            },
            Frame::WindowUpdate {
                stream_id: StreamId::CONNECTION,
                increment: 0,
            },
        ])
        .await;
        let Frame::GoAway { last_stream_id, .. } = goaway else {
            panic!("expected GOAWAY, got {goaway:?}");
        };
        assert_eq!(
            last_stream_id,
            StreamId::new(3),
            "GOAWAY must name the last stream we processed (§6.8)",
        );
    }

    #[tokio::test]
    async fn a_malformed_frame_produces_a_goaway() {
        let (mut peer, _tx, _handle) = TestPeer::connect();
        peer.complete_handshake().await;

        // RST_STREAM with a 3-octet payload: a frame-size error (§6.4).
        peer.send_raw(&[0, 0, 3, 0x3, 0, 0, 0, 0, 1, 0, 0, 0]).await;

        let goaway = peer.recv().await;
        assert!(
            matches!(
                goaway,
                Frame::GoAway {
                    error_code: ErrorCode::FrameSizeError,
                    ..
                }
            ),
            "expected GOAWAY(FRAME_SIZE_ERROR), got {goaway:?}",
        );
    }

    // ---- HPACK on the connection --------------------------------------------

    #[tokio::test]
    async fn a_request_header_block_is_decoded() {
        let (mut peer, _tx, handle) = TestPeer::connect();
        peer.complete_handshake().await;

        let mut encoder = HpackEncoder::new(Settings::default().header_table_size as usize);
        let mut block = BytesMut::new();
        encoder.encode(
            &[
                Header::new(":method", "GET"),
                Header::new(":scheme", "https"),
                Header::new(":path", "/"),
                Header::new(":authority", "example.com"),
            ],
            &mut block,
        );
        peer.send(&Frame::Headers {
            stream_id: StreamId::new(1),
            block: block.freeze(),
            end_stream: true,
            end_headers: true,
        })
        .await;

        // Close the peer and read the tally back off the summary. It has to be
        // EOF rather than the shutdown signal: the reader's select is biased
        // toward shutdown, so signalling would race the buffered frame.
        drop(peer);
        let summary = tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("the connection did not finish")
            .expect("the reader task panicked");
        assert_eq!(summary.header_blocks_decoded, 1);
    }

    /// The block is one HPACK unit however many frames carry it, so a block
    /// split across CONTINUATION must decode identically to an unsplit one.
    #[tokio::test]
    async fn a_header_block_split_across_continuation_decodes_once() {
        let (mut peer, _tx, handle) = TestPeer::connect();
        peer.complete_handshake().await;

        let mut encoder = HpackEncoder::new(Settings::default().header_table_size as usize);
        let mut block = BytesMut::new();
        encoder.encode(
            &[
                Header::new(":method", "GET"),
                Header::new(":path", "/split"),
                Header::new("x-custom", "value"),
            ],
            &mut block,
        );
        let block = block.freeze();
        // Cut the block mid-representation, which is legal: fragment
        // boundaries have nothing to do with field boundaries (§4.3).
        let (head, tail) = block.split_at(3);
        peer.send(&Frame::Headers {
            stream_id: StreamId::new(1),
            block: Bytes::copy_from_slice(head),
            end_stream: false,
            end_headers: false,
        })
        .await;
        peer.send(&Frame::Continuation {
            stream_id: StreamId::new(1),
            block: Bytes::copy_from_slice(tail),
            end_headers: true,
        })
        .await;

        drop(peer);
        let summary = tokio::time::timeout(TIMEOUT, handle)
            .await
            .expect("the connection did not finish")
            .expect("the reader task panicked");
        assert_eq!(
            summary.header_blocks_decoded, 1,
            "the two fragments are one block",
        );
    }

    #[tokio::test]
    async fn a_corrupt_header_block_produces_a_compression_goaway() {
        // Index 62 with an empty dynamic table: a reference to an entry that
        // was never inserted, which means the tables have diverged.
        let goaway = goaway_after(&[Frame::Headers {
            stream_id: StreamId::new(1),
            block: Bytes::from_static(&[0xbe]),
            end_stream: true,
            end_headers: true,
        }])
        .await;
        assert!(
            matches!(
                goaway,
                Frame::GoAway {
                    error_code: ErrorCode::CompressionError,
                    ..
                }
            ),
            "expected GOAWAY(COMPRESSION_ERROR), got {goaway:?}",
        );
    }

    /// The CONTINUATION-flood guard: a peer that never sets END_HEADERS must
    /// not be able to grow our buffer without bound (design doc §6).
    #[tokio::test]
    async fn an_endless_header_block_is_cut_off() {
        let (mut peer, _tx, _handle) = TestPeer::connect();
        peer.complete_handshake().await;

        peer.send(&Frame::Headers {
            stream_id: StreamId::new(1),
            block: Bytes::from(vec![0u8; 1024]),
            end_stream: false,
            end_headers: false,
        })
        .await;
        // Keep going past the cap. The server stops reading once it errors, so
        // ignore write failures rather than racing it.
        for _ in 0..(MAX_HEADER_BLOCK_BYTES / 1024) {
            let mut out = BytesMut::new();
            peer.codec
                .encode(
                    &Frame::Continuation {
                        stream_id: StreamId::new(1),
                        block: Bytes::from(vec![0u8; 1024]),
                        end_headers: false,
                    },
                    &mut out,
                )
                .expect("encode");
            if peer.io.write_all(&out).await.is_err() {
                break;
            }
        }

        let goaway = peer
            .drain()
            .await
            .into_iter()
            .find(|f| matches!(f, Frame::GoAway { .. }))
            .expect("the flood should have been cut off with a GOAWAY");
        assert!(
            matches!(
                goaway,
                Frame::GoAway {
                    error_code: ErrorCode::EnhanceYourCalm,
                    ..
                }
            ),
            "expected GOAWAY(ENHANCE_YOUR_CALM), got {goaway:?}",
        );
    }

    #[test]
    fn a_sensitive_value_is_not_written_to_the_log() {
        let headers = [
            Header::new(":path", "/"),
            Header::sensitive("authorization", "Bearer hunter2"),
        ];
        let rendered = format!("{:?}", Redacted(&headers));
        assert!(rendered.contains(":path: /"), "{rendered}");
        assert!(
            !rendered.contains("hunter2"),
            "a never-indexed value must not reach the log: {rendered}",
        );
        assert!(rendered.contains("authorization: <redacted>"), "{rendered}");
    }

    #[tokio::test]
    async fn the_advertised_settings_bound_the_header_list() {
        let settings = Settings::server();
        assert_eq!(
            settings.max_header_list_size,
            Some(MAX_HEADER_LIST_SIZE),
            "the bomb guard must be advertised, or a peer cannot respect it",
        );
        let params = match settings.to_frame() {
            Frame::Settings { params, .. } => params,
            other => panic!("expected SETTINGS, got {other:?}"),
        };
        assert!(
            params.contains(&(setting_id::MAX_HEADER_LIST_SIZE, MAX_HEADER_LIST_SIZE)),
            "MAX_HEADER_LIST_SIZE missing from {params:?}",
        );
    }

    // ---- the milestone: a real h2 client completes the handshake -------------

    #[tokio::test]
    async fn a_real_h2_client_completes_the_handshake() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (_tx, rx) = broadcast::channel::<()>(1);
        let server = tokio::spawn(Connection::new(server_io, rx).run());

        let (send_request, mut connection) =
            tokio::time::timeout(TIMEOUT, h2::client::handshake(client_io))
                .await
                .expect("h2 handshake timed out")
                .expect("h2 handshake failed");

        // Taken before the connection future is spawned, since it borrows it.
        let mut ping_pong = connection.ping_pong().expect("a fresh connection");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });

        // `ready()` resolves only once our SETTINGS have arrived and been
        // applied — this resolving *is* the week-3 milestone.
        let send_request = tokio::time::timeout(TIMEOUT, send_request.ready())
            .await
            .expect("h2 client never became ready")
            .expect("h2 client reported a connection error");

        // And the connection is genuinely healthy afterwards: h2's own PING
        // round-trips against our control-frame handling.
        tokio::time::timeout(TIMEOUT, ping_pong.ping(h2::Ping::opaque()))
            .await
            .expect("PING timed out")
            .expect("PING failed");

        drop(send_request);
        driver.abort();
        server.abort();
    }
}
