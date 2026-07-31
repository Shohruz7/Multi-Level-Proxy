//! Connection layer: one HTTP/2 connection, owned by one task.
//!
//! Owns the preface + SETTINGS handshake and ack lifecycle (RFC 9113 §3.4,
//! §6.5), the frame-reader task that demuxes inbound frames to per-stream
//! handlers over bounded mpsc channels (the §2.2 concurrency model — the
//! channel bound is the backpressure mechanism), the outbound mux, PING, and
//! GOAWAY handling.
//!
//! Also home of the error model's protocol side (ADR 0008): the
//! connection-error (→ GOAWAY, connection dies) vs stream-error
//! (→ RST_STREAM, connection lives) distinction as types, carrying the RFC
//! §7 error codes they must emit.
//!
//! Week 3 landed the handshake and the connection-control frames (SETTINGS,
//! PING, GOAWAY). Week 4 adds the HPACK seam: header blocks are reassembled
//! across HEADERS + CONTINUATION and decoded through a *per-connection* codec,
//! because the dynamic table spans every block the peer sends. Per-stream
//! demux/mux is week 5, and the graceful GOAWAY drain and Rapid-Reset / flood
//! accounting are week 7 (design doc §6).

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, trace, warn};

use crate::flow::MAX_WINDOW_SIZE;
use crate::frame::{DEFAULT_MAX_FRAME_SIZE, Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};
use crate::hpack::{HpackDecoder, HpackEncoder};
use crate::stream::StreamId;

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
/// without ever finishing a block. Week 7's CONTINUATION-flood mitigation
/// builds on this.
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
    /// how large a header list we will decode (design doc §6).
    pub fn server() -> Self {
        Settings {
            enable_push: false,
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
// Task topology (§2.2) — prototyped in week 2, populated in week 5.
//
// One connection is owned by one task. A single *reader* owns the read half,
// decodes frames, and dispatches each to the matching per-stream handler over a
// bounded mpsc channel keyed by `StreamId` (a `Dispatcher`). Handlers send
// outbound frames back over one shared channel to a *writer* that owns the write
// half and serializes the mux. Connection-control frames (SETTINGS/PING/GOAWAY
// on stream 0) are handled by the reader directly.
//
// The channel bound is the backpressure mechanism (design doc §4.2): once a
// stream's handler falls `STREAM_CHANNEL_BOUND` frames behind, the reader's
// send blocks, so it stops pulling from the socket, so the peer stalls — which
// in week 6 is coupled to the upstream's flow-control window. See
// docs/adr/0009 for the choice of this topology over the alternatives.
// ---------------------------------------------------------------------------

/// Bound on each per-stream inbound channel — the backpressure knob (§4.2).
pub const STREAM_CHANNEL_BOUND: usize = 64;

/// A message from the connection reader to a single stream's handler (inbound
/// demux).
#[derive(Debug)]
pub enum ToStream {
    /// A frame addressed to this stream.
    Frame(Frame),
    /// The stream is being torn down (RST_STREAM received or connection going
    /// away); the handler should finish and drop.
    Reset(ErrorCode),
}

/// A message from a stream's handler back to the connection writer (outbound
/// mux).
#[derive(Debug)]
pub enum FromStream {
    /// A frame to serialize onto the connection.
    Frame(Frame),
}

/// The reader task's routing table: which bounded channel feeds each live
/// stream's handler. Entries are added as streams open and removed as they
/// close (week 5).
pub type Dispatcher = HashMap<StreamId, mpsc::Sender<ToStream>>;

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

/// One HTTP/2 connection, driven by its reader task.
///
/// Week 3 brings the connection up: it sends the server's SETTINGS, validates
/// the client preface, and runs the frame loop, answering the connection-control
/// frames (SETTINGS/ACK, PING, GOAWAY) itself. Stream-addressed frames decode
/// and are counted, but dispatch to per-stream handlers over a [`Dispatcher`] is
/// week 5, and the graceful GOAWAY drain is week 7 — both slot into the loop
/// below without disturbing this lifecycle.
pub struct Connection<IO> {
    io: IO,
    shutdown: broadcast::Receiver<()>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    codec: FrameCodec,
    /// What we advertised. Our `MAX_FRAME_SIZE` is what bounds the *decoder*:
    /// it is the limit we imposed on the peer.
    local_settings: Settings,
    /// The peer's settings as applied. Bounds what we may *send* (week 5, when
    /// there is an outbound path to bound).
    peer_settings: Settings,
    /// Set while a header block is open — a HEADERS or CONTINUATION arrived
    /// without END_HEADERS, so only a CONTINUATION on this stream may follow
    /// (§4.3).
    open_header_block: Option<StreamId>,
    /// Partial header block, accumulated across CONTINUATION frames. Empty
    /// whenever [`Self::open_header_block`] is `None`.
    header_block: BytesMut,
    /// Inbound HPACK state. One per connection, not per stream: the dynamic
    /// table spans every header block the peer sends, which is why a header
    /// block is indivisible and why a decode failure kills the connection
    /// (design doc §3.3).
    hpack_dec: HpackDecoder,
    /// Outbound HPACK state. Nothing encodes responses until week 5, but the
    /// peer's `HEADER_TABLE_SIZE` is negotiated now, at handshake, so the
    /// encoder has to exist to receive it.
    hpack_enc: HpackEncoder,
    /// Highest client-initiated stream seen, for GOAWAY's last-stream-id (§6.8).
    last_peer_stream_id: StreamId,
    summary: ConnectionSummary,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Connection<IO> {
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
        Connection {
            io,
            shutdown,
            read_buf: BytesMut::with_capacity(16 * 1024),
            write_buf: BytesMut::with_capacity(1024),
            codec: FrameCodec::new(local_settings.max_frame_size),
            local_settings,
            peer_settings: Settings::default(),
            open_header_block: None,
            header_block: BytesMut::new(),
            hpack_dec: HpackDecoder::new(
                local_settings.header_table_size as usize,
                local_settings.max_header_list_size.map(|n| n as usize),
            ),
            hpack_enc: HpackEncoder::new(Settings::default().header_table_size as usize),
            last_peer_stream_id: StreamId::CONNECTION,
            summary: ConnectionSummary::default(),
        }
    }

    /// Run the connection until the peer closes it, a shutdown signal arrives,
    /// or a protocol error forces it down.
    ///
    /// A connection error is reported to the peer as GOAWAY before the socket
    /// closes — the ADR-0008 error model in practice.
    pub async fn run(mut self) -> ConnectionSummary {
        if let Stop::Failed(err) = self.drive().await {
            warn!(code = ?err.code, reason = %err.debug, "connection error; sending GOAWAY");
            let go_away = Frame::GoAway {
                last_stream_id: self.last_peer_stream_id,
                error_code: err.code,
                debug_data: Bytes::from(err.debug.into_bytes()),
            };
            // A failed write here just means the peer is already gone.
            let _ = self.write_frame(&go_away).await;
        }
        self.summary
    }

    /// The handshake, then the frame loop.
    async fn drive(&mut self) -> Stop {
        // Our half of the connection preface is a SETTINGS frame, sent
        // immediately and before anything is read (§3.4).
        let settings = self.local_settings.to_frame();
        if let Err(e) = self.write_frame(&settings).await {
            debug!(error = %e, "could not send server SETTINGS");
            return Stop::Quiet;
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
                let decoded = self.codec.decode(&mut self.read_buf);
                match decoded {
                    Ok(Some(frame)) => {
                        self.summary.frames_received += 1;
                        match self.handle_frame(frame).await {
                            Ok(Next::Continue) => {}
                            Ok(Next::Close) => return Stop::Quiet,
                            Err(e) => return Stop::Failed(e),
                        }
                    }
                    // A partial frame: leave it buffered and read more.
                    Ok(None) => break,
                    Err(e) => {
                        return Stop::Failed(ConnectionError::new(e.code(), e.to_string()));
                    }
                }
            }

            if !self.fill().await {
                return Stop::Quiet;
            }
        }
    }

    /// Act on one decoded frame.
    ///
    /// Connection-control frames are answered here. Stream-addressed frames are
    /// validated and counted; routing them to per-stream handlers is week 5.
    async fn handle_frame(&mut self, frame: Frame) -> Result<Next, ConnectionError> {
        // A header block is atomic: once HEADERS or CONTINUATION arrives without
        // END_HEADERS, nothing but a CONTINUATION on that same stream may follow
        // (§4.3). Interleaving anything else corrupts the shared HPACK state, so
        // it is a connection error even though only one stream looks affected.
        if let Some(open) = self.open_header_block {
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

        match &frame {
            Frame::Settings { ack: false, params } => {
                self.peer_settings.apply(params)?;
                // Their HEADER_TABLE_SIZE bounds *our* encoder's dynamic table;
                // the change is announced back as a size update on our next
                // header block (RFC 7541 §6.3).
                self.hpack_enc
                    .set_max_table_size(self.peer_settings.header_table_size as usize);
                trace!(?self.peer_settings, "applied peer SETTINGS");
                // Every SETTINGS frame must be acknowledged (§6.5.3). The first
                // one completes the handshake.
                self.write_settings_ack().await?;
                self.summary.handshake_completed = true;
            }
            Frame::Settings { ack: true, .. } => {
                // The peer accepted ours. Enforcing SETTINGS_TIMEOUT when this
                // never arrives is week-7 hardening.
                trace!("peer acknowledged our SETTINGS");
            }
            Frame::Ping { data, ack: false } => {
                let pong = Frame::Ping {
                    data: *data,
                    ack: true,
                };
                self.write_frame(&pong).await.map_err(io_as_conn_error)?;
            }
            Frame::Ping { ack: true, .. } => {
                // A reply to a keepalive we sent; nothing to do until week 7
                // starts measuring them.
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
                // §6.9: a zero increment is a protocol violation, scoped to the
                // connection on stream 0 and to the stream otherwise. Only the
                // connection-level half is actionable before week 5's per-stream
                // state exists.
                if *increment == 0 && stream_id.is_connection() {
                    return Err(ConnectionError::new(
                        ErrorCode::ProtocolError,
                        "WINDOW_UPDATE with a zero increment on stream 0",
                    ));
                }
                // Window accounting itself is week 5.
                trace!(stream = stream_id.get(), increment, "WINDOW_UPDATE");
            }
            Frame::Headers {
                stream_id,
                block,
                end_headers,
                ..
            }
            | Frame::Continuation {
                stream_id,
                block,
                end_headers,
            } => {
                self.note_peer_stream(*stream_id);
                self.read_header_block(*stream_id, block, *end_headers)?;
            }
            Frame::Data { stream_id, .. } | Frame::RstStream { stream_id, .. } => {
                self.note_peer_stream(*stream_id);
                // Week 5 dispatches these to per-stream handlers over the
                // `Dispatcher`; for now the connection stays up and drops them.
                trace!(
                    stream = stream_id.get(),
                    kind = ?frame.kind(),
                    "stream frame received (dispatch lands in week 5)",
                );
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

    /// Track the highest client-initiated stream seen, for GOAWAY's
    /// last-stream-id (§6.8).
    fn note_peer_stream(&mut self, stream_id: StreamId) {
        if stream_id.is_client_initiated() && stream_id > self.last_peer_stream_id {
            self.last_peer_stream_id = stream_id;
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
            fragment.clone()
        } else {
            self.header_block.extend_from_slice(fragment);
            if !end_headers {
                trace!(
                    stream = stream_id.get(),
                    buffered = self.header_block.len(),
                    "header block continues",
                );
                return Ok(());
            }
            self.header_block.split().freeze()
        };

        // A failure here is fatal to the connection, not just this stream: the
        // dynamic table is shared, so we can no longer decode anything the peer
        // sends (design doc §3.3). `HpackError` converts to the GOAWAY-bearing
        // `ConnectionError` accordingly.
        let headers = self.hpack_dec.decode(&block)?;
        self.summary.header_blocks_decoded += 1;
        debug!(
            stream = stream_id.get(),
            fields = headers.len(),
            headers = ?Redacted(&headers),
            "decoded header block",
        );
        // Week 5 hands this list to the stream's handler; today it is proof the
        // request was understood.
        Ok(())
    }

    /// Acknowledge the peer's SETTINGS (§6.5.3).
    async fn write_settings_ack(&mut self) -> Result<(), ConnectionError> {
        let ack = Frame::Settings {
            ack: true,
            params: Vec::new(),
        };
        self.write_frame(&ack).await.map_err(io_as_conn_error)
    }

    /// Encode `frame` and flush it to the peer.
    ///
    /// Writes go straight out from the reader task for now; the dedicated writer
    /// task that serializes the outbound mux arrives with per-stream handlers in
    /// week 5.
    async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
        self.write_buf.clear();
        // Failing to encode a frame *we* built is a bug in this crate, not
        // something the peer did.
        self.codec
            .encode(frame, &mut self.write_buf)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await
    }

    /// Read until the buffer holds at least `n` octets. `false` if the peer
    /// closed or the daemon started draining first.
    async fn fill_at_least(&mut self, n: usize) -> bool {
        while self.read_buf.len() < n {
            if !self.fill().await {
                return false;
            }
        }
        true
    }

    /// One read into the reassembly buffer, racing the shutdown signal.
    /// `false` on EOF, read error, or shutdown.
    async fn fill(&mut self) -> bool {
        tokio::select! {
            biased;
            // Closed channel (Err) or a value both mean "drain now".
            _ = self.shutdown.recv() => false,
            read = self.io.read_buf(&mut self.read_buf) => {
                match read {
                    Ok(0) => false,  // peer closed the connection
                    Ok(_) => true,
                    Err(e) => {
                        debug!(error = %e, "read failed; closing connection");
                        false
                    }
                }
            }
        }
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

/// A failed write means the transport is gone; report it as an internal
/// connection error so the caller unwinds the same way as a protocol fault.
fn io_as_conn_error(e: std::io::Error) -> ConnectionError {
    ConnectionError::new(ErrorCode::InternalError, e.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        let (_client, server) = tokio::io::duplex(1024);
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let handle = tokio::spawn(Connection::new(server, shutdown_rx).run());
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
        assert!(
            matches!(peer.recv().await, Frame::Settings { ack: true, .. }),
            "the server must acknowledge our SETTINGS (§6.5.3)",
        );
    }

    #[tokio::test]
    async fn a_completed_handshake_is_reported() {
        let (mut peer, _tx, handle) = TestPeer::connect();
        peer.send_preface().await;
        let _ = peer.recv().await; // server SETTINGS
        let _ = peer.recv().await; // ACK of ours
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
            frames
                .iter()
                .all(|f| matches!(f, Frame::Settings { ack: false, .. })),
            "expected only our own preface SETTINGS, got {frames:?}",
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
        peer.send_preface().await;
        let _ = peer.recv().await; // server SETTINGS
        let _ = peer.recv().await; // ACK of ours

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
        peer.send_preface().await;
        let _ = peer.recv().await;
        let _ = peer.recv().await;

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
        peer.send_preface().await;
        let _ = peer.recv().await;
        let _ = peer.recv().await;
        for frame in frames {
            peer.send(frame).await;
        }
        peer.recv().await
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
        peer.send_preface().await;
        let _ = peer.recv().await;
        let _ = peer.recv().await;

        peer.send(&Frame::Headers {
            stream_id: StreamId::new(1),
            block: Bytes::from_static(b"\x82"),
            end_stream: false,
            end_headers: false,
        })
        .await;
        peer.send(&Frame::Continuation {
            stream_id: StreamId::new(1),
            block: Bytes::from_static(b"\x86"),
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
        assert_eq!(
            peer.recv().await,
            Frame::Ping {
                data: [1; 8],
                ack: true
            },
        );
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
        peer.send_preface().await;
        let _ = peer.recv().await;
        let _ = peer.recv().await;

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
        peer.send_preface().await;
        let _ = peer.recv().await; // our SETTINGS
        let _ = peer.recv().await; // the ACK of theirs

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
        peer.send_preface().await;
        let _ = peer.recv().await;
        let _ = peer.recv().await;

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
        peer.send_preface().await;
        let _ = peer.recv().await;
        let _ = peer.recv().await;

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
