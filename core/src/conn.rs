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
//! Handshake lands in week 3, demux/mux in week 5, graceful GOAWAY drain and
//! the Rapid-Reset / flood accounting in week 7 (design doc §6).

use std::collections::HashMap;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{broadcast, mpsc};

use crate::frame::Frame;
use crate::stream::StreamId;

/// The client connection preface (RFC 9113 §3.4). A client opens every HTTP/2
/// connection by sending these 24 octets, immediately followed by its first
/// SETTINGS frame.
pub const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

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
    /// Fold a received SETTINGS payload (id/value pairs) into these settings,
    /// validating each parameter's legal range and rejecting violations with
    /// the connection error the RFC mandates. Implemented in week 3 with the
    /// handshake.
    pub fn apply(&mut self, params: &[(u16, u32)]) -> Result<(), ConnectionError> {
        let _ = params;
        todo!("SETTINGS validation + apply — week 3")
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

/// One HTTP/2 connection, driven by its reader task.
///
/// Week 2 wires only the lifecycle: the reader loop runs, races the shutdown
/// signal, and exits cleanly when the peer closes or shutdown is requested. The
/// preface + SETTINGS handshake lands in week 3, frame decode and per-stream
/// dispatch (over a [`Dispatcher`]) in week 5, and the graceful GOAWAY drain in
/// week 7 — each slotting into the loop below without disturbing this lifecycle.
pub struct Connection<IO> {
    io: IO,
    shutdown: broadcast::Receiver<()>,
    read_buf: BytesMut,
}

impl<IO: AsyncRead + Unpin> Connection<IO> {
    /// Build a connection over an established (TLS-terminated) byte stream.
    /// `shutdown` fires — as a value or by the sender being dropped — when the
    /// daemon is draining.
    pub fn new(io: IO, shutdown: broadcast::Receiver<()>) -> Self {
        Connection {
            io,
            shutdown,
            read_buf: BytesMut::with_capacity(16 * 1024),
        }
    }

    /// Run the reader task until the peer closes the connection or a shutdown
    /// signal arrives.
    ///
    /// Week 2 does no protocol work: it fills the reassembly buffer and exits
    /// cleanly. Week 5 decodes frames from `read_buf` with a [`FrameCodec`] and
    /// dispatches them; week 7 turns the shutdown branch into a GOAWAY drain.
    ///
    /// [`FrameCodec`]: crate::frame::FrameCodec
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                // Closed channel (Err) or a value both mean "drain now".
                _ = self.shutdown.recv() => break,
                read = self.io.read_buf(&mut self.read_buf) => {
                    match read {
                        Ok(0) => break,   // peer closed the connection
                        Ok(_) => {
                            // Week 5: decode complete frames out of `read_buf`
                            // and dispatch each to its stream handler.
                        }
                        Err(_) => break,  // read error → the connection dies
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_returns_on_shutdown_signal() {
        // Keep the client half alive and never write, so the reader's socket
        // read pends forever — the only way `run` can return is the shutdown
        // branch. That isolates the lifecycle we care about.
        let (_client, server) = tokio::io::duplex(1024);
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let handle = tokio::spawn(Connection::new(server, shutdown_rx).run());
        shutdown_tx.send(()).expect("a live receiver");

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
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

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("run() did not return after the peer closed")
            .expect("the reader task panicked");
    }
}
