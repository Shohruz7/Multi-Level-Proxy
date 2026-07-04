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
