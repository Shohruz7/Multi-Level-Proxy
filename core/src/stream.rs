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
