//! Framing layer: raw connection bytes ⇄ typed HTTP/2 frames.
//!
//! Owns the 9-octet frame header, the `Frame` enum (DATA, HEADERS, SETTINGS,
//! WINDOW_UPDATE, RST_STREAM, PING, GOAWAY, CONTINUATION), and the streaming
//! codec with its reassembly buffer — the parser only advances on a complete
//! frame, so partial frames across TCP segment boundaries never consume input
//! (RFC 9113 §4, §6; design doc §3.2).
//!
//! Per-type size/placement validation lives here (which lengths are legal,
//! which stream IDs, which flags); *semantic* stream rules live in
//! [`crate::stream`]. Header block fragments pass through opaque — HPACK is
//! deliberately above this layer, in [`crate::hpack`], because the proxy
//! re-encodes headers with separate table state per side.
//!
//! Implemented in week 3; every frame type is round-tripped against the `h2`
//! crate (encode ours → decode theirs, and vice versa) and fuzzed.
