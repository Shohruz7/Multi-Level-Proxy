//! HPACK header compression: encoder, decoder, and their table state.
//!
//! Owns the integer/string primitives, the static Huffman table, the 61-entry
//! static table, and the dynamic table with its +32-byte-per-entry accounting
//! and FIFO eviction, plus the four representations including never-indexed
//! (RFC 7541; design doc §3.3).
//!
//! Encoder and decoder each hold dynamic-table state that must stay in
//! lockstep with the peer — a single desync corrupts every later header block,
//! which is why all failures here are *connection* errors
//! (`COMPRESSION_ERROR`). Enforces `MAX_HEADER_LIST_SIZE` during decoding (the
//! HPACK-bomb guard, design doc §6).
//!
//! Implemented in week 4; ground truth is RFC 7541 Appendix C vectors plus
//! multi-block differential sequences against the `h2` crate.
