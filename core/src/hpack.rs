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

use bytes::{Bytes, BytesMut};

use crate::conn::ConnectionError;

/// A decoded header field. Names and values are owned `Bytes` so they outlive
/// the frame buffer they were parsed from.
///
/// `sensitive` marks a field (e.g. `authorization`, `cookie`) that must be sent
/// with the never-indexed representation so it is never placed in the dynamic
/// table (RFC 7541 §7.1.3).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Header {
    pub name: Bytes,
    pub value: Bytes,
    pub sensitive: bool,
}

impl Header {
    pub fn new(name: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Header {
            name: name.into(),
            value: value.into(),
            sensitive: false,
        }
    }
}

/// The HPACK encoder: turns a header list into a header block, maintaining the
/// encoder-side dynamic table. Table state is added in week 4.
#[derive(Debug, Default)]
pub struct HpackEncoder {}

impl HpackEncoder {
    /// An encoder whose dynamic table is bounded by `max_table_size` octets
    /// (SETTINGS_HEADER_TABLE_SIZE).
    pub fn new(max_table_size: usize) -> Self {
        let _ = max_table_size;
        HpackEncoder {}
    }

    /// Encode `headers` into `out`. Implemented in week 4.
    pub fn encode(&mut self, headers: &[Header], out: &mut BytesMut) {
        let _ = (headers, out);
        todo!("HPACK encoding — week 4")
    }
}

/// The HPACK decoder: turns a header block back into a header list, maintaining
/// the decoder-side dynamic table and enforcing `MAX_HEADER_LIST_SIZE`. Table
/// state is added in week 4.
#[derive(Debug, Default)]
pub struct HpackDecoder {}

impl HpackDecoder {
    /// A decoder whose dynamic table is bounded by `max_table_size` octets, and
    /// which rejects any header list larger than `max_header_list_size`
    /// (`None` = unbounded) — the HPACK-bomb guard.
    pub fn new(max_table_size: usize, max_header_list_size: Option<usize>) -> Self {
        let _ = (max_table_size, max_header_list_size);
        HpackDecoder {}
    }

    /// Decode `block` into a header list. A desync or bound violation is a
    /// connection `COMPRESSION_ERROR`. Implemented in week 4.
    pub fn decode(&mut self, block: &Bytes) -> Result<Vec<Header>, ConnectionError> {
        let _ = block;
        todo!("HPACK decoding — week 4")
    }
}
