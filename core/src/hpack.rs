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
//! Ground truth is RFC 7541 Appendix C (`core/tests/hpack_vectors.rs`) plus
//! multi-block differential sequences against the `h2` crate
//! (`core/tests/hpack_differential.rs`). See ADR 0012 for the design.

mod huffman;
mod static_table;

use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};

use crate::conn::{ConnectionError, ErrorCode};

/// Per-entry overhead in the dynamic table's size accounting (RFC 7541 §4.1).
/// It stands in for the bookkeeping a real implementation needs, so that the
/// table's octet budget bounds *memory*, not just header bytes.
const ENTRY_OVERHEAD: usize = 32;

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

    /// The same field, marked never-indexed (§7.1.3).
    pub fn sensitive(name: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Header {
            sensitive: true,
            ..Header::new(name, value)
        }
    }

    /// This field's cost against `SETTINGS_MAX_HEADER_LIST_SIZE` (RFC 9113
    /// §6.5.2, which reuses the §4.1 accounting).
    fn list_size(&self) -> usize {
        self.name.len() + self.value.len() + ENTRY_OVERHEAD
    }
}

/// What can go wrong decoding a header block.
///
/// The two variants differ in *blast radius*, which is why they are not one
/// type: a compression fault means the shared table state is unknowable and the
/// connection cannot continue, while an oversized list is a property of one
/// request that the connection survives.
#[derive(Clone, Debug, thiserror::Error)]
pub enum HpackError {
    /// The block is malformed, or references a table entry that does not exist.
    /// Unrecoverable: our dynamic table and the peer's have diverged, so every
    /// later block on this connection would decode to garbage.
    #[error("HPACK compression error: {0}")]
    Compression(String),
    /// The decoded list exceeded the `MAX_HEADER_LIST_SIZE` we advertised — the
    /// HPACK-bomb guard (design doc §6). The block was still decoded to the end,
    /// so table state is intact and only this message need be rejected.
    ///
    /// Week 4's connection layer treats this as a connection error for want of
    /// a stream layer to reject it on; week 5 answers it with RST_STREAM and a
    /// 431 instead, which is what the RFC intends.
    #[error("header list of {size} octets exceeds the {limit}-octet limit")]
    HeaderListTooLarge { size: usize, limit: usize },
}

impl From<HpackError> for ConnectionError {
    fn from(e: HpackError) -> ConnectionError {
        let code = match e {
            HpackError::Compression(_) => ErrorCode::CompressionError,
            HpackError::HeaderListTooLarge { .. } => ErrorCode::EnhanceYourCalm,
        };
        ConnectionError::new(code, e.to_string())
    }
}

fn compression(msg: impl Into<String>) -> HpackError {
    HpackError::Compression(msg.into())
}

// ---------------------------------------------------------------------------
// Primitives: integers (§5.1) and string literals (§5.2).
// ---------------------------------------------------------------------------

/// A read cursor over a header block. Slicing yields `Bytes` that share the
/// block's allocation, so a literal header costs no copy (ADR 0007).
struct Cursor<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a Bytes) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn next_byte(&mut self) -> Result<u8, HpackError> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| compression("block ended mid-representation"))?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, len: usize) -> Result<Bytes, HpackError> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| {
                compression(format!(
                    "string of {len} octets runs past the end of the block"
                ))
            })?;
        let slice = self.buf.slice(self.pos..end);
        self.pos = end;
        Ok(slice)
    }
}

/// Decode an integer whose `prefix_bits` low bits are already in `first`
/// (RFC 7541 §5.1).
///
/// Bounded twice over, because both bounds are reachable from a hostile peer: a
/// continuation run that never terminates, and one that terminates on a value
/// too large to be a length or an index.
fn decode_int(first: u8, prefix_bits: u32, cur: &mut Cursor) -> Result<usize, HpackError> {
    debug_assert!((4..=7).contains(&prefix_bits));
    let mask = (1u32 << prefix_bits) - 1;
    let mut value = u64::from(u32::from(first) & mask);
    if value < u64::from(mask) {
        return Ok(value as usize);
    }

    let mut shift = 0u32;
    loop {
        let byte = cur.next_byte()?;
        value += u64::from(byte & 0x7f) << shift;
        if value > u64::from(u32::MAX) {
            return Err(compression("integer overflows 32 bits"));
        }
        if byte & 0x80 == 0 {
            return Ok(value as usize);
        }
        shift += 7;
        if shift > 28 {
            return Err(compression("integer continuation is too long"));
        }
    }
}

/// Encode `value` into `prefix_bits` low bits, with `high_bits` set in the
/// leading octet to select the representation (§5.1).
fn encode_int(value: usize, prefix_bits: u32, high_bits: u8, out: &mut BytesMut) {
    debug_assert!((4..=7).contains(&prefix_bits));
    let mask = ((1u32 << prefix_bits) - 1) as usize;
    if value < mask {
        out.put_u8(high_bits | value as u8);
        return;
    }
    out.put_u8(high_bits | mask as u8);
    let mut rest = value - mask;
    while rest >= 0x80 {
        out.put_u8((rest & 0x7f) as u8 | 0x80);
        rest >>= 7;
    }
    out.put_u8(rest as u8);
}

/// Decode a string literal: an H bit, a 7-bit-prefix length, then the octets
/// (§5.2). A non-Huffman string is sliced out of the block without copying.
fn decode_string(cur: &mut Cursor) -> Result<Bytes, HpackError> {
    let first = cur.next_byte()?;
    let huffman = first & 0x80 != 0;
    let len = decode_int(first, 7, cur)?;
    let raw = cur.take(len)?;
    if huffman {
        Ok(Bytes::from(huffman::decode(&raw)?))
    } else {
        Ok(raw)
    }
}

/// Encode a string literal, Huffman-coding it whenever that does not make it
/// longer.
///
/// Ties go to Huffman, which is what RFC 7541's own examples do — see
/// `:status: 307` in Appendix C.6.2, three octets either way and coded anyway.
/// The choice is the encoder's to make (§5.2); matching the RFC's makes the
/// appendices double as encoder tests (ADR 0012).
fn encode_string(s: &[u8], huffman_allowed: bool, out: &mut BytesMut) {
    let coded_len = huffman::encoded_len(s);
    if huffman_allowed && coded_len <= s.len() {
        encode_int(coded_len, 7, 0x80, out);
        huffman::encode(s, out);
    } else {
        encode_int(s.len(), 7, 0x00, out);
        out.put_slice(s);
    }
}

// ---------------------------------------------------------------------------
// The dynamic table (§2.3.2) and the shared index space (§2.3.3).
// ---------------------------------------------------------------------------

/// The dynamic table: a FIFO of recently sent fields, bounded by an octet
/// budget rather than an entry count.
#[derive(Debug)]
struct DynamicTable {
    /// Newest first, which is also the order the wire indexes them in.
    entries: VecDeque<(Bytes, Bytes)>,
    size: usize,
    max_size: usize,
}

impl DynamicTable {
    fn new(max_size: usize) -> Self {
        DynamicTable {
            entries: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    /// Entry `i` counting from the newest (`0`-based).
    fn get(&self, i: usize) -> Option<&(Bytes, Bytes)> {
        self.entries.get(i)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Add a field, evicting the oldest entries until it fits (§4.4).
    ///
    /// An entry larger than the whole table is not an error: it empties the
    /// table and is simply not stored, leaving both peers in the same state.
    fn insert(&mut self, name: Bytes, value: Bytes) {
        let cost = name.len() + value.len() + ENTRY_OVERHEAD;
        self.evict_to(self.max_size.saturating_sub(cost));
        if cost > self.max_size {
            return;
        }
        self.entries.push_front((name, value));
        self.size += cost;
    }

    /// Apply a dynamic table size update (§6.3).
    fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        self.evict_to(max_size);
    }

    fn evict_to(&mut self, target: usize) {
        while self.size > target {
            let (name, value) = self
                .entries
                .pop_back()
                .expect("a non-empty table has an oldest entry");
            self.size -= name.len() + value.len() + ENTRY_OVERHEAD;
        }
    }
}

/// Resolve a wire index across the static and dynamic tables (§2.3.3).
fn lookup(table: &DynamicTable, index: usize) -> Result<(Bytes, Bytes), HpackError> {
    if index == 0 {
        return Err(compression("index 0 is not a valid table entry"));
    }
    if index <= static_table::LEN {
        return static_table::get(index).ok_or_else(|| compression("bad static index"));
    }
    table
        .get(index - static_table::LEN - 1)
        .cloned()
        .ok_or_else(|| {
            compression(format!(
                "index {index} is past the end of the dynamic table ({} entries)",
                table.len()
            ))
        })
}

/// The best table entry for a field, used by the encoder to pick a
/// representation.
enum Match {
    /// Name and value both match at this index — one octet on the wire.
    Full(usize),
    /// Only the name matches; the value is sent as a literal.
    Name(usize),
    None,
}

/// Find the lowest-indexed entry matching `name`/`value`, preferring a full
/// match over a name-only one and the static table over the dynamic.
fn find(table: &DynamicTable, name: &[u8], value: &[u8]) -> Match {
    let mut name_only = None;

    for (i, (n, v)) in static_table::ENTRIES.iter().enumerate() {
        if n.as_bytes() == name {
            if v.as_bytes() == value {
                return Match::Full(i + 1);
            }
            name_only.get_or_insert(i + 1);
        }
    }
    for (i, (n, v)) in table.entries.iter().enumerate() {
        if n == name {
            let index = static_table::LEN + i + 1;
            if v == value {
                return Match::Full(index);
            }
            name_only.get_or_insert(index);
        }
    }

    match name_only {
        Some(index) => Match::Name(index),
        None => Match::None,
    }
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// The HPACK decoder: turns a header block back into a header list, maintaining
/// the decoder-side dynamic table and enforcing `MAX_HEADER_LIST_SIZE`.
#[derive(Debug)]
pub struct HpackDecoder {
    table: DynamicTable,
    /// The `SETTINGS_HEADER_TABLE_SIZE` we advertised. The peer may shrink its
    /// table below this with a size update, but never grow past it (§6.3).
    capacity: usize,
    max_header_list_size: Option<usize>,
}

impl Default for HpackDecoder {
    fn default() -> Self {
        HpackDecoder::new(4096, None)
    }
}

impl HpackDecoder {
    /// A decoder whose dynamic table is bounded by `max_table_size` octets, and
    /// which rejects any header list larger than `max_header_list_size`
    /// (`None` = unbounded) — the HPACK-bomb guard.
    pub fn new(max_table_size: usize, max_header_list_size: Option<usize>) -> Self {
        HpackDecoder {
            table: DynamicTable::new(max_table_size),
            capacity: max_table_size,
            max_header_list_size,
        }
    }

    /// The dynamic table's entries, newest first.
    ///
    /// Introspection for the RFC-vector tests: Appendix C states the table's
    /// contents after every step, and checking them is what turns those vectors
    /// from a round-trip test into a test of *table* state (design doc §3.3).
    pub fn table_entries(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.table.entries.iter().map(|(n, v)| (&n[..], &v[..]))
    }

    /// The dynamic table's size in octets, per the §4.1 accounting.
    pub fn table_size(&self) -> usize {
        self.table.size
    }

    /// Decode `block` into a header list.
    ///
    /// A desync or bound violation is reported, but never by stopping early:
    /// the whole block is processed either way, because the peer has already
    /// applied its own table insertions and skipping ours would desync every
    /// later block on the connection.
    pub fn decode(&mut self, block: &Bytes) -> Result<Vec<Header>, HpackError> {
        let mut cur = Cursor::new(block);
        let mut headers = Vec::new();
        let mut list_size = 0usize;
        let mut oversized = None;
        // A size update is only legal before the first field line of a block
        // (RFC 9113 §4.3.1).
        let mut size_update_allowed = true;

        while !cur.is_empty() {
            let first = cur.next_byte()?;
            let header = match first {
                // Indexed header field (§6.1).
                _ if first & 0x80 != 0 => {
                    let index = decode_int(first, 7, &mut cur)?;
                    let (name, value) = lookup(&self.table, index)?;
                    Header {
                        name,
                        value,
                        sensitive: false,
                    }
                }
                // Literal with incremental indexing (§6.2.1).
                _ if first & 0xc0 == 0x40 => {
                    let header = self.literal(first, 6, &mut cur, false)?;
                    self.table.insert(header.name.clone(), header.value.clone());
                    header
                }
                // Dynamic table size update (§6.3).
                _ if first & 0xe0 == 0x20 => {
                    if !size_update_allowed {
                        return Err(compression(
                            "dynamic table size update after a header field",
                        ));
                    }
                    let size = decode_int(first, 5, &mut cur)?;
                    if size > self.capacity {
                        return Err(compression(format!(
                            "dynamic table size update to {size} exceeds the advertised \
                             maximum of {}",
                            self.capacity
                        )));
                    }
                    self.table.set_max_size(size);
                    continue;
                }
                // Literal never indexed (§6.2.3).
                _ if first & 0xf0 == 0x10 => self.literal(first, 4, &mut cur, true)?,
                // Literal without indexing (§6.2.2).
                _ => self.literal(first, 4, &mut cur, false)?,
            };

            size_update_allowed = false;
            list_size += header.list_size();
            match self.max_header_list_size {
                // Past the limit we stop *accumulating* but keep decoding, so
                // the table still tracks the peer's.
                Some(limit) if list_size > limit => {
                    oversized.get_or_insert(limit);
                    headers.clear();
                    headers.shrink_to_fit();
                }
                _ => headers.push(header),
            }
        }

        match oversized {
            Some(limit) => Err(HpackError::HeaderListTooLarge {
                size: list_size,
                limit,
            }),
            None => Ok(headers),
        }
    }

    /// A literal representation: an optional name index, then the strings.
    fn literal(
        &mut self,
        first: u8,
        prefix_bits: u32,
        cur: &mut Cursor,
        sensitive: bool,
    ) -> Result<Header, HpackError> {
        let index = decode_int(first, prefix_bits, cur)?;
        let name = if index == 0 {
            decode_string(cur)?
        } else {
            lookup(&self.table, index)?.0
        };
        Ok(Header {
            name,
            value: decode_string(cur)?,
            sensitive,
        })
    }
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// The HPACK encoder: turns a header list into a header block, maintaining the
/// encoder-side dynamic table.
///
/// The indexing policy is the one RFC 7541's own examples use — index a full
/// match, otherwise emit a literal *with* incremental indexing so the field is
/// available to later blocks, and Huffman-code any string that gets shorter for
/// it. Sensitive fields are the exception: never indexed, never stored
/// (§7.1.3). See ADR 0012.
#[derive(Debug)]
pub struct HpackEncoder {
    table: DynamicTable,
    /// A size update to emit before the next block, set when the peer changes
    /// `SETTINGS_HEADER_TABLE_SIZE` (§6.3).
    pending_size_update: Option<usize>,
    huffman: bool,
}

impl Default for HpackEncoder {
    fn default() -> Self {
        HpackEncoder::new(4096)
    }
}

impl HpackEncoder {
    /// An encoder whose dynamic table is bounded by `max_table_size` octets
    /// (the peer's `SETTINGS_HEADER_TABLE_SIZE`).
    pub fn new(max_table_size: usize) -> Self {
        HpackEncoder {
            table: DynamicTable::new(max_table_size),
            pending_size_update: None,
            huffman: true,
        }
    }

    /// Turn Huffman coding off.
    ///
    /// Only the RFC-vector tests use this: Appendix C gives each example twice,
    /// once coded and once not, and this lets both halves be checked against the
    /// same encoder. On a connection it stays on.
    pub fn set_huffman(&mut self, on: bool) {
        self.huffman = on;
    }

    /// The dynamic table's size in octets. Paired with
    /// [`HpackDecoder::table_size`], this is how a test asserts the two halves
    /// have not drifted.
    pub fn table_size(&self) -> usize {
        self.table.size
    }

    /// Adopt a new table bound after the peer's SETTINGS change it. The change
    /// is announced to the peer as a size update on the next block (§6.3).
    pub fn set_max_table_size(&mut self, max_table_size: usize) {
        if max_table_size != self.table.max_size {
            self.table.set_max_size(max_table_size);
            self.pending_size_update = Some(max_table_size);
        }
    }

    /// Encode `headers` into `out`.
    pub fn encode(&mut self, headers: &[Header], out: &mut BytesMut) {
        if let Some(size) = self.pending_size_update.take() {
            encode_int(size, 5, 0x20, out);
        }
        for header in headers {
            self.encode_one(header, out);
        }
    }

    fn encode_one(&mut self, header: &Header, out: &mut BytesMut) {
        let (name, value) = (&header.name[..], &header.value[..]);

        // A sensitive field must never reach the dynamic table — not ours, and
        // not (via an indexed representation) the peer's (§7.1.3).
        if header.sensitive {
            match find(&self.table, name, value) {
                Match::Full(i) | Match::Name(i) => encode_int(i, 4, 0x10, out),
                Match::None => {
                    out.put_u8(0x10);
                    encode_string(name, self.huffman, out);
                }
            }
            encode_string(value, self.huffman, out);
            return;
        }

        match find(&self.table, name, value) {
            Match::Full(i) => encode_int(i, 7, 0x80, out),
            Match::Name(i) => {
                encode_int(i, 6, 0x40, out);
                encode_string(value, self.huffman, out);
                self.table.insert(header.name.clone(), header.value.clone());
            }
            Match::None => {
                out.put_u8(0x40);
                encode_string(name, self.huffman, out);
                encode_string(value, self.huffman, out);
                self.table.insert(header.name.clone(), header.value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_int_of(bytes: &[u8], prefix_bits: u32) -> Result<usize, HpackError> {
        let buf = Bytes::copy_from_slice(bytes);
        let mut cur = Cursor::new(&buf);
        let first = cur.next_byte()?;
        decode_int(first, prefix_bits, &mut cur)
    }

    /// RFC 7541 Appendix C.1: the three worked integer examples.
    #[test]
    fn rfc_c1_integer_vectors() {
        // C.1.1 — 10 in a 5-bit prefix.
        assert_eq!(decode_int_of(&[0b0000_1010], 5).unwrap(), 10);
        // C.1.2 — 1337 in a 5-bit prefix.
        assert_eq!(decode_int_of(&[0b0001_1111, 0x9a, 0x0a], 5).unwrap(), 1337);
        // C.1.3 — 42 starting at an octet boundary (8-bit value, 7-bit prefix
        // is the closest this codec offers; the RFC uses a full octet).
        assert_eq!(decode_int_of(&[42], 7).unwrap(), 42);

        for (value, prefix, expected) in [
            (10usize, 5u32, vec![0b0000_1010]),
            (1337, 5, vec![0b0001_1111, 0x9a, 0x0a]),
            (42, 7, vec![42]),
        ] {
            let mut out = BytesMut::new();
            encode_int(value, prefix, 0, &mut out);
            assert_eq!(&out[..], &expected[..], "encoding {value}");
        }
    }

    #[test]
    fn integers_round_trip_at_every_prefix_width() {
        for prefix in 4..=7u32 {
            for value in [0usize, 1, 30, 31, 127, 128, 255, 16_383, 16_384, 1 << 24] {
                let mut out = BytesMut::new();
                encode_int(value, prefix, 0, &mut out);
                assert_eq!(
                    decode_int_of(&out, prefix).unwrap(),
                    value,
                    "{value} at prefix {prefix}"
                );
            }
        }
    }

    /// A continuation run that never terminates must be rejected, not looped on.
    #[test]
    fn rejects_unterminated_and_oversized_integers() {
        assert!(decode_int_of(&[0xff, 0x80, 0x80, 0x80], 7).is_err());
        assert!(decode_int_of(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 7).is_err());
    }

    /// RFC 7541 C.2.1: a literal header field with incremental indexing, name
    /// given as a literal.
    #[test]
    fn rfc_c21_literal_with_indexing() {
        let block = Bytes::from_static(&[
            0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k', b'e', b'y', 0x0d, b'c',
            b'u', b's', b't', b'o', b'm', b'-', b'h', b'e', b'a', b'd', b'e', b'r',
        ]);
        let mut dec = HpackDecoder::new(4096, None);
        let headers = dec.decode(&block).expect("decode");
        assert_eq!(headers, vec![Header::new("custom-key", "custom-header")]);
        // The field was added to the table: 10 + 13 + 32.
        assert_eq!(dec.table.len(), 1);
        assert_eq!(dec.table.size, 55);
    }

    /// RFC 7541 C.2.2: literal without indexing, name from the static table.
    #[test]
    fn rfc_c22_literal_without_indexing() {
        let block = Bytes::from_static(&[
            0x04, 0x0c, b'/', b's', b'a', b'm', b'p', b'l', b'e', b'/', b'p', b'a', b't', b'h',
        ]);
        let mut dec = HpackDecoder::new(4096, None);
        assert_eq!(
            dec.decode(&block).expect("decode"),
            vec![Header::new(":path", "/sample/path")]
        );
        assert_eq!(dec.table.len(), 0, "this representation must not index");
    }

    /// RFC 7541 C.2.3: never-indexed, which is what marks a field sensitive.
    #[test]
    fn rfc_c23_never_indexed_marks_sensitive() {
        let block = Bytes::from_static(&[
            0x10, 0x08, b'p', b'a', b's', b's', b'w', b'o', b'r', b'd', 0x06, b's', b'e', b'c',
            b'r', b'e', b't',
        ]);
        let mut dec = HpackDecoder::new(4096, None);
        let headers = dec.decode(&block).expect("decode");
        assert_eq!(headers, vec![Header::sensitive("password", "secret")]);
        assert!(headers[0].sensitive);
        assert_eq!(dec.table.len(), 0, "a sensitive field must not be indexed");
    }

    /// RFC 7541 C.2.4: an indexed field, the one-octet case.
    #[test]
    fn rfc_c24_indexed_field() {
        let block = Bytes::from_static(&[0x82]);
        let mut dec = HpackDecoder::new(4096, None);
        assert_eq!(
            dec.decode(&block).expect("decode"),
            vec![Header::new(":method", "GET")]
        );
    }

    #[test]
    fn static_table_matches_the_rfc_at_its_edges() {
        assert_eq!(static_table::LEN, 61);
        assert_eq!(lookup(&DynamicTable::new(0), 1).unwrap().0, ":authority");
        assert_eq!(lookup(&DynamicTable::new(0), 8).unwrap().1, "200");
        assert_eq!(
            lookup(&DynamicTable::new(0), 16).unwrap(),
            (
                Bytes::from_static(b"accept-encoding"),
                Bytes::from_static(b"gzip, deflate")
            )
        );
        assert_eq!(
            lookup(&DynamicTable::new(0), 61).unwrap().0,
            "www-authenticate"
        );
    }

    #[test]
    fn index_zero_and_out_of_range_indices_are_errors() {
        let table = DynamicTable::new(4096);
        assert!(lookup(&table, 0).is_err());
        assert!(lookup(&table, 62).is_err(), "the dynamic table is empty");
        assert!(lookup(&table, usize::MAX).is_err());
    }

    #[test]
    fn dynamic_table_evicts_oldest_first() {
        // Two entries of 32 + 1 + 1 = 34 octets each fit; a third evicts the
        // first.
        let mut table = DynamicTable::new(70);
        table.insert(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        table.insert(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        assert_eq!(table.len(), 2);
        assert_eq!(table.size, 68);

        table.insert(Bytes::from_static(b"c"), Bytes::from_static(b"3"));
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(0).unwrap().0, "c", "newest is index 0");
        assert_eq!(table.get(1).unwrap().0, "b");
    }

    /// §4.4: an entry too big for the table empties it and is dropped — an
    /// awkward case that must not be an error, or the peers would disagree.
    #[test]
    fn entry_larger_than_the_table_empties_it() {
        let mut table = DynamicTable::new(64);
        table.insert(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        assert_eq!(table.len(), 1);

        table.insert(Bytes::from_static(b"big"), Bytes::from(vec![b'x'; 100]));
        assert_eq!(table.len(), 0);
        assert_eq!(table.size, 0);
    }

    #[test]
    fn size_update_shrinks_the_table_and_must_come_first() {
        // Two entries, then a size update to 0, which evicts both.
        let mut dec = HpackDecoder::new(4096, None);
        let mut enc = HpackEncoder::new(4096);
        let mut block = BytesMut::new();
        enc.encode(&[Header::new("a", "1"), Header::new("b", "2")], &mut block);
        dec.decode(&block.split().freeze()).expect("decode");
        assert_eq!(dec.table.len(), 2);

        dec.decode(&Bytes::from_static(&[0x20])).expect("size 0");
        assert_eq!(dec.table.len(), 0, "a size update to 0 empties the table");

        // The same update *after* a field is a protocol violation (§4.3.1).
        let mut dec = HpackDecoder::new(4096, None);
        let err = dec
            .decode(&Bytes::from_static(&[0x82, 0x20]))
            .expect_err("size update after a field");
        assert!(matches!(err, HpackError::Compression(_)), "{err:?}");
    }

    #[test]
    fn size_update_above_the_advertised_maximum_is_rejected() {
        let mut dec = HpackDecoder::new(256, None);
        // 0x3f 0xe1 0x1f = size update to 4096.
        let err = dec
            .decode(&Bytes::from_static(&[0x3f, 0xe1, 0x1f]))
            .expect_err("update past the advertised capacity");
        assert!(matches!(err, HpackError::Compression(_)), "{err:?}");
    }

    #[test]
    fn truncated_blocks_are_errors_not_panics() {
        let mut dec = HpackDecoder::new(4096, None);
        for block in [
            &[0x40][..],             // literal, no name
            &[0x40, 0x05, b'a'][..], // name length past the end
            &[0x00, 0x00][..],       // name, then no value
            &[0xff][..],             // an index whose continuation is missing
        ] {
            assert!(
                dec.decode(&Bytes::copy_from_slice(block)).is_err(),
                "{block:02x?} should not decode"
            );
        }
    }

    /// The bomb guard: the list is capped, but the block is still decoded to
    /// the end so the table keeps tracking the peer's.
    #[test]
    fn oversized_header_list_is_rejected_but_leaves_the_table_in_step() {
        let mut enc = HpackEncoder::new(4096);
        let headers = [
            Header::new("a", "x".repeat(100)),
            Header::new("b", "y".repeat(100)),
        ];
        let mut block = BytesMut::new();
        enc.encode(&headers, &mut block);

        let mut dec = HpackDecoder::new(4096, Some(150));
        let err = dec.decode(&block.freeze()).expect_err("over the limit");
        match err {
            HpackError::HeaderListTooLarge { size, limit } => {
                assert_eq!(limit, 150);
                assert_eq!(size, 133 + 133);
            }
            other => panic!("expected HeaderListTooLarge, got {other:?}"),
        }
        // Both fields were still indexed, so a later block that references them
        // decodes correctly.
        assert_eq!(dec.table.len(), 2);
        assert_eq!(dec.table.get(0).unwrap().0, "b");
    }

    #[test]
    fn encoder_indexes_a_repeated_field_on_the_second_use() {
        let mut enc = HpackEncoder::new(4096);
        let headers = [Header::new("custom", "value")];

        let mut first = BytesMut::new();
        enc.encode(&headers, &mut first);
        let mut second = BytesMut::new();
        enc.encode(&headers, &mut second);

        assert!(first.len() > second.len(), "the second use must be indexed");
        assert_eq!(&second[..], &[0xbe], "dynamic index 62");
    }

    #[test]
    fn encoder_never_indexes_a_sensitive_field() {
        let mut enc = HpackEncoder::new(4096);
        let headers = [Header::sensitive("authorization", "Bearer hunter2")];

        let mut first = BytesMut::new();
        enc.encode(&headers, &mut first);
        let mut second = BytesMut::new();
        enc.encode(&headers, &mut second);

        assert_eq!(first, second, "a sensitive field is never indexed");
        assert_eq!(first[0] & 0xf0, 0x10, "never-indexed representation");

        let mut dec = HpackDecoder::new(4096, None);
        let decoded = dec.decode(&first.freeze()).expect("decode");
        assert!(decoded[0].sensitive, "sensitivity survives the round trip");
        assert_eq!(dec.table.len(), 0);
    }

    /// A size update crossing the wire must leave both tables agreeing.
    #[test]
    fn encoder_announces_a_table_resize() {
        let mut enc = HpackEncoder::new(4096);
        let mut dec = HpackDecoder::new(4096, None);

        let mut block = BytesMut::new();
        enc.encode(&[Header::new("a", "1")], &mut block);
        dec.decode(&block.split().freeze()).expect("decode");
        assert_eq!(dec.table.len(), 1);

        enc.set_max_table_size(0);
        enc.encode(&[Header::new("b", "2")], &mut block);
        assert_eq!(block[0] & 0xe0, 0x20, "the block opens with a size update");
        dec.decode(&block.freeze()).expect("decode");
        assert_eq!(dec.table.len(), 0, "both tables were emptied");
    }
}
