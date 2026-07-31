//! Property tests for HPACK.
//!
//! The headline property is **lockstep**: an encoder and a decoder that have
//! seen the same sequence of header blocks must agree, block after block, for
//! any sequence at all. That is the invariant the design doc (§3.3) says a
//! single missed insertion or eviction destroys, and unlike the RFC vectors —
//! which exercise four fixed sequences — this states it over arbitrary ones,
//! including the eviction-heavy cases a small table forces.
//!
//! The primitives get their own round-trip properties, because they are the
//! layer a fuzzer reaches only indirectly.

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;

use h2proxy_core::hpack::{Header, HpackDecoder, HpackEncoder};

/// Field names are drawn from a small pool with some static-table members, so
/// that runs actually hit the indexed representations rather than always
/// producing fresh literals.
fn name() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => prop::sample::select(vec![
            ":method",
            ":path",
            ":scheme",
            ":status",
            "accept",
            "content-type",
            "cookie",
            "custom-key",
            "user-agent",
        ])
        .prop_map(str::to_owned),
        1 => "[a-z][a-z0-9-]{0,30}",
    ]
}

/// Values include the empty string and the odd non-UTF-8-ish byte, since a
/// header value is octets, not text.
fn value() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        3 => prop::sample::select(vec!["GET", "/", "https", "200", "text/plain", ""])
            .prop_map(|s| s.as_bytes().to_vec()),
        2 => proptest::collection::vec(any::<u8>(), 0..64),
    ]
}

fn header() -> impl Strategy<Value = Header> {
    (name(), value(), proptest::bool::weighted(0.15)).prop_map(|(n, v, sensitive)| Header {
        name: Bytes::from(n.into_bytes()),
        value: Bytes::from(v),
        sensitive,
    })
}

fn header_list() -> impl Strategy<Value = Vec<Header>> {
    proptest::collection::vec(header(), 0..12)
}

/// A sequence of header lists on one connection.
fn sequence() -> impl Strategy<Value = Vec<Vec<Header>>> {
    proptest::collection::vec(header_list(), 1..8)
}

proptest! {
    /// The property the whole module exists for: over any sequence, and any
    /// table size, every block decodes to exactly what was encoded.
    ///
    /// A table size of 0 disables indexing entirely; the small sizes force
    /// constant eviction, which is where accounting bugs live.
    #[test]
    fn encoder_and_decoder_stay_in_lockstep(
        lists in sequence(),
        table_size in prop::sample::select(vec![0usize, 64, 256, 4096]),
        huffman in any::<bool>(),
    ) {
        let mut encoder = HpackEncoder::new(table_size);
        encoder.set_huffman(huffman);
        let mut decoder = HpackDecoder::new(table_size, None);

        for (i, list) in lists.iter().enumerate() {
            let mut block = BytesMut::new();
            encoder.encode(list, &mut block);
            let decoded = decoder.decode(&block.freeze())
                .map_err(|e| TestCaseError::fail(format!("block {i} failed to decode: {e}")))?;
            prop_assert_eq!(&decoded, list, "block {} of {}", i, lists.len());
            // The tables are the state that has to agree; if they diverge, the
            // *next* block is the one that breaks, so check them every time.
            prop_assert_eq!(
                encoder.table_size(),
                decoder.table_size(),
                "dynamic tables diverged after block {}",
                i,
            );
        }
    }

    /// Decoding must be total: no input, however malformed, may panic. The fuzz
    /// target makes this claim with far more inputs, but this runs on every
    /// `cargo test`.
    #[test]
    fn decoding_arbitrary_bytes_never_panics(block in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut decoder = HpackDecoder::new(4096, Some(1 << 16));
        let _ = decoder.decode(&Bytes::from(block));
    }

    /// A sensitive field must never be indexed, so it cannot shrink on a
    /// second use — the property that keeps an `authorization` header out of
    /// the dynamic table where a compression-ratio side channel could read it.
    #[test]
    fn sensitive_fields_are_never_indexed(header in header()) {
        let sensitive = Header { sensitive: true, ..header };
        let mut encoder = HpackEncoder::new(4096);

        let mut first = BytesMut::new();
        encoder.encode(std::slice::from_ref(&sensitive), &mut first);
        let mut second = BytesMut::new();
        encoder.encode(std::slice::from_ref(&sensitive), &mut second);

        prop_assert_eq!(&first[..], &second[..]);
        prop_assert_eq!(encoder.table_size(), 0);
    }
}
