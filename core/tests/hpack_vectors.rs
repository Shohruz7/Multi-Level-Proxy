//! RFC 7541 Appendix C: the specification's own worked examples, as sequences.
//!
//! These are the ground truth for HPACK. What makes them worth more than a
//! round-trip test is that each appendix is a *sequence* of header lists on one
//! connection, with the dynamic table carried between them — so a table that
//! drifts by one entry fails at the next step, which is exactly the desync this
//! module's tests exist to catch (design doc §3.3).
//!
//! Each step is checked in both directions:
//!
//! - **decode** the RFC's octets and compare the header list *and* the dynamic
//!   table's contents and total size against the RFC's tables;
//! - **encode** the same header list and compare the octets to the RFC's,
//!   byte for byte.
//!
//! The second claim only holds because our encoder deliberately uses the same
//! indexing strategy the examples do (ADR 0012). An HPACK encoder is free to
//! choose otherwise, so this is a test of *our policy*, not of conformance —
//! the appendices with and without Huffman coding differ only in that choice,
//! which is why the encoder exposes `set_huffman`.
//!
//! The data below is transcribed mechanically from the RFC text; the entry
//! sizes and table totals it asserts are the RFC's own, not recomputed.

use bytes::{Bytes, BytesMut};

use h2proxy_core::hpack::{Header, HpackDecoder, HpackEncoder};

/// One header list in a sequence, with everything the RFC states about it.
struct Step {
    /// The header list, before compression.
    headers: &'static [(&'static str, &'static str)],
    /// The RFC's hex dump of the encoded block.
    encoded: &'static [u8],
    /// The dynamic table after the step, newest entry first.
    table: &'static [(&'static str, &'static str)],
    /// The RFC's stated total table size, including the 32-octet per-entry
    /// overhead.
    table_size: usize,
}

/// Run one appendix end to end: decode every block in order through a single
/// decoder, and encode every list in order through a single encoder.
fn check_sequence(steps: &[Step], table_size: usize, huffman: bool) {
    let mut decoder = HpackDecoder::new(table_size, None);
    let mut encoder = HpackEncoder::new(table_size);
    encoder.set_huffman(huffman);

    for (i, step) in steps.iter().enumerate() {
        let expected: Vec<Header> = step
            .headers
            .iter()
            .map(|&(n, v)| Header::new(n, v))
            .collect();

        // Decode the RFC's octets.
        let decoded = decoder
            .decode(&Bytes::from_static(step.encoded))
            .unwrap_or_else(|e| panic!("step {i}: decoding the RFC's block failed: {e}"));
        assert_eq!(decoded, expected, "step {i}: decoded header list");

        // The decoder's table must now match the RFC's, entry for entry.
        let table: Vec<(String, String)> = decoder
            .table_entries()
            .map(|(n, v)| {
                (
                    String::from_utf8_lossy(n).into_owned(),
                    String::from_utf8_lossy(v).into_owned(),
                )
            })
            .collect();
        let want: Vec<(String, String)> = step
            .table
            .iter()
            .map(|&(n, v)| (n.to_owned(), v.to_owned()))
            .collect();
        assert_eq!(table, want, "step {i}: dynamic table contents");
        assert_eq!(
            decoder.table_size(),
            step.table_size,
            "step {i}: dynamic table size",
        );

        // Encode the same list and expect the RFC's octets back.
        let mut out = BytesMut::new();
        encoder.encode(&expected, &mut out);
        assert_eq!(
            &out[..],
            step.encoded,
            "step {i}: our encoding differs from the RFC's\n  ours: {:02x?}\n   rfc: {:02x?}",
            &out[..],
            step.encoded,
        );
        assert_eq!(
            encoder.table_size(),
            step.table_size,
            "step {i}: the encoder's table drifted from the decoder's",
        );
    }
}

#[test]
fn rfc_c3_requests_without_huffman_coding() {
    check_sequence(C3_REQUESTS_PLAIN, 4096, false);
}

#[test]
fn rfc_c4_requests_with_huffman_coding() {
    check_sequence(C4_REQUESTS_HUFFMAN, 4096, true);
}

/// C.5 and C.6 run with a 256-octet table, which forces evictions partway
/// through the sequence — the case a naive implementation gets wrong.
#[test]
fn rfc_c5_responses_without_huffman_coding() {
    check_sequence(C5_RESPONSES_PLAIN, 256, false);
}

#[test]
fn rfc_c6_responses_with_huffman_coding() {
    check_sequence(C6_RESPONSES_HUFFMAN, 256, true);
}

// ---------------------------------------------------------------------------
// The vectors themselves, from RFC 7541 Appendix C.
// ---------------------------------------------------------------------------

#[rustfmt::skip]
const C3_REQUESTS_PLAIN: &[Step] = &[
    Step {
        // C.3.1
        headers: &[(":method", "GET"), (":scheme", "http"), (":path", "/"), (":authority", "www.example.com")],
        encoded: &[0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d],
        table: &[(":authority", "www.example.com")],
        table_size: 57,
    },
    Step {
        // C.3.2
        headers: &[(":method", "GET"), (":scheme", "http"), (":path", "/"), (":authority", "www.example.com"), ("cache-control", "no-cache")],
        encoded: &[0x82, 0x86, 0x84, 0xbe, 0x58, 0x08, 0x6e, 0x6f, 0x2d, 0x63, 0x61, 0x63, 0x68, 0x65],
        table: &[("cache-control", "no-cache"), (":authority", "www.example.com")],
        table_size: 110,
    },
    Step {
        // C.3.3
        headers: &[(":method", "GET"), (":scheme", "https"), (":path", "/index.html"), (":authority", "www.example.com"), ("custom-key", "custom-value")],
        encoded: &[0x82, 0x87, 0x85, 0xbf, 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0c, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x76, 0x61, 0x6c, 0x75, 0x65],
        table: &[("custom-key", "custom-value"), ("cache-control", "no-cache"), (":authority", "www.example.com")],
        table_size: 164,
    },
];

#[rustfmt::skip]
const C4_REQUESTS_HUFFMAN: &[Step] = &[
    Step {
        // C.4.1
        headers: &[(":method", "GET"), (":scheme", "http"), (":path", "/"), (":authority", "www.example.com")],
        encoded: &[0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff],
        table: &[(":authority", "www.example.com")],
        table_size: 57,
    },
    Step {
        // C.4.2
        headers: &[(":method", "GET"), (":scheme", "http"), (":path", "/"), (":authority", "www.example.com"), ("cache-control", "no-cache")],
        encoded: &[0x82, 0x86, 0x84, 0xbe, 0x58, 0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf],
        table: &[("cache-control", "no-cache"), (":authority", "www.example.com")],
        table_size: 110,
    },
    Step {
        // C.4.3
        headers: &[(":method", "GET"), (":scheme", "https"), (":path", "/index.html"), (":authority", "www.example.com"), ("custom-key", "custom-value")],
        encoded: &[0x82, 0x87, 0x85, 0xbf, 0x40, 0x88, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f, 0x89, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf],
        table: &[("custom-key", "custom-value"), ("cache-control", "no-cache"), (":authority", "www.example.com")],
        table_size: 164,
    },
];

#[rustfmt::skip]
const C5_RESPONSES_PLAIN: &[Step] = &[
    Step {
        // C.5.1
        headers: &[(":status", "302"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")],
        encoded: &[0x48, 0x03, 0x33, 0x30, 0x32, 0x58, 0x07, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x61, 0x1d, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x32, 0x31, 0x20, 0x4f, 0x63, 0x74, 0x20, 0x32, 0x30, 0x31, 0x33, 0x20, 0x32, 0x30, 0x3a, 0x31, 0x33, 0x3a, 0x32, 0x31, 0x20, 0x47, 0x4d, 0x54, 0x6e, 0x17, 0x68, 0x74, 0x74, 0x70, 0x73, 0x3a, 0x2f, 0x2f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d],
        table: &[("location", "https://www.example.com"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("cache-control", "private"), (":status", "302")],
        table_size: 222,
    },
    Step {
        // C.5.2
        headers: &[(":status", "307"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")],
        encoded: &[0x48, 0x03, 0x33, 0x30, 0x37, 0xc1, 0xc0, 0xbf],
        table: &[(":status", "307"), ("location", "https://www.example.com"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("cache-control", "private")],
        table_size: 222,
    },
    Step {
        // C.5.3
        headers: &[(":status", "200"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:22 GMT"), ("location", "https://www.example.com"), ("content-encoding", "gzip"), ("set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1")],
        encoded: &[0x88, 0xc1, 0x61, 0x1d, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x32, 0x31, 0x20, 0x4f, 0x63, 0x74, 0x20, 0x32, 0x30, 0x31, 0x33, 0x20, 0x32, 0x30, 0x3a, 0x31, 0x33, 0x3a, 0x32, 0x32, 0x20, 0x47, 0x4d, 0x54, 0xc0, 0x5a, 0x04, 0x67, 0x7a, 0x69, 0x70, 0x77, 0x38, 0x66, 0x6f, 0x6f, 0x3d, 0x41, 0x53, 0x44, 0x4a, 0x4b, 0x48, 0x51, 0x4b, 0x42, 0x5a, 0x58, 0x4f, 0x51, 0x57, 0x45, 0x4f, 0x50, 0x49, 0x55, 0x41, 0x58, 0x51, 0x57, 0x45, 0x4f, 0x49, 0x55, 0x3b, 0x20, 0x6d, 0x61, 0x78, 0x2d, 0x61, 0x67, 0x65, 0x3d, 0x33, 0x36, 0x30, 0x30, 0x3b, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x3d, 0x31],
        table: &[("set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"), ("content-encoding", "gzip"), ("date", "Mon, 21 Oct 2013 20:13:22 GMT")],
        table_size: 215,
    },
];

#[rustfmt::skip]
const C6_RESPONSES_HUFFMAN: &[Step] = &[
    Step {
        // C.6.1
        headers: &[(":status", "302"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")],
        encoded: &[0x48, 0x82, 0x64, 0x02, 0x58, 0x85, 0xae, 0xc3, 0x77, 0x1a, 0x4b, 0x61, 0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x82, 0xa6, 0x2d, 0x1b, 0xff, 0x6e, 0x91, 0x9d, 0x29, 0xad, 0x17, 0x18, 0x63, 0xc7, 0x8f, 0x0b, 0x97, 0xc8, 0xe9, 0xae, 0x82, 0xae, 0x43, 0xd3],
        table: &[("location", "https://www.example.com"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("cache-control", "private"), (":status", "302")],
        table_size: 222,
    },
    Step {
        // C.6.2
        headers: &[(":status", "307"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")],
        encoded: &[0x48, 0x83, 0x64, 0x0e, 0xff, 0xc1, 0xc0, 0xbf],
        table: &[(":status", "307"), ("location", "https://www.example.com"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("cache-control", "private")],
        table_size: 222,
    },
    Step {
        // C.6.3
        headers: &[(":status", "200"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:22 GMT"), ("location", "https://www.example.com"), ("content-encoding", "gzip"), ("set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1")],
        encoded: &[0x88, 0xc1, 0x61, 0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x84, 0xa6, 0x2d, 0x1b, 0xff, 0xc0, 0x5a, 0x83, 0x9b, 0xd9, 0xab, 0x77, 0xad, 0x94, 0xe7, 0x82, 0x1d, 0xd7, 0xf2, 0xe6, 0xc7, 0xb3, 0x35, 0xdf, 0xdf, 0xcd, 0x5b, 0x39, 0x60, 0xd5, 0xaf, 0x27, 0x08, 0x7f, 0x36, 0x72, 0xc1, 0xab, 0x27, 0x0f, 0xb5, 0x29, 0x1f, 0x95, 0x87, 0x31, 0x60, 0x65, 0xc0, 0x03, 0xed, 0x4e, 0xe5, 0xb1, 0x06, 0x3d, 0x50, 0x07],
        table: &[("set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"), ("content-encoding", "gzip"), ("date", "Mon, 21 Oct 2013 20:13:22 GMT")],
        table_size: 215,
    },
];
