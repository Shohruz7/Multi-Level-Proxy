#![no_main]
//! Fuzz the HPACK decoder, which is the most attacker-exposed parser in the
//! engine: it is stateful, it allocates, and it runs before any authentication.
//!
//! Two contracts are under test.
//!
//! **Totality.** For any input, `decode` returns a header list or an error, and
//! never panics — no slice overrun on a truncated literal, no arithmetic
//! overflow on an attacker-chosen integer, no unbounded loop on a continuation
//! run, no index past the end of the dynamic table.
//!
//! **The bomb guard (design doc §6).** HPACK amplifies: a handful of octets of
//! indexed references can expand into an arbitrarily large header list. So the
//! decoder must never hand back a list exceeding the `MAX_HEADER_LIST_SIZE` it
//! was built with, whatever the input claims. That is asserted below rather
//! than assumed.
//!
//! The input is treated as a *sequence* of blocks fed to one decoder, because a
//! single block cannot reach the interesting state: the dynamic table, its
//! evictions, and the size updates that resize it all persist across blocks.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use h2proxy_core::hpack::HpackDecoder;

/// The bounds the connection layer uses (`conn::MAX_HEADER_LIST_SIZE` and the
/// protocol-default table size), so the fuzzer explores the real configuration.
const MAX_TABLE_SIZE: usize = 4096;
const MAX_HEADER_LIST_SIZE: usize = 64 * 1024;

/// Per-entry overhead in the RFC 7541 §4.1 accounting.
const ENTRY_OVERHEAD: usize = 32;

fuzz_target!(|data: &[u8]| {
    let mut decoder = HpackDecoder::new(MAX_TABLE_SIZE, Some(MAX_HEADER_LIST_SIZE));

    // Split the input into length-prefixed blocks so one run can drive a whole
    // connection's worth of header blocks through a single decoder.
    let mut rest = data;
    while rest.len() >= 2 {
        let len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        let len = len.min(rest.len() - 2);
        let (block, tail) = rest[2..].split_at(len);
        rest = tail;

        if let Ok(headers) = decoder.decode(&Bytes::copy_from_slice(block)) {
            let size: usize = headers
                .iter()
                .map(|h| h.name.len() + h.value.len() + ENTRY_OVERHEAD)
                .sum();
            assert!(
                size <= MAX_HEADER_LIST_SIZE,
                "decoded a {size}-octet header list, over the \
                 {MAX_HEADER_LIST_SIZE}-octet limit",
            );
        }
    }
});
