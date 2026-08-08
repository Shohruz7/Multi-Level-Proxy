# ADR 0012 — HPACK: Huffman decoding, indexing policy, and error scope

Status: accepted · Date: 2026-07-28 · Design doc: §3.3, §6 · RFC 7541

## Context

Week 4 implements HPACK (RFC 7541), the header compression HTTP/2 mandates.
Three decisions inside it are not forced by the RFC and are worth recording,
because each trades something real.

HPACK is the stateful layer, and that shapes everything below. Encoder and
decoder each maintain a *dynamic table* that must mirror the peer's exactly. The
table is mutated by the header blocks themselves, so a single missed insertion
or eviction does not corrupt one message — it silently corrupts every message
after it, on that connection, forever. There is no resynchronization mechanism.

## Decision 1 — Huffman decoding by a nibble-indexed state machine

The RFC's Appendix B code is a canonical prefix code over 257 symbols, 5 to 30
bits per symbol, unaligned to octet boundaries.

**Decided:** derive a 4-bit-indexed state machine from the code table at first
use (`LazyLock`), and decode two lookups per octet.

The code is *complete* (its Kraft sum is exactly 1 — asserted in a unit test), so
its tree has exactly 256 internal nodes; each becomes a state holding 16
transitions. Because the shortest code is 5 bits, a 4-bit step can complete at
most one symbol, which keeps each transition a fixed-size record rather than a
variable-length emit list.

Rejected: a per-bit tree walk. It is about a third of the code and obviously
correct, but it branches eight times per octet on the hottest inbound path in
the proxy, and week 8's tuning pass would have had to revisit it.

The cost of the choice is that the transition table is *derived*, not written —
so it is only as correct as the code table and the derivation. Both are pinned
by tests: the table is checked for prefix-freedom and completeness, all 257
symbols round-trip, and the RFC's own coded strings decode to the RFC's text.

Two rules fall out of the derivation rather than being bolted on: the EOS symbol
appearing as an actual code is a decoding error (§5.2), and trailing padding must
be a prefix of EOS — all 1-bits — and shorter than 8 bits. Both are properties of
the *state* the decoder ends in, so both are table lookups.

## Decision 2 — Index aggressively, matching the RFC's own examples

An HPACK encoder may represent any field as an index, a literal with indexing, a
literal without, or never-indexed; all are correct, and the choice is pure
policy.

**Decided:** full match → indexed; otherwise literal **with incremental
indexing** (reusing the name's index when only the name matches); Huffman-code
any string that does not get longer for it; and `sensitive` fields → never
indexed, never stored.

This is exactly what RFC 7541's Appendix C examples do, which buys something
concrete: **the RFC's worked examples become tests of our encoder**, not just our
decoder. All four request/response sequences (C.3–C.6) are asserted byte-for-byte
in both directions. An encoder is otherwise very hard to test — a round-trip only
proves it agrees with our own decoder, which a matched pair of bugs satisfies.

Two details follow from matching the RFC rather than from first principles.
Ties in the Huffman decision go to coding (`:status: 307` is three octets either
way, and C.6.2 codes it). And the encoder exposes `set_huffman`, because
Appendix C gives each sequence twice — coded and not — and both halves have to
run against the same encoder.

The alternative was a conservative encoder that never touches the dynamic table.
It is trivially safe against desync, but it gives up most of HPACK's compression
and, worse, leaves the encoder-side table logic untested — which is half the
thing week 4 exists to get right.

## Decision 3 — Compression faults kill the connection; oversized lists do not

`HpackError` has two variants, and the split is about blast radius:

- `Compression` → connection `COMPRESSION_ERROR` (GOAWAY). A malformed block or
  a reference to a table entry that does not exist means our table and the
  peer's have diverged. Nothing later on this connection can be trusted, so it
  ends. This matches ADR 0008's model: connection-scoped faults get GOAWAY.
- `HeaderListTooLarge` → a property of one message, not of the connection. The
  tables are still in step, so the connection could survive it.

**The subtle part is what the decoder does when the limit is hit: it keeps
decoding.** Bailing out early would skip the table insertions the peer has
already made on its side, which is precisely the desync above — the bomb guard
would *cause* the corruption it exists to prevent. So the decoder stops
accumulating into the output list (bounding memory, which is the actual defense)
but processes the block to its end, and reports afterwards.

Week 4 still answers both with GOAWAY, because there is no stream layer yet to
reject a single message on. Week 5 answers `HeaderListTooLarge` with RST_STREAM
and a 431 instead — the split exists so that change touches the connection layer
and not the decoder.

## Consequences

- The `hpack` module is split three ways: `huffman.rs` and `static_table.rs` hold
  data transcribed from the RFC, and `hpack.rs` holds logic. The two data files
  were generated by parsing the RFC text and cross-checking each row (bits
  against hex, prefix-freedom, entry sizes), rather than typed by hand.
- HPACK output is not byte-comparable against `h2`, unlike frames. So
  `hpack_differential.rs` asserts *semantic* equality over a multi-request
  sequence, and the byte-level claim lives in the RFC vectors instead. The two
  suites cover what the other cannot.
- The bomb guard is bounded twice: `MAX_HEADER_LIST_SIZE` (64 KiB, advertised in
  SETTINGS) caps the decoded list, and a separate `MAX_HEADER_BLOCK_BYTES` caps
  the *compressed* block reassembled across CONTINUATION — the decoded-size limit
  cannot be applied to a block that never ends. Week 7 closed the remaining gap:
  the byte cap bounds the buffer but not the frame *count*, so a stream of
  1-octet CONTINUATIONs stayed under it forever. `guard::Limits::max_continuations`
  bounds that (ADR 0019).
- A dedicated fuzz target (`hpack_decoder`) drives sequences of blocks through
  one decoder, and asserts the returned list never exceeds the limit — the guard
  as an executable claim rather than a comment.

## Alternatives considered

- **Use a third-party HPACK crate.** Rejected on the same ground as ADR 0003:
  the compression layer is one of the parts of HTTP/2 worth building, and §3.3
  is a talking point precisely because the lockstep problem is subtle.
- **Per-stream decoders.** Not an option — HPACK state is per *connection* by
  construction. Worth stating, because it is what makes a header block
  indivisible and a decode failure fatal to every stream at once.
