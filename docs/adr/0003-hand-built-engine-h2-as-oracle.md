# ADR 0003 — Hand-built HTTP/2 engine, with the h2 crate as test oracle only

Status: accepted · Date: 2026-07-03 · Design doc: §1.2, §8

## Context

A production-grade `h2` crate exists. Using it would make the proxy a few hundred
lines of glue — and teach nothing. Not using any reference risks being subtly
wrong on the wire in ways interop testing alone won't catch.

## Decision

Implement the protocol engine **by hand** against RFC 9113/7541 — frame codec,
HPACK (static + dynamic tables, Huffman, primitives), stream state machine, flow
control, connection management. Pull **h2 in as a dev-dependency only**, used as a
*differential-testing oracle*: our encoder's output must decode identically in h2
and vice versa, over single frames (unit), generated frames (proptest), and raw
bytes (fuzz).

## Rejected alternative

Building on the h2 crate (defeats the learning goal — the engine *is* the
project), or building with no oracle (correctness would rest entirely on our own
reading of the RFCs; "from scratch" must not mean "subtly wrong").

## Consequences

- Every frame type and HPACK sequence gets an instant, authoritative
  correctness check; the differential harness is week 2's deliverable and every
  later week's safety net.
- h2 never appears in the release dependency graph — the shipped binary is
  entirely our engine.
- We own every protocol bug; the oracle finds disagreements, the RFC arbitrates.
