# ADR 0008 — Error model: thiserror in the lib, anyhow in the bin, connection vs stream errors as types

Status: accepted · Date: 2026-07-03 · Design doc: §8; RFC 9113 §5.4

## Context

RFC 9113 splits every protocol failure into two blast radii: **connection errors**
(framing/HPACK state unrecoverable → GOAWAY + close, all streams die) and **stream
errors** (one exchange bad → RST_STREAM, connection lives). An error model that
blurs this distinction produces proxies that tear down 5,000 healthy streams over
one malformed frame — or worse, keep a poisoned HPACK session alive.

## Decision

- **h2proxy-core** (library): structured errors via **thiserror**. The central
  type encodes the RFC distinction directly, e.g.
  `enum ProtocolError { Connection(ConnectionError), Stream { id: StreamId, error: StreamError } }`,
  with each variant carrying the RFC error code it must emit (GOAWAY code vs
  RST_STREAM code). No `anyhow` in the library's public surface.
- **h2proxyd** (binary): **anyhow** at the edges (config, sockets, TLS setup,
  main), where errors mean "log and exit/close", not "pick a frame to send".

## Rejected alternative

One stringly/anyhow error type everywhere (loses the connection/stream
distinction exactly where control flow depends on it), or thiserror in the binary
too (ceremony with no consumer — nothing matches on the daemon's startup errors).

## Consequences

- "Which error code, at which scope?" is decided at the point the violation is
  detected, type-checked, and impossible to forget at the GOAWAY/RST_STREAM
  dispatch site.
- The library stays `anyhow`-free, so downstream tests can assert exact error
  variants (the differential harness matches on them).
- Mirrors h2's own split (`proto/error.rs`), which the oracle tests confirm.
