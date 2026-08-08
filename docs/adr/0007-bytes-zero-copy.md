# ADR 0007 — Payload representation: bytes::Bytes for zero-copy DATA handling

Status: accepted · Date: 2026-07-03 · Design doc: §3.2, §8

## Context

A proxy's data plane is dominated by moving DATA payloads: read from one socket,
parse, demux, re-frame, write to another socket. Copying payload bytes at each
hop would dominate CPU at the §10 throughput targets.

## Decision

Use **`bytes::Bytes`/`BytesMut`** as the payload type throughout the engine: the
read buffer is a `BytesMut`; a parsed DATA frame holds a `Bytes` slice of it
(reference-counted view, no copy); that same `Bytes` flows through demux → bounded
channel → remux and is written upstream via vectored I/O.

## Rejected alternative

`Vec<u8>` payloads (a copy per frame per hop, ~2 copies per proxied byte), or
lifetime-bound `&[u8]` views (zero-copy but the borrow can't cross task/channel
boundaries, which our task topology (§2.2) requires).

## Consequences

- Payload bytes are copied at most where the kernel demands it (socket read,
  socket write); everything between is refcount bumps.
- `Bytes` is `Send + Clone`, so slices cross the bounded mpsc channels freely —
  this is what makes the task-per-stream topology affordable.
- Watch-out: a small `Bytes` slice keeps its whole backing allocation alive;
  long-buffered small frames can pin large read buffers (memory-bound tests in
  week 6 must watch RSS, not just queue depths).
- h2 models frames the same way (`Frame<T = Bytes>`), so differential tests
  exchange payloads without conversion.
