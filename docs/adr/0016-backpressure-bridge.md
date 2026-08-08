# ADR 0016 — The backpressure bridge: credit relayed, never bytes buffered

Status: accepted · Date: 2026-08-03 · Design doc: §4.2 · RFC 9113 §5.2, §6.9 · Builds on [0014](0014-flow-control-windows.md)

## Context

A proxy sits between two peers with no reason to run at the same speed. The
dangerous case is a fast backend and a slow client: the backend can produce
faster than the client will take, and every octet of the difference has to live
somewhere. In the obvious implementation that somewhere is the proxy's memory,
and its size is the *response* size — so one 4 GB download, or a thousand slow
clients, is an out-of-memory kill rather than a slowdown.

HTTP/2 already carries the mechanism to prevent this. The question is only where
to put the seam.

## Decision

**Do not couple the two legs by buffering. Couple them by withholding credit.**

Two rules, one per direction, and nothing else:

- **Response direction.** The upstream connection calls `RecvWindow::release`
  only when told `Released { n }` — meaning `n` octets were *written to the
  client*, under the client's own window. Until then the backend holds no credit
  for them and cannot send more.
- **Request direction.** The client connection calls its own `release` only on
  `BodyAccepted { n }` — meaning the upstream leg actually spent its send window
  on those octets.

Neither side ever *waits* on the other. `release` is delayed, not blocked on. The
week-5 split of `record` (octets arrived) from `release` (octets moved onward)
exists for exactly this and needed no change.

## Why this bounds memory

Every octet a backend sends debits both its stream window and the connection
window, and neither is credited back until the client has the data. So the most
the proxy can be holding for one upstream connection is one connection window —
1 MiB (ADR 0014) — regardless of the response size, the client's speed, or how
many streams share it. Per-stream windows only decide how that budget is shared.

That is a constant, and constants are testable. `ProxyStats::peak_buffered`
counts octets received from a backend and not yet delivered, and
`backpressure.rs` asserts it stays under one connection window plus a frame while
a 64 MiB response is fetched by a client reading a few KB at a time. It also
asserts the *backend* stopped — it never gets more than a window ahead of what
the client took — because "our memory is flat" would also be true of a proxy that
simply dropped data.

The measurement is deliberately not RSS: allocator behaviour is not
deterministic, and a flaky test proves nothing about a protocol.

## Consequences

- **Credit that is withheld must never be lost.** Deferring `release` creates a
  new failure mode the week-5 code could not have: a stream that dies while
  holding credit. Both engines track `pending_conn_release` per stream and return
  it when the stream retires; `StreamTable::retire` parks it in `reclaimed` for
  the connection loop to hand back. Skipping this shrinks the connection window a
  little on every cancelled transfer, and the symptom is a connection that gets
  slower over hours with nothing in the logs.
- **DATA for a stream that is already closed releases immediately.** No responder
  will ever accept those octets, so waiting for a `BodyAccepted` that cannot come
  would leak the same way.
- **A slow client costs one stream, not a connection.** The withheld credit is
  per stream and per upstream connection; other streams keep their share of the
  connection window.
- **The bridge is four lines of forwarding.** `Service::released` → `Released`;
  `BodyAccepted` → `release`. No buffer, no watermark, no timer. That is the
  argument for having built the window accounting properly in week 5.

## Rejected alternatives

**A bounded per-stream buffer with a high/low watermark.** The design doc's own
sketch, and the shape most proxies use. It works, but it is a second
flow-control system layered on the one HTTP/2 already provides, with its own
tuning constants and its own wake-up path — and the numbers would have to be
re-derived every time the window sizes changed. Withholding credit needs no
constants at all.

**Releasing on receipt and relying on TCP backpressure.** Cheap and wrong: the
client's TCP window says nothing about which *stream* is slow, so one stalled
stream throttles every stream sharing the connection — which is the head-of-line
blocking HTTP/2 exists to remove.

**Blocking the upstream task on a bounded channel to the client.** Bounds memory
correctly and reintroduces head-of-line blocking one level up: the upstream task
serves many clients, so awaiting one of them stalls the others.
