# ADR 0014 — Flow-control window sizing and the consumption-driven receive side

Status: accepted · Date: 2026-07-30 · Design doc: §3.5, §4.2 · RFC 9113 §5.2, §6.9

## Context

HTTP/2 flow control is two windows applied simultaneously to every DATA octet:
one per stream, one per connection. Three things have to be chosen: how large
each is, when credit is returned, and which of the two is the real memory bound.
Getting the third wrong is how a proxy ends up with unbounded memory while
believing it is bounded.

## Decision

```rust
pub const CONNECTION_WINDOW: i32 = 1024 * 1024;      // 1 MiB
pub const STREAM_INITIAL_WINDOW: i32 = 256 * 1024;   // 256 KiB
pub const SEND_BUDGET: usize = 16 * 1024;            // one DEFAULT_MAX_FRAME_SIZE
// WINDOW_UPDATE is due once half a window has been consumed.
```

The receive side is **consumption-driven**: `RecvWindow` separates `record`
(octets arrived) from `release` (octets were handed onward), and only `release`
can produce a WINDOW_UPDATE.

## Why these numbers

**The connection window is the memory bound, and it is the only one.**
256 concurrent streams × 256 KiB is 64 MiB of *nominal* per-stream credit, but
actual in-flight octets are capped at 1 MiB regardless of how many streams are
open, because every octet debits both levels. Per-stream windows only decide how
that one budget is shared. This is the sentence ADR 0013 needed and week 2 got
wrong.

**256 KiB per stream** is large enough that a single stream is not round-trip
bound on a LAN, and small enough that one stream cannot claim the whole
connection budget.

**Half-window replenishment.** Returning credit per octet puts a WINDOW_UPDATE
on the wire per DATA frame; waiting for the window to empty stalls the peer for a
full round trip. Half keeps credit in flight while the peer is still spending.

**`SEND_BUDGET` = one max-size frame** is the §4.1 fair-share quantum: a visit
produces at most one DATA frame, so a 10 MiB response cannot starve a 1 KiB one.

**These are reasoned, not measured.** Week 8's tuning pass is where they meet a
load profile; this ADR is the baseline it argues against.

## Two traps, both of which bit during implementation

**1. `INITIAL_WINDOW_SIZE` cannot raise the connection window.** §6.9.1 scopes
that setting to *streams*. The connection window starts at 65,535 and moves only
by WINDOW_UPDATE, so the handshake sends an explicit stream-0 increment
(`CONNECTION_WINDOW_BOOTSTRAP`). Omitting it is not a failure — it is a silent
throughput cap with nothing in the logs to explain it. `flow_control.rs` asserts
the frame is sent.

**2. Stalling must not cost a stream its turn.** The scheduler rebuilds its ring
after every lap with stalled streams first, in order, and served streams behind
them. Rotating everyone to the back as they are visited — the obvious
implementation — starves: a stream that stalls lands *behind* whoever wrote in
the same lap, so the same stream wins every scarce credit forever. This passed
every test until a two-stream test with a deliberately tiny connection window
was written for it.

## Consequences

- **Week 6's backpressure bridge is already the mechanism, not a rewrite.**
  Withholding a WINDOW_UPDATE until the client drains is "delay the `release`
  call". The split exists for that and nothing else today.
- `Window` is signed and every operation is checked: a SETTINGS decrease may
  legally drive a window negative (§6.9.2), and the `u32` increment space is
  wider than the `i32` window, so the naive `+=` would wrap rather than error.
- The stall count is exported as `h2proxy_flow_control_stalls_total`. A nonzero
  value under load is the observable evidence that flow control is doing
  something rather than being nominally present — 269 stalls over a 10,000
  request `h2load` run on the week-5 build.

## Rejected alternative

Immediate replenishment (WINDOW_UPDATE as each DATA frame is decoded). Simpler
and marginally faster, and the window arithmetic would still be exercised. It
was rejected because it leaves nothing to withhold: week 6's slow-client memory
bound would need the release path rebuilt rather than delayed.
