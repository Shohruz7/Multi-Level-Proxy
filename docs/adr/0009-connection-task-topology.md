# ADR 0009 — Connection task topology: one reader task, per-stream bounded channels

Status: accepted · Date: 2026-07-04 · Design doc: §2.2, §4.2

## Context

Each HTTP/2 connection multiplexes many streams over one socket. We have to
decide how the work of one connection is split across tasks and how frames move
between the socket and the per-stream logic. That choice fixes the ownership and
data-flow of the whole engine, and it is where backpressure will later attach
(§4.2), so it is worth committing before the byte-level codec is written.

## Decision

Per connection: **one reader task** owns the read half, decodes frames, and
dispatches each to the matching **per-stream handler** over a **bounded
`tokio::mpsc` channel**, keyed by `StreamId` in a `Dispatcher` map. Handlers send
outbound frames back over one shared channel to a **writer** that owns the write
half and serializes the mux. Connection-control frames (SETTINGS/PING/GOAWAY on
stream 0) are handled by the reader directly.

The per-stream channel bound (`STREAM_CHANNEL_BOUND`) is the backpressure
mechanism: when a handler falls behind, the reader's `send` blocks, so it stops
pulling from the socket, so the peer stalls. In week 6 that stall is coupled to
the upstream's flow-control window (§4.2), giving bounded memory under a
fast-sender/slow-receiver mismatch.

## Rejected alternatives

- **One task per connection, no per-stream tasks** (a big `select!` over all
  streams). Simpler, but a single slow stream's processing blocks the whole
  connection, and there is no natural place for per-stream backpressure.
- **One task per stream fed directly from the socket.** Multiple tasks cannot
  own the single read half; something must still demux first — that something is
  the reader task, so this collapses back to the chosen design with extra
  machinery.
- **Unbounded channels.** Removes the head-of-line stall but reintroduces the
  exact unbounded-memory failure the project exists to avoid; a fast peer would
  grow the queue without limit.

## Consequences

- Backpressure is structural: the channel bound *is* the knob, not a bolt-on.
- The reader is the one place that touches the dynamic HPACK decode state and
  stream-ID rules, keeping that state single-owner and lock-free.
- Cost: every inbound frame crosses a channel (an allocation/handoff) and each
  stream handler is a task with `Send + 'static` state. Measured against the
  §10 baseline, not assumed away.
- Week 2 prototypes the types (`ToStream`, `FromStream`, `Dispatcher`,
  `STREAM_CHANNEL_BOUND`) and the reader lifecycle (`Connection::run`); dispatch
  landed in week 5 and the GOAWAY drain in week 7 (ADR 0018).
