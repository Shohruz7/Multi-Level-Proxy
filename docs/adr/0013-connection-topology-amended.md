# ADR 0013 — Connection task topology, amended: one task, streams as table entries

Status: accepted · Date: 2026-07-30 · **Supersedes [0009](0009-connection-task-topology.md)** · Design doc: §2.2, §4.1, §4.2

## Context

ADR 0009 (week 2, before any of the engine existed) committed to a three-tier
pipeline per connection: a **reader** task, one spawned **handler task per
stream** fed by a bounded `tokio::mpsc`, and a **writer** task owning the write
half. Week 5 is where that had to be built, and building it surfaced three
things the week-2 sketch could not have known.

## Decision

**One task per connection.** Streams are entries in a `StreamTable`, not tasks.
The reader/writer split is a phase of the connection loop, not a task boundary:

```
loop {
    while let Some(frame) = codec.decode_any(&mut read_buf)? { handle_frame(frame)? }
    self.pump_outbound().await?;    // write what the windows allow
    if !self.fill().await { break } // park for more input
}
```

`ToStream`, `FromStream`, `Dispatcher`, and `STREAM_CHANNEL_BOUND` are deleted
rather than populated.

## Why

1. **The state machine spans both directions.** A stream advances on
   `SendData` *and* `RecvData`, `SendHeaders` *and* `RecvHeaders` (RFC 9113
   §5.1). Splitting reader from writer splits the one piece of state that has to
   see both, forcing either a mutex per stream or two half-machines that can
   disagree. Single-owner, lock-free state is the whole benefit of the actor
   shape — and it argues for *one* task, not two.

2. **`STREAM_CHANNEL_BOUND = 64` bounded messages, not octets.** Sixty-four DATA
   frames at `MAX_FRAME_SIZE` is ~1 MiB per stream; at the 256-stream
   concurrency limit that is 256 MiB of "bounded" buffering. The real memory
   bound is the **connection receive window** (ADR 0014), which caps in-flight
   octets regardless of stream count. Week 2 named the wrong knob.

3. **Per-stream tasks solve a week-6 problem.** Their purpose is keeping a slow
   *upstream* from blocking the reader. With a synchronous responder there is
   nothing to block on, so 256 tasks buy allocation and scheduling latency and
   nothing else.

## Consequences

- Flow control is single-owner: both windows, the stream table, and the
  round-robin scheduler are touched from one place, so the accounting has no
  interleaving to get wrong.
- The milestone is still reachable. When every window is exhausted
  `pump_outbound` writes nothing and the loop parks in `fill`; the peer's
  WINDOW_UPDATE wakes it and the next pass resumes. Reading is never what
  blocks, so there is no deadlock.
- **Cost, stated plainly:** `pump_outbound` uses `write_all`, so a client that
  stops reading its socket parks the connection loop and stops our reads. For a
  server that is tolerable — a peer that will not read has no claim on being
  read from — but it is not acceptable for a proxy, where a stalled client leg
  would stall an unrelated upstream one. Week 6 replaces this with a select over
  readable *and* writable.
- **What brings per-stream tasks back:** week 6's upstream leg. A stream that
  must await a backend genuinely cannot run inline. When they return they will
  be *proxy* tasks on the upstream side, and the bound on their channel will be
  chosen in octets, not messages.

## Rejected alternative

Building 0009 as written. Rejected on reason 1: the locking it would have
required is a real cost paid to honour a decision made before the state machine
existed. An ADR is a record of reasoning, not a contract — this one is amended
in the open rather than worked around.
