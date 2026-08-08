# ADR 0018 — Graceful drain: two GOAWAYs, on both legs

Status: accepted · Date: 2026-08-08 · Design doc: §5.3 · RFC 9113 §6.8

## Context

Until week 7 the shutdown signal ended a connection immediately: `tick`'s
shutdown arm returned `false` and the socket closed, cutting every in-flight
response in half. The mirror was worse — a GOAWAY *received* from a backend
returned straight out of the frame loop and `fail_all_routes()` answered every
live stream with 502, including the ones at or below `last_stream_id` that the
backend had explicitly promised to finish and was often mid-send on. Every
rolling backend restart produced a burst of errors for requests that were fine.

## Decision

**Two GOAWAYs toward clients.** On the shutdown signal a connection sends
`GOAWAY(NO_ERROR, last_stream_id = 2^31-1)` and keeps serving; after a grace
period it sends a second naming `highest_peer_id` and refuses anything above it
with `REFUSED_STREAM`; it closes when the table empties or a deadline expires.

The advisory GOAWAY is the part that is easy to skip and wrong to skip. Sending
only the final one retroactively refuses whatever the peer already put on the
wire — a client that sent HEADERS microseconds before our GOAWAY did nothing
wrong, and refusing it turns a clean deploy into client-visible errors. Naming
the maximum possible id commits to nothing and says only "open no more streams".

**A GOAWAY received is a drain, not a hang-up.** The connection leaves the pool
(so nothing new is leased onto it), everything above `last_stream_id` is failed
with `REFUSED_STREAM` — retryable, because §6.8 promises those were never
processed — and the rest is allowed to complete.

**Idleness is about quiet, not emptiness.** A connection with nothing in flight
when the signal arrives has no in-flight request for the grace to protect, so it
should not wait. But `live_count() == 0` is true of a *busy* connection between
every pair of requests: under `h2load -m 20` that collapsed a 2 s grace to 4.8 ms
and closed connections out from under clients still sending. Since a request in
flight is at most one round trip old, the test is "quiet for at least a whole
grace period", tracked as `Connection::last_stream_at`.

## Consequences

- **The daemon must drop `Shared` after the client drain.** The pool owns the
  sender for every upstream inbox, so while anything holds it those tasks cannot
  observe that their last client has gone. The stats sampler holds a `Weak` for
  exactly this reason — a strong reference there would make the upstream drain
  hit its deadline every single time.
- **The deadline is bounded by the container runtime, not by us.** 30 s matches
  Kubernetes' `terminationGracePeriodSeconds` and ECS' `StopTimeout`; being
  SIGKILLed mid-drain is strictly worse than closing cleanly a moment early. It
  is configurable because that number belongs to whoever wrote the manifest.
- **A client may still hang up first, and that is fine.** `h2load` closes its
  connection the instant it sees the advisory GOAWAY. The drain's job is to stop
  *us* being the cause of an error; what a peer does with the invitation is its
  own business. Measured: SIGTERM under `h2load -c 50 -m 20` produced zero 5xx
  across 75,000 requests, and a 20 MB response being read at 2 MB/s completed in
  full across a SIGTERM sent 1.5 s into the transfer.
- **Two policies, not one.** `PEER_DRAIN_DEADLINE` (10 s) is shorter than the
  client-facing one, because a client is waiting on us for all of it: a backend
  that says goodbye and then stops answering should cost one slow request rather
  than one very slow one.

## Rejected alternatives

**One GOAWAY.** Simpler, and it loses the requests already on the wire. The
whole value of a graceful drain is those requests.

**Draining without a deadline.** A single stalled stream would hold the process
open until the runtime SIGKILLed it, converting a clean shutdown into an unclean
one at the worst possible moment.

**Treating a received GOAWAY as a connection error.** What the code did. It is
easy to defend in isolation — the backend *is* going away — and it discards work
the backend has already agreed to finish. §6.8 exists precisely to make that
distinction available.
