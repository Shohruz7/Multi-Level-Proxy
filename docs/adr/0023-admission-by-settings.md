# ADR 0023 — Admission control belongs in SETTINGS, not in 503s

Status: accepted · Date: 2026-09-16 · Design doc: §5.3, §10.1 · Supersedes the shed path added for [0019](0019-abuse-mitigations.md)-adjacent overload handling

## Context

Week 9 added a bounded upstream queue: past `Tuning::max_pending` (128 per
upstream connection) the proxy refuses a request with 503 and counts it in
`h2proxy_upstream_shed_total`. The stated reasoning was that unbounded queueing
converts overload into unbounded latency, which is true, and that a stated worst
case is better than a latency that is "whatever the overload decides", which is
also true.

It was never measured against the code it replaced. When it finally was, by an
interleaved A/B of `f77ec11` (the commit before the bound) against `HEAD`, three
pairs at 500 connections x 40 streams:

| Arm | Served | Failed | p99 | Peak client streams |
|---|---:|---:|---:|---:|
| before the bound | **52,324-59,459 req/s** | **0** | 354-403 ms | 17,809-17,863 |
| after the bound | 6,209-8,662 req/s | ~1.3-1.4M | 2,196-2,367 ms | 5,073-10,359 |

A 7x throughput regression and a 6x latency regression, disjoint across all three
pairs, measured on the same machine with the arms interleaved.

The mechanism is visible in the counters. The bounded build produces **more**
total responses than the unbounded one - roughly 113,000/s against 55,000/s -
and **93% of them are 503s**. The proxy is not overloaded by requests; it is
overloaded by its own refusals.

That is congestion collapse, and the cause is a feedback loop rather than a
tuning error:

> Against a client that keeps a fixed number of requests in flight, a refusal
> that is cheaper than a service completes sooner, which frees the client to
> re-offer sooner, which raises the arrival rate. Rejection that costs less than
> service is positive feedback.

Raising `max_pending` does not break the loop; it only moves the rate at which
the loop starts. Any depth-bounded refusal has the same shape.

The corollary is that the "cliff" this bound was built to fix was largely not a
defect. At 17,872 streams in flight against a backend delivering 53,600 req/s,
Little's law puts the residency time at 17,872 / 53,600 = 333 ms, and the
measured p99 was ~400 ms. That is the arithmetic of the offered concurrency, not
a pathology.

## Decision

**Bound admission where the protocol already provides for it: advertise a
`SETTINGS_MAX_CONCURRENT_STREAMS` that reflects real upstream capacity, and stop
manufacturing 503s.**

HTTP/2 has a flow-control mechanism for stream admission, and this proxy was not
using it. `h2proxyd` advertises a fixed 256 streams per client connection while
total upstream capacity is `max_conns_per_backend` x the backend's advertised
limit - about 1,600. At 500 client connections that is an 80x over-commitment,
and every stream admitted past capacity is a stream that can only be queued or
refused.

So the advertised limit becomes a control loop rather than a constant.

**Not a capacity estimate.** That was tried first and is recorded here because
the failure is instructive: the budget was `upstream_capacity / client
connections`, with capacity counted in upstream stream slots. It measured 22,000
req/s where the same box did 55,000 with no admission control at all, *and* made
latency worse. A client stream spends most of its life not holding an upstream
slot, so sizing admission to slot count leaves the upstream idle for every client
round trip - and the queue does not disappear, it moves to the client. Against a
client with fixed demand, admission control cannot reduce latency. It can only
move the queue, and if it throttles below what the system can serve, lose
throughput.

So the budget stays as generous as it can be and retreats only from evidence:

```
distress = shed_total increased since the last sample || any upstream queue not draining
budget   = distress      -> max(budget / 2, MIN_ADMIT)      and leave slow start
           slow start    -> min(budget * 2, ceiling)
           otherwise     -> min(budget + 1, ceiling)
```

recomputed on the existing one-second sampler and pushed to each connection as a
SETTINGS frame when it changes.

Three details, each of which was a measured defect before it was a decision:

- **Distress is `shed_total` moving, not queue depth.** Depth alone read clear
  between samples while the proxy was shedding 38,000 requests a second: the loop
  settled into a comfortable sawtooth around a level that still refused most of
  the offered work. Shedding is not a proxy for the failure, it *is* the failure.
  Depth stays as an early warning, because it trips before anything is refused.
- **The additive step is one.** The budget is per connection, so a step of `n`
  admits `n x connections` more streams at once; at 500 clients a step of 8 was
  +4,000 streams per sample and overshot every time. Additive increase has to be
  additive in the quantity that is shared, and the shared quantity is the total.
- **Slow start, from the floor.** Opening at `max_concurrent_streams` spends the
  first second of every busy period maximally over-committed - 54,000 shed
  requests before the loop took its first sample. Starting at the floor and
  doubling reaches the level in about seven samples instead of a hundred, and
  doubling ends permanently at the first distress.

`MAX_PENDING` is re-derived as part of this. 128 was chosen as "a burst, not a
backlog" and never measured; it is smaller than the 200-odd streams a backend
typically admits, so the queue could not hold one slot-turnover, and eight
connections could buffer 1,024 requests against a workload offering sixteen times
that. The bound that matters is a *time*: how long overshoot must be absorbed
before admission reacts. One second of arrivals at ~50,000 req/s across eight
connections is ~6,000 each, so 4,096. A faster control loop is the better lever
and is not taken only because the one-second period is shared with the metrics
sampler.

This works because a conforming client **blocks** rather than re-offering: it
waits for a stream slot before sending. The feedback loop cannot form, because
there is no cheap response to complete. Backpressure propagates to the client's
own send queue, which is where it belongs.

Streams opened past the advertised limit are still refused with
`RST_STREAM(REFUSED_STREAM)`, which the engine already does. That path is for
clients that ignore the setting; against a conforming one it never fires.

## Result

Same interleaved A/B, three pairs, after the change:

| Arm | Served | Failed | Shed | Peak client streams |
|---|---:|---:|---:|---:|
| before the bound (`f77ec11`) | 37,139-61,265 req/s | 0 | 0 | 17,845-18,021 |
| the bound as first written | 6,209-8,662 req/s | ~1.4M | ~1.6M | 5,073-10,359 |
| **admission by SETTINGS** | **32,880-61,520 req/s** | 0-2,300 | 0-1,397 | **17,817-17,861** |

The `f77ec11` row is re-measured here rather than copied from the table above,
which is why its spread differs: the box drifts over a session, and a baseline
carried across runs is a baseline measured on a different machine. Only arms
measured against each other, interleaved, are compared.

The arms overlap and split 2/1 across the pairs, so by the rule `bench/confirm.sh`
applies there is no effect to report - which is the outcome being sought. Peak
concurrency is restored to within 1% of the pre-admission build, and the proxy now
reaches it by admitting the work rather than by queueing it silently.

Shedding is not yet identically zero: one run in three refused 1,397 during the
slow-start ramp. That is the transient the ADR predicts and does not claim to have
removed.

## Alternatives considered

**Stop reading the client socket while saturated.** This was the first proposal
and it is the one that reads as most obviously "real" backpressure. It
deadlocks. If we stop reading, we stop receiving WINDOW_UPDATE, so a response
larger than the client's receive window cannot finish being written; the bridge
fills, upstream flow control stops the backend, the in-flight request never
completes, and the capacity that would have let us resume reading never frees.
The benchmark would not have caught it - its responses are 1 KiB, far inside the
window - which is exactly why it is written down here rather than discovered
later.

**Raise `max_pending`.** Moves the onset of collapse without changing its shape,
and picks a number by looking at a benchmark until it stops looking bad. That is
the move that produced the defect this ADR corrects.

**Bound the queue by time rather than depth.** Better than depth, because a
draining queue is never refused. It still ends in refusals under sustained
overload, so it still closes the feedback loop against a closed-loop client. Worth
revisiting as a second line of defence once admission is correct.

**Revert to unbounded queueing.** Restores the throughput and the zero failures,
and restores the original objection: latency under sustained overload is bounded
by nothing. The point of this ADR is that the bound belongs at admission, where
it costs a client a wait, rather than at dispatch, where it costs the proxy a
refusal.

## Consequences

- `h2proxy_upstream_shed_total` should sit at zero against conforming clients.
  It stops being a measure of overload handled and becomes an alarm: if it
  moves, either a client is ignoring SETTINGS or the budget is wrong.
- The advertised limit is now dynamic, so a client that caches the first SETTINGS
  it sees will under-use the proxy. That is already non-conforming behaviour, but
  it is a new way to be punished for it.
- Per-connection fairness is crude: capacity is divided evenly across live
  connections, so an idle connection holds a share it is not using. Acceptable
  while connection counts are stable; a weighted scheme is future work.
- The claim "10,000+ concurrent streams" is once again reachable, and is once
  again a statement about what the proxy will *admit* rather than what it will
  queue.
