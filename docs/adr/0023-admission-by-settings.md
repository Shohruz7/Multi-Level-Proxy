# ADR 0023 — Admission control belongs in SETTINGS, not in 503s

Status: accepted, corrected 2026-09-19 · Date: 2026-09-16 · Design doc: §5.3, §10.1 · Supersedes the shed path added for [0019](0019-abuse-mitigations.md)-adjacent overload handling

## Correction (2026-09-19): the first implementation was a 5.4x regression

The reasoning below is unchanged and still holds. The implementation of it did
not, and the gap between the two is the point of this section.

Measured against `bench/results/attack-20260808T013308Z.txt`, the retained run
from before any of this existed, on the same command (`h2load -c 50 -m 20`):

| | Aug, pre-admission | As first shipped | After the correction |
|---|---:|---:|---:|
| control, no attack | 178,301 req/s | 32,756 req/s | 134,323 req/s |
| control errors | 0 | 800 | **0** |

Five defects, and only one of them was in the control loop proper.

**1. `MAX_PENDING` silently disabled pool growth — the dominant cause.** The
"is this connection coping" threshold was `max_pending / 2`, so raising the queue
bound from 128 to 4,096 moved it from 64 to 2,048, past anything a real queue
reaches. Upstream parallelism collapsed from five connections to one on an
unchanged workload (`bench/results/soak-*.csv`, 48 samples at 5 against 51 at 1)
and throughput went with it. A memory bound was setting parallelism policy.
`UpstreamRecord::backed_up` now asks the peer's own advertised limit instead, and
`growing_the_pool_does_not_depend_on_the_memory_bound` fails if that coupling
ever returns.

**2. The loop was a ratchet.** Slow start exited permanently at the first
distress and then grew one stream per connection per one-second sample: 254
seconds from floor to ceiling, against a three-second benchmark. Replaced with a
remembered `ssthresh`, a 200 ms period of its own, and a probe step proportional
to the current total — a flat step is the same ratchet in slower clothing, since
the range runs to `ceiling x connections`.

**3. It steered the wrong quantity.** The budget is advertised per connection
while distress is global, so the same budget meant 100 streams at 50 clients and
1,000 at 500. The loop now steers the total and divides for advertisement.

**4. It refused streams the client was entitled to open.** `SETTINGS_MAX_CONCURRENT_STREAMS`
is unlimited until the server says otherwise, so a client that opens twenty
before our first SETTINGS lands has broken no rule. Enforcement now waits for the
acknowledgement — and for *each* change, tracked as a count of outstanding
SETTINGS, because gating only on "has it ever acked" closed the handshake race
and left the identical race open on every later reduction, worth 149 refusals.

**5. Admission reused the pool's distress signal.** `backed_up` asks whether a
connection wants a sibling, which is permanently true once the pool is at its
ceiling under load. Feeding it to admission made every sample look like distress
and walked the budget down to the floor: 3,679 streams held where the same box
held 18,317. Admission now measures against the shed bound it exists to avoid,
one control period of arrivals below it — a margin that has to be derived rather
than picked, because three quarters held on an idle box and shed thousands under
a loaded one.

### Why none of this was caught

The A/B that blessed the original ran a single shape, 500 connections x 40
streams. That workload is already overloaded, and a throttle is nearly free when
everything is queueing anyway — so the arms looked equal. The regression lived
entirely in the un-overloaded regime, which was never measured. `bench/admission-ab.sh`
now runs both shapes and records `pool_conns`, the variable that collapsed while
nothing was looking at it.

Its verdict was wrong twice in the same way, which is worth recording separately:
it first compared only throughput and printed "no regression" for a run in which
concurrency had fallen 4.4x and 3,248 requests had failed, and then flagged the
fixed build being 1.26x *faster* as a defect. A benchmark that reports the number
you were watching while a different number moves is how this class of defect
survives.

### What this changes about the concurrency claim

At 500 x 40 the corrected build holds **6,661** streams where the pre-admission
build held **18,215** — and serves 29,975 req/s against 23,784, with zero
failures on both. By Little's law that is 222 ms of residency against 766 ms.

Holding less work is the feature. The pre-admission build held 18,215 because it
had no bound and queued everything, and "concurrent streams held" measures
exactly what admission control exists to limit. The two claims are mutually
exclusive, and the honest headline is the throughput-and-latency pair rather than
the stream count. `bench/admission-ab.sh` therefore *reports* concurrency and
asserts only throughput and failures.

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

## Known holes (2026-09-19)

Recorded here rather than left to be rediscovered. None of these is load-bearing
for the decision above; all three are places where the implementation is weaker
than the reasoning.

**`MIN_ADMIT` defeats the total bound.** The controlled quantity is the total,
but what each connection is told is `clamp(total / conns, MIN_ADMIT, ceiling)`.
The lower clamp is unconditional, so at 10,000 connections the proxy advertises
at least `10,000 x MIN_ADMIT = 20,000` streams no matter how low `admit_total`
has been driven. The floor exists so that a connection is never advertised zero
— which would be a functional deadlock, not a throttle — so it cannot simply be
removed. The bound is therefore real only while `conns x MIN_ADMIT` is below the
capacity the loop is steering toward, which is true at every load shape measured
so far and stops being true in exactly the situation admission exists for.

**A queued request holds its body.** `Tuning::max_pending` is described as a
bound on overshoot, and it is, but the thing being bounded is not free:
`upstream.rs` pushes body frames onto `Pending`, so the worst case is
`conns x max_pending x body_size` rather than `conns x max_pending x header_size`.
The GET-only benchmarks cannot see this. It is the reason additional capacity
should be bought with upstream *connections* rather than with a deeper queue,
and the reason `MAX_PENDING` is derived from the control period rather than
raised until a benchmark stops complaining.

**Admission is not the binding constraint at the shapes measured.** At 500
client connections the ceiling is `256 x 500 = 128,000` streams, roughly an
order of magnitude above anything the pool can hold. Everything the loop does at
these shapes is therefore invisible, and the measured concurrency is set by pool
capacity and pool growth instead. This is not an argument for capacity-targeted
admission — see the rejection recorded in `Shared::recompute_admission`, which
measured 22,000 req/s against 55,000 — but it does mean a benchmark at these
shapes proves nothing about the loop, in either direction.
