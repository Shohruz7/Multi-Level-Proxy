# ADR 0020 — Health checking, ejection, and idempotent retries

Status: accepted · Date: 2026-08-08 · Amended 2026-08-08 (active probing wired) ·
Design doc: §5.2, §5.3

## Context

Week 6 balanced across backends but had no notion of whether a backend was worth
sending to: `LoadBalancer::pick` saw every configured address, and `Pool::checkout`
would happily lease onto a connection to a host that had stopped answering. A
`REFUSED_STREAM` reached the client as an error even when nothing had been sent
to a backend and another backend sat idle.

## Decision

### The `intercept` seam

Both features need to know how a request *turned out*, and week 6's design hides
that: a responder hands its `Events` sender to the upstream task, so everything a
backend produces goes straight to the client connection and the `Proxy` never
sees it. That is right for throughput — no extra hop, no extra task — so rather
than change it, `Service` gains:

```rust
fn intercept(&mut self, event: ServiceEvent) -> Option<ServiceEvent>;
```

One line at the top of `handle_service_event`, defaulting to a pass-through.
Returning `None` swallows the event, which is what lets a retry replace a failure
the client then never learns about.

### Ejection is passive, with a single-request probe back

Consecutive failures eject; any success clears the run. When the ejection
expires the backend is *half-open*: exactly one trial request is admitted and its
outcome decides. Letting the full share resume the instant a timer fires is what
makes health checking flap — a backend that is still sick gets a burst of
traffic, fails it, is re-ejected, and the cycle repeats forever at the period of
the backoff, costing real requests each time. One trial costs one request.

**The trial is claimed after the balancer chooses, not when the candidate is
offered.** The first version claimed it inside `eligible`, which looks
equivalent and is not: when the balancer picked the other candidate the flag was
set with no request behind it and nothing could ever clear it, so a recovering
backend stayed half-open and out of rotation permanently — ejected by its own
recovery path. An integration test caught it.

### What counts as a failure is narrower than it looks

- **`Gone` (the connection died) is a failure.** This is the only signal that
  genuinely says "unreachable".
- **A 5xx from a backend is not.** A backend that answers is a backend that is
  there, and health is asking about reachability, not correctness.
- **`REFUSED_STREAM` is not.** It is what a backend says when it is at its
  concurrency limit or draining gracefully — both correct behaviour. Ejecting for
  it is how a load spike becomes an outage.
- **A backend `RST_STREAM` is not.** It is a considered answer about one request;
  counting it would let one pathological client eject a healthy backend for
  everybody.

That distinction is not academic. `Gone` had to be *added*: a dying upstream
connection used to synthesize `Head{502}`, indistinguishable from a real backend
response, so `intercept` reported every dead connection to health as a
**success**. Health checking was present, unit-tested, and completely inert —
killing one of two backends under load produced 36,632 5xx out of 200,000
requests with the ejection counter at zero. After the split: **0 5xx**, 2
ejections, 247 retries.

### Active probing: the failure that produces no failures

*Amended 2026-08-08. This section was originally "specified but not wired" — the
honest cut when week 7 ran out of days. It is now implemented, and wiring it
changed two things the original text got wrong.*

Passive checking learns from failures, so it learns nothing from a backend that
produces none. A process that accepts TCP, completes the handshake, and then
answers nothing fails no request: the requests on it **hang**, and a hang is the
one failure mode a proxy must never produce. Neither ejection nor a retry can
help, because neither has an event to fire on.

A PING is the probe because §6.7 obliges an answer, so an unanswered one is
evidence rather than inference — and unlike an HTTP probe to `/healthz` it needs
no agreement with any backend and no config knob naming a path.

**Quiet, not empty.** The first instinct is to probe only connections with no
streams in flight, which skips exactly the case that matters: a black-holed
backend looks *busy*, because its streams are all open and none of them will ever
finish. The trigger is silence on the socket — the same correction the client
drain needed for the same reason (ADR 0018).

**Any traffic answers the probe, not just the ACK.** The plan said the nonce
would be checked because "a backend echoing a stale payload is not proof of
liveness". On reflection that is wrong: a backend echoing anything is sending us
octets right now, which is precisely what liveness means. Insisting on the ACK
specifically would disconnect a backend that was mid-response when the PING
arrived and slow to interleave the reply — a false positive on a demonstrably
live peer, which is the one error a health check must not make. So the probe
detects **silence**, the nonce still rides along and is still matched (it ends
the probe early rather than waiting out the deadline), and a stale ACK keeps the
connection alive on the general rule rather than on a payload it never sent.

What this does not cover is a backend that chatters while processing nothing.
That is a stuck-request problem, and the tool for it is a per-request deadline,
which this proxy does not have and does not pretend to.

**The report is the feature.** An unanswered probe closes the connection, which
fails its streams through the existing path — but an *idle* connection has no
streams to fail, and that is the case the probe exists for. So the pool, which is
the only layer that knows both which backend a connection belongs to and that it
died of a probe, reports the failure to `Health` directly. Without that step the
probe would be a socket recycler with a metric attached: running, detecting, and
telling nobody, which is the exact shape of the bug in the section above. The
`Pool` therefore holds an `Arc<Health>`, and `Shared` shares one rather than
owning it.

**A test harness that does not ACK is not a simplified backend, it is an
unresponsive one.** `RawPeer` — the scripted peer behind most of the integration
suites — ignored PING, and would have been disconnected by its own proxy. It now
answers, as §6.7 requires of any endpoint.

### Fail open

If every backend is ejected, `eligible` returns them **all** rather than none —
a deliberate deviation from the spec's "no eligible backend → 503".

Ejection is a guess. Five consecutive failures might mean five broken backends,
or one broken dependency they share, or a bad deploy of our own, or a threshold
that is simply too tight. If the guess is wrong, refusing everything converts a
partial outage into a total one that *we* caused. Sending traffic to backends
that were probably failing is the strictly better error: at worst it fails the
way it was already failing, at best it discovers we were wrong. Envoy calls the
same idea a panic threshold.

### Retries are narrow, and buffer nothing

All three conditions must hold:

1. an **idempotent** method (RFC 9110 §9.2.2 — POST and PATCH are absent and stay
   absent; a retried POST can charge a card twice),
2. a **`REFUSED_STREAM`** or a `Gone` before any response — the only signals that
   promise nothing was processed (§5.1.2, §6.8),
3. no **`:status`** on the client's wire yet, because there is no way to un-send
   one.

One attempt, on a different backend, no delay. That bounds amplification at 2×
and needs no jitter, no budget accounting, and no second tuning knob.

Nothing is buffered, anywhere. A retryable request is by definition one whose
HEADERS carried END_STREAM, so there is nothing to replay but the head — which is
why ADR 0016's "the bridge holds nothing" survives this ADR intact. Buffering
bodies to make more requests retryable would reintroduce exactly the unbounded
per-stream buffer the bridge exists to avoid.

## Consequences

- **The lease must be dropped before the retry's checkout.** Otherwise a retry
  can be refused by the concurrency slot its own failed attempt is still holding
  — the week-6 leaked-lease bug wearing a different hat.
- **A retry keeps the original stream's clock and span**, so the latency
  histogram reports what the client actually waited, not what the second attempt
  took.
- **A busy connection never probes at all**, because every response resets the
  silence timer. The cost on a loaded proxy is one comparison per pass of the
  connection loop and a timer arm that never fires.
- **An idle pooled connection is now kept alive by its own probing**, every
  `ping_idle`. That is a side effect worth having — it sits well inside the NLB's
  350 s idle timeout (ADR 0005) — but it means a connection no longer dies of
  neglect, and the pool's `idle_timeout` recycling is what retires it instead.
- **`h2proxy_upstream_probes_total` counts as each probe goes out**, not when the
  connection ends. A counter rolled up on close reads zero for a connection that
  is alive and idle — which is the only connection anyone would be asking about,
  and indistinguishable from probing being switched off.

## Rejected alternatives

**A retry budget as a fraction of request rate.** The right answer at a scale
where one bad backend can double the load on the others. This project is not at
that scale, and an effectively-unbounded retry policy is a classic way to turn a
partial failure into a self-inflicted outage.

**Active HTTP probes to a `/healthz` path.** Closer to what production proxies
do, and it adds a config knob plus an implicit contract with every backend. PING
is a frame we already implement and needs no agreement with anyone.

**Probing only connections with no live streams.** The cheaper trigger, and it
misses the black hole, which is the only failure the probe was added for.

**Requiring the matching PING ACK.** Stricter, and strictly worse: it converts a
backend that is slow to interleave a control frame into a backend that is
disconnected. See the amendment above.

**Ejecting on error *ratio* rather than consecutive failures.** A ratio needs a
window, and a window needs its own tuning. Consecutive failures have one
parameter and an obvious meaning.
