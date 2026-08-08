# ADR 0019 — Abuse mitigations: what we meter, and how the numbers were chosen

Status: accepted · Date: 2026-08-08 · Design doc: §6 · CVE-2023-44487

## Context

The HTTP/2 attacks that matter are not malformed frames — the codec already
rejects those — but *legal* frames sent at a rate that costs the server far more
than the client. Rapid Reset is the canonical one: HEADERS + RST_STREAM is two
tiny frames to send and a full request pipeline to process, and because a reset
stream leaves the concurrency budget instantly, `MAX_CONCURRENT_STREAMS` never
engages. The request *rate* on one connection becomes unbounded.

## Decision

A per-connection `Guard` (`core/src/guard.rs`) holding token buckets and
counters. It does no I/O, owns no timer, and takes `now` as a parameter — which
is what makes threshold tests exact and sleep-free. A trip becomes
`ConnectionError(ENHANCE_YOUR_CALM)`, which the existing error model already
turns into a GOAWAY and a clean close, so the entire mitigation is a counter plus
one `?` at each call site. The blast radius is one connection.

### The signal is "reset before answered", not "reset"

RST_STREAM alone is not an attack signal — browsers cancel constantly, for
navigations and abandoned fetches, and h2spec resets streams deliberately. What
distinguishes Rapid Reset is that streams are abandoned *before they can be
served*: the client asks for work and discards it, which is the asymmetry the
attack is built on. `Stream::response_started` is the whole discriminator, and it
is a bool rather than a timestamp because the timing is already carried by the
rate.

### Token buckets

A fixed window lets an attacker spend the whole budget at the end of one window
and again at the start of the next — 2× the intended rate, forever. An EWMA has
no exact trip condition a test can assert. A bucket is two numbers, refills
continuously, and states its threshold as a sentence: *this many at once, that
many per second sustained.*

### WINDOW_UPDATE is not a control-frame flood signal

The first version metered PING, SETTINGS **and** connection-level WINDOW_UPDATE
together. It failed the bounded-memory test, and the failure was correct: a
client draining 64 MiB a few KB at a time releases capacity hundreds of times,
which is exactly the behaviour the proxy exists to reward. WINDOW_UPDATE obliges
no reply and its rate is a function of how fast a peer consumes data *we chose to
send* — it measures legitimate traffic, not abuse.

PING and SETTINGS stay, and the reason is precise: each **obliges us to write an
ACK**. That mandatory reply is the amplification, and it is what defines the
signal. The genuinely abusive WINDOW_UPDATE shape — a zero increment — was
already a protocol error.

### The client leg only

A backend is not the adversary. Metering the upstream leg would turn a busy or
unusual backend into a dropped connection, in exchange for defending against a
threat model ("our own backend attacks us") that does not justify the false
positives.

## The numbers, and how they were measured

Derived from an argument, then **checked against real traffic**. `just calibrate`
runs the honest workloads — h2load throughput and concurrency profiles, h2spec,
and a browser-shaped burst profile — with `Limits::observe_only`, so the guard is
genuinely in the frame path, recording peaks, unable to break anything. Any
signal with less than **10× headroom** is too tight and gets raised.

| Signal | Burst / rate | Peak observed | Headroom |
|---|---|---|---|
| resets | 100 / 20 per s | 1/s | 20× |
| unanswered resets | 50 / **15** per s | 1/s | 15× |
| PING+SETTINGS | 200 / 50 per s | 4/s | 12.5× |
| consecutive empty DATA | 32 | 0 | — |
| CONTINUATION per block | 64 | 0 | — |

`unanswered_rate` **was 5 and the calibration run rejected it** at 5× headroom.
Raised to 15, which keeps it below `reset_rate` — so it remains the tighter, more
specific signal — while clearing the bar. It still stops an attack in well under
a second, because Rapid Reset exhausts `unanswered_burst` on its first flight of
frames, long before the rate matters.

The two count-based signals need no rate. Zero-length DATA carrying END_STREAM is
how a body ends and is never counted; zero-length DATA *without* it has no
legitimate use at all, so any nonzero cap works. For CONTINUATION the arithmetic
gives the number directly: `MAX_HEADER_LIST_SIZE` is 64 KiB and `MAX_FRAME_SIZE`
is 16 KiB, so an honest block needs at most four frames, and 64 is 16× that.

**The measuring instrument had a bug of its own, and the calibration found it.**
The first run reported 9,220 resets/sec and 40,631 control frames/sec from
ordinary h2load traffic. `RateMeter::peak` was dividing an in-progress window by
the *elapsed* time, which extrapolates: two events 200 µs apart read as 10,000
per second. A partial window is now scored over a whole one. Bursts are not lost
by that — bursts are what `burst` is for; this measures the sustained rate the
*rate* limit is set against.

## Consequences

- **`observe_only` is a first-class mode**, not a test affordance. It is how
  calibration measures, and it is the safe way to roll the guard out: watch the
  peaks for a week, then enforce.
- **Every threshold is settable from the environment.** The right value is a
  property of the traffic, and laptop-scale traffic is not the deployed traffic —
  week 8's run is the first sight of the real shape, and it should cost a restart
  rather than a build.
- **Cost, measured** (`cargo bench --bench hot_path`): 5.6 ns for a reset check,
  5.4 ns for a control frame, 0.59 ns for the per-DATA check. Against frame
  dispatch the guard adds **0.46%** (272.1 ns → 273.4 ns).
- **The false-positive suite is the real gate.** h2spec passes 146/146 in both
  modes with the guard enforcing, and 200,000 requests through the proxy trip
  nothing. A trip on legitimate traffic is never fixed by exempting the workload.

## Rejected alternatives

**Counting all resets equally.** Simple, and it punishes browsers. The
before/after distinction is what makes the signal specific enough to act on.

**Rejecting the offending stream instead of the connection.** RST_STREAM in
response to RST_STREAM is free for the attacker and costs us another frame. The
connection is the unit an attacker pays for, so it is the unit to take away.

**A global rate limit across connections.** Punishes every client for one
attacker, and the shared counter is contention on the hot path. Per-connection
accounting needs no synchronisation at all.
