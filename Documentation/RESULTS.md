# Results

The measurements behind every performance claim in this repository, and — just
as important — the exact conditions each one was taken under.

> ## What these numbers are, and what they are not
>
> **They are laptop numbers.** Every figure here was produced on one Apple
> Silicon machine (10 cores, 24 GB) over **loopback**, with the load generator,
> the proxy and the backend all competing for the same CPUs. There is no NIC, no
> network RTT, no load balancer, and no other tenant.
>
> **The deployment did not happen.** The AWS stack in [`infra/`](../infra/) is
> written, synthesizes, and is checked by template assertions on every push — and
> it has never been deployed ([ADR 0022](../docs/adr/0022-infrastructure-as-code.md)).
> Nothing here is a Graviton number or an NLB number.
>
> **What that costs, specifically.** Loopback flatters latency (no wire) and
> punishes throughput (the generator steals cores). The knee below is where *this
> machine* stops keeping up, and on a dedicated instance pair it would be
> somewhere else. Absolute figures should be read as "this engine, on one laptop,
> under these exact scripts"; the *relative* figures — before/after a change, one
> allocator against another, corrected against uncorrected — are the ones that
> carry over.
>
> Every table below names the harness that produced it. All of them are in
> [`bench/`](../bench/), and all of them are re-runnable with one command.

## Methodology, in one paragraph

Two profiles, because they fail differently ([bench/README.md](../bench/README.md)).
The **throughput** profile asks how many small requests per second the proxy
serves at an honest tail, and is measured **open-loop**: a fixed request
schedule, no throttling, and latency measured from when each request was
*supposed* to be sent. The **concurrency** profile asks how many simultaneous
streams hold up, and is measured **closed-loop**, because in an open loop live
streams are rate × latency (Little's law) — offering more load raises the rate,
not the concurrency. Concurrency has to be set, not offered. Every run discards a
warm-up window, and the proxy is always built `--release`.

## The correction, stated

`h2load` — which produced every number in this project before week 8 — cannot
measure a tail. Its `--rate` creates *connections* per period rather than
requests, `-D` and `-r` are mutually exclusive, and it keeps *n* requests in
flight per connection, issuing the next when the last completes. That is a
closed loop, and a closed loop cannot queue: a server that stalls for a second
simply receives fewer requests during the stall, so the stall is measured once
instead of in every request a real arrival process would have piled up behind it.

[`loadgen`](../loadgen/) measures both numbers for the same requests, so the
size of the correction is reported rather than asserted. It is small while the
proxy is comfortable and grows into the dominant term as the knee approaches —
which is exactly where a p99 gets quoted.

> ## Correction (2026-09-19, later): every concurrency number in this file was undercounted, including the one used to retire the concurrency claim
>
> The fifth correction, and the first that makes the project look **better** than
> it had recorded. It is here for the same reason as the other four: the number
> was wrong.
>
> `h2proxy_client_streams_active` is a gauge, and the daemon publishes it once a
> second. Every concurrency figure in this file was an external scraper's maximum
> over a handful of one-second samples of an instantaneous quantity, so it could
> not see any peak that opened and closed between two ticks. Polling it faster
> does not help — the value on the other side of the scrape only changes at 1 Hz.
>
> The engine now counts the high-water mark at every stream open
> (`h2proxy_client_streams_peak`, with `h2proxy_upstream_streams_peak` beside it
> as an independently accounted second opinion). Measured in the *same run*:
>
> | 500 x 40, one run | sampled at 100 ms | counted at the open |
> |---|---:|---:|
> | peak client streams | 7,357 | **19,403** |
>
> A factor of 2.6, and always in the same direction: a sampled maximum is a lower
> bound on the true one.
>
> **This invalidates the correction immediately below.** That correction retired
> the concurrency headline on the finding that the corrected build held 6,661
> streams where the pre-admission build held 18,215. Both figures came from the
> sampling method above. The reasoning attached to them — that "concurrent
> streams held" measures what admission control exists to limit, so the two
> claims pull against each other — is still sound, and that is why the headline
> is not simply reinstated. But the *number* it rested on was not measured, and
> the conclusion that this build cannot hold five figures of streams was wrong.
>
> Measured exactly, at the shipping default of 8 upstream connections, three
> alternating repeats per shape, fresh daemon per run
> ([`bench/ceiling.csv`](../bench/ceiling.csv)):
>
> | offered | achieved req/s | peak streams | failed | 5xx | shed |
> |---|---:|---:|---:|---:|---:|
> | 500 x 12 | 47,081 | 4,666 | 0 | 0 | 1,214 |
> | 500 x 20 | 48,373 | **8,018** | **0** | **0** | **0** |
> | 500 x 30 | 48,577 | 8,833 | 0 | 0 | 1,878 |
> | 500 x 40 | 47,085 | 14,051 | 0 | 0 | 37 |
>
> **This machine cannot produce a stable absolute number, and the size of the
> instability is now measured rather than guessed.** The same code, the same
> script and the same shapes, run earlier in the session as four separate
> invocations with idle gaps between them, gave 53,711 / 57,569 / 59,913 /
> 68,582 req/s for those four rows — 12% to 31% higher. The table above is the
> back-to-back run, twenty-four loads with no time to cool. Nothing changed but
> the thermal state of a laptop.
>
> Two consequences, and they point opposite ways:
>
> - **Absolute throughput here is not quotable.** A figure that moves 31% with
>   nothing but elapsed time is a property of the box. Every absolute number in
>   this file inherits that caveat; it was always true and is only now
>   quantified. The fix is a quiet, dedicated machine — the deployment target —
>   not another run here.
> - **The stream counts are sturdier than the rates, and one is sturdy.** 500 x
>   20 gave 8,004 streams in the cool set and 8,018 in the hot one, with zero
>   failures in both: **the one concurrency figure in this file that has
>   reproduced across independent runs in two different machine states.** 500 x
>   40 held 14,115 and 14,051, also close, but it failed requests in one run of
>   eight and so does not clear the bar. 500 x 30 is the fragile one, 12,974
>   against 8,833 — because a slower box sheds more (1,878 against 504),
>   admission halves on shedding, and a proxy that is protecting itself admits
>   less. That is the control loop working, and it is why residency is not a
>   number to chase.
>
> So the defensible concurrency claim from this machine is **8,018 concurrent
> streams with zero failed requests**, not the larger figures also measured
> here. The larger ones are real observations of a proxy on a cool box; they are
> not reproducible on demand, which is what a quoted number has to be.
>
> **`H2PROXYD_MAX_UPSTREAM_CONNS` was raised to 16 and the change was rejected on
> the measurement.** A first sweep had it roughly doubling streams and raising
> throughput, but that sweep ran each arm once, in ascending order, back to back
> on one box, and read concurrency off the 1 Hz gauge. Repeated with alternating
> arm order, three repeats and exact counting, 16 wins at one shape and loses at
> the rest:
>
> | offered | 8 conns req/s | 16 conns req/s |
> |---|---:|---:|
> | 500 x 12 | **53,711** | 35,359 |
> | 500 x 20 | **57,569** | 53,286 |
> | 500 x 30 | **59,913** | 58,290 |
> | 500 x 40 | 68,582 | **83,806** |
>
> 34% slower at the shape the proxy is most comfortable at, where the pool does
> not even reach its new ceiling (11 connections of 16). This is the same error
> as the one corrected below — a change blessed at 500 x 40 alone — caught this
> time because the harness now runs more than one shape by default.
>
> **A proxy that has already been hurt admits less, and the harness has to say
> which one it measured.** Admission halves its budget on distress and climbs
> back at about six percent per tick, so daemon history changes the answer. The
> same 500 x 40 shape holds 14,115 streams on a freshly started proxy and 8,827
> when it is reached through `bench/curve.sh`, where one daemon serves thirteen
> steps in sequence and has been driven into overload by the time the
> concurrency profile runs. Neither number is wrong. `bench/ceiling.sh` restarts
> the daemon for every run and is the authority for a concurrency figure; the
> `streams_peak` column in the curve table below is a floor, for that reason and
> because its counter is cumulative across steps.
>
> The stream counts in every section below this line were produced by the old
> sampling method and are **lower bounds**, not measurements.

> ## Correction (2026-09-19): the fix for *that* was itself a 5.4x regression
>
> The fourth correction in this file, and the pattern is now the subject rather
> than an embarrassment: every number here that was measured in one regime and
> quoted in another has been wrong.
>
> The admission control added on 2026-09-16 was blessed by an A/B at 500
> connections x 40 streams. That workload is already overloaded, and a throttle
> is nearly free when everything is queueing anyway, so the arms looked equal. It
> was never measured on load the proxy was *not* struggling with. There:
>
> | `bench/attack.sh`, control section | Aug, pre-admission | As shipped | Corrected |
> |---|---:|---:|---:|
> | throughput | 178,301 req/s | 32,756 req/s | 134,323 req/s |
> | errors | 0 | 800 | **0** |
>
> Five separate defects, detailed in [ADR 0023](../docs/adr/0023-admission-by-settings.md).
> The dominant one was not in the control loop at all: `MAX_PENDING` and the
> pool's "is this connection coping" threshold shared a constant, so widening the
> queue from 128 to 4,096 moved the growth threshold from 64 to 2,048 and the
> pool stopped growing. Upstream parallelism fell from five connections to one —
> visible directly in the soak CSVs, 48 samples at 5 against 51 at 1 — and
> throughput fell with it. A memory bound was quietly setting parallelism policy.
>
> **The concurrency headline does not survive this, and should not.** At 500 x 40
> the corrected build holds 6,661 streams where the pre-admission build held
> 18,215, while serving 29,975 req/s against 23,784 with zero failures on both —
> 222 ms of residency against 766 ms by Little's law.
>
> Holding less work is the feature. "Concurrent streams held" measures precisely
> what admission control exists to limit, so that number and this proxy's current
> behaviour are mutually exclusive claims. The 18,792 figure quoted below was
> measured on a build with no admission control; it was true of that build and is
> not true of this one. The honest pair is throughput and latency.
>
> Two of the five defects were in the measurement rather than the code — a
> single-shape A/B, and a verdict that compared only throughput and so reported
> "no regression" for a run in which concurrency had fallen 4.4x and 3,248
> requests had failed. `bench/admission-ab.sh` now runs both an overloaded and an
> un-overloaded shape, records `pool_conns`, and asserts throughput and failures
> while *reporting* concurrency.

> ## Correction (2026-09-16, later): the fix for the cliff was a 7x regression, and it contaminated the numbers below
>
> The bounded upstream queue added on 2026-09-15 (`a58cd21`) was never measured
> against the code it replaced. Measured now — interleaved against `f77ec11`, the
> commit before it, three pairs at 500 connections x 40 streams:
>
> | Arm | Served | Failed | Peak client streams |
> |---|---:|---:|---:|
> | before the bound | 52,324-59,459 req/s | 0 | 17,809-17,863 |
> | after the bound | 6,209-8,662 req/s | ~1.4M | 5,073-10,359 |
>
> A 7x throughput regression, disjoint across all three pairs. The mechanism is
> in the counters: the bounded build produced **more** responses than the
> unbounded one, ~113,000/s against ~55,000/s, and **93% of them were 503s**. It
> was not overloaded by requests. It was overloaded by its own refusals.
>
> Against a client holding a fixed number of requests in flight, a refusal that
> is cheaper than a service completes sooner, which frees the client to re-offer
> sooner. Rejection cheaper than service is positive feedback, and no depth bound
> breaks that loop — it only moves where the loop starts.
>
> **Fixed** by moving admission to where HTTP/2 already provides for it: the
> advertised `SETTINGS_MAX_CONCURRENT_STREAMS` is now a control loop rather than
> a constant, so a conforming client waits for a slot instead of being refused
> (ADR 0023). Re-measured on the same box: 32,880-61,520 req/s, 17,817-17,861
> peak streams — within 1% of the pre-admission concurrency, arms overlapping
> with no effect to report. `bench/admission-ab.csv`.
>
> **What this contaminates.** The correction immediately below retracts the
> ">=210,000 req/s" headline and replaces it with 52,967 and 76,919 req/s from a
> closed-loop re-measurement. Those runs were taken on **2026-09-15, after
> `a58cd21` landed** — that is, on the regressed build. They are therefore a
> lower bound on a proxy that was refusing most of its offered work, not a
> measurement of this one.
>
> The retraction of 210,000 still stands on its own evidence: it was never
> reproducible and never had an artifact, which is why it was retracted. But the
> *replacement* numbers are not trustworthy either, and are re-measured in the
> table at the top of this file rather than carried forward. The 18,792 figure
> this paragraph originally quoted has since been superseded twice over — see the
> 2026-09-19 correction above, which retires the concurrency headline entirely.
>
> Three defects were found inside the fix itself, each by measurement rather than
> reasoning, and each is recorded in ADR 0023 because the reasoning that produced
> them was plausible every time: sizing admission to upstream *slot count*
> (measured 22,000 req/s — worse than no admission control at all); reacting to
> queue depth rather than to shedding (shed 38,000/s while the depth gauge read
> clear between samples); and an AIMD additive step of 8 on a *per-connection*
> budget, which is +4,000 streams per sample across 500 clients.

> ## Correction (2026-09-16): the pool-fix numbers below are a single run, and they do not reproduce
>
> This is the third correction in this file and it follows the same pattern as
> the first two exactly: a number that was *quoted* rather than *committed*
> turned out to be wrong. The previous correction, immediately below, closes with
> a promise that nothing about the saturation regime would be claimed. It then
> claims two things anyway - "one connection gave p99 21 ms where eight gave
> 297 ms", and "at 20,000 req/s the pool settles on one upstream connection,
> p99 0.36 ms" - both from one run each, on the same contended laptop the
> paragraph warns about.
>
> Re-measured properly. `bench/confirm.sh` runs both pool policies from **one
> binary with one flag different** (`H2PROXYD_POOL_GROWTH=queue|eager`), six
> alternating pairs at each of three offered rates, and applies a decision rule
> fixed before the run: if the arms' observed ranges overlap, no ratio is
> reported. Every row is in [`bench/confirm.csv`](../bench/confirm.csv).
>
> | Offered | queue p99 (median, range) | eager p99 (median, range) | Pool | Verdict |
> |---|---|---|---|---|
> | 10,000 | 0.393 ms (0.205-0.630) | 0.511 ms (0.293-0.878) | 1 vs 1 | no effect |
> | 20,000 | 209 ms (22.1-409) | 215 ms (18.4-439) | 8 vs 8 | no effect |
> | 30,000 | 568 ms (434-793) | 451 ms (279-650) | 8 vs 8 | no effect |
>
> **The 14x is retracted.** The two policies could not be separated at any rate.
> At 20,000 and 30,000 req/s `eager` won 4 of 6 pairs - if anything the wrong
> direction. The original pair was one run per arm with `queue` going first, and
> the first version of this harness reproduced that artefact perfectly: `queue`
> won 5/5 at 30,000 req/s while the sequence degraded monotonically as it ran, so
> whichever arm went first would have swept. The order now alternates.
>
> **The 0.36 ms is real but the rate attached to it is wrong.** It reproduces at
> **10,000** req/s, not 20,000: median 0.393 ms over six runs, one upstream
> connection, zero shed, every time. At 20,000 req/s this proxy is at its
> capacity edge on this machine, and which side it falls on depends on how long
> the box has been under load:
>
> | Run | Delivered | p99 | Pool | Shed |
> |---|---|---|---|---|
> | first at 20,000 | 20,000 | 22.1 ms | 1 conn | 0 |
> | twelfth at 20,000 | 16,736 | 409 ms | 8 conns | 57,641 |
>
> Same binary, same flag, same offered rate, twelve runs apart. That is the most
> useful thing this run produced, and it is a statement about the measurement
> environment rather than about the proxy.
>
> **The noise floor, finally measured.** At 10,000 req/s neither policy ever
> opens a second connection, so the two arms are *identical by construction* -
> and their median p99 still differs by 30%. Any effect smaller than that is not
> visible on this hardware, which is why single-run comparisons kept producing
> numbers that evaporated.
>
> **What the pool change is still worth.** Sockets, not latency: one upstream
> connection instead of eight at rates the proxy is keeping up with. The two
> policies differ only while a connection sits at the backend's stream limit
> *and* the pool is below its ceiling; outside that band they are the same
> policy. That is a narrower claim than the one it replaces, and it is the one
> the evidence supports.

> ## Correction (2026-09-15): the 210k figure below is also wrong, and the open defect is closed
>
> Two things in the 2026-08-11 correction immediately below need correcting in
> turn. The pattern is not an accident and is the reason both are left standing:
> every number in this file that was *quoted* rather than *committed* has so far
> turned out to be wrong.
>
> **The "≥210,000 req/s" is not reproducible.** It appears in this document, in
> the README and in a commit message, and in no artifact — `git log` shows
> `bench/curve.csv` was never regenerated after it was claimed, and the only
> committed closed-loop table, `bench/methodology.csv`, tops out at 82,091.
> Re-measured closed-loop through a backend:
>
> | Shape | Delivered |
> |---|---:|
> | 50 conns × 20 streams (the standard shape) | 52,967 req/s |
> | 50 conns × 40 streams | 47,625 req/s |
> | 10 conns × 100 streams (best observed) | **76,919 req/s** |
>
> Throughput varies **2.4× at identical concurrency** purely by how it is
> distributed across connections, which is a result worth having on its own: the
> per-connection cost dominates, and "requests in flight" is not by itself a
> description of a load.
>
> **The unexplained cliff was ordinary saturation with no admission control.**
> Six hypotheses, each killed by a measurement rather than an argument:
>
> | Hypothesis | Test | Result |
> |---|---|---|
> | The load generator | `--workers 65536` | no change |
> | Client-leg admission | `H2PROXYD_MAX_CONCURRENT_STREAMS=2048` | no change |
> | Backend capacity | backend alone, the proxy's exact shape | **464,315 req/s** — 15× the cliff |
> | Read starvation in `tick`'s biased select | deleted `biased` | no change |
> | Lock convoy in `Pool::checkout` | 8 s profile under load | mutex wait 2.8%; workers parked 4,787/5,082 |
> | Upstream slot scarcity | `BACKEND_MAX_CONCURRENT_STREAMS=2000` | queue eliminated, latency **unchanged** at 281 ms |
>
> The last is conclusive: removing the queue's *location* did not remove the
> queue. The proxy used **1.14 of 10 cores** throughout, so it was never
> CPU-bound. What was missing was a bound: past capacity, nothing stopped the
> proxy accepting work, so overload was converted into latency and stayed
> converted for as long as it lasted. The tell was that *fewer* upstream
> connections were better — one connection gave p99 21 ms where eight gave
> 297 ms — because the 200-slot stream limit was acting as accidental admission
> control.
>
> **Fixed.** The upstream queue is bounded and refuses past it with a 503
> (`h2proxy_upstream_shed_total`), and the pool now opens a connection when the
> existing ones are backing up rather than when they merely reach the backend's
> `MAX_CONCURRENT_STREAMS` — a protocol fact that says nothing about throughput.
> At 20,000 req/s the pool settles on one upstream connection instead of eight,
> p99 0.36 ms.
>
> **What is still not claimed.** Nothing about the saturation regime. The box
> these runs happen on is a laptop running an editor, a VPN and Docker; its load
> average sat at 4.5 with none of ours running, and interleaved A/B runs at
> 30k–50k req/s disagreed with each other by more than the effect under test. A
> number for that regime needs a quiet machine, and a third harness-shaped
> retraction in this file is not wanted.

> ## Correction (2026-08-11): the knee below is the harness's, not the proxy's
>
> The table in the next section reports a knee at 25,000 req/s and attributes the
> collapse past it to the proxy's admission limit. **That attribution is wrong**,
> and it was caught while building a chart to compare the two methodologies.
>
> The discriminating test: hold the *same* ~11,200 requests in flight the open
> loop reached at its "cliff", but drive them closed-loop. If the proxy were the
> constraint, throughput would collapse identically.
>
> | Measurement | Delivered | p99 |
> |---|---:|---:|
> | Open loop, offering 30k | 30,000 req/s | 435 ms |
> | Closed loop, same ~11,200 in flight | **169,030 req/s** | 143 ms |
> | Closed loop, 22,400 in flight | **209,928 req/s** | 170 ms |
>
> The proxy sustains **at least 210,000 req/s** and was still climbing. The
> Little's-law arithmetic in that section is still correct — it just describes a
> queue the *generator* was creating, not a limit the proxy was imposing.
>
> **What has been fixed since:** `loadgen`'s open loop spawned a task per
> request; it now uses a pre-spawned worker pool, like the closed loop, which
> improved the 25k point from 2.045 ms to **0.445 ms** p99.
>
> **What is still unexplained:** a cliff remains between 25k and 30k req/s
> *through a backend*. It is not the generator (dispatch lag is 0.046 ms at the
> cliff), not CPU (the box is 55% idle), not the pool cap (raising it from 8 to
> 128 does not move it), and not the client leg or the engine — in echo mode,
> with no upstream hop, the same rig does **40,000 req/s at p99 0.216 ms**. It is
> something in the upstream leg, and it is open.
>
> Until that is understood, **no latency claim in this document below the
> correction line should be quoted**, and the honest capability figure is the
> closed-loop one: ≥210k req/s. The numbers are left in place rather than deleted
> because how a benchmark misleads is worth keeping on the record.

## Throughput profile — the curve

50 connections, 1 KiB responses, offered rate stepped past saturation. 12 s of
steady state per step after a 3 s warm-up. `just curve` → `bench/curve.csv`.

![delivered rate and p99 against offered load](../bench/curve.svg)

| Offered req/s | Delivered | p50 ms | p99 ms | p99 uncorrected | Generator lag p99 | Streams live at proxy |
|---:|---:|---:|---:|---:|---:|---:|
| 2,000 | 2,000 | 0.275 | 0.455 | 0.345 | 0.123 | 1 |
| 5,000 | 5,000 | 0.149 | 0.193 | 0.139 | 0.061 | 0 |
| 10,000 | 10,000 | 0.127 | 0.297 | 0.254 | 0.050 | 1 |
| 15,000 | 15,000 | 0.149 | 0.414 | 0.369 | 0.062 | 1 |
| 20,000 | 20,000 | 0.158 | 0.785 | 0.665 | 0.087 | 3 |
| **25,000** | **25,000** | **0.542** | **2.045** | 2.023 | 0.060 | 16 |
| 30,000 | 30,000 | 370.400 | 434.323 | 434.303 | 6.004 | 11,106 |
| 40,000 | 40,000 | 315.359 | 332.656 | 332.624 | 0.114 | 11,190 |
| 50,000 | 50,000 | 256.944 | 278.606 | 278.582 | 0.187 | 11,205 |

**Zero failed requests at every step.** The headline: **25,000 req/s at a
corrected p99 of 2.0 ms**, which is the project's stated sub-3 ms target. One
step further costs a factor of 200 in latency.

### Three things in that table are worth explaining

**Delivered rate never stops tracking offered rate — even at twice the knee.**
The proxy does not shed load past saturation; it *queues*. That is why the knee
above is defined by the latency target rather than by throughput falling off: a
knee read from the throughput column alone would sit at the right-hand edge of a
chart whose entire right half is unusable.

**The streams-live column explains the cliff, and it is the admission limit.**
50 connections × `MAX_CONCURRENT_STREAMS` of 256 = **12,800** admissible streams.
Past the knee the live-stream count pins near that ceiling and stays there. Once
concurrency is capped, Little's law runs backwards — latency becomes
*ceiling ÷ throughput*:

| Offered | Ceiling ÷ rate | Measured p50 |
|---:|---:|---:|
| 30,000 | 12,800 / 30,000 = 427 ms | 370 ms |
| 40,000 | 12,800 / 40,000 = 320 ms | 315 ms |
| 50,000 | 12,800 / 50,000 = 256 ms | 257 ms |

Which is also why p50 *falls* as the offered rate rises above the knee — the
one genuinely counter-intuitive number here, and not noise. Requests wait for a
stream slot, and more offered load does not make the wait longer; it makes each
slot turn over faster.

**The correction is small in this setup, and saying so is the point.** Between
0.02 and 0.13 ms below the knee. That is the generator's own queueing, which a
closed-loop tool would have silently subtracted. The larger effect of a closed
loop is not visible as a delta at all: a closed loop cannot offer 30,000 req/s
to a proxy that is only comfortable at 25,000, so it would never have produced
the bottom three rows — the ones that show what saturation actually costs.

## Concurrency profile

500 connections, closed loop (concurrency is set, not offered — in an open loop
live streams are rate × latency, so offering more load raises the rate, not the
concurrency). `streams live` is read from the proxy's **own** gauge, not the
client's intent.

![delivered rate and p99 against requests in flight](../bench/curve-concurrency.svg)

| Requests in flight | Delivered req/s | p50 ms | p99 ms | Streams live at proxy |
|---:|---:|---:|---:|---:|
| 1,000 | 23,959 | 41.6 | 45.1 | 503 |
| 4,000 | 30,625 | 140.8 | 147.7 | 1,526 |
| 10,000 | 52,940 | 184.1 | 218.6 | **8,119** |
| 20,000 | 53,600 | 377.4 | 411.6 | **17,872** |

**17,872 streams open simultaneously**, at 53,600 req/s, with zero failures —
which is what backs the "10,000+ concurrent streams" claim, measured at the
proxy rather than asserted by the client. Delivered rate flattens at ~53k
between the last two rows while concurrency doubles: past that point more
in-flight requests buy queue depth, not throughput.

## Memory profile — what concurrency costs

The first goal in the README is bounded memory under any speed mismatch. It had
a test behind it (`a_slow_client_throttles_a_fast_backend_instead_of_filling_memory`)
and, until now, no number. [`bench/memory.sh`](../bench/memory.sh), promoted to
[`bench/memory.csv`](../bench/memory.csv): 500 connections held fixed while
streams per connection are swept, three repeats, sweep direction alternating.

| streams/conn | peak streams | idle RSS | peak RSS | bridge held, ever | failed | 5xx | shed |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2 | 967 | 5.6 MB | 36.9 MB | 2,048 B | 0 | 0 | 0 |
| 8 | 3,119 | 5.5 MB | 44.3 MB | 3,072 B | 0 | 0 | 0 |
| 20 | 7,955 | 5.6 MB | 63.9 MB | 3,072 B | 0 | 0 | 0 |
| 40 | 14,051 | 5.5 MB | 74.0 MB | 4,096 B | 746 | 0 | 746 |
| 80 | 16,187 | 5.5 MB | 74.7 MB | 4,096 B | 0 | 0 | 515 |
| 120 | 23,030 | 5.6 MB | 101.6 MB | 3,072 B | 0 | 0 | 0 |
| 160 | 31,412 | 5.6 MB | 104.4 MB | 4,096 B | 0 | 0 | 1,285 |
| **200** | **38,028** | 5.5 MB | **125.3 MB** | 3,072 B | 0 | 0 | 463 |

Medians of two. Least squares over the eight points puts the marginal cost of a
stream at **2,381 bytes**, on a fixed cost of 5.6 MB - quoted as a fit rather
than a constant, because it is not one: over 2 to 40 it is 3,126 bytes and over
80 to 120 it is 1,691, so per-stream cost amortises as the table fills.

**Resident memory plateaus while offered load triples.** From 500 x 120 to
500 x 200 the offered in-flight count goes 60,000 -> 100,000 and peak RSS moves
101.6 -> 125.3 MB. The proxy stops growing and starts refusing, which is the
bound doing its job rather than a number to be pushed. Note also that the
highest shapes are not the ones that failed.

**The earlier ceiling was the harness, not the proxy.** Every concurrency figure
in this project came from 500 x 40 or below, a shape described throughout as
"overloaded". It is not: at 500 x 40 the proxy holds 14,103 streams and shows no
client-visible failure, and it goes on doing that to 24,904. Nothing in the
proxy changed to produce these numbers - no constant, no policy, not one line of
`core/` or `h2proxyd/` - the load was simply never turned up far enough to find
the edge. The old numbers were not wrong about what they measured; they were
measurements of the benchmark.

**Shedding sometimes reaches the client, and an earlier version of this section
said it never did. That was wrong.** `shed` counts a request the pool refused
because a connection's wait-for-a-slot queue was full. The handler for it
(`ServiceEvent::Shed`) drops the route and closes the client stream, and its
comment is explicit that a retry is pointless - the queue that refused this
request is the queue every retry would land in. So a shed is *not* absorbed by
the retry path, and the first write-up of this table claimed it was.

The measurements say the relationship is real but not simple. Across the runs in
`bench/results/`, `failed` equals `shed` exactly in several (15/15, 211/211,
535/535) and is far below it in others (448 against 1,238; 149 against 817), and
there are runs with thousands shed and nothing failed at all. `responses_5xx` is
zero in every case, so whatever reaches the client is a stream-level refusal
rather than a 503.

**Client-visible failures are sporadic and not ordered by load**, which is the
part worth chasing. 500 x 40 has come back clean in most runs this file is built
on and failed 535 and 211 requests in two others; 500 x 200, five times the
offered concurrency, has not failed once. A failure mode that does not worsen
with load is not saturation, and the shape of it - a refusal issued for a stream
the client had already opened - is the same class as the defect ADR 0023 records
and ack-gating was meant to close. **It is an open defect, not a tuning
artefact, and no zero-failure claim should be made above 500 x 8 until it is
understood.**

**The spread widens with the load, and the medians hide it.** At 500 x 2 the
three repeats read 959 / 977 / 949 streams, inside 3%. At 500 x 120 they read
30,702 / 24,904 / 21,691 - a 41% spread around the median quoted above. More
offered concurrency means more queueing and more variance in what is resident at
any instant, so the honest form of the headline is *a median of 24,904 with runs
between 21,691 and 30,702*, not a single figure.

**Connections are held fixed and streams swept** precisely so the two costs
separate: peak RSS over peak streams would fold 500 TLS connections and their
record buffers into a number reported as the price of a stream, and would
flatter or damn the proxy depending only on the shape picked. The intercept is
the connections, the slope is the streams. The slope is not constant either -
fitted over 2 to 40 it is 3,126 bytes and over 80 to 120 it is 1,691, so
per-stream cost amortises as the table fills.

**At 1 KiB the bridge never held more than one page, and that says more about
the benchmark than the proxy.** The high-water mark of response octets received
from backends and not yet delivered to clients is 4,096 bytes at every shape
above - but a kilobyte reaches a client about as fast as it arrives, so there is
never anything to hold. Reporting that as the bounded-memory result would be
measuring the response size.

### Where the bridge actually has something to hold

Same harness, `BODY_SIZE=65536`, three repeats:

| streams/conn | peak streams | peak RSS | bridge held, ever | body in flight | failed | 5xx | shed |
|---|---:|---:|---:|---:|---:|---:|---:|
| 8 | 3,934 | 143.2 MB | 602 KB | 258 MB | 0 | 0 | 0 |
| 20 | 7,837 | 170.0 MB | 754 KB | 514 MB | 0 | 0 | 0 |
| 40 | **9,747** | **178.4 MB** | **803 KB** | **639 MB** | **0** | **0** | **0** |

**639 MB of response body moving through the proxy, and under 1 MB of it
resident at any instant** - three orders of magnitude between what is in flight
and what is held. That is the claim this project was built to make, and at 1 KiB
it could not be made at all, because nothing was ever in flight.

Delivered payload at 500 x 40 is about 5,500 req/s of 64 KiB, or **~360 MB/s**.
Nothing shed and nothing failed at any shape, which is the other half of the
result: the bound is held by flow control coupling the two connections, not by
refusing work.

Per-stream cost rises to 6,470 bytes here against 2,865 at 1 KiB, which is the
same fact from the other side - a stream carrying a 64 KiB response holds more
of it mid-flight. The complementary case, a client that stops reading entirely
and where the bridge is *supposed* to fill to the window and stop, is the
backpressure test rather than this table.

**`settled` is not a leak check.** Resident memory after the load stops tracks
the peak to within tens of KiB, because a general-purpose allocator keeps freed
pages rather than returning them. Paired with the active-stream gauge reading 0,
it says the process is holding address space, not request state. Growth *across*
cycles is what a leak looks like, and that is `bench/soak.sh`: five minutes, a
backend killed every 30 s, RSS plateaued at +1.9%.

**These numbers are quotable from this machine in a way the rates are not.** In
the same runs, achieved throughput at 500 x 2 varied 24,560 / 36,856 / 39,304
req/s - a 60% spread - while peak RSS read 37.9 / 37.9 / 38.1 MB, inside 1%.
Resident memory is set by what the process is holding; a rate is set by how fast
the cores are willing to run, and on this laptop that is a function of how long
it has been running. Every rate in this file carries the thermal caveat recorded
in the 2026-09-19 correction. The memory column does not.

## Tuning pass 1 — flow-control windows

The windows were *reasoned* from week 5 to week 8: 256 KiB per stream, a 1 MiB
connection window, derived from the RFC and from arithmetic. `just tune` sweeps
them. 20,000 req/s of small requests, plus a bulk transfer and a deliberately
slow reader at each point.

| Conn window | Stream window | Bulk MiB/s | Bridge peak | Stalls |
|---:|---:|---:|---:|---:|
| 256 KiB | 64 KiB | 244.8 | 66 KB | 0 |
| 256 KiB | 256 KiB | 272.6 | 243 KB | 0 |
| **1 MiB** | 64 KiB | 216.0 | 66 KB | 0 |
| **1 MiB** | **256 KiB** ← default | **274.0** | **262 KB** | 0 |
| 1 MiB | 1 MiB | 294.2 | 328 KB | 0 |
| 4 MiB | 64 KiB | 232.7 | 67 KB | 0 |
| 4 MiB | 256 KiB | 282.5 | 263 KB | 0 |
| 4 MiB | 1 MiB | 281.9 | 951 KB | 0 |
| 16 MiB | 64 KiB | 214.3 | 68 KB | **158** |
| 16 MiB | 256 KiB | 246.6 | 262 KB | 0 |
| 16 MiB | 1 MiB | 278.7 | 902 KB | 0 |

**Outcome: the defaults are kept, and now they are measured rather than
reasoned.** 1 MiB / 256 KiB delivers 274 MiB/s — within 7% of the best point in
the sweep — while holding 262 KB in the bridge, roughly a quarter of what the
fastest settings hold. Raising the stream window to 1 MiB buys about 5% more
bulk throughput for 3.5× the memory the bridge may hold for a stalled client. On
a proxy whose headline property is bounded memory, that is the wrong side of the
trade.

Three things this sweep taught that the numbers alone do not show:

**The small-request columns are blind to flow control by construction**, and the
first version of this sweep did not notice. A 1 KiB response cannot fill even a
64 KiB window, so request rate and its latency are identical at every setting —
the whole point of a window is how many octets may be in flight before the
sender must stop, which only bites on transfers larger than the window. The
`bulk_mbps` column exists because the first run produced eleven identical rows
and a shrug.

**The bridge peak tracks the *stream* window, not the connection window**, in
this harness — because the slow reader is a single stream, and a single stream is
bounded by its own window first. The connection window is the bound that matters
when *many* streams are slow at once, which is what
`core/tests/backpressure.rs` asserts directly rather than by benchmark.

**The one row with nonzero stalls is the mechanism showing itself.** 16 MiB
connection window with a 64 KiB stream window produced 158 flow-control stalls,
the only nonzero count in the sweep: a connection window so large it never binds,
against stream windows small enough that they always do, leaves the outbound
scheduler holding octets it has no per-stream credit to send. That is exactly
what a stall is, and it appears precisely where the arithmetic says it should.

The p99 column of that sweep is omitted here on purpose: it ranged from 0.49 ms
to 148 ms across settings that differ by nothing relevant, which is machine
noise, and quoting it would be dressing up a coin toss.

## Tuning pass 2 — the allocator (ADR 0010)

Deferred since July with the instruction "report the number, don't assume the
win". The number is in
[ADR 0010](../docs/adr/0010-jemalloc-allocator.md#the-measurement-2026-08-09--and-the-verdict);
the summary is that **jemalloc is not enabled**.

| | system (musl) | jemalloc |
|---|---|---|
| Throughput | 20,000 req/s | 20,000 req/s (+0.0%) |
| p99, median of 6 | 37.1 ms | 28.4 ms (−23.5%) |
| p99, **range** | **2.8 – 122.2 ms** | **5.5 – 88.0 ms** |
| RSS, range | **3.6 – 5.6 MiB** | **64.9 – 73.3 MiB** |

Six interleaved pairs in the musl container. The p99 ranges overlap almost
entirely, so the 23% median difference is not an effect — it is two noisy samples
landing where they landed. The RSS difference does not overlap at all: a
consistent **13×**, from jemalloc's per-CPU arenas, on a process whose working
set is otherwise under 6 MiB. The feature stays in the tree behind
`--features jemalloc`, switched off, because the question deserves re-asking on
hardware that can put the allocator under real pressure.

Getting the arm to build at all required three toolchain fixes, none of which
could have been found by reading the Dockerfile — including aarch64 GCC's default
`-moutline-atomics`, which links a libgcc object calling the glibc-only
`__getauxval` and makes jemalloc's configure conclude the platform has no
atomics. The ADR's claim of "no new build-image cost" was simply wrong.

## Resilience (week 7 harnesses, unchanged)

| Claim | Number | Harness |
|---|---|---|
| Backend killed mid-load, 200k requests | **0 5xx**, 2 ejections, 247 retries rescued | `just attack` |
| Backend that accepts and then answers nothing | detected and ejected in ~2× `ping_idle`; requests get an answer instead of hanging | `core/tests/probe.rs` |
| 5-minute soak, backend killed and restarted every 30 s | 13.0M requests, **0 5xx**, 1,177 retries, 16 ejections, 126 probes / 0 probe failures; RSS plateaued, every in-flight gauge settled to **0** | `just soak` |
| SIGTERM mid-load, 75k requests | **0 5xx**; a 20 MB response completed in full across it | manual; see `docs/adr/0018` — `just attack` has no SIGTERM section |
| Rapid Reset flood beside ordinary load | attacker GOAWAYed; bystander p99 **11.65 → 8.11 ms** (unharmed) | `just attack` |
| Abuse-guard cost per frame | **below the noise floor**: 5.6 / 5.4 / 0.59 ns per call, and the guarded arm of `frame_dispatch` measured 272.6 ns against 273.7 ns unguarded — overlapping intervals, so the honest claim is "not measurable", not a percentage | `just bench-hot` |
| Threshold headroom vs. legitimate traffic | **12.5×–20×**, measured | `just calibrate` |
| Accounting invariants, 3,000 requests over every ending | 0 leases outstanding, 0 streams, 0 buffered, `latency_count == 3000` | `core/tests/invariants.rs` |

## Conformance and correctness

| Check | Result |
|---|---|
| h2spec (RFC 9113), engine only | **146/146** |
| h2spec, through the proxy to a real backend | **146/146** |
| Test suite, debug | 260 passing |
| Test suite, release | 260 passing |
| Fuzz targets (frame parser, HPACK decoder, guard) | build clean; **14.2M executions, zero crashes** on the unshaped frame-parser target (`docs/adr/0011`). The "8.5M+" previously quoted here is real but is the *week-2* number from a 10-second run (`docs/progress/week-2-completion.md`) — smaller, older, and not what the row was describing |
| Abuse guard during conformance | `h2proxy_connections_terminated_total` = 0 — no false positive on legitimate traffic |

## Reproducing all of it

```sh
just curve        # the two profiles above; promotes bench/curve.csv and curve.svg
just tune         # the flow-control sweep
just allocator    # the ADR 0010 A/B (needs Docker)
just calibrate    # abuse thresholds vs legitimate traffic
just attack       # Rapid Reset + backend kill, each with a control
just soak         # five minutes, backend dying throughout
just bench-hot    # criterion micro-benchmarks
just synth        # the CDK stack's template assertions
```
