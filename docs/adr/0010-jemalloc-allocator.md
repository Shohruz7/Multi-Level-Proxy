# ADR 0010 — Allocator: swap in jemalloc for benchmark/production builds

Status: **superseded by its own measurement (2026-08-09)** — implemented, measured,
**not enabled**. Original decision below, verdict at the bottom.
· Date: 2026-07-04 · Design doc: §9.2, §10.5

## Context

The proxy allocates constantly under load: per-frame buffers, per-stream state,
HPACK tables, channel messages. The target deployment is a static
**aarch64-musl** binary, and **musl's default allocator is notoriously slow
under multi-threaded contention** — exactly the workload here (a work-stealing
runtime with many concurrent streams). Allocator choice can move tail latency
and throughput by a large margin, so it belongs on the record as a tuning knob,
measured against the §10 baseline rather than assumed.

## Decision

For benchmark and production builds, **replace the global allocator with
jemalloc** (`tikv-jemallocator`), set as `#[global_allocator]` in the `h2proxyd`
binary. Keep the default allocator for ordinary `cargo build`/tests so local
iteration stays dependency-light.

**Wiring is deferred to week 8**, when there are real numbers to move: it is a
one-line allocator swap plus a Cargo feature, and doing it before the engine
exists would tune against noise. The production `Dockerfile` and this ADR record
the intent so week 8 is a measured change, not a decision.

## Rejected alternatives

- **musl's default allocator.** Simplest, but its known multi-threaded
  contention cost is the specific thing we are avoiding on the deploy target.
- **mimalloc.** A strong alternative with similar wins; jemalloc is chosen for
  its maturity, its `MALLOC_CONF` tunables, and its long track record in exactly
  this proxy/server niche. Revisit only if a benchmark says so.
- **Switch the deploy off musl to glibc** to get glibc's allocator. Gives up the
  static single-binary/`scratch` image (ADR 0006); not worth it when swapping the
  allocator keeps both.

## Consequences

- One measured tuning pass in week 8: build with jemalloc, re-run both profiles,
  record the delta against the baseline (§10.5). Report the number, don't assume
  the win.
- jemalloc links C, so the musl build already needs the cmake/clang toolchain the
  `Dockerfile` installs for aws-lc-rs — no new build-image cost.
- The self-signed-cert decision (§9.3) is unchanged and already lives in
  `h2proxyd/src/tls.rs`: the daemon generates its cert in-process, so the image
  has nothing to mount or rotate.

---

## The measurement (2026-08-09) — and the verdict

This ADR asked for a number rather than an assumption. Here is the number, and
it does not support the decision above.

**Harness:** `bench/allocator.sh`. Both arms built from the same `Dockerfile`
with one build argument different, run **inside the aarch64-musl container** —
the only environment where this ADR's premise means anything — against the same
backend container, **interleaved** A/B/A/B (drift between two blocks is
indistinguishable from an effect), six pairs, 20 s each at 20,000 req/s.

| | system (musl) | jemalloc |
|---|---|---|
| Throughput | 20,000 req/s | 20,000 req/s (**+0.0%**) |
| p99, median of 6 | 37.1 ms | 28.4 ms (−23.5%) |
| p99, **range** across 6 | **2.8 – 122.2 ms** | **5.5 – 88.0 ms** |
| RSS, range across 6 | **3.6 – 5.6 MiB** | **64.9 – 73.3 MiB** |

**Verdict: not enabled.** Read the ranges before the medians. The p99
distributions overlap almost completely, so the 23% median improvement is not an
effect — it is one draw from two noisy samples that happened to land that way.
The RSS difference is the opposite: six pairs, no overlap, a consistent **13×**.
jemalloc's per-CPU arenas cost ~65 MiB on a process whose entire working set is
otherwise under 6 MiB.

Trading a 13× memory increase for a latency change that cannot be distinguished
from noise is not a trade worth making, and on a proxy whose central claim is
*bounded memory* it is close to self-parody.

**What this measurement cannot say.** Throughput was identical because 20,000
req/s is below what the container could deliver — both arms simply met the
offered rate, so the test never reached the allocator-bound saturation where the
original argument (musl's malloc contending across threads) would show itself.
Running at saturation inside Docker Desktop's VM produced numbers too noisy to
separate, which is why the honest report is "no measurable difference here"
rather than "jemalloc does not help".

**The code stays, off by default.** `--features jemalloc` still builds and the
`Dockerfile` still takes `--build-arg FEATURES=jemalloc`, because the question is
worth re-asking on real hardware where the deployed run would put the proxy under
genuine allocator pressure. What is retired is the *assumption* that it would
help — and the ADR's status now says so.

### The other claim in this ADR was also wrong

> "jemalloc links C, so the musl build already needs the cmake/clang toolchain
> the `Dockerfile` installs for aws-lc-rs — **no new build-image cost**."

Three toolchain fixes were required before it compiled at all, none of which
could have been found by reading:

1. `AR=llvm-ar` named a binary the `clang` package does not ship.
2. `CC=clang` compiled C against **glibc** headers and then failed to link
   against musl (`undefined reference to __isoc23_strtol`). `musl-gcc` is the
   only compiler here that sees a consistent set of headers and libraries.
3. aarch64 GCC defaults to `-moutline-atomics`, which links a libgcc startup
   object calling `__getauxval` — a **glibc** symbol musl does not have. Every
   atomics link therefore failed, and jemalloc's `configure` reported the result
   as `#error "Don't have atomics implemented on this platform."` — several
   hundred lines into the build, naming nothing that would lead you to the
   cause. `CFLAGS=-mno-outline-atomics` fixes it.

The general lesson is in [the retrospective](../retrospective.md): an artifact
that has never been executed is a hypothesis, however carefully it was written
and however many documents cite it.
