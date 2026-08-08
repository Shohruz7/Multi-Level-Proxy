# ADR 0010 — Allocator: swap in jemalloc for benchmark/production builds

Status: accepted (wiring deferred to week 8) · Date: 2026-07-04 · Design doc: §9.2, §10.5

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
