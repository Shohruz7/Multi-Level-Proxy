# ADR 0001 — Async runtime: tokio (work-stealing) over glommio (thread-per-core)

Status: accepted · Date: 2026-07-03 · Design doc: §2.2, §8

## Context

The proxy is a concurrency-heavy network daemon: one task per connection, many
streams per connection, and load that is *skewed* — a single client connection can
carry thousands of streams while others idle.

## Decision

Use **tokio** with the multi-threaded, work-stealing scheduler.

## Rejected alternative

**glommio** (thread-per-core, io_uring). It can post higher peak numbers on
Linux, but per-core sharding means a hot connection pins one core while others
sit idle unless connection sharding is managed by hand; the ecosystem (tokio-rustls,
tracing, tokio-console, most of the crates this project leans on) is also built
around tokio. io_uring is Linux-only, complicating macOS development.

## Consequences

- Skewed per-connection load is absorbed by work stealing; no manual sharding.
- Full ecosystem compatibility (tokio-rustls, tokio-console profiling).
- We accept work-stealing overhead (cross-core wakeups, `Send + 'static` bounds on
  task state) as the price; the §10 benchmarks measure the result honestly.
