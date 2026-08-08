# ADR 0002 — TLS stack: rustls over OpenSSL

Status: accepted · Date: 2026-07-03 · Design doc: §3.1, §8, §9.3

## Context

The proxy terminates TLS itself (see ADR 0005) and negotiates `h2` via ALPN, so
the TLS library is on the hot path of every connection and every byte.

## Decision

Use **rustls** (with **tokio-rustls** for the async adapter), TLS 1.3, ALPN
advertising `h2`.

## Rejected alternative

**OpenSSL via FFI.** It reintroduces exactly the memory-safety risk surface Rust
was chosen to avoid (the language decision loses its force if the byte-parsing
hot path is C), adds a native build/link dependency that complicates the static
musl target (ADR 0006), and its ALPN/config API is clumsier than rustls's.

## Consequences

- Memory-safe TLS 1.3 end to end; no C toolchain in the container build.
- Clean ALPN support: `ServerConfig::alpn_protocols = [b"h2"]`.
- We accept rustls's constraints (no TLS < 1.2, stricter cert handling) — fine,
  since we control both the deployment and the test certs (rcgen self-signed,
  §9.3).
