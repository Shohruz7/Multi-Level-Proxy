# ADR 0017 — Plain h2c to backends, TLS only on the client edge

Status: accepted · Date: 2026-08-03 · Design doc: §4.3, §10.3 · Related: [0002](0002-rustls-over-openssl.md), [0005](0005-nlb-self-terminated-tls.md)

## Context

The proxy terminates TLS 1.3 with ALPN `h2` on the client edge (ADR 0002). The
upstream leg is a separate decision: the backends live behind the same NLB, in
the same VPC, on a private subnet.

## Decision

**Backends are spoken to as plaintext HTTP/2 with prior knowledge** — h2c, no
upgrade dance, no ALPN, no certificate verification. `H2PROXYD_UPSTREAMS` is a
comma-separated list of `host:port`, resolved once at startup.

## Why

- **The threat model does not reach there.** The client edge is the trust
  boundary: it faces the internet and carries credentials from unknown peers.
  Backend traffic never leaves the VPC's private subnets. Encrypting it would
  defend against an attacker who already has packet capture inside the VPC, at
  which point the proxy's own memory is a softer target than its sockets.
- **It keeps the week's cost where the week's value is.** TLS upstream means a
  client config, a trust store, certificate rotation for backends, and a
  handshake on every pool connection — none of which teaches anything the client
  edge did not already teach, and all of which competes with the pool, the load
  balancer, and the bridge.
- **h2c with prior knowledge has no negotiation to get wrong.** No `Upgrade:
  h2c` (removed in RFC 9113), no ALPN, no fallback path. The connection opens
  with the 24-octet preface and the engine is identical to the TLS one, because
  the engine is generic over `AsyncRead + AsyncWrite`.
- **The measurements stay honest.** Week 8 compares against a week-2 baseline
  taken over plaintext h2c. Adding TLS to the upstream leg now would move that
  number for a reason unrelated to anything being measured.

## Consequences

- **Backends must be reachable only from the proxy.** This decision is only
  defensible with a security group that says so; it is a network control
  substituting for a cryptographic one, and it fails silently if the subnet is
  ever made public. Week 8's CDK stack owns that rule, and it is the one place
  this ADR can be invalidated by a config change.
- **No upstream certificate identity.** The proxy trusts DNS and the VPC to tell
  it what a backend is. Acceptable for a static, private, single-tenant backend
  list; not acceptable if backends ever become multi-tenant or externally
  addressable.
- **Adding TLS later is contained.** `UpstreamConnection` is generic over the
  byte stream, so a `tokio_rustls::TlsConnector` wraps the `TcpStream` in
  `Pool::open` and nothing else changes. The seam is one line; it is deliberately
  not abstracted ahead of a need.

## Rejected alternatives

**TLS to backends now.** Realistic for a zero-trust deployment and the right
answer for a multi-tenant one. Rejected for v1 on cost-versus-learning, with the
security-group dependency written down rather than assumed.

**A pluggable transport trait so either could be dropped in.** An abstraction
with one implementation and no second caller. The generic parameter already there
does the same job when the day comes.
