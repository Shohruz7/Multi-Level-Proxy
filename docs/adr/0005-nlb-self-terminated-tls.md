# ADR 0005 — Edge: NLB (L4 passthrough) + TLS terminated in the proxy, not an ALB

Status: accepted · Date: 2026-07-03 · Design doc: §8.2, §9.1, §9.3

## Context

The AWS deployment needs an internet-facing load balancer in front of the proxy
instances. AWS offers ALB (L7) and NLB (L4).

## Decision

**Network Load Balancer** with a plain TCP listener; the Rust proxy terminates
TLS itself and owns ALPN negotiation.

## Rejected alternative

**Application Load Balancer.** An ALB is itself an HTTP/2 endpoint: it terminates
the client's h2 connection, parses it, and opens its own connections to targets.
Behind an ALB our proxy would never see a raw HTTP/2 connection — AWS's
implementation would hide exactly the layer this project exists to build. TLS
offload at the edge (ACM on the LB) fails the same way: no TLS in the proxy means
no ALPN in the proxy.

## Consequences

- The TLS handshake, ALPN, and every h2 frame arrive at the proxy intact; the
  NLB forwards TCP segments untouched.
- The proxy carries the TLS cost itself — that cost is part of what the §10
  benchmarks measure, honestly.
- Certificate strategy is self-managed (self-signed for load tests, §9.3) since
  ACM certs can't be exported to an instance.
- Health checks from the NLB are TCP-level; richer checks live in our own §5.2
  logic.
