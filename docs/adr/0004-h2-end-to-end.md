# ADR 0004 — Upstream protocol: HTTP/2 end-to-end, no h2→h1 downgrade in v1

Status: accepted · Date: 2026-07-03 · Design doc: §1.3, §4, §8, §11

## Context

A reverse proxy must speak *something* to its upstreams. Many real proxies
downgrade to HTTP/1.1 upstream because legacy backends demand it.

## Decision

Speak **HTTP/2 on both sides**. The upstream pool holds warm h2 connections and
coalesces many client streams onto few upstream connections.

## Rejected alternative

An **h2→HTTP/1.1 downgrade path**. It is a reasonable production feature, but it
(a) forfeits upstream multiplexing — the connection-collapse thesis of the whole
project (§1.1), and (b) removes the hardest and most interesting problem,
bidirectional flow-control/backpressure bridging (§4.2), replacing it with h1
connection-pool bookkeeping. It also doubles the protocol surface for v1.

## Consequences

- Backpressure bridging is exercised in both directions — the centerpiece week-6
  deliverable and the headline interview story.
- Stream-ID remapping per connection pair (§4.3) becomes necessary and explicit.
- The test/dev backend must speak h2 (hyper/nginx/Caddy all do).
- h1 downgrade is recorded as future work (§11), not scope creep.
