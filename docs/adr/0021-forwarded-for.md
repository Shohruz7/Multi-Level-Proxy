# ADR 0021 — `x-forwarded-for`: overwrite by default

Status: accepted · Date: 2026-08-08 · Design doc: §4.3 · RFC 7239

## Context

A reverse proxy terminates the client's connection, so every request a backend
sees comes from the proxy's address. Without a forwarding header the backend
cannot log, rate-limit, or geo-route by client — a visible gap in anything
calling itself a reverse proxy. Week 6 deferred this because it needs the peer
address plumbed from the daemon into the engine.

## Decision

**Set `x-forwarded-for` to the observed peer address, replacing any value the
client sent.** Also set `x-forwarded-proto: https`, which is true by
construction — the daemon only hands the engine an h2-over-TLS stream.

Appending is available behind `H2PROXYD_TRUST_FORWARDED=1`, and off by default.

The security argument is the whole decision. If we append, any client can send
`x-forwarded-for: 10.0.0.1` and the backend receives `10.0.0.1, <real ip>` — with
no way to tell which entry the proxy observed from which the client invented.
Anything keyed on the chain, which in practice means the *first* entry, is then
trivially forged: allowlists, rate limits, abuse attribution, audit logs. The
proxy sits directly behind an NLB (ADR 0005), which is layer 4 and adds no such
header, so the observed peer *is* the client and overwriting is both correct and
safe. Trusting an inbound chain is only sound when something trustworthy
produced it, and that is a deployment fact the operator has to assert.

**This lives in `Proxy`, not in the engine.** A `Proxy` is built per client
connection, so it can simply hold the address; `conn.rs` needs no knowledge of
sockets, and the change is `sanitize`-adjacent and nothing more.

**No `Forwarded` header.** RFC 7239 standardised this in 2014 and almost nothing
reads it. Emitting both means two sources of truth that can disagree — and if
they disagree, a backend has no principled way to choose. One header that
everything parses beats two that half-parse.

## Consequences

- **A proxy with no peer address emits nothing.** The differential tests run the
  engine over duplex pipes, where there is no address to name; inventing one
  would put a falsehood in a header a backend might act on.
- **A chain of proxies needs the flag.** Deployments that put something in front
  of this proxy must set `H2PROXYD_TRUST_FORWARDED=1`, and are then responsible
  for that thing being trustworthy.
- **The header is added after sanitation**, so a client cannot smuggle one past
  the `te` filter or duplicate it — exactly one `x-forwarded-for` reaches the
  backend, which spares it an arbitrary choice.

## Rejected alternatives

**Append by default.** What most proxies do, and the reason forged
`x-forwarded-for` is a standing web-security footgun. The permissive default is
only right when the deployment guarantees a trusted hop in front, which is not a
guarantee a default can make.

**Emit RFC 7239 `Forwarded` as well.** Correct and unread. See above.

**Put it in the engine.** `conn.rs` would need the peer address for a concern
that is entirely about HTTP semantics rather than framing. The `Service` seam
exists to keep that separation.
