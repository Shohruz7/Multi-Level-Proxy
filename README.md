# h2proxy — an HTTP/2 multiplexing reverse proxy, from scratch in Rust

A reverse proxy that speaks HTTP/2 on **both** sides — terminating TLS,
negotiating `h2` via ALPN, multiplexing thousands of client streams, and
coalescing them onto a handful of warm upstream connections. The HTTP/2
protocol engine is hand-built against the wire format; the point of the
project is the parts of HTTP/2 that are easy to gloss over and hard to get
right.

> Status: **week 1 of 8** — toolchain, workspace, RFC notes/ADRs, and a
> runnable TLS 1.3 + ALPN handshake are in place. The protocol engine
> (framing, HPACK, streams, flow control, pooling, load balancing) is stubbed
> and lands over weeks 3–6. See the [roadmap](#roadmap).

## Why build it from scratch

Modern services live or die on how well they multiplex work over a few
expensive resources: TCP connections, TLS sessions, file descriptors. HTTP/1.1
forces one in-flight request per connection; HTTP/2 carries many interleaved
streams over one long-lived connection. A proxy that speaks h2 on both sides
takes thousands of client streams arriving over a few hundred connections and
fans them onto a few warm upstream connections. That collapse — fewer
connections doing more work — is the source of the latency and throughput wins
this project targets.

## Scope of "from scratch" (the honest version)

**Hand-built** (this is the learning goal): the frame codec, the complete HPACK
encoder/decoder (static + dynamic tables, Huffman, integer/string primitives),
the stream state machine, connection and stream flow control, and the
connection-management logic that bridges the client and upstream sides.

**Reused, deliberately:** the async runtime (**tokio**) and the TLS 1.3 stack
(**rustls**). Reimplementing an async scheduler or a TLS record layer would be a
different project and a security liability.

**Used for tests only:** the mature [`h2`](https://github.com/hyperium/h2) crate,
as a **differential-testing oracle** — the hand-written codec is fuzzed and
round-tripped against it so that "from scratch" never means "subtly wrong on
the wire."

The reasoning behind each of these — and tokio-vs-glommio, NLB-vs-ALB,
aarch64-musl, the error model, and more — is recorded as
[Architecture Decision Records](docs/adr/). Condensed notes on the relevant
RFCs (9113, 7541, 9218) and the Rapid Reset CVE are in [docs/notes/](docs/notes/).

## Goals and non-goals

- **Goal** — a correct HTTP/2 intermediary that negotiates `h2` over TLS via
  ALPN, multiplexes streams, honors flow control in both directions, and keeps
  **bounded memory** under any speed mismatch between client and upstream.
- **Goal** — 10,000+ concurrent streams and ~85k req/s on small responses with
  sub-3 ms p99 (two figures from two test profiles; see the design doc §10).
- **Goal** — reproducible infrastructure as code (AWS CDK) and a load-test
  harness that reports tail latency honestly.
- **Non-goal** — server push (`ENABLE_PUSH = 0`), a dynamic control plane, or an
  HTTP/1.1 downgrade path in v1.

## Workspace layout

```
core/        h2proxy-core — the hand-built protocol engine (library)
  frame  hpack  stream  flow  conn  pool  lb  proxy   (one module per concern)
h2proxyd/    the reverse-proxy daemon (binary): TLS, sockets, config, signals
docs/notes/  condensed RFC 9113 / 7541 / 9218 + Rapid Reset notes
docs/adr/    architecture decision records (0001–0008)
```

The library/binary split is what lets the differential tests target the engine
directly, without a socket in the way.

## Quickstart

Requires the pinned toolchain (installed automatically from
[`rust-toolchain.toml`](rust-toolchain.toml)).

```sh
# Build and test the workspace
cargo nextest run --workspace      # or: cargo test --workspace

# Run the daemon (defaults to 127.0.0.1:8443; override with H2PROXYD_LISTEN)
cargo run -p h2proxyd
```

Verify the current milestone — TLS 1.3 with ALPN negotiating `h2`:

```sh
openssl s_client -alpn h2 -connect 127.0.0.1:8443 </dev/null | grep ALPN
#   → ALPN protocol: h2

curl -vk --http2 https://127.0.0.1:8443/ 2>&1 | grep ALPN
#   → * ALPN: server accepted h2
```

(The request then stalls — expected: there is no frame layer yet.)

## Roadmap

| Week | Deliverable |
|------|-------------|
| 1 | Toolchain, workspace, RFC notes + ADRs, TLS/ALPN handshake ✅ |
| 2 | Connection skeleton, core types/traits, differential-test harness, dev loop, baseline |
| 3 | Framing codec + preface + SETTINGS handshake |
| 4 | HPACK (static/dynamic tables, Huffman) |
| 5 | Stream state machine + multiplexing + flow control |
| 6 | Backpressure bridging + upstream pool + load balancing — *it becomes a proxy* |
| 7 | Resilience + security hardening (Rapid Reset, flood limits, health checks) |
| 8 | Deploy to Graviton behind an NLB, load-test both profiles, tune, write up |

## License

MIT — see [LICENSE](LICENSE).
