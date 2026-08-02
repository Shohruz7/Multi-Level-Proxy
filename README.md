# h2proxy — an HTTP/2 multiplexing reverse proxy, from scratch in Rust

A reverse proxy that speaks HTTP/2 on **both** sides — terminating TLS,
negotiating `h2` via ALPN, multiplexing thousands of client streams, and
coalescing them onto a handful of warm upstream connections. The HTTP/2
protocol engine is hand-built against the wire format; the point of the
project is the parts of HTTP/2 that are easy to gloss over and hard to get
right.

## What it does

A client opens one TLS connection and multiplexes many concurrent requests
over it. h2proxy terminates that connection, decodes the HTTP/2 framing and
compressed headers itself, and forwards each request onto a small pool of
long-lived upstream connections — many streams in, few connections out.

```
                   ┌──────────────── h2proxy ────────────────┐
  clients          │                                          │      upstreams
  ───────          │  TLS 1.3 ─ ALPN h2                        │      ─────────
  ~500 conns ─────▶│  frame codec ─ HPACK ─ stream machine     │──┐
  10k+ streams     │  flow control ─ backpressure bridge       │  ├─▶ few warm
                   │  connection pool ─ load balancer          │──┘   h2 conns
                   └──────────────────────────────────────────┘
```

The parts that carry the weight:

- **Framing** — a streaming codec over a reassembly buffer that advances only
  on a complete frame, so a frame split across TCP segments is never
  mis-parsed. All eight frame types the proxy uses, with their length,
  stream-id and flag rules, and each violation mapped to the error code the
  RFC mandates.
- **HPACK** — the stateful one. Integer and string primitives, the Huffman
  code as a derived state machine, the 61-entry static table, and the dynamic
  table with its eviction accounting. Encoder and decoder tables must stay in
  lockstep across a whole connection: a single desync corrupts every later
  header block.
- **Streams and flow control** — the lifecycle state machine, fair outbound
  interleaving so one large response cannot starve small ones, and two-level
  (connection + stream) windows.
- **Backpressure bridging** — the centerpiece. The proxy withholds the
  upstream's `WINDOW_UPDATE` until bytes have drained to the client, so a slow
  client transitively throttles a fast upstream and proxy memory stays bounded
  under any speed mismatch.
- **Resilience** — connection pooling and coalescing, load balancing by
  least-outstanding-streams, health checking with outlier ejection, and
  mitigations for the HTTP/2 abuse patterns (Rapid Reset, control-frame and
  empty-frame floods).

Errors follow HTTP/2's own split: a connection error emits GOAWAY and takes
the connection down; a stream error emits RST_STREAM and leaves it up.

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

## How it is tested

Correctness here is not self-reported. The engine is checked in layers:

- **Unit tests** for each frame type's size, stream-id, flag and padding rules.
- **Differential tests** against the `h2` crate: a real `h2` client runs a full
  session against our engine, and every frame it puts on the wire is decoded
  and re-encoded to **byte-identical** octets.
- **RFC vectors** — HPACK's Appendix C sequences pass byte-for-byte in both
  directions, including the dynamic-table evictions they were designed to
  provoke.
- **Property tests** — encode/decode identity, in-order decoding of a frame
  stream, byte-at-a-time reads never mis-parsing, and encoder/decoder tables
  staying in lockstep over arbitrary header sequences.
- **Property tests over flow control** — that a window never wraps or exceeds
  2³¹−1, that a closed stream leaves no entry behind, and the one that matters:
  we never emit more octets than the peer credited, at either level.
- **Fuzzing** — `cargo-fuzz` targets for the frame parser and the HPACK
  decoder, both fed wholly unconstrained input. The contract is total: any
  input yields `Err`, `Ok(None)` or a frame — never a panic.
- **Conformance** — [h2spec](https://github.com/summerwind/h2spec)'s RFC 9113
  suite runs against the live daemon: **146/146**. It earned its place by
  finding four real defects the suite above did not, including a duplicate
  END_STREAM that every well-behaved client hides.

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
backend/     a tiny hyper h2c upstream, for the local dev loop and baselines
bench/       load-test harness (h2load) and the committed reference baseline
docs/notes/  condensed RFC 9113 / 7541 / 9218 + Rapid Reset notes
docs/adr/    architecture decision records
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

# Prometheus metrics
curl -s http://127.0.0.1:9090/metrics | grep h2proxy_
```

Dev loop (needs [`just`](https://github.com/casey/just)): `just dev` runs a local
h2 backend plus the proxy; `just baseline` captures the no-proxy baseline (see
[bench/README.md](bench/README.md)). Fuzzing: `just fuzz 60 frame_parser` or
`just fuzz 60 hpack_decoder`. Conformance: `just h2spec` against a running
daemon (needs `brew install h2spec`).

Connect a real HTTP/2 client and get a response:

```sh
cargo run -p h2proxyd
curl -k --http2 https://127.0.0.1:8443/ -o /dev/null -w '%{http_version} %{http_code} %{size_download}\n'
#   → 2 200 1024

# Ask for an exact response size — the handle the load and flow-control tests use
curl -k https://127.0.0.1:8443/bytes/1000000 -o /dev/null -w '%{size_download}\n'
#   → 1000000
```

Many streams at once, and the flow-control evidence:

```sh
h2load -n 10000 -c 10 -m 100 https://127.0.0.1:8443/
#   → 10000 succeeded, 0 failed
curl -s http://127.0.0.1:9090/metrics | grep -E 'concurrency|stalls'
#   → h2proxy_stream_concurrency_max 100
#   → h2proxy_flow_control_stalls_total 269      ← backpressure, actually engaging
```

## Current state

The client-facing half of the engine is built and tested, and it is a working
HTTP/2 server: TLS 1.3 with ALPN, the preface and SETTINGS handshake, the full
frame codec, HPACK, the stream lifecycle with its ID and concurrency rules, and
two-level flow control with fair outbound interleaving. Hundreds of streams run
concurrently on one connection, and a sender that exhausts its window
demonstrably stops and resumes on `WINDOW_UPDATE`. It passes h2spec 146/146.

**It does not forward traffic yet.** Requests are answered by a built-in
responder standing in for the real request path. The upstream pool, the load
balancer, and the backpressure bridge that couples the two connections' windows
are the remaining work; the modules for each are in place with their types and
contracts defined.

## License

MIT — see [LICENSE](LICENSE).
