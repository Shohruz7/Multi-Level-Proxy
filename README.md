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
- **Fuzzing** — `cargo-fuzz` targets for the frame parser and the HPACK
  decoder, both fed wholly unconstrained input. The contract is total: any
  input yields `Err`, `Ok(None)` or a frame — never a panic.

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
`just fuzz 60 hpack_decoder`.

Connect a real HTTP/2 client and watch a request get decoded to its fields:

```sh
RUST_LOG=h2proxy_core=debug cargo run -p h2proxyd
curl -vk --http2 --max-time 3 https://127.0.0.1:8443/
#   → * ALPN: server accepted h2 ... * using HTTP/2
#   daemon logs: decoded header block
#     headers=[:method: GET, :path: /, :authority: 127.0.0.1:8443, ...]
```

## Current state

The client-facing half of the engine is built and tested: TLS 1.3 with ALPN,
the connection preface and SETTINGS handshake, the full frame codec, HPACK, and
the connection-control frames (SETTINGS, PING, GOAWAY) with the GOAWAY error
path. A request is accepted, decompressed and understood down to its individual
header fields.

It does not forward traffic yet. Per-stream demultiplexing and responses, flow
control, the upstream pool and load balancer, and the backpressure bridge are
the remaining work — so a request currently establishes, decodes, and then
waits. The modules for each are in place with their types and contracts
defined.

## License

MIT — see [LICENSE](LICENSE).
