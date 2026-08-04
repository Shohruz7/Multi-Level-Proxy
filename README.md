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
  under any speed mismatch. Nothing is buffered and nothing blocks: credit is
  relayed, so the bound is the *window* (1 MiB) rather than the response size.
- **Coalescing** — a shared pool behind every client connection. Twenty client
  connections carrying 400 streams land on a handful of upstream connections,
  and the stream-id remapping between the two id spaces is the pool's job.
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
- **Differential tests in both roles** — the `h2` crate is the oracle on each
  side: a real `h2` client runs against our server engine, and a real
  `h2::server` runs against our hand-built *client* engine. A session only
  progresses if the frames we synthesize are ones a mature implementation
  accepts.
- **A bounded-memory test** — a backend producing 64 MiB against a client
  reading a few KB at a time, asserting that the octets held between them stay
  under one connection window *and* that the backend provably stopped. Both
  halves matter: flat memory alone would also describe a proxy that dropped data.
- **Conformance** — [h2spec](https://github.com/summerwind/h2spec)'s RFC 9113
  suite runs against the live daemon: **146/146, with and without the proxy path
  in front of a real backend**. It earned its place by finding six real defects
  the suites above did not, including a duplicate END_STREAM that every
  well-behaved client hides, and a second field section being read as trailers
  before the state machine was asked whether the peer could still send.

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
  frame  hpack  stream  flow          the protocol primitives
  conn  upstream                      the two connection engines: server, client
  service  proxy  pool  lb            what answers a stream, and where it goes
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
h2c backend *and* the proxy wired to it — the full client → proxy → backend path.
`just run-server` runs the engine with no backends, answering from its built-in
responder. `just baseline` captures the no-proxy baseline (see
[bench/README.md](bench/README.md)). Fuzzing: `just fuzz 60 frame_parser` or
`just fuzz 60 hpack_decoder`. Conformance: `just h2spec` (server) or
`just h2spec-proxy` (through the proxy); needs `brew install h2spec`.

Proxy a request to a real backend:

```sh
just dev    # backend on :8080, proxy on :8443 forwarding to it

curl -k --http2 https://127.0.0.1:8443/bytes/100000 -o /dev/null \
  -w '%{http_version} %{http_code} %{size_download}\n'
#   → 2 200 100000        ← served by the backend, through the proxy
```

Many streams, and the coalescing evidence:

```sh
h2load -n 20000 -c 50 -m 20 https://127.0.0.1:8443/
#   → 20000 succeeded, 0 failed

curl -s http://127.0.0.1:9090/metrics | grep -E 'upstream_|bridge_'
#   → h2proxy_upstream_pool_connections 8        ← 50 client connections in
#   → h2proxy_upstream_streams_active 0
#   → h2proxy_bridge_buffered_bytes_peak 3072    ← the bridge is holding ~nothing
```

The bounded-memory claim, watched live: run a slow client against a large
response and `h2proxy_bridge_buffered_bytes` stays flat near one connection
window while the transfer runs, instead of tracking the response size.

## Current state

**It is a proxy.** Both halves of the engine are hand-built and tested: the
server side that faces clients, and the client side that faces backends. A
request arrives over TLS, is decoded and validated, forwarded over a pooled h2c
connection to a backend, and the response is streamed back — with the two
connections' flow-control windows coupled so that neither peer can outrun the
other into this process's memory.

What works and is covered by tests: TLS 1.3 + ALPN, the full frame codec, HPACK
in both directions with independent dynamic tables, the stream lifecycle,
two-level flow control with fair interleaving, the upstream connection pool with
coalescing and stream-id remapping, least-outstanding-streams load balancing,
trailers across both legs, and the backpressure bridge. h2spec passes 146/146
both as a plain server and with the proxy path in front of a real backend.

Still to come: **week 7** — graceful GOAWAY drain, health checking with outlier
ejection, idempotent retries, and the §6 abuse mitigations (Rapid Reset,
control-frame and empty-frame floods). **Week 8** — the CDK stack, the deployed
load test, and the tuning pass that turns the reasoned window sizes into
measured ones.

## License

MIT — see [LICENSE](LICENSE).
