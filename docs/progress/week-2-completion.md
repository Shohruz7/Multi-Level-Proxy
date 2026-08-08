# Week 2 — Completion Report

Date: 2026-07-04 · Branch: `week2-engine-skeleton` · Spec: `Documentation/Week-2-tasks.md`

Local-only record (this file lives under the gitignored `docs/`, so it is not
pushed). Summarizes what was built, how it was verified, where it deviated from
plan, and what remains.

---

## 1. Scope

Executed against the seven Week-2 workstreams. Two decisions set by the user
before implementation:

- **Scope:** *Code + Dockerfile, defer CDK.* Everything local plus the aarch64
  static-musl Dockerfile; the live AWS CDK/NLB deploy and account setup are
  deferred (tracked, not this week).
- **Dev-loop backend:** a tiny in-repo **Rust hyper** h2 server (for control over
  payload size and h2 SETTINGS), not Caddy/nginx.

Already done coming in (week 1 / early overlap): accept loop + `JoinSet` spawn +
SIGTERM/SIGINT→broadcast→drain in `h2proxyd/src/main.rs`, `tracing` env-filter,
and the README roadmap.

---

## 2. What shipped, by workstream

### 2.1 Core types & traits — `core/src/*.rs`
Filled all eight doc-only stubs with real types and `todo!()`-bodied signatures
(bodies land weeks 3–6). Everything is `pub` so `clippy -D warnings` stays clean
(no dead-code on staged fields).

- `frame.rs` — `FrameHeader`, `FrameType` (incl. `Unknown(u8)` for §4.1 ignore),
  `Flags`, the `Frame` enum (Data/Headers/Settings/WindowUpdate/RstStream/Ping/
  GoAway/Continuation), `FrameError` (+ `code()` → `ErrorCode`), and **`FrameCodec`
  with a real, byte-exact SETTINGS encode/decode** — the one frame implemented
  now so the differential harness has a passing case. Streaming `decode` returns
  `Ok(None)` and consumes nothing on a partial frame (the §3.2 reassembly rule).
  Includes 3 unit tests.
- `stream.rs` — `StreamId` newtype (31-bit, client-odd/server-even helpers),
  `StreamState` (reserved/push states intentionally omitted — `ENABLE_PUSH=0`),
  `StreamEvent`, `transition()` sig.
- `conn.rs` — `PREFACE`, `ErrorCode` (RFC §7, `as_u32`/`from_u32`), the ADR-0008
  error model (`ConnectionError`→GOAWAY, `StreamError`→RST_STREAM, via thiserror),
  `Settings` + protocol defaults + `setting_id` constants. (Topology added below.)
- `flow.rs` — signed `Window`, `consume`/`increase` sigs, default/max window consts.
- `hpack.rs` — `Header`, empty `HpackEncoder`/`HpackDecoder` + `encode`/`decode` sigs.
- `pool.rs` — `ConnectionPool` trait (assoc `Conn`, checkout/checkin), `PoolError`.
- `lb.rs` — `Backend`, `LoadBalancer` trait (`pick`).
- `proxy.rs` — thin `Proxy` placeholder.

### 2.2 Differential-test harness — `core/tests/`
- `differential.rs` — **Design note:** the `h2` crate's `frame`/`codec`/`hpack`
  modules are private (confirmed not exposed even under its `unstable` feature).
  So the oracle runs at the **handshake boundary**: it drives `h2::client::handshake`
  over a `tokio::io::duplex`, captures the real connection preface + initial
  SETTINGS bytes h2 puts on the wire, decodes them with our `FrameCodec`, and
  asserts a re-encode is **byte-identical** to h2's output. This is the scaffolding
  week-3 frame types plug into.
- `frame_proptest.rs` — proptest strategy generating SETTINGS frames; encode→decode
  identity.
- `fuzz/` — cargo-fuzz crate (own `[workspace]`), target `frame_parser` shaped onto
  the SETTINGS path. Ran **8.5M executions in 11s, zero crashes**.

### 2.3 Connection task topology — `core/src/conn.rs`, `h2proxyd/src/main.rs`
- `Connection<IO>` reader-lifecycle: `run()` selects socket-read vs the shutdown
  signal and exits cleanly on EOF or drain. Prototyped the §2.2 seam: `ToStream`,
  `FromStream`, `Dispatcher` (`HashMap<StreamId, mpsc::Sender<ToStream>>`), and
  `STREAM_CHANNEL_BOUND` (the §4.2 backpressure knob). Two lifecycle unit tests.
- Wired into the daemon: `handle_connection` now races the TLS handshake against
  shutdown, checks ALPN, then hands the stream to `Connection::run`. Rationale in
  `docs/adr/0009-connection-task-topology.md`.

### 2.4 Local dev loop
- `backend/` — hyper **h2c** upstream; env-tunable `BACKEND_LISTEN`,
  `BACKEND_BODY_SIZE`, `BACKEND_MAX_CONCURRENT_STREAMS`, `BACKEND_INITIAL_WINDOW_SIZE`.
- `compose.yaml` + `backend/Dockerfile` — `docker compose up` runs the backend on
  a fixed port. `.dockerignore` keeps the build context tiny (excludes `target/`
  etc.).
- `justfile` — `run-proxy`, `run-backend`, `dev`, `backend-up`, `curl-backend`,
  `curl-through`, `test`, `fmt`, `clippy`, `fuzz-build`, `fuzz`, `baseline`.

### 2.5 Observability + baseline
- `/metrics` — Prometheus exporter on `:9090` (`metrics` + `metrics-exporter-
  prometheus`), three seeded stub series (`h2proxy_active_streams`,
  `h2proxy_requests_total`, `h2proxy_upstream_pool_connections`). Bind failure is
  non-fatal. In `h2proxyd/src/main.rs::init_metrics`.
- `bench/` — `baseline.sh` (h2load), `baseline.csv` (committed reference),
  `README.md` (profiles + methodology). See §4 for the deviation from wrk2.

### 2.6 Production image + allocator
- `Dockerfile` — multi-stage: `rust:1.96-slim` build (musl-tools/clang/cmake for
  aws-lc-rs) → `cargo build --release --target aarch64-unknown-linux-musl` →
  `scratch`. Build with `docker buildx build --platform linux/arm64 .`.
- `docs/adr/0010-jemalloc-allocator.md` — jemalloc swap decided, wiring deferred to
  week 8; self-signed-cert decision already in `tls.rs`.

---

## 3. Verification evidence

| Check | Result |
|---|---|
| `cargo fmt --all --check` | clean (exit 0) |
| `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| `cargo nextest run --workspace` | **8/8 pass** (frame ×3, conn ×2, differential, proptest, smoke) |
| `cargo +nightly fuzz build frame_parser --target aarch64-apple-darwin` | builds |
| fuzz run (10s) | 8.5M execs, **0 crashes** |
| Daemon live | `openssl s_client -alpn h2` → `ALPN protocol: h2`; `curl --http2` → "server accepted h2"; engine logs "connection closed" on EOF; SIGTERM drains, exit 0 |
| `/metrics` | returns all three stub series in Prometheus text |
| Backend live | `curl --http2-prior-knowledge` → http_version=2, 200 |

**Baseline captured** (`bench/baseline.csv`, loopback, client→backend, no proxy,
64-byte responses):

| profile | conns | streams/conn | requests | req/s | p99 | mean |
|---|---|---|---|---|---|---|
| throughput | 100 | 10 | 200000 | ~161,800 | 13.85 ms | 5.06 ms |
| concurrency | 50 | 200 | 100000 | ~246,900 | 108.57 ms | 33.64 ms |

These are the numbers every later optimization is measured against.

---

## 4. Deviations from the plan (and why)

- **Differential oracle at the handshake boundary, not frame-level.** `h2`'s frame
  codec is private, so "our encode → h2 decode" on a lone frame is impossible via
  its public API. Capturing h2's real wire bytes and round-tripping them is the
  faithful, realizable version — arguably stronger (tests against real output).
- **h2load instead of wrk2 for the baseline.** wrk2 is HTTP/1.1-only and cannot
  drive the h2 backend. h2load (nghttp2) is the h2 load tool; wrk2 remains the
  coordinated-omission methodology reference for week-8 tail-latency (documented in
  `bench/README.md`). Flagged during planning.
- **`StreamState` omits reserved/push states.** Matches the module's own contract
  (push disabled → PUSH_PROMISE is a protocol error, not a modeled state).

---

## 5. Blockers / unverified

- **Docker Desktop is broken on this machine** — the containerd/buildkit store
  throws `input/output error` on every operation (host disk is fine; the Docker VM
  disk is corrupted). `docker compose config` parses fine, but **no image actually
  builds**. Therefore `backend/Dockerfile` and the aarch64 `Dockerfile` are written
  and correct but **unverified**. Also blocks native musl cross-build (aws-lc-rs
  needs a Linux cross toolchain). Action: reset/repair Docker Desktop, then
  `docker buildx build --platform linux/arm64 -t h2proxyd:arm64 .` and
  `docker compose build backend`.

---

## 6. Deferred (per scope choice)

AWS account basics (deploy IAM role, region, creds, budget alarm), the CDK
skeleton stack (VPC, Graviton instance, SG, internet-facing NLB/TCP listener), and
the live deploy-behind-NLB smoke test. Start once account basics are in place.

---

## 7. Environment changes made this session

- Installed **nightly** toolchain (minimal, for cargo-fuzz), **just** (cargo-binstall),
  **nghttp2/h2load** (brew). Stable pin remains 1.96.1.
- New workspace deps: `hyper`, `hyper-util`, `http-body-util` (backend);
  `metrics`, `metrics-exporter-prometheus` (h2proxyd). `Cargo.lock` updated.
- `cargo-fuzz` mis-defaults to x86_64 on Apple Silicon — always pass
  `--target aarch64-apple-darwin` (the justfile handles this).

---

## 8. Files

**Modified:** `Cargo.toml`, `Cargo.lock`, `.gitignore`, `README.md`,
`core/src/{frame,stream,conn,flow,hpack,pool,lb,proxy}.rs`, `h2proxyd/Cargo.toml`,
`h2proxyd/src/main.rs`.

**New (tracked):** `.dockerignore`, `Dockerfile`, `compose.yaml`, `justfile`,
`backend/` (Cargo.toml, Dockerfile, src/main.rs), `core/tests/{differential,frame_proptest}.rs`,
`fuzz/` (Cargo.toml, fuzz_targets/frame_parser.rs), `bench/{baseline.sh,baseline.csv,README.md}`.

**New (local-only, gitignored `docs/`):** `docs/adr/0009-connection-task-topology.md`,
`docs/adr/0010-jemalloc-allocator.md`, this report.

---

## 9. Next — Week 3

Framing codec + preface + SETTINGS handshake (spec: `Documentation/Week3-8.md`):
implement encode/decode for the remaining frame types, the preface + SETTINGS
exchange, and feed every frame type into the differential harness as it lands.
Milestone: a real h2 client completes the handshake against our server, every
frame type passes round-trip-vs-h2, and the fuzzer runs clean.
