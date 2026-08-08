# Benchmarks

The project makes two performance claims, and it measures them with **two
distinct load profiles** (design doc §10). Keeping them separate is the point:
one stresses request rate, the other stresses concurrency, and they fail in
different ways.

| Profile | Question it answers | Shape |
|---|---|---|
| **Throughput** | How many small requests/sec at an honest tail? | Many short requests over a moderate connection count; push the offered rate to the saturation knee. Target ≈ **85k req/s** on small payloads with **p99 < 3 ms**. |
| **Concurrency** | How many simultaneous streams hold up? | Long-lived streams ramped to **10,000+** concurrent, memory and fairness watched, rate secondary. |

### The harnesses

| Script | `just` | What it answers |
|---|---|---|
| `baseline.sh` | `baseline` | Client → backend directly, no proxy. The subtrahend. |
| `proxy-baseline.sh` | `bench-proxy` | Client → proxy → backend, closed loop. The regression gate. |
| `curve.sh` | `curve` | **The headline**: delivered rate and corrected p99 vs *offered* rate, stepped to and past the knee. Promotes `curve.csv` and `curve.svg`. |
| `tune.sh` | — | Sweeps the flow-control windows and concurrency, reporting throughput **and** the bridge's peak occupancy, because the connection window is the memory bound. |
| `allocator.sh` | — | The ADR 0010 A/B, interleaved, inside the musl container where the claim lives. |
| `calibrate.sh` | `calibrate` | Measures the abuse thresholds against legitimate traffic; fails under 10× headroom. |
| `attack.sh` | `attack` | Rapid Reset and a backend kill, each against a control run. |
| `soak.sh` | `soak` | Five minutes with a backend dying every 30 s; fails if anything that must stay flat grew. |

### Week-8 target parameters (full run, on Graviton behind the NLB)

- **Throughput:** ~500 connections, small (≤1 KB) responses, offered rate stepped
  up to the knee, 30–60 s steady-state per step, coordinated-omission-correct.
- **Concurrency:** connections × streams ramped so concurrent streams cross 10k,
  long-lived, watching `h2proxy_active_streams`, pool utilization, and RSS.

These are the deployment numbers. The script here captures a **laptop-scale,
no-proxy baseline** with the same *shape* so the machinery (generator, parser,
CSV) is proven now and every later number has something to diff against.

## Traffic generators: h2load for throughput, `loadgen` for the tail

The backend and the proxy speak HTTP/2. **wrk2 is HTTP/1.1-only**, so it cannot
drive an h2 endpoint — pointing it at the backend would silently measure a
different protocol. **h2load** (from nghttp2) drives the throughput and
regression harnesses (`baseline.sh`, `proxy-baseline.sh`, `attack.sh`,
`soak.sh`).

**It cannot produce the tail-latency number this page promised**, and week 8
found out why. h2load's `--rate` creates *connections* per period, not requests;
`-D` and `-r` are mutually exclusive; and it keeps `-m` requests in flight per
connection, issuing the next when the last completes. That is a **closed loop**,
and a closed loop cannot queue: a server that stalls for a second simply receives
fewer requests during the stall, so the stall is measured once instead of in
every request a real arrival process would have piled up behind it. That is
**coordinated omission** (§10.1), and it makes closed-loop p99s optimistic in
exactly the circumstances anyone cares about.

So the corrected numbers come from [`loadgen/`](../loadgen/), written for this:

- a **fixed request schedule**, `start + n × interval`, never derived from "now";
- **no throttling of any kind** — an in-flight cap would be a closed loop
  reintroduced by the back door, since it stops offering load precisely when the
  server gets slow;
- latency measured from when each request was **supposed** to be sent;
- the closed-loop figure for the same requests reported alongside, so a run
  **states the size of its own correction** rather than asserting it matters;
- and the generator's own **dispatch lag** printed beside every number, because
  a load generator that cannot fail its own honesty check will eventually
  report itself instead of the target.

Quote a p99 from `curve.sh`/`loadgen`, not from an h2load run.

## Capturing the baseline

```sh
# 1. Start the h2c backend (fixed port 8080):
just run-backend            # or: cargo run -p backend

# 2. In another shell, capture the no-proxy baseline:
just baseline               # or: bench/baseline.sh
```

Output:

- `bench/results/baseline-<UTC>.csv` — timestamped capture (git-ignored).
- `bench/results/h2load-<profile>-<UTC>.txt` — raw h2load logs (git-ignored).
- `bench/baseline.csv` — the latest capture, promoted to the committed reference
  number.

Override the target with `BACKEND=http://host:port bench/baseline.sh`.
