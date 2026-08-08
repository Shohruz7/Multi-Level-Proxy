# Benchmarks

The project makes two performance claims, and it measures them with **two
distinct load profiles** (design doc §10). Keeping them separate is the point:
one stresses request rate, the other stresses concurrency, and they fail in
different ways.

| Profile | Question it answers | Shape |
|---|---|---|
| **Throughput** | How many small requests/sec at an honest tail? | Many short requests over a moderate connection count; push the offered rate to the saturation knee. Target ≈ **85k req/s** on small payloads with **p99 < 3 ms**. |
| **Concurrency** | How many simultaneous streams hold up? | Long-lived streams ramped to **10,000+** concurrent, memory and fairness watched, rate secondary. |

### Week-8 target parameters (full run, on Graviton behind the NLB)

- **Throughput:** ~500 connections, small (≤1 KB) responses, offered rate stepped
  up to the knee, 30–60 s steady-state per step, coordinated-omission-correct.
- **Concurrency:** connections × streams ramped so concurrent streams cross 10k,
  long-lived, watching `h2proxy_active_streams`, pool utilization, and RSS.

These are the deployment numbers. The script here captures a **laptop-scale,
no-proxy baseline** with the same *shape* so the machinery (generator, parser,
CSV) is proven now and every later number has something to diff against.

## Traffic generator: h2load, not wrk2

The backend and the proxy speak HTTP/2. **wrk2 is HTTP/1.1-only**, so it cannot
drive an h2 endpoint — pointing it at the backend would silently measure a
different protocol. We therefore use **h2load** (from nghttp2) for all h2
profiles.

wrk2 still matters for one thing: its **coordinated-omission** correction is the
reference methodology for honest tail latency (§10.1). In week 8, tail-latency
numbers are reported with that correction (via wrk2 against any h1 control path,
or h2load driven at a fixed rate); raw h2load req/s alone is throughput, not a
p99 claim. Don't quote a p99 that came from an uncorrected open-loop run.

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
