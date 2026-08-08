#!/usr/bin/env bash
# The latency-vs-offered-load curve (design doc §10.1) — the headline measurement
# of week 8, and the one every earlier harness deferred to.
#
# What makes this different from baseline.sh and proxy-baseline.sh: those are
# *closed loop*. h2load keeps -m requests in flight per connection and issues the
# next when the last completes, so it cannot offer a fixed request rate and it
# cannot suffer a queue. A server that stalls simply receives fewer requests,
# and the stall is measured once instead of in everything that a real arrival
# process would have piled up behind it. That is coordinated omission, and
# bench/README.md has promised since week 2 that week 8 would correct for it.
#
# So this harness drives `loadgen` instead (see loadgen/src/main.rs): a fixed
# schedule, no throttling, and latency measured from when each request was
# *supposed* to go out. It steps the offered rate up to and past the knee — the
# point where achieved rate stops tracking offered rate — because the knee, not
# the peak, is the number that means anything.
#
# Each step also records the closed-loop p99 for the same requests, so the run
# states the size of the correction rather than asserting that it matters.
#
# Usage:
#   bench/curve.sh                       # both profiles, promotes bench/curve.csv
#   RATES="10000 20000 30000" bench/curve.sh
#   PROFILE=throughput bench/curve.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LABEL="${LABEL:-curve}"
CSV="$RESULTS/curve-$LABEL-$STAMP.csv"

BACKEND_ADDR="${BACKEND_ADDR:-127.0.0.1:8080}"
PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:8443}"
METRICS="${METRICS:-127.0.0.1:9090}"
TARGET="https://$PROXY_ADDR/"

# Offered rates, in req/s. The default ladder brackets the knee found on the dev
# machine (between 20k and 40k); override for other hardware.
RATES="${RATES:-2000 5000 10000 15000 20000 25000 30000 40000 50000}"
CONNECTIONS="${CONNECTIONS:-50}"
STEP_SECONDS="${STEP_SECONDS:-15}"
WARMUP="${WARMUP:-3}"
PROFILE="${PROFILE:-both}"

mkdir -p "$RESULTS"

echo "building release binaries (a debug measurement is not a measurement)" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd -p loadgen)

"$ROOT/target/release/backend" &
backend_pid=$!
H2PROXYD_UPSTREAMS="$BACKEND_ADDR" H2PROXYD_LISTEN="$PROXY_ADDR" \
  H2PROXYD_METRICS="$METRICS" \
  "$ROOT/target/release/h2proxyd" >"$RESULTS/curve-$LABEL-$STAMP-proxy.log" 2>&1 &
proxy_pid=$!
trap 'kill $backend_pid $proxy_pid 2>/dev/null || true' EXIT

# Wait for the listener rather than sleeping a guess: a step that starts before
# the proxy is up measures the connect retry.
for _ in $(seq 1 50); do
  if curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null; then break; fi
  sleep 0.2
done

# Which allocator the binary under test was built with, read off /metrics rather
# than assumed — the A/B in bench/allocator.sh depends on being able to tell the
# arms apart afterwards.
allocator=$(curl -s "http://$METRICS/metrics" \
  | sed -n 's/^h2proxy_build_info{allocator="\([a-z]*\)".*/\1/p' | head -1)
allocator="${allocator:-unknown}"

echo "profile,offered_rps,achieved_rps,completed,failed,p50_ms,p90_ms,p99_ms,p999_ms,max_ms,closed_loop_p99_ms,dispatch_lag_p99_ms,streams_peak,allocator" > "$CSV"

step() {
  local profile="$1" rate="$2" conns="$3"
  echo "== $profile: $rate req/s over $conns connections ==" >&2
  local out
  out=$("$ROOT/target/release/loadgen" \
        --url "$TARGET" --rate "$rate" --connections "$conns" \
        --duration "$STEP_SECONDS" --warmup "$WARMUP" --label "$profile-$rate" \
        2>>"$RESULTS/curve-$LABEL-$STAMP-loadgen.log" | tail -1)

  # loadgen prints: label,mode,connections,rate_offered,duration,completed,
  # failed,achieved,p50,p90,p99,p999,max,closed_p99,lag_p99
  local completed failed achieved p50 p90 p99 p999 max closed lag
  IFS=, read -r _ _ _ _ _ completed failed achieved p50 p90 p99 p999 max closed lag <<<"$out"

  # The concurrency the proxy actually saw, from its own gauge — the claim
  # "10,000+ concurrent streams" is only worth making with the server's number
  # beside the client's intent.
  local peak
  peak=$(curl -s "http://$METRICS/metrics" \
    | awk '/^h2proxy_stream_concurrency_max /{print $2}' | head -1)

  echo "$profile,$rate,${achieved:-NA},${completed:-NA},${failed:-NA},${p50:-NA},${p90:-NA},${p99:-NA},${p999:-NA},${max:-NA},${closed:-NA},${lag:-NA},${peak:-NA},$allocator" >> "$CSV"
}

if [ "$PROFILE" = "throughput" ] || [ "$PROFILE" = "both" ]; then
  # Small responses over a moderate connection count: the request-rate question.
  for rate in $RATES; do
    step throughput "$rate" "$CONNECTIONS"
  done
fi

if [ "$PROFILE" = "concurrency" ] || [ "$PROFILE" = "both" ]; then
  # The same ladder over many more connections: the question is how many
  # simultaneous streams hold up, and rate is secondary. 500 connections against
  # a 256-stream-per-connection cap is 128,000 admissible streams, so what binds
  # here is the proxy's own bookkeeping rather than its admission limit.
  for rate in $RATES; do
    step concurrency "$rate" "${CONCURRENCY_CONNECTIONS:-500}"
  done
fi

if [ "$LABEL" = "curve" ]; then
  cp "$CSV" "$HERE/curve.csv"
  "$ROOT/bench/plot-curve.py" "$HERE/curve.csv" "$HERE/curve.svg"
  echo >&2
  echo "promoted to bench/curve.csv and bench/curve.svg" >&2
fi

echo >&2
echo "written: $CSV" >&2
column -s, -t "$CSV" >&2

# The knee, stated rather than left to the reader: the last rate at which the
# proxy still delivered what was offered, within 5%.
awk -F, 'NR>1 && $3 != "NA" && $2 > 0 && ($3 / $2) > 0.95 { knee=$2; p99=$8 }
         END { if (knee) printf "\nknee: %s req/s offered, still delivered, p99 %s ms\n", knee, p99 }' \
  "$CSV" >&2
