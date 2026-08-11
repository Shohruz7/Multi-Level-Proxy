#!/usr/bin/env bash
# Two methodologies, one proxy: what a closed-loop generator reports, and what
# was actually happening.
#
# This is the experiment behind the claim that h2load cannot measure a tail
# (bench/README.md, loadgen/src/main.rs). Everything is held constant except the
# discipline:
#
#   * same proxy, same backend, same 50 connections, same duration per point;
#   * **open loop** — a fixed request schedule, no throttling, latency measured
#     from when each request was *supposed* to be sent. The setpoint is a rate.
#   * **closed loop** — N workers per connection, each issuing its next request
#     when the last completes. The setpoint is concurrency, and the rate is
#     whatever the proxy allows. This is the h2load shape.
#
# Both series are plotted against **delivered** throughput, because that is the
# only axis the two share: it is what the proxy actually did, and it lets the
# question be asked directly — at the same delivered rate, what does each
# methodology say the p99 was?
#
# The two are interleaved rather than run in blocks: a laptop drifts, and drift
# between two blocks is indistinguishable from a difference between methods.
#
# Usage:
#   bench/methodology.sh
#   RATES="10000 20000" STREAMS="1 4 16" bench/methodology.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/methodology-$STAMP.csv"

BACKEND_ADDR="${BACKEND_ADDR:-127.0.0.1:8080}"
PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:8443}"
METRICS="${METRICS:-127.0.0.1:9090}"
TARGET="https://$PROXY_ADDR/"

CONNECTIONS="${CONNECTIONS:-50}"
STEP_SECONDS="${STEP_SECONDS:-12}"
WARMUP="${WARMUP:-3}"
# Offered rates for the open loop, bracketing the knee found in bench/curve.csv.
RATES="${RATES:-5000 10000 15000 20000 25000 30000 40000 50000}"
# Streams per connection for the closed loop. With 50 connections these are
# 50 … 6,400 requests in flight, which spans the same delivered-rate range.
STREAMS="${STREAMS:-1 2 4 8 16 32 64 128}"

mkdir -p "$RESULTS"

echo "building release binaries" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd -p loadgen)

"$ROOT/target/release/backend" &
backend_pid=$!
H2PROXYD_UPSTREAMS="$BACKEND_ADDR" H2PROXYD_LISTEN="$PROXY_ADDR" \
  H2PROXYD_METRICS="$METRICS" \
  "$ROOT/target/release/h2proxyd" >"$RESULTS/methodology-$STAMP-proxy.log" 2>&1 &
proxy_pid=$!
trap 'kill $backend_pid $proxy_pid 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null && break
  sleep 0.2
done

echo "mode,setpoint,achieved_rps,p50_ms,p99_ms,p999_ms,max_ms,service_p99_ms,lag_p99_ms" > "$CSV"

run() {
  local mode="$1" setpoint="$2"
  local args
  if [ "$mode" = "open" ]; then
    args="--rate $setpoint"
  else
    args="--closed-loop --streams $setpoint"
  fi
  echo "== $mode, setpoint $setpoint ==" >&2

  local out
  # shellcheck disable=SC2086
  out=$("$ROOT/target/release/loadgen" --url "$TARGET" $args \
        --connections "$CONNECTIONS" --duration "$STEP_SECONDS" --warmup "$WARMUP" \
        --label "$mode-$setpoint" \
        2>>"$RESULTS/methodology-$STAMP-loadgen.log" | tail -1)

  local achieved p50 p99 p999 max service lag
  IFS=, read -r _ _ _ _ _ _ _ achieved p50 _ p99 p999 max service lag <<<"$out"
  echo "$mode,$setpoint,${achieved:-NA},${p50:-NA},${p99:-NA},${p999:-NA},${max:-NA},${service:-NA},${lag:-NA}" >> "$CSV"
  echo "  -> ${achieved:-NA} req/s delivered, p99 ${p99:-NA} ms" >&2
}

# Interleaved: one open point, one closed point, alternating down both lists.
set -- $STREAMS
for rate in $RATES; do
  run open "$rate"
  if [ $# -gt 0 ]; then
    run closed "$1"
    shift
  fi
done
# Any closed-loop setpoints left over.
for streams in "$@"; do
  run closed "$streams"
done

if [ "${PROMOTE:-1}" = "1" ]; then
  cp "$CSV" "$HERE/methodology.csv"
  "$ROOT/bench/plot-methodology.py" "$HERE/methodology.csv" "$HERE/methodology.svg"
  echo "promoted to bench/methodology.csv and bench/methodology.svg" >&2
fi

echo >&2
column -s, -t "$CSV" >&2

# The headline, computed: at the highest delivered rate the closed loop reached,
# what did each methodology report?
awk -F, '
  NR>1 && $3 != "NA" {
    if ($1 == "closed" && $3 + 0 > cmax) { cmax = $3 + 0; cp99 = $5 }
    if ($1 == "open") { o_rate[NR] = $3 + 0; o_p99[NR] = $5 + 0 }
  }
  END {
    if (!cmax) exit
    # The open-loop point closest to the closed loop peak delivered rate.
    best = ""; gap = 1e18
    for (i in o_rate) { d = o_rate[i] - cmax; if (d < 0) d = -d; if (d < gap) { gap = d; best = i } }
    printf "\nclosed loop peaked at %.0f req/s and reported p99 %s ms\n", cmax, cp99
    if (best != "") printf "open loop at %.0f req/s reported p99 %.3f ms\n", o_rate[best], o_p99[best]
  }' "$CSV" >&2

echo "written: $CSV" >&2
