#!/usr/bin/env bash
# The flow-control tuning sweep (design doc §10.5).
#
# The windows have been reasoned, not measured, since week 5: 256 KiB per stream
# and a 1 MiB connection window, derived from the RFC and from the arithmetic in
# core/src/flow.rs. That was honest while there was nothing to measure them
# against. This sweeps them.
#
# What is actually being traded, and why the sweep reports both columns:
#
#   The connection window **is** the bounded-memory bound. Every DATA octet
#   debits both the stream and the connection window, so in-flight octets can
#   never exceed the connection window no matter how many streams are open —
#   that is the claim core/tests/backpressure.rs asserts. Raising the window
#   buys throughput on a fast link and raises the ceiling of memory the proxy
#   will hold for a client that has stopped reading. There is no setting that is
#   better at both, so a sweep that reported only req/s would be recommending a
#   memory regression.
#
# Usage:
#   bench/tune.sh
#   CONN_WINDOWS="1048576 4194304" STREAM_WINDOWS="262144" bench/tune.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/tune-$STAMP.csv"

BACKEND_ADDR="${BACKEND_ADDR:-127.0.0.1:8080}"
PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:8443}"
METRICS="${METRICS:-127.0.0.1:9090}"
TARGET="https://$PROXY_ADDR/"

# Defaults are 1 MiB / 256 KiB / 256 streams (core/src/flow.rs, core/src/conn.rs).
CONN_WINDOWS="${CONN_WINDOWS:-262144 1048576 4194304 16777216}"
STREAM_WINDOWS="${STREAM_WINDOWS:-65536 262144 1048576}"
CONCURRENCIES="${CONCURRENCIES:-256}"
RATE="${RATE:-20000}"
SECONDS_PER_POINT="${SECONDS_PER_POINT:-12}"
CONNECTIONS="${CONNECTIONS:-50}"
# The slow-client probe: a large response nobody drains quickly, which is what
# makes the memory column mean something. Without it every point reports a
# bridge that never filled.
BIG="${BIG:-4194304}"

mkdir -p "$RESULTS"

echo "building release binaries" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd -p loadgen)

"$ROOT/target/release/backend" &
backend_pid=$!
trap 'kill $backend_pid ${proxy_pid:-} 2>/dev/null || true' EXIT

echo "conn_window,stream_window,max_concurrent_streams,achieved_rps,p50_ms,p99_ms,p999_ms,bridge_peak_bytes,stalls" > "$CSV"

point() {
  local conn_window="$1" stream_window="$2" concurrency="$3"

  H2PROXYD_UPSTREAMS="$BACKEND_ADDR" H2PROXYD_LISTEN="$PROXY_ADDR" \
    H2PROXYD_METRICS="$METRICS" \
    H2PROXYD_CONN_WINDOW="$conn_window" \
    H2PROXYD_STREAM_WINDOW="$stream_window" \
    H2PROXYD_MAX_CONCURRENT_STREAMS="$concurrency" \
    "$ROOT/target/release/h2proxyd" >>"$RESULTS/tune-$STAMP-proxy.log" 2>&1 &
  proxy_pid=$!
  for _ in $(seq 1 50); do
    curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null && break
    sleep 0.2
  done

  # A deliberately slow reader of a large response, running underneath the rate
  # test: this is what pushes octets into the bridge, and the bridge's peak is
  # the memory half of the trade-off.
  (curl -sk --http2 --limit-rate 200K "https://$PROXY_ADDR/bytes/$BIG" -o /dev/null || true) &
  local slow_pid=$!

  local out
  out=$("$ROOT/target/release/loadgen" --url "$TARGET" --rate "$RATE" \
        --connections "$CONNECTIONS" --duration "$SECONDS_PER_POINT" --warmup 3 \
        --label "cw$conn_window-sw$stream_window-mcs$concurrency" \
        2>>"$RESULTS/tune-$STAMP-loadgen.log" | tail -1)
  local achieved p50 p99 p999
  IFS=, read -r _ _ _ _ _ _ _ achieved p50 _ p99 p999 _ _ _ <<<"$out"

  local scrape peak stalls
  scrape=$(curl -s "http://$METRICS/metrics")
  peak=$(echo "$scrape" | awk '/^h2proxy_bridge_buffered_bytes_peak /{print $2}')
  stalls=$(echo "$scrape" | awk '/^h2proxy_flow_control_stalls_total /{print $2}')

  kill "$slow_pid" 2>/dev/null || true
  # SIGTERM, not SIGKILL: the drain is part of what is being measured, and a
  # proxy killed mid-run would leave the next point's port in TIME_WAIT.
  kill "$proxy_pid" 2>/dev/null || true
  wait "$proxy_pid" 2>/dev/null || true

  echo "$conn_window,$stream_window,$concurrency,${achieved:-NA},${p50:-NA},${p99:-NA},${p999:-NA},${peak:-NA},${stalls:-NA}" >> "$CSV"
  echo "  cw=$conn_window sw=$stream_window mcs=$concurrency -> ${achieved:-NA} req/s, p99 ${p99:-NA} ms, bridge peak ${peak:-NA} B" >&2
}

for conn_window in $CONN_WINDOWS; do
  for stream_window in $STREAM_WINDOWS; do
    for concurrency in $CONCURRENCIES; do
      # A stream window above the connection window is legal and pointless: the
      # connection window binds first, so the point would duplicate another one.
      if [ "$stream_window" -gt "$conn_window" ]; then
        continue
      fi
      echo "== cw=$conn_window sw=$stream_window mcs=$concurrency ==" >&2
      point "$conn_window" "$stream_window" "$concurrency"
    done
  done
done

echo >&2
column -s, -t "$CSV" >&2
echo >&2
echo "The column to read second is bridge_peak_bytes: throughput bought with" >&2
echo "memory is a trade, and the connection window is the bounded-memory bound." >&2
echo "written: $CSV" >&2
