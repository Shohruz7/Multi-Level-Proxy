#!/usr/bin/env bash
# Capture the no-proxy baseline: client -> backend directly, no proxy in the
# path (design doc §10). Every later optimization is measured as a delta against
# this number, so capture it before the proxy can touch traffic.
#
# Traffic generator: h2load (nghttp2) — the backend speaks h2, and h2load is the
# HTTP/2 load tool. wrk2 is HTTP/1.1-only; see bench/README.md for why we use
# h2load here and keep wrk2 for the coordinated-omission methodology in week 8.
#
# Usage:
#   bench/baseline.sh                 # assumes a backend at $BACKEND
#   BACKEND=http://127.0.0.1:8080 bench/baseline.sh
set -euo pipefail

BACKEND="${BACKEND:-http://127.0.0.1:8080}"
HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/baseline-$STAMP.csv"

mkdir -p "$RESULTS"

if ! command -v h2load >/dev/null 2>&1; then
  echo "h2load not found — install nghttp2 (brew install nghttp2)." >&2
  exit 1
fi

echo "profile,connections,streams_per_conn,requests,req_per_s,p99_request,mean_request,raw_log" > "$CSV"

run() {
  local name="$1" conns="$2" streams="$3" reqs="$4"
  local log="$RESULTS/h2load-$name-$STAMP.txt"
  echo "== $name: h2load -c $conns -m $streams -n $reqs $BACKEND ==" >&2
  h2load -c "$conns" -m "$streams" -n "$reqs" "$BACKEND" | tee "$log" >&2

  # Aggregate throughput from the "finished in Xs, Y req/s, ..." summary line;
  # p99 and mean request latency from h2load's percentile table row
  #   request : min  max  median  p95  p99  mean  sd  +/-sd
  # (latency units are kept as printed — us / ms — so they read honestly).
  local rps p99 mean
  rps=$(grep -Eo '[0-9.]+ req/s' "$log" | head -1 | awk '{print $1}')
  p99=$(awk '/^request[[:space:]]*:/{print $7; exit}' "$log")
  mean=$(awk '/^request[[:space:]]*:/{print $8; exit}' "$log")
  echo "$name,$conns,$streams,$reqs,${rps:-NA},${p99:-NA},${mean:-NA},$(basename "$log")" >> "$CSV"
}

# Laptop-scale baseline. The full week-8 profiles (500 conns, saturation-knee
# rate, 10k+ streams) are documented in bench/README.md; these are sized to run
# quickly and still produce a real, comparable number.
run throughput 100 10 200000
run concurrency 50 200 100000

# Publish the latest capture as the committed reference number.
cp "$CSV" "$HERE/baseline.csv"

echo >&2
echo "baseline written: $CSV  (copied to bench/baseline.csv)" >&2
column -s, -t "$CSV" >&2
