#!/usr/bin/env bash
# Capture the through-proxy profile: client -> proxy -> backend, the full path
# (design doc §10). The sibling of baseline.sh, which measures client -> backend
# with no proxy in the way; the difference between the two CSVs is the cost of
# the proxy itself.
#
# Why this exists, and why it is captured before week 7's hardening lands: week 7
# adds per-frame work to the connection loop (the abuse guard) and per-request
# work to the proxy path (the RED histogram). "It costs nothing" is a claim, and
# a claim needs a number from before. Re-run this at the end of the week and diff.
#
# Traffic generator: h2load, as in baseline.sh — see bench/README.md for why not
# wrk2. Note that these are *closed-loop* numbers: throughput and mean are honest,
# and the p99 is not a coordinated-omission-corrected tail. Week 8 does the
# corrected run; this is a regression gate, not a headline.
#
# Usage:
#   bench/proxy-baseline.sh                 # builds, starts both, measures, cleans up
#   LABEL=post-week7 bench/proxy-baseline.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LABEL="${LABEL:-baseline}"
CSV="$RESULTS/proxy-$LABEL-$STAMP.csv"

BACKEND_ADDR="${BACKEND_ADDR:-127.0.0.1:8080}"
PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:8443}"
TARGET="https://$PROXY_ADDR/"

mkdir -p "$RESULTS"

if ! command -v h2load >/dev/null 2>&1; then
  echo "h2load not found — install nghttp2 (brew install nghttp2)." >&2
  exit 1
fi

echo "building backend + h2proxyd (release: the measurement is meaningless in debug)" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd)

"$ROOT/target/release/backend" &
backend_pid=$!
H2PROXYD_UPSTREAMS="$BACKEND_ADDR" H2PROXYD_LISTEN="$PROXY_ADDR" \
  "$ROOT/target/release/h2proxyd" &
proxy_pid=$!
trap 'kill $backend_pid $proxy_pid 2>/dev/null || true' EXIT

# Wait for the proxy to answer rather than sleeping a guessed interval: a run
# that starts before the listener is up measures the connect retry, not the proxy.
for _ in $(seq 1 50); do
  if curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null; then break; fi
  sleep 0.2
done

echo "profile,connections,streams_per_conn,requests,req_per_s,p99_request,mean_request,raw_log" > "$CSV"

run() {
  local name="$1" conns="$2" streams="$3" reqs="$4"
  local log="$RESULTS/h2load-proxy-$name-$LABEL-$STAMP.txt"
  echo "== $name: h2load -c $conns -m $streams -n $reqs $TARGET ==" >&2
  # Every profile gets one discarded warm-up tenth: the first requests pay for
  # the TLS handshakes and the pool's cold connect, and folding that into a
  # steady-state number makes the proxy look slower than it is.
  h2load -c "$conns" -m "$streams" -n "$((reqs / 10))" "$TARGET" >/dev/null 2>&1 || true
  h2load -c "$conns" -m "$streams" -n "$reqs" "$TARGET" | tee "$log" >&2

  local rps p99 mean
  rps=$(grep -Eo '[0-9.]+ req/s' "$log" | head -1 | awk '{print $1}')
  p99=$(awk '/^request[[:space:]]*:/{print $7; exit}' "$log")
  mean=$(awk '/^request[[:space:]]*:/{print $8; exit}' "$log")
  echo "$name,$conns,$streams,$reqs,${rps:-NA},${p99:-NA},${mean:-NA},$(basename "$log")" >> "$CSV"
}

# Same shape and sizes as baseline.sh, so the two CSVs subtract cleanly.
run throughput 100 10 200000
run concurrency 50 200 100000

if [ "$LABEL" = "baseline" ]; then
  cp "$CSV" "$HERE/proxy-baseline.csv"
  echo >&2
  echo "promoted to bench/proxy-baseline.csv — the number week 7 regresses against" >&2
fi

echo >&2
echo "written: $CSV" >&2
column -s, -t "$CSV" >&2
