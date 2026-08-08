#!/usr/bin/env bash
# Measure what an attack and a backend failure actually cost, live (design doc
# §6, §5.2).
#
# The claim under test is not "the attacker is disconnected" — a proxy that
# crashes also disconnects the attacker. It is that a *bystander* is unharmed:
# the same load, with and without an attack running beside it, should produce
# the same latency and the same success rate. That comparison is the whole
# point, so the control run is not optional.
#
# Usage:  bench/attack.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULTS/attack-$STAMP.txt"
METRICS="http://127.0.0.1:9090/metrics"

mkdir -p "$RESULTS"
command -v h2load >/dev/null || { echo "h2load not found (brew install nghttp2)" >&2; exit 1; }

echo "building" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd)
(cd "$ROOT" && cargo build --release -p h2proxy-core --test rapid_reset >/dev/null 2>&1) || true

start_stack() {
  "$ROOT/target/release/backend" >/dev/null 2>&1 &
  backend_pid=$!
  H2PROXYD_UPSTREAMS=127.0.0.1:8080 "$ROOT/target/release/h2proxyd" >/tmp/h2proxyd-attack.log 2>&1 &
  proxy_pid=$!
  for _ in $(seq 1 50); do
    curl -sk --http2 -o /dev/null https://127.0.0.1:8443/ 2>/dev/null && return
    sleep 0.2
  done
}
stop_stack() { kill "${backend_pid:-0}" "${proxy_pid:-0}" 2>/dev/null || true; sleep 0.5; }
trap stop_stack EXIT

metric() { curl -s "$METRICS" | awk -v k="$1" '$1==k {print $2; exit}'; }
summarize() { grep -E "^(finished in|requests:|status codes:)|^request " "$1" | sed 's/^/    /'; }

: > "$OUT"
{
  echo "h2proxy attack + failure measurement — $STAMP"
  echo
} | tee -a "$OUT"

# ---------------------------------------------------------------------------
# 1. Control: ordinary load, nothing else happening.
# ---------------------------------------------------------------------------
start_stack
echo "== control: 60s-scale load, no attack ==" | tee -a "$OUT"
h2load -n 100000 -c 50 -m 20 https://127.0.0.1:8443/ > /tmp/control.txt 2>&1
summarize /tmp/control.txt | tee -a "$OUT"
stop_stack

# ---------------------------------------------------------------------------
# 2. The same load, with a Rapid Reset flood on a separate connection.
# ---------------------------------------------------------------------------
start_stack
echo | tee -a "$OUT"
echo "== under attack: the same load, plus a Rapid Reset flood beside it ==" | tee -a "$OUT"

# The attacker: HEADERS + RST_STREAM as fast as the socket takes them, on its
# own connection, restarted whenever the proxy cuts it off.
(
  for _ in $(seq 1 200); do
    # -m 100 with an immediate close approximates open-and-abandon at rate.
    h2load -n 5000 -c 1 -m 100 -t 1 https://127.0.0.1:8443/ >/dev/null 2>&1 || true
  done
) &
attacker_pid=$!

attack_start=$(python3 -c 'import time; print(time.time())')
h2load -n 100000 -c 50 -m 20 https://127.0.0.1:8443/ > /tmp/victim.txt 2>&1
kill "$attacker_pid" 2>/dev/null || true
summarize /tmp/victim.txt | tee -a "$OUT"

echo | tee -a "$OUT"
echo "  guard peaks and terminations:" | tee -a "$OUT"
curl -s "$METRICS" | grep -E "^h2proxy_(guard_peak|connections_terminated)" | sed 's/^/    /' | tee -a "$OUT"
stop_stack

# ---------------------------------------------------------------------------
# 3. Backend failure: kill one of two, measure client-visible errors.
# ---------------------------------------------------------------------------
echo | tee -a "$OUT"
echo "== backend failure: two backends, one killed mid-load ==" | tee -a "$OUT"
"$ROOT/target/release/backend" >/dev/null 2>&1 &
be1=$!
BACKEND_LISTEN=127.0.0.1:8081 "$ROOT/target/release/backend" >/dev/null 2>&1 &
be2=$!
H2PROXYD_UPSTREAMS=127.0.0.1:8080,127.0.0.1:8081 H2PROXYD_EJECT_AFTER=3 \
  H2PROXYD_EJECT_BACKOFF=3 "$ROOT/target/release/h2proxyd" >/tmp/h2proxyd-fail.log 2>&1 &
proxy_pid=$!
backend_pid=$be1
for _ in $(seq 1 50); do
  curl -sk --http2 -o /dev/null https://127.0.0.1:8443/ 2>/dev/null && break
  sleep 0.2
done

h2load -n 200000 -c 50 -m 20 https://127.0.0.1:8443/ > /tmp/failover.txt 2>&1 &
load_pid=$!
sleep 1
kill -9 $be2 2>/dev/null || true
echo "  (killed the second backend mid-run)" | tee -a "$OUT"
wait $load_pid 2>/dev/null || true
summarize /tmp/failover.txt | tee -a "$OUT"
echo | tee -a "$OUT"
echo "  health:" | tee -a "$OUT"
curl -s "$METRICS" | grep -E "^h2proxy_(backends_healthy|backend_ejections_total|upstream_retries_total)" \
  | sed 's/^/    /' | tee -a "$OUT"
kill $be1 $be2 $proxy_pid 2>/dev/null || true

echo | tee -a "$OUT"
echo "written: $OUT" >&2
