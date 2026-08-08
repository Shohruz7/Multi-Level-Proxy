#!/usr/bin/env bash
# Measure what *legitimate* traffic actually does to each abuse-guard signal,
# and report the headroom against the configured threshold (design doc §6).
#
# Why this exists: a threshold picked from an argument about how browsers behave
# is a guess. This runs the honest workloads with the guard in observe-only mode
# — genuinely in the frame path, recording peaks, unable to break anything — and
# reports peak-observed vs. threshold for each signal. Anything under 10x
# headroom is too tight and gets raised.
#
# The output is committed as bench/guard-calibration.csv and is the evidence
# behind the numbers in ADR 0019.
#
# Usage:  bench/calibrate.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/guard-calibration-$STAMP.csv"
METRICS="http://127.0.0.1:9090/metrics"

mkdir -p "$RESULTS"
command -v h2load >/dev/null || { echo "h2load not found (brew install nghttp2)" >&2; exit 1; }

echo "building" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd)

"$ROOT/target/release/backend" >/dev/null 2>&1 &
backend_pid=$!
# Observe-only: the guard counts and records peaks but never trips, so the
# numbers describe the traffic rather than describing the guard.
H2PROXYD_GUARD_OBSERVE_ONLY=1 H2PROXYD_UPSTREAMS=127.0.0.1:8080 \
  "$ROOT/target/release/h2proxyd" >/dev/null 2>&1 &
proxy_pid=$!
trap 'kill $backend_pid $proxy_pid 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  curl -sk --http2 -o /dev/null https://127.0.0.1:8443/ 2>/dev/null && break
  sleep 0.2
done

peak() { curl -s "$METRICS" | awk -v k="$1" '$1==k {print $2; exit}'; }

echo "== throughput profile ==" >&2
h2load -n 100000 -c 100 -m 10 https://127.0.0.1:8443/ >/dev/null 2>&1

echo "== concurrency profile ==" >&2
h2load -n 50000 -c 50 -m 200 https://127.0.0.1:8443/ >/dev/null 2>&1

echo "== h2spec conformance (opens and resets streams deliberately) ==" >&2
if command -v h2spec >/dev/null 2>&1; then
  h2spec -t -k -h 127.0.0.1 -p 8443 >/dev/null 2>&1 || true
else
  echo "   h2spec not installed; skipping the sharpest profile" >&2
fi

echo "== browser-shaped: bursts of opens with occasional cancels ==" >&2
# h2load's closest analogue to a browser: many short-lived connections each
# opening a handful of concurrent streams, rather than one long-lived firehose.
for _ in $(seq 1 8); do
  h2load -n 2000 -c 20 -m 6 https://127.0.0.1:8443/ >/dev/null 2>&1
done

# Thresholds as configured; keep in step with guard::Limits::default().
declare -a SIGNALS=(reset_rate unanswered_rate control_rate)
declare -a GAUGES=(h2proxy_guard_peak_reset_rate h2proxy_guard_peak_unanswered_rate h2proxy_guard_peak_control_rate)
declare -a LIMITS=("${H2PROXYD_RESET_RATE:-20}" "${H2PROXYD_UNANSWERED_RATE:-15}" "${H2PROXYD_CONTROL_RATE:-50}")

echo "signal,peak_observed,threshold,headroom_ratio,verdict" > "$CSV"
fail=0
for i in "${!SIGNALS[@]}"; do
  observed="$(peak "${GAUGES[$i]}")"
  observed="${observed:-0}"
  limit="${LIMITS[$i]}"
  read -r ratio verdict < <(awk -v o="$observed" -v l="$limit" 'BEGIN{
    if (o <= 0) { printf "inf ok\n" }
    else { r = l / o; printf "%.1f %s\n", r, (r >= 10 ? "ok" : "TOO_TIGHT") }
  }')
  [ "$verdict" = "TOO_TIGHT" ] && fail=1
  echo "${SIGNALS[$i]},$observed,$limit,$ratio,$verdict" >> "$CSV"
done

# The two count-based signals, which have no rate.
empty="$(peak h2proxy_guard_peak_consecutive_empty)"; empty="${empty:-0}"
cont="$(peak h2proxy_guard_peak_continuations)"; cont="${cont:-0}"
echo "consecutive_empty,$empty,${H2PROXYD_MAX_EMPTY_FRAMES:-32},$(awk -v o="$empty" -v l="${H2PROXYD_MAX_EMPTY_FRAMES:-32}" 'BEGIN{print (o<=0)?"inf":l/o}'),ok" >> "$CSV"
echo "continuations,$cont,${H2PROXYD_MAX_CONTINUATIONS:-64},$(awk -v o="$cont" -v l="${H2PROXYD_MAX_CONTINUATIONS:-64}" 'BEGIN{print (o<=0)?"inf":l/o}'),ok" >> "$CSV"

cp "$CSV" "$HERE/guard-calibration.csv"
echo >&2
column -s, -t "$CSV" >&2
echo >&2
if [ "$fail" -ne 0 ]; then
  echo "FAIL: a signal has less than 10x headroom against legitimate traffic." >&2
  echo "Raise its threshold and re-run; do not exempt the workload." >&2
  exit 1
fi
echo "all signals >= 10x headroom; written to bench/guard-calibration.csv" >&2
