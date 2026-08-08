#!/usr/bin/env bash
# Sustained load with a backend dying and coming back, for long enough that a
# slow leak becomes visible (plan §9.7).
#
# Every other harness here answers a question about a moment: how fast, how many
# 5xx, how long to eject. This one answers a question about *time*. The bugs it
# is looking for do not fail a request — they accumulate:
#
#   - a lease dropped on a path no test walks, so `outstanding` drifts up and the
#     load balancer's view of every backend slowly rots (week 6's worst bug),
#   - flow-control credit withheld and never reclaimed, so the bridge's occupancy
#     climbs a few KB per cancelled download and the process eventually stalls,
#   - a pooled connection retired but never dropped, so the count creeps,
#   - anything that grows RSS at a constant rate per request.
#
# None of those is visible in a 20-second run. All of them are obvious in five
# minutes of the same traffic with the numbers sampled throughout, which is the
# entire design of this script: it is not a throughput measurement, it is a set
# of quantities that must be *flat*.
#
# The backend is killed and restarted every 30 s, so the failure paths — Gone,
# retry, ejection, probe-back, reconnect — are on the hot path for the whole run
# rather than exercised once at the end. Those are the paths where the leaks are.
#
# Usage:
#   bench/soak.sh              # 300 s
#   SECONDS_TOTAL=900 bench/soak.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/soak-$STAMP.csv"
LOG="$RESULTS/soak-$STAMP.log"

SECONDS_TOTAL="${SECONDS_TOTAL:-300}"
KILL_EVERY="${KILL_EVERY:-30}"
SAMPLE_EVERY="${SAMPLE_EVERY:-5}"
CLIENTS="${CLIENTS:-50}"
STREAMS="${STREAMS:-20}"
METRICS="${METRICS:-http://127.0.0.1:9090/metrics}"
PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:8443}"

mkdir -p "$RESULTS"
command -v h2load >/dev/null || { echo "h2load not found (brew install nghttp2)" >&2; exit 1; }

echo "building (release: a debug soak measures the wrong program)" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd)

# Two backends on fixed ports. One is killed and restarted throughout; the other
# stays up, so the run measures *rerouting* rather than a total outage — traffic
# has somewhere to go, which is the condition under which a leak keeps leaking.
start_backend_a() { BACKEND_BODY_SIZE=4096 "$ROOT/target/release/backend" >>"$LOG" 2>&1 & echo $!; }
start_backend_b() { BACKEND_LISTEN=127.0.0.1:8081 BACKEND_BODY_SIZE=4096 \
  "$ROOT/target/release/backend" >>"$LOG" 2>&1 & echo $!; }

a_pid="$(start_backend_a)"
b_pid="$(start_backend_b)"
H2PROXYD_UPSTREAMS=127.0.0.1:8080,127.0.0.1:8081 \
  "$ROOT/target/release/h2proxyd" >>"$LOG" 2>&1 &
proxy_pid=$!
cleanup() {
  kill "$a_pid" "$b_pid" "$proxy_pid" 2>/dev/null || true
  kill "${load_pid:-0}" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  curl -sk --http2 -o /dev/null "https://$PROXY_ADDR/" 2>/dev/null && break
  sleep 0.2
done

metric() { curl -s "$METRICS" | awk -v k="$1" '$1==k {print $2; exit}'; }
labelled() { curl -s "$METRICS" | awk -v p="$1" '$1 ~ p {s+=$2} END {print s+0}'; }
rss_kb() { ps -o rss= -p "$proxy_pid" 2>/dev/null | tr -d ' '; }

echo "soaking for ${SECONDS_TOTAL}s: c=$CLIENTS m=$STREAMS, killing a backend every ${KILL_EVERY}s" >&2

# One long h2load run rather than repeated short ones: a reconnect at every
# sample point would hide exactly the connection-lifetime leaks being looked for.
h2load -D "$SECONDS_TOTAL" -c "$CLIENTS" -m "$STREAMS" "https://$PROXY_ADDR/" \
  >"$RESULTS/soak-$STAMP-h2load.txt" 2>&1 &
load_pid=$!

echo "t_s,rss_kb,pool_conns,bridge_bytes,bridge_peak,client_streams,upstream_streams,requests,retries,ejections,probes,probe_failures,5xx" > "$CSV"

elapsed=0
while [ "$elapsed" -lt "$SECONDS_TOTAL" ]; do
  sleep "$SAMPLE_EVERY"
  elapsed=$((elapsed + SAMPLE_EVERY))

  echo "$elapsed,$(rss_kb),$(metric h2proxy_upstream_pool_connections),\
$(metric h2proxy_bridge_buffered_bytes),$(metric h2proxy_bridge_buffered_bytes_peak),\
$(metric h2proxy_client_streams_active),$(metric h2proxy_upstream_streams_active),\
$(metric h2proxy_upstream_requests_total),$(metric h2proxy_upstream_retries_total),\
$(metric h2proxy_backend_ejections_total),$(metric h2proxy_upstream_probes_total),\
$(metric h2proxy_upstream_probe_failures_total),\
$(labelled 'h2proxy_responses_total.*class="5xx"')" | tr -d ' ' >> "$CSV"

  if [ $((elapsed % KILL_EVERY)) -eq 0 ]; then
    if kill -0 "$a_pid" 2>/dev/null; then
      echo "  t=${elapsed}s: kill -9 backend A" >&2
      kill -9 "$a_pid" 2>/dev/null || true
      wait "$a_pid" 2>/dev/null || true
    else
      echo "  t=${elapsed}s: restarting backend A" >&2
      a_pid="$(start_backend_a)"
    fi
  fi
done

wait "$load_pid" 2>/dev/null || true

# ---------------------------------------------------------------------------
# The verdict. A soak that only produces a CSV is a soak nobody reads, so the
# flatness claims are checked here rather than left to the eye.
# ---------------------------------------------------------------------------
echo
echo "== soak result — $CSV =="
column -s, -t < "$CSV" | tail -n +1

fail=0
check_flat() {
  local col="$1" name="$2" tolerance="$3"
  # Compare the mean of the first third against the last third. A leak is a
  # trend; comparing single samples would be at the mercy of whichever moment a
  # backend happened to be down.
  awk -F, -v col="$col" -v name="$name" -v tol="$tolerance" '
    NR>1 { v[n++]=$col }
    END {
      if (n < 8) { print "  " name ": too few samples to judge"; exit 0 }
      # Skip the first quarter. A process that has just started is still growing
      # its allocator arenas, its connection pool, and its h2load client count,
      # and none of that is a leak — including it would make a 40 s run fail and
      # a 40 minute run pass, which is exactly backwards.
      warm = int(n/4)
      m = n - warm
      third = int(m/3); if (third < 1) third = 1
      for (i=warm;i<warm+third;i++) early += v[i]
      for (i=n-third;i<n;i++) late += v[i]
      early/=third; late/=third
      grew = (early > 0) ? (late-early)/early : (late > 0 ? 1 : 0)
      status = (grew > tol) ? "GREW" : "flat"
      printf "  %-22s %10.1f -> %10.1f  (%+.1f%%)  %s\n", name, early, late, grew*100, status
      if (grew > tol) exit 1
    }' "$CSV" || fail=1
}

# RSS is judged differently from the gauges, and the difference is the point.
#
# A gauge that must be flat is flat from the first sample. RSS is not: an
# allocator grows arenas, and a burst — every connection to a killed backend
# failing at once — leaves behind memory the system allocator has no reason to
# return. That shows up as a one-time *step*, and a step is not a leak.
#
# A leak is a slope that does not stop. So RSS is judged on whether it has
# stopped: the last fifth of the run against the fifth before it. A constant-rate
# leak still shows its full per-window growth there; a step that happened earlier
# does not. The whole trace is printed above precisely so that a *repeated* step,
# which is a leak with a lumpy shape, is visible to the eye that a single ratio
# would miss.
check_plateau() {
  local col="$1" name="$2" tolerance="$3"
  awk -F, -v col="$col" -v name="$name" -v tol="$tolerance" '
    NR>1 { v[n++]=$col }
    END {
      if (n < 10) { print "  " name ": too few samples to judge"; exit 0 }
      fifth = int(n/5)
      for (i=n-2*fifth;i<n-fifth;i++) early += v[i]
      for (i=n-fifth;i<n;i++) late += v[i]
      early/=fifth; late/=fifth
      grew = (early > 0) ? (late-early)/early : (late > 0 ? 1 : 0)
      status = (grew > tol) ? "STILL GROWING" : "plateaued"
      printf "  %-22s %10.1f -> %10.1f  (%+.1f%%)  %s\n", name, early, late, grew*100, status
      if (grew > tol) exit 1
    }' "$CSV" || fail=1
}

echo
echo "rss (tail plateau — last fifth vs the fifth before it):"
check_plateau 2 "rss_kb" 0.05

echo
echo "flatness (first third vs last third):"
check_flat 3 "pool_connections" 0.50
check_flat 4 "bridge_buffered" 1.00
check_flat 6 "client_streams" 0.50
check_flat 7 "upstream_streams" 0.50

# The end state is the sharper claim: with the load stopped, everything
# in-flight must be exactly zero. A quantity that is merely small is a leak that
# has not run long enough.
sleep 3
for series in h2proxy_client_streams_active h2proxy_upstream_streams_active h2proxy_bridge_buffered_bytes; do
  value="$(metric "$series")"
  if [ "${value%%.*}" != "0" ]; then
    echo "  LEAK: $series is $value with no load running" >&2
    fail=1
  else
    echo "  settled: $series = 0"
  fi
done

echo
if [ "$fail" -ne 0 ]; then
  echo "SOAK FAILED — something grew that should not have. Full log: $LOG" >&2
  exit 1
fi
echo "soak clean: nothing grew, everything settled to zero."
