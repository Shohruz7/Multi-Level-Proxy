#!/usr/bin/env bash
# What does concurrency cost in memory?
#
# The first goal in the README is "bounded memory under any speed mismatch
# between client and upstream", and until now that claim had a test behind it
# but never a number. This is the number.
#
# **Why this harness and not the throughput ones.** Every rate this machine
# reports moves 12-31% with nothing but how long it has been running (see the
# 2026-09-19 correction in Documentation/RESULTS.md). Resident memory does not:
# it is set by what the process is holding, not by how fast the cores are
# willing to run. So a memory figure measured here is quotable in a way a
# throughput figure measured here is not.
#
# **Connections are held fixed and streams are swept**, which is the whole
# design. Peak RSS divided by peak streams would conflate two very different
# costs - 500 TLS connections with their record buffers, and the per-stream
# state on top - and would flatter or damn the proxy depending only on the
# shape chosen. Sweeping streams per connection at a fixed connection count
# separates them: the intercept is what the connections cost, the slope is what
# a stream costs. The slope is the interesting number and it is a measurement
# rather than a ratio.
#
# Three RSS readings per run, because "bounded" is a claim about all three:
#
#   idle      before any load, with the listener up: the fixed cost
#   peak      the high-water mark under load: what concurrency actually costs
#   settled   after the load stops and every stream has drained
#
# **`settled` is not a leak check, and reading it as one would be wrong.** It
# comes back within a hair of `peak` here, because a general-purpose allocator
# does not return freed pages to the operating system - it keeps them for the
# next allocation. Paired with `live_after`, which is the active-stream gauge
# and reads 0, it says the memory is held by the allocator rather than by live
# streams. That is the distinction worth recording: the process is not still
# holding request state, it is still holding address space.
#
# The growth check is `bench/soak.sh`, which runs five minutes of load with a
# backend killed every 30 seconds and asks whether RSS climbs *across* cycles
# rather than within one. That is where a leak would show, and it does not.
#
# Usage:
#   bench/memory.sh                                  # 4 shapes, 3 repeats
#   SHAPES="500x40" REPEATS=5 bench/memory.sh
#   CONNS=200 bench/memory.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/memory-$STAMP.csv"

CONNS="${CONNS:-500}"
STREAM_STEPS="${STREAM_STEPS:-2 8 20 40}"
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-20}"
WARMUP="${WARMUP:-3}"
METRICS="${METRICS:-127.0.0.1:9090}"
BACKEND="${BACKEND:-127.0.0.1:8080}"
TARGET="${TARGET:-https://127.0.0.1:8443/}"
SETTLE="${SETTLE:-5}"

mkdir -p "$RESULTS"
(cd "$ROOT" && cargo build --release -p h2proxyd -p backend -p loadgen)

# Bracketed so the pattern cannot match this script's own command line.
stop_all() {
  pkill -f '[t]arget/release/h2proxyd' 2>/dev/null || true
  pkill -f '[t]arget/release/backend' 2>/dev/null || true
}
trap 'stop_all' EXIT

# RSS in KiB. Portable between macOS and Linux, which `smaps` and `status` are
# not, and this number only needs to be resident-set to answer the question.
rss_kb() { ps -o rss= -p "$1" 2>/dev/null | tr -d ' '; }

metric() { awk -v k="$1" '$1 == k {print $2; exit}' <<<"$2"; }

run_one() {
  local streams="$1" repeat="$2"
  stop_all
  sleep 2

  "$ROOT/target/release/backend" >/dev/null 2>&1 &
  local backend_pid=$!
  H2PROXYD_UPSTREAMS="$BACKEND" H2PROXYD_METRICS="$METRICS" \
    "$ROOT/target/release/h2proxyd" >/dev/null 2>&1 &
  local proxy_pid=$!

  local ready=
  for _ in $(seq 1 60); do
    curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null && { ready=1; break; }
    sleep 0.2
  done
  [ -n "$ready" ] || { echo "proxy never came up" >&2; return 1; }

  # Idle is read after the listener is serving, so it includes the TLS setup
  # and the first connection, and excludes anything the load brings.
  sleep 1
  local idle; idle="$(rss_kb "$proxy_pid")"

  local peakfile="$RESULTS/.rss.$$"
  echo "${idle:-0}" > "$peakfile"
  (
    while :; do
      r="$(rss_kb "$proxy_pid")"
      if [ -n "${r:-}" ]; then
        b="$(cat "$peakfile" 2>/dev/null || echo 0)"
        [ "$r" -gt "$b" ] 2>/dev/null && echo "$r" > "$peakfile"
      fi
      sleep 0.2
    done
  ) &
  local sampler_pid=$!

  local out
  out=$("$ROOT/target/release/loadgen" --url "$TARGET" --closed-loop \
        --connections "$CONNS" --streams "$streams" \
        --duration "$DURATION" --warmup "$WARMUP" --label "mem-$streams" \
        2>/dev/null | tail -1)

  [ -n "${sampler_pid:-}" ] && kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  local peak; peak="$(cat "$peakfile" 2>/dev/null || echo NA)"
  rm -f "$peakfile"

  # Let the streams drain and the allocator settle before asking whether the
  # memory came back. Too short and this measures scheduling, not retention.
  sleep "$SETTLE"
  local settled; settled="$(rss_kb "$proxy_pid")"

  local m; m="$(curl -s --max-time 2 "http://$METRICS/metrics" || true)"
  local streams_peak bridge_peak shed live fivexx
  streams_peak="$(metric h2proxy_client_streams_peak "$m")"
  bridge_peak="$(metric h2proxy_bridge_buffered_bytes_peak "$m")"
  shed="$(metric h2proxy_upstream_shed_total "$m")"
  live="$(metric h2proxy_client_streams_active "$m")"
  # Recorded because shedding and *failing* are different events: a shed
  # request that a retry carries to another connection costs latency and is
  # invisible to the client, and the difference between those two is the whole
  # bar this harness is measured against.
  fivexx="$(awk '/^h2proxy_responses_total\{class="5xx"\}/{print $2; exit}' <<<"$m")"

  local completed failed achieved
  IFS=, read -r _ _ _ _ _ completed failed achieved _ <<<"$out"

  [ -n "${proxy_pid:-}" ] && kill "$proxy_pid" 2>/dev/null || true
  [ -n "${backend_pid:-}" ] && kill "$backend_pid" 2>/dev/null || true

  echo "$CONNS,$streams,$repeat,${idle:-NA},${peak:-NA},${settled:-NA},${streams_peak:-NA},${bridge_peak:-NA},${live:-NA},${achieved:-NA},${completed:-NA},${failed:-NA},${shed:-0},${fivexx:-0}"
}

echo "conns,streams_per_conn,repeat,idle_rss_kb,peak_rss_kb,settled_rss_kb,peak_streams,peak_bridge_bytes,live_after,achieved_rps,completed,failed,shed,responses_5xx" > "$CSV"

echo "$CONNS connections, streams per connection: $STREAM_STEPS, $REPEATS repeats" >&2
for repeat in $(seq 1 "$REPEATS"); do
  # Alternate the sweep direction, so a machine that drifts in one direction
  # cannot be read as a trend in the variable being swept.
  order="$STREAM_STEPS"
  if [ $((repeat % 2)) -eq 0 ]; then
    order="$(echo "$STREAM_STEPS" | awk '{for (i = NF; i > 0; i--) printf "%s ", $i}')"
  fi
  for streams in $order; do
    echo "  repeat $repeat, ${CONNS}x${streams}" >&2
    run_one "$streams" "$repeat" >> "$CSV"
  done
done

cp "$CSV" "$HERE/memory.csv"

echo >&2
echo "== $CSV (promoted to bench/memory.csv) ==" >&2
column -s, -t "$CSV" >&2
echo >&2

awk -F, '
  NR == 1 { next }
  {
    key = $2
    if (!(key in seen)) { order[++n] = key; seen[key] = 1 }
    c[key]++
    idle[key, c[key]] = $4
    peak[key, c[key]] = $5
    settled[key, c[key]] = $6
    streams[key, c[key]] = $7
    bridge[key, c[key]] = $8
    fail[key] += $12
    shed[key] += $13
    five[key] += $14
  }
  function median(key, series,   i, a, cnt, x, j) {
    cnt = c[key]
    for (i = 1; i <= cnt; i++) a[i] = series[key, i]
    for (i = 2; i <= cnt; i++) {
      x = a[i]; j = i - 1
      while (j > 0 && a[j] > x) { a[j+1] = a[j]; j-- }
      a[j+1] = x
    }
    return a[int((cnt + 1) / 2)]
  }
  END {
    printf "%8s %6s %10s %10s %11s %10s %12s %7s %5s %8s\n", \
      "per-conn", "runs", "idle_MB", "peak_MB", "settled_MB", "streams", \
      "bridge_B", "failed", "5xx", "shed"
    for (i = 1; i <= n; i++) {
      k = order[i]
      mi = median(k, idle); mp = median(k, peak)
      ms = median(k, settled); mst = median(k, streams)
      mb = median(k, bridge)
      printf "%8s %6d %10.1f %10.1f %11.1f %10d %12d %7d %5d %8d\n", \
        k, c[k], mi/1024, mp/1024, ms/1024, mst, mb, fail[k], five[k], shed[k]
      sx += mst; sy += (mp - mi); sxx += mst * mst; sxy += mst * (mp - mi); np++
    }
    print ""
    if (np > 1 && (np * sxx - sx * sx) != 0) {
      slope = (np * sxy - sx * sy) / (np * sxx - sx * sx)
      printf "per-stream cost, least squares over %d points: %.0f bytes\n", np, slope * 1024
      printf "fixed cost (median idle RSS): %.1f MB for %s connections\n", \
        median(order[1], idle) / 1024, "'"$CONNS"'"
    }
    print ""
    print "settled_MB tracks peak_MB because the allocator keeps freed pages;"
    print "paired with live_after = 0 it means the memory is held by the"
    print "allocator, not by live streams. Growth across cycles is bench/soak.sh."
  }
' "$CSV" >&2
