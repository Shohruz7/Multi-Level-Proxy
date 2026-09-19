#!/usr/bin/env bash
# How many upstream connections should the pool be allowed to open?
#
# `H2PROXYD_MAX_UPSTREAM_CONNS` sets a ceiling on upstream parallelism, and two
# numbers this project wants to claim are both bounded by it:
#
#   streams held    = pool_conns x (max_concurrent + max_pending)
#   upstream slots  = pool_conns x max_concurrent      -> the throughput bound
#
# So it looks like a single knob that buys both. This script is what stops that
# from being asserted rather than measured, and it is shaped by two mistakes
# already made here.
#
# **Ordering.** The first sweep of this question ran 8, 16, 32, 64 once each, in
# that order, back to back on one box, and read 16 as a clear winner. The same
# confound has produced two false results in this repo already: the pool-growth
# sweep where `queue` won 5/5 purely because it ran first, and an A/B that read
# 29,975 req/s for a shape a fresher box read 84,083 for. Arms here alternate by
# repeat parity and every arm is reported as a **median** of REPEATS runs.
#
# **Sampling.** That sweep also read concurrency by polling
# `h2proxy_client_streams_active`, which the daemon publishes once per second.
# Polling a 1 Hz gauge at 100 ms does not make it a peak: it read 7,310 where
# the engine's exact count was 19,403. This script reads
# `h2proxy_client_streams_peak`, counted at every stream open, and records the
# upstream peak beside it as an independently accounted second opinion.
#
# `pool` is sampled **during** the run, not scraped at the end, because an idle
# connection is recycled (`UpstreamRecord::usable`) and a pool that grew to its
# ceiling and then shrank is indistinguishable at teardown from one that never
# grew.
#
# The bar is **zero client-visible failures**. An arm that holds more streams by
# failing requests has not won; `failed` and `5xx` are reported first for that
# reason.

# **Each run gets a fresh daemon, and that makes these numbers the optimistic
# end of the range.** Admission halves its budget on distress and climbs back at
# about six percent per 200 ms tick, so a proxy that has already been driven
# into overload admits less than one that has not — by a lot. The same shape
# measured through `bench/curve.sh`, where one daemon serves every step in
# sequence, holds 8,827 streams at 500 x 40 where a fresh daemon holds 14,115.
# Neither is wrong: they measure a proxy that has been hurt and a proxy that has
# not. What must not happen is quoting one and describing the other, so the
# restart is deliberate and stated rather than incidental.
#
# Usage:
#   bench/ceiling.sh                          # arms 8 and 16, 3 repeats, 4 shapes
#   ARMS="8 16 32" REPEATS=5 bench/ceiling.sh
#   SHAPES="500x20 500x40" bench/ceiling.sh
#   CONNS=500 STREAMS=30 bench/ceiling.sh     # drill into one shape
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/ceiling-$STAMP.csv"

ARMS="${ARMS:-8 16}"
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-15}"
WARMUP="${WARMUP:-3}"
# Shapes, not one shape. Every mistake this file guards against was a change
# blessed at 500x40 alone: admission control, and then the proposal to raise the
# connection ceiling. An arm that wins where the proxy is drowning and loses
# where it is comfortable is a regression, and one shape cannot tell the two
# apart.
SHAPES="${SHAPES:-500x12 500x20 500x30 500x40}"
CONNS="${CONNS:-}"
STREAMS="${STREAMS:-}"
# CONNS/STREAMS override SHAPES, for drilling into one shape.
if [ -n "$CONNS" ] || [ -n "$STREAMS" ]; then
  SHAPES="${CONNS:-500}x${STREAMS:-40}"
fi
METRICS="${METRICS:-127.0.0.1:9090}"
BACKEND="${BACKEND:-127.0.0.1:8080}"
TARGET="${TARGET:-https://127.0.0.1:8443/}"

mkdir -p "$RESULTS"
(cd "$ROOT" && cargo build --release -p h2proxyd -p backend -p loadgen)

# Bracketed so the pattern cannot match this script's own command line. Killing
# my own shell this way has already cost one session.
stop_all() {
  pkill -f '[t]arget/release/h2proxyd' 2>/dev/null || true
  pkill -f '[t]arget/release/backend' 2>/dev/null || true
}
trap 'stop_all' EXIT

metric() { awk -v k="$1" '$1==k{print $2; exit}' <<<"$2"; }
scrape() { curl -s --max-time 2 "http://$METRICS/metrics" || true; }

# One run of one arm. Echoes a CSV row.
run_arm() {
  local conns_max="$1" repeat="$2"
  stop_all
  sleep 2

  "$ROOT/target/release/backend" >/dev/null 2>&1 &
  local backend_pid=$!
  H2PROXYD_UPSTREAMS="$BACKEND" H2PROXYD_MAX_UPSTREAM_CONNS="$conns_max" \
    H2PROXYD_METRICS="$METRICS" \
    "$ROOT/target/release/h2proxyd" >/dev/null 2>&1 &
  local proxy_pid=$!

  local ready=
  for _ in $(seq 1 60); do
    curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null && { ready=1; break; }
    sleep 0.2
  done
  [ -n "$ready" ] || { echo "proxy never came up for arm $conns_max" >&2; return 1; }

  # Pool depth has no high-water gauge of its own, so it is sampled here. A
  # scrape after the run would see a pool that had already been recycled down.
  local poolfile="$RESULTS/.pool.$$"
  echo 0 > "$poolfile"
  (
    while :; do
      p=$(curl -s --max-time 1 "http://$METRICS/metrics" \
          | awk '$1=="h2proxy_upstream_pool_connections"{print $2; exit}')
      if [ -n "${p:-}" ]; then
        b=$(cat "$poolfile" 2>/dev/null || echo 0)
        awk -v a="$p" -v b="$b" 'BEGIN{exit !(a>b)}' && echo "$p" > "$poolfile"
      fi
      sleep 0.2
    done
  ) &
  local sampler_pid=$!

  local out
  out=$("$ROOT/target/release/loadgen" --url "$TARGET" --closed-loop \
        --connections "$shape_conns" --streams "$shape_streams" \
        --duration "$DURATION" --warmup "$WARMUP" --label "conns$conns_max" \
        2>/dev/null | tail -1)

  [ -n "${sampler_pid:-}" ] && kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  local pool_max
  pool_max=$(cat "$poolfile" 2>/dev/null || echo NA)
  rm -f "$poolfile"

  # The peak counters are exact but published on the daemon's one-second tick,
  # so a scrape taken the instant the load stops can miss the last tick's worth
  # of this run. One tick of patience is cheaper than an understated number.
  sleep 1.2
  local m; m="$(scrape)"
  local client_peak upstream_peak shed fivexx
  client_peak=$(metric h2proxy_client_streams_peak "$m")
  upstream_peak=$(metric h2proxy_upstream_streams_peak "$m")
  shed=$(metric h2proxy_upstream_shed_total "$m")
  fivexx=$(awk '/^h2proxy_responses_total\{class="5xx"\}/{print $2; exit}' <<<"$m")

  local completed failed achieved p99
  IFS=, read -r _ _ _ _ _ completed failed achieved _ _ p99 _ <<<"$out"

  # Killed by PID with a guard: an unguarded `kill ${x:-0}` signals the whole
  # process group and has already killed the harness running it.
  [ -n "${proxy_pid:-}" ] && kill "$proxy_pid" 2>/dev/null || true
  [ -n "${backend_pid:-}" ] && kill "$backend_pid" 2>/dev/null || true

  echo "$conns_max,$repeat,$shape_conns,$shape_streams,${achieved:-NA},${client_peak:-NA},${upstream_peak:-NA},${p99:-NA},${pool_max:-NA},${completed:-NA},${failed:-NA},${shed:-0},${fivexx:-0}"
}

echo "max_conns,repeat,client_conns,client_streams,achieved_rps,client_streams_peak,upstream_streams_peak,p99_ms,pool_max,completed,failed,shed,responses_5xx" > "$CSV"

echo "shapes: $SHAPES, $REPEATS repeats, arms: $ARMS" >&2
for shape in $SHAPES; do
  shape_conns="${shape%x*}"
  shape_streams="${shape#*x}"
  echo "shape $shape (${shape_conns} conns x ${shape_streams} streams)" >&2
  for repeat in $(seq 1 "$REPEATS"); do
    # Alternate the order every other repeat, so a monotonic drift in the box
    # cannot be read as an effect of the arm.
    order="$ARMS"
    if [ $((repeat % 2)) -eq 0 ]; then
      order="$(echo "$ARMS" | awk '{for (i = NF; i > 0; i--) printf "%s ", $i}')"
    fi
    for arm in $order; do
      echo "  repeat $repeat, arm $arm" >&2
      run_arm "$arm" "$repeat" >> "$CSV"
    done
  done
done

# Promoted beside the script, because `bench/results/` is local by design and a
# quoted number with no committed artifact behind it is the thing this file
# exists to prevent.
cp "$CSV" "$HERE/ceiling.csv"

echo >&2
echo "== $CSV (promoted to bench/ceiling.csv) ==" >&2
column -s, -t "$CSV" >&2
echo >&2

# Medians per shape and arm. The bar is client-visible failure: an arm that
# holds more streams by refusing requests has not won, so `failed` and `5xx`
# are printed beside every median rather than summarised away.
awk -F, '
  NR == 1 { next }
  {
    shape = $3 "x" $4
    key = shape SUBSEP $1
    if (!(shape in seen_shape)) { shape_order[++ns] = shape; seen_shape[shape] = 1 }
    if (!(key in seen_key)) { key_order[++nk] = key; seen_key[key] = 1 }
    n[key]++
    rps[key, n[key]] = $5
    cpk[key, n[key]] = $6
    upk[key, n[key]] = $7
    fail[key] += $11
    shed[key] += $12
    five[key] += $13
  }
  END {
    printf "%12s %10s %5s %12s %14s %16s %8s %6s %8s\n", \
      "shape", "max_conns", "runs", "median_rps", "median_streams", \
      "median_upstream", "failed", "5xx", "shed"
    for (si = 1; si <= ns; si++) {
      shape = shape_order[si]
      for (ki = 1; ki <= nk; ki++) {
        split(key_order[ki], parts, SUBSEP)
        if (parts[1] != shape) continue
        key = key_order[ki]
        c = n[key]
        # c is 3-5, so an insertion sort per series is more than fast enough.
        for (i = 1; i <= c; i++) { r[i] = rps[key, i]; p[i] = cpk[key, i]; q[i] = upk[key, i] }
        for (i = 2; i <= c; i++) {
          x = r[i]; j = i - 1; while (j > 0 && r[j] > x) { r[j+1] = r[j]; j-- } r[j+1] = x
          x = p[i]; j = i - 1; while (j > 0 && p[j] > x) { p[j+1] = p[j]; j-- } p[j+1] = x
          x = q[i]; j = i - 1; while (j > 0 && q[j] > x) { q[j+1] = q[j]; j-- } q[j+1] = x
        }
        mid = int((c + 1) / 2)
        printf "%12s %10s %5d %12.0f %14.0f %16.0f %8d %6d %8d\n", \
          shape, parts[2], c, r[mid], p[mid], q[mid], fail[key], five[key], shed[key]
      }
    }
    print ""
    print "The bar is zero client-visible failures. An arm that wins on streams"
    print "at one shape and loses at the others has not won: that is the error"
    print "this harness exists to catch."
  }
' "$CSV" >&2
