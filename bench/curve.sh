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
#   MODE=echo bench/curve.sh             # no upstream hop; promotes curve-echo.*
#   BODY_SIZE=16384 bench/curve.sh       # sizes both ends together
#   LABEL=experiment bench/curve.sh      # writes to bench/results/, promotes nothing
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
# proxy: client -> proxy -> backend, the whole path.
# echo:  client -> proxy, answered by the daemon's built-in responder with no
#        upstream hop at all. The difference between the two is the cost of the
#        upstream leg, and it is the only way to attribute a latency to one side
#        or the other. Until this existed, the "engine and client leg alone do
#        40k at p99 0.216 ms" claim came from a hand-run daemon and no committed
#        script could reproduce it.
MODE="${MODE:-proxy}"
LABEL="${LABEL:-$([ "$MODE" = echo ] && echo curve-echo || echo curve)}"
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
# Both ends are sized from one variable on purpose. `backend` and `h2proxyd`
# default to 1024 independently, and an echo-vs-proxy comparison only means
# anything if both legs return the same number of octets.
BODY_SIZE="${BODY_SIZE:-1024}"

mkdir -p "$RESULTS"

echo "building release binaries (a debug measurement is not a measurement)" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd -p loadgen)

if [ "$MODE" = "proxy" ]; then
  BACKEND_BODY_SIZE="$BODY_SIZE" "$ROOT/target/release/backend" &
  backend_pid=$!
  upstreams="$BACKEND_ADDR"
else
  backend_pid=""
  # No upstreams configured: h2proxyd answers from its built-in responder.
  upstreams=""
fi
H2PROXYD_UPSTREAMS="$upstreams" H2PROXYD_LISTEN="$PROXY_ADDR" \
  H2PROXYD_METRICS="$METRICS" H2PROXYD_BODY_SIZE="$BODY_SIZE" \
  "$ROOT/target/release/h2proxyd" >"$RESULTS/curve-$LABEL-$STAMP-proxy.log" 2>&1 &
proxy_pid=$!
trap 'kill ${backend_pid:-} $proxy_pid 2>/dev/null || true' EXIT

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

  # Sample the live concurrency *while the step runs*, and keep the maximum.
  #
  # The obvious gauge, `h2proxy_stream_concurrency_max`, is the wrong one: the
  # daemon sets it from a connection's summary when that connection *closes*, so
  # during a run — when every connection is still open, which is the entire point
  # of HTTP/2 — it reports whatever the last few closed connections happened to
  # reach. It read 1 against a proxy carrying thousands of streams. A gauge that
  # only moves at teardown cannot answer a question about steady state.
  #
  # `h2proxy_client_streams_active` is sampled by the daemon every second and is
  # the number the "10,000+ concurrent streams" claim is actually about. Watching
  # it costs one curl per 200 ms against a local port.
  local peakfile="$RESULTS/.peak.$$"
  echo 0 > "$peakfile"
  (
    while :; do
      live=$(curl -s --max-time 1 "http://$METRICS/metrics" \
        | awk '/^h2proxy_client_streams_active /{print $2; exit}')
      if [ -n "${live:-}" ]; then
        best=$(cat "$peakfile" 2>/dev/null || echo 0)
        awk -v a="$live" -v b="$best" 'BEGIN{exit !(a>b)}' && echo "$live" > "$peakfile"
      fi
      sleep 0.2
    done
  ) &
  local sampler_pid=$!

  local out
  out=$("$ROOT/target/release/loadgen" \
        --url "$TARGET" --rate "$rate" --connections "$conns" \
        --duration "$STEP_SECONDS" --warmup "$WARMUP" --label "$profile-$rate" \
        2>>"$RESULTS/curve-$LABEL-$STAMP-loadgen.log" | tail -1)

  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  local peak
  peak=$(cat "$peakfile" 2>/dev/null || echo NA)
  rm -f "$peakfile"

  # loadgen prints: label,mode,connections,rate_offered,duration,completed,
  # failed,achieved,p50,p90,p99,p999,max,closed_p99,lag_p99
  local completed failed achieved p50 p90 p99 p999 max closed lag
  IFS=, read -r _ _ _ _ _ completed failed achieved p50 p90 p99 p999 max closed lag <<<"$out"

  echo "$profile,$rate,${achieved:-NA},${completed:-NA},${failed:-NA},${p50:-NA},${p90:-NA},${p99:-NA},${p999:-NA},${max:-NA},${closed:-NA},${lag:-NA},${peak:-NA},$allocator" >> "$CSV"
}

if [ "$PROFILE" = "throughput" ] || [ "$PROFILE" = "both" ]; then
  # Small responses over a moderate connection count: the request-rate question.
  #
  # The profile name doubles as the series name. bench/plot-curve.py writes the
  # first known profile to the primary SVG and any other to curve-<profile>.svg,
  # so an echo run produces its own chart with no change to the plotter — and a
  # new *column* would have broken its fixed-column reader.
  profile_name=$([ "$MODE" = echo ] && echo echo || echo throughput)
  for rate in $RATES; do
    step "$profile_name" "$rate" "$CONNECTIONS"
  done
fi

concurrency_step() {
  local conns="$1" streams="$2"
  local want=$((conns * streams))
  echo "== concurrency: $conns connections x $streams streams = $want in flight ==" >&2

  local peakfile="$RESULTS/.peak.$$"
  echo 0 > "$peakfile"
  (
    while :; do
      live=$(curl -s --max-time 1 "http://$METRICS/metrics" \
        | awk '/^h2proxy_client_streams_active /{print $2; exit}')
      if [ -n "${live:-}" ]; then
        best=$(cat "$peakfile" 2>/dev/null || echo 0)
        awk -v a="$live" -v b="$best" 'BEGIN{exit !(a>b)}' && echo "$live" > "$peakfile"
      fi
      sleep 0.2
    done
  ) &
  local sampler_pid=$!

  local out
  out=$("$ROOT/target/release/loadgen" \
        --url "$TARGET" --closed-loop --connections "$conns" --streams "$streams" \
        --duration "$STEP_SECONDS" --warmup "$WARMUP" --label "concurrency-$want" \
        2>>"$RESULTS/curve-$LABEL-$STAMP-loadgen.log" | tail -1)

  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  local peak
  peak=$(cat "$peakfile" 2>/dev/null || echo NA)
  rm -f "$peakfile"

  local completed failed achieved p50 p90 p99 p999 max closed lag
  IFS=, read -r _ _ _ _ _ completed failed achieved p50 p90 p99 p999 max closed lag <<<"$out"

  # `offered` is the in-flight count here, not a rate: in a closed loop
  # concurrency is what you set and rate is what you get, which is the opposite
  # of the throughput profile above and the reason the two are separate.
  echo "concurrency,$want,${achieved:-NA},${completed:-NA},${failed:-NA},${p50:-NA},${p90:-NA},${p99:-NA},${p999:-NA},${max:-NA},${closed:-NA},${lag:-NA},${peak:-NA},$allocator" >> "$CSV"
  echo "  -> ${achieved:-NA} req/s, p99 ${p99:-NA} ms, proxy saw ${peak:-NA} streams live" >&2
}

# Skipped in echo mode: the concurrency profile asks how the pool and the
# coalescing behave with five figures of streams open, and in echo mode there is
# no pool.
if [ "$MODE" = "proxy" ] && { [ "$PROFILE" = "concurrency" ] || [ "$PROFILE" = "both" ]; }; then
  # The concurrency question cannot be asked with the open loop above.
  #
  # In an open loop, live streams = rate x latency (Little's law), so at 25,000
  # req/s and a 0.3 ms response the proxy carries about *eight* streams at once.
  # Offering more load does not raise concurrency; it raises rate. To hold 10,000
  # streams open simultaneously the concurrency has to be *set*, which is exactly
  # what a closed loop does: N workers each keep one request outstanding.
  #
  # So this profile is deliberately closed-loop, and its numbers are labelled
  # that way. Rate is secondary here — the question is whether the stream table,
  # the pool and the bookkeeping hold up with five figures of streams open, and
  # the answer is the `streams_peak` column read off the proxy's own gauge.
  for streams in ${CONCURRENCY_STREAMS:-2 8 20 40}; do
    concurrency_step "${CONCURRENCY_CONNECTIONS:-500}" "$streams"
  done
fi

if [ "$LABEL" = "curve" ] || [ "$LABEL" = "curve-echo" ]; then
  cp "$CSV" "$HERE/$LABEL.csv"
  "$ROOT/bench/plot-curve.py" "$HERE/$LABEL.csv" "$HERE/$LABEL.svg"
  echo >&2
  echo "promoted to bench/$LABEL.csv and bench/$LABEL.svg" >&2
fi

echo >&2
echo "written: $CSV" >&2
column -s, -t "$CSV" >&2

# The knee, stated rather than left to the reader.
#
# Defined by the **latency target** (3 ms p99, the project's own goal), not by
# throughput falling off. Throughput does not fall off here: past saturation the
# proxy keeps accepting requests and they queue, so delivered still equals
# offered while p99 rises by two orders of magnitude. A knee read from the
# throughput column alone would report the largest rate ever offered, on a run
# whose top half is unusable.
#
# Throughput rows only — in the concurrency rows column 2 is an in-flight count
# rather than a rate.
awk -F, -v target=3.0 '
  $1 == "throughput" && $8 != "NA" && $8 <= target { knee=$2; p99=$8 }
  $1 == "throughput" && $8 != "NA" && $8 > target && !past { past=$2; pastp99=$8 }
  END {
    if (knee) printf "\nknee: %s req/s at corrected p99 %s ms (target %.0f ms)\n", knee, p99, target
    if (past) printf "      next step up, %s req/s, cost p99 %s ms\n", past, pastp99
  }' "$CSV" >&2

awk -F, '$1 == "concurrency" && $13 != "NA" && $13 > peak { peak=$13; rps=$3; p99=$8 }
         END { if (peak) printf "peak concurrency: %s streams live at the proxy, %s req/s, p99 %s ms\n", peak, rps, p99 }' \
  "$CSV" >&2
