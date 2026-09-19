#!/usr/bin/env bash
# The regressions this project has actually shipped, asserted.
#
# Every check below is a defect that reached main, passed every test, and was
# found later by someone thinking to look. They are cheap, they are structural,
# and none of them is a timing assertion — this has to survive a shared CI runner
# where throughput is noise.
#
# That constraint is the design. The worst regression in this project's history
# was a 5.4x throughput loss, and asserting throughput is exactly what a CI runner
# cannot do. But it *also* collapsed the connection pool from five connections to
# one, and "did the pool grow past one" is a yes/no fact that holds on any
# hardware at any speed. Structural invariants are what make a performance defect
# catchable without a performance measurement.
#
# Usage:  bench/regressions.sh          # exits non-zero on any violation
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
METRICS="${METRICS:-127.0.0.1:9090}"
PROXY="${PROXY:-127.0.0.1:8443}"
CONNS="${CONNS:-50}"
STREAMS="${STREAMS:-20}"
# Duration, not a request count. At the rates this proxy reaches, 100,000
# requests are gone in under a second — less than the pool gauge is sampled in,
# so the pool check read zero and failed on a perfectly healthy build. A guard
# whose own timing decides the verdict is not a guard.
DURATION="${DURATION:-10}"
# Below this the run did not happen and the checks would pass vacuously, which is
# worse than no check at all.
#
# Deliberately far below any plausible runner rather than just below this one.
# It is an anti-vacuity check, not a performance check: 10,000 over ten seconds
# is 1,000 req/s, which a shared two-core runner doing TLS, proxy, backend and
# load generator on the same box will clear comfortably, while still catching a
# run that never started. Tightening it toward what the dev machine achieves
# would turn a correctness guard into a flaky throughput assertion, which is
# precisely what this file exists to avoid.
MIN_REQUESTS="${MIN_REQUESTS:-10000}"
MIN_POOL="${MIN_POOL:-2}"
# 512 MiB against an observed ~35 MiB at this shape. A leak check with two
# orders of magnitude of headroom, not a memory budget.
MAX_RSS_KB="${MAX_RSS_KB:-524288}"

command -v h2load >/dev/null || { echo "h2load not found (brew install nghttp2 / apt install nghttp2-client)" >&2; exit 1; }

echo "building" >&2
(cd "$ROOT" && cargo build --release -p h2proxyd -p backend >/dev/null)

backend_pid=""; proxy_pid=""
# Never `kill "${pid:-0}"`: an unset pid becomes 0, and `kill 0` signals the whole
# process group including this script.
cleanup() {
  [ -n "${proxy_pid:-}" ] && kill "$proxy_pid" 2>/dev/null || true
  [ -n "${backend_pid:-}" ] && kill "$backend_pid" 2>/dev/null || true
}
trap cleanup EXIT

"$ROOT/target/release/backend" >/dev/null 2>&1 &
backend_pid=$!
H2PROXYD_UPSTREAMS=127.0.0.1:8080 H2PROXYD_LISTEN="$PROXY" H2PROXYD_METRICS="$METRICS" \
  "$ROOT/target/release/h2proxyd" >/dev/null 2>&1 &
proxy_pid=$!
for _ in $(seq 1 100); do
  curl -sk --http2 -o /dev/null "https://$PROXY/" 2>/dev/null && break
  sleep 0.2
done

# The pool gauge has to be sampled *while* the load runs. Read afterwards it
# reports the pool after connections have begun retiring, which is not the
# question being asked.
peakfile="$(mktemp)"; echo 0 > "$peakfile"
(
  while :; do
    v=$(curl -s --max-time 1 "http://$METRICS/metrics" \
        | awk '/^h2proxy_upstream_pool_connections /{print $2; exit}')
    if [ -n "${v:-}" ]; then
      b=$(cat "$peakfile" 2>/dev/null || echo 0)
      awk -v a="$v" -v b="$b" 'BEGIN{exit !(a>b)}' && echo "$v" > "$peakfile"
    fi
    sleep 0.1
  done
) &
sampler=$!

out="$(mktemp)"
h2load -D "$DURATION" -c "$CONNS" -m "$STREAMS" "https://$PROXY/" > "$out" 2>&1 || true
kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true

pool_peak=$(cat "$peakfile" 2>/dev/null || echo 0); rm -f "$peakfile"
metrics="$(curl -s --max-time 2 "http://$METRICS/metrics" || true)"
shed=$(awk '/^h2proxy_upstream_shed_total /{print $2; exit}' <<<"$metrics")
streams_peak=$(awk '/^h2proxy_client_streams_peak /{print $2; exit}' <<<"$metrics")
streams_live=$(awk '/^h2proxy_client_streams_active /{print $2; exit}' <<<"$metrics")
# The proxy's own resident set, not this script's. Portable between the macOS
# dev box and a Linux runner, which /proc and smaps are not.
rss_kb=$(ps -o rss= -p "$proxy_pid" 2>/dev/null | tr -d ' ')
srv5xx=$(awk -F'[ }]' '/^h2proxy_responses_total\{class="5xx"\}/{print $NF; exit}' <<<"$metrics")

read -r done_n succeeded failed errored < <(
  awk '/^ *requests:/ {
         for (i = 1; i <= NF; i++) {
           if ($(i+1) ~ /^done/)      d = $i
           if ($(i+1) ~ /^succeeded/) s = $i
           if ($(i+1) ~ /^failed/)    f = $i
           if ($(i+1) ~ /^errored/)   e = $i
         }
         print d, s, f, e; exit
       }' "$out")
codes_5xx=$(awk '/^ *status codes:/ { for (i=1;i<=NF;i++) if ($(i+1) ~ /^5xx/) print $i }' "$out")
rate=$(awk '/^finished in/ {print $4}' "$out")

echo
printf 'requests  %s done, %s succeeded, %s failed, %s errored\n' "$done_n" "$succeeded" "$failed" "$errored"
printf 'status    %s 5xx (client), %s 5xx (server counter)\n' "${codes_5xx:-0}" "${srv5xx:-0}"
printf 'pool      %s connections at peak\n' "$pool_peak"
printf 'shed      %s\n' "${shed:-0}"
printf 'streams   %s peak, %s live at scrape\n' "${streams_peak:-0}" "${streams_live:-0}"
printf 'rss       %s KiB\n' "${rss_kb:-0}"
printf 'rate      %s (reported, never asserted)\n' "${rate:-unknown}"
echo

fail=0
check() { # name, actual, test, expected, why
  if awk -v a="$2" -v b="$4" "BEGIN{exit !(a $3 b)}"; then
    printf '  ok    %s\n' "$1"
  else
    printf '  FAIL  %s: got %s, want %s %s\n        %s\n' "$1" "$2" "$3" "$4" "$5" >&2
    fail=1
  fi
}

# The run has to have happened. A guard that passes because nothing ran is worse
# than no guard, and this is the only check here that is about the harness rather
# than the proxy.
check "the load actually ran" "${done_n:-0}" ">=" "$MIN_REQUESTS" \
  "h2load completed too few requests for the rest of these checks to mean anything"

# 2026-09-16: admission control enforced a stream limit against clients that had
# opened streams before our first SETTINGS arrived — legal under RFC 9113, whose
# initial MAX_CONCURRENT_STREAMS is unlimited. 800 errors on a benchmark with no
# attack and no overload in it.
check "no client-visible errors" "${errored:-1}" "==" "0" \
  "streams are being refused on load that is neither attacking nor overloading"

# 2026-09-15: a bounded upstream queue turned overload into a refusal storm —
# 93% of all responses were 503s, at one seventh the throughput of the build
# without the bound.
check "no 5xx to the client" "${codes_5xx:-0}" "==" "0" \
  "the proxy is refusing work it should be serving"
check "no shedding" "${shed:-1}" "==" "0" \
  "admission control is engaging on load the proxy is not struggling with"

# 2026-09-16: raising MAX_PENDING moved the pool's growth threshold with it, so
# the pool stopped opening connections. Upstream parallelism fell 5 -> 1 and
# throughput fell with it. Nothing failed; no test moved. This is the check that
# would have caught it, and it needs no timing.
check "the pool grows under load" "${pool_peak:-0}" ">=" "$MIN_POOL" \
  "upstream parallelism collapsed — the pool is not opening connections under load"

# 2026-09-19: every concurrency figure this project quoted came from a gauge the
# daemon publishes once a second, sampled by a scraper that could not see
# between ticks. It read 7,310 where the engine counted 19,403. The counted peak
# replaced it, and this asserts the replacement is actually wired: a peak that
# reads zero after a run that served a million requests is the same class of
# defect wearing the new name.
check "the stream peak is counted" "${streams_peak:-0}" ">" "0" \
  "the high-water stream gauge read zero through a run that served traffic"

# The one relationship between these two that is true by construction rather
# than by workload: every live stream was once a peak candidate, so the peak can
# never be below a later instantaneous reading.
check "the stream peak is not below live" "${streams_peak:-0}" ">=" "${streams_live:-0}" \
  "the peak is lower than the instantaneous gauge, so one of them is miscounted"

# 2026-09-19: ProxyStats::response was wired only to responses arriving from a
# backend, so every 502 and 503 the proxy wrote *itself* reached clients
# uncounted and h2proxy_responses_total{class="5xx"} read zero unconditionally.
# Asserting "no 5xx" against a counter that cannot move is not a check at all.
#
# This run should shed nothing, so both are expected to be zero — the assertion
# is on the *implication*: if the pool ever shed, the 5xx counter must have
# moved with it. It costs nothing when both are zero and fails loudly if the
# wiring is ever removed while shedding is happening.
if [ "${shed:-0}" -gt 0 ] 2>/dev/null; then
  check "shedding reaches the 5xx counter" "${srv5xx:-0}" ">" "0" \
    "the pool shed requests but the 5xx counter did not move; it is disconnected"
fi

# Memory is the one quantity a shared runner can assert honestly: it is set by
# what the process holds, not by how fast the cores run. The bound is
# deliberately far above the ~35 MiB this shape actually uses — it is a leak
# check, not a budget, and tightening it toward the observed value would turn it
# into the flaky assertion this file exists to avoid.
check "resident memory stays bounded" "${rss_kb:-0}" "<=" "$MAX_RSS_KB" \
  "the proxy is holding far more memory than this shape can account for"

echo
if [ "$fail" -ne 0 ]; then
  echo "REGRESSION: one or more invariants above are broken." >&2
  exit 1
fi
echo "all invariants hold."
