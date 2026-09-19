#!/usr/bin/env bash
# Does admission control cost anything on load the proxy is not struggling with?
#
# This script exists because the first version of admission control was blessed
# by an A/B that only ever ran **one** shape: 500 connections x 40 streams. That
# workload is genuinely overloaded, and when everything is queueing anyway a
# throttle costs almost nothing — so the arms looked equal and a 5.4x regression
# shipped. The regression lived entirely in the un-overloaded regime, which was
# never measured.
#
# So the shape is a parameter and the default is to run **both**:
#
#   CONNS=50  STREAMS=20   ~1,000 streams   not overloaded; where regressions hide
#   CONNS=500 STREAMS=40   ~20,000 streams  overloaded; what admission is for
#
# `pool_conns` is a recorded column, not a convenience. The regression was a
# collapse of upstream parallelism from five connections to one, and nothing in
# the harness was looking at it — the throughput number moved and the cause was
# invisible. A column that was there would have named it immediately.
#
# Arms come from two binaries rather than one flag, because the comparison is
# against a commit rather than a setting. BASELINE_REF defaults to the commit
# before admission control existed.
#
# Usage:
#   bench/admission-ab.sh                       # both shapes, 3 pairs each
#   CONNS=50 STREAMS=20 bench/admission-ab.sh   # one shape
#   REPEATS=5 bench/admission-ab.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/admission-ab-$STAMP.csv"

BASELINE_REF="${BASELINE_REF:-f77ec11}"
WORKTREE="${WORKTREE:-$RESULTS/.baseline-$BASELINE_REF}"
DURATION="${DURATION:-15}"
REPEATS="${REPEATS:-3}"
COOLDOWN="${COOLDOWN:-60}"
METRICS="${METRICS:-127.0.0.1:9090}"
# Both shapes unless the caller pins one.
SHAPES="${SHAPES:-50x20 500x40}"
if [ -n "${CONNS:-}" ] && [ -n "${STREAMS:-}" ]; then SHAPES="${CONNS}x${STREAMS}"; fi

mkdir -p "$RESULTS"

# The baseline binary, built once from a worktree of the reference commit. Same
# toolchain and same flags as HEAD; only the source differs.
if [ ! -x "$WORKTREE/target/release/h2proxyd" ]; then
  echo "building the $BASELINE_REF baseline (once)" >&2
  [ -d "$WORKTREE" ] || git -C "$ROOT" worktree add -f "$WORKTREE" "$BASELINE_REF" >/dev/null
  (cd "$WORKTREE" && cargo build --release -p h2proxyd >/dev/null 2>&1)
fi
OLD="$WORKTREE/target/release/h2proxyd"
NEW="$ROOT/target/release/h2proxyd"

echo "building HEAD" >&2
(cd "$ROOT" && cargo build --release -p h2proxyd -p backend -p loadgen >/dev/null)

echo "cooling the box for ${COOLDOWN}s before measuring" >&2
sleep "$COOLDOWN"
uptime >&2

echo "shape,conns,streams,arm,repeat,order,achieved_rps,completed,failed,p99_ms,streams_peak,pool_conns,shed_total" > "$CSV"

backend_pid=""; proxy_pid=""
# Never `kill "${pid:-0}"`: an unset pid becomes 0 and `kill 0` signals the whole
# process group, which includes this script.
stop() {
  [ -n "${proxy_pid:-}" ] && kill "$proxy_pid" 2>/dev/null || true
  [ -n "${backend_pid:-}" ] && kill "$backend_pid" 2>/dev/null || true
  [ -n "${proxy_pid:-}" ] && wait "$proxy_pid" 2>/dev/null || true
  [ -n "${backend_pid:-}" ] && wait "$backend_pid" 2>/dev/null || true
  proxy_pid=""; backend_pid=""
}
trap stop EXIT

metric() { curl -s --max-time 1 "http://$METRICS/metrics" | awk -v k="$1" '$1==k {print $2; exit}'; }

run() {
  local shape="$1" conns="$2" streams="$3" arm="$4" repeat="$5" order="$6" bin
  [ "$arm" = baseline ] && bin="$OLD" || bin="$NEW"
  stop; sleep 2
  "$ROOT/target/release/backend" >/dev/null 2>&1 &
  backend_pid=$!
  H2PROXYD_UPSTREAMS=127.0.0.1:8080 "$bin" >/dev/null 2>&1 &
  proxy_pid=$!
  for _ in $(seq 1 60); do
    curl -sk --http2 -o /dev/null https://127.0.0.1:8443/ 2>/dev/null && break
    sleep 0.2
  done

  # Both peaks are sampled live. A pool gauge read after the load stops reports
  # the pool after connections have begun retiring, which is not the number the
  # claim is about.
  local pf="$RESULTS/.p.$$" cf="$RESULTS/.c.$$"
  echo 0 > "$pf"; echo 0 > "$cf"
  (
    while :; do
      m=$(curl -s --max-time 1 "http://$METRICS/metrics" || true)
      for pair in "h2proxy_client_streams_active $pf" "h2proxy_upstream_pool_connections $cf"; do
        set -- $pair
        v=$(awk -v k="$1" '$1==k{print $2;exit}' <<<"$m")
        if [ -n "${v:-}" ]; then
          b=$(cat "$2" 2>/dev/null || echo 0)
          awk -v a="$v" -v b="$b" 'BEGIN{exit !(a>b)}' && echo "$v" > "$2"
        fi
      done
      sleep 0.25
    done
  ) &
  local sampler=$!

  local out
  out=$("$ROOT/target/release/loadgen" --url https://127.0.0.1:8443/ --closed-loop \
        --connections "$conns" --streams "$streams" --duration "$DURATION" \
        --warmup 3 --label "$arm-$shape-$repeat" 2>/dev/null | tail -1)

  kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true
  local peak pool shed
  peak=$(cat "$pf" 2>/dev/null || echo NA); pool=$(cat "$cf" 2>/dev/null || echo NA)
  rm -f "$pf" "$cf"
  shed=$(metric h2proxy_upstream_shed_total)

  local completed failed achieved p99
  IFS=, read -r _ _ _ _ _ completed failed achieved _ _ p99 _ _ _ _ <<<"$out"

  echo "$shape,$conns,$streams,$arm,$repeat,$order,${achieved:-NA},${completed:-NA},${failed:-NA},${p99:-NA},${peak:-NA},${pool:-NA},${shed:-0}" >> "$CSV"
  printf '  %-8s %-8s #%s: %s req/s, %s failed, p99 %s ms, %s streams, %s conns, shed %s\n' \
    "$shape" "$arm" "$repeat" "${achieved:-NA}" "${failed:-NA}" "${p99:-NA}" "${peak:-NA}" "${pool:-NA}" "${shed:-0}" >&2
  stop
  sleep 3
}

for shape in $SHAPES; do
  conns="${shape%x*}"; streams="${shape#*x}"
  echo "== $shape ($((conns * streams)) streams in flight), $REPEATS alternating pairs ==" >&2
  for r in $(seq 1 "$REPEATS"); do
    # Alternate which arm goes first. Interleaving alone is not enough: drift
    # within a run lands on whichever arm consistently goes second.
    if [ $((r % 2)) -eq 1 ]; then
      run "$shape" "$conns" "$streams" baseline "$r" 1
      run "$shape" "$conns" "$streams" head     "$r" 2
    else
      run "$shape" "$conns" "$streams" head     "$r" 1
      run "$shape" "$conns" "$streams" baseline "$r" 2
    fi
  done
done

echo >&2
column -s, -t "$CSV" >&2

awk -F, -v shapes="$SHAPES" '
  NR>1 { k=$1 SUBSEP $4; rps[k]=rps[k] " " $7; fail[k]=fail[k] " " $9; pool[k]=pool[k] " " $12; peak[k]=peak[k] " " $11 }
  function median(list,   n,a,i,j,t) {
    n=split(list,a," "); if(n==0) return "NA"
    for(i=1;i<n;i++) for(j=i+1;j<=n;j++) if(a[j]+0<a[i]+0){t=a[i];a[i]=a[j];a[j]=t}
    return a[int((n+1)/2)]
  }
  function lo(list,   n,a,i,m){n=split(list,a," ");if(n==0)return "NA";m=a[1]+0;for(i=2;i<=n;i++)if(a[i]+0<m)m=a[i]+0;return m}
  function hi(list,   n,a,i,m){n=split(list,a," ");if(n==0)return "NA";m=a[1]+0;for(i=2;i<=n;i++)if(a[i]+0>m)m=a[i]+0;return m}
  function total(list,  n,a,i,t){n=split(list,a," ");t=0;for(i=1;i<=n;i++)t+=a[i]+0;return t}
  END {
    printf "\n"
    n = split(shapes, sh, " ")
    for (s = 1; s <= n; s++) {
      shape = sh[s]
      bk = shape SUBSEP "baseline"; hk = shape SUBSEP "head"
      if (rps[bk] == "" || rps[hk] == "") continue
      printf "=== %s ===\n", shape
      printf "  baseline  %8s req/s (%.0f-%.0f)  pool %s-%s  peak %s  failed %d\n",
             median(rps[bk]), lo(rps[bk]), hi(rps[bk]), lo(pool[bk]), hi(pool[bk]), median(peak[bk]), total(fail[bk])
      printf "  head      %8s req/s (%.0f-%.0f)  pool %s-%s  peak %s  failed %d\n",
             median(rps[hk]), lo(rps[hk]), hi(rps[hk]), lo(pool[hk]), hi(pool[hk]), median(peak[hk]), total(fail[hk])
      # Three axes, not one. An earlier version of this verdict compared only
      # throughput and printed "no regression" for a run in which peak
      # concurrency had fallen 4.4x and 3,248 requests had failed. A benchmark
      # that reports the number you were looking at, while a different number
      # moves, is how this class of defect survives.
      bad = 0
      # Only a *slower* head is a regression. An earlier version flagged any
      # disjoint pair, so it reported head being 1.26x faster as a defect.
      if (hi(rps[hk]) < lo(rps[bk])) {
        printf "  REGRESSION: head is slower (head/baseline = %.2fx, ranges disjoint)\n",
               median(rps[hk]) / median(rps[bk]); bad = 1
      }
      if (total(fail[hk]) > total(fail[bk])) {
        printf "  REGRESSION: head failed %d requests where baseline failed %d\n",
               total(fail[hk]), total(fail[bk]); bad = 1
      }
      # Concurrency gets *reported*, never asserted, and the distinction is the
      # whole point of this block.
      #
      # Holding fewer streams is what admission control is for. The baseline held
      # 18,215 because it had no bound and queued everything; refusing to sit on
      # that much work is the feature, not a regression. Asserting a floor on
      # concurrency here would be asserting that the feature is absent.
      #
      # It only means something alongside the other two axes: fewer streams with
      # worse throughput or new failures is a throttle, and those are already
      # flagged above. Fewer streams with better throughput is Littles law - the
      # same work moving through a shorter queue.
      if (median(peak[hk]) + 0 < median(peak[bk]) * 0.75) {
        printf "  NOTE: peak concurrency %s vs %s. With throughput and failures\n",
               median(peak[hk]), median(peak[bk])
        printf "        unharmed this is admission holding less work, not a loss —\n"
        printf "        residency %.0f ms vs %.0f ms.\n",
               median(peak[hk]) / median(rps[hk]) * 1000,
               median(peak[bk]) / median(rps[bk]) * 1000
      }
      if (bad == 0)
        printf "  VERDICT: throughput and failures within baseline.\n\n"
      else
        printf "\n\n" 
    }
    printf "A throttle is nearly free on load that is already queueing, so the\n"
    printf "un-overloaded shape is the one that catches a regression. Both are run\n"
    printf "because the first version of this comparison ran only the other one.\n"
  }' "$CSV" >&2

if [ "${PROMOTE:-1}" = "1" ]; then
  cp "$CSV" "$HERE/admission-ab.csv"
  echo >&2
  echo "promoted to bench/admission-ab.csv" >&2
fi
echo "written: $CSV" >&2
