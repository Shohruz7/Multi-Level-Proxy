#!/usr/bin/env bash
# Confirm — or decline to confirm — the two numbers this project quotes that rest
# on a single run each.
#
# Both live in more than the README. The p99 at 20,000 req/s is in
# Documentation/RESULTS.md; the pool-growth comparison is quoted in a *source
# comment* (core/src/pool.rs) as the justification for the default policy. A
# justification nobody can re-run is an assertion, and this file is the answer to
# that.
#
# The method is bench/allocator.sh's, for the same reason: both arms come from
# one binary with one flag different (H2PROXYD_POOL_GROWTH), and the arms are
# **interleaved** rather than run in blocks, because a laptop drifts and drift
# between two blocks is indistinguishable from an effect.
#
# What is different here is that the decision rule is **pre-registered and
# enforced by the script**. allocator.sh states it in prose at the end — "read
# the spread before the medians; if the two arms overlap there is no effect to
# report" — and leaves obeying it to the reader. Here, if the arms' observed
# ranges overlap, no ratio is printed at all. That matters because this project
# has already had to retract two harness-shaped numbers, and both retractions
# began with a single run that looked convincing.
#
# Two rates, on purpose, because the two claims were made at different ones:
#   20,000 req/s — below the knee, where the p99 and "settles on one connection"
#                  claims were made, and where repeats are stable.
#   30,000 req/s — the saturation regime the pool.rs comment quotes. Runs here
#                  are noisy by nature; the decision rule is expected to do real
#                  work at this rate rather than rubber-stamp it.
#
# Usage:
#   bench/confirm.sh                       # 5 interleaved pairs per rate
#   REPEATS=3 RATES=20000 bench/confirm.sh
#   PROMOTE=0 bench/confirm.sh             # leave bench/confirm.csv alone
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/confirm-$STAMP.csv"
LOG="$RESULTS/confirm-$STAMP.log"

REPEATS="${REPEATS:-5}"
RATES="${RATES:-10000 20000 30000}"
CONNECTIONS="${CONNECTIONS:-50}"
SECONDS_PER_RUN="${SECONDS_PER_RUN:-15}"
WARMUP="${WARMUP:-3}"
BODY_SIZE="${BODY_SIZE:-1024}"

BACKEND_ADDR="${BACKEND_ADDR:-127.0.0.1:8080}"
PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:8443}"
METRICS="${METRICS:-127.0.0.1:9090}"
TARGET="https://$PROXY_ADDR/"

mkdir -p "$RESULTS"

echo "building release binaries (a debug measurement is not a measurement)" >&2
(cd "$ROOT" && cargo build --release -p backend -p h2proxyd -p loadgen)

backend_pid=""
proxy_pid=""
stop_stack() {
  kill "${proxy_pid:-0}" "${backend_pid:-0}" 2>/dev/null || true
  wait "${proxy_pid:-0}" 2>/dev/null || true
  wait "${backend_pid:-0}" 2>/dev/null || true
  backend_pid=""; proxy_pid=""
}
trap stop_stack EXIT

# A fresh stack per run, not per arm. Leaving the proxy up between arms would
# carry the previous arm's pool into the next one — and the pool *is* the thing
# under test, so that would be measuring history.
start_stack() {
  local growth="$1"
  BACKEND_BODY_SIZE="$BODY_SIZE" "$ROOT/target/release/backend" >>"$LOG" 2>&1 &
  backend_pid=$!
  H2PROXYD_UPSTREAMS="$BACKEND_ADDR" H2PROXYD_LISTEN="$PROXY_ADDR" \
    H2PROXYD_METRICS="$METRICS" H2PROXYD_BODY_SIZE="$BODY_SIZE" \
    H2PROXYD_POOL_GROWTH="$growth" \
    "$ROOT/target/release/h2proxyd" >>"$LOG" 2>&1 &
  proxy_pid=$!
  for _ in $(seq 1 50); do
    curl -sk --http2 -o /dev/null "$TARGET" 2>/dev/null && return 0
    sleep 0.2
  done
  echo "proxy did not come up" >&2
  exit 1
}

metric() {
  curl -s --max-time 1 "http://$METRICS/metrics" | awk -v k="$1" '$1==k {print $2; exit}'
}

echo "rate,arm,repeat,order,seq,achieved_rps,p50_ms,p99_ms,p999_ms,max_ms,pool_conns_peak,shed_total" > "$CSV"
SEQ=0

run_arm() {
  local rate="$1" arm="$2" repeat="$3" order="$4"
  SEQ=$((SEQ + 1))
  start_stack "$arm"

  # The pool size has to be sampled *while the run happens*. It is a live gauge,
  # and reading it after the load stops reports the pool after the connections
  # have started retiring — which is not the number the claim is about.
  local peakfile="$RESULTS/.conns.$$"
  echo 0 > "$peakfile"
  (
    while :; do
      v=$(metric h2proxy_upstream_pool_connections)
      if [ -n "${v:-}" ]; then
        best=$(cat "$peakfile" 2>/dev/null || echo 0)
        awk -v a="$v" -v b="$best" 'BEGIN{exit !(a>b)}' && echo "$v" > "$peakfile"
      fi
      sleep 0.25
    done
  ) &
  local sampler_pid=$!

  local out
  out=$("$ROOT/target/release/loadgen" --url "$TARGET" \
        --rate "$rate" --connections "$CONNECTIONS" \
        --duration "$SECONDS_PER_RUN" --warmup "$WARMUP" \
        --label "$arm-$rate-$repeat" 2>>"$LOG" | tail -1)

  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  local conns
  conns=$(cat "$peakfile" 2>/dev/null || echo NA)
  rm -f "$peakfile"

  # Shedding at these rates would mean the admission bound is engaging, which
  # changes what a latency number means: a shed request is fast and did not get
  # served. Recorded so that a p99 improvement bought with 503s is visible.
  local shed
  shed=$(metric h2proxy_upstream_shed_total)

  local achieved p50 p99 p999 max
  IFS=, read -r _ _ _ _ _ _ _ achieved p50 _ p99 p999 max _ _ <<<"$out"

  echo "$rate,$arm,$repeat,$order,$SEQ,${achieved:-NA},${p50:-NA},${p99:-NA},${p999:-NA},${max:-NA},${conns:-NA},${shed:-0}" >> "$CSV"
  printf '  %-5s #%s (ran %s) @%s: %s req/s, p99 %s ms, pool %s conns, shed %s\n' \
    "$arm" "$repeat" "$order" "$rate" "${achieved:-NA}" "${p99:-NA}" "${conns:-NA}" "${shed:-0}" >&2

  stop_stack
  # A cooldown, not politeness. The first run of this harness degraded by 2x
  # across eight minutes of continuous load; letting the box settle between runs
  # shrinks the drift the pairing then has to cancel.
  sleep 3
}

# Interleaving alone is not enough, and the first run of this harness proved it.
# With `queue` always going first, the queue arm won all five pairs at 30,000
# req/s - but the whole sequence degraded monotonically as it ran (the queue arm
# itself went from 199 ms to 408 ms across the five repeats), so *whichever* arm
# went first would have won all five. Position in the pair was worth more than
# the policy under test.
#
# So the order alternates by repeat parity. Drift still exists, but it no longer
# lands on one arm, and the `order` column makes the residual bias checkable
# rather than assumed.
for rate in $RATES; do
  echo "== $rate req/s, $REPEATS alternating pairs ==" >&2
  for repeat in $(seq 1 "$REPEATS"); do
    if [ $((repeat % 2)) -eq 1 ]; then
      run_arm "$rate" queue "$repeat" 1
      run_arm "$rate" eager "$repeat" 2
    else
      run_arm "$rate" eager "$repeat" 1
      run_arm "$rate" queue "$repeat" 2
    fi
  done
done

echo >&2
column -s, -t "$CSV" >&2

# The verdict, computed rather than eyeballed - and withheld when the data does
# not support one.
#
# Two analyses, because interleaved runs produce *paired* data and the two
# analyses answer different questions:
#
#   unpaired range: do the arms' observed values even separate? This is the
#       conservative rule. It is the right one when the arms were measured under
#       conditions that drifted, because a median computed across drift is a
#       median of two different machines.
#
#   paired sign test: within each pair - two runs seconds apart, under nearly the
#       same conditions - which arm won? Drift that moves both arms together
#       cancels here, so this sees effects the range rule cannot. n pairs all
#       agreeing has probability 0.5^n under the null.
#
# Where they disagree, say so rather than picking the flattering one.
awk -F, -v rates="$RATES" '
  NR>1 && $8 != "NA" {
    key = $1 SUBSEP $2
    p99[key] = p99[key] " " $8
    rps[key] = rps[key] " " $6
    conns[key] = conns[key] " " $11
    shed[key] = shed[key] " " $12
    pair[$1 SUBSEP $3 SUBSEP $2] = $8
    firstarm[$1 SUBSEP $3] = ($4 == 1 ? $2 : firstarm[$1 SUBSEP $3])
    repeats[$1] = ($3 + 0 > repeats[$1] + 0) ? $3 + 0 : repeats[$1] + 0
  }
  # Every scratch variable is declared as a parameter, including the inner loop
  # counters. bench/allocator.sh carries the same note because an earlier version
  # left `j` and `t` global while the caller also used `j` to hold a result, so
  # the loop clobbered the value being assigned and the summary reported a
  # catastrophic regression for a run in which both arms were identical. A
  # reporting bug that invents a result is worse than no summary at all.
  function median(list,   n, a, i, k, t) {
    n = split(list, a, " "); if (n == 0) return "NA"
    for (i = 1; i < n; i++)
      for (k = i + 1; k <= n; k++)
        if (a[k] + 0 < a[i] + 0) { t = a[i]; a[i] = a[k]; a[k] = t }
    return a[int((n + 1) / 2)]
  }
  function lo(list,   n, a, i, m) {
    n = split(list, a, " "); if (n == 0) return "NA"
    m = a[1] + 0; for (i = 2; i <= n; i++) if (a[i]+0 < m) m = a[i]+0
    return m
  }
  function hi(list,   n, a, i, m) {
    n = split(list, a, " "); if (n == 0) return "NA"
    m = a[1] + 0; for (i = 2; i <= n; i++) if (a[i]+0 > m) m = a[i]+0
    return m
  }
  function total(list,   n, a, i, t) {
    n = split(list, a, " "); t = 0
    for (i = 1; i <= n; i++) t += a[i] + 0
    return t
  }
  function pow5(n,   i, v) { v = 1; for (i = 0; i < n; i++) v *= 0.5; return v }
  # The rate list comes in from the shell rather than from the keys, because
  # macOS ships the one true awk and `asorti` is a gawk extension. Iterating the
  # list the caller passed also keeps the report in the order the runs happened.
  END {
    printf "\n"
    n = split(rates, ordered, " ")
    for (r = 1; r <= n; r++) {
      rate = ordered[r]
      qk = rate SUBSEP "queue"; ek = rate SUBSEP "eager"
      if (p99[qk] == "" || p99[ek] == "") continue
      qm = median(p99[qk]); em = median(p99[ek])
      qlo = lo(p99[qk]); qhi = hi(p99[qk])
      elo = lo(p99[ek]); ehi = hi(p99[ek])
      reps = repeats[rate]

      printf "=== %s req/s (%d pairs) ===\n", rate, reps
      printf "  queue  p99 median %8s ms, observed %8.3f-%-8.3f  pool %s-%s conns  %s req/s  shed %d\n",
             qm, qlo, qhi, lo(conns[qk]), hi(conns[qk]), median(rps[qk]), total(shed[qk])
      printf "  eager  p99 median %8s ms, observed %8.3f-%-8.3f  pool %s-%s conns  %s req/s  shed %d\n",
             em, elo, ehi, lo(conns[ek]), hi(conns[ek]), median(rps[ek]), total(shed[ek])

      # Shedding means the admission bound engaged, so the arms are two
      # *overloaded* systems and a p99 gap between them is not a latency result:
      # the faster arm may simply have refused more work. Said plainly, because
      # the number this harness re-measures was taken before admission control
      # existed and therefore cannot be reproduced in this regime at all.
      if (total(shed[qk]) > 0 || total(shed[ek]) > 0)
        printf "  NOTE: both arms shed here, so this compares two overloaded systems.\n         p99 is not comparable across arms that refused different amounts.\n"

      # Paired: same repeat, seconds apart, alternating which went first.
      qwins = 0; ewins = 0; pairs = 0
      for (i = 1; i <= reps; i++) {
        qv = pair[rate SUBSEP i SUBSEP "queue"]; ev = pair[rate SUBSEP i SUBSEP "eager"]
        if (qv == "" || ev == "") continue
        pairs++
        if (qv + 0 < ev + 0) qwins++; else ewins++
      }
      if (pairs > 0)
        printf "  paired: queue won %d/%d, eager won %d/%d (p = %.3f if one arm swept)\n",
               qwins, pairs, ewins, pairs, pow5(pairs)

      overlap = (qhi >= elo && ehi >= qlo)
      swept   = (pairs > 0 && (qwins == pairs || ewins == pairs))

      if (!overlap && qm + 0 > 0) {
        printf "  VERDICT: ranges are disjoint. eager/queue p99 = %.2fx (%s vs %s ms).\n\n",
               em / qm, em, qm
      } else if (swept && pairs >= 5) {
        printf "  VERDICT: ranges overlap, but %s won every one of %d pairs.\n",
               (qwins == pairs ? "queue" : "eager"), pairs
        printf "           Report this as a direction, not a ratio: the arms overlap\n"
        printf "           (%.3f-%.3f vs %.3f-%.3f), so the magnitude is not established.\n\n",
               qlo, qhi, elo, ehi
      } else {
        printf "  VERDICT: no effect to report - the arms overlap (%.3f-%.3f vs %.3f-%.3f)\n",
               qlo, qhi, elo, ehi
        printf "           and no arm swept the pairs. Quote the range, not a ratio.\n\n"
      }
    }
    printf "Read the observed ranges before the medians. This script declines to\n"
    printf "state a ratio the data does not separate, which is the whole reason it\n"
    printf "exists: both numbers it re-measures came from a single run each.\n"
  }' "$CSV" >&2

if [ "${PROMOTE:-1}" = "1" ]; then
  cp "$CSV" "$HERE/confirm.csv"
  echo >&2
  echo "promoted to bench/confirm.csv - the numbers README.md, Documentation/RESULTS.md" >&2
  echo "and the core/src/pool.rs growth comment cite" >&2
fi

echo "written: $CSV" >&2
