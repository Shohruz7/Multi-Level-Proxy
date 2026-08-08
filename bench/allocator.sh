#!/usr/bin/env bash
# The allocator A/B that ADR 0010 has been deferring since July: does swapping
# musl's allocator for jemalloc actually buy anything?
#
# Measured *inside the container*, on the musl target, because that is the only
# place the question means anything. ADR 0010's argument is specifically about
# musl's allocator contending under a work-stealing runtime; running the A/B on
# macOS would compare two allocators neither of which is the one being replaced.
#
# Both arms come from the same Dockerfile with one build argument different, and
# the runs are **interleaved** rather than run in blocks — a Docker Desktop VM
# drifts (thermal, page cache, other containers), and drift between two blocks is
# indistinguishable from an effect. Alternating A/B/A/B leaves drift in both arms.
#
# The ADR asked for the number rather than the assumption, so this reports the
# delta whatever its sign. A wash is a result.
#
# Usage:
#   bench/allocator.sh              # 3 interleaved pairs, throughput profile
#   REPEATS=5 RATE=20000 bench/allocator.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CSV="$RESULTS/allocator-$STAMP.csv"

REPEATS="${REPEATS:-3}"
RATE="${RATE:-20000}"
SECONDS_PER_RUN="${SECONDS_PER_RUN:-20}"
CONNECTIONS="${CONNECTIONS:-50}"
NET=h2bench
PROXY_PORT=18443
METRICS_PORT=19090

mkdir -p "$RESULTS"

command -v docker >/dev/null || { echo "docker required" >&2; exit 1; }

echo "building the load generator" >&2
(cd "$ROOT" && cargo build --release -p loadgen)

echo "building both arms from the same Dockerfile" >&2
docker build --platform linux/arm64 -t h2proxyd:system  "$ROOT" >/dev/null
docker build --platform linux/arm64 -t h2proxyd:jemalloc --build-arg FEATURES=jemalloc "$ROOT" >/dev/null
docker build --platform linux/arm64 -f "$ROOT/backend/Dockerfile" -t h2backend:bench "$ROOT" >/dev/null

cleanup() {
  docker rm -f h2bench-proxy h2bench-backend >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
docker network create "$NET" >/dev/null

docker run -d --name h2bench-backend --network "$NET" \
  -e BACKEND_LISTEN=0.0.0.0:8080 h2backend:bench >/dev/null
sleep 2
BACKEND_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' h2bench-backend)

echo "arm,repeat,allocator_reported,achieved_rps,p50_ms,p99_ms,p999_ms,max_ms,rss_bytes" > "$CSV"

run_arm() {
  local arm="$1" repeat="$2"
  docker rm -f h2bench-proxy >/dev/null 2>&1 || true
  # h2proxyd takes literal addresses, never names (see its `upstreams()` doc
  # comment), so the container IP is resolved here — the same constraint the
  # CDK user-data works around with getent.
  docker run -d --name h2bench-proxy --network "$NET" \
    -p "$PROXY_PORT:8443" -p "$METRICS_PORT:9090" \
    -e "H2PROXYD_UPSTREAMS=$BACKEND_IP:8080" \
    "h2proxyd:$arm" >/dev/null
  for _ in $(seq 1 50); do
    curl -sk --http2 -o /dev/null "https://127.0.0.1:$PROXY_PORT/" 2>/dev/null && break
    sleep 0.2
  done

  # Read the allocator off /metrics rather than trusting the tag: the whole A/B
  # is worthless if the two arms cannot be told apart after the fact.
  local reported
  reported=$(curl -s "http://127.0.0.1:$METRICS_PORT/metrics" \
    | sed -n 's/^h2proxy_build_info{allocator="\([a-z]*\)".*/\1/p' | head -1)
  if [ "$reported" != "$arm" ]; then
    echo "arm mismatch: expected $arm, container reports '${reported:-none}'" >&2
    exit 1
  fi

  local out
  out=$("$ROOT/target/release/loadgen" --url "https://127.0.0.1:$PROXY_PORT/" \
        --rate "$RATE" --connections "$CONNECTIONS" \
        --duration "$SECONDS_PER_RUN" --warmup 3 --label "$arm-$repeat" \
        2>>"$RESULTS/allocator-$STAMP.log" | tail -1)
  local achieved p50 p99 p999 max
  IFS=, read -r _ _ _ _ _ _ _ achieved p50 _ p99 p999 max _ _ <<<"$out"

  # Resident memory of the proxy container, which is the other half of an
  # allocator claim: a throughput win paid for in RSS is a trade, not a win.
  local rss
  rss=$(docker stats --no-stream --format '{{.MemUsage}}' h2bench-proxy 2>/dev/null \
        | awk '{print $1}')

  echo "$arm,$repeat,$reported,${achieved:-NA},${p50:-NA},${p99:-NA},${p999:-NA},${max:-NA},${rss:-NA}" >> "$CSV"
  echo "  $arm #$repeat: ${achieved:-NA} req/s, p99 ${p99:-NA} ms, rss ${rss:-NA}" >&2
}

for repeat in $(seq 1 "$REPEATS"); do
  echo "== pair $repeat of $REPEATS ==" >&2
  run_arm system "$repeat"
  run_arm jemalloc "$repeat"
done

echo >&2
column -s, -t "$CSV" >&2

# The verdict, computed rather than eyeballed. Medians, because three runs and a
# mean is one outlier away from a conclusion.
awk -F, '
  NR>1 && $4 != "NA" { rps[$1] = rps[$1] " " $4; p99[$1] = p99[$1] " " $6 }
  function median(list,   n, a, i) {
    n = split(list, a, " "); if (n == 0) return "NA"
    for (i = 1; i < n; i++) for (j = i+1; j <= n; j++) if (a[j] < a[i]) { t=a[i]; a[i]=a[j]; a[j]=t }
    return a[int((n+1)/2)]
  }
  END {
    s = median(rps["system"]); j = median(rps["jemalloc"])
    sp = median(p99["system"]); jp = median(p99["jemalloc"])
    printf "\nmedian throughput: system %s req/s, jemalloc %s req/s", s, j
    if (s > 0) printf " (%+.1f%%)", (j - s) / s * 100
    printf "\nmedian p99:        system %s ms, jemalloc %s ms", sp, jp
    if (sp > 0) printf " (%+.1f%%)", (jp - sp) / sp * 100
    printf "\n\nRecord this in docs/adr/0010 whatever it says. A wash is a result.\n"
  }' "$CSV" >&2

echo "written: $CSV" >&2
