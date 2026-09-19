# Dev-loop shortcuts for h2proxy. Run `just` (no args) to list recipes.
# Install just: https://github.com/casey/just  (cargo binstall just)

# cargo-fuzz mis-defaults to the x86_64 target on Apple Silicon, so pass the
# host triple explicitly.
fuzz_triple := arch() + if os() == "macos" { "-apple-darwin" } else { "-unknown-linux-gnu" }

# List available recipes.
default:
    @just --list

# Run the proxy daemon (127.0.0.1:8443, TLS + ALPN h2).
run-proxy:
    cargo run -p h2proxyd

# Run the local h2c backend (127.0.0.1:8080).
run-backend:
    cargo run -p backend

# Build both, run the backend in the background, then the proxy in the foreground
# with the backend wired up as its upstream — the full client -> proxy -> backend
# path.
dev:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p backend -p h2proxyd
    ./target/debug/backend &
    backend_pid=$!
    trap 'kill $backend_pid 2>/dev/null || true' EXIT
    H2PROXYD_UPSTREAMS=127.0.0.1:8080 cargo run -p h2proxyd

# The week-5 server: no upstreams, so the built-in responder answers. What
# h2spec and the engine-only benchmarks run against.
run-server:
    cargo run -p h2proxyd

# Bring the dockerized backend up on a fixed port (8080).
backend-up:
    docker compose up --build

# Hit the backend directly over h2c — the no-proxy baseline path.
curl-backend:
    curl -s --http2-prior-knowledge -o /dev/null \
      -w 'http_version=%{http_version} code=%{http_code} size=%{size_download}\n' \
      http://127.0.0.1:8080/

# Hit the proxy over TLS + h2 and show what came back through it.
curl-through:
    curl -s -k --http2 -o /dev/null \
      -w 'http_version=%{http_version} code=%{http_code} size=%{size_download}\n' \
      https://127.0.0.1:8443/bytes/100000

# Workspace checks (mirror CI).
test:
    cargo nextest run --workspace

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Build a fuzz target (needs the nightly toolchain + cargo-fuzz).
fuzz-build target='frame_parser':
    cargo +nightly fuzz build {{target}} --target {{fuzz_triple}}

# Fuzz a target for N seconds (default 30). Targets: frame_parser, hpack_decoder, guard.
fuzz seconds='30' target='frame_parser':
    cargo +nightly fuzz run {{target}} --target {{fuzz_triple}} -- -max_total_time={{seconds}}

# Build every fuzz target — the check CI could run without a nightly fuzz run.
fuzz-build-all:
    just fuzz-build frame_parser
    just fuzz-build hpack_decoder
    just fuzz-build guard

# Capture the no-proxy baseline (client -> backend directly). See bench/README.md.
baseline:
    bench/baseline.sh

# RFC 9113 conformance against a running daemon (`just run-server` in another
# shell). Needs `brew install h2spec`; -t -k = TLS, skip cert verification.
h2spec target='':
    h2spec -t -k -h 127.0.0.1 -p 8443 {{target}}

# Conformance with the proxy in the path: backend + proxy + h2spec, all here.
# The engine is the same either way, so a difference between this and `h2spec`
# is a proxy-path bug rather than a protocol one.
h2spec-proxy target='':
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p backend -p h2proxyd
    ./target/debug/backend &
    backend_pid=$!
    H2PROXYD_UPSTREAMS=127.0.0.1:8080 ./target/debug/h2proxyd &
    proxy_pid=$!
    trap 'kill $backend_pid $proxy_pid 2>/dev/null || true' EXIT
    sleep 1
    h2spec -t -k -h 127.0.0.1 -p 8443 {{target}}

# Pool coalescing and bridge occupancy, live, from a running daemon's metrics.
coalescing:
    curl -s http://127.0.0.1:9090/metrics | grep -E 'upstream_|bridge_'

# Through-proxy throughput, against the committed bench/proxy-baseline.csv.
bench-proxy:
    bench/proxy-baseline.sh

# Micro-benchmarks: what the abuse guard and the RED histogram cost per frame.
bench-hot:
    cargo bench -p h2proxy-core --bench hot_path

# The headline measurement: delivered rate and coordinated-omission-corrected
# p99 against *offered* rate, stepped to and past the knee. Promotes
# bench/curve.csv and bench/curve.svg. Uses `loadgen`, not h2load — see
# bench/README.md for why h2load structurally cannot produce this number.
curve:
    bench/curve.sh

# The same ladder with no upstream hop: the daemon answers from its built-in
# responder, so the difference from `just curve` is the cost of the upstream
# leg. Promotes bench/curve-echo.csv and bench/curve-echo.svg.
curve-echo:
    MODE=echo bench/curve.sh

# Two methodologies against one proxy: what a closed-loop generator reports, and
# what was actually happening at the same delivered rate. This is the experiment
# behind the claim that h2load cannot measure a tail. Promotes
# bench/methodology.csv and bench/methodology.svg; PROMOTE=0 leaves them alone.
methodology:
    bench/methodology.sh

# Sweep the flow-control windows and concurrency (design doc §10.5). Reports
# throughput and the bridge's peak occupancy together, because the connection
# window is the bounded-memory bound and buying throughput with it is a trade.
tune:
    bench/tune.sh

# Both arms from one binary (H2PROXYD_POOL_GROWTH), interleaved, with a
# pre-registered decision rule: if the two arms' observed ranges overlap the
# script states no ratio at all. Re-measures the two numbers that rest on a
# single run each - the p99 at 20k, and the pool-growth pair quoted in
# core/src/pool.rs. Promotes bench/confirm.csv.
confirm:
    bench/confirm.sh

# The regressions this project has actually shipped, asserted. Structural
# invariants only - no timing - so it runs the same on a shared CI runner as it
# does here. Exits non-zero if the pool stops growing, if streams are refused on
# healthy load, or if anything sheds. Runs on every push.
regressions:
    bench/regressions.sh

# Admission control against the commit before it existed, at two shapes: one
# overloaded and one not. The un-overloaded shape is the one that catches
# regressions, because a throttle is nearly free on load that is already
# queueing - which is how a 5.4x regression once passed an A/B. Records
# pool_conns, the variable that collapsed while nothing was watching it.
# Promotes bench/admission-ab.csv.
admission-ab:
    bench/admission-ab.sh

# The ADR 0010 allocator A/B: system vs jemalloc, interleaved, inside the musl
# container — the only environment where the claim means anything. Needs Docker.
allocator:
    bench/allocator.sh

# Measure the abuse-guard thresholds against legitimate traffic (design doc §6).
# Runs the honest profiles in observe-only mode and reports the headroom; fails
# if any signal is within 10x of tripping on traffic that did nothing wrong.
calibrate:
    bench/calibrate.sh

# Rapid Reset flood and a backend kill, each against a control run.
attack:
    bench/attack.sh

# Synthesize the CDK stack and run its template assertions. Needs no AWS
# account: the stack is environment-agnostic and does no context lookups, which
# is what makes this the substitute for having deployed it (docs/adr/0022).
#
# Falls back to the container when the local toolchain hangs. On this machine
# `tsc` stalls at 0% CPU inside node's bootstrap for this project specifically
# (see infra/README.md); CI on node 22 compiles it fine, so it is an environment
# fault rather than a code one. A recipe that hangs forever teaches you to stop
# running it, which is the real cost.
synth timeout='240':
    #!/usr/bin/env bash
    set -euo pipefail
    cd infra
    [ -d node_modules ] || npm ci
    if command -v gtimeout >/dev/null; then TO=gtimeout
    elif command -v timeout >/dev/null; then TO=timeout
    else TO=""; fi
    if [ -n "$TO" ] && ! $TO {{timeout}} npx tsc --noEmit -p tsconfig.json; then
      echo "local tsc did not finish in {{timeout}}s; falling back to the container" >&2
      cd "$(git rev-parse --show-toplevel)"
      just synth-docker
      exit 0
    fi
    npm test
    npx cdk synth > /dev/null
    echo "stack synthesized and template assertions passed"

# The same checks inside node:22, for when the local toolchain will not run them.
#
# The source goes in over **stdin** rather than a bind mount: mounting this
# directory into the container fails separately with `Unknown system error -35`
# (EAGAIN) reading typescript's 5.9 MB `_tsc.js`. Tarring it in avoids the mount
# entirely. This is how the template assertions were verified while the local
# hang was unresolved.
synth-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    cd infra
    tar czf - --exclude=node_modules --exclude=dist --exclude=cdk.out --exclude=.git . \
      | docker run --rm -i node:22-slim bash -c \
        'mkdir -p /w && tar xzf - -C /w && cd /w \
         && npm ci --no-audit --no-fund --loglevel=error \
         && npm test && npx cdk synth > /dev/null \
         && echo "stack synthesized and template assertions passed (container)"'

# Five minutes of load with a backend dying and restarting throughout, sampling
# the quantities that must stay flat. The leak detector: every other harness here
# measures a moment, this one measures a trend. Fails if anything grew.
soak seconds='300':
    SECONDS_TOTAL={{seconds}} bench/soak.sh
