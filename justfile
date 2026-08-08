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

# Measure the abuse-guard thresholds against legitimate traffic (design doc §6).
# Runs the honest profiles in observe-only mode and reports the headroom; fails
# if any signal is within 10x of tripping on traffic that did nothing wrong.
calibrate:
    bench/calibrate.sh

# Rapid Reset flood and a backend kill, each against a control run.
attack:
    bench/attack.sh
