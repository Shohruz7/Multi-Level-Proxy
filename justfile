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

# Build both, run the backend in the background, then the proxy in the foreground.
dev:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p backend -p h2proxyd
    ./target/debug/backend &
    backend_pid=$!
    trap 'kill $backend_pid 2>/dev/null || true' EXIT
    cargo run -p h2proxyd

# Bring the dockerized backend up on a fixed port (8080).
backend-up:
    docker compose up --build

# Hit the backend directly over h2c — the no-proxy baseline path.
curl-backend:
    curl -s --http2-prior-knowledge -o /dev/null \
      -w 'http_version=%{http_version} code=%{http_code} size=%{size_download}\n' \
      http://127.0.0.1:8080/

# Hit the proxy over TLS + h2 (ALPN negotiates; request stalls until week 5).
curl-through:
    curl -sv -k --http2 --max-time 3 https://127.0.0.1:8443/ 2>&1 | grep -iE 'alpn|http'

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

# Fuzz a target for N seconds (default 30). Targets: frame_parser, hpack_decoder.
fuzz seconds='30' target='frame_parser':
    cargo +nightly fuzz run {{target}} --target {{fuzz_triple}} -- -max_total_time={{seconds}}

# Build every fuzz target — the check CI could run without a nightly fuzz run.
fuzz-build-all:
    just fuzz-build frame_parser
    just fuzz-build hpack_decoder

# Capture the no-proxy baseline (client -> backend directly). See bench/README.md.
baseline:
    bench/baseline.sh
