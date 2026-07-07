# syntax=docker/dockerfile:1
#
# Production image for h2proxyd: one static aarch64 (Graviton) binary on
# `scratch`. Build it for arm64 — the target arch of the week-8 deploy:
#
#     docker buildx build --platform linux/arm64 -t h2proxyd:arm64 .
#
# The binary is statically linked against musl (aarch64-unknown-linux-musl, the
# target pinned in rust-toolchain.toml), so the runtime layer needs no libc and
# `scratch` suffices. Building on an arm64 host (Apple Silicon / Graviton) keeps
# this a same-CPU musl build, not a cross-arch one.
#
# jemalloc is swapped in for benchmark builds (docs/adr/0010); wiring it is
# deferred to week 8, so it is intentionally absent here.

FROM --platform=$BUILDPLATFORM rust:1.96-slim AS build

# rustls' crypto provider is aws-lc-rs (ADR 0002); it compiles C through cmake,
# and the musl target needs a musl C toolchain. clang + musl-tools cover both.
RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools clang cmake make perl pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG TARGET=aarch64-unknown-linux-musl
RUN rustup target add "$TARGET"

WORKDIR /src
COPY . .

# Use clang as the musl-target C compiler so aws-lc-rs builds.
ENV CC_aarch64_unknown_linux_musl=clang \
    AR_aarch64_unknown_linux_musl=llvm-ar

RUN cargo build --release --locked --target "$TARGET" -p h2proxyd \
    && cp "target/${TARGET}/release/h2proxyd" /h2proxyd

FROM scratch
COPY --from=build /h2proxyd /h2proxyd
# 8443: TLS + h2 listener (H2PROXYD_LISTEN); 9090: Prometheus /metrics.
EXPOSE 8443 9090
ENV H2PROXYD_LISTEN=0.0.0.0:8443 \
    H2PROXYD_METRICS=0.0.0.0:9090
ENTRYPOINT ["/h2proxyd"]
