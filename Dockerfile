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
# The allocator is a build argument (docs/adr/0010): `--build-arg FEATURES=jemalloc`
# produces the benchmark/production arm, and the default produces the control.
# Both are built from this one file so that an A/B between them differs in the
# allocator and in nothing else.

FROM --platform=$BUILDPLATFORM rust:1.96-slim AS build

# rustls' crypto provider is aws-lc-rs (ADR 0002) compiles C through cmake, so
# the musl target needs a musl C toolchain: `musl-tools` provides `musl-gcc`,
# which is a gcc wrapper pointed at musl's headers and libc for the *native*
# architecture — which is this one, because the image is built on arm64 for
# arm64 (ADR 0006).
RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools musl-dev cmake make perl pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG TARGET=aarch64-unknown-linux-musl
ARG FEATURES=""
RUN rustup target add "$TARGET"

WORKDIR /src
COPY . .

# The C half of the build must use musl's headers, not the image's glibc ones.
#
# Both of this file's original values were wrong, and neither could have been
# known without running it. `AR=llvm-ar` named a binary the `clang` package does
# not ship. `CC=clang` then compiled aws-lc-rs against *glibc* headers and linked
# the result against musl, which fails at the very end of a ten-minute build with
# `undefined reference to __isoc23_strtol` — glibc 2.38 redirects `strtol` to a
# symbol musl has never had. `musl-gcc` is the only one of the three that sees a
# consistent set of headers and libraries.
ENV CC_aarch64_unknown_linux_musl=musl-gcc \
    AR_aarch64_unknown_linux_musl=ar

RUN cargo build --release --locked --target "$TARGET" -p h2proxyd \
        ${FEATURES:+--features "$FEATURES"} \
    && cp "target/${TARGET}/release/h2proxyd" /h2proxyd

FROM scratch
COPY --from=build /h2proxyd /h2proxyd
# 8443: TLS + h2 listener (H2PROXYD_LISTEN); 9090: Prometheus /metrics.
EXPOSE 8443 9090
ENV H2PROXYD_LISTEN=0.0.0.0:8443 \
    H2PROXYD_METRICS=0.0.0.0:9090
ENTRYPOINT ["/h2proxyd"]
