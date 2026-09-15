# syntax=docker/dockerfile:1
#
# Production image for any binary in this workspace: one static aarch64
# (Graviton) binary on `scratch`. Build it for arm64 — the target arch of the
# week-8 deploy:
#
#     docker buildx build --platform linux/arm64 -t h2proxyd:arm64 .
#     docker buildx build --platform linux/arm64 --build-arg PACKAGE=backend .
#     docker buildx build --platform linux/arm64 --build-arg PACKAGE=loadgen .
#
# One recipe for all three because the deploy needs all three and they must not
# differ in libc, allocator or toolchain. `backend/Dockerfile` is a separate,
# glibc, host-arch image that exists only for `docker compose up` on a laptop;
# building *that* for a Graviton instance produces an amd64 image that will not
# start, which is the mistake this parameterisation removes.
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
# Which workspace binary to build. The default keeps `docker build .` meaning
# what it has always meant.
ARG PACKAGE=h2proxyd
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
#
# `-mno-outline-atomics` is the third thing that had to be discovered by running
# this file, and the least guessable. GCC 10+ on aarch64 defaults to
# `-moutline-atomics`, which links a libgcc startup object that calls
# `__getauxval` to detect LSE atomics at runtime. `__getauxval` is a **glibc**
# symbol; musl has only `getauxval`. So every link of a C dependency that uses
# atomics fails with `undefined reference to __getauxval` — and jemalloc's
# configure sees those failures as "this platform has no atomics" and emits
# `#error "Don't have atomics implemented on this platform."` several hundred
# lines into the build, naming nothing that would lead you here.
ENV CC_aarch64_unknown_linux_musl=musl-gcc \
    AR_aarch64_unknown_linux_musl=ar \
    CFLAGS_aarch64_unknown_linux_musl=-mno-outline-atomics

RUN cargo build --release --locked --target "$TARGET" -p "$PACKAGE" \
        ${FEATURES:+--features "$FEATURES"} \
    && cp "target/${TARGET}/release/${PACKAGE}" /app

FROM scratch
# A fixed path, because ENTRYPOINT's exec form cannot expand a build argument.
COPY --from=build /app /app
# 8443: TLS + h2 listener (H2PROXYD_LISTEN); 9090: Prometheus /metrics. Both
# are metadata and the defaults below are read only by h2proxyd — the other two
# binaries ignore them, which is why one final stage serves all three.
EXPOSE 8443 9090
ENV H2PROXYD_LISTEN=0.0.0.0:8443 \
    H2PROXYD_METRICS=0.0.0.0:9090
ENTRYPOINT ["/app"]
