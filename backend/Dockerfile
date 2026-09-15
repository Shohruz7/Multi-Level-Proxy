# syntax=docker/dockerfile:1
# Dev-only image for the h2c backend (backend/src/main.rs), for `docker compose
# up` on a laptop. Glibc, host architecture, no platform pinning.
#
# **Not** the deployed image. The infra stack runs backends on Graviton, and
# this file built on an x86 laptop produces an amd64 image that will not start
# there. The deployed one comes from the top-level Dockerfile:
#
#     docker buildx build --platform linux/arm64 --build-arg PACKAGE=backend .
#
# Kept separate because a compose-up should not pay for a cross-arch static musl
# build to hand a laptop a stable upstream on a fixed port.
FROM rust:1.96-slim AS build
WORKDIR /src
COPY . .
# Only the backend and its deps compile here (it does not depend on the engine).
RUN cargo build --release -p backend

FROM debian:bookworm-slim
COPY --from=build /src/target/release/backend /usr/local/bin/backend
ENV BACKEND_LISTEN=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["backend"]
