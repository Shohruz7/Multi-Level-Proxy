# syntax=docker/dockerfile:1
# Dev-only image for the h2c backend (backend/src/main.rs). This is NOT the
# proxy — the proxy's production image is the top-level Dockerfile (aarch64
# static-musl). Kept simple on purpose: it exists to give `docker compose up` a
# stable upstream on a fixed port.
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
