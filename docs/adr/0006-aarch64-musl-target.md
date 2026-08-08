# ADR 0006 — Deploy target: aarch64-unknown-linux-musl static binary on Graviton

Status: accepted · Date: 2026-07-03 · Design doc: §8, §9.2

## Context

The proxy deploys to EC2 in a container; instance family and linking strategy
determine cost, image size, and runtime surprises.

## Decision

Target **EC2 Graviton (ARM, c7g/c8g)** and compile a **statically linked
aarch64-unknown-linux-musl** release binary, shipped in a distroless/scratch
image (multi-stage Docker build, §9.2).

## Rejected alternative

x86_64 + glibc dynamic linking. Comparable x86 instances price worse per unit of
performance; glibc dynamic linking drags a base image and its CVE surface into
the container and invites version-skew surprises at runtime.

## Consequences

- The final image is essentially one binary: minimal size, minimal attack
  surface, trivially reproducible.
- musl's default allocator is slow under multithreaded load — we swap in
  **jemalloc** before benchmarking (§9.2, week 8), and measure the delta.
- Cross-compiling from macOS needs the musl cross toolchain in the Docker build
  stage (not on the host); `rustup target add aarch64-unknown-linux-musl` is
  pinned in `rust-toolchain.toml` from week 1 so CI can build it early.
- Rust cross-compiles to aarch64 cleanly, neutralizing the "less ubiquitous
  target" risk.
