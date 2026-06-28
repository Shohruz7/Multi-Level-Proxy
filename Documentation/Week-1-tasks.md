Week 1 — Spec grounding, toolchain, repo, and committed decisions
1. Toolchain (pin everything)

Install via rustup and pin a rust-toolchain.toml (channel = a recent stable, e.g. 1.8x; check the current release rather than trusting a number from me). Pinning kills "works on my machine."
Add rustfmt + clippy components; add the aarch64-unknown-linux-musl target now (Graviton + static musl), even though you won't deploy real code until week 8.
cargo install the support tooling you'll lean on: cargo-nextest (test runner), cargo-llvm-cov (coverage), cargo-fuzz (differential fuzzing later), cargo-flamegraph and tokio-console (profiling later). Installing now means no detours mid-implementation.

2. Repo, workspace layout, and CI

Make it a Cargo workspace, not one crate: a library crate (h2proxy-core) holding the protocol engine, a binary crate (h2proxyd), and a bench/xtask crate later. The lib/bin split is what lets the h2-oracle tests target the engine directly.
Create the module skeleton as empty stubs with doc comments stating each module's responsibility: core/src/{frame,hpack,stream,flow,conn,pool,lb,proxy}.rs. Drawing these boundaries now is the architecture work.
Stand up GitHub Actions on day one: cargo fmt --check, cargo clippy -- -D warnings, cargo nextest run on every push. Setting CI up before there's code means it never goes red without you noticing.
README with the thesis, scope, and the same "from scratch" honesty note from the design doc.

3. Spec grounding (read + take committed notes) — this is the bulk of week 1's "thinking" time.

RFC 9113 (HTTP/2), targeted: connection preface, frame format (§4), frame types (§6), streams/multiplexing (§5), the stream state machine (§5.1), flow control (§5.2), error handling (§5.4), SETTINGS (§6.5).
RFC 7541 (HPACK) end to end: static table, dynamic-table sizing/eviction (the +32-byte accounting), integer and string primitives, Huffman, the four representations.
Skim RFC 9218 (priorities) just enough to justify skipping the dependency tree, and read the Rapid Reset advisory (CVE-2023-44487) so the §6 mitigation is grounded.
Output: condensed notes per section in docs/notes/. These double as your interview cheat sheet.

4. Read reference implementations (study, don't copy)

Walk the h2 crate source to learn how it models frames/streams and what its API looks like — you'll be testing against it, so this pays off immediately. Optionally skim nghttp2's framing for a second perspective. Then make your own boundary choices.

5. Lock decisions as ADRs

Write short Architecture Decision Records in docs/adr/ — chosen option, rejected alternative, consequence — one each for: tokio vs glommio; rustls vs OpenSSL; hand-built vs h2-as-oracle; h2 end-to-end vs h2→h1; NLB+self-terminated TLS vs ALB; aarch64-musl target; bytes::Bytes for zero-copy payloads; and the error model (thiserror in the lib, anyhow in the bin, plus the connection-error vs stream-error distinction). These are the §8 decisions made binding.
Commit the Cargo.toml dependency set: tokio (rt-multi-thread, net, io-util, sync, time, macros), rustls + tokio-rustls, bytes, tracing + tracing-subscriber, thiserror, anyhow; dev-deps h2 (oracle), proptest, criterion, rcgen (test certs). Check crates.io for current versions.

6. First runnable milestone: TLS + ALPN handshake

Build a tokio TCP listener that completes a rustls TLS 1.3 handshake and negotiates ALPN to "h2", using an rcgen self-signed cert. No frame parsing — just confirm the handshake and log the negotiated protocol. This retires all TLS-layer risk in week 1 and gives you a real main.

End of week 1: CI green on a pinned toolchain; workspace + module stubs committed; RFC notes and 8 ADRs in the repo; openssl s_client -alpn h2 -connect localhost:8443 (or curl --http2) shows ALPN negotiating h2.
