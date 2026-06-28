Week 2 — Engine skeleton, test/bench harnesses, and a real deploy
1. Async connection skeleton

Build the multi-threaded runtime entrypoint, accept loop, per-connection task spawn, and graceful shutdown (SIGTERM → broadcast → drain). No frame logic yet — just the task lifecycle and a clean shutdown path, establishing the §2.2 concurrency model.
Decide the task topology concretely and prototype the channel types: one frame-reader task dispatching to per-stream handlers over bounded mpsc channels (the bound is what later becomes your backpressure mechanism — §4.2). Document the choice.

2. Core types and traits (stubbed)

Define FrameHeader, a Frame enum (Data/Headers/Settings/WindowUpdate/RstStream/Ping/GoAway/Continuation), StreamId, StreamState, ErrorCode, Settings — with encode/decode signatures that todo!() for now.
Define the seams: FrameCodec, HpackEncoder/HpackDecoder, a LoadBalancer trait, a ConnectionPool interface. Designing these interfaces before the byte-level work forces the ownership/data-flow decisions while they're still cheap to change.

3. Differential-test harness against h2 (the safety net)

Stand up the round-trip scaffolding now: your codec encodes → h2 decodes (and vice versa) → assert equality. Write one passing case (a SETTINGS frame) to prove it end-to-end, so next week every frame type you implement is instantly checkable.
Scaffold a cargo-fuzz target for the parser and a proptest strategy that generates valid frames. This is the concrete payoff of the "h2 as oracle" ADR.

4. Local backend + dev loop

Run a real h2 upstream to develop against (a tiny hyper h2 server, or nginx/Caddy with h2), wrapped in docker-compose so docker compose up gives you a backend on a fixed port.
Add a justfile (or cargo aliases) for the run-proxy / run-backend / curl-through loop. Cheap ergonomics now save real friction over six weeks.

5. Observability + benchmark baseline

Wire tracing-subscriber with env-filter, and a /metrics Prometheus stub via metrics-exporter-prometheus (static values are fine) so the §7 hooks exist before there's data.
Build wrk2 from source (it is not wrk — that distinction is the whole §10.1 point) and write the load script + an output→CSV parser. Capture a baseline now: client → backend directly, no proxy. Every later optimization gets measured against that number. Write down the two profiles' concrete params (connections, target rate, duration, payload sizes).

6. Prove the AWS pipeline (de-risk the part that bites in week 8)

Account basics first: a deploy IAM role, region, CLI creds, and a billing/budget alarm so a forgotten instance can't surprise you.
cdk init app --language typescript, then a skeleton stack: VPC, one Graviton instance (c7g/c8g), security group, and an internet-facing NLB with a TCP listener — and actually deploy the week-1 TLS+ALPN echo behind it. Confirm you can reach it from the internet and that the NLB passes TCP through untouched. One real deploy in week 2 turns week 8 into tuning instead of firefighting. cdk destroy when done so it costs almost nothing.
Write the multi-stage Dockerfile now (rust build stage → static musl binary → distroless/scratch), targeting aarch64, and get it building locally even with the trivial binary. Record the jemalloc-swap and self-signed-cert decisions (§9.2–9.3); you can defer wiring jemalloc until you're benchmarking.

7. Roadmap for weeks 3–8

Commit a one-paragraph roadmap to the README so scope stays bounded: W3 framing codec + preface + SETTINGS handshake; W4 HPACK (static/dynamic/Huffman); W5 stream state machine + multiplexing + flow control; W6 backpressure bridging + pool + LB; W7 resilience + security hardening; W8 full deploy + load test + tuning + writeup. This also sanity-checks that weeks 1–2 set up the right things.

End of week 2: accept-loop + graceful-shutdown skeleton running; core types/traits stubbed; one passing differential round-trip; dockerized backend + smooth dev loop; tracing/metrics plumbing live; wrk2 installed with a captured baseline; CDK deploying a trivial service reachable through an NLB; aarch64 Dockerfile building; 8-week roadmap committed.
