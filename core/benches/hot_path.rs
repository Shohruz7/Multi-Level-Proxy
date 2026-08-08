//! What week 7's additions cost on the per-frame and per-request path.
//!
//! Week 7 put an abuse guard in front of every frame and a histogram write on
//! every request, and "it costs nothing" is a claim like any other. These are
//! the numbers behind it. The gates, checked by eye against the criterion
//! output rather than asserted (a benchmark that fails CI on a noisy laptop is
//! a benchmark nobody runs):
//!
//! - `guard/*` under **20 ns** per call, and under 1% of `frame_dispatch`.
//! - `span_disabled` within noise of `span_none` — the per-stream span is at
//!   `debug`, so at the default `info` filter it must be free.
//! - `histogram_record` under 50 ns; it happens once per request, against a
//!   request that costs microseconds.
//!
//! If the guard cannot hit its number, the token bucket becomes a plain
//! saturating counter with a coarser reset. The mitigation is what matters; the
//! elegance of its arithmetic is not.

use std::time::Duration;

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use h2proxy_core::frame::{Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};
use h2proxy_core::guard::{Guard, Limits};
use h2proxy_core::proxy::ProxyStats;
use h2proxy_core::stream::StreamId;
use std::hint::black_box;

fn guard_signals(c: &mut Criterion) {
    let mut group = c.benchmark_group("guard");
    let base = tokio::time::Instant::now();

    group.bench_function("reset_answered", |b| {
        // Permissive limits so the bucket never empties: the cost being measured
        // is the common path, not the trip.
        let mut guard = Guard::new(Limits::permissive(), base);
        let mut tick = 0u64;
        b.iter(|| {
            tick += 1;
            let now = base + Duration::from_micros(tick);
            black_box(guard.on_reset(black_box(true), now))
        });
    });

    group.bench_function("control_frame", |b| {
        let mut guard = Guard::new(Limits::permissive(), base);
        let mut tick = 0u64;
        b.iter(|| {
            tick += 1;
            let now = base + Duration::from_micros(tick);
            black_box(guard.on_control_frame(now))
        });
    });

    group.bench_function("data", |b| {
        // The hottest of the three by far: every DATA frame on every stream.
        let mut guard = Guard::new(Limits::permissive(), base);
        b.iter(|| black_box(guard.on_data(black_box(16_384), black_box(false))));
    });

    group.finish();
}

fn frame_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_dispatch");

    // A 16 KiB DATA frame, the shape that dominates a proxy's frame mix.
    let mut encoded = BytesMut::new();
    FrameCodec::new(MAX_ALLOWED_FRAME_SIZE)
        .encode(
            &Frame::Data {
                stream_id: StreamId::new(1),
                data: bytes::Bytes::from(vec![0u8; 16 * 1024]),
                end_stream: false,
            },
            &mut encoded,
        )
        .expect("encode");
    let encoded = encoded.freeze();

    group.bench_function("decode_only", |b| {
        let mut codec = FrameCodec::new(MAX_ALLOWED_FRAME_SIZE);
        b.iter(|| {
            let mut buf = BytesMut::from(&encoded[..]);
            black_box(codec.decode(&mut buf))
        });
    });

    group.bench_function("decode_and_guard", |b| {
        // The same work with the guard call the connection loop actually makes,
        // so the difference between the two lines *is* the guard's share.
        let mut codec = FrameCodec::new(MAX_ALLOWED_FRAME_SIZE);
        let mut guard = Guard::new(Limits::default(), tokio::time::Instant::now());
        b.iter(|| {
            let mut buf = BytesMut::from(&encoded[..]);
            let frame = codec.decode(&mut buf);
            if let Ok(Some(Frame::Data {
                data, end_stream, ..
            })) = &frame
            {
                let _ = black_box(guard.on_data(data.len(), *end_stream));
            }
            black_box(frame)
        });
    });

    group.finish();
}

fn observability(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability");

    group.bench_function("histogram_record", |b| {
        let stats = ProxyStats::default();
        b.iter(|| stats.observe_latency(black_box(Duration::from_micros(850))));
    });

    group.bench_function("span_none", |b| {
        b.iter(|| black_box(StreamId::new(1)));
    });

    group.bench_function("span_disabled", |b| {
        // No subscriber is installed, so this is the "filtered out" cost — what
        // the per-stream span costs in production at the default `info` filter.
        b.iter(|| black_box(tracing::debug_span!("stream", id = 1u32)));
    });

    group.finish();
}

criterion_group!(benches, guard_signals, frame_dispatch, observability);
criterion_main!(benches);
