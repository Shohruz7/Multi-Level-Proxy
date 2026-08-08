#![no_main]
//! The abuse guard's contract is total: any sequence of signals, at any timing,
//! yields a verdict — never a panic, an overflow, or a NaN.
//!
//! Worth fuzzing despite being "just counters", because the arithmetic is the
//! part most likely to be wrong: the bucket does floating-point refill against a
//! caller-supplied clock, the thresholds may be zero or infinite, and the
//! counters saturate. A panic here would be reachable from the wire, since every
//! input is peer-controlled — the frame types, their order, and (through network
//! timing) the intervals between them.

use libfuzzer_sys::fuzz_target;

use h2proxy_core::guard::{Guard, Limits};

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    // The first octets choose the policy, so the fuzzer explores degenerate
    // limits — zero rates, zero bursts, zero caps — as well as ordinary ones.
    let limits = Limits {
        reset_burst: f64::from(data[0]),
        reset_rate: f64::from(data[1]),
        unanswered_burst: f64::from(data[0] / 2),
        unanswered_rate: f64::from(data[1] / 2),
        control_burst: f64::from(data[2]),
        control_rate: f64::from(data[2]),
        max_consecutive_empty: u32::from(data[3]),
        max_continuations: u32::from(data[3]),
        observe_only: data[0] & 1 == 0,
    };

    let base = tokio::time::Instant::now();
    let mut guard = Guard::new(limits, base);
    let mut now = base;

    // Each remaining octet is one event: the low bits pick the signal, the high
    // bits advance the clock. Time only moves forward, matching `Instant`'s
    // contract — the backwards case is covered by a unit test, which can state
    // the expectation instead of merely surviving it.
    for byte in &data[4..] {
        now += std::time::Duration::from_millis(u64::from(byte >> 3));
        match byte & 0b111 {
            0 => drop(guard.on_reset(true, now)),
            1 => drop(guard.on_reset(false, now)),
            2 => drop(guard.on_control_frame(now)),
            3 => drop(guard.on_data(0, false)),
            4 => drop(guard.on_data(usize::from(*byte), false)),
            5 => drop(guard.on_data(0, true)),
            6 => drop(guard.on_continuation()),
            _ => guard.on_header_block_end(),
        }
    }

    // The peaks are what the calibration run reads, so they have to be finite
    // however the counters were driven — a NaN here would silently poison the
    // gauge and the threshold derived from it.
    let peaks = guard.peaks(now);
    assert!(peaks.resets_per_sec.is_finite());
    assert!(peaks.unanswered_per_sec.is_finite());
    assert!(peaks.control_per_sec.is_finite());
});
