#![no_main]
//! Fuzz the streaming frame parser with wholly unconstrained input.
//!
//! Week 2 steered the fuzzer's bytes onto the SETTINGS path, because every other
//! frame type was still a `todo!()`. Now that all eight decode, the shaping is
//! gone: these are raw octets straight off a hostile socket, which is exactly
//! what the parser must survive on a public listener.
//!
//! The contract under test is total: for *any* input, `decode` returns `Err` for
//! a malformed frame, `Ok(None)` when more bytes are needed, and `Ok(Some(..))`
//! otherwise. It must never panic — no slice-index overrun on a truncated
//! payload, no arithmetic overflow on an attacker-chosen 24-bit length or pad
//! length. A panic here is a remote denial of service.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use h2proxy_core::frame::{FrameCodec, MAX_ALLOWED_FRAME_SIZE};

fuzz_target!(|data: &[u8]| {
    // The largest limit the wire format allows, so nothing is rejected early as
    // oversized and the per-type parsers see the most input.
    let mut codec = FrameCodec::new(MAX_ALLOWED_FRAME_SIZE);
    let mut buf = BytesMut::from(data);

    // Drain the buffer the way a connection does: keep decoding until the codec
    // asks for more bytes or rejects the stream. This exercises the reassembly
    // loop and the unknown-frame skip path, not just a single frame.
    loop {
        match codec.decode(&mut buf) {
            Ok(Some(_frame)) => continue,
            Ok(None) | Err(_) => break,
        }
    }
});
