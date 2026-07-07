#![no_main]
//! Fuzz the streaming frame parser.
//!
//! Only SETTINGS is implemented for now (other frame types are week-3
//! `todo!()`s), so we steer the fuzzer's bytes onto the SETTINGS path: force the
//! type octet to `0x4`, clear the ACK flag, and zero the stream id. Everything
//! that actually exercises the parser — the 24-bit length, the reassembly cutoff
//! when input is short, and the 6-octet id/value splitting — stays fully
//! fuzzer-controlled. Week 3 drops the shaping as each frame type lands.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use h2proxy_core::frame::{FRAME_HEADER_LEN, FrameCodec};

fuzz_target!(|data: &[u8]| {
    if data.len() < FRAME_HEADER_LEN {
        return;
    }
    let mut buf = BytesMut::from(data);
    buf[3] = 0x04; // type = SETTINGS
    buf[4] &= !0x01; // clear ACK so non-empty payloads are parsed
    buf[5..9].fill(0); // stream id = 0 (SETTINGS rides the connection stream)

    let mut codec = FrameCodec::new(16_384);
    // Must never panic: malformed lengths/payloads are `Err`, short input is
    // `Ok(None)`. A panic here is a real parser bug.
    let _ = codec.decode(&mut buf);
});
