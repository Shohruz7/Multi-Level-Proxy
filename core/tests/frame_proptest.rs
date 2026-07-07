//! Property tests for the frame codec: encode → decode is the identity, over
//! randomly generated frames. Seeded now with SETTINGS (the one implemented
//! frame); every week-3 frame type adds its own generator here.

use bytes::BytesMut;
use proptest::prelude::*;

use h2proxy_core::frame::{Frame, FrameCodec};

/// Up to 31 arbitrary id/value pairs — one shy of the 32 that would still fit
/// well under the default 16,384-octet max frame size.
fn settings_params() -> impl Strategy<Value = Vec<(u16, u32)>> {
    proptest::collection::vec((any::<u16>(), any::<u32>()), 0..32)
}

proptest! {
    #[test]
    fn settings_frames_round_trip(ack in any::<bool>(), params in settings_params()) {
        // A SETTINGS ACK carries no parameters on the wire.
        let params = if ack { Vec::new() } else { params };
        let frame = Frame::Settings { ack, params };

        let mut codec = FrameCodec::new(16_384);
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).expect("encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("decode")
            .expect("a complete frame");
        prop_assert_eq!(decoded, frame);
        prop_assert!(buf.is_empty(), "the whole frame should be consumed");
    }
}
