//! The blocking half of the week-5 milestone: a sender that exhausts its
//! flow-control window provably stops, and provably resumes on WINDOW_UPDATE.
//!
//! These drive the connection with a hand-written peer rather than the `h2`
//! client, because the whole point is to behave *badly*: withhold a
//! WINDOW_UPDATE that a well-behaved client would send immediately, and check
//! that the server sits there rather than sending octets it has no credit for.
//! `h2` will not do that, and a test that cannot starve the server cannot prove
//! the server stops.
//!
//! "Stops" is asserted as silence — a bounded read that times out with nothing
//! in it. That is the only direct evidence available: a server that ignored flow
//! control would have written the rest of the body during that window.

use std::collections::HashMap;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::broadcast;

use h2proxy_core::conn::{Connection, PREFACE, Settings, Tuning, setting_id};
use h2proxy_core::flow::{CONNECTION_WINDOW, DEFAULT_INITIAL_WINDOW_SIZE};
use h2proxy_core::frame::{Frame, FrameCodec, MAX_ALLOWED_FRAME_SIZE};
use h2proxy_core::hpack::{Header, HpackEncoder};
use h2proxy_core::stream::StreamId;

const TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for the *first* frame of a round before concluding the
/// server has stopped. Generous, because concluding "it stalled" when it was
/// merely descheduled behind the rest of the suite is the one way these tests
/// can lie.
const FIRST_FRAME: Duration = Duration::from_secs(2);

/// How long to wait between frames once the server is clearly writing. A server
/// mid-response does not pause for this long, so a gap means it ran out of
/// window.
const GAP: Duration = Duration::from_millis(200);

/// A scripted client that speaks frames directly, with no flow-control manners.
struct Peer {
    io: DuplexStream,
    codec: FrameCodec,
    buf: BytesMut,
    hpack: HpackEncoder,
}

impl Peer {
    fn new(io: DuplexStream) -> Peer {
        Peer {
            io,
            codec: FrameCodec::new(MAX_ALLOWED_FRAME_SIZE),
            buf: BytesMut::new(),
            hpack: HpackEncoder::new(Settings::default().header_table_size as usize),
        }
    }

    async fn send(&mut self, frame: &Frame) {
        let mut out = BytesMut::new();
        self.codec.encode(frame, &mut out).expect("encode");
        self.io.write_all(&out).await.expect("write");
        self.io.flush().await.expect("flush");
    }

    /// Preface + SETTINGS, then swallow the server's SETTINGS, its stream-0
    /// WINDOW_UPDATE, and its ACK. Returns the bootstrap increment so a test can
    /// assert on it.
    async fn handshake(&mut self, initial_window: u32) -> u32 {
        self.io.write_all(PREFACE).await.expect("preface");
        self.send(&Frame::Settings {
            ack: false,
            params: vec![(setting_id::INITIAL_WINDOW_SIZE, initial_window)],
        })
        .await;

        assert!(matches!(
            self.recv().await,
            Frame::Settings { ack: false, .. }
        ));
        let Frame::WindowUpdate {
            stream_id,
            increment,
        } = self.recv().await
        else {
            panic!("the server must raise the connection window at handshake");
        };
        assert!(stream_id.is_connection());
        assert!(matches!(
            self.recv().await,
            Frame::Settings { ack: true, .. }
        ));
        increment
    }

    /// A bodyless GET for `path`, on `stream_id`.
    async fn request(&mut self, stream_id: StreamId, path: &str) {
        let mut block = BytesMut::new();
        self.hpack.encode(
            &[
                Header::new(":method", "GET"),
                Header::new(":scheme", "https"),
                Header::new(":authority", "example.com"),
                Header::new(":path", path.to_owned()),
            ],
            &mut block,
        );
        self.send(&Frame::Headers {
            stream_id,
            block: block.freeze(),
            end_stream: true,
            end_headers: true,
        })
        .await;
    }

    async fn recv(&mut self) -> Frame {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(frame) = self.codec.decode(&mut self.buf).expect("decode") {
                    return frame;
                }
                let n = self.io.read_buf(&mut self.buf).await.expect("read");
                assert!(n > 0, "the server closed the connection unexpectedly");
            }
        })
        .await
        .expect("timed out waiting for a frame")
    }

    /// Read DATA until the server goes quiet, returning the octets and
    /// END_STREAM flag seen per stream.
    ///
    /// The silence is the measurement. A server that ignored flow control would
    /// have written the rest of the body inside that window, so an empty result
    /// is positive evidence that it stopped rather than merely being slow.
    async fn drain(&mut self) -> Drained {
        let mut drained = Drained::default();
        loop {
            // Patient before the first frame, brisk after it: a server that has
            // started writing does not pause mid-response, but one that has not
            // started yet may just be waiting its turn on a loaded machine.
            let patience = if drained.total == 0 { FIRST_FRAME } else { GAP };
            let next = tokio::time::timeout(patience, async {
                loop {
                    if let Some(frame) = self.codec.decode(&mut self.buf).expect("decode") {
                        return Some(frame);
                    }
                    let n = self.io.read_buf(&mut self.buf).await.expect("read");
                    if n == 0 {
                        return None;
                    }
                }
            })
            .await;

            match next {
                // Quiet: either out of window or finished. Which one is the
                // assertion the caller makes.
                Err(_) | Ok(None) => return drained,
                Ok(Some(Frame::Data {
                    stream_id,
                    data,
                    end_stream,
                })) => {
                    let entry = drained.per_stream.entry(stream_id).or_default();
                    entry.0 += data.len();
                    entry.1 |= end_stream;
                    entry.2 += 1;
                    drained.total += data.len();
                }
                // HEADERS and the control frames are not flow-controlled, so
                // they are not evidence either way.
                Ok(Some(_)) => {}
            }
        }
    }
}

/// DATA seen during one [`Peer::drain`], keyed by stream.
#[derive(Default, Debug)]
struct Drained {
    total: usize,
    /// Per stream: octets, whether END_STREAM was seen, and how many DATA
    /// frames carried it.
    per_stream: HashMap<StreamId, (usize, bool, usize)>,
}

impl Drained {
    fn octets(&self, stream: StreamId) -> usize {
        self.per_stream.get(&stream).map_or(0, |e| e.0)
    }

    fn ended(&self, stream: StreamId) -> bool {
        self.per_stream.get(&stream).is_some_and(|e| e.1)
    }

    fn frames(&self, stream: StreamId) -> usize {
        self.per_stream.get(&stream).map_or(0, |e| e.2)
    }
}

/// Start a connection and return the scripted peer. The shutdown sender is
/// returned only to keep it alive: dropping it would signal a drain.
fn start() -> (Peer, broadcast::Sender<()>) {
    let (client, server) = tokio::io::duplex(1 << 20);
    let (tx, rx) = broadcast::channel::<()>(1);
    tokio::spawn(Connection::new(server, rx).run());
    (Peer::new(client), tx)
}

/// As [`start`], with the flow-control sizes a deployment might tune to.
fn start_tuned(tuning: Tuning) -> (Peer, broadcast::Sender<()>) {
    let (client, server) = tokio::io::duplex(1 << 20);
    let (tx, rx) = broadcast::channel::<()>(1);
    tokio::spawn(
        Connection::with_service(
            server,
            rx,
            tuning.server_settings(),
            h2proxy_core::service::Echo::new(64),
        )
        .with_connection_window(tuning.connection_window)
        .run(),
    );
    (Peer::new(client), tx)
}

#[tokio::test]
async fn the_connection_window_is_raised_at_handshake() {
    // §6.9.1: SETTINGS cannot move the connection window, only WINDOW_UPDATE
    // can. Forgetting this caps throughput at 64 KiB with nothing in the logs
    // to explain it, which is why it gets its own test rather than being left
    // implicit in the ones below.
    let (mut peer, _tx) = start();
    let increment = peer.handshake(1 << 20).await;
    assert_eq!(
        increment as i32,
        CONNECTION_WINDOW - DEFAULT_INITIAL_WINDOW_SIZE,
        "the bootstrap must lift the default window to CONNECTION_WINDOW",
    );
}

#[tokio::test]
async fn tuning_the_connection_window_moves_the_bootstrap_with_it() {
    // The hazard this exists for: the window and the increment that opens it
    // are two numbers that must agree, and nothing forces them to. Send the
    // *constant* increment beside a *tuned* window and the peer is credited an
    // amount we never reserved — which surfaces much later, under load, as a
    // flow-control error nobody can trace back to a config change.
    //
    // Tuning that silently does nothing is the other failure here, and it is the
    // one this project keeps finding: a feature that runs, passes its tests and
    // reports nothing.
    const TUNED: i32 = 4 * 1024 * 1024;
    let tuning = Tuning {
        connection_window: TUNED,
        stream_window: 512 * 1024,
        max_concurrent_streams: 32,
    };

    let (mut peer, _tx) = start_tuned(tuning);
    let increment = peer.handshake(1 << 20).await;
    assert_eq!(
        increment as i32,
        TUNED - DEFAULT_INITIAL_WINDOW_SIZE,
        "the bootstrap must open exactly the window that was configured",
    );
    assert_ne!(
        increment as i32,
        CONNECTION_WINDOW - DEFAULT_INITIAL_WINDOW_SIZE,
        "and must not be the default increment in disguise",
    );
}

#[tokio::test]
async fn tuning_reaches_the_settings_the_peer_actually_receives() {
    // The other half: the stream window and concurrency travel in SETTINGS, so
    // a peer can be asked what it was told. Without this, `Tuning` could be
    // plumbed everywhere except into the frame and every test above would still
    // pass.
    let tuning = Tuning {
        connection_window: 2 * 1024 * 1024,
        stream_window: 128 * 1024,
        max_concurrent_streams: 17,
    };
    let (client, server) = tokio::io::duplex(1 << 20);
    let (_tx, rx) = broadcast::channel::<()>(1);
    tokio::spawn(
        Connection::with_service(
            server,
            rx,
            tuning.server_settings(),
            h2proxy_core::service::Echo::new(64),
        )
        .with_connection_window(tuning.connection_window)
        .run(),
    );

    let mut peer = Peer::new(client);
    peer.io.write_all(PREFACE).await.expect("preface");
    peer.send(&Frame::Settings {
        ack: false,
        params: vec![],
    })
    .await;

    let Frame::Settings { ack: false, params } = peer.recv().await else {
        panic!("the server opens with its SETTINGS");
    };
    let value = |id: u16| params.iter().find(|(k, _)| *k == id).map(|(_, v)| *v);
    assert_eq!(
        value(setting_id::INITIAL_WINDOW_SIZE),
        Some(tuning.stream_window as u32),
    );
    assert_eq!(
        value(setting_id::MAX_CONCURRENT_STREAMS),
        Some(tuning.max_concurrent_streams),
    );
}

#[tokio::test]
async fn a_stream_stops_at_its_window_and_resumes_on_a_window_update() {
    // A tiny per-stream window and a generous connection one, so the *stream*
    // window is unambiguously what stops the response.
    const WINDOW: usize = 1024;
    const BODY: usize = 8 * 1024;

    let (mut peer, _tx) = start();
    peer.handshake(WINDOW as u32).await;
    let stream = StreamId::new(1);
    peer.request(stream, &format!("/bytes/{BODY}")).await;

    // Exactly one window's worth, then silence — not one octet more.
    let first = peer.drain().await;
    assert_eq!(
        first.octets(stream),
        WINDOW,
        "the server sent {} octets against a {WINDOW}-octet window",
        first.octets(stream),
    );
    assert!(!first.ended(stream), "the response cannot be complete yet");

    // Each credit releases exactly that much and no more. The loop is the point:
    // a server that checked its window once would pass a single round and fail
    // here.
    let mut total = first.octets(stream);
    while total < BODY {
        let grant = WINDOW.min(BODY - total);
        peer.send(&Frame::WindowUpdate {
            stream_id: stream,
            increment: grant as u32,
        })
        .await;
        let round = peer.drain().await;
        assert_eq!(
            round.octets(stream),
            grant,
            "after crediting {grant} octets, at {total}/{BODY}",
        );
        total += round.octets(stream);
        assert_eq!(
            round.ended(stream),
            total == BODY,
            "END_STREAM must arrive with the last octet and not before",
        );
    }
    assert_eq!(total, BODY);
}

#[tokio::test]
async fn end_stream_is_sent_exactly_once() {
    // Regression: the scheduler cleared its send queue but not its END_STREAM
    // flag, so the stream stayed "ready", got a second visit, and emitted an
    // extra empty DATA with END_STREAM. Every h2 client this project tests
    // against sends END_STREAM on the request, which retires the stream before
    // that second visit — so the bug was invisible until h2spec left a stream
    // open. This request deliberately does the same: no END_STREAM.
    let (mut peer, _tx) = start();
    peer.handshake(1 << 20).await;

    let stream = StreamId::new(1);
    let mut block = BytesMut::new();
    peer.hpack.encode(
        &[
            Header::new(":method", "POST"),
            Header::new(":scheme", "https"),
            Header::new(":authority", "example.com"),
            Header::new(":path", "/bytes/64"),
        ],
        &mut block,
    );
    peer.send(&Frame::Headers {
        stream_id: stream,
        block: block.freeze(),
        end_stream: false,
        end_headers: true,
    })
    .await;

    let drained = peer.drain().await;
    assert_eq!(drained.octets(stream), 64, "the whole body, once");
    assert!(drained.ended(stream), "END_STREAM must arrive");
    assert_eq!(
        drained.frames(stream),
        1,
        "one DATA frame carried the body and its END_STREAM; a second frame \
         means END_STREAM was sent twice",
    );
}

#[tokio::test]
async fn the_connection_window_binds_across_streams() {
    // The hazard the RFC notes call out: replenishing stream windows while
    // forgetting the connection window stalls every stream at once. Here the
    // per-stream windows are enormous and the *connection* window is the scarce
    // one, so what must be capped is the total across streams — a server that
    // only tracked stream windows would sail past it.
    const STREAM_WINDOW: u32 = 1 << 20;
    const BODY: usize = 200 * 1024;

    let (mut peer, _tx) = start();
    peer.handshake(STREAM_WINDOW).await;

    // We never credit the connection window, so it stays at the protocol
    // default however large the stream windows are.
    let big = StreamId::new(1);
    peer.request(big, &format!("/bytes/{BODY}")).await;
    let first = peer.drain().await;
    assert_eq!(
        first.total, DEFAULT_INITIAL_WINDOW_SIZE as usize,
        "the connection window bounds the total, not the {STREAM_WINDOW}-octet stream window",
    );
    assert!(!first.ended(big));

    // A second stream, with a completely fresh 1 MiB window of its own, gets
    // nothing: the credit it would spend is connection-level and already gone.
    let small = StreamId::new(3);
    peer.request(small, "/bytes/4096").await;
    let starved = peer.drain().await;
    assert_eq!(
        starved.total, 0,
        "a fresh stream must not spend connection credit that is exhausted; \
         got {starved:?}",
    );

    // Crediting the connection — and only the connection — releases exactly that
    // much and not an octet more.
    const GRANT: usize = 4096;
    let mut seen: HashMap<StreamId, usize> = HashMap::new();
    for round in 0..4 {
        peer.send(&Frame::WindowUpdate {
            stream_id: StreamId::CONNECTION,
            increment: GRANT as u32,
        })
        .await;
        let freed = peer.drain().await;
        assert_eq!(
            freed.total, GRANT,
            "round {round}: the connection credit is exactly what both streams draw on",
        );
        for (id, (octets, _, _)) in freed.per_stream {
            *seen.entry(id).or_default() += octets;
        }
    }

    // Both streams got some of it. Not necessarily in the same round — a credit
    // smaller than one `SEND_BUDGET` visit is taken whole by whichever stream is
    // at the front of the ring — but the loser rotates to the back, so over
    // successive credits neither can be starved. That rotation *is* the fairness
    // guarantee; a scheduler without it would leave stream 3 at zero forever.
    assert_eq!(
        seen.len(),
        2,
        "both streams must make progress across successive credits: {seen:?}",
    );
}
