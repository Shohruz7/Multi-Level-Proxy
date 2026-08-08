//! The upstream leg: one HTTP/2 connection to a backend, where *we* are the
//! client (design doc §4.3).
//!
//! The mirror image of [`crate::conn`], built from the same parts — the same
//! [`FrameCodec`], the same HPACK codecs, the same [`StreamTable`], the same
//! two-level windows. Only the role differs, and the role is exactly four
//! things:
//!
//! 1. **We send the preface.** Our SETTINGS follows it rather than preceding it.
//! 2. **The ids are ours, and this task alone allocates them.** [`StreamTable::
//!    open_local`] instead of `open_peer`, and the peer's
//!    `MAX_CONCURRENT_STREAMS` is a budget we obey rather than one we impose.
//!    Clients name their work with a [`RequestId`] from the pool and learn
//!    nothing about stream ids, because §5.1.1 requires ids to increase *in the
//!    order they reach the wire* — and several client connections sharing this
//!    one have no ordering between them. Allocating here is the only place that
//!    ordering exists.
//! 3. **HEADERS mean the other thing.** Outbound they are a request; inbound
//!    they are a response, or — after a response — a trailer section.
//! 4. **Nothing here decides anything.** A client connection asked for this
//!    work and a client connection gets the answer; this module translates
//!    between two id spaces and two flow-control regimes and holds no policy.
//!
//! # The connection is a task, and that is the whole point
//!
//! Each pooled upstream connection runs [`UpstreamConnection::run`] on its own
//! task and is reached only by [`ToUpstream`] messages (ADR 0015). That is what
//! lets *many* client connections share *few* upstream ones — the coalescing
//! this project is about. A per-client upstream would need no channels at all
//! and would also make coalescing impossible.
//!
//! # The bridge (§4.2, ADR 0016)
//!
//! This side never calls [`RecvWindow::release`] on its own initiative. Response
//! octets are recorded on arrival, forwarded to the client connection, and the
//! credit is returned only when a [`ToUpstream::Released`] says those octets
//! reached the client. A backend faster than the client therefore runs out of
//! window and stops, holding at most one connection window (1 MiB) of data in
//! this process — no matter how large the response or how slow the client.
//!
//! The request direction is the same trick pointed the other way: the client's
//! credit comes back through [`ServiceEvent::BodyAccepted`] only once we have
//! actually spent our send window on those octets.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, trace, warn};

use crate::conn::{
    ConnectionError, ErrorCode, MAX_HEADER_LIST_SIZE, MAX_WRITE_QUEUE, PREFACE, Settings, Tuning,
};
use crate::flow::{RecvWindow, SEND_BUDGET, Window};
use crate::frame::{Decoded, Frame, FrameCodec, FrameType};
use crate::hpack::{Header, HpackDecoder, HpackEncoder, HpackError};
use crate::service::{
    Events, RequestHead, Response, ResponseHead, ServiceEvent, validate_trailers,
};
use crate::stream::{Lookup, StreamEvent, StreamId, StreamTable};

/// The largest compressed header block we will reassemble from a backend, the
/// same bound the client side applies to a client.
const MAX_HEADER_BLOCK_BYTES: usize = MAX_HEADER_LIST_SIZE as usize;

/// How long to let a backend finish the streams it promised after sending
/// GOAWAY, before giving up on them.
///
/// Shorter than the client-facing drain deadline on purpose: the client is
/// waiting on us for all of it, and a backend that has said goodbye and then
/// stops answering should cost one slow request rather than one very slow one.
const PEER_DRAIN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// Names one request on one upstream connection, from the moment the pool leases
/// a slot until the stream ends.
///
/// Deliberately *not* a stream id: the id does not exist until this connection
/// task sends the HEADERS. A client can queue body octets behind a request the
/// task has not looked at yet, and they still find their stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct RequestId(u32);

impl RequestId {
    pub const fn new(raw: u32) -> Self {
        RequestId(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// What a client connection asks of an upstream connection.
///
/// `id` is the [`RequestId`] the pool leased. Messages for one request always
/// arrive in order, because they all come from that request's client connection
/// task over one channel.
#[derive(Debug)]
pub enum ToUpstream {
    /// Start a request. The stream id is chosen when this is handled.
    Request {
        id: RequestId,
        client_id: StreamId,
        head: Box<RequestHead>,
        end_stream: bool,
        events: Events,
    },
    /// Request body octets.
    Body {
        id: RequestId,
        data: Bytes,
        end_stream: bool,
    },
    /// A request trailer section, which ends the request.
    Trailers { id: RequestId, fields: Vec<Header> },
    /// `n` octets of the response reached the client, so the backend may send
    /// `n` more. The response-direction half of the bridge.
    Released { id: RequestId, n: u32 },
    /// The client abandoned this stream.
    Cancel { id: RequestId, code: ErrorCode },
}

/// A handle to one pooled upstream connection.
///
/// Cloneable and cheap: it is a channel sender plus the shared counters the
/// pool and the load balancer read without taking a lock.
#[derive(Clone, Debug)]
pub struct UpstreamHandle {
    tx: mpsc::UnboundedSender<ToUpstream>,
}

impl UpstreamHandle {
    /// Send a message to the connection task. `false` if the task is gone,
    /// which the pool treats as "this connection is dead, open another".
    pub fn send(&self, msg: ToUpstream) -> bool {
        self.tx.send(msg).is_ok()
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// A request waiting for a stream slot on this connection.
///
/// The backend's `MAX_CONCURRENT_STREAMS` is a hard limit, and the alternative
/// to waiting for it is refusing the request — which turns a busy backend into
/// client-visible errors. Body octets that arrive meanwhile are buffered here,
/// and bounded by the same thing everything else is: no `BodyAccepted` goes back
/// until they are actually sent, so the client's own window stops it from
/// queueing more than one window's worth (§4.2).
struct Pending {
    request: RequestId,
    client_id: StreamId,
    head: Box<RequestHead>,
    end_stream: bool,
    events: Events,
    body: VecDeque<(Bytes, bool)>,
    trailers: Option<Vec<Header>>,
}

/// What one client stream is doing on this connection.
struct Route {
    /// The id to address the client connection with. Every [`ServiceEvent`] we
    /// emit is translated into this id, which is what spares the client
    /// connection a reverse map (§4.3).
    client_id: StreamId,
    events: Events,
    /// What the pool and the client call this request, so the reverse mapping
    /// can be cleaned up when the stream ends.
    request: RequestId,
    /// Whether the response head has already gone to the client. Decides
    /// whether an inbound HEADERS is a response or a trailer section, and
    /// whether a failure can still be reported as a status.
    head_sent: bool,
    /// Response octets recorded against the connection window that the client
    /// has not confirmed yet. Handed back when the stream dies, or the window
    /// shrinks a little on every cancelled download.
    pending_conn_release: u32,
}

/// What a finished upstream connection did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct UpstreamSummary {
    pub handshake_completed: bool,
    pub requests_sent: u64,
    pub responses_received: u64,
    pub streams_reset: u64,
    pub data_bytes_received: u64,
    /// PINGs sent to prove the backend was still there.
    pub probes_sent: u64,
    /// Set when a probe went unanswered and this connection was closed for it.
    ///
    /// The pool reads this to report a *backend* failure to [`crate::health`].
    /// Without that report the probe would only recycle a socket, which is not
    /// health checking — it is the same shape as the week-7 bug where a feature
    /// ran, detected the failure, and told nobody.
    pub probe_timed_out: bool,
}

/// Active liveness probing for one upstream connection (design doc §5.2).
///
/// A backend can accept TCP, complete a handshake, and then answer nothing. No
/// passive check sees that: the requests on it do not fail, they *hang*, and a
/// hang is the one failure mode a proxy must never produce. A PING has an
/// answer obliged by §6.7, so an unanswered one is evidence rather than
/// inference.
///
/// **Quiet, not empty** — the same distinction the client-side drain had to
/// learn (ADR 0018). Probing only connections with no streams in flight would
/// skip exactly the case that matters, because a black-holed backend looks busy:
/// its streams are open, and none of them is ever going to finish.
struct Probe {
    /// How long the socket must be silent before a probe is worth sending. Zero
    /// disables probing entirely.
    idle: Duration,
    /// How long to wait for an answer before calling the backend dead.
    timeout: Duration,
    /// The last time the backend said anything at all.
    last_heard: Instant,
    /// The probe awaiting an answer.
    outstanding: Option<Outstanding>,
    next_nonce: u64,
}

/// A probe on the wire.
#[derive(Clone, Copy, Debug)]
struct Outstanding {
    /// The payload, so an ACK can be matched to the PING that asked for it.
    nonce: [u8; 8],
    /// When it went out. Anything heard *after* this answers it — see
    /// [`Probe::due`].
    sent_at: Instant,
    deadline: Instant,
}

/// What the probe timer wants done when it fires.
#[derive(PartialEq, Eq, Debug)]
enum ProbeAction {
    /// Not due yet, or probing is off.
    Nothing,
    /// Send a PING carrying this nonce.
    Send([u8; 8]),
    /// The backend never answered. Give up on the connection.
    Expired,
}

impl Probe {
    fn new(idle: Duration, timeout: Duration, now: Instant) -> Self {
        Probe {
            idle,
            timeout,
            last_heard: now,
            outstanding: None,
            next_nonce: 1,
        }
    }

    /// Probing off: the default for a connection nobody configured, so the
    /// differential harness and the one-off `connect` helper park on their
    /// sockets exactly as they did before.
    fn disabled() -> Self {
        Probe::new(Duration::ZERO, Duration::ZERO, Instant::now())
    }

    const fn enabled(&self) -> bool {
        !self.idle.is_zero()
    }

    /// Anything from the backend counts as liveness, so an ordinary busy
    /// connection never probes at all.
    fn heard(&mut self, now: Instant) {
        self.last_heard = now;
    }

    /// When the loop next needs waking for this. `None` parks it indefinitely.
    fn next_wake(&self) -> Option<Instant> {
        if !self.enabled() {
            return None;
        }
        match self.outstanding {
            Some(probe) => Some(probe.deadline),
            None => Some(self.last_heard + self.idle),
        }
    }

    fn due(&mut self, now: Instant) -> ProbeAction {
        if !self.enabled() {
            return ProbeAction::Nothing;
        }
        match self.outstanding {
            Some(probe) if now >= probe.deadline => {
                // Anything heard since the probe went out answers it, ACK or
                // not. The question being asked is "is this backend still
                // there", and a backend streaming a response has answered it —
                // killing that connection because a PING ACK was slow behind a
                // large write would be a false positive on a demonstrably live
                // peer, which is the one thing a health check must not produce.
                // What the probe really detects is *silence*.
                if self.last_heard > probe.sent_at {
                    self.outstanding = None;
                    return ProbeAction::Nothing;
                }
                ProbeAction::Expired
            }
            Some(_) => ProbeAction::Nothing,
            None if now.duration_since(self.last_heard) >= self.idle => {
                let nonce = self.next_nonce.to_be_bytes();
                self.next_nonce += 1;
                self.outstanding = Some(Outstanding {
                    nonce,
                    sent_at: now,
                    deadline: now + self.timeout,
                });
                ProbeAction::Send(nonce)
            }
            None => ProbeAction::Nothing,
        }
    }

    /// A PING ACK arrived. `true` if it was the one we were waiting for, which
    /// ends the probe early rather than waiting out its deadline.
    fn acked(&mut self, data: &[u8; 8], now: Instant) -> bool {
        self.heard(now);
        match self.outstanding {
            Some(probe) if probe.nonce == *data => {
                self.outstanding = None;
                true
            }
            _ => false,
        }
    }
}

/// Why the connection loop stopped.
enum Stop {
    /// The backend closed, or we ran out of work. Nothing to report.
    Quiet,
    /// A protocol violation on our side or theirs: GOAWAY, then close.
    Failed(ConnectionError),
}

/// One HTTP/2 connection to a backend, driven by a single task.
pub struct UpstreamConnection<IO> {
    reader: ReadHalf<IO>,
    writer: WriteHalf<IO>,
    out: BytesMut,
    read_buf: BytesMut,
    encode_buf: BytesMut,
    codec: FrameCodec,
    local_settings: Settings,
    peer_settings: Settings,
    open_header_block: Option<StreamId>,
    open_header_end_stream: bool,
    header_block: BytesMut,
    hpack_dec: HpackDecoder,
    hpack_enc: HpackEncoder,
    streams: StreamTable,
    conn_send_window: Window,
    conn_recv_window: RecvWindow,
    /// The size `conn_recv_window` was opened to; the handshake's stream-0
    /// WINDOW_UPDATE is derived from it rather than from the default constant.
    conn_window: i32,
    ready: VecDeque<StreamId>,
    /// Upstream stream id → the client stream waiting on it.
    routes: HashMap<StreamId, Route>,
    /// The pool's name for a request → the stream id we gave it. The §4.3
    /// remapping, and the only place the two id spaces meet.
    requests: HashMap<RequestId, StreamId>,
    /// Requests cancelled before their HEADERS went out.
    cancelled: std::collections::HashSet<RequestId>,
    /// Requests waiting for a stream slot, oldest first.
    pending: VecDeque<Pending>,
    /// The next id to open. Odd and strictly increasing, because we are the
    /// client here (§5.1.1).
    next_stream_id: u32,
    /// Set when the backend has sent GOAWAY: the last id it promised to
    /// process, and when we stop waiting for those to finish.
    ///
    /// A GOAWAY is a *drain request*, not a hang-up — §6.8 is explicit that
    /// streams at or below `last_stream_id` may still complete, and a backend
    /// restarting behind a rolling deploy relies on exactly that. Treating it as
    /// an immediate close is what turned every ordinary backend restart into a
    /// burst of 502s for requests the backend was still perfectly willing to
    /// answer.
    peer_draining: Option<(StreamId, tokio::time::Instant)>,
    /// Active liveness probing. Off unless the pool turned it on.
    probe: Probe,
    inbox: mpsc::UnboundedReceiver<ToUpstream>,
    stats: std::sync::Arc<crate::proxy::ProxyStats>,
    /// The pool's view of this connection, if it came from one. The peer's
    /// `MAX_CONCURRENT_STREAMS` is pushed here at handshake so the pool stops
    /// leasing past it — the one piece of connection state the pool cannot
    /// learn on its own.
    record: Option<std::sync::Arc<crate::pool::UpstreamRecord>>,
    summary: UpstreamSummary,
}

impl<IO: AsyncRead + AsyncWrite + Unpin + Send + 'static> UpstreamConnection<IO> {
    /// Build a connection over an established byte stream, reading its work
    /// from `inbox`.
    ///
    /// The inbox is passed in rather than created here because the pool needs
    /// the *sending* end before the socket exists: a checkout has to be able to
    /// hand out a lease while the TCP connect is still in flight, or every
    /// client stream that opens a new backend connection would block its whole
    /// connection task waiting for a handshake (ADR 0015).
    pub fn new(
        io: IO,
        inbox: mpsc::UnboundedReceiver<ToUpstream>,
        stats: std::sync::Arc<crate::proxy::ProxyStats>,
        record: Option<std::sync::Arc<crate::pool::UpstreamRecord>>,
    ) -> Self {
        Self::with_tuning(io, inbox, stats, record, Tuning::default())
    }

    /// As [`UpstreamConnection::new`], with the flow-control sizes the
    /// deployment tuned. The pool passes what the daemon measured; everything
    /// else takes the defaults.
    pub fn with_tuning(
        io: IO,
        inbox: mpsc::UnboundedReceiver<ToUpstream>,
        stats: std::sync::Arc<crate::proxy::ProxyStats>,
        record: Option<std::sync::Arc<crate::pool::UpstreamRecord>>,
        tuning: Tuning,
    ) -> Self {
        let local_settings = tuning.client_settings();
        let defaults = Settings::default();
        let (reader, writer) = tokio::io::split(io);
        UpstreamConnection {
            reader,
            writer,
            out: BytesMut::with_capacity(16 * 1024),
            read_buf: BytesMut::with_capacity(16 * 1024),
            encode_buf: BytesMut::with_capacity(1024),
            codec: FrameCodec::new(local_settings.max_frame_size),
            local_settings,
            peer_settings: defaults,
            open_header_block: None,
            open_header_end_stream: false,
            header_block: BytesMut::new(),
            hpack_dec: HpackDecoder::new(
                local_settings.header_table_size as usize,
                local_settings.max_header_list_size.map(|n| n as usize),
            ),
            hpack_enc: HpackEncoder::new(defaults.header_table_size as usize),
            streams: StreamTable::new(
                u32::MAX,
                defaults.initial_window_size as i32,
                local_settings.initial_window_size as i32,
            ),
            conn_send_window: Window::new(defaults.initial_window_size as i32),
            conn_recv_window: RecvWindow::new(tuning.connection_window),
            conn_window: tuning.connection_window,
            ready: VecDeque::new(),
            routes: HashMap::new(),
            requests: HashMap::new(),
            cancelled: std::collections::HashSet::new(),
            pending: VecDeque::new(),
            next_stream_id: 1,
            peer_draining: None,
            probe: Probe::disabled(),
            inbox,
            stats,
            record,
            summary: UpstreamSummary::default(),
        }
    }

    /// Prove the backend is still there when the socket goes quiet for `idle`,
    /// and give up on it if the PING is unanswered for `timeout`.
    ///
    /// Opt-in rather than on by default because only a *pooled* connection has
    /// anywhere to report the answer: the pool knows which backend this is, and
    /// the health table is what the report is for. A connection built directly —
    /// the differential harness, [`connect`] — has neither, and a probe it could
    /// not report would be a timer that only ever loses requests.
    #[must_use]
    pub fn with_probe(mut self, idle: Duration, timeout: Duration) -> Self {
        self.probe = Probe::new(idle, timeout, Instant::now());
        self
    }

    /// Run until the backend closes, every client has gone, or the protocol
    /// breaks.
    pub async fn run(mut self) -> UpstreamSummary {
        let stop = self.drive().await;
        if let Stop::Failed(err) = stop {
            warn!(code = ?err.code, reason = %err.debug, "upstream connection error; sending GOAWAY");
            let go_away = Frame::GoAway {
                last_stream_id: self.streams.highest_local_id(),
                error_code: err.code,
                debug_data: Bytes::from(err.debug.into_bytes()),
            };
            let _ = self.queue_frame(&go_away);
        }
        self.flush().await;
        // Whatever was in flight is not coming back. Every client still waiting
        // has to hear that now, or it waits for a response that will never
        // arrive — a hang is the one failure mode a proxy must never produce.
        self.fail_all_routes();
        // And the requests that never even got read: a connection can die
        // between a checkout and the connection task's next pass, leaving a
        // request sitting in the inbox with a client behind it. Dropping the
        // receiver would drop those messages silently, which looks like nothing
        // at all from the client's side — it just never hears back.
        self.inbox.close();
        while let Ok(msg) = self.inbox.try_recv() {
            if let ToUpstream::Request {
                client_id, events, ..
            } = msg
            {
                let _ = events.send(ServiceEvent::Gone { id: client_id });
            }
        }
        self.stats.close_connection();
        self.summary
    }

    /// The handshake, then the frame loop.
    async fn drive(&mut self) -> Stop {
        // Our half of the preface: 24 fixed octets, then SETTINGS (§3.4). This
        // is the one ordering the client role adds.
        self.out.extend_from_slice(PREFACE);
        let settings = self.local_settings.to_frame();
        if let Err(e) = self.queue_frame(&settings) {
            return Stop::Failed(e);
        }
        // §6.9.1 again, from the other side: only a stream-0 WINDOW_UPDATE can
        // raise the connection receive window, and on this leg it is the window
        // a large response has to fit through.
        let bootstrap = Frame::WindowUpdate {
            stream_id: StreamId::CONNECTION,
            increment: crate::flow::connection_window_bootstrap(self.conn_window),
        };
        if let Err(e) = self.queue_frame(&bootstrap) {
            return Stop::Failed(e);
        }

        loop {
            loop {
                match self.codec.decode_any(&mut self.read_buf) {
                    Ok(Some(Decoded::Ignored { kind, stream_id })) => {
                        if let Err(e) = self.reject_if_mid_header_block(kind, stream_id) {
                            return Stop::Failed(e);
                        }
                        trace!(?kind, stream = stream_id.get(), "ignoring frame");
                    }
                    Ok(Some(Decoded::Frame(frame))) => match self.handle_frame(frame) {
                        Ok(true) => {}
                        Ok(false) => return Stop::Quiet,
                        Err(e) => return Stop::Failed(e),
                    },
                    Ok(None) => break,
                    Err(e) => return Stop::Failed(ConnectionError::new(e.code(), e.to_string())),
                }
            }

            if let Err(e) = self.pump_outbound() {
                return Stop::Failed(e);
            }

            // Checked after every batch of frames, because the thing that ends a
            // peer drain is the last promised response arriving — a socket
            // wake-up, not a clock one.
            if self.peer_drain_complete() {
                return Stop::Quiet;
            }

            match self.tick().await {
                Ok(true) => {}
                Ok(false) => return Stop::Quiet,
                Err(e) => return Stop::Failed(e),
            }

            if self.peer_drain_complete() {
                return Stop::Quiet;
            }
        }
    }

    /// One pass of the I/O select: the socket both ways, or a client's request.
    async fn tick(&mut self) -> Result<bool, ConnectionError> {
        let mut wrote = 0usize;
        let mut msg = None;
        let mut probe_fired = false;
        let mut heard = false;
        // Only during a peer drain, so an idle connection still parks on its
        // sockets rather than waking on a timer it has no use for.
        let drain_deadline = self.peer_draining.map(|(_, at)| at);
        // Likewise: `None` unless probing is on, so a connection nobody
        // configured has exactly the wake-ups it had before.
        let probe_at = self.probe.next_wake();

        let keep_going = tokio::select! {
            biased;
            // Wakes the loop when a draining backend has gone quiet without
            // finishing what it promised. Without it the deadline is only
            // noticed if some other event happens to arrive.
            _ = async {
                match drain_deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => true,

            // The probe timer. A silent socket is precisely the case where no
            // other arm will ever wake this loop, which is why the probe needs
            // its own and why the failure it catches is invisible without one.
            _ = async {
                match probe_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => { probe_fired = true; true }

            written = self.writer.write(&self.out), if !self.out.is_empty() => {
                match written {
                    Ok(0) => false,
                    Ok(n) => { wrote = n; true }
                    Err(e) => {
                        debug!(error = %e, "upstream write failed; closing");
                        false
                    }
                }
            }

            received = self.inbox.recv() => {
                match received {
                    Some(received) => { msg = Some(received); true }
                    // Every handle is gone: the pool dropped this connection and
                    // no client can reach it again.
                    None => false,
                }
            }

            read = self.reader.read_buf(&mut self.read_buf) => {
                match read {
                    Ok(0) => false,
                    Ok(_) => { heard = true; true }
                    Err(e) => {
                        debug!(error = %e, "upstream read failed; closing");
                        false
                    }
                }
            }
        };

        self.out.advance(wrote);
        if heard {
            self.probe.heard(Instant::now());
        }
        if probe_fired && !self.probe_timer()? {
            return Ok(false);
        }
        if let Some(msg) = msg {
            self.handle_message(msg)?;
            while let Ok(msg) = self.inbox.try_recv() {
                self.handle_message(msg)?;
            }
        }
        Ok(keep_going)
    }

    /// The probe timer fired: send the PING, or conclude the backend is gone.
    ///
    /// `false` ends the connection. Ending it *quietly* rather than as a
    /// protocol error is deliberate — the backend has broken no rule that we can
    /// see, it has simply stopped talking, and the streams on it are failed by
    /// [`Self::fail_all_routes`] on the way out exactly as they would be for any
    /// other death. That is what turns a hang into a 502, and then — through the
    /// pool's report to `health` — into an ejection.
    fn probe_timer(&mut self) -> Result<bool, ConnectionError> {
        match self.probe.due(Instant::now()) {
            ProbeAction::Nothing => Ok(true),
            ProbeAction::Send(nonce) => {
                trace!("backend has gone quiet; probing with PING");
                self.queue_frame(&Frame::Ping {
                    data: nonce,
                    ack: false,
                })?;
                self.summary.probes_sent += 1;
                self.stats.probe_sent();
                Ok(true)
            }
            ProbeAction::Expired => {
                warn!(
                    timeout_s = self.probe.timeout.as_secs_f64(),
                    live = self.routes.len(),
                    "backend did not answer a PING; closing the connection",
                );
                self.summary.probe_timed_out = true;
                // Out of rotation now, not when the task finally unwinds: a
                // checkout racing this must not be handed a socket we have
                // already given up on.
                if let Some(record) = &self.record {
                    record.retire();
                }
                Ok(false)
            }
        }
    }

    /// Bounded for the same reason the client side's is: a backend that stops
    /// reading must not pin this task, which is shared by every client using it.
    async fn flush(&mut self) {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !self.out.is_empty() {
                match self.writer.write(&self.out).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => self.out.advance(n),
                }
            }
            let _ = self.writer.flush().await;
        })
        .await;
    }

    // ---- what clients ask for ----------------------------------------------

    fn handle_message(&mut self, msg: ToUpstream) -> Result<(), ConnectionError> {
        match msg {
            ToUpstream::Request {
                id,
                client_id,
                head,
                end_stream,
                events,
            } => self.send_request(id, client_id, head, end_stream, events),
            ToUpstream::Body {
                id,
                data,
                end_stream,
            } => {
                let Some(id) = self.requests.get(&id).copied() else {
                    // Either still queued for a slot, or already gone. If it is
                    // queued the octets ride along with it; if it is gone the
                    // client hears about that separately.
                    if let Some(pending) = self.pending.iter_mut().find(|p| p.request == id) {
                        pending.body.push_back((data, end_stream));
                    }
                    return Ok(());
                };
                let Some(stream) = self.streams.get_mut(id) else {
                    // The stream died between the client queueing this and us
                    // reading it. The client will hear about that separately.
                    return Ok(());
                };
                if !data.is_empty() {
                    stream.send_queue.push_back(data);
                }
                if end_stream {
                    stream.send_end_stream = true;
                }
                self.enqueue_ready(id);
                Ok(())
            }
            ToUpstream::Trailers { id, fields } => {
                let Some(id) = self.requests.get(&id).copied() else {
                    if let Some(pending) = self.pending.iter_mut().find(|p| p.request == id) {
                        pending.trailers = Some(fields);
                    }
                    return Ok(());
                };
                let Some(stream) = self.streams.get_mut(id) else {
                    return Ok(());
                };
                stream.pending_trailers = Some(fields);
                stream.send_end_stream = true;
                self.enqueue_ready(id);
                Ok(())
            }
            ToUpstream::Released { id, n } => match self.requests.get(&id).copied() {
                Some(id) => self.release(id, n),
                None => Ok(()),
            },
            ToUpstream::Cancel { id, code } => {
                let Some(id) = self.requests.get(&id).copied() else {
                    // Cancelled before we ever sent it: drop it from the queue
                    // if it is waiting, and remember the id in case the request
                    // itself is still in the inbox behind this message.
                    self.pending.retain(|p| p.request != id);
                    self.cancelled.insert(id);
                    return Ok(());
                };
                if self.streams.get_mut(id).is_some() {
                    self.summary.streams_reset += 1;
                    self.write_rst_stream(id, code)?;
                }
                self.forget(id);
                Ok(())
            }
        }
    }

    /// Open a stream on this connection and send the request head.
    ///
    /// **The stream id is chosen here and nowhere else.** It is the next odd id,
    /// taken at the instant the HEADERS is queued, so ids reach the wire in the
    /// order §5.1.1 requires however many client connections are feeding this
    /// one.
    fn send_request(
        &mut self,
        request: RequestId,
        client_id: StreamId,
        head: Box<RequestHead>,
        end_stream: bool,
        events: Events,
    ) -> Result<(), ConnectionError> {
        if self.cancelled.remove(&request) {
            // The client gave up while this sat in the inbox. Sending it now
            // would open a stream nobody is waiting for and immediately reset
            // it — work for the backend and a wasted id.
            return Ok(());
        }
        // Leased just before the backend's GOAWAY landed. The backend has said
        // it will process nothing further, so this provably never started.
        if self.peer_draining.is_some() {
            debug!(
                ?request,
                "backend is draining; refusing a newly leased request"
            );
            let _ = events.send(ServiceEvent::Reset {
                id: client_id,
                code: ErrorCode::RefusedStream,
            });
            return Ok(());
        }
        // No slot at the backend's limit: wait for one rather than refuse.
        if !self.streams.can_open_local() {
            self.pending.push_back(Pending {
                request,
                client_id,
                head,
                end_stream,
                events,
                body: VecDeque::new(),
                trailers: None,
            });
            return Ok(());
        }
        let id = StreamId::new(self.next_stream_id);
        self.next_stream_id = self.next_stream_id.saturating_add(2);
        match self.streams.open_local(id) {
            Ok(_) => {}
            Err(rejection) => {
                // The pool leased an id this connection cannot honour — a bug
                // on our side, not the backend's, so the client is told the
                // request never started rather than the connection torn down.
                debug!(stream = id.get(), ?rejection, "cannot open upstream stream");
                let _ = events.send(ServiceEvent::Reset {
                    id: client_id,
                    code: ErrorCode::RefusedStream,
                });
                return Ok(());
            }
        }
        self.routes.insert(
            id,
            Route {
                client_id,
                events,
                request,
                head_sent: false,
                pending_conn_release: 0,
            },
        );
        self.requests.insert(request, id);
        self.stats.open_stream();

        let mut block = BytesMut::new();
        self.hpack_enc.encode(&head.to_headers(), &mut block);
        self.queue_frame(&Frame::Headers {
            stream_id: id,
            block: block.freeze(),
            end_stream,
            end_headers: true,
        })?;
        self.apply_stream_event(id, StreamEvent::SendHeaders { end_stream })?;
        self.summary.requests_sent += 1;
        Ok(())
    }

    /// Start as many queued requests as there are slots for.
    ///
    /// Called wherever a stream retires, because that is the only thing that
    /// frees a slot. A queue with nothing draining it is just a slower way to
    /// hang.
    fn drain_pending(&mut self) -> Result<(), ConnectionError> {
        while self.streams.can_open_local() {
            let Some(pending) = self.pending.pop_front() else {
                return Ok(());
            };
            let Pending {
                request,
                client_id,
                head,
                end_stream,
                events,
                body,
                trailers,
            } = pending;
            self.send_request(request, client_id, head, end_stream, events)?;
            // Whatever arrived while it waited follows it immediately, in order.
            let Some(id) = self.requests.get(&request).copied() else {
                continue;
            };
            if let Some(stream) = self.streams.get_mut(id) {
                for (data, end) in body {
                    if !data.is_empty() {
                        stream.send_queue.push_back(data);
                    }
                    if end {
                        stream.send_end_stream = true;
                    }
                }
                if let Some(fields) = trailers {
                    stream.pending_trailers = Some(fields);
                    stream.send_end_stream = true;
                }
            }
            self.enqueue_ready(id);
        }
        Ok(())
    }

    /// Give the backend back `n` octets of credit, now that the client has them.
    fn release(&mut self, id: StreamId, n: u32) -> Result<(), ConnectionError> {
        if n == 0 {
            return Ok(());
        }
        if let Some(route) = self.routes.get_mut(&id) {
            route.pending_conn_release = route.pending_conn_release.saturating_sub(n);
        }
        self.stats.unbuffer(n);
        if let Some(stream) = self.streams.get_mut(id)
            && let Some(increment) = stream.recv_window.release(n)
        {
            self.queue_frame(&Frame::WindowUpdate {
                stream_id: id,
                increment,
            })?;
        }
        if let Some(increment) = self.conn_recv_window.release(n) {
            self.queue_frame(&Frame::WindowUpdate {
                stream_id: StreamId::CONNECTION,
                increment,
            })?;
        }
        Ok(())
    }

    /// Drop a route, returning any connection credit it was still holding, and
    /// hand the slot it just freed to whatever is waiting.
    fn forget(&mut self, id: StreamId) {
        self.streams.retire(id);
        self.ready.retain(|queued| *queued != id);
        let _ = self.streams.take_reclaimed();
        if let Some(route) = self.routes.remove(&id) {
            self.requests.remove(&route.request);
            self.stats.close_stream();
            if route.pending_conn_release > 0 {
                self.stats.unbuffer(route.pending_conn_release);
                // Best effort: a WINDOW_UPDATE that cannot be queued is on a
                // connection that is already going down.
                if let Some(increment) = self.conn_recv_window.release(route.pending_conn_release) {
                    let _ = self.queue_frame(&Frame::WindowUpdate {
                        stream_id: StreamId::CONNECTION,
                        increment,
                    });
                }
            }
        }
        let _ = self.drain_pending();
    }

    /// The backend is going away. Retire this connection from the pool, release
    /// everything it will never process, and keep the rest running.
    ///
    /// The split is the whole point of §6.8: a stream at or below
    /// `last_stream_id` was accepted and its response is still coming, while
    /// anything above it — including every request still queued for a slot —
    /// provably never started. The latter get REFUSED_STREAM, which promises the
    /// client nothing was processed and makes the request safe to send
    /// elsewhere.
    fn begin_peer_drain(&mut self, last_stream_id: StreamId) {
        // Out of the pool first, so no further request is leased onto a
        // connection that cannot open a stream for it.
        if let Some(record) = &self.record {
            record.retire();
        }

        let deadline = tokio::time::Instant::now() + PEER_DRAIN_DEADLINE;
        self.peer_draining = Some((last_stream_id, deadline));

        // Queued requests never got an id, so they are unambiguously unstarted.
        for pending in std::mem::take(&mut self.pending) {
            let _ = pending.events.send(ServiceEvent::Reset {
                id: pending.client_id,
                code: ErrorCode::RefusedStream,
            });
        }

        let abandoned: Vec<StreamId> = self
            .routes
            .keys()
            .copied()
            .filter(|id| *id > last_stream_id)
            .collect();
        for id in abandoned {
            if let Some(route) = self.routes.get(&id) {
                let _ = route.events.send(ServiceEvent::Reset {
                    id: route.client_id,
                    code: ErrorCode::RefusedStream,
                });
            }
            self.forget(id);
        }
    }

    /// Whether a peer drain is finished — nothing left to wait for, or waited
    /// long enough.
    fn peer_drain_complete(&self) -> bool {
        let Some((_, deadline)) = self.peer_draining else {
            return false;
        };
        if self.routes.is_empty() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(
                live = self.routes.len(),
                "backend drain deadline expired; abandoning in-flight streams",
            );
            return true;
        }
        false
    }

    /// Tell every client still waiting that this connection is gone.
    fn fail_all_routes(&mut self) {
        self.requests.clear();
        for pending in std::mem::take(&mut self.pending) {
            let _ = pending.events.send(ServiceEvent::Gone {
                id: pending.client_id,
            });
        }
        for (_, route) in self.routes.drain() {
            self.stats.close_stream();
            if route.pending_conn_release > 0 {
                self.stats.unbuffer(route.pending_conn_release);
            }
            let event = if route.head_sent {
                // The status line is already on the client's wire; the only
                // truthful ending left is an abort.
                ServiceEvent::Reset {
                    id: route.client_id,
                    code: ErrorCode::InternalError,
                }
            } else {
                ServiceEvent::Gone {
                    id: route.client_id,
                }
            };
            let _ = route.events.send(event);
        }
    }

    // ---- what the backend says ---------------------------------------------

    fn handle_frame(&mut self, frame: Frame) -> Result<bool, ConnectionError> {
        // §4.3: nothing may come between a HEADERS and its CONTINUATION.
        match self.open_header_block {
            Some(open) => {
                let continues = matches!(
                    &frame,
                    Frame::Continuation { stream_id, .. } if *stream_id == open
                );
                if !continues {
                    return Err(ConnectionError::new(
                        ErrorCode::ProtocolError,
                        format!(
                            "expected CONTINUATION on stream {}, got {:?}",
                            open.get(),
                            frame.kind(),
                        ),
                    ));
                }
            }
            None if matches!(frame, Frame::Continuation { .. }) => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    "CONTINUATION with no header block open",
                ));
            }
            None => {}
        }

        match &frame {
            Frame::Settings { ack: false, params } => {
                let previous_window = self.peer_settings.initial_window_size;
                self.peer_settings.apply(params)?;
                self.hpack_enc
                    .set_max_table_size(self.peer_settings.header_table_size as usize);
                // The backend's concurrency limit is one we obey. The pool reads
                // it through the shared record so it knows when to open another
                // connection instead of queueing behind this one.
                if let Some(max) = self.peer_settings.max_concurrent_streams {
                    self.streams.set_max_concurrent(max);
                    if let Some(record) = &self.record {
                        record.set_max_concurrent(max as usize);
                    }
                }
                let delta = self.peer_settings.initial_window_size as i64 - previous_window as i64;
                if delta != 0 {
                    self.streams
                        .apply_initial_window_delta(delta as i32)
                        .map_err(|code| {
                            ConnectionError::new(code, "INITIAL_WINDOW_SIZE change overflows")
                        })?;
                }
                self.queue_frame(&Frame::Settings {
                    ack: true,
                    params: Vec::new(),
                })?;
                self.summary.handshake_completed = true;
            }
            Frame::Settings { ack: true, .. } => {}
            Frame::Ping { data, ack: false } => {
                self.queue_frame(&Frame::Ping {
                    data: *data,
                    ack: true,
                })?;
            }
            Frame::Ping { data, ack: true } => {
                // The nonce is checked rather than just the flag. A backend that
                // echoes a stale payload has proved it was alive when *that*
                // probe went out, which is the question we already had an answer
                // to; only a matching payload answers the one we just asked.
                if self.probe.enabled() && !self.probe.acked(data, Instant::now()) {
                    debug!("ignoring a PING ACK we did not ask for");
                }
            }
            Frame::GoAway {
                last_stream_id,
                error_code,
                ..
            } => {
                debug!(
                    last_stream_id = last_stream_id.get(),
                    ?error_code,
                    live = self.routes.len(),
                    "backend sent GOAWAY; draining this connection",
                );
                self.begin_peer_drain(*last_stream_id);
                // Keep serving. Everything at or below `last_stream_id` was
                // accepted by the backend and is still coming; the loop ends in
                // `drive` once the last of them retires.
                return Ok(true);
            }
            Frame::WindowUpdate {
                stream_id,
                increment,
            } => self.recv_window_update(*stream_id, *increment)?,
            Frame::Headers {
                stream_id,
                block,
                end_stream,
                end_headers,
            } => {
                if self.open_header_block.is_none() {
                    self.open_header_end_stream = *end_stream;
                }
                self.read_header_block(*stream_id, block, *end_headers)?;
            }
            Frame::Continuation {
                stream_id,
                block,
                end_headers,
            } => self.read_header_block(*stream_id, block, *end_headers)?,
            Frame::Data {
                stream_id,
                data,
                end_stream,
            } => self.recv_data(*stream_id, data, *end_stream)?,
            Frame::RstStream {
                stream_id,
                error_code,
            } => {
                if let Some(route) = self.routes.get(stream_id) {
                    let _ = route.events.send(ServiceEvent::Reset {
                        id: route.client_id,
                        code: *error_code,
                    });
                    self.summary.streams_reset += 1;
                }
                self.forget(*stream_id);
            }
        }

        self.open_header_block = match &frame {
            Frame::Headers {
                stream_id,
                end_headers,
                ..
            }
            | Frame::Continuation {
                stream_id,
                end_headers,
                ..
            } if !*end_headers => Some(*stream_id),
            Frame::Headers { .. } | Frame::Continuation { .. } => None,
            _ => self.open_header_block,
        };
        Ok(true)
    }

    fn reject_if_mid_header_block(
        &self,
        kind: FrameType,
        stream_id: StreamId,
    ) -> Result<(), ConnectionError> {
        match self.open_header_block {
            Some(open) => Err(ConnectionError::new(
                ErrorCode::ProtocolError,
                format!(
                    "expected CONTINUATION on stream {}, got {kind:?} on stream {}",
                    open.get(),
                    stream_id.get(),
                ),
            )),
            None => Ok(()),
        }
    }

    fn read_header_block(
        &mut self,
        stream_id: StreamId,
        fragment: &Bytes,
        end_headers: bool,
    ) -> Result<(), ConnectionError> {
        if self.header_block.len() + fragment.len() > MAX_HEADER_BLOCK_BYTES {
            self.header_block.clear();
            return Err(ConnectionError::new(
                ErrorCode::EnhanceYourCalm,
                "backend header block too large",
            ));
        }
        let block = if end_headers && self.header_block.is_empty() {
            fragment.clone()
        } else {
            self.header_block.extend_from_slice(fragment);
            if !end_headers {
                return Ok(());
            }
            self.header_block.split().freeze()
        };

        // A backend that corrupts the HPACK stream poisons the shared dynamic
        // table, exactly as a client would: fatal to this connection, and the
        // pool opens another.
        let headers = match self.hpack_dec.decode(&block) {
            Ok(headers) => headers,
            Err(HpackError::HeaderListTooLarge { size, limit }) => {
                debug!(size, limit, "backend header list too large");
                self.fail_stream(stream_id, 502)?;
                return Ok(());
            }
            Err(e @ HpackError::Compression(_)) => return Err(e.into()),
        };

        let end_stream = self.open_header_end_stream;
        self.recv_response(stream_id, &headers, end_stream)
    }

    /// A response head, or a trailer section if we have already forwarded one.
    fn recv_response(
        &mut self,
        stream_id: StreamId,
        headers: &[Header],
        end_stream: bool,
    ) -> Result<(), ConnectionError> {
        if self.streams.lookup(stream_id).is_idle() {
            return Err(ConnectionError::new(
                ErrorCode::ProtocolError,
                format!(
                    "HEADERS on stream {}, which we never opened",
                    stream_id.get()
                ),
            ));
        }
        let Some(route) = self.routes.get_mut(&stream_id) else {
            return Ok(()); // cancelled while in flight
        };

        if route.head_sent {
            // A second field section is a trailer section (§8.1).
            if validate_trailers(headers).is_err() {
                debug!(stream = stream_id.get(), "backend sent malformed trailers");
            } else {
                let _ = route.events.send(ServiceEvent::Trailers {
                    id: route.client_id,
                    fields: headers.to_vec(),
                });
            }
            self.apply_stream_event(stream_id, StreamEvent::RecvHeaders { end_stream })?;
            if end_stream {
                self.forget(stream_id);
            }
            return Ok(());
        }

        let head = match ResponseHead::from_headers(headers) {
            Ok(head) => head,
            Err(code) => {
                debug!(
                    stream = stream_id.get(),
                    ?code,
                    "malformed backend response"
                );
                self.fail_stream(stream_id, 502)?;
                return Ok(());
            }
        };
        // 1xx is a preview, not the answer: forward nothing and keep waiting.
        // Treating it as the response would leave the real one with nowhere to
        // go, since a stream carries exactly one final head.
        if head.is_informational() {
            self.apply_stream_event(stream_id, StreamEvent::RecvHeaders { end_stream: false })?;
            return Ok(());
        }

        route.head_sent = true;
        let client_id = route.client_id;
        let _ = route.events.send(ServiceEvent::Head {
            id: client_id,
            response: Response {
                status: head.status,
                fields: head.fields,
            },
            end_stream,
        });
        self.summary.responses_received += 1;
        self.apply_stream_event(stream_id, StreamEvent::RecvHeaders { end_stream })?;
        if end_stream {
            self.forget(stream_id);
        }
        Ok(())
    }

    /// Response body octets: recorded, forwarded, and **not** released.
    fn recv_data(
        &mut self,
        stream_id: StreamId,
        data: &Bytes,
        end_stream: bool,
    ) -> Result<(), ConnectionError> {
        let len = data.len() as u32;
        self.conn_recv_window.record(len).map_err(|code| {
            ConnectionError::new(code, "backend exceeded the connection flow-control window")
        })?;

        match self.streams.lookup(stream_id) {
            Lookup::Idle => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!("DATA on stream {}, which we never opened", stream_id.get()),
                ));
            }
            Lookup::Closed => {
                // Nothing will ever accept these, so the credit goes back now.
                if let Some(increment) = self.conn_recv_window.release(len) {
                    self.queue_frame(&Frame::WindowUpdate {
                        stream_id: StreamId::CONNECTION,
                        increment,
                    })?;
                }
                return Ok(());
            }
            Lookup::Live(stream) => {
                if stream.recv_window.record(len).is_err() {
                    self.write_rst_stream(stream_id, ErrorCode::FlowControlError)?;
                    self.fail_stream(stream_id, 502)?;
                    return Ok(());
                }
            }
        }

        self.summary.data_bytes_received += u64::from(len);
        if let Some(route) = self.routes.get_mut(&stream_id) {
            route.pending_conn_release = route.pending_conn_release.saturating_add(len);
            self.stats.buffer(len);
            let _ = route.events.send(ServiceEvent::Data {
                id: route.client_id,
                data: data.clone(),
                end_stream,
            });
        }
        self.apply_stream_event(stream_id, StreamEvent::RecvData { end_stream })?;
        if end_stream {
            self.forget(stream_id);
        }
        Ok(())
    }

    fn recv_window_update(
        &mut self,
        stream_id: StreamId,
        increment: u32,
    ) -> Result<(), ConnectionError> {
        if increment == 0 {
            return if stream_id.is_connection() {
                Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    "WINDOW_UPDATE with a zero increment on stream 0",
                ))
            } else {
                self.write_rst_stream(stream_id, ErrorCode::ProtocolError)
            };
        }
        if stream_id.is_connection() {
            return self.conn_send_window.increase(increment).map_err(|code| {
                ConnectionError::new(code, "WINDOW_UPDATE overflows the connection window")
            });
        }
        let overflowed = match self.streams.lookup(stream_id) {
            Lookup::Idle => {
                return Err(ConnectionError::new(
                    ErrorCode::ProtocolError,
                    format!(
                        "WINDOW_UPDATE on stream {}, which we never opened",
                        stream_id.get()
                    ),
                ));
            }
            Lookup::Closed => return Ok(()),
            Lookup::Live(stream) => stream.send_window.increase(increment).is_err(),
        };
        if overflowed {
            return self.write_rst_stream(stream_id, ErrorCode::FlowControlError);
        }
        self.enqueue_ready(stream_id);
        Ok(())
    }

    // ---- sending ------------------------------------------------------------

    /// Round-robin the request bodies waiting on window, exactly as the client
    /// side schedules responses.
    fn pump_outbound(&mut self) -> Result<(), ConnectionError> {
        while !self.ready.is_empty() {
            if self.out.len() >= MAX_WRITE_QUEUE {
                break;
            }
            let lap: Vec<StreamId> = self.ready.drain(..).collect();
            let mut stalled: VecDeque<StreamId> = VecDeque::new();
            let mut served: VecDeque<StreamId> = VecDeque::new();
            let mut wrote = false;

            for id in lap {
                match self.visit_ready_stream(id)? {
                    Some(true) => {
                        wrote = true;
                        served.push_back(id);
                    }
                    Some(false) => wrote = true,
                    None => stalled.push_back(id),
                }
            }

            self.ready = stalled;
            self.ready.extend(served);
            for id in self.ready.clone() {
                if let Some(stream) = self.streams.get_mut(id) {
                    stream.queued = true;
                }
            }
            let streams = &mut self.streams;
            self.ready.retain(|id| streams.get_mut(*id).is_some());

            if !wrote {
                break;
            }
        }
        Ok(())
    }

    /// `Some(more)` if something was written, `None` if the stream is stalled.
    fn visit_ready_stream(&mut self, id: StreamId) -> Result<Option<bool>, ConnectionError> {
        let Some(stream) = self.streams.get_mut(id) else {
            return Ok(Some(false));
        };
        stream.queued = false;

        let budget = SEND_BUDGET
            .min(stream.send_window.sendable())
            .min(self.conn_send_window.sendable())
            .min(self.peer_settings.max_frame_size as usize);
        if stream.pending_send() > 0 && budget == 0 {
            return Ok(None);
        }

        let chunk = crate::conn::take_from_queue(&mut stream.send_queue, budget);
        let drained = stream.send_queue.is_empty();
        let trailers = if drained {
            stream.pending_trailers.take()
        } else {
            None
        };
        let end_stream = drained && stream.send_end_stream && trailers.is_none();
        if chunk.is_empty() && !end_stream && trailers.is_none() {
            return Ok(Some(false));
        }
        if end_stream || trailers.is_some() {
            stream.send_end_stream = false;
        }

        let len = chunk.len() as u32;
        if len > 0 {
            stream.send_window.consume(len).map_err(|code| {
                ConnectionError::new(code, "overspent an upstream stream window")
            })?;
            self.conn_send_window.consume(len).map_err(|code| {
                ConnectionError::new(code, "overspent the upstream connection window")
            })?;
        }

        if len > 0 || end_stream {
            self.queue_frame(&Frame::Data {
                stream_id: id,
                data: chunk,
                end_stream,
            })?;
            self.apply_stream_event(id, StreamEvent::SendData { end_stream })?;
        }

        // The backend has taken these octets, so the *client* may send us that
        // many more. This is the request-direction half of the bridge: a client
        // uploading faster than the backend accepts simply stops being credited.
        if len > 0
            && let Some(route) = self.routes.get(&id)
        {
            let _ = route.events.send(ServiceEvent::BodyAccepted {
                id: route.client_id,
                n: len,
            });
        }

        if let Some(fields) = trailers {
            let mut block = BytesMut::new();
            self.hpack_enc.encode(&fields, &mut block);
            self.queue_frame(&Frame::Headers {
                stream_id: id,
                block: block.freeze(),
                end_stream: true,
                end_headers: true,
            })?;
            self.apply_stream_event(id, StreamEvent::SendHeaders { end_stream: true })?;
        }

        let more = self
            .streams
            .get_mut(id)
            .is_some_and(|stream| stream.has_pending_send());
        Ok(Some(more))
    }

    fn enqueue_ready(&mut self, id: StreamId) {
        let Some(stream) = self.streams.get_mut(id) else {
            return;
        };
        if stream.queued || !stream.has_pending_send() {
            return;
        }
        stream.queued = true;
        self.ready.push_back(id);
    }

    /// Answer one client with a synthetic status because the backend could not.
    ///
    /// Only possible before the head has been forwarded — afterwards the status
    /// line is already on the client's wire and RST_STREAM is the only honest
    /// ending.
    fn fail_stream(&mut self, id: StreamId, status: u16) -> Result<(), ConnectionError> {
        if let Some(route) = self.routes.get(&id) {
            let event = if route.head_sent {
                ServiceEvent::Reset {
                    id: route.client_id,
                    code: ErrorCode::InternalError,
                }
            } else {
                ServiceEvent::Head {
                    id: route.client_id,
                    response: Response::status(status),
                    end_stream: true,
                }
            };
            let _ = route.events.send(event);
        }
        if self.streams.get_mut(id).is_some() {
            self.write_rst_stream(id, ErrorCode::Cancel)?;
        }
        self.forget(id);
        Ok(())
    }

    fn write_rst_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), ConnectionError> {
        self.streams.retire(stream_id);
        let _ = self.streams.take_reclaimed();
        self.ready.retain(|id| *id != stream_id);
        self.queue_frame(&Frame::RstStream {
            stream_id,
            error_code,
        })
    }

    fn apply_stream_event(
        &mut self,
        id: StreamId,
        event: StreamEvent,
    ) -> Result<(), ConnectionError> {
        match self.streams.apply(id, event) {
            Ok(state) => {
                if state.is_closed() {
                    self.ready.retain(|queued| *queued != id);
                }
                Ok(())
            }
            Err(e) if e.code == ErrorCode::ProtocolError => Err(ConnectionError::new(
                ErrorCode::ProtocolError,
                format!("frame for upstream stream {}, which is idle", id.get()),
            )),
            Err(e) => {
                debug!(stream = id.get(), code = ?e.code, "illegal upstream transition");
                self.fail_stream(id, 502)
            }
        }
    }

    fn queue_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        self.encode_buf.clear();
        self.codec
            .encode(frame, &mut self.encode_buf)
            .map_err(|e| {
                ConnectionError::new(
                    ErrorCode::InternalError,
                    format!("could not encode {:?}: {e}", frame.kind()),
                )
            })?;
        self.out.extend_from_slice(&self.encode_buf);
        Ok(())
    }
}

/// The channel an [`UpstreamConnection`] is driven through: the handle for the
/// pool, the receiver for the connection task.
pub fn channel() -> (UpstreamHandle, mpsc::UnboundedReceiver<ToUpstream>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (UpstreamHandle { tx }, rx)
}

/// Answer everything queued on an inbox whose connection never came up.
///
/// A failed connect is not a reason for a client to wait: leaving these in the
/// channel would hang every one of them until the client gave up. They get the
/// same **502** as every other backend failure, which is the rule the whole
/// proxy follows: *502 means we tried a backend and it let us down; 503 means we
/// had no backend to try.* Splitting them the other way round would tell a
/// client "try again later" about a backend that is simply gone.
pub fn fail_pending(mut inbox: mpsc::UnboundedReceiver<ToUpstream>) {
    inbox.close();
    while let Ok(msg) = inbox.try_recv() {
        if let ToUpstream::Request {
            client_id, events, ..
        } = msg
        {
            let _ = events.send(ServiceEvent::Gone { id: client_id });
        }
    }
}

/// Spawn a connection task over an already-connected stream.
pub fn spawn<IO>(io: IO, stats: std::sync::Arc<crate::proxy::ProxyStats>) -> UpstreamHandle
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (handle, inbox) = channel();
    tokio::spawn(async move {
        let summary = UpstreamConnection::new(io, inbox, stats, None).run().await;
        debug!(?summary, "upstream connection closed");
    });
    handle
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 5 s / 5 s probe whose clock starts exactly at `start`, so every
    /// assertion below is about the arithmetic rather than about how long the
    /// test itself took.
    fn probe(start: Instant) -> Probe {
        Probe::new(Duration::from_secs(5), Duration::from_secs(5), start)
    }

    #[tokio::test]
    async fn a_connection_nobody_configured_never_probes_and_never_wakes() {
        let mut off = Probe::disabled();
        let now = Instant::now() + Duration::from_secs(3600);
        assert_eq!(off.next_wake(), None, "a disabled probe must park forever");
        assert_eq!(off.due(now), ProbeAction::Nothing);
    }

    #[tokio::test]
    async fn a_quiet_socket_is_probed_and_a_busy_one_is_not() {
        let start = Instant::now();
        let mut p = probe(start);

        assert_eq!(p.due(start + Duration::from_secs(4)), ProbeAction::Nothing);
        // Traffic resets the clock, which is why an ordinary busy connection
        // never sends a probe at all.
        p.heard(start + Duration::from_secs(4));
        assert_eq!(p.due(start + Duration::from_secs(8)), ProbeAction::Nothing);
        assert!(matches!(
            p.due(start + Duration::from_secs(9)),
            ProbeAction::Send(_)
        ));
    }

    #[tokio::test]
    async fn an_unanswered_probe_expires_and_an_answered_one_does_not() {
        let start = Instant::now();
        let mut p = probe(start);
        let ProbeAction::Send(nonce) = p.due(start + Duration::from_secs(5)) else {
            panic!("a probe was due and was not sent");
        };
        // Still waiting, not yet expired.
        assert_eq!(p.due(start + Duration::from_secs(9)), ProbeAction::Nothing);
        assert_eq!(p.due(start + Duration::from_secs(10)), ProbeAction::Expired);

        let mut answered = probe(start);
        let ProbeAction::Send(first) = answered.due(start + Duration::from_secs(5)) else {
            panic!("a probe was due and was not sent");
        };
        assert!(answered.acked(&first, start + Duration::from_secs(6)));
        assert_eq!(
            answered.due(start + Duration::from_secs(10)),
            ProbeAction::Nothing,
            "an answered probe expired anyway; every idle backend would be ejected",
        );
        // The next probe carries a different payload, so this round's ACK cannot
        // answer the next round's question.
        let ProbeAction::Send(second) = answered.due(start + Duration::from_secs(11)) else {
            panic!("the idle period elapsed again with no probe");
        };
        assert_ne!(first, second, "two probes reused a nonce");
        assert_eq!(
            nonce, first,
            "unrelated probes disagreed on the first nonce"
        );
    }

    #[tokio::test]
    async fn a_stale_ack_is_not_mistaken_for_the_answer_to_this_probe() {
        // The payload is checked, not just the flag, so an ACK for a probe two
        // rounds ago does not end the one in flight early. It is still traffic,
        // though, and traffic is liveness — the connection survives on that
        // basis rather than on a payload it never sent back.
        let start = Instant::now();
        let stale = 99u64.to_be_bytes();
        let mut p = probe(start);
        assert!(matches!(
            p.due(start + Duration::from_secs(5)),
            ProbeAction::Send(_)
        ));
        assert!(
            !p.acked(&stale, start + Duration::from_secs(6)),
            "a payload we never sent was matched to the probe in flight",
        );
        assert!(
            p.outstanding.is_some(),
            "a stale ACK cleared the probe it did not answer",
        );
        assert_eq!(
            p.due(start + Duration::from_secs(10)),
            ProbeAction::Nothing,
            "the backend spoke; only silence should end a connection",
        );
    }

    #[tokio::test]
    async fn a_backend_that_talks_without_acking_is_left_alone() {
        // The false-positive case that matters under load: a peer mid-response
        // is alive whatever it does with our PING, and disconnecting it would
        // cost real requests to defend against nothing.
        let start = Instant::now();
        let mut p = probe(start);
        assert!(matches!(
            p.due(start + Duration::from_secs(5)),
            ProbeAction::Send(_)
        ));
        p.heard(start + Duration::from_secs(7));
        assert_eq!(p.due(start + Duration::from_secs(10)), ProbeAction::Nothing);
        // ...and the cycle restarts from the last thing it said, not from the
        // probe, so the next probe is a full idle period away.
        assert_eq!(p.due(start + Duration::from_secs(11)), ProbeAction::Nothing);
        assert!(matches!(
            p.due(start + Duration::from_secs(12)),
            ProbeAction::Send(_)
        ));
    }
}
