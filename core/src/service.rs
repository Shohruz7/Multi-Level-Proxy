//! The request/response seam: what answers a stream.
//!
//! A neutral module rather than part of [`crate::conn`] or [`crate::proxy`],
//! because both sides need it and `conn` depending on `proxy` would read
//! backwards. Week 5 shipped [`Echo`], a built-in responder that makes the
//! engine a working HTTP/2 server; week 6's [`crate::proxy::Proxy`] implements
//! the same trait and the connection layer does not notice the difference.
//!
//! # Week 6: the seam is asynchronous
//!
//! Week 5's `respond(&req) -> Response` could not survive contact with a proxy:
//! there is no response until a backend produces one, and blocking the
//! connection task on that would stall every other stream sharing it. So the
//! trait is now **fire-and-forget in, events out**. A responder is told about a
//! stream ([`Service::dispatch`], [`Service::body`], …) and answers whenever it
//! can by pushing [`ServiceEvent`]s into the channel it was handed at
//! [`Service::attach`]. [`Echo`] pushes its events synchronously inside
//! `dispatch`; [`crate::proxy::Proxy`] pushes them when the upstream replies.
//! The connection loop cannot tell the difference, which is exactly the point.
//!
//! The event channel is **unbounded**, and that is a deliberate claim rather
//! than an oversight: the only two producers are bounded already. `Echo`'s
//! bodies are materialized up front, and the proxy cannot produce a byte the
//! upstream was not credited for — and the upstream is credited only as the
//! client drains (§4.2, ADR 0016). A bound here would add a place to block
//! without removing any octets.
//!
//! Also home of **message-semantics validation** (RFC 9113 §8.3):
//! [`RequestHead::from_headers`] turns a decoded HPACK header list into a
//! request or rejects it, and [`ResponseHead::from_headers`] does the same for
//! what an upstream sends back. HPACK decodes faithfully and judges nothing —
//! the judgement belongs here, one layer up, where "this is not a well-formed
//! HTTP message" is a *stream* error and the other 200 streams on the
//! connection carry on.

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::conn::ErrorCode;
use crate::hpack::Header;
use crate::stream::StreamId;

/// Ceiling on a `/bytes/<n>` response, so a hostile path cannot ask the server
/// to materialize 4 GiB.
pub const MAX_SIZED_BODY: usize = 64 * 1024 * 1024;

/// Fields that describe a single HTTP/1.x hop and have no meaning in HTTP/2
/// (§8.2.2). Their presence makes a message malformed.
const CONNECTION_SPECIFIC: [&[u8]; 5] = [
    b"connection",
    b"keep-alive",
    b"proxy-connection",
    b"transfer-encoding",
    b"upgrade",
];

/// A validated request line plus its regular fields.
///
/// The fields are kept as the decoded [`Header`] list rather than parsed
/// further: week 6 re-encodes them toward an upstream, and the `sensitive` flag
/// has to survive that trip (RFC 7541 §7.1.3).
#[derive(Clone, Debug)]
pub struct RequestHead {
    pub method: Bytes,
    pub scheme: Bytes,
    pub authority: Option<Bytes>,
    pub path: Bytes,
    pub fields: Vec<Header>,
}

impl RequestHead {
    /// Validate a decoded header list into a request (RFC 9113 §8.3.1).
    ///
    /// Every rejection here is a **stream** error — `PROTOCOL_ERROR` via
    /// RST_STREAM — never a connection one. One client sending one malformed
    /// request must not take down the other streams sharing the connection,
    /// which is the whole point of the ADR-0008 split. (Contrast HPACK, where a
    /// failure poisons the shared dynamic table and *has* to be fatal.)
    pub fn from_headers(headers: &[Header]) -> Result<RequestHead, ErrorCode> {
        let mut method = None;
        let mut scheme = None;
        let mut authority = None;
        let mut path = None;
        let mut fields = Vec::with_capacity(headers.len());
        let mut seen_regular = false;

        for header in headers {
            let name = &header.name[..];

            if name.first() == Some(&b':') {
                // Ordering matters: all pseudo-headers precede all regular
                // fields, so a proxy can stop parsing the request line early.
                if seen_regular {
                    return Err(ErrorCode::ProtocolError);
                }
                let slot = match name {
                    b":method" => &mut method,
                    b":scheme" => &mut scheme,
                    b":authority" => &mut authority,
                    b":path" => &mut path,
                    // Response pseudo-headers and anything unrecognized are
                    // malformed in a request (§8.3.1).
                    _ => return Err(ErrorCode::ProtocolError),
                };
                if slot.replace(header.value.clone()).is_some() {
                    return Err(ErrorCode::ProtocolError);
                }
                continue;
            }

            seen_regular = true;
            // Field names are lowercase on the wire in HTTP/2 (§8.2.1); an
            // uppercase byte means an HTTP/1 message was translated carelessly.
            if name.is_empty() || name.iter().any(u8::is_ascii_uppercase) {
                return Err(ErrorCode::ProtocolError);
            }
            if CONNECTION_SPECIFIC.contains(&name) {
                return Err(ErrorCode::ProtocolError);
            }
            // `te` survives, but only to say "trailers" — any other value is a
            // hop-by-hop negotiation HTTP/2 does not have.
            if name == b"te" && &header.value[..] != b"trailers" {
                return Err(ErrorCode::ProtocolError);
            }
            fields.push(header.clone());
        }

        let (Some(method), Some(scheme), Some(path)) = (method, scheme, path) else {
            return Err(ErrorCode::ProtocolError);
        };
        // §8.3.1: `:path` must not be empty for the schemes we serve.
        if path.is_empty() {
            return Err(ErrorCode::ProtocolError);
        }

        Ok(RequestHead {
            method,
            scheme,
            authority,
            path,
            fields,
        })
    }

    /// Rebuild the header list to send upstream: pseudo-headers first and in
    /// the canonical order, then the regular fields as they arrived.
    ///
    /// The fields are the *decoded* ones, so `sensitive` rides along and the
    /// upstream encoder will keep an `authorization` out of its dynamic table
    /// exactly as the client asked (RFC 7541 §7.1.3). Re-deriving the list from
    /// scratch here — rather than forwarding the original block — is what makes
    /// the two HPACK contexts independent, which they must be: the client's
    /// dynamic table and the upstream's have nothing to do with each other.
    pub fn to_headers(&self) -> Vec<Header> {
        let mut headers = Vec::with_capacity(self.fields.len() + 4);
        headers.push(Header::new(
            Bytes::from_static(b":method"),
            self.method.clone(),
        ));
        headers.push(Header::new(
            Bytes::from_static(b":scheme"),
            self.scheme.clone(),
        ));
        if let Some(authority) = &self.authority {
            headers.push(Header::new(
                Bytes::from_static(b":authority"),
                authority.clone(),
            ));
        }
        headers.push(Header::new(Bytes::from_static(b":path"), self.path.clone()));
        headers.extend(self.fields.iter().cloned());
        headers
    }

    /// The path with any query string removed.
    pub fn path_only(&self) -> &[u8] {
        let path = &self.path[..];
        match path.iter().position(|&b| b == b'?') {
            Some(i) => &path[..i],
            None => path,
        }
    }

    /// The body length the request declares, if any.
    ///
    /// §8.1.2.6 makes a `content-length` that disagrees with the DATA actually
    /// sent a malformed message, so the connection carries this value alongside
    /// the stream and checks it when the request body ends. An unparseable value
    /// is malformed in its own right, reported as `Some(Err(..))` rather than
    /// quietly ignored.
    pub fn content_length(&self) -> Option<Result<u64, ErrorCode>> {
        let field = self
            .fields
            .iter()
            .find(|h| h.name.as_ref() == b"content-length")?;
        Some(
            std::str::from_utf8(&field.value)
                .ok()
                .and_then(|v| v.parse().ok())
                .ok_or(ErrorCode::ProtocolError),
        )
    }
}

/// Validate a trailer section (§8.1).
///
/// Trailers are a plain field section with two extra rules: they may not carry
/// pseudo-headers (those belong to the request line, which is long gone), and
/// the same connection-specific fields are still forbidden. Reusing
/// [`RequestHead::from_headers`] would be wrong here — it *requires* the
/// pseudo-headers that trailers must not have.
pub fn validate_trailers(headers: &[Header]) -> Result<(), ErrorCode> {
    for header in headers {
        let name = &header.name[..];
        if name.first() == Some(&b':') {
            return Err(ErrorCode::ProtocolError);
        }
        if name.is_empty() || name.iter().any(u8::is_ascii_uppercase) {
            return Err(ErrorCode::ProtocolError);
        }
        if CONNECTION_SPECIFIC.contains(&name) {
            return Err(ErrorCode::ProtocolError);
        }
    }
    Ok(())
}

/// A validated response line plus its regular fields — what an upstream sends
/// back, and the mirror of [`RequestHead`].
#[derive(Clone, Debug)]
pub struct ResponseHead {
    pub status: u16,
    pub fields: Vec<Header>,
}

impl ResponseHead {
    /// Validate a decoded response header list (RFC 9113 §8.3.2).
    ///
    /// A malformed *response* is not the client's fault, so unlike
    /// [`RequestHead::from_headers`] the caller turns this into a 502 toward the
    /// client rather than passing the error along — but it is still stream-
    /// scoped: one broken backend response must not disturb the other streams
    /// riding the same upstream connection.
    pub fn from_headers(headers: &[Header]) -> Result<ResponseHead, ErrorCode> {
        let mut status = None;
        let mut fields = Vec::with_capacity(headers.len());
        let mut seen_regular = false;

        for header in headers {
            let name = &header.name[..];
            if name.first() == Some(&b':') {
                if seen_regular || name != b":status" {
                    // Request pseudo-headers in a response, an unknown one, or
                    // any of them after a regular field: all malformed.
                    return Err(ErrorCode::ProtocolError);
                }
                if status.is_some() {
                    return Err(ErrorCode::ProtocolError);
                }
                status = Some(
                    std::str::from_utf8(&header.value)
                        .ok()
                        .and_then(|v| v.parse::<u16>().ok())
                        .filter(|s| (100..=599).contains(s))
                        .ok_or(ErrorCode::ProtocolError)?,
                );
                continue;
            }

            seen_regular = true;
            if name.is_empty() || name.iter().any(u8::is_ascii_uppercase) {
                return Err(ErrorCode::ProtocolError);
            }
            if CONNECTION_SPECIFIC.contains(&name) {
                return Err(ErrorCode::ProtocolError);
            }
            fields.push(header.clone());
        }

        Ok(ResponseHead {
            status: status.ok_or(ErrorCode::ProtocolError)?,
            fields,
        })
    }

    /// Is this an informational (1xx) response? Those arrive *before* the real
    /// one and must not retire the stream (§8.1).
    pub const fn is_informational(&self) -> bool {
        self.status >= 100 && self.status < 200
    }
}

/// A response to send on one stream: the head only. Body octets travel as
/// separate [`ServiceEvent::Data`]s, because a proxy never has the whole body
/// when it has the head.
#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub fields: Vec<Header>,
}

impl Response {
    /// A response with no fields beyond `:status`.
    pub fn status(status: u16) -> Self {
        Response {
            status,
            fields: Vec::new(),
        }
    }

    /// The full header list to hand to HPACK: `:status` first, as §8.3 requires
    /// of every pseudo-header.
    pub fn to_headers(&self) -> Vec<Header> {
        let mut headers = Vec::with_capacity(self.fields.len() + 1);
        headers.push(Header::new(
            Bytes::from_static(b":status"),
            Bytes::from(self.status.to_string()),
        ));
        headers.extend(self.fields.iter().cloned());
        headers
    }
}

/// What a responder tells the connection to do with a stream.
///
/// Every `id` is a **client** stream id. On the upstream side that means the
/// engine translates out of its own id space before sending (design doc §4.3),
/// which is what spares the client connection a reverse map.
#[derive(Clone, Debug)]
pub enum ServiceEvent {
    /// The response head. `end_stream` means no body follows.
    Head {
        id: StreamId,
        response: Response,
        end_stream: bool,
    },
    /// Response body octets.
    Data {
        id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    /// A trailer section, which always ends the stream (§8.1).
    Trailers { id: StreamId, fields: Vec<Header> },
    /// `n` octets of the *request* body have been consumed for good — handed to
    /// an upstream that had the window for them, or simply absorbed by a local
    /// responder. This is what releases the client's receive window, and
    /// withholding it is the request-direction half of the bridge (§4.2).
    BodyAccepted { id: StreamId, n: u32 },
    /// Abort this stream with `code` (RST_STREAM).
    Reset { id: StreamId, code: ErrorCode },
    /// The upstream connection carrying this stream died before it could
    /// answer — a connect failure, a socket error, a task that went away.
    ///
    /// Distinct from a `Head` carrying 502, which is what this used to be, and
    /// the distinction is load-bearing: a 502 *from a backend* proves the
    /// backend is there and answering, while this proves the opposite. Collapsed
    /// into one event, health checking recorded every dead connection as a
    /// successful response and never ejected anything — killing a backend under
    /// load produced 18% 5xx with `h2proxy_backend_ejections_total` sitting at
    /// zero.
    ///
    /// The connection layer turns it into a 502 (or a reset, if a `:status` has
    /// already gone out), so what the client sees is unchanged.
    Gone { id: StreamId },
}

/// The channel a responder answers on.
pub type Events = mpsc::UnboundedSender<ServiceEvent>;

/// What answers a request on a stream.
///
/// Every method is **synchronous and non-blocking** — a responder records what
/// it was told and returns immediately, then answers later through its
/// [`Events`] channel. That keeps the connection task free to serve the other
/// streams while one of them waits on a backend, which is the whole reason the
/// week-5 signature had to change.
pub trait Service {
    /// Hand the responder the channel it answers on. Called once, before any
    /// stream exists.
    fn attach(&mut self, events: Events);

    /// A complete, validated request head arrived on `id`.
    fn dispatch(&mut self, id: StreamId, head: RequestHead, end_stream: bool);

    /// Request body octets. The responder owes a
    /// [`ServiceEvent::BodyAccepted`] for every octet it takes, or the client's
    /// window will never reopen.
    fn body(&mut self, id: StreamId, data: Bytes, end_stream: bool);

    /// A trailer section closed the request. Rare enough to default to
    /// "accepted and ignored".
    fn trailers(&mut self, id: StreamId, fields: Vec<Header>) {
        let _ = (id, fields);
    }

    /// The client abandoned the stream (RST_STREAM, or a connection error).
    /// Nothing may be sent for `id` afterwards.
    fn cancel(&mut self, id: StreamId, code: ErrorCode) {
        let _ = (id, code);
    }

    /// The stream completed normally — END_STREAM has passed in both
    /// directions and it is gone from the table.
    ///
    /// Distinct from [`Service::cancel`] because the two mean opposite things
    /// upstream: a cancel has to *reset* the backend stream, while a finish has
    /// only to let go of it. But a responder that holds any per-stream
    /// resource — a pool lease, a map entry — **must** release it here.
    /// Forgetting to is invisible in a test that makes a few requests and fatal
    /// in one that makes a few thousand: the leases pile up, every pooled
    /// connection looks full, and the proxy starts answering 503 with nothing
    /// in the logs.
    fn finish(&mut self, id: StreamId) {
        let _ = id;
    }

    /// `n` octets of this stream's response were **written to the client**. The
    /// response-direction half of the bridge: a proxy turns this into the
    /// upstream WINDOW_UPDATE it has been withholding (§4.2).
    fn released(&mut self, id: StreamId, n: u32) {
        let _ = (id, n);
    }

    /// A last look at an event before the connection acts on it. Return the
    /// event to have it delivered, or `None` to swallow it.
    ///
    /// This exists because of an asymmetry in the week-6 design: a responder
    /// hands its `Events` sender *to the upstream task*, so everything the
    /// backend produces goes straight to the client connection and the responder
    /// never learns how any of its requests turned out. That is the right shape
    /// for throughput — no extra hop, no extra task — but it leaves a proxy
    /// unable to do anything that depends on an outcome.
    ///
    /// Two things depend on outcomes: health checking needs to know a backend
    /// answered or failed, and a retry needs to *replace* a failure rather than
    /// forward it. Returning `None` is what makes the second possible — the
    /// stream stays open, the client hears nothing, and a second attempt is
    /// already on its way to a different backend.
    ///
    /// The default is a pass-through, so a responder that does not care (like
    /// [`Echo`]) is unaffected and the connection behaves exactly as before.
    fn intercept(&mut self, event: ServiceEvent) -> Option<ServiceEvent> {
        Some(event)
    }
}

/// The built-in responder: enough of a server to prove the engine multiplexes,
/// and the control against which the proxy is measured.
///
/// - `/bytes/<n>` → 200 with exactly `n` octets. Deterministic response sizes
///   are what let the flow-control and interleaving tests assert on byte counts,
///   and what gives week 8's `h2load` a target it can size per profile.
/// - anything else → 200 echoing the request body, or `default_body` octets when
///   the request had none.
///
/// Everything it produces goes into the same event channel the proxy uses, so
/// the week-5 test suite exercises the week-6 loop rather than a bypass of it.
#[derive(Debug)]
pub struct Echo {
    default_body: usize,
    events: Option<Events>,
    /// Streams answered with `Body::Echo`, i.e. still mirroring their request
    /// body back. A stream leaves the set when its request ends.
    echoing: std::collections::HashSet<StreamId>,
}

impl Echo {
    pub fn new(default_body: usize) -> Self {
        Echo {
            default_body,
            events: None,
            echoing: std::collections::HashSet::new(),
        }
    }

    fn emit(&self, event: ServiceEvent) {
        if let Some(events) = &self.events {
            // The receiver is the connection that owns this responder; it going
            // away means the connection is already gone.
            let _ = events.send(event);
        }
    }
}

impl Default for Echo {
    fn default() -> Self {
        Echo::new(1024)
    }
}

impl Service for Echo {
    fn attach(&mut self, events: Events) {
        self.events = Some(events);
    }

    fn dispatch(&mut self, id: StreamId, head: RequestHead, end_stream: bool) {
        if let Some(n) = sized_body_request(head.path_only()) {
            self.emit(ServiceEvent::Head {
                id,
                response: Response {
                    status: 200,
                    fields: vec![Header::new(
                        Bytes::from_static(b"content-length"),
                        Bytes::from(n.to_string()),
                    )],
                },
                end_stream: n == 0,
            });
            if n > 0 {
                self.emit(ServiceEvent::Data {
                    id,
                    data: Bytes::from(vec![b'x'; n]),
                    end_stream: true,
                });
            }
            return;
        }

        // A request that carries a body gets it back; one that does not gets the
        // configured filler, so a plain `curl /` still exercises the send path.
        let echoing =
            !end_stream && head.method.as_ref() != b"GET" && head.method.as_ref() != b"HEAD";
        self.emit(ServiceEvent::Head {
            id,
            response: Response::status(200),
            end_stream: false,
        });
        if echoing {
            self.echoing.insert(id);
        } else {
            self.emit(ServiceEvent::Data {
                id,
                data: Bytes::from(vec![b'x'; self.default_body]),
                end_stream: true,
            });
        }
    }

    fn body(&mut self, id: StreamId, data: Bytes, end_stream: bool) {
        let n = data.len() as u32;
        if self.echoing.contains(&id) && (!data.is_empty() || end_stream) {
            self.emit(ServiceEvent::Data {
                id,
                data,
                end_stream,
            });
        }
        if end_stream {
            self.echoing.remove(&id);
        }
        // A local responder consumes the request body the instant it sees it,
        // so the credit goes straight back. The proxy is the one that waits.
        if n > 0 {
            self.emit(ServiceEvent::BodyAccepted { id, n });
        }
    }

    fn cancel(&mut self, id: StreamId, _code: ErrorCode) {
        self.echoing.remove(&id);
    }
}

/// Parse `/bytes/<n>`, returning the requested length capped at
/// [`MAX_SIZED_BODY`]. `None` for any other path.
fn sized_body_request(path: &[u8]) -> Option<usize> {
    let rest = path.strip_prefix(b"/bytes/")?;
    if rest.is_empty() || !rest.iter().all(u8::is_ascii_digit) {
        return None;
    }
    // Saturating rather than failing: a 30-digit length is a silly request, not
    // a malformed one, and the cap applies either way.
    let n = std::str::from_utf8(rest)
        .ok()?
        .parse::<usize>()
        .unwrap_or(MAX_SIZED_BODY);
    Some(n.min(MAX_SIZED_BODY))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(headers: &[(&'static str, &'static str)]) -> Result<RequestHead, ErrorCode> {
        let list: Vec<Header> = headers
            .iter()
            .map(|(n, v)| {
                Header::new(
                    Bytes::from_static(n.as_bytes()),
                    Bytes::from_static(v.as_bytes()),
                )
            })
            .collect();
        RequestHead::from_headers(&list)
    }

    const MINIMAL: [(&str, &str); 3] = [(":method", "GET"), (":scheme", "https"), (":path", "/")];

    // ---- §8.3 request validation -------------------------------------------

    #[test]
    fn a_minimal_request_validates() {
        let req = get(&MINIMAL).expect("well-formed");
        assert_eq!(&req.method[..], b"GET");
        assert_eq!(&req.scheme[..], b"https");
        assert_eq!(&req.path[..], b"/");
        assert!(req.authority.is_none());
        assert!(
            req.fields.is_empty(),
            "pseudo-headers are not regular fields"
        );
    }

    #[test]
    fn every_required_pseudo_header_is_required() {
        for missing in [":method", ":scheme", ":path"] {
            let kept: Vec<_> = MINIMAL
                .iter()
                .copied()
                .filter(|(n, _)| *n != missing)
                .collect();
            assert_eq!(
                get(&kept).err(),
                Some(ErrorCode::ProtocolError),
                "a request without {missing} is malformed",
            );
        }
    }

    #[test]
    fn pseudo_headers_must_precede_regular_fields() {
        let out_of_order = [
            (":method", "GET"),
            (":scheme", "https"),
            ("accept", "*/*"),
            (":path", "/"),
        ];
        assert_eq!(get(&out_of_order).err(), Some(ErrorCode::ProtocolError));
    }

    #[test]
    fn unknown_and_duplicate_pseudo_headers_are_rejected() {
        let unknown = [
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            (":status", "200"),
        ];
        assert_eq!(get(&unknown).err(), Some(ErrorCode::ProtocolError));

        let duplicate = [
            (":method", "GET"),
            (":method", "POST"),
            (":scheme", "https"),
            (":path", "/"),
        ];
        assert_eq!(get(&duplicate).err(), Some(ErrorCode::ProtocolError));
    }

    #[test]
    fn field_names_must_be_lowercase() {
        let shouty = [
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            ("Accept", "*/*"),
        ];
        assert_eq!(get(&shouty).err(), Some(ErrorCode::ProtocolError));
    }

    #[test]
    fn connection_specific_fields_are_rejected() {
        for name in [
            "connection",
            "keep-alive",
            "proxy-connection",
            "transfer-encoding",
            "upgrade",
        ] {
            let list = [
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/"),
                (name, "x"),
            ];
            assert_eq!(
                get(&list).err(),
                Some(ErrorCode::ProtocolError),
                "{name} has no meaning in HTTP/2",
            );
        }
    }

    #[test]
    fn te_survives_only_to_say_trailers() {
        let ok = [
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            ("te", "trailers"),
        ];
        get(&ok).expect("te: trailers is the one legal value");

        let bad = [
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            ("te", "gzip"),
        ];
        assert_eq!(get(&bad).err(), Some(ErrorCode::ProtocolError));
    }

    #[test]
    fn an_empty_path_is_malformed() {
        let list = [(":method", "GET"), (":scheme", "https"), (":path", "")];
        assert_eq!(get(&list).err(), Some(ErrorCode::ProtocolError));
    }

    #[test]
    fn the_authority_and_regular_fields_survive_validation() {
        let list = [
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/upload?x=1"),
            ("content-type", "text/plain"),
        ];
        let req = get(&list).expect("well-formed");
        assert_eq!(req.authority.as_deref(), Some(&b"example.com"[..]));
        assert_eq!(req.path_only(), b"/upload");
        assert_eq!(req.fields.len(), 1);
    }

    // ---- §8.3 response validation ------------------------------------------

    fn response_of(headers: &[(&'static str, &'static str)]) -> Result<ResponseHead, ErrorCode> {
        let list: Vec<Header> = headers
            .iter()
            .map(|(n, v)| {
                Header::new(
                    Bytes::from_static(n.as_bytes()),
                    Bytes::from_static(v.as_bytes()),
                )
            })
            .collect();
        ResponseHead::from_headers(&list)
    }

    #[test]
    fn a_response_needs_exactly_one_status() {
        let head = response_of(&[(":status", "204"), ("server", "backend")]).expect("well-formed");
        assert_eq!(head.status, 204);
        assert_eq!(head.fields.len(), 1, ":status is not a regular field");

        assert_eq!(
            response_of(&[("server", "backend")]).err(),
            Some(ErrorCode::ProtocolError),
        );
        assert_eq!(
            response_of(&[(":status", "200"), (":status", "204")]).err(),
            Some(ErrorCode::ProtocolError),
        );
    }

    #[test]
    fn a_response_rejects_request_pseudo_headers_and_late_ones() {
        assert_eq!(
            response_of(&[(":status", "200"), (":method", "GET")]).err(),
            Some(ErrorCode::ProtocolError),
        );
        assert_eq!(
            response_of(&[("server", "backend"), (":status", "200")]).err(),
            Some(ErrorCode::ProtocolError),
            "pseudo-headers may not follow a regular field",
        );
    }

    #[test]
    fn a_response_status_must_be_a_three_digit_code() {
        for bad in ["", "OK", "20", "99", "600", "1000"] {
            assert_eq!(
                response_of(&[(":status", Box::leak(bad.to_string().into_boxed_str()))]).err(),
                Some(ErrorCode::ProtocolError),
                "status {bad:?}",
            );
        }
        assert!(
            response_of(&[(":status", "100")])
                .expect("1xx is a real response")
                .is_informational()
        );
    }

    #[test]
    fn a_response_rejects_connection_specific_fields() {
        // The same §8.2.2 rule as requests, and the reason a proxy cannot just
        // relay whatever a careless HTTP/1 backend produced.
        assert_eq!(
            response_of(&[(":status", "200"), ("transfer-encoding", "chunked")]).err(),
            Some(ErrorCode::ProtocolError),
        );
    }

    // ---- forwarding a request head -----------------------------------------

    #[test]
    fn a_request_head_rebuilds_with_pseudo_headers_first() {
        let req = get(&[
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/upload"),
            ("content-type", "text/plain"),
        ])
        .expect("well-formed");
        let forwarded = req.to_headers();
        let names: Vec<&[u8]> = forwarded.iter().map(|h| &h.name[..]).collect();
        assert_eq!(
            names,
            vec![
                &b":method"[..],
                &b":scheme"[..],
                &b":authority"[..],
                &b":path"[..],
                &b"content-type"[..],
            ],
        );
    }

    #[test]
    fn forwarding_preserves_the_never_indexed_flag() {
        // The flag is the client saying "this is a secret"; dropping it on the
        // upstream leg would put a bearer token in a compression table
        // (RFC 7541 §7.1.3).
        let mut header = Header::new(
            Bytes::from_static(b"authorization"),
            Bytes::from_static(b"Bearer hunter2"),
        );
        header.sensitive = true;
        let req = RequestHead::from_headers(&[
            Header::new(Bytes::from_static(b":method"), Bytes::from_static(b"GET")),
            Header::new(Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
            Header::new(Bytes::from_static(b":path"), Bytes::from_static(b"/")),
            header,
        ])
        .expect("well-formed");
        let forwarded = req.to_headers();
        let auth = forwarded
            .iter()
            .find(|h| h.name.as_ref() == b"authorization")
            .expect("still there");
        assert!(auth.sensitive, "never-indexed must survive the round trip");
    }

    // ---- the built-in responder --------------------------------------------

    /// Run one `Echo` exchange and collect everything it emitted.
    fn echo_events(
        req: &[(&'static str, &'static str)],
        end_stream: bool,
        body: &[(&'static [u8], bool)],
    ) -> Vec<ServiceEvent> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut echo = Echo::default();
        echo.attach(tx);
        let id = StreamId::new(1);
        echo.dispatch(id, get(req).expect("well-formed"), end_stream);
        for (chunk, end) in body {
            echo.body(id, Bytes::from_static(chunk), *end);
        }
        let mut out = Vec::new();
        while let Ok(event) = rx.try_recv() {
            out.push(event);
        }
        out
    }

    #[test]
    fn a_sized_path_produces_exactly_that_many_octets() {
        let events = echo_events(
            &[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/bytes/4096"),
            ],
            true,
            &[],
        );
        let [
            ServiceEvent::Head {
                response,
                end_stream: false,
                ..
            },
            ServiceEvent::Data {
                data,
                end_stream: true,
                ..
            },
        ] = &events[..]
        else {
            panic!("expected a head then a body, got {events:?}");
        };
        assert_eq!(response.status, 200);
        assert_eq!(data.len(), 4096);
    }

    #[test]
    fn a_sized_path_is_capped() {
        assert_eq!(
            sized_body_request(b"/bytes/999999999999"),
            Some(MAX_SIZED_BODY)
        );
        // Not a size request at all.
        assert_eq!(sized_body_request(b"/bytes/"), None);
        assert_eq!(sized_body_request(b"/bytes/abc"), None);
        assert_eq!(sized_body_request(b"/"), None);
    }

    #[test]
    fn a_request_with_a_body_gets_it_echoed_and_its_credit_back() {
        let events = echo_events(
            &[(":method", "POST"), (":scheme", "https"), (":path", "/")],
            false,
            &[(b"hello", false), (b"", true)],
        );
        let data: Vec<&Bytes> = events
            .iter()
            .filter_map(|e| match e {
                ServiceEvent::Data { data, .. } => Some(data),
                _ => None,
            })
            .collect();
        assert_eq!(data.iter().map(|d| d.len()).sum::<usize>(), 5);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ServiceEvent::BodyAccepted { n: 5, .. })),
            "a local responder returns the credit immediately: {events:?}",
        );
        assert!(
            matches!(
                events.last(),
                Some(ServiceEvent::Data {
                    end_stream: true,
                    ..
                })
            ),
            "the echo has to end the stream: {events:?}",
        );
    }

    #[test]
    fn a_bodyless_post_still_gets_a_finished_response() {
        // Week 5 answered this with HEADERS and no END_STREAM, then waited for a
        // request body that had already ended — the client hung. A request that
        // is over when it arrives can only be answered with a complete response.
        let events = echo_events(
            &[(":method", "POST"), (":scheme", "https"), (":path", "/")],
            true,
            &[],
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                ServiceEvent::Data {
                    end_stream: true,
                    ..
                } | ServiceEvent::Head {
                    end_stream: true,
                    ..
                }
            )),
            "nothing ended the stream: {events:?}",
        );
    }

    #[test]
    fn a_response_puts_status_before_its_fields() {
        let response = Response {
            status: 431,
            fields: vec![Header::new(
                Bytes::from_static(b"content-length"),
                Bytes::from_static(b"0"),
            )],
        };
        let headers = response.to_headers();
        assert_eq!(&headers[0].name[..], b":status");
        assert_eq!(&headers[0].value[..], b"431");
        assert_eq!(&headers[1].name[..], b"content-length");
    }
}
