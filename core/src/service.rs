//! The request/response seam: what answers a stream.
//!
//! A neutral module rather than part of [`crate::conn`] or [`crate::proxy`],
//! because both sides need it and `conn` depending on `proxy` would read
//! backwards. Week 5 ships [`Echo`], a built-in responder that makes the engine
//! a working HTTP/2 server; week 6's [`crate::proxy::Proxy`] implements the same
//! trait and the connection layer does not notice the difference.
//!
//! Also home of **message-semantics validation** (RFC 9113 §8.3):
//! [`RequestHead::from_headers`] turns a decoded HPACK header list into a
//! request or rejects it. HPACK decodes faithfully and judges nothing — the
//! judgement belongs here, one layer up, where "this is not a well-formed HTTP
//! request" is a *stream* error and the other 200 streams on the connection
//! carry on.

use bytes::Bytes;

use crate::conn::ErrorCode;
use crate::hpack::Header;

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

/// How a response body is produced.
#[derive(Clone, Debug)]
pub enum Body {
    /// No body; the response HEADERS carries END_STREAM.
    Empty,
    /// A body known in full up front.
    Fixed(Bytes),
    /// Mirror the request body back as it arrives. The connection feeds inbound
    /// DATA straight into the send queue, which is also the smallest possible
    /// rehearsal for week 6's proxying.
    Echo,
}

/// A response to send on one stream.
#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub fields: Vec<Header>,
    pub body: Body,
}

impl Response {
    /// A response with no fields beyond `:status`.
    pub fn status(status: u16, body: Body) -> Self {
        Response {
            status,
            fields: Vec::new(),
            body,
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

/// What answers a request on a stream.
///
/// Synchronous on purpose. A sized body needs no I/O, a sync trait is dyn-safe
/// and trivially testable, and an `async fn` here would promise a concurrency
/// story that does not exist yet — the connection is one task, so awaiting a
/// responder would block every other stream on it. Week 6 makes this async and
/// runs it on a per-stream task, because a real upstream *can* be slow; that is
/// a planned change, not an oversight.
pub trait Service {
    fn respond(&mut self, req: &RequestHead) -> Response;
}

/// The built-in responder: enough of a server to prove the engine multiplexes.
///
/// - `/bytes/<n>` → 200 with exactly `n` octets. Deterministic response sizes
///   are what let the flow-control and interleaving tests assert on byte counts,
///   and what gives week 8's `h2load` a target it can size per profile.
/// - anything else → 200 echoing the request body, or `default_body` octets when
///   the request had none.
#[derive(Clone, Copy, Debug)]
pub struct Echo {
    default_body: usize,
}

impl Echo {
    pub const fn new(default_body: usize) -> Self {
        Echo { default_body }
    }
}

impl Default for Echo {
    fn default() -> Self {
        Echo::new(1024)
    }
}

impl Service for Echo {
    fn respond(&mut self, req: &RequestHead) -> Response {
        if let Some(n) = sized_body_request(req.path_only()) {
            let body = if n == 0 {
                Body::Empty
            } else {
                Body::Fixed(Bytes::from(vec![b'x'; n]))
            };
            return Response {
                status: 200,
                fields: vec![Header::new(
                    Bytes::from_static(b"content-length"),
                    Bytes::from(n.to_string()),
                )],
                body,
            };
        }

        // A request that carries a body gets it back; one that does not gets the
        // configured filler, so a plain `curl /` still exercises the send path.
        if req.method.as_ref() == b"GET" || req.method.as_ref() == b"HEAD" {
            Response::status(200, Body::Fixed(Bytes::from(vec![b'x'; self.default_body])))
        } else {
            Response::status(200, Body::Echo)
        }
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

    // ---- the built-in responder --------------------------------------------

    #[test]
    fn a_sized_path_produces_exactly_that_many_octets() {
        let req = get(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/bytes/4096"),
        ])
        .expect("well-formed");
        let response = Echo::default().respond(&req);
        assert_eq!(response.status, 200);
        let Body::Fixed(body) = response.body else {
            panic!("expected a fixed body");
        };
        assert_eq!(body.len(), 4096);
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
    fn a_request_with_a_body_gets_it_echoed() {
        let req =
            get(&[(":method", "POST"), (":scheme", "https"), (":path", "/")]).expect("well-formed");
        assert!(matches!(Echo::default().respond(&req).body, Body::Echo));
    }

    #[test]
    fn a_response_puts_status_before_its_fields() {
        let response = Response {
            status: 431,
            fields: vec![Header::new(
                Bytes::from_static(b"content-length"),
                Bytes::from_static(b"0"),
            )],
            body: Body::Empty,
        };
        let headers = response.to_headers();
        assert_eq!(&headers[0].name[..], b":status");
        assert_eq!(&headers[0].value[..], b"431");
        assert_eq!(&headers[1].name[..], b"content-length");
    }
}
