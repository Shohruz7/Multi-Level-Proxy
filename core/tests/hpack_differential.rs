//! Differential tests for HPACK: our encoder and decoder against the `h2`
//! crate, across a *sequence* of messages on one connection.
//!
//! This is the week-4 milestone, and it is deliberately not shaped like
//! `differential.rs`. There, the claim is byte-exactness: a frame has one legal
//! encoding, so our octets must equal h2's. HPACK has no such property — an
//! encoder may represent a field as an index, a literal with indexing, or a
//! literal without it, and all three are correct. So the claim here is
//! *semantic* equality, made over a sequence rather than a single block.
//!
//! The sequence is the point (design doc §3.3). Encoder and decoder each carry a
//! dynamic table that must track the peer's exactly; a single missed insertion
//! or eviction desyncs the tables, and every block after it decodes to garbage
//! — silently, and possibly only under load. So the session below sends four
//! requests over one connection, with repeats that force h2 to index into the
//! table it built, and answers each with headers our encoder compressed against
//! *its* table. A drift on either side fails a later message, not the one that
//! caused it, which is exactly the failure mode being guarded against.

mod support;

use bytes::BytesMut;
use tokio::io::DuplexStream;

use h2proxy_core::conn::Settings;
use h2proxy_core::frame::Frame;
use h2proxy_core::hpack::{Header, HpackDecoder, HpackEncoder};

use support::{Oracle, TIMEOUT};

/// How many requests the scripted client sends.
const REQUESTS: usize = 4;

/// Long enough to overflow one 16,384-octet frame, so the fourth request's
/// header block arrives split across HEADERS + CONTINUATION and has to be
/// reassembled before HPACK sees it.
const LONG_VALUE_LEN: usize = 20_000;

/// The response header list for request `n`, built by our encoder and decoded
/// by h2.
fn response_headers(n: usize) -> Vec<Header> {
    vec![
        Header::new(":status", "200"),
        Header::new("content-type", "text/plain"),
        Header::new("x-response", n.to_string()),
    ]
}

/// The four requests, as (path, extra headers) — mirrored by the assertions on
/// the server side.
fn long_value() -> String {
    "a".repeat(LONG_VALUE_LEN)
}

/// Header lists compare as sorted pairs: HPACK preserves field order, but which
/// order `h2` emits its pseudo-headers in is its business, not our decoder's.
fn normalized(headers: &[Header]) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = headers
        .iter()
        .map(|h| {
            (
                String::from_utf8_lossy(&h.name).into_owned(),
                String::from_utf8_lossy(&h.value).into_owned(),
            )
        })
        .collect();
    pairs.sort();
    pairs
}

fn expected(path: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut pairs = vec![
        (":method".to_owned(), "GET".to_owned()),
        (":scheme".to_owned(), "https".to_owned()),
        (":authority".to_owned(), "example.com".to_owned()),
        (":path".to_owned(), path.to_owned()),
    ];
    pairs.extend(extra.iter().map(|&(n, v)| (n.to_owned(), v.to_owned())));
    pairs.sort();
    pairs
}

/// The client half: four requests on one connection, each response checked.
async fn h2_client_script(io: DuplexStream) {
    let (send_request, connection) = h2::client::handshake(io).await.expect("h2 handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut send_request = send_request;

    for n in 1..=REQUESTS {
        let mut ready = send_request.ready().await.expect("client ready");

        let request = match n {
            // Requests 1 and 2 are identical, so h2 must compress the second
            // almost entirely out of the dynamic table it built for the first.
            1 | 2 => http::Request::builder()
                .uri("https://example.com/")
                .header("accept", "*/*")
                .body(())
                .expect("build request"),
            3 => {
                let mut request = http::Request::builder()
                    .uri("https://example.com/private")
                    .body(())
                    .expect("build request");
                // Marking the value sensitive is what makes h2 emit the
                // never-indexed representation (RFC 7541 §7.1.3) — the one
                // our decoder must report back as `sensitive`.
                let mut value = http::HeaderValue::from_static("Bearer hunter2");
                value.set_sensitive(true);
                request
                    .headers_mut()
                    .insert(http::header::AUTHORIZATION, value);
                request
            }
            _ => http::Request::builder()
                .uri("https://example.com/big")
                .header("x-long", long_value())
                .body(())
                .expect("build request"),
        };

        let (response, _stream) = ready.send_request(request, true).expect("send_request");
        let response = response.await.expect("response headers");

        assert_eq!(response.status(), 200, "request {n}: status");
        let headers = response.headers();
        assert_eq!(
            headers.get("content-type").map(|v| v.as_bytes()),
            Some(&b"text/plain"[..]),
            "request {n}: content-type",
        );
        assert_eq!(
            headers.get("x-response").map(|v| v.as_bytes()),
            Some(n.to_string().as_bytes()),
            "request {n}: x-response",
        );
        send_request = ready;
    }

    drop(send_request);
    let _ = driver.await;
}

#[tokio::test]
async fn header_sets_round_trip_across_a_multi_message_sequence() {
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let client = tokio::spawn(h2_client_script(client_io));

    let session = async {
        let mut oracle = Oracle::new(server_io);
        // Bounded exactly as the connection layer bounds its own decoder: by
        // what we advertise, not by what we hope to receive.
        let local = Settings::server();
        let mut decoder = HpackDecoder::new(
            local.header_table_size as usize,
            local.max_header_list_size.map(|n| n as usize),
        );
        let mut encoder = HpackEncoder::new(Settings::default().header_table_size as usize);

        oracle.send(&local.to_frame()).await;
        oracle.read_preface().await;

        let mut block = BytesMut::new();
        let mut decoded: Vec<Vec<Header>> = Vec::new();
        let mut request_block_sizes: Vec<usize> = Vec::new();
        let mut response_block_sizes: Vec<usize> = Vec::new();

        while let Some((frame, _raw)) = oracle.next_frame().await {
            match &frame {
                Frame::Settings { ack: false, params } => {
                    let mut peer = Settings::default();
                    peer.apply(params).expect("h2 sent valid SETTINGS");
                    // Their table bound governs our encoder (RFC 7541 §6.3).
                    encoder.set_max_table_size(peer.header_table_size as usize);
                    oracle
                        .send(&Frame::Settings {
                            ack: true,
                            params: Vec::new(),
                        })
                        .await;
                }
                Frame::Headers {
                    stream_id,
                    block: fragment,
                    end_headers,
                    ..
                }
                | Frame::Continuation {
                    stream_id,
                    block: fragment,
                    end_headers,
                } => {
                    block.extend_from_slice(fragment);
                    if !*end_headers {
                        continue;
                    }
                    let whole = block.split().freeze();
                    request_block_sizes.push(whole.len());
                    let headers = decoder
                        .decode(&whole)
                        .expect("our decoder rejected a block h2 produced");
                    decoded.push(headers);

                    let mut out = BytesMut::new();
                    encoder.encode(&response_headers(decoded.len()), &mut out);
                    response_block_sizes.push(out.len());
                    oracle
                        .send(&Frame::Headers {
                            stream_id: *stream_id,
                            block: out.freeze(),
                            end_stream: true,
                            end_headers: true,
                        })
                        .await;
                }
                _ => {}
            }
        }

        assert!(
            block.is_empty(),
            "a header block was left unterminated: {} octets",
            block.len(),
        );
        (decoded, request_block_sizes, response_block_sizes)
    };

    let (decoded, request_blocks, response_blocks) = tokio::time::timeout(TIMEOUT, session)
        .await
        .expect("the HPACK differential session did not finish in time");

    tokio::time::timeout(TIMEOUT, client)
        .await
        .expect("the h2 client did not finish")
        .expect("the h2 client panicked");

    assert_eq!(decoded.len(), REQUESTS, "every request must have decoded");

    // Each request's header list, exactly as sent.
    assert_eq!(
        normalized(&decoded[0]),
        expected("/", &[("accept", "*/*")]),
        "first request",
    );
    assert_eq!(
        normalized(&decoded[1]),
        expected("/", &[("accept", "*/*")]),
        "second request (compressed against the table the first built)",
    );
    assert_eq!(
        normalized(&decoded[2]),
        expected("/private", &[("authorization", "Bearer hunter2")]),
        "third request",
    );
    let long = long_value();
    assert_eq!(
        normalized(&decoded[3]),
        expected("/big", &[("x-long", long.as_str())]),
        "fourth request, reassembled across HEADERS + CONTINUATION",
    );

    // The never-indexed representation must survive decoding as `sensitive`,
    // because week 6 has to keep such a field out of the *upstream* table too.
    let authorization = decoded[2]
        .iter()
        .find(|h| h.name == "authorization")
        .expect("the third request carried an authorization header");
    assert!(
        authorization.sensitive,
        "a never-indexed field must decode as sensitive",
    );
    assert!(
        decoded[0].iter().all(|h| !h.sensitive),
        "ordinary fields must not be marked sensitive",
    );

    // Proof the dynamic tables actually did work, on both sides: two identical
    // requests do not produce two identical blocks, and neither do two nearly
    // identical responses.
    assert!(
        request_blocks[1] < request_blocks[0],
        "h2 did not compress the repeated request against its dynamic table \
         ({} then {} octets) — our decoder may have been following a table that \
         was never populated",
        request_blocks[0],
        request_blocks[1],
    );
    assert!(
        response_blocks[1] < response_blocks[0],
        "our encoder did not index into its own dynamic table ({} then {} octets)",
        response_blocks[0],
        response_blocks[1],
    );
}
