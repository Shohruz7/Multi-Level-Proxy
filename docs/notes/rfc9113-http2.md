# RFC 9113 — HTTP/2 (condensed notes)

Scope of these notes: the sections this project actually implements — preface, framing
(§4), frame types (§6), streams/multiplexing (§5), the stream state machine (§5.1),
flow control (§5.2), error handling (§5.4), SETTINGS (§6.5). Server push is out of
scope (we set `ENABLE_PUSH = 0`), so PUSH_PROMISE and the reserved states are noted
only where the state machine requires acknowledging their existence.

---

## 1. Connection establishment and the preface (§3.4)

- ALPN over TLS negotiates the token `h2`. `h2c` (cleartext upgrade) is deprecated in
  9113 and not implemented here.
- After TLS, the **client** sends the fixed 24-octet preface:

  ```
  PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n
  0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a
  ```

  followed by a (possibly empty) SETTINGS frame.
- The **server** preface is just a (possibly empty) SETTINGS frame — no magic bytes.
- Each side must send a SETTINGS ack (SETTINGS frame with ACK flag, empty payload)
  after receiving the peer's SETTINGS. Frames may be sent before the peer's SETTINGS
  arrives, but you must be prepared for the peer's settings to apply retroactively.
- Anything other than a valid preface → connection error `PROTOCOL_ERROR`.

## 2. Frame format (§4)

Every frame starts with a fixed **9-octet header**:

```
+-----------------------------------------------+
|                 Length (24)                   |
+---------------+---------------+---------------+
|   Type (8)    |   Flags (8)   |
+-+-------------+---------------+-------------------------------+
|R|                 Stream Identifier (31)                      |
+=+=============================================================+
|                   Frame Payload (0 ... 2^24-1)              ...
+---------------------------------------------------------------+
```

- **Length**: payload length only (header's 9 octets not counted). Default max is
  2^14 = 16,384; a peer may raise it up to 2^24−1 via `SETTINGS_MAX_FRAME_SIZE`.
  Receiving a frame larger than your advertised limit → `FRAME_SIZE_ERROR`
  (connection error if the frame affects connection state, e.g. SETTINGS).
- **Type**: unknown types MUST be ignored and discarded (extensibility, §4.1 / §5.5).
- **Flags**: semantics are per-type; undefined flags MUST be ignored on receipt and
  sent as zero.
- **R** (reserved bit) of the stream ID: MUST remain unset when sending, MUST be
  ignored when receiving.
- **Stream Identifier**: 31-bit. `0x0` = the connection itself (SETTINGS, PING,
  GOAWAY, connection-level WINDOW_UPDATE).

**Implementation hazard**: frames arrive fragmented across TCP segments. The parser
must buffer until `9 + Length` octets are available and only then advance — partial
reads must never consume input.

## 3. Streams and multiplexing (§5)

- A stream is one bidirectional request/response exchange. Many streams interleave
  on one connection; frames from different streams may be arbitrarily interleaved,
  **except** a HEADERS/CONTINUATION sequence, which must be contiguous on the
  connection (no other frames in between — this is the one place HTTP/2 blocks the
  whole connection).
- **Stream ID rules (§5.1.1)**:
  - Client-initiated streams are odd; server-initiated (push only) are even.
  - IDs must strictly increase. Receiving a HEADERS on an ID numerically ≤ the
    highest seen → connection error `PROTOCOL_ERROR`.
  - An ID one side has skipped past is implicitly **closed** (receiving a frame for
    it, other than PRIORITY, is an error).
  - IDs are never reused; a connection that exhausts IDs (2^31−1) must open a new
    connection (client) or send GOAWAY (server).
- **Concurrency (§5.1.2)**: `SETTINGS_MAX_CONCURRENT_STREAMS` bounds streams in the
  open or half-closed states. Streams in reserved states don't count. Exceeding the
  peer's advertised limit → they respond with `PROTOCOL_ERROR` or `REFUSED_STREAM`
  (REFUSED_STREAM signals "safe to retry").

## 4. Stream state machine (§5.1)

```
                             +--------+
                     send PP |        | recv PP
                    ,--------+  idle  +--------.
                   /         |        |         \
                  v          +--------+          v
           +----------+          |           +----------+
           |          |          | send H /  |          |
    ,------+ reserved |          | recv H    | reserved +------.
    |      | (local)  |          |           | (remote) |      |
    |      +---+------+          v           +------+---+      |
    |          |             +--------+             |          |
    |          |     recv ES |        | send ES     |          |
    |   send H |     ,-------+  open  +-------.     | recv H   |
    |          |    /        |        |        \    |          |
    |          v   v         +---+----+         v   v          |
    |      +----------+          |           +----------+      |
    |      |   half-  |          |           |   half-  |      |
    |      |  closed  |          | send R /  |  closed  |      |
    |      | (remote) |          | recv R    | (local)  |      |
    |      +----+-----+          |           +-----+----+      |
    |           |                |                 |           |
    |           | send ES /      |        recv ES /|           |
    |           | send R /       v         send R /|           |
    |           | recv R     +--------+   recv R   |           |
    | send R /  `----------->|        |<-----------'  send R / |
    | recv R                 | closed |               recv R   |
    `------------------------|        |<-----------------------'
                             +--------+

    H  = HEADERS (with implied CONTINUATIONs)     ES = END_STREAM flag
    PP = PUSH_PROMISE (disabled in this project)  R  = RST_STREAM
```

With push disabled, the states we actually traverse: **idle → open →
half-closed(local|remote) → closed**, plus the RST_STREAM shortcut from any
non-idle state straight to closed.

- **idle**: only HEADERS (or PRIORITY) is legal. Anything else → connection error
  `PROTOCOL_ERROR` (the peer referenced a stream that was never opened).
- **open**: both sides may send anything. END_STREAM moves the *sender* to
  half-closed(local) and tells the receiver the peer is half-closed(remote).
- **half-closed(remote)**: peer said END_STREAM; receiving further DATA/HEADERS
  from them → stream error `STREAM_CLOSED`. We may still send.
- **half-closed(local)**: we said END_STREAM; we may only send WINDOW_UPDATE,
  PRIORITY, RST_STREAM; we must keep receiving.
- **closed**: brief grace period — frames may legitimately arrive after we send
  RST_STREAM (they were in flight). RFC allows ignoring them "for a period";
  receiving DATA on a closed stream still counts against **connection** flow
  control (the bytes were sent regardless).
- Illegal transitions: the RFC names the required error code per case; the two that
  matter most: frames on idle streams → connection `PROTOCOL_ERROR`; frames after
  END_STREAM → stream `STREAM_CLOSED`.

## 5. Flow control (§5.2, §6.9)

- Applies to **DATA payload bytes only** (including padding + pad-length octet).
  HEADERS, CONTINUATION, and all control frames are exempt.
- **Two levels simultaneously**: a per-stream window and a connection window.
  Sending n DATA payload octets decrements both. Either hitting ≤0 blocks sending.
- Initial windows: both default to **65,535** octets. The per-stream initial window
  is changed via `SETTINGS_INITIAL_WINDOW_SIZE` (affects all streams, and
  retroactively adjusts existing streams' windows by the delta — windows can go
  **negative** this way and that's legal). The **connection** window can only be
  grown via WINDOW_UPDATE, never via SETTINGS.
- WINDOW_UPDATE carries a 31-bit positive increment (0 is a `PROTOCOL_ERROR`);
  stream 0 = connection window. Window may never exceed 2^31−1; overflow →
  `FLOW_CONTROL_ERROR` (connection error if on stream 0, stream error otherwise).
- Receiver-driven: only the receiver of data sends WINDOW_UPDATE. This is the hook
  the proxy uses for backpressure bridging (design doc §4.2): withhold the upstream's
  WINDOW_UPDATE until the client drains, and the upstream provably stops sending.
- **Deadlock hazard**: forgetting to replenish the *connection* window while
  replenishing stream windows stalls every stream at once. Account for the two
  separately and test both.

## 6. Frame types (§6) — the ones we implement

| Type | Code | Stream ID | Flags | Payload |
|---|---|---|---|---|
| DATA | 0x00 | ≠0 | END_STREAM(0x1), PADDED(0x8) | [pad len][data][padding] |
| HEADERS | 0x01 | ≠0 | END_STREAM(0x1), END_HEADERS(0x4), PADDED(0x8), PRIORITY(0x20) | [pad][prio: 5 octets][header block fragment][padding] |
| PRIORITY | 0x02 | ≠0 | — | 5 octets (deprecated; tolerate + ignore) |
| RST_STREAM | 0x03 | ≠0 | — | 4-octet error code |
| SETTINGS | 0x04 | =0 | ACK(0x1) | n × (16-bit id + 32-bit value) |
| PUSH_PROMISE | 0x05 | ≠0 | — | not implemented (push disabled) |
| PING | 0x06 | =0 | ACK(0x1) | exactly 8 octets, echoed verbatim |
| GOAWAY | 0x07 | =0 | — | last-stream-id (31) + error code (32) + debug data |
| WINDOW_UPDATE | 0x08 | 0 or ≠0 | — | 1 reserved bit + 31-bit increment |
| CONTINUATION | 0x09 | ≠0 | END_HEADERS(0x4) | header block fragment |

Size/placement rules worth memorizing (each violated rule names its error):

- DATA on stream 0 → connection `PROTOCOL_ERROR`. Padding length ≥ payload length →
  connection `PROTOCOL_ERROR`.
- RST_STREAM payload ≠ 4 octets → connection `FRAME_SIZE_ERROR`. RST_STREAM on an
  idle stream → connection `PROTOCOL_ERROR`.
- SETTINGS with stream ID ≠ 0, or length not a multiple of 6 → connection error.
  SETTINGS ACK with non-empty payload → `FRAME_SIZE_ERROR`.
- PING ≠ 8 octets → `FRAME_SIZE_ERROR`; PING on stream ≠0 → `PROTOCOL_ERROR`.
- WINDOW_UPDATE length ≠ 4 → `FRAME_SIZE_ERROR`.
- CONTINUATION must follow HEADERS/CONTINUATION on the **same stream** without
  END_HEADERS set; anything else interleaved → connection `PROTOCOL_ERROR`.

## 7. SETTINGS (§6.5)

Parameters (id → meaning, initial value, bounds):

| Id | Name | Initial | Notes |
|---|---|---|---|
| 0x01 | HEADER_TABLE_SIZE | 4,096 | Max HPACK dynamic table the *sender of the setting* will use for decoding |
| 0x02 | ENABLE_PUSH | 1 | We advertise **0**; value >1 → `PROTOCOL_ERROR` |
| 0x03 | MAX_CONCURRENT_STREAMS | unlimited | We advertise a real limit (abuse guard) |
| 0x04 | INITIAL_WINDOW_SIZE | 65,535 | >2^31−1 → `FLOW_CONTROL_ERROR` |
| 0x05 | MAX_FRAME_SIZE | 16,384 | Legal range [2^14, 2^24−1]; outside → `PROTOCOL_ERROR` |
| 0x06 | MAX_HEADER_LIST_SIZE | unlimited | We advertise a real limit (HPACK-bomb guard, design doc §6) |

- Unknown setting ids MUST be ignored.
- Settings apply in the order they appear; the receiver must send the ACK promptly.
  A sender may treat a missing ack after a reasonable timeout as
  `SETTINGS_TIMEOUT` (connection error).
- SETTINGS is **not** negotiation: each direction is independent. What you send
  constrains what the *peer* may do to you.

## 8. Error handling (§5.4)

The distinction that shapes the whole error model (→ ADR 0008):

- **Connection error**: the framing/compression state is unrecoverable (e.g. HPACK
  desync, malformed frame header, protocol violation on stream 0). Send GOAWAY with
  the last successfully processed stream ID + error code, then close the TCP
  connection. All other streams die with it.
- **Stream error**: one exchange is bad but the connection is fine. Send RST_STREAM
  with the error code; other streams proceed. Never send more than one RST_STREAM
  per stream.

Error codes (§7):

| Code | Name | Typical trigger |
|---|---|---|
| 0x0 | NO_ERROR | graceful GOAWAY |
| 0x1 | PROTOCOL_ERROR | generic protocol violation |
| 0x2 | INTERNAL_ERROR | implementation fault |
| 0x3 | FLOW_CONTROL_ERROR | window overflow / violated |
| 0x4 | SETTINGS_TIMEOUT | SETTINGS not acked in time |
| 0x5 | STREAM_CLOSED | frame on a half-closed(remote)/closed stream |
| 0x6 | FRAME_SIZE_ERROR | frame length invalid for its type |
| 0x7 | REFUSED_STREAM | stream declined before processing (safe to retry) |
| 0x8 | CANCEL | endpoint no longer wants this stream |
| 0x9 | COMPRESSION_ERROR | HPACK state cannot be maintained |
| 0xa | CONNECT_ERROR | CONNECT target connection failed |
| 0xb | ENHANCE_YOUR_CALM | peer is misbehaving (used by Rapid-Reset mitigations) |
| 0xc | INADEQUATE_SECURITY | TLS properties unacceptable |
| 0xd | HTTP_1_1_REQUIRED | peer should retry over HTTP/1.1 |

Unknown error codes must be treated as INTERNAL_ERROR-equivalent (don't reject).

GOAWAY mechanics (§6.8): `last-stream-id` promises streams ≤ id were (or may have
been) processed; higher ones definitely were not — the client may retry those
elsewhere. Graceful drain = send GOAWAY with last-stream-id = 2^31−1 (or current
max), finish in-flight streams, optionally send a second GOAWAY with the real last
ID, then close. This is the week-7 drain mechanism.

## 9. HTTP semantics on top (§8) — the parts a proxy must enforce

- Requests use pseudo-headers `:method`, `:scheme`, `:authority`, `:path`;
  responses use `:status`. Pseudo-headers must precede regular fields; unknown or
  misplaced pseudo-headers → malformed (stream error `PROTOCOL_ERROR`).
- Field names must be lowercase. Connection-specific headers (`connection`,
  `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`) are forbidden;
  `te` is allowed only with value `trailers`. A proxy must strip/reject these when
  bridging.
- If `content-length` is present it must match the sum of DATA payload lengths →
  otherwise malformed.
