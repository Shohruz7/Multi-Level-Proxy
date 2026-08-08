# h2 crate — source study notes

Read of `hyperium/h2` (the crate we use as a differential-test oracle, per ADR
0003). Goal: learn how a production implementation draws its boundaries, then make
our own choices deliberately. Study, don't copy.

## Layer map

```
client.rs / server.rs      public API: handshake() → (SendRequest|Connection) etc.
    │
proto/connection.rs        the connection state machine + main poll loop
proto/streams/*            all per-stream state, shared client/server
proto/{settings,ping_pong,go_away}.rs   connection-level frame handling
    │
codec/                     Codec = FramedRead<FramedWrite<T>>  (bytes ⇄ Frame)
frame/                     one module per frame type + Head, StreamId, Reason
hpack/                     encoder, decoder, table, huffman
```

Observations worth keeping:

- **The codec owns HPACK decoding.** `codec::FramedRead` holds the
  `hpack::Decoder` and produces fully-decoded `Frame::Headers` values; the layers
  above never see raw header block fragments. Consequence: CONTINUATION frames
  don't exist above the codec — `FramedRead` keeps a `Partial { frame, buf,
  continuation_frames_count }` and refuses any interleaved frame while a header
  sequence is open (exactly RFC 9113's contiguity rule). It also caps the number
  of CONTINUATION frames (flood guard) and enforces `max_header_list_size`
  *during* accumulation — the HPACK-bomb guard lives in the reader, not in some
  later validation pass.
- **Length-prefix framing is delegated.** `FramedRead` wraps tokio-util's
  `LengthDelimitedCodec` (3-byte length, 6-byte "skipped" header) so partial-frame
  reassembly across TCP segments is handled by a well-tested utility, and h2's own
  code starts from "here is one complete frame's bytes". Our streaming-parser
  design fork (design doc §3.2) is exactly the choice of whether to hand-roll this.
- **Frames are cheap views over `Bytes`.** `frame::Frame<T = Bytes>` is an enum of
  per-type structs; DATA payloads stay as `Bytes` slices of the read buffer all
  the way through (zero-copy — validates ADR 0007). `Head { kind, flag, stream_id }`
  is the parsed 9-octet header; each frame module owns its `load()`/`encode()` pair
  plus its size/placement validation.
- **Stream state is a table, not tasks.** `proto/streams/store.rs` keeps a
  `slab::Slab<Stream>` + `IndexMap<StreamId, SlabIndex>` — h2 does *not* spawn a
  task per stream. One connection task polls everything; per-stream waker
  registration wakes user-facing handles. The state machine in
  `proto/streams/state.rs` is a small enum (`Idle`, `ReservedLocal/Remote`,
  `Open { local, remote }`, `HalfClosedLocal/Remote`, `Closed(Cause)`) where each
  half is itself a `Peer` sub-state — the open/half-closed duality is modeled as
  two independent directions, which collapses many transition rules into two
  symmetric checks.
- **Send-side scheduling is centralized in `prioritize.rs`.** Three queues:
  streams waiting for socket capacity, streams waiting for flow-control window,
  streams waiting on MAX_CONCURRENT_STREAMS. A subtle correctness comment there:
  pending-open streams must be sent in **stream-ID order**, because sending a
  higher ID implicitly closes lower idle IDs at the peer. Our fair scheduler
  (design doc §4.1) has to respect the same invariant.
- **Flow-control windows are signed.** `proto/streams/flow_control.rs` keeps the
  window as a value that "can go negative if a SETTINGS_INITIAL_WINDOW_SIZE is
  received" — direct confirmation that i64/i32 (not u32) is the right
  representation, since a settings decrease retro-applies to in-flight streams.
- **Errors split exactly along RFC lines.** `proto/error.rs` distinguishes
  connection-level vs stream-level `Reset`; `frame/reason.rs` is the error-code
  table. User-visible `crate::Error` wraps both. Matches our ADR 0008 shape.
- **HPACK accounting**: `hpack/table.rs` charges the RFC's +32 per entry and the
  encoder tracks the decoder's table as a mirror; the `hpack/test/` directory pulls
  fixture-based and fuzz tests — the same fixtures we can reuse as oracle inputs.

## API shape (what our tests will call)

- Server side: `h2::server::handshake(io) → Connection`; `conn.accept() →
  (Request<RecvStream>, SendResponse)`.
- Client side: `h2::client::handshake(io) → (SendRequest, Connection)`; spawn the
  `Connection` (it's the I/O driver future), then `send_request(req, end_of_stream)`.
- The `Connection` future must be polled for *anything* to progress — the
  driver/handle split. Useful for tests: drive h2 against our engine over an
  in-memory duplex pipe (`tokio::io::duplex`).
- For pure codec-level differential tests, `fuzz_bridge.rs` shows the pattern h2
  itself uses: feed raw bytes into `FramedRead` directly, no TCP needed.

## What we deliberately do differently

- **Task-per-stream handlers over bounded mpsc** (design doc §2.2) instead of h2's
  single-task + slab: costs more context switches, but makes backpressure bridging
  (§4.2) explicit — the bounded channel *is* the flow-control coupling. h2's design
  is optimized for being a library; ours for being a legible proxy.
- **HPACK above the codec, not inside it**: we keep the framing codec stateless
  (bytes ⇄ frames with raw header fragments) and put HPACK in the connection
  layer, because the proxy must *re-encode* headers toward the upstream with a
  separate encoder table — a decode-only codec boundary would hide the state we
  need to manage. (Continuity rule then must be enforced in the connection layer.)
- **Hand-rolled reassembly buffer** rather than `LengthDelimitedCodec` — the
  streaming parser is a stated learning goal (§3.2), and we differential-test it
  against h2 to keep it honest.
- No reserved states, no PUSH_PROMISE path at all (push disabled), no priority
  tree (see rfc9218 notes).
