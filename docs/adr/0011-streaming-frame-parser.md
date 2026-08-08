# ADR 0011 — A streaming frame parser over a reassembly buffer

Status: accepted · Date: 2026-07-06 · Design doc: §3.2 · RFC 9113 §4, §6

## Context

Week 3's first real design fork (design doc §3.2): how the codec turns a TCP
byte stream into frames. TCP gives no message boundaries, so a 9-octet frame
header can arrive split across two segments, and a 16 KiB DATA payload can
arrive in a dozen. The parser has to be correct for every split, and it runs on
a public listener, so it also has to be correct for every *hostile* split.

The alternative on the table was a simpler buffered parser: wait until a whole
frame is known to be present, then hand the parser a contiguous slice it can
index freely.

## Decision

**A streaming decoder over a caller-owned reassembly buffer**, with a strict
consume-or-nothing contract:

```rust
fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, FrameError>
```

- `Ok(None)` — `buf` does not yet hold a complete frame. **Nothing is
  consumed**, so the caller reads more bytes and retries with the same buffer.
- `Ok(Some(frame))` — exactly one frame's octets are removed from the front.
- `Err(_)` — a framing violation; every variant carries the connection
  [`ErrorCode`] the RFC mandates, via `FrameError::code()`.

Four consequences worth recording, because each was a decision and not a
detail:

1. **The length check precedes buffering the payload.** A header claiming more
   than `SETTINGS_MAX_FRAME_SIZE` is rejected the moment the header is
   readable, so a peer cannot make us hold 16 MB by lying about a length it
   never intends to send.
2. **Unknown frame types are consumed and skipped inside `decode`**, which
   loops to the next frame rather than surfacing them. §4.1 requires ignoring
   unrecognized types; doing it in the codec means no caller can forget to.
   PUSH_PROMISE is the deliberate exception — the proxy advertises
   `ENABLE_PUSH = 0`, so §8.4 makes receiving one a `PROTOCOL_ERROR`, not
   something to ignore. The deprecated PRIORITY (0x2) *is* skipped.
3. **Padding and priority are normalized away on decode.** DATA/HEADERS padding
   is validated (padding ≥ payload → `PROTOCOL_ERROR`, §6.1) and stripped;
   HEADERS' priority block is skipped, because RFC 9113 deprecates the priority
   scheme and the proxy models no dependency tree. The encoder emits neither, so
   `decode(encode(f)) == f` holds for every frame the engine builds.
4. **`FrameError` is connection-scoped, by design.** Violations the RFC scopes
   to a single stream — a zero WINDOW_UPDATE increment is the clean example,
   which §6.9 makes a *stream* error on a stream but a *connection* error on
   stream 0 — are left to `conn`, because choosing between them needs stream
   state the framing layer does not have and should not acquire (ADR 0008).

Backed by `bytes::BytesMut`/`Bytes`, so splitting a frame off the buffer and
slicing a payload out of it are refcount bumps, not copies (ADR 0007).

## Rejected alternatives

- **A buffered parser** that waits for a whole frame, then parses a contiguous
  slice. Simpler per-type code, but it needs a second buffer and a copy per
  frame on the hot path, and it still has to implement the same "is a whole
  frame here yet?" logic — so it pays a copy to avoid nothing.
- **`tokio_util::codec::Decoder`.** The same shape as what we built, and it
  would have been reasonable. Rejected because the framing layer is the
  hand-built part of this project — deriving the reassembly discipline from the
  RFC *is* the exercise — and because it would add a dependency for one trait.
- **Surfacing unknown frames to the caller** as `Frame::Unknown`. Pushes an
  §4.1 obligation onto every call site, and the engine has nothing useful to do
  with one.

## Consequences

- The contract is total, which is exactly what the fuzz target asserts: for any
  input, `decode` returns `Err`, `Ok(None)`, or `Ok(Some(..))` — never a panic.
  Week 3's unshaped target ran **14.2M executions with zero crashes**.
- The consume-or-nothing rule is property-tested directly: feeding a frame one
  octet at a time must yield `Ok(None)` with the buffer untouched until the
  final octet arrives.
- The same buffer is reused across reads for the life of a connection, so a
  steady-state connection does no per-frame allocation for reassembly.
- Unknown-type skipping is invisible to `conn`, which is why the connection
  layer's frame loop has no "ignore this" branch.
