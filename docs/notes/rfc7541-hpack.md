# RFC 7541 — HPACK header compression (condensed notes)

HPACK is the stateful part of HTTP/2: encoder and decoder each maintain a dynamic
table, and a single desynchronization corrupts **every subsequent header block** on
the connection — which is why HPACK failures are always *connection* errors
(`COMPRESSION_ERROR`), never stream errors. Week 4 implements this file end to end.

---

## 1. The model

- A header block is a sequence of *representations*, each emitting one header field.
- Two tables, addressed as one index space:
  - **Static table**: 61 fixed entries (indices 1–61), Appendix A of the RFC.
  - **Dynamic table**: FIFO, newest entry is index 62. Entries are added only by
    the two "with incremental indexing" representations, evicted only by size
    pressure or table-size updates.
- The decoder's dynamic table is driven entirely by the byte stream it receives.
  The encoder maintains a mirror of what the decoder's table must look like. Any
  disagreement (e.g. encoder thinks an entry survived eviction that the decoder
  evicted) = silent corruption. **Test with sequences of header blocks, not single
  blocks** — desyncs only show up on the 2nd+ block.

## 2. Primitives

### Integers (§5.1)

An integer is encoded with an N-bit prefix (N = bits left in the first octet after
the pattern bits):

- If value < 2^N − 1: encode in the prefix directly.
- Else: prefix = all ones (2^N − 1), then the remainder in little-endian base-128
  continuation octets (high bit = "more follows").

```
value = 1337, N = 5:
  prefix   11111  (31)
  1337−31 = 1306 = 0b10100011010
  octets:  00011010 | 0x80 → 10011010, then 00001010
  wire:    xxx11111 10011010 00001010
```

Decoder hazards: unbounded continuation octets (cap the length — 5 continuation
octets covers u32), and overlong encodings.

### Strings (§5.2)

`1 bit Huffman flag | 7-bit-prefix length | data`. If H=1, data is Huffman-coded
(Appendix B table — a static, canonical code built from HTTP header frequency).

Huffman rules that trip people up:
- Padding: fill the final octet with the **most significant bits of the EOS
  symbol** (i.e. all 1s). Padding longer than 7 bits, or padding that isn't the
  EOS prefix → decoding error (connection error).
- The EOS symbol itself appearing in the data → decoding error.
- Encoders should only Huffman-encode when it actually shrinks the string (both
  are legal; measure and pick).

## 3. Dynamic table sizing (§4)

- Entry size = `len(name) + len(value) + 32`. The 32 is the RFC's charge for
  pointers/overhead — get this wrong and encoder/decoder evict at different times
  (the classic desync).
- The table has a **maximum size** — an upper bound signaled out-of-band by HTTP/2's
  `SETTINGS_HEADER_TABLE_SIZE` (default 4096 octets).
- Adding an entry evicts from the **oldest end** until the new entry fits. An entry
  larger than the whole table doesn't error: it empties the table and is *not*
  inserted.
- **Dynamic table size update** (§6.3, pattern `001xxxxx`, 5-bit prefix integer):
  sent *inside* a header block, must appear **at the start** of the block. Required
  after the encoder acknowledges a SETTINGS reduction of HEADER_TABLE_SIZE; the new
  size must be ≤ the SETTINGS value, otherwise → `COMPRESSION_ERROR`. A size update
  may immediately evict entries.

## 4. The four representations (§6)

First-octet bit patterns (the decoder's dispatch table):

| Pattern | Representation | Effect on dynamic table |
|---|---|---|
| `1xxxxxxx` (7-bit index) | **Indexed** — full field from table index | none |
| `01xxxxxx` (6-bit index) | **Literal with incremental indexing** — name by index or literal (index=0), value literal | **inserts** at index 62 |
| `0000xxxx` (4-bit index) | **Literal without indexing** — as above | none |
| `0001xxxx` (4-bit index) | **Literal never indexed** — as above | none, and *must survive re-encoding* |
| `001xxxxx` | Dynamic table size update | may evict |

- Index 0 in an Indexed representation → decoding error.
- **Never-indexed** is the security-relevant one: it tells every hop "do not put
  this in a compression table" — used for `authorization`, `cookie`-like secrets —
  because table hits are observable via length/timing (CRIME-class attacks). An
  intermediary re-encoding the block MUST preserve the never-indexed flag. Our
  proxy re-encodes headers toward the upstream, so this rule applies to us
  directly.
- Encoder policy is free (always-literal is legal); ours: index common fields,
  never-index known-sensitive names, Huffman when shorter.

## 5. Limits and the HPACK bomb (design doc §6)

- A tiny header block can reference huge table entries repeatedly ("HPACK bomb"):
  insert a max-size entry once, then send thousands of 1-octet indexed
  representations for it. Decompressed size is unbounded relative to wire size.
- Guard: enforce `SETTINGS_MAX_HEADER_LIST_SIZE` **while decoding** (sum of
  `name + value + 32` per field, uncompressed) and abort the connection when
  exceeded — do not buffer the whole decoded list first.
- Also cap: total header block size (sum of HEADERS + CONTINUATION fragments)
  before feeding the decoder.

## 6. Test plan hooks (week 4)

- RFC Appendix C carries complete worked vectors: C.1 integers, C.2 single
  representations, C.3 request sequences without Huffman, C.4 with Huffman
  (three consecutive blocks sharing table state — the lockstep test), C.5/C.6
  response sequences with evictions (table size 256). These are the ground truth.
- Differential: encode with ours → decode with `h2`'s hpack, and vice versa, over
  *sequences* of header sets; property-test with random valid header lists.
