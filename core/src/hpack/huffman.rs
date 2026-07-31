//! The RFC 7541 Appendix B Huffman code, and the encoder/decoder built on it.
//!
//! The 257-entry code table below is transcribed from the RFC's Appendix B and
//! is the single source of truth in this module: the decoder's state machine is
//! *derived* from it at first use rather than written out separately, so the two
//! cannot drift apart.
//!
//! Decoding uses a nibble-indexed state machine (ADR 0012). The code is a
//! complete prefix code over 257 symbols, so its tree has exactly 256 internal
//! nodes; each becomes a state, and each state holds the 16 transitions for the
//! next four bits. Two table lookups per octet, no per-bit branching.

use std::sync::LazyLock;

use bytes::{BufMut, BytesMut};

use super::HpackError;

/// The end-of-string symbol. It is only ever a *padding* prefix: a Huffman
/// string that actually decodes it is malformed (RFC 7541 §5.2).
const EOS: u16 = 256;

/// `(code, bit length)` per symbol, indexed by the symbol's octet value, with
/// [`EOS`] last. Transcribed from RFC 7541 Appendix B; the code is aligned to
/// the least significant bit of `code`.
#[rustfmt::skip]
const CODES: [(u32, u8); 257] = [
    (0x00001ff8, 13), //   0 0
    (0x007fffd8, 23), //   1 1
    (0x0fffffe2, 28), //   2 2
    (0x0fffffe3, 28), //   3 3
    (0x0fffffe4, 28), //   4 4
    (0x0fffffe5, 28), //   5 5
    (0x0fffffe6, 28), //   6 6
    (0x0fffffe7, 28), //   7 7
    (0x0fffffe8, 28), //   8 8
    (0x00ffffea, 24), //   9 9
    (0x3ffffffc, 30), //  10 10
    (0x0fffffe9, 28), //  11 11
    (0x0fffffea, 28), //  12 12
    (0x3ffffffd, 30), //  13 13
    (0x0fffffeb, 28), //  14 14
    (0x0fffffec, 28), //  15 15
    (0x0fffffed, 28), //  16 16
    (0x0fffffee, 28), //  17 17
    (0x0fffffef, 28), //  18 18
    (0x0ffffff0, 28), //  19 19
    (0x0ffffff1, 28), //  20 20
    (0x0ffffff2, 28), //  21 21
    (0x3ffffffe, 30), //  22 22
    (0x0ffffff3, 28), //  23 23
    (0x0ffffff4, 28), //  24 24
    (0x0ffffff5, 28), //  25 25
    (0x0ffffff6, 28), //  26 26
    (0x0ffffff7, 28), //  27 27
    (0x0ffffff8, 28), //  28 28
    (0x0ffffff9, 28), //  29 29
    (0x0ffffffa, 28), //  30 30
    (0x0ffffffb, 28), //  31 31
    (0x00000014,  6), //  32 ' '
    (0x000003f8, 10), //  33 '!'
    (0x000003f9, 10), //  34 '"'
    (0x00000ffa, 12), //  35 '#'
    (0x00001ff9, 13), //  36 '$'
    (0x00000015,  6), //  37 '%'
    (0x000000f8,  8), //  38 '&'
    (0x000007fa, 11), //  39 "'"
    (0x000003fa, 10), //  40 '('
    (0x000003fb, 10), //  41 ')'
    (0x000000f9,  8), //  42 '*'
    (0x000007fb, 11), //  43 '+'
    (0x000000fa,  8), //  44 ','
    (0x00000016,  6), //  45 '-'
    (0x00000017,  6), //  46 '.'
    (0x00000018,  6), //  47 '/'
    (0x00000000,  5), //  48 '0'
    (0x00000001,  5), //  49 '1'
    (0x00000002,  5), //  50 '2'
    (0x00000019,  6), //  51 '3'
    (0x0000001a,  6), //  52 '4'
    (0x0000001b,  6), //  53 '5'
    (0x0000001c,  6), //  54 '6'
    (0x0000001d,  6), //  55 '7'
    (0x0000001e,  6), //  56 '8'
    (0x0000001f,  6), //  57 '9'
    (0x0000005c,  7), //  58 ':'
    (0x000000fb,  8), //  59 ';'
    (0x00007ffc, 15), //  60 '<'
    (0x00000020,  6), //  61 '='
    (0x00000ffb, 12), //  62 '>'
    (0x000003fc, 10), //  63 '?'
    (0x00001ffa, 13), //  64 '@'
    (0x00000021,  6), //  65 'A'
    (0x0000005d,  7), //  66 'B'
    (0x0000005e,  7), //  67 'C'
    (0x0000005f,  7), //  68 'D'
    (0x00000060,  7), //  69 'E'
    (0x00000061,  7), //  70 'F'
    (0x00000062,  7), //  71 'G'
    (0x00000063,  7), //  72 'H'
    (0x00000064,  7), //  73 'I'
    (0x00000065,  7), //  74 'J'
    (0x00000066,  7), //  75 'K'
    (0x00000067,  7), //  76 'L'
    (0x00000068,  7), //  77 'M'
    (0x00000069,  7), //  78 'N'
    (0x0000006a,  7), //  79 'O'
    (0x0000006b,  7), //  80 'P'
    (0x0000006c,  7), //  81 'Q'
    (0x0000006d,  7), //  82 'R'
    (0x0000006e,  7), //  83 'S'
    (0x0000006f,  7), //  84 'T'
    (0x00000070,  7), //  85 'U'
    (0x00000071,  7), //  86 'V'
    (0x00000072,  7), //  87 'W'
    (0x000000fc,  8), //  88 'X'
    (0x00000073,  7), //  89 'Y'
    (0x000000fd,  8), //  90 'Z'
    (0x00001ffb, 13), //  91 '['
    (0x0007fff0, 19), //  92 '\\'
    (0x00001ffc, 13), //  93 ']'
    (0x00003ffc, 14), //  94 '^'
    (0x00000022,  6), //  95 '_'
    (0x00007ffd, 15), //  96 '`'
    (0x00000003,  5), //  97 'a'
    (0x00000023,  6), //  98 'b'
    (0x00000004,  5), //  99 'c'
    (0x00000024,  6), // 100 'd'
    (0x00000005,  5), // 101 'e'
    (0x00000025,  6), // 102 'f'
    (0x00000026,  6), // 103 'g'
    (0x00000027,  6), // 104 'h'
    (0x00000006,  5), // 105 'i'
    (0x00000074,  7), // 106 'j'
    (0x00000075,  7), // 107 'k'
    (0x00000028,  6), // 108 'l'
    (0x00000029,  6), // 109 'm'
    (0x0000002a,  6), // 110 'n'
    (0x00000007,  5), // 111 'o'
    (0x0000002b,  6), // 112 'p'
    (0x00000076,  7), // 113 'q'
    (0x0000002c,  6), // 114 'r'
    (0x00000008,  5), // 115 's'
    (0x00000009,  5), // 116 't'
    (0x0000002d,  6), // 117 'u'
    (0x00000077,  7), // 118 'v'
    (0x00000078,  7), // 119 'w'
    (0x00000079,  7), // 120 'x'
    (0x0000007a,  7), // 121 'y'
    (0x0000007b,  7), // 122 'z'
    (0x00007ffe, 15), // 123 '{'
    (0x000007fc, 11), // 124 '|'
    (0x00003ffd, 14), // 125 '}'
    (0x00001ffd, 13), // 126 '~'
    (0x0ffffffc, 28), // 127 127
    (0x000fffe6, 20), // 128 128
    (0x003fffd2, 22), // 129 129
    (0x000fffe7, 20), // 130 130
    (0x000fffe8, 20), // 131 131
    (0x003fffd3, 22), // 132 132
    (0x003fffd4, 22), // 133 133
    (0x003fffd5, 22), // 134 134
    (0x007fffd9, 23), // 135 135
    (0x003fffd6, 22), // 136 136
    (0x007fffda, 23), // 137 137
    (0x007fffdb, 23), // 138 138
    (0x007fffdc, 23), // 139 139
    (0x007fffdd, 23), // 140 140
    (0x007fffde, 23), // 141 141
    (0x00ffffeb, 24), // 142 142
    (0x007fffdf, 23), // 143 143
    (0x00ffffec, 24), // 144 144
    (0x00ffffed, 24), // 145 145
    (0x003fffd7, 22), // 146 146
    (0x007fffe0, 23), // 147 147
    (0x00ffffee, 24), // 148 148
    (0x007fffe1, 23), // 149 149
    (0x007fffe2, 23), // 150 150
    (0x007fffe3, 23), // 151 151
    (0x007fffe4, 23), // 152 152
    (0x001fffdc, 21), // 153 153
    (0x003fffd8, 22), // 154 154
    (0x007fffe5, 23), // 155 155
    (0x003fffd9, 22), // 156 156
    (0x007fffe6, 23), // 157 157
    (0x007fffe7, 23), // 158 158
    (0x00ffffef, 24), // 159 159
    (0x003fffda, 22), // 160 160
    (0x001fffdd, 21), // 161 161
    (0x000fffe9, 20), // 162 162
    (0x003fffdb, 22), // 163 163
    (0x003fffdc, 22), // 164 164
    (0x007fffe8, 23), // 165 165
    (0x007fffe9, 23), // 166 166
    (0x001fffde, 21), // 167 167
    (0x007fffea, 23), // 168 168
    (0x003fffdd, 22), // 169 169
    (0x003fffde, 22), // 170 170
    (0x00fffff0, 24), // 171 171
    (0x001fffdf, 21), // 172 172
    (0x003fffdf, 22), // 173 173
    (0x007fffeb, 23), // 174 174
    (0x007fffec, 23), // 175 175
    (0x001fffe0, 21), // 176 176
    (0x001fffe1, 21), // 177 177
    (0x003fffe0, 22), // 178 178
    (0x001fffe2, 21), // 179 179
    (0x007fffed, 23), // 180 180
    (0x003fffe1, 22), // 181 181
    (0x007fffee, 23), // 182 182
    (0x007fffef, 23), // 183 183
    (0x000fffea, 20), // 184 184
    (0x003fffe2, 22), // 185 185
    (0x003fffe3, 22), // 186 186
    (0x003fffe4, 22), // 187 187
    (0x007ffff0, 23), // 188 188
    (0x003fffe5, 22), // 189 189
    (0x003fffe6, 22), // 190 190
    (0x007ffff1, 23), // 191 191
    (0x03ffffe0, 26), // 192 192
    (0x03ffffe1, 26), // 193 193
    (0x000fffeb, 20), // 194 194
    (0x0007fff1, 19), // 195 195
    (0x003fffe7, 22), // 196 196
    (0x007ffff2, 23), // 197 197
    (0x003fffe8, 22), // 198 198
    (0x01ffffec, 25), // 199 199
    (0x03ffffe2, 26), // 200 200
    (0x03ffffe3, 26), // 201 201
    (0x03ffffe4, 26), // 202 202
    (0x07ffffde, 27), // 203 203
    (0x07ffffdf, 27), // 204 204
    (0x03ffffe5, 26), // 205 205
    (0x00fffff1, 24), // 206 206
    (0x01ffffed, 25), // 207 207
    (0x0007fff2, 19), // 208 208
    (0x001fffe3, 21), // 209 209
    (0x03ffffe6, 26), // 210 210
    (0x07ffffe0, 27), // 211 211
    (0x07ffffe1, 27), // 212 212
    (0x03ffffe7, 26), // 213 213
    (0x07ffffe2, 27), // 214 214
    (0x00fffff2, 24), // 215 215
    (0x001fffe4, 21), // 216 216
    (0x001fffe5, 21), // 217 217
    (0x03ffffe8, 26), // 218 218
    (0x03ffffe9, 26), // 219 219
    (0x0ffffffd, 28), // 220 220
    (0x07ffffe3, 27), // 221 221
    (0x07ffffe4, 27), // 222 222
    (0x07ffffe5, 27), // 223 223
    (0x000fffec, 20), // 224 224
    (0x00fffff3, 24), // 225 225
    (0x000fffed, 20), // 226 226
    (0x001fffe6, 21), // 227 227
    (0x003fffe9, 22), // 228 228
    (0x001fffe7, 21), // 229 229
    (0x001fffe8, 21), // 230 230
    (0x007ffff3, 23), // 231 231
    (0x003fffea, 22), // 232 232
    (0x003fffeb, 22), // 233 233
    (0x01ffffee, 25), // 234 234
    (0x01ffffef, 25), // 235 235
    (0x00fffff4, 24), // 236 236
    (0x00fffff5, 24), // 237 237
    (0x03ffffea, 26), // 238 238
    (0x007ffff4, 23), // 239 239
    (0x03ffffeb, 26), // 240 240
    (0x07ffffe6, 27), // 241 241
    (0x03ffffec, 26), // 242 242
    (0x03ffffed, 26), // 243 243
    (0x07ffffe7, 27), // 244 244
    (0x07ffffe8, 27), // 245 245
    (0x07ffffe9, 27), // 246 246
    (0x07ffffea, 27), // 247 247
    (0x07ffffeb, 27), // 248 248
    (0x0ffffffe, 28), // 249 249
    (0x07ffffec, 27), // 250 250
    (0x07ffffed, 27), // 251 251
    (0x07ffffee, 27), // 252 252
    (0x07ffffef, 27), // 253 253
    (0x07fffff0, 27), // 254 254
    (0x03ffffee, 26), // 255 255
    (0x3fffffff, 30), // 256 EOS
];

/// How many octets `src` occupies once Huffman-coded. The encoder uses this to
/// decide whether coding is worth it at all — a string of rare symbols gets
/// *longer*, and RFC 7541 §5.2 leaves the choice to the encoder.
pub(super) fn encoded_len(src: &[u8]) -> usize {
    let bits: usize = src.iter().map(|&b| CODES[b as usize].1 as usize).sum();
    bits.div_ceil(8)
}

/// Huffman-code `src` into `out`.
///
/// The final octet is padded with the most significant bits of the EOS code,
/// which are all ones (§5.2) — that is what lets a decoder tell padding from a
/// truncated code.
pub(super) fn encode(src: &[u8], out: &mut BytesMut) {
    // Codes are at most 30 bits and we flush whenever 8 bits are ready, so the
    // accumulator never holds more than 37 bits.
    let mut acc: u64 = 0;
    let mut nbits: u32 = 0;

    for &byte in src {
        let (code, len) = CODES[byte as usize];
        acc = (acc << len) | u64::from(code);
        nbits += u32::from(len);
        while nbits >= 8 {
            nbits -= 8;
            out.put_u8((acc >> nbits) as u8);
        }
    }

    if nbits > 0 {
        let pad = 8 - nbits;
        let ones = (1u64 << pad) - 1;
        out.put_u8(((acc << pad) | ones) as u8);
    }
}

// ---------------------------------------------------------------------------
// Decoding: a nibble-indexed state machine derived from CODES (ADR 0012).
// ---------------------------------------------------------------------------

/// A state that no legal input can reach; marks a transition that must fail.
const INVALID: u16 = u16::MAX;

/// The root of the code tree — the state at every code boundary.
const ROOT: u16 = 0;

/// What consuming one nibble from a given state does.
#[derive(Clone, Copy)]
struct Transition {
    /// The state the four bits lead to, or [`INVALID`] if they decode the EOS
    /// symbol (which is never legal inside a string).
    next: u16,
    /// The symbol completed while consuming these bits, if any.
    ///
    /// At most one: the shortest code is 5 bits, so after a symbol completes,
    /// the 3 or fewer bits left in the nibble cannot complete another.
    emit: Option<u8>,
}

/// The derived decoding table, plus the two facts about each state needed to
/// validate trailing padding.
struct DecodeTable {
    states: Vec<[Transition; 16]>,
    /// Bits consumed since the last completed symbol, per state.
    depth: Vec<u8>,
    /// Whether the path from the root to this state is all 1-bits — i.e. the
    /// state is a prefix of the EOS code, which is what padding must look like.
    eos_prefix: Vec<bool>,
}

static DECODE: LazyLock<DecodeTable> = LazyLock::new(DecodeTable::build);

impl DecodeTable {
    fn build() -> DecodeTable {
        // The code tree as an arena. `children[n]` are node `n`'s 0/1 branches
        // (`NONE` where absent) and `symbol[n]` is set on leaves.
        const NONE: u32 = u32::MAX;
        let mut children: Vec<[u32; 2]> = vec![[NONE; 2]];
        let mut symbol: Vec<Option<u16>> = vec![None];

        for (sym, &(code, len)) in CODES.iter().enumerate() {
            let mut node = 0usize;
            for bit in (0..len).rev() {
                let branch = ((code >> bit) & 1) as usize;
                if children[node][branch] == NONE {
                    children.push([NONE; 2]);
                    symbol.push(None);
                    children[node][branch] = (children.len() - 1) as u32;
                }
                node = children[node][branch] as usize;
            }
            symbol[node] = Some(sym as u16);
        }

        // Every internal node becomes a state. The code is complete (its Kraft
        // sum is exactly 1), so this is 256 states and no bit path dead-ends.
        let mut state_of = vec![INVALID; children.len()];
        let mut nodes = Vec::new();
        for (node, kids) in children.iter().enumerate() {
            if kids != &[NONE; 2] {
                state_of[node] = nodes.len() as u16;
                nodes.push(node);
            }
        }
        assert_eq!(state_of[0], ROOT, "the root must be state 0");

        let mut table = DecodeTable {
            states: Vec::with_capacity(nodes.len()),
            depth: vec![0; nodes.len()],
            eos_prefix: vec![false; nodes.len()],
        };

        for &start in &nodes {
            let mut row = [Transition {
                next: INVALID,
                emit: None,
            }; 16];
            for (nibble, slot) in row.iter_mut().enumerate() {
                let mut node = start;
                let mut emit = None;
                let mut failed = false;
                for bit in (0..4).rev() {
                    let branch = (nibble >> bit) & 1;
                    let next = children[node][branch];
                    debug_assert_ne!(next, NONE, "the code table is not complete");
                    node = next as usize;
                    if let Some(sym) = symbol[node] {
                        if sym == EOS {
                            // The EOS code appearing in a string is malformed;
                            // it may only ever be a truncated padding prefix.
                            failed = true;
                            break;
                        }
                        debug_assert!(emit.is_none(), "two symbols in one nibble");
                        emit = Some(sym as u8);
                        node = 0;
                    }
                }
                *slot = if failed {
                    Transition {
                        next: INVALID,
                        emit: None,
                    }
                } else {
                    Transition {
                        next: state_of[node],
                        emit,
                    }
                };
            }
            table.states.push(row);
        }

        // Depth and "is a prefix of EOS" per state, from one descent of the
        // tree. A node's depth from the root *is* its bit count since the last
        // completed symbol, because completing one returns to the root.
        let mut stack = vec![(0usize, 0u8, true)];
        while let Some((node, depth, all_ones)) = stack.pop() {
            if symbol[node].is_some() {
                continue; // a leaf, not a state
            }
            let state = state_of[node] as usize;
            table.depth[state] = depth;
            table.eos_prefix[state] = all_ones;
            for branch in [0usize, 1] {
                let child = children[node][branch];
                if child != NONE {
                    stack.push((child as usize, depth + 1, all_ones && branch == 1));
                }
            }
        }

        table
    }
}

/// Decode the Huffman-coded octets `src`.
///
/// Two things make an input malformed, both of which the fuzzer hunts for
/// (§5.2): the EOS symbol appearing as an actual code, and trailing padding
/// that is either longer than 7 bits or not all 1-bits.
pub(super) fn decode(src: &[u8]) -> Result<Vec<u8>, HpackError> {
    let table = &*DECODE;
    // The shortest code is 5 bits, so the output is at most 8/5 the input.
    let mut out = Vec::with_capacity(src.len() * 8 / 5);
    let mut state = ROOT;

    for &byte in src {
        for nibble in [byte >> 4, byte & 0x0f] {
            let t = table.states[state as usize][nibble as usize];
            if t.next == INVALID {
                return Err(HpackError::Compression(
                    "huffman string contains the EOS symbol".into(),
                ));
            }
            if let Some(sym) = t.emit {
                out.push(sym);
            }
            state = t.next;
        }
    }

    // Whatever bits are left over must be a genuine padding prefix.
    if state != ROOT {
        let (depth, eos_prefix) = (
            table.depth[state as usize],
            table.eos_prefix[state as usize],
        );
        if depth > 7 {
            return Err(HpackError::Compression(format!(
                "huffman padding is {depth} bits, more than the 7 allowed"
            )));
        }
        if !eos_prefix {
            return Err(HpackError::Compression(
                "huffman padding is not a prefix of the EOS code".into(),
            ));
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Appendix B is only useful if it was transcribed correctly: the code must
    /// be a *complete* prefix code, or some bit string would decode to nothing.
    #[test]
    fn code_table_is_a_complete_prefix_code() {
        // Kraft sum, scaled by 2^30 (the longest code) to stay in integers.
        let total: u64 = CODES
            .iter()
            .map(|&(_, len)| 1u64 << (30 - u32::from(len)))
            .sum();
        assert_eq!(total, 1 << 30, "the Huffman code is not complete");

        for (i, &(c1, l1)) in CODES.iter().enumerate() {
            for (j, &(c2, l2)) in CODES.iter().enumerate() {
                if i != j && l1 <= l2 {
                    assert_ne!(c2 >> (l2 - l1), c1, "code {i} is a prefix of code {j}");
                }
            }
        }
    }

    /// The RFC's own worked example (Appendix B): '/' is the six bits 011000.
    #[test]
    fn rfc_worked_example() {
        assert_eq!(CODES[b'/' as usize], (0x18, 6));
    }

    #[test]
    fn every_symbol_round_trips() {
        for byte in 0u8..=255 {
            let mut out = BytesMut::new();
            encode(&[byte], &mut out);
            assert_eq!(out.len(), encoded_len(&[byte]), "symbol {byte}");
            assert_eq!(decode(&out).expect("decode"), vec![byte], "symbol {byte}");
        }
    }

    #[test]
    fn decodes_the_rfc_string_vectors() {
        // C.4.1: "www.example.com", and C.6.1: "Mon, 21 Oct 2013 20:13:21 GMT".
        let cases: [(&str, &[u8]); 2] = [
            (
                "www.example.com",
                &[
                    0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
                ],
            ),
            (
                "Mon, 21 Oct 2013 20:13:21 GMT",
                &[
                    0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04,
                    0x0b, 0x81, 0x66, 0xe0, 0x82, 0xa6, 0x2d, 0x1b, 0xff,
                ],
            ),
        ];
        for (text, coded) in cases {
            let mut out = BytesMut::new();
            encode(text.as_bytes(), &mut out);
            assert_eq!(&out[..], coded, "encoding {text:?}");
            assert_eq!(decode(coded).expect("decode"), text.as_bytes(), "{text:?}");
        }
    }

    #[test]
    fn rejects_the_eos_symbol() {
        // The EOS code is thirty 1-bits; pad to four octets with more ones so
        // the symbol itself is complete rather than being read as padding.
        let eos = [0xff, 0xff, 0xff, 0xff];
        let err = decode(&eos).expect_err("EOS in a string must be rejected");
        assert!(matches!(err, HpackError::Compression(_)), "{err:?}");
    }

    #[test]
    fn rejects_overlong_padding() {
        // 'a' is five bits (00011), leaving three bits in the octet; a whole
        // extra octet of padding after it is more than the 7 bits allowed.
        let mut out = BytesMut::new();
        encode(b"a", &mut out);
        out.put_u8(0xff);
        let err = decode(&out).expect_err("padding longer than 7 bits");
        assert!(matches!(err, HpackError::Compression(_)), "{err:?}");
    }

    #[test]
    fn rejects_padding_that_is_not_the_eos_prefix() {
        // '0' is the five bits 00000. Pad with zeroes instead of ones.
        let coded = [0b0000_0000u8];
        let err = decode(&coded).expect_err("padding must be all ones");
        assert!(matches!(err, HpackError::Compression(_)), "{err:?}");
    }

    #[test]
    fn empty_string_round_trips() {
        let mut out = BytesMut::new();
        encode(b"", &mut out);
        assert!(out.is_empty());
        assert!(decode(b"").expect("decode empty").is_empty());
    }
}
