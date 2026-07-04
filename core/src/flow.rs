//! Flow control: connection-level and per-stream windows (RFC 9113 §5.2).
//!
//! Owns window accounting for both levels simultaneously — every DATA payload
//! octet decrements a stream window *and* the connection window — plus
//! WINDOW_UPDATE bookkeeping and the retroactive `INITIAL_WINDOW_SIZE`
//! adjustment (windows are signed: a SETTINGS decrease can legally drive them
//! negative).
//!
//! This module is the mechanism behind backpressure bridging (design doc
//! §4.2): the proxy withholds the upstream's WINDOW_UPDATE until bytes drain
//! to the client, so the client's window transitively governs the upstream's
//! send rate with bounded proxy memory.
//!
//! Implemented in week 5; the off-by-one hazards only surface under load, so
//! the week-6 slow-client test asserts flat memory, not just green units.
