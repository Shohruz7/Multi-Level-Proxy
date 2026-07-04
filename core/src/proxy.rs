//! The proxy request path: demux → route → balance → upstream → remux
//! (design doc §2.1, §4).
//!
//! Ties the engine together: takes a decoded request from a client stream
//! ([`crate::conn`]/[`crate::stream`]), strips connection-specific headers,
//! picks an upstream via [`crate::lb`] and a connection via [`crate::pool`],
//! re-encodes headers with the upstream-side HPACK state, and bridges DATA in
//! both directions through the bounded per-stream buffer that couples the two
//! connections' flow-control windows ([`crate::flow`], §4.2).
//!
//! This module is deliberately thin: if logic needs protocol knowledge it
//! belongs in the layer that owns that state, not here.
//!
//! Implemented in week 6 — the week the server becomes a proxy.
