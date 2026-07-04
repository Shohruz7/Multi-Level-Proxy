//! TLS setup for the daemon: a self-signed certificate and a rustls server
//! config that terminates TLS 1.3 and negotiates ALPN `h2` (ADR 0002, ADR 0005).
//!
//! The certificate is generated in-process at startup rather than read from
//! disk (design doc §9.3): local runs and load tests behind an NLB don't need
//! a CA-signed cert, and generating one keeps the daemon self-contained with
//! nothing to mount or rotate.

use std::sync::Arc;

use anyhow::Context;
use rustls::ServerConfig;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

/// The single ALPN protocol this proxy speaks. HTTP/2 over TLS is identified
/// by the token `h2` (RFC 9113 §3.3).
pub const ALPN_H2: &[u8] = b"h2";

/// Build a TLS 1.3-only server config with a fresh self-signed cert for
/// `localhost`, advertising ALPN `h2`.
///
/// Restricting to TLS 1.3 (rather than the rustls default of 1.2 + 1.3) is
/// deliberate: HTTP/2 forbids the older cipher suites anyway, and pinning the
/// version removes a class of downgrade surprises from the benchmarks.
pub fn server_config() -> anyhow::Result<Arc<ServerConfig>> {
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .context("generating self-signed certificate")?;

    let cert_der = issued.cert.der().clone();
    let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(issued.signing_key.serialize_der()));

    // Explicit crypto provider (aws-lc-rs) so the config never depends on a
    // process-wide default being installed elsewhere.
    let mut config =
        ServerConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("selecting TLS 1.3")?
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .context("installing self-signed certificate")?;

    config.alpn_protocols = vec![ALPN_H2.to_vec()];

    Ok(Arc::new(config))
}
