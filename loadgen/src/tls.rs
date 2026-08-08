//! TLS for the generator: TLS 1.3, ALPN `h2`, and no certificate verification.
//!
//! The proxy generates a self-signed certificate in process (design doc §9.3,
//! `h2proxyd/src/tls.rs`), so there is no CA to trust and nothing to install.
//! Verification is therefore switched off — which is the same thing `h2load -k`
//! and `curl -k` do against this target, and is confined to this benchmark
//! binary. It has no business anywhere else in the workspace.

use std::sync::Arc;

use anyhow::Context;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Accepts any certificate.
///
/// Deliberately total: the alternative — pinning the proxy's freshly generated
/// self-signed cert — would mean the generator could only run against a proxy it
/// had started itself, which is exactly the case a deployed run is not.
#[derive(Debug)]
struct AcceptAny(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Open one TLS + h2 connection and return its request sender.
///
/// The `h2` connection future is spawned and detached: it is the thing that
/// actually drives frames on the socket, and without it every request would be
/// created and none would ever be sent.
pub async fn connect(
    authority: &http::uri::Authority,
) -> anyhow::Result<h2::client::SendRequest<bytes::Bytes>> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("selecting TLS 1.3")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        .with_no_client_auth();
    // Without this the server sees no ALPN, refuses to speak h2, and the run
    // fails at the handshake rather than producing a confusing number.
    config.alpn_protocols = vec![b"h2".to_vec()];

    let port = authority.port_u16().unwrap_or(443);
    let addr = format!("{}:{}", authority.host(), port);
    let socket = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connecting to {addr}"))?;
    // Nagle would batch small frames behind a round trip and add latency this
    // benchmark would then attribute to the proxy.
    let _ = socket.set_nodelay(true);

    let server_name = ServerName::try_from(authority.host().to_string())
        .context("authority is not a valid server name")?;
    let stream = TlsConnector::from(Arc::new(config))
        .connect(server_name, socket)
        .await
        .context("TLS handshake")?;

    let (sender, connection) = h2::client::handshake(stream)
        .await
        .context("h2 handshake")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender)
}
