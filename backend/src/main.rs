//! A tiny HTTP/2 (h2c, prior-knowledge) upstream for the local dev loop and
//! benchmark baselines.
//!
//! This is *not* part of the proxy — it just gives the proxy something real to
//! talk to, and lets the baseline hit a backend directly (client → backend, no
//! proxy). Everything is env-tunable so later benchmark profiles can vary the
//! payload size and the backend's advertised h2 limits without a rebuild:
//!
//! - `BACKEND_LISTEN` — listen address (default `127.0.0.1:8080`)
//! - `BACKEND_BODY_SIZE` — response body length in bytes (default `1024`)
//! - `BACKEND_MAX_CONCURRENT_STREAMS` — SETTINGS_MAX_CONCURRENT_STREAMS
//! - `BACKEND_INITIAL_WINDOW_SIZE` — SETTINGS_INITIAL_WINDOW_SIZE

use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listen: SocketAddr = env_or("BACKEND_LISTEN", "127.0.0.1:8080").parse()?;
    let body_size: usize = env_parse("BACKEND_BODY_SIZE").unwrap_or(1024);
    let max_concurrent_streams = env_parse::<u32>("BACKEND_MAX_CONCURRENT_STREAMS");
    let initial_window = env_parse::<u32>("BACKEND_INITIAL_WINDOW_SIZE");

    // One shared, pre-allocated response body; cheap `Bytes` clones per request.
    let body = Bytes::from(vec![b'x'; body_size]);

    let listener = TcpListener::bind(listen).await?;
    eprintln!(
        "backend: h2c on http://{listen}, {body_size}-byte responses \
         (max_concurrent_streams={max_concurrent_streams:?}, initial_window={initial_window:?})"
    );

    loop {
        let (stream, _peer) = listener.accept().await?;
        let body = body.clone();

        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        if let Some(n) = max_concurrent_streams {
            builder.max_concurrent_streams(n);
        }
        if let Some(w) = initial_window {
            builder.initial_stream_window_size(w);
        }

        tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let body = body.clone();
                async move {
                    // `/bytes/<n>` mirrors the proxy's own built-in responder,
                    // so a test or a benchmark profile can name the response
                    // size in the URL instead of restarting the backend. The
                    // backpressure test needs exactly this: a response far
                    // larger than any window, produced on demand.
                    let body = match sized_path(req.uri().path()) {
                        Some(n) => Bytes::from(vec![b'x'; n]),
                        None => body,
                    };
                    Ok::<_, Infallible>(Response::new(Full::new(body)))
                }
            });
            if let Err(e) = builder
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                eprintln!("backend: connection error: {e}");
            }
        });
    }
}

/// Parse `/bytes/<n>`, capped so a stray URL cannot ask this process to
/// allocate the machine.
fn sized_path(path: &str) -> Option<usize> {
    const MAX: usize = 256 * 1024 * 1024;
    let rest = path.strip_prefix("/bytes/")?;
    let n: usize = rest.parse().ok()?;
    Some(n.min(MAX))
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: FromStr>(key: &str) -> Option<T> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}
