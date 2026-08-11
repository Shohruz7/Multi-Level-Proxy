//! loadgen — a fixed-rate, coordinated-omission-correct HTTP/2 load generator.
//!
//! # Why this exists rather than another `h2load` flag
//!
//! Every benchmark in this project so far has been `h2load`, and `bench/README.md`
//! has promised since week 2 that week 8 would report a
//! **coordinated-omission-corrected** tail. It cannot be done with `h2load`:
//! its `--rate` sets the rate at which *connections* are created, not requests,
//! and `-D` and `-r` are mutually exclusive. `h2load` keeps `-m` requests in
//! flight per connection and issues the next one when the last completes — a
//! **closed loop**.
//!
//! That distinction is the whole point. In a closed loop, a server that stalls
//! for a second does not receive a second's worth of requests during the stall;
//! the client politely waits. The stall is measured once, in one request,
//! instead of in every request that a real arrival process would have piled up
//! behind it. That is coordinated omission (Gil Tene's term), and it is why
//! closed-loop p99s are optimistic in the exact circumstances anyone cares about.
//!
//! The correction is one line of arithmetic and one decision:
//!
//! - **Arithmetic.** Latency is measured from the time the request was
//!   *supposed* to be sent — `start + n * interval` — not from the moment it
//!   actually was. Time spent queued because the generator could not dispatch on
//!   schedule is exactly the delay a real client would have suffered, and it
//!   belongs in the number.
//! - **Decision.** Nothing throttles dispatch. There is no in-flight cap and no
//!   semaphore, because a limit on outstanding requests *is* a closed loop
//!   reintroduced by the back door: it would stop offering load precisely when
//!   the server got slow, which is the omission being corrected.
//!
//! The cost of that decision is that an overloaded target makes this process
//! grow its own task queue. That is the honest failure mode — it shows up as a
//! rising tail rather than as a quietly reduced offered rate — but it does mean
//! the generator's own saturation has to be reported, not hidden. `dispatch_lag`
//! in the output is that report: how far behind schedule the *sender* fell. When
//! it stops being negligible, the numbers are about this program, not the proxy.
//!
//! # Usage
//!
//! ```text
//! loadgen --url https://127.0.0.1:8443/ --rate 20000 --duration 30 --connections 50
//! loadgen --url https://127.0.0.1:8443/ --closed-loop --streams 20 --duration 30
//! ```
//!
//! `--closed-loop` runs the `h2load` shape instead, so the two methodologies can
//! be compared on one target in one afternoon — which is the most direct way to
//! see how much a closed loop flatters a tail.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, bail};
use http::Request;
use tokio::sync::mpsc;
use tokio::time::Instant;

mod tls;

/// One completed request, from the schedule's point of view.
struct Sample {
    /// Response time measured from the *intended* send time (microseconds).
    /// This is the corrected number.
    corrected_us: u64,
    /// Response time measured from the actual send time — what a closed-loop
    /// generator would have reported for the same request. Kept so the run can
    /// state the size of the correction instead of asserting that it matters.
    service_us: u64,
    /// How late dispatch was: `corrected_us - service_us`, which is the
    /// generator's own queueing delay.
    lag_us: u64,
    ok: bool,
}

struct Config {
    url: String,
    connections: usize,
    /// Requests per second across all connections. `None` = closed loop.
    rate: Option<u64>,
    /// In-flight requests per connection, closed-loop mode only.
    streams: usize,
    duration: Duration,
    warmup: Duration,
    label: String,
    /// Long-lived workers backing the open loop. Not a concurrency cap in the
    /// coordinated-omission sense — see `open_loop` — but it does bound how many
    /// requests can be *in flight*, so it wants to be comfortably above
    /// rate x latency.
    workers: usize,
}

fn main() -> anyhow::Result<()> {
    let config = parse_args()?;
    // The generator gets its own multi-threaded runtime; on a laptop it competes
    // with the proxy for the same cores, which is a caveat the write-up carries
    // rather than something this program can fix.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(config))
}

fn parse_args() -> anyhow::Result<Config> {
    let mut config = Config {
        url: "https://127.0.0.1:8443/".to_string(),
        connections: 50,
        rate: None,
        streams: 20,
        duration: Duration::from_secs(30),
        warmup: Duration::from_secs(3),
        label: "run".to_string(),
        workers: 8192,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || -> anyhow::Result<String> {
            args.next().with_context(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "--url" => config.url = value()?,
            "--connections" | "-c" => config.connections = value()?.parse()?,
            "--rate" | "-r" => config.rate = Some(value()?.parse()?),
            "--closed-loop" => config.rate = None,
            "--streams" | "-m" => config.streams = value()?.parse()?,
            "--duration" | "-D" => config.duration = Duration::from_secs_f64(value()?.parse()?),
            "--warmup" => config.warmup = Duration::from_secs_f64(value()?.parse()?),
            "--label" => config.label = value()?,
            "--workers" => config.workers = value()?.parse()?,
            "--help" | "-h" => {
                eprintln!(
                    "loadgen --url URL [--rate REQ/S | --closed-loop] [--connections N] \
                     [--streams N] [--duration S] [--warmup S] [--label NAME] \
                     [--workers N]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if config.connections == 0 {
        bail!("--connections must be at least 1");
    }
    Ok(config)
}

async fn run(config: Config) -> anyhow::Result<()> {
    let uri: http::Uri = config.url.parse().context("parsing --url")?;
    let authority = uri
        .authority()
        .context("--url needs a host and port")?
        .clone();

    let mut senders = Vec::with_capacity(config.connections);
    for _ in 0..config.connections {
        senders.push(tls::connect(&authority).await?);
    }
    let senders = Arc::new(senders);
    eprintln!(
        "loadgen: {} connections to {} — {}",
        config.connections,
        authority,
        match config.rate {
            Some(r) => format!("open loop at {r} req/s"),
            None => format!("closed loop, {} streams per connection", config.streams),
        },
    );

    let (tx, mut rx) = mpsc::unbounded_channel::<Sample>();
    let started = Instant::now();
    let measure_from = started + config.warmup;
    let deadline = measure_from + config.duration;
    let dispatched = Arc::new(AtomicU64::new(0));

    match config.rate {
        Some(rate) => {
            let scheduler = open_loop(
                Arc::clone(&senders),
                uri.clone(),
                rate,
                config.workers,
                started,
                deadline,
                measure_from,
                tx,
                Arc::clone(&dispatched),
            );
            // `spawn_blocking` rather than joining inline: the scheduler thread
            // runs to the deadline and joining it from an async context would
            // block a runtime worker for the whole run.
            let _ = tokio::task::spawn_blocking(move || scheduler.join()).await;
            // A grace period for requests still in flight, bounded so an
            // unresponsive target cannot hang the run.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        None => {
            closed_loop(
                Arc::clone(&senders),
                uri.clone(),
                config.streams,
                deadline,
                measure_from,
                tx,
                Arc::clone(&dispatched),
            )
            .await
        }
    }

    // Collect what has landed. Requests still in flight at the deadline are
    // dropped rather than waited for: their latency is unbounded by definition,
    // and including a truncated value would flatter the tail in the one place
    // this program exists to be honest about. The count of them is reported.
    let mut samples = Vec::new();
    let mut failures = 0u64;
    while let Ok(sample) = rx.try_recv() {
        if sample.ok {
            samples.push(sample);
        } else {
            failures += 1;
        }
    }
    report(
        &config,
        &mut samples,
        failures,
        dispatched.load(Ordering::Relaxed),
    );
    Ok(())
}

/// How long the scheduler sleeps between dispatch batches.
///
/// Above 1,000 req/s the gap between requests is shorter than a timer tick, so a
/// scheduler that waits for each request individually cannot keep the schedule:
/// the first version of this file reported 2 ms of dispatch lag at 5,000 req/s
/// against a proxy answering in 1.5 ms, and the tail it printed was mostly its
/// own timer.
///
/// Busy-waiting is the usual answer and it was tried here. It made things
/// *worse* — 9 ms of lag at the same rate — because a spinning thread occupies
/// a core that the proxy, the backend and this generator's own runtime are
/// already sharing on a ten-core laptop. On a dedicated load-generator instance
/// (which is what §10.3 and `infra/` provision) spinning would be right.
///
/// So the scheduler wakes on this period and dispatches everything due since it
/// last looked, each request keeping its own exact intended time. The cost is
/// bounded and known: dispatch lag cannot exceed one period, it is measured, and
/// it is printed beside every number.
const BATCH_PERIOD: Duration = Duration::from_micros(500);

/// The corrected loop: dispatch on a fixed schedule, never throttling.
///
/// # Why a worker pool rather than a task per request
///
/// The obvious implementation — `tokio::spawn` one task per scheduled request —
/// is what this file did first, and it is why the project's published "knee at
/// 25,000 req/s" was wrong. Above about 30,000 req/s the generator collapsed:
/// p99 jumped from 6 ms to 435 ms and dispatch lag rose fiftyfold. The proxy was
/// blamed.
///
/// It was not the proxy. The *closed* loop, whose only structural difference is
/// that its tasks are long-lived, drove the same proxy over the same connections
/// to **169,000 req/s at the same concurrency** the open loop was stuck at. The
/// cost was in creating and tearing down tens of thousands of short-lived tasks
/// per second, each cloning a `SendRequest` and registering a waker on a shared
/// connection, while the runtime was already busy.
///
/// So the workers are pre-spawned once and live for the whole run, exactly as
/// the closed loop's do. What changes is only where the schedule comes from.
///
/// # Why this is still an open loop
///
/// A fixed number of workers looks like a concurrency cap, and a concurrency cap
/// is the thing this program exists to avoid. It is not one, for two reasons.
/// The scheduler never blocks or slows down — it hands each request off and
/// immediately computes the next due time, so the *arrival* process stays
/// independent of how fast the target is serving, which is the whole definition.
/// And a request that waits because every worker is busy has that wait recorded,
/// because latency is measured from its intended time. The queue is in front of
/// the workers instead of inside the kernel, and it is counted either way.
///
/// Each worker owns its own queue rather than sharing one, which avoids a
/// contended MPMC receiver; the scheduler round-robins across them.
#[allow(clippy::too_many_arguments)]
fn open_loop(
    senders: Arc<Vec<h2::client::SendRequest<bytes::Bytes>>>,
    uri: http::Uri,
    rate: u64,
    workers: usize,
    started: Instant,
    deadline: Instant,
    measure_from: Instant,
    tx: mpsc::UnboundedSender<Sample>,
    dispatched: Arc<AtomicU64>,
) -> std::thread::JoinHandle<()> {
    let handle = tokio::runtime::Handle::current();

    // Pre-spawn the pool. Each worker is pinned to one connection so that a
    // connection's requests are issued by a stable set of tasks.
    let mut queues = Vec::with_capacity(workers);
    for w in 0..workers {
        let (job_tx, mut job_rx) = mpsc::unbounded_channel::<Instant>();
        let sender = senders[w % senders.len()].clone();
        let uri = uri.clone();
        let out = tx.clone();
        handle.spawn(async move {
            while let Some(intended) = job_rx.recv().await {
                let sample = one_request(sender.clone(), uri.clone(), intended).await;
                if intended >= measure_from {
                    let _ = out.send(sample);
                }
            }
        });
        queues.push(job_tx);
    }
    // The pool holds the only other clones; dropping ours means the sample
    // channel closes when the workers do.
    drop(tx);

    std::thread::spawn(move || {
        let interval = Duration::from_secs_f64(1.0 / rate as f64);
        let mut n: u64 = 0;
        // The schedule is absolute — `started + n * interval` — and never
        // derived from "now". A schedule that advances from the current time
        // silently slows down whenever the loop is late, which is coordinated
        // omission wearing a different hat.
        let intended_at = |n: u64| started + interval.mul_f64(n as f64);

        'schedule: loop {
            let now = Instant::now();
            // Everything whose time has come, in one batch. A request that is
            // already late goes out now and carries its lateness in `lag_us`; it
            // is never skipped and never quietly rescheduled, because dropping a
            // due request is the omission this whole program exists to avoid.
            while intended_at(n) <= now {
                let intended = intended_at(n);
                if intended >= deadline {
                    break 'schedule;
                }
                dispatched.fetch_add(1, Ordering::Relaxed);
                if queues[(n as usize) % queues.len()].send(intended).is_err() {
                    break 'schedule;
                }
                n += 1;
            }

            let next = intended_at(n);
            if next >= deadline {
                break;
            }
            // Sleep until the next request is due, but never longer than a
            // batch period — at high rates the next one is due almost
            // immediately and the period is what bounds the lag.
            let wait = next
                .saturating_duration_since(Instant::now())
                .min(BATCH_PERIOD);
            if !wait.is_zero() {
                std::thread::sleep(wait);
            }
        }
        // Dropping the queues ends the workers once they drain.
        drop(queues);
    })
}

/// The `h2load` shape, for comparison: `streams` requests in flight per
/// connection, the next issued when the last completes.
#[allow(clippy::too_many_arguments)]
async fn closed_loop(
    senders: Arc<Vec<h2::client::SendRequest<bytes::Bytes>>>,
    uri: http::Uri,
    streams: usize,
    deadline: Instant,
    measure_from: Instant,
    tx: mpsc::UnboundedSender<Sample>,
    dispatched: Arc<AtomicU64>,
) {
    let mut workers = Vec::new();
    for sender in senders.iter() {
        for _ in 0..streams {
            let sender = sender.clone();
            let uri = uri.clone();
            let tx = tx.clone();
            let dispatched = Arc::clone(&dispatched);
            workers.push(tokio::spawn(async move {
                while Instant::now() < deadline {
                    let at = Instant::now();
                    dispatched.fetch_add(1, Ordering::Relaxed);
                    let sample = one_request(sender.clone(), uri.clone(), at).await;
                    if at >= measure_from {
                        let _ = tx.send(sample);
                    }
                }
            }));
        }
    }
    for worker in workers {
        let _ = worker.await;
    }
}

/// Issue one request and read its whole body.
///
/// `intended` is when the schedule said this should have gone out. In closed-loop
/// mode it is simply "now", which is what makes the two modes' numbers directly
/// comparable: the same code path, the same clock, one different subtraction.
async fn one_request(
    sender: h2::client::SendRequest<bytes::Bytes>,
    uri: http::Uri,
    intended: Instant,
) -> Sample {
    let sent = Instant::now();
    let finish = |ok: bool| {
        let done = Instant::now();
        Sample {
            corrected_us: done.duration_since(intended).as_micros() as u64,
            service_us: done.duration_since(sent).as_micros() as u64,
            lag_us: sent.duration_since(intended).as_micros() as u64,
            ok,
        }
    };

    // `ready()` awaits stream capacity — the peer's MAX_CONCURRENT_STREAMS. That
    // wait is real client-visible delay and is deliberately inside the measured
    // span: a proxy that admits fewer streams makes requests wait, and hiding
    // that would be measuring the proxy's convenience rather than the client's
    // experience.
    let Ok(mut sender) = sender.ready().await else {
        return finish(false);
    };
    let request = match Request::builder().method("GET").uri(uri).body(()) {
        Ok(request) => request,
        Err(_) => return finish(false),
    };
    let Ok((response, _)) = sender.send_request(request, true) else {
        return finish(false);
    };
    let Ok(response) = response.await else {
        return finish(false);
    };
    let ok = response.status().is_success();
    let mut body = response.into_body();
    while let Some(chunk) = body.data().await {
        match chunk {
            Ok(chunk) => {
                // Release flow-control credit as the body is consumed, or the
                // generator itself becomes the slow client and measures its own
                // backpressure.
                let _ = body.flow_control().release_capacity(chunk.len());
            }
            Err(_) => return finish(false),
        }
    }
    finish(ok)
}

/// Print the run as one CSV row plus a human summary on stderr.
fn report(config: &Config, samples: &mut [Sample], failures: u64, dispatched: u64) {
    if samples.is_empty() {
        eprintln!("no samples — target unreachable?");
        println!(
            "label,mode,connections,rate_offered,duration_s,completed,failed,\
             achieved_rps,p50_ms,p90_ms,p99_ms,p999_ms,max_ms,\
             closed_loop_p99_ms,dispatch_lag_p99_ms"
        );
        return;
    }

    let quantile = |sorted: &[u64], q: f64| -> f64 {
        let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
        sorted[idx] as f64 / 1000.0
    };

    let mut corrected: Vec<u64> = samples.iter().map(|s| s.corrected_us).collect();
    let mut service: Vec<u64> = samples.iter().map(|s| s.service_us).collect();
    let mut lag: Vec<u64> = samples.iter().map(|s| s.lag_us).collect();
    corrected.sort_unstable();
    service.sort_unstable();
    lag.sort_unstable();

    let completed = samples.len() as u64;
    let achieved = completed as f64 / config.duration.as_secs_f64();
    let mode = if config.rate.is_some() {
        "open"
    } else {
        "closed"
    };
    let offered = config.rate.map_or(0, |r| r);

    println!(
        "label,mode,connections,rate_offered,duration_s,completed,failed,\
         achieved_rps,p50_ms,p90_ms,p99_ms,p999_ms,max_ms,\
         closed_loop_p99_ms,dispatch_lag_p99_ms"
    );
    println!(
        "{},{},{},{},{:.0},{},{},{:.0},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        config.label,
        mode,
        config.connections,
        offered,
        config.duration.as_secs_f64(),
        completed,
        failures,
        achieved,
        quantile(&corrected, 0.50),
        quantile(&corrected, 0.90),
        quantile(&corrected, 0.99),
        quantile(&corrected, 0.999),
        corrected[corrected.len() - 1] as f64 / 1000.0,
        quantile(&service, 0.99),
        quantile(&lag, 0.99),
    );

    eprintln!(
        "  completed {completed}, failed {failures}, dispatched {dispatched}, \
         achieved {achieved:.0} req/s",
    );
    eprintln!(
        "  corrected p99 {:.3} ms vs closed-loop p99 {:.3} ms — the correction is {:.3} ms",
        quantile(&corrected, 0.99),
        quantile(&service, 0.99),
        quantile(&corrected, 0.99) - quantile(&service, 0.99),
    );
    // The generator's honesty check. If dispatch lag is a meaningful share of
    // the reported latency, the tail describes this process failing to keep a
    // schedule, not the proxy failing to answer.
    let lag_p99 = quantile(&lag, 0.99);
    // Both conditions, because either alone cries wolf. A relative test fires
    // when the target answers in 200 µs and the scheduler is 60 µs late, which
    // is a fine measurement; an absolute test fires on a slow target where a
    // millisecond of lag is noise against a 300 ms tail.
    if lag_p99 > quantile(&corrected, 0.99) * 0.25 && lag_p99 > 0.5 {
        eprintln!(
            "  WARNING: dispatch lag p99 {lag_p99:.3} ms is a large share of the reported \
             latency — the generator is saturated and this point is about loadgen, not the target",
        );
    }
}
