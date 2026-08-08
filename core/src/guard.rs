//! Per-connection abuse accounting (design doc §6).
//!
//! The HTTP/2 attacks that matter are not malformed frames — the codec already
//! rejects those — but *legal* frames sent at a rate that costs the server far
//! more than the client. Rapid Reset (CVE-2023-44487) is the canonical one:
//! HEADERS followed immediately by RST_STREAM is two tiny frames to send and a
//! full request pipeline to process, and because a reset stream leaves the
//! concurrency budget instantly, `MAX_CONCURRENT_STREAMS` never engages. The
//! same shape recurs with PING (CVE-2019-9512), SETTINGS (CVE-2019-9515), and
//! empty DATA (CVE-2019-9518).
//!
//! # What this module is, and what it is not
//!
//! It is a pile of counters with thresholds. It holds no protocol state, does no
//! I/O, and owns no timer: **every method takes `now`**. That is what lets the
//! thresholds be tested exactly, with no sleeping and no flakes, and it is why
//! there is no async in this file.
//!
//! A trip becomes `GOAWAY(ENHANCE_YOUR_CALM)` at the call site, through the
//! error model that already exists (ADR 0008) — so the whole mitigation is a
//! counter here and one `?` in [`crate::conn`]. The blast radius is one
//! connection, which is the point: the process stays up and every other client
//! is untouched.
//!
//! # The client leg only
//!
//! A backend is not the adversary. Metering the upstream leg would turn a busy
//! or unusual backend into a dropped connection, and the failure it would
//! "protect" against — our own backend attacking us — is not a threat model
//! worth the false positives.
//!
//! # Why token buckets
//!
//! A fixed window lets an attacker send the whole budget at the end of one
//! window and again at the start of the next, i.e. 2× the intended rate forever.
//! An EWMA has no exact trip condition to assert in a test. A bucket is two
//! numbers, refills continuously, and its threshold is a sentence: *this many at
//! once, that many per second sustained.*
//!
//! # False positives are the hard part
//!
//! RST_STREAM is not an attack signal by itself — browsers reset streams
//! constantly, for navigations and cancelled fetches, and h2spec's conformance
//! suite opens and resets streams deliberately. Every threshold here therefore
//! ships with a measured headroom against real traffic rather than a guess; see
//! [`Limits`] and the calibration run in `bench/`. [`Limits::observe_only`]
//! exists so that measurement can be taken with the guard in the path but
//! unable to break anything.

use std::time::Duration;

use tokio::time::Instant;

/// What tripped, for the log line, the metric label, and the GOAWAY debug data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Signal {
    /// RST_STREAM received, at any point in a stream's life.
    Resets,
    /// Streams reset before we had sent any response — the Rapid Reset
    /// signature specifically, and a far more specific signal than resets alone.
    UnansweredResets,
    /// PING and SETTINGS — the frame types that oblige us to send an ACK.
    ControlFrames,
    /// Zero-length DATA that does not end its stream.
    EmptyFrames,
    /// CONTINUATION frames within a single header block.
    Continuations,
}

impl Signal {
    /// A stable, lowercase name for metric labels and logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Signal::Resets => "resets",
            Signal::UnansweredResets => "unanswered_resets",
            Signal::ControlFrames => "control_frames",
            Signal::EmptyFrames => "empty_frames",
            Signal::Continuations => "continuations",
        }
    }
}

/// The outcome of one accounting call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[must_use = "a Trip must become a connection error, or the guard does nothing"]
pub enum Verdict {
    Ok,
    Trip(Signal),
}

impl Verdict {
    pub const fn tripped(self) -> Option<Signal> {
        match self {
            Verdict::Ok => None,
            Verdict::Trip(signal) => Some(signal),
        }
    }
}

/// Thresholds, with the derivation for each in its field doc.
///
/// Plain data with a `Default`, so the daemon can move any of them from the
/// environment and a test can set one to zero. The library never reads the
/// environment.
///
/// The numbers below are **derived, then measured**. Each is chosen from an
/// argument about legitimate traffic, and the calibration run
/// (`just calibrate`) then records what real traffic actually peaked at, so the
/// headroom is a number rather than a hope. Any signal that comes within 10× of
/// its threshold under legitimate load is wrong and gets raised.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Limits {
    /// Resets absorbed instantly. A browser navigating away from a page with
    /// many in-flight fetches cancels a burst all at once, and that burst is
    /// tens, not hundreds.
    pub reset_burst: f64,
    /// Resets per second, sustained. Legitimate cancellation is bursty but not
    /// continuous; an attack is continuous by construction.
    pub reset_rate: f64,

    /// Unanswered resets absorbed instantly. Tighter than `reset_burst`,
    /// because a client that has not yet been answered has had less reason to
    /// change its mind.
    pub unanswered_burst: f64,
    /// Unanswered resets per second. The sharpest Rapid Reset signal there is:
    /// the attack's whole shape is opening streams and abandoning them before
    /// they can be served.
    ///
    /// Raised from 5 to 15 by the calibration run. Legitimate traffic — mostly
    /// h2spec, which resets unanswered streams deliberately — peaked at 1/s,
    /// and 5 left only 5x headroom against the 10x rule. Still well under
    /// `reset_rate`, so it remains the tighter and more specific of the two, and
    /// still trips an attack in well under a second: Rapid Reset exhausts
    /// `unanswered_burst` on its first flight of frames, long before the rate
    /// matters.
    pub unanswered_rate: f64,

    /// Control frames absorbed instantly — enough for a settings exchange plus
    /// a window-update flurry at the start of a busy connection.
    pub control_burst: f64,
    /// PING+SETTINGS per second. h2 clients PING for keepalive on the order of
    /// once every 10–30 s, so this is orders of magnitude above any real client
    /// and still trivially cheap for an attacker to exceed.
    pub control_rate: f64,

    /// Consecutive zero-length DATA frames without END_STREAM.
    ///
    /// Not a rate, because there is no legitimate use for even one: an empty
    /// DATA carrying END_STREAM ends a body, and an empty DATA that does not is
    /// pure parsing work with no flow-control cost. A small tolerance rather
    /// than zero, so a pathological-but-honest client is not punished.
    pub max_consecutive_empty: u32,

    /// CONTINUATION frames within one header block.
    ///
    /// The byte cap (`MAX_HEADER_BLOCK_BYTES`) already bounds the *buffer*, but
    /// not the frame count: 1-byte CONTINUATIONs cost a decode round each and
    /// stay under it forever. `MAX_HEADER_LIST_SIZE` is 64 KiB and
    /// `MAX_FRAME_SIZE` is 16 KiB, so an honest block needs at most four
    /// frames; this is 16× that.
    pub max_continuations: u32,

    /// Count and record, never trip.
    ///
    /// Two uses. It is how the calibration run measures legitimate traffic's
    /// real peak rates with the guard genuinely in the path, and it is the safe
    /// first mode to deploy in — watch the peaks for a week, then enforce.
    pub observe_only: bool,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            reset_burst: 100.0,
            reset_rate: 20.0,
            unanswered_burst: 50.0,
            unanswered_rate: 15.0,
            control_burst: 200.0,
            control_rate: 50.0,
            max_consecutive_empty: 32,
            max_continuations: 64,
            observe_only: false,
        }
    }
}

impl Limits {
    /// Never trip. For tests about something else, and for the `Echo` paths
    /// where an adversary is not part of the scenario.
    pub fn permissive() -> Self {
        Limits {
            reset_burst: f64::INFINITY,
            reset_rate: f64::INFINITY,
            unanswered_burst: f64::INFINITY,
            unanswered_rate: f64::INFINITY,
            control_burst: f64::INFINITY,
            control_rate: f64::INFINITY,
            max_consecutive_empty: u32::MAX,
            max_continuations: u32::MAX,
            observe_only: false,
        }
    }
}

/// A token bucket: `burst` tokens, refilled at `rate` per second.
#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    last: Instant,
}

impl Bucket {
    fn new(burst: f64, rate: f64, now: Instant) -> Self {
        Bucket {
            tokens: burst,
            rate,
            burst,
            last: now,
        }
    }

    /// Spend one token. `false` once the budget is exhausted.
    fn take(&mut self, now: Instant) -> bool {
        // Monotonic in principle, but `saturating_duration_since` costs nothing
        // and makes a clock that goes backwards a no-op instead of a panic.
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        // `min` with a NaN-free operand keeps an infinite rate finite-safe:
        // f64::INFINITY * 0.0 is NaN, so the multiply is guarded.
        if self.rate.is_finite() {
            self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        } else {
            self.tokens = self.burst;
        }
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

/// The peak rate a signal actually reached, in events per second.
///
/// Separate from the bucket on purpose. The bucket answers "should this trip?";
/// this answers "how close did legitimate traffic come?", which is the number
/// the thresholds are calibrated against. Without it, tuning is guesswork.
#[derive(Clone, Copy, Debug)]
struct RateMeter {
    window_start: Instant,
    count: u32,
    peak_per_sec: f64,
}

impl RateMeter {
    const WINDOW: Duration = Duration::from_secs(1);

    fn new(now: Instant) -> Self {
        RateMeter {
            window_start: now,
            count: 0,
            peak_per_sec: 0.0,
        }
    }

    fn record(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.window_start);
        if elapsed >= Self::WINDOW {
            let rate = f64::from(self.count) / elapsed.as_secs_f64();
            if rate > self.peak_per_sec {
                self.peak_per_sec = rate;
            }
            self.window_start = now;
            self.count = 0;
        }
        self.count = self.count.saturating_add(1);
    }

    /// The peak *sustained* rate, including a window still in progress —
    /// otherwise a short-lived attack connection reports zero, which is exactly
    /// the case worth seeing.
    ///
    /// The in-progress window is divided by a full [`Self::WINDOW`] even when
    /// less than that has elapsed, and that floor is the whole subtlety.
    /// Dividing by the real elapsed time *extrapolates*: two control frames
    /// 200 us apart become "10,000 per second", and a connection that lived for
    /// a millisecond reports a rate no traffic ever reached. The first
    /// calibration run measured 40,631 control frames/sec against ordinary
    /// `h2load` traffic for exactly that reason, which would have made every
    /// threshold below look hopelessly tight.
    ///
    /// Bursts are not lost by this — that is what the bucket's `burst` is for.
    /// This measures the sustained rate the *rate* limit is set against, and a
    /// burst compressed into a moment is not a sustained rate.
    fn peak(&self, now: Instant) -> f64 {
        let elapsed = now
            .saturating_duration_since(self.window_start)
            .max(Self::WINDOW)
            .as_secs_f64();
        let partial = f64::from(self.count) / elapsed;
        self.peak_per_sec.max(partial)
    }
}

/// One rate-limited signal: the enforcement half and the measurement half.
#[derive(Clone, Copy, Debug)]
struct Meter {
    bucket: Bucket,
    rate: RateMeter,
}

impl Meter {
    fn new(burst: f64, rate: f64, now: Instant) -> Self {
        Meter {
            bucket: Bucket::new(burst, rate, now),
            rate: RateMeter::new(now),
        }
    }

    /// Record one event, returning whether it stayed inside the budget.
    fn record(&mut self, now: Instant) -> bool {
        self.rate.record(now);
        self.bucket.take(now)
    }
}

/// Peak observed rates, for the calibration run and the gauges (§7).
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Peaks {
    pub resets_per_sec: f64,
    pub unanswered_per_sec: f64,
    pub control_per_sec: f64,
    pub max_consecutive_empty: u32,
    pub max_continuations: u32,
}

/// Per-connection abuse accounting. One of these per client connection.
#[derive(Clone, Debug)]
pub struct Guard {
    limits: Limits,
    resets: Meter,
    unanswered: Meter,
    control: Meter,
    consecutive_empty: u32,
    continuations: u32,
    peak_empty: u32,
    peak_continuations: u32,
    /// What tripped, kept so the connection can name it in a metric label after
    /// the verdict has become a `ConnectionError`.
    last_trip: Option<Signal>,
}

impl Guard {
    pub fn new(limits: Limits, now: Instant) -> Self {
        Guard {
            resets: Meter::new(limits.reset_burst, limits.reset_rate, now),
            unanswered: Meter::new(limits.unanswered_burst, limits.unanswered_rate, now),
            control: Meter::new(limits.control_burst, limits.control_rate, now),
            limits,
            consecutive_empty: 0,
            continuations: 0,
            peak_empty: 0,
            peak_continuations: 0,
            last_trip: None,
        }
    }

    /// The signal that tripped, if one did.
    pub const fn last_trip(&self) -> Option<Signal> {
        self.last_trip
    }

    /// A trip, unless we are only observing.
    fn verdict(&mut self, signal: Signal) -> Verdict {
        if self.limits.observe_only {
            Verdict::Ok
        } else {
            self.last_trip = Some(signal);
            Verdict::Trip(signal)
        }
    }

    /// A RST_STREAM arrived. `answered` is whether we had already sent this
    /// stream's response head.
    ///
    /// Two counters, because they mean different things. Resets alone are
    /// ordinary — a browser cancels constantly. A reset on a stream we have not
    /// answered yet is the Rapid Reset signature: the client asked for work and
    /// abandoned it before it could be delivered, which is the asymmetry the
    /// attack is built on.
    pub fn on_reset(&mut self, answered: bool, now: Instant) -> Verdict {
        if !self.resets.record(now) {
            return self.verdict(Signal::Resets);
        }
        if !answered && !self.unanswered.record(now) {
            return self.verdict(Signal::UnansweredResets);
        }
        Verdict::Ok
    }

    /// A PING, SETTINGS, or connection-level WINDOW_UPDATE arrived. None are
    /// flow-controlled, and each costs us a reply or a state update.
    pub fn on_control_frame(&mut self, now: Instant) -> Verdict {
        if !self.control.record(now) {
            return self.verdict(Signal::ControlFrames);
        }
        Verdict::Ok
    }

    /// A DATA frame arrived. Empty frames that do not end their stream are
    /// counted consecutively; anything else resets the run.
    pub fn on_data(&mut self, len: usize, end_stream: bool) -> Verdict {
        if len > 0 || end_stream {
            self.consecutive_empty = 0;
            return Verdict::Ok;
        }
        self.consecutive_empty += 1;
        self.peak_empty = self.peak_empty.max(self.consecutive_empty);
        if self.consecutive_empty > self.limits.max_consecutive_empty {
            return self.verdict(Signal::EmptyFrames);
        }
        Verdict::Ok
    }

    /// A CONTINUATION arrived for the header block currently open.
    pub fn on_continuation(&mut self) -> Verdict {
        self.continuations += 1;
        self.peak_continuations = self.peak_continuations.max(self.continuations);
        if self.continuations > self.limits.max_continuations {
            return self.verdict(Signal::Continuations);
        }
        Verdict::Ok
    }

    /// A header block finished, so the CONTINUATION count starts again.
    pub fn on_header_block_end(&mut self) {
        self.continuations = 0;
    }

    /// What this connection actually reached, for the calibration run.
    pub fn peaks(&self, now: Instant) -> Peaks {
        Peaks {
            resets_per_sec: self.resets.rate.peak(now),
            unanswered_per_sec: self.unanswered.rate.peak(now),
            control_per_sec: self.control.rate.peak(now),
            max_consecutive_empty: self.peak_empty,
            max_continuations: self.peak_continuations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    fn guard(limits: Limits) -> (Guard, Instant) {
        let now = Instant::now();
        (Guard::new(limits, now), now)
    }

    // ---- the bucket --------------------------------------------------------

    #[test]
    fn a_burst_is_absorbed_and_the_next_one_is_not() {
        let limits = Limits {
            reset_burst: 10.0,
            reset_rate: 1.0,
            ..Limits::permissive()
        };
        let (mut g, base) = guard(limits);
        // Ten at the same instant is exactly the burst.
        for i in 0..10 {
            assert_eq!(
                g.on_reset(true, base),
                Verdict::Ok,
                "reset {i} of the burst"
            );
        }
        assert_eq!(
            g.on_reset(true, base),
            Verdict::Trip(Signal::Resets),
            "the eleventh has no token to spend",
        );
    }

    #[test]
    fn the_bucket_refills_at_its_rate() {
        let limits = Limits {
            reset_burst: 5.0,
            reset_rate: 10.0,
            ..Limits::permissive()
        };
        let (mut g, base) = guard(limits);
        for _ in 0..5 {
            assert_eq!(g.on_reset(true, base), Verdict::Ok);
        }
        assert_eq!(g.on_reset(true, base), Verdict::Trip(Signal::Resets));
        // 10/s means one token per 100 ms.
        assert_eq!(g.on_reset(true, at(base, 100)), Verdict::Ok);
        assert_eq!(
            g.on_reset(true, at(base, 100)),
            Verdict::Trip(Signal::Resets)
        );
    }

    #[test]
    fn a_sustained_rate_below_the_limit_never_trips() {
        // The property that matters more than any trip: legitimate traffic must
        // survive indefinitely, not merely for a while.
        let limits = Limits {
            reset_burst: 10.0,
            reset_rate: 10.0,
            ..Limits::permissive()
        };
        let (mut g, base) = guard(limits);
        // 5/s for a simulated minute, against a 10/s limit.
        for i in 0..300u64 {
            let now = at(base, i * 200);
            assert_eq!(
                g.on_reset(true, now),
                Verdict::Ok,
                "tripped at event {i}, {}s in, at half the configured rate",
                i / 5,
            );
        }
    }

    #[test]
    fn a_bucket_never_refills_past_its_burst() {
        // Otherwise an hour of silence buys an hour's worth of attack.
        let limits = Limits {
            reset_burst: 3.0,
            reset_rate: 100.0,
            ..Limits::permissive()
        };
        let (mut g, base) = guard(limits);
        let later = base + Duration::from_secs(3600);
        for _ in 0..3 {
            assert_eq!(g.on_reset(true, later), Verdict::Ok);
        }
        assert_eq!(
            g.on_reset(true, later),
            Verdict::Trip(Signal::Resets),
            "an idle hour must not bank 360,000 tokens",
        );
    }

    // ---- the Rapid Reset signature -----------------------------------------

    #[test]
    fn unanswered_resets_trip_sooner_than_answered_ones() {
        // The distinction the whole mitigation rests on. A client that cancels
        // streams it has already been answered on is behaving normally; one that
        // abandons streams before they can be served is the attack.
        let limits = Limits {
            reset_burst: 1000.0,
            reset_rate: 1000.0,
            unanswered_burst: 10.0,
            unanswered_rate: 1.0,
            ..Limits::permissive()
        };
        let (mut g, base) = guard(limits);

        // 50 resets on streams we answered: ordinary cancellation, no trip.
        for _ in 0..50 {
            assert_eq!(g.on_reset(true, base), Verdict::Ok);
        }
        // 10 unanswered exhausts that budget, and the 11th trips.
        for _ in 0..10 {
            assert_eq!(g.on_reset(false, base), Verdict::Ok);
        }
        assert_eq!(
            g.on_reset(false, base),
            Verdict::Trip(Signal::UnansweredResets),
        );
    }

    // ---- the count-based signals -------------------------------------------

    #[test]
    fn empty_data_frames_must_be_consecutive_to_count() {
        let limits = Limits {
            max_consecutive_empty: 3,
            ..Limits::permissive()
        };
        let (mut g, _) = guard(limits);
        for _ in 0..3 {
            assert_eq!(g.on_data(0, false), Verdict::Ok);
        }
        // One real frame clears the run, so an honest client that happens to
        // emit empty frames now and then is unaffected.
        assert_eq!(g.on_data(16, false), Verdict::Ok);
        for _ in 0..3 {
            assert_eq!(g.on_data(0, false), Verdict::Ok);
        }
        assert_eq!(g.on_data(0, false), Verdict::Trip(Signal::EmptyFrames));
    }

    #[test]
    fn an_empty_frame_that_ends_a_stream_is_not_a_flood() {
        // An empty DATA carrying END_STREAM is how a body ends. Counting it
        // would break every client that sends one.
        let limits = Limits {
            max_consecutive_empty: 2,
            ..Limits::permissive()
        };
        let (mut g, _) = guard(limits);
        for _ in 0..100 {
            assert_eq!(g.on_data(0, true), Verdict::Ok);
        }
    }

    #[test]
    fn continuations_are_counted_per_block_not_per_connection() {
        let limits = Limits {
            max_continuations: 4,
            ..Limits::permissive()
        };
        let (mut g, _) = guard(limits);
        for _ in 0..20 {
            for _ in 0..4 {
                assert_eq!(g.on_continuation(), Verdict::Ok);
            }
            g.on_header_block_end();
        }
        // ...but one block may not exceed it.
        for _ in 0..4 {
            assert_eq!(g.on_continuation(), Verdict::Ok);
        }
        assert_eq!(g.on_continuation(), Verdict::Trip(Signal::Continuations));
    }

    // ---- observe-only ------------------------------------------------------

    #[test]
    fn observe_only_measures_without_tripping() {
        let limits = Limits {
            reset_burst: 1.0,
            reset_rate: 0.0,
            max_consecutive_empty: 0,
            max_continuations: 0,
            observe_only: true,
            ..Limits::default()
        };
        let (mut g, base) = guard(limits);
        for i in 0..1000u64 {
            assert_eq!(g.on_reset(false, at(base, i)), Verdict::Ok);
            assert_eq!(g.on_data(0, false), Verdict::Ok);
            assert_eq!(g.on_continuation(), Verdict::Ok);
        }
        let peaks = g.peaks(at(base, 1000));
        assert!(
            peaks.resets_per_sec > 0.0,
            "observe-only must still measure, or calibration has nothing to read",
        );
        assert_eq!(peaks.max_consecutive_empty, 1000);
        assert_eq!(peaks.max_continuations, 1000);
    }

    #[test]
    fn the_peak_rate_reflects_what_actually_happened() {
        let (mut g, base) = guard(Limits::permissive());
        // 100 resets inside one second.
        for i in 0..100u64 {
            let _ = g.on_reset(true, at(base, i * 10));
        }
        let peak = g.peaks(at(base, 1000)).resets_per_sec;
        assert!(
            (90.0..=110.0).contains(&peak),
            "expected about 100/s, measured {peak}",
        );
    }

    #[test]
    fn a_short_burst_is_not_reported_as_a_colossal_rate() {
        // The bug the first calibration run found in its own instrument.
        // Dividing an in-progress window by the elapsed time extrapolates: two
        // events 200 us apart read as 10,000 per second, and a connection that
        // lived a millisecond reported a rate no traffic ever reached. Against
        // ordinary `h2load` the gauge said 40,631 control frames/sec, which
        // would have condemned every threshold as hopelessly tight.
        //
        // Bursts are the bucket's job; this measures *sustained* rate.
        let (mut g, base) = guard(Limits::permissive());
        for i in 0..5u64 {
            let _ = g.on_control_frame(base + Duration::from_micros(i * 50));
        }
        let peak = g.peaks(base + Duration::from_micros(200)).control_per_sec;
        assert!(
            peak <= 5.0,
            "five events inside a millisecond reported as {peak}/s; a partial \
             window must be scored over a whole one, not extrapolated",
        );
    }

    #[test]
    fn a_permissive_guard_never_trips_on_anything() {
        // The escape hatch has to be total, or every test that is not about the
        // guard becomes about the guard.
        let (mut g, base) = guard(Limits::permissive());
        for i in 0..10_000u64 {
            assert_eq!(g.on_reset(false, at(base, i / 100)), Verdict::Ok);
            assert_eq!(g.on_control_frame(at(base, i / 100)), Verdict::Ok);
            assert_eq!(g.on_data(0, false), Verdict::Ok);
            assert_eq!(g.on_continuation(), Verdict::Ok);
        }
    }

    #[test]
    fn a_clock_that_goes_backwards_is_survivable() {
        // `Instant` is monotonic by contract, but the guard is fed from a call
        // site that could plausibly reorder, and a panic here would be a
        // remotely triggerable crash.
        let limits = Limits {
            reset_burst: 5.0,
            reset_rate: 5.0,
            ..Limits::permissive()
        };
        let (mut g, base) = guard(limits);
        let later = at(base, 10_000);
        let _ = g.on_reset(true, later);
        let _ = g.on_reset(true, base);
        let _ = g.peaks(base);
    }
}
