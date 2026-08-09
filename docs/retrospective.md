# Retrospective — what this project actually taught

Eight weeks, a hand-built HTTP/2 engine, and a proxy that speaks h2 on both
sides. This is the document to reread before an interview: not what was built —
the [README](../README.md) covers that — but what was *learned*, and which parts
of it were surprising enough to be worth someone's attention.

## The one pattern worth leading with

**Five bugs were found by measuring. None was found by reading code, and none by
adding a unit test.** Every one had the same shape: a feature that ran, passed
its tests, and reported nothing.

| Bug | What the tests saw | What was actually happening |
|---|---|---|
| `RecvWindow::release` inside a `debug_assert!` | Six weeks of green | `--release` compiled the window credit away. The WINDOW_UPDATE still went on the wire while local accounting fell to zero, so a peer spending credit we had issued looked like a violation and the connection died with GOAWAY(FLOW_CONTROL_ERROR). 14% 5xx. |
| Health checking | Green | A dying upstream synthesized `ServiceEvent::Head{502}` — indistinguishable from a backend answering 502 — so every dead connection was reported to health as a **success**. A killed backend gave 36,632 5xx with the ejection counter at zero. |
| Probe-back | Green | `Health::eligible` claimed the single trial slot when a backend was *offered*, not when it was *sent to*. If the balancer picked the other candidate, the flag was set with no request behind it and nothing could clear it. A recovering backend stayed half-open forever, ejected by its own recovery path. |
| Stream accounting on client hang-up | Green | A client that hangs up mid-stream sends neither RST_STREAM nor END_STREAM, so `cancel` and `finish` never ran. Nothing leaked but the *numbers*: the active-stream gauge could only climb, and the latency histogram silently omitted every abandoned stream. Found by a soak that watched what must stay flat. |
| The `Dockerfile` | Nothing — it was never run | Written in week 2 and wrong in **three** independent ways, none discoverable by reading. `AR=llvm-ar` names a binary the `clang` package does not ship. `CC=clang` compiles C against glibc headers and then fails to link against musl. And aarch64 GCC's default `-moutline-atomics` links a libgcc object calling `__getauxval`, a glibc symbol musl lacks — which jemalloc's configure interprets as "this platform has no atomics". |

The lesson is not "write more tests". Every one of these had tests, and the
tests passed. It is that **a test asserts what you thought to assert, and a
measurement shows you what is there.** The harnesses in `bench/` are in the
repository rather than in someone's terminal history for exactly this reason.

The `Dockerfile` row deserves its own sentence, because it is the cleanest case:
it was written carefully, reviewed, referenced by an ADR, and described in the
README — and it had never once been executed, because the machine's Docker was
broken the week it was written. Three defects sat in nine lines of `ENV` for five
weeks. **Every artifact that has never been run is a hypothesis**, however
confident its author and however many documents cite it.

The corollary, which is the more useful interview answer: the classes of defect
that unit tests structurally cannot reach are (a) anything that differs between
debug and release, (b) anything whose only symptom is a number nobody reads,
(c) anything where the failure is *silence* rather than an error, and (d)
anything in an artifact that has never been executed.

## The four design forks worth defending

**Streaming frame parser over a buffered one** (ADR 0011). The reassembly buffer
advances only on a complete frame, so a frame split across TCP segments is never
mis-parsed. The buffered alternative is simpler and is wrong the first time a
9-octet header lands in two segments — which under real load is constantly.

**HPACK encoder and decoder tables must stay in lockstep across a whole
connection** (§3.3, ADR 0012). A single desync corrupts every *later* header
block, not the one that caused it, so the bug reports as "requests fail after a
while". That is why the differential tests run *sequences* of header sets
against the `h2` crate rather than individual headers: a per-header test cannot
observe the failure mode.

**Backpressure by withholding WINDOW_UPDATE** (§4.2, ADR 0016). The centrepiece.
The proxy does not buffer and then forward; it *relays credit*. Response octets
are recorded on arrival and the upstream's window is only reopened once those
octets have reached the client, so a slow client transitively throttles a fast
backend and the memory bound is one connection window (1 MiB) rather than the
response size. Nothing is buffered and nothing blocks — the mechanism is a
delayed call, not a queue.

The test that makes this a claim rather than a hope asserts **both halves**: that
the octets held between the two legs stay under one window, *and* that the
backend provably stopped. Flat memory alone would also describe a proxy that
dropped data.

**Fail open when every backend is ejected** (ADR 0020), against the design doc's
own "return `None` if all ejected". Ejection is a guess. A wrong guess that
refuses everything converts a partial outage into a total one that the proxy
itself caused. A health system that can cause an outage on its own is worse than
the thing it detects.

## The measurement lessons, which are the ones nobody expects

**A closed-loop load generator cannot measure a tail.** `h2load` keeps *n*
requests in flight and issues the next when the last completes, so a server that
stalls for a second simply receives fewer requests during the stall. The stall
is measured once instead of in every request a real arrival process would have
piled up behind it. That is coordinated omission, and it is why week 8 has its
own generator: fixed schedule, no throttling, latency measured from when each
request was *supposed* to be sent. On this proxy the correction is small at low
load and dominates near the knee — which is precisely where a p99 gets quoted.

**A load generator has to report its own saturation.** Two scheduler designs were
wrong before the third: sleeping per request cannot keep a schedule above 1,000
req/s because the interval is shorter than a timer tick, and busy-waiting was
*worse* because a spinning thread takes a core the proxy is already using. Both
were caught only because the generator measures and prints how far behind
schedule it fell. A benchmark that cannot fail its own honesty check is a
benchmark that will eventually report itself.

**Thresholds must be derived from measured traffic, not chosen.** `just
calibrate` runs legitimate workloads with the guard in observe-only mode and
fails if any signal is within 10× of tripping. It rejected an
`unanswered_rate` of 5 at 5× headroom. It also caught a bug in the meter itself:
`RateMeter::peak` extrapolated a partial window, so two events 200 µs apart read
as 10,000/s and ordinary traffic reported 40,631 control frames per second.

**Capture the "before" number before writing the code.** The through-proxy
baseline was captured before any week-7 hardening landed, because once the guard
is in the frame path the pre-hardening number no longer exists. Capturing it is
also what surfaced the release-only flow-control bug.

## The §8.2 talking points, each with the code behind it

- **Many streams in, few connections out.** 20 client connections carrying 400
  streams land on ~8 upstream connections; the id remapping between the two
  spaces is the pool's job (`core/src/pool.rs`, `core/src/upstream.rs`).
- **Stream ids must increase in the order they reach the wire** (§5.1.1), and
  several client connections sharing one upstream have no ordering between them.
  That is why ids are allocated inside the upstream connection task and clients
  address work by an opaque `RequestId` instead.
- **A connection error takes the connection down; a stream error does not.**
  HTTP/2's own split, mapped onto Rust's type system in ADR 0008 so the compiler
  will not let one be returned where the other is meant.
- **Rapid Reset (CVE-2023-44487) is cheap because resets are free to the
  attacker.** The mitigation meters resets on *unanswered* streams — the
  signature — rather than resets in general, since a browser cancelling
  downloads resets streams too. WINDOW_UPDATE is deliberately *not* metered: it
  obliges no ACK and its rate tracks legitimate data transfer. The asymmetry
  that makes PING and SETTINGS meterable is the mandatory ACK.
- **The probe detects silence, not failure.** A black-holed backend fails no
  request; it hangs them, and it looks *busy* — its streams are all open and
  none will finish. Nothing passive can see that, which is why probing keys on
  quiet rather than on "no live streams".

## What was cut, and honestly why

- **The AWS deployment.** The stack is written and template-tested and has never
  run (ADR 0022). Every performance number in this repository is loopback on a
  ten-core laptop where the load generator competes with the proxy for CPU, and
  [RESULTS.md](../Documentation/RESULTS.md) labels each one with the environment
  that produced it.
- **Server push, an HTTP/1.1 downgrade path, a dynamic control plane.** Stated
  non-goals from week 1, and still the right call: none of them would have
  taught anything the rest did not.
- **Priorities (RFC 9218).** Studied (`docs/notes/rfc9218-priorities.md`), not
  implemented. The scheduler is round-robin with a per-stream byte budget, which
  is enough to keep a large response from starving small ones — the property
  that actually matters here.

## If there were a week 9

1. **Deploy it**, and re-calibrate the guard against real traffic shape. Every
   threshold is already an environment variable for that reason.
2. **A `--closed-loop` vs open-loop study across the whole curve**, published as
   a chart. The data is already collected per step; nothing has plotted the two
   against each other yet.
3. **HPACK dynamic-table pressure under adversarial header sets.** The bomb
   guard is tested; table *eviction* behaviour under sustained pressure from a
   hostile peer is reasoned about but not measured.
