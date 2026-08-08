# ADR 0015 — The upstream leg: connections as tasks, reached by messages

Status: accepted · Date: 2026-08-03 · **Amends [0013](0013-connection-topology-amended.md)** · Design doc: §2.2, §4.2, §4.3

## Context

Week 5 ended with one task per client connection and streams as table entries.
ADR 0013 closed by naming what would bring tasks back: "week 6's upstream leg. A
stream that must await a backend genuinely cannot run inline." Week 6 is that
week, and it has to decide three things at once: what owns an upstream socket,
how a client connection reaches it, and how the two legs stay independent.

## Decision

**Each pooled upstream connection is its own task**, owning its socket, its HPACK
contexts, its stream table and its windows. Client connections reach it only
through `ToUpstream` messages on an unbounded channel, and hear back only through
`ServiceEvent`s on the channel their `Service` was attached to.

```
  client conn task  ──ToUpstream──▶  upstream conn task  ──▶ backend (h2c)
        ▲                                    │
        └────────────ServiceEvent────────────┘
```

Three supporting decisions come with it:

1. **The client connection's socket is split** (`tokio::io::split`), and its loop
   selects over readable, writable, and the responder's channel. Frame handlers
   append to an outbound buffer instead of awaiting a write.
2. **The pool leases a `RequestId`, not a stream id.** The upstream stream id is
   allocated by the connection task at the instant it queues the HEADERS.
3. **Channels are unbounded, and the flow-control windows are the bound.**

## Why

**Why a task per upstream connection rather than one per client connection.**
A client-owned upstream socket needs no channels at all and is simpler in every
respect — and it forecloses the entire project. Coalescing means *many client
connections* sharing *few* upstream ones; if the upstream socket belongs to one
client connection, N clients produce N backend connections and the headline claim
is gone. Shared ownership needs a single owner, and a task is how you get one
without a mutex around a protocol state machine.

**Why the write path had to split.** ADR 0013 recorded the cost of `write_all` in
the frame loop honestly: "a client that stops reading parks the connection loop".
For a server that was tolerable. For a proxy it is not — the parked task is also
the one that has to keep feeding an unrelated upstream leg, so one client that
stops reading its socket would stall streams belonging to other clients. Two
halves of the socket in one `select!` costs a `BiLock` and removes the coupling.

**Why the stream id is allocated in the connection task.** This is the one
decision that was made twice. Leasing the id in the pool is attractive: a
checkout becomes one atomic increment, and body octets can follow their request
without waiting for the task. But §5.1.1 requires stream ids to increase *in the
order they reach the wire*, and two client connections leasing from the same
pooled connection have no ordering between them — a request that leased id 5 can
arrive after one that leased id 7. The first version did exactly this, and the
coalescing test caught it as sporadic REFUSED_STREAMs under concurrency. The id
must be chosen where the ordering exists, which is the single task that writes.
The `RequestId` → stream-id map that replaces it *is* the §4.3 remapping.

**Why unbounded channels do not unbound memory.** The only producer of response
octets is an upstream connection, and it cannot produce an octet the backend was
not credited for — and the backend is credited only as the client drains
(ADR 0016). A bound here would add a place to block without removing a byte, and
blocking is precisely what must not happen: an upstream task that awaits one
client's channel stops serving every other client sharing that connection.
ADR 0013 left this sentence unfinished ("the bound on their channel will be
chosen in octets, not messages"); the answer is that the window *is* the bound in
octets, and the channel needs none.

## Consequences

- **Failure has to be spoken, not implied.** A dropped channel is invisible to a
  client, so every path that abandons work answers it: a connection that dies
  fails its live routes (502) *and* drains its inbox for requests it never read.
  Two of the week's tests exist only to hold that line.
- **Cancellation is two things.** Dropping a `Lease` frees the concurrency slot
  the load balancer counts; sending `Cancel` is what actually resets the upstream
  stream. Only the connection task may write a frame.
- **The client engine gained a third source of work** and its handlers became
  synchronous, which is a simplification: no handler awaits, so none of them can
  interleave with another's state changes.
- **Week 7's graceful drain has two legs to coordinate** (done; ADR 0018). A GOAWAY toward
  clients must stop new streams while in-flight upstream streams finish, which
  means the drain signal has to reach the pool as well as the listener.

## Rejected alternatives

**One upstream connection per client connection.** Simplest correct code, no
channels, no shared state. Rejected because it makes coalescing impossible, which
is the point of the project.

**A shared connection behind a mutex instead of a task.** Every client
connection would lock the upstream engine to write a frame. It replaces message
passing with lock contention on the hot path and puts a protocol state machine
under a lock held across `await` points — the thing ADR 0013 argued against.

**Bounded channels with `try_send` and a park list.** Correct but ornate: it
needs a wake-up path for "capacity freed", which is a second flow-control system
running alongside the one HTTP/2 already specifies. Withholding credit is the
mechanism the protocol gives us; using it twice is a design smell.
