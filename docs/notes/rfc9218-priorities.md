# RFC 9218 — Extensible Prioritization (skim notes)

Read just deep enough to justify a scoping decision: **this proxy does not
implement stream prioritization.**

## What happened to HTTP/2 priorities

- RFC 7540 (§5.3, 2015) defined a **dependency tree**: every stream could declare a
  parent stream, a weight (1–256), and an exclusive flag, signaled via the PRIORITY
  frame and the priority fields in HEADERS. Senders were supposed to allocate
  bandwidth by walking the tree.
- In practice it failed: implementations were wildly inconsistent (some ignored it,
  some built pathological trees), clients disagreed on how to use it, and the tree
  state was a memory/CPU liability (dangling parents, priority churn attacks).
- RFC 9113 (the 2022 HTTP/2 revision) formally **deprecates** the dependency
  scheme: the signaling still parses on the wire, but endpoints are told not to
  rely on it and may ignore it entirely.
- RFC 9218 replaces it with something deliberately dumber: a `priority` header
  field (plus a PRIORITY_UPDATE frame) carrying just **urgency** (0–7, default 3)
  and an **incremental** boolean. It applies to both HTTP/2 and HTTP/3.

## What this project does

- **Tolerate and ignore**: parse PRIORITY frames (5-octet payload, stream ≠ 0) only
  enough to validate size and discard them; ignore the priority fields in HEADERS
  when the PRIORITY flag is set. PRIORITY is the one frame legal on idle *and*
  closed streams — tolerating it avoids spurious errors.
- **No dependency tree**: no tree state, no reprioritization, no weight math. This
  removes an entire class of state-exhaustion attacks and matches what RFC 9113
  itself recommends.
- **Fairness instead of priorities**: outbound DATA is interleaved round-robin with
  a per-stream byte budget (design doc §4.1), so a large response cannot starve
  small ones. That achieves the practical goal priorities were meant to serve,
  without trusting client-supplied scheduling hints.
- Honoring RFC 9218 urgency in the scheduler is possible **future work** — the
  fair scheduler's budget knob is where urgency would plug in.
