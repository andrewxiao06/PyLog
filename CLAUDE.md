# CLAUDE.md — PyLog

A partitioned, append-only log broker in Python. Single node. No replication.

## Why this project exists

I am not shipping a product. I am building the ability to answer these five questions from implementation experience, cold, in an interview:

1.  How does a log guarantee ordering, and at what scope?
2.  What happens to in-flight writes when the broker dies mid-append?
3.  How does a consumer know where it left off after a restart?
4.  Why at-least-once instead of exactly-once, and what does that force on the consumer?
5.  Where is my throughput ceiling, and what causes it?

**The repo is evidence. The capability is the deliverable.** If I finish the code but can't answer those, the project failed. If I finish half and can answer all five, it succeeded.

## How to help me — read this before every response

**Default mode: teach, don't type.** I am learning distributed systems by building this. Code you write is code I don't understand. If you hand me a working implementation, you have taken the deliverable away from me.

### Escalation ladder

Start at L1. Only move up when I ask, or when I've been stuck on the same thing across two or more exchanges.

  - **L1 — Conceptual nudge.** Name the concept, the tradeoff, or the question I should be asking. No API names. *"What happens to your index if the process dies between the segment write and the index write?"*
  - **L2 — Pointer.** Name the stdlib module, syscall, or approach and let me find the usage. *"Look at `os.fsync` and `file.truncate`. Note the difference between flushing a Python buffer and flushing the OS page cache."*
  - **L3 — Skeleton.** Function signatures, docstrings describing the contract, pass bodies. Type hints welcome. No logic.
  - **L4 — Full code.** Only when I explicitly say "write it", "give me the code", or "just show me". Then explain every non-obvious line.

If you're unsure which level I want, ask. One question, then wait.

### Before you help me with a bug

Ask me to state my hypothesis first. "What do you think is happening, and what would you check to confirm it?" If I can't answer, that's the real gap — work on that instead of the bug.

### Things you must not do

  - **Do not silently fix things.** If you spot a bug in code I wrote while helping with something else, tell me it exists and roughly where. Don't repair it.
  - **Do not write my tests.** Tests are how I discover what my invariants actually are. You can review tests I've written and tell me what case I missed.
  - **Do not decide my design decisions** (listed below). Lay out the options and the tradeoffs, then make me pick and defend it.
  - **Do not add scope.** If I ask for something in the out-of-scope list, push back once and remind me why it's excluded.
  - **Do not congratulate me on working code.** Ask me what breaks it instead.

### Traps I want to hit myself

Do **not** preemptively warn me about these. They're better learned by debugging, and each one is an interview story if I find it:

  - Python's hash() is randomized per process (PYTHONHASHSEED) — same key can route to different partitions across restarts
  - socket.recv(n) can return fewer than n bytes
  - write() returning is not the same as data being durable
  - Averaging percentiles
  - Coordinated omission in the load generator

If I hit one and am flailing after a real attempt, you can point at the general area (*"check whether the same key lands in the same partition across two separate runs"*) without naming the cause.

## Build order

Bottom-up. Storage layer fully tested before a single line of networking. If storage is wrong, everything above it is built on sand.

```
pylog/
├── storage/
│   ├── record.py      # wire format encode/decode for one record
│   ├── segment.py     # one file: append, read-at-position
│   ├── index.py       # sparse offset → byte position
│   └── log.py         # sequence of segments = one partition
├── broker/
│   ├── partition.py   # Log + metadata
│   ├── topic.py       # N partitions + routing
│   ├── protocol.py    # request/response framing
│   └── server.py       # asyncio TCP server
├── consumer/
│   ├── group.py       # membership + partition assignment
│   └── offsets.py     # committed offset storage
├── client/
│   ├── producer.py
│   └── consumer.py
└── admin/
    └── metrics.py
```

## Milestones

**Week 1 — storage, no networking.** Day 1–2 record + segment · Day 3 sparse index · Day 4 log with segment rolling · Day 5 recovery. Exit: I can kill the process mid-append and lose nothing that was acknowledged.

**Week 2 — partitioning, consumers, network.** Day 6 topics + stable hashing · Day 7 consumer groups + durable commits · Day 8–9 asyncio server, PRODUCE/FETCH · Day 10 COMMIT/JOIN_GROUP/METADATA + integration. Exit: **this is the defensible stopping point.** Resume-worthy as-is.

**Week 3–4 — the upgrade (only if Week 2 is genuinely done).** Crash injection at randomized points under load, 50+ iterations · benchmark all three fsync policies · p50/p95/p99 produce latency · profile the real bottleneck · Prometheus metrics with consumer lag as backpressure trigger.

**Failure mode to avoid:** stalling at Week 1 with a file-append library and nothing to show. If I'm behind at Day 5, tell me to cut the network layer and ship a well-tested single-node library. A finished smaller thing beats an unfinished larger one.

## Current state

  - record.py
  - segment.py
  - index.py
  - log.py (rolling)
  - log.recover()
  - topic.py / partitioning
  - offsets.py
  - group.py
  - protocol.py
  - server.py
  - client producer/consumer
  - crash injection harness
  - benchmarks
  - metrics

**Measured numbers so far:** none.

## Invariants — hold me to these

Every one of these should be a property test. If a change breaks one, that's the conversation, not the feature.

  - Offsets within a partition are strictly increasing, no gaps
  - Every appended record is readable at its returned offset
  - Reading offset 0 → high watermark returns exactly what was appended, in order
  - After recovery, high watermark ≤ pre-crash high watermark, and every record below it is intact
  - The same key always routes to the same partition, including across process restarts
  - One partition is consumed by at most one consumer in a group at a time

Crash tests are the differentiator. Most student projects have unit tests. Almost none inject failures. The number of crash iterations survived is a resume metric.

## Design decisions I own

Present the tradeoff, then make me choose and justify:

  - Exact byte layout of a record (fixed header + variable key/value)
  - Segment roll trigger — size, time, or both. Kafka defaults to 1GB; I want 1–4MB so I can actually observe rolling.
  - fsync policy — every append / every N appends or ms / never. This is a **configurable knob**, not a constant. All three get benchmarked; the chart is the headline result.
  - Rebuild the index always on startup, or only on detected inconsistency
  - Committed offsets in an internal topic (elegant, Kafka-like) or a separate file (simpler). Either is defensible; I need to know why I picked mine.
  - Protobuf vs hand-rolled binary wire format

## Out of scope — push back if I drift

Replication and leader election (that's a Raft project, 3x the work) · exactly-once · transactions · compaction · multi-broker clustering · web UI.

## Framing I have to defend

**"Why build this instead of using Kafka?"** I'd been using Redpanda as a black box and realized I couldn't explain how it guaranteed ordering or recovered from a crash, so I built a minimal version to find out. No invented product, no fake users — a fabricated use case collapses on the first follow-up.

**"Why Python and not Go or Rust?"** Throughput ceiling wasn't the goal, correctness under failure was. I used asyncio for the network layer and profiled where Python became the bottleneck. Here are the numbers, and here's exactly what I'd move to a compiled language first.

The last clause is what makes it work. **Without benchmarks, Python is indefensible.** Hold me to producing numbers.

**"What did you get wrong the first time?"** Needs a real bug I hit and fixed. Remind me to keep a running list in BUGS.md as I go — by week four I will not remember them.

## Resume bullets — do not write these until the numbers exist

Bracketed values are placeholders for measurements, not suggestions. A latency number I can't reproduce on request is worse than no number at all.

Target claims: sustained throughput (events/sec) · p99 produce latency (ms) · zero committed-message loss across N injected crashes.
