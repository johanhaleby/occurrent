# 107. A live push handler runs outside the handover lock

Date: 2026-08-07

## Status

Accepted. Unit B3 of the post-0.31.0 API review remediation arc (#588), which trails the 0.32.0 tag.

## Context

`BlockingHandover` is the shared engine behind `CatchupThenPushSubscriptionModel` and the projection DSL's
`CatchupProjectionFeed` on the blocking stack. After the catch-up replay finishes and the handover goes live,
every live payload arrived at through `accept(T)` while holding the handover's own monitor, and the fold
(`deliver.accept(payload)`) ran *inside* that `synchronized` block, permanently, for the rest of the handover's
life.

[ADR 90](0090-a-push-sink-feeds-one-consumer.md) already settled the topology this runs under: **one sink per
consumer, not one calling thread.** A listener container with concurrency 8 pointed at one sink is a legal
setup under that decision, and its eight threads each call `accept(..)` for a different broker message. Every
one of them queued on the single lock `deliverLive` held for the length of the fold, so the eight-way
concurrency the listener container was configured for collapsed to one payload folding at a time regardless.
Nobody decided this. It fell out of `deliverLive` needing the lock for its de-dup check and the fold happening
to sit in the same method.

Moving the fold outside the lock is not a plain optimisation. It changes what a handler must tolerate, from
serialised invocation to concurrent invocation, and neither the class javadoc nor `CatchupThenPushSubscriptionModel`'s
said which one applied. A handler written assuming the lock's accidental serialisation (a non-thread-safe map, an
unsynchronized counter) would misbehave under real concurrency the moment the lock stopped providing it. So the unit
measured first, per its brief: is the lock actually costing throughput for a handler with realistic per-event cost,
before deciding whether the contract change is worth making.

### Measurement

A JMH benchmark drove the real `BlockingHandover.accept(T)` (already live) from 1, 2, 4 and 8 concurrent
threads, and compared it against a variant with the same shape as the fix below: the de-dup key reserved
under a short lock, the handler call itself outside it. The handler was a busy-spin standing in for real
per-event work, at three costs: 1 microsecond (near free — an in-memory map write), 50 microseconds (a light
in-memory fold), and 200 microseconds (roughly a small synchronous I/O call, e.g. a lightweight Mongo write).

Throughput, ops/s, `-wi 3 -i 5 -f 1`, JMH `@Threads` per benchmark method:

| workload | threads | current (locked) | proposed (dedup-only lock) |
|---|---|---|---|
| 50µs  | 1 | 13 923 | 11 998 |
| 50µs  | 2 |  5 713 (noisy) | 31 973 |
| 50µs  | 4 | 17 912 | 53 674 |
| 50µs  | 8 | 19 588 | 32 311 (noisy) |
| 200µs | 1 |  1 902 |  4 823 |
| 200µs | 2 |  3 984 |  7 619 |
| 200µs | 4 |  4 566 |  9 334 |
| 200µs | 8 |  4 201 | 14 408 |

The 1µs row is omitted: at that size the benchmark measures lock/dedup overhead, not the question asked, and
the error bars (±100–400%) swamp the signal.

The 200µs row is the clean one. `1 / 200µs = 5000` ops/s is the hard ceiling a *fully serialised* deliverer can
never exceed no matter how many threads call it, and the locked column sits right at it (4 201–4 566 from 2
threads on), which is a textbook fingerprint of full serialisation: adding threads past 2 buys nothing.
`workMicros=50` shows the same shape against its own 20 000 ops/s ceiling (17 912–19 588 from 4 threads on).
The proposed column has no such ceiling and keeps climbing with thread count in both rows: 3.4x higher than
locked at 200µs/8 threads (14 408 vs 4 201), 2.7x at 50µs/4 threads before both variants get noisy at 8
threads (a JMH warning about non-forked-run comparability applies here; the direction is unambiguous, the
exact multiplier at 8 threads on this workload is not worth trusting further than "clearly higher").

**The win is real for any handler whose per-event cost is not negligible** — which is nearly every real push
handler, since a no-op fold has no reason to exist. The lock was capping throughput at roughly
`1 / (handler duration)` regardless of how much concurrency the caller configured, silently defeating the
concurrency ADR 90 already declared legal.

## Decision

**The de-dup key is reserved under the handover's lock; the handler call runs outside it.** `BlockingHandover`
now has a `reserve(T)` that does only the lock-requiring part — check `deliveredIds`, add the key if the
payload needs delivering — and returns whether the caller should deliver. `accept(T)` calls `reserve` inside
`synchronized (lock)`, then calls `deliver.accept(payload)` after the block has exited, only if `reserve`
returned `true`. The buffer-to-live drain (`drainBufferAndGoLive`) does the same: it collects the buffered
payloads that need delivering while holding the lock, then delivers them after releasing it. The replay loop in
`catchUp` was already outside the lock and needed no change.

**This applies to `BlockingHandover` only.** `ReactiveHandover` has no equivalent monitor: its phases are
serialised by `concatMap`, not by a shared lock a caller's thread blocks on, so concurrent broker threads were
never queuing on anything comparable there. #588 was scoped to the blocking engine and the measurement above
does not extend to the reactor one.

## Consequences

**A push handler must now tolerate concurrent invocation, wherever the caller configures more than one
delivering thread.** This was always legal under ADR 90's topology; it was just never actually reachable
because the lock serialised it by accident. A handler that is not thread-safe and is fed from a listener
container with concurrency greater than one will now see real races it did not see before. This is a
breaking behavioural change on **published** API: `CatchupThenPushSubscriptionModel` and `CatchupProjectionFeed`
(blocking stack) both shipped in 0.31.0. There is no OpenRewrite recipe, the same as ADR 90 and ADR 104 in this
same area — this is a behaviour change at an unchanged call site, and there is no source edit for a recipe to
make. The migration guide carries it instead (`doc/migration/upgrading-to-0.32.0.md`).

**Delivery order across concurrently-delivering threads is no longer guaranteed**, where the lock used to impose a
total (if arbitrary) order as a side effect. This was never a documented guarantee, so nothing that relied on it
was relying on something this project promised. A single-threaded caller (concurrency 1, the common case) sees no
change: `accept` still delivers synchronously on the calling thread before returning, in the order it was called.

**De-dup correctness is unaffected.** The reservation that decides whether a payload is delivered at all still
happens atomically under the lock, so the replay-to-live overlap is still de-duplicated exactly as before —
only the fold itself moved outside the monitor, not the bookkeeping that guards it.

**`BlockingHandoverTest` gained a concurrency test** that rendezvous four `accept` calls inside `deliver` on a
shared `CyclicBarrier`. It fails by timeout under the pre-fix, fully-locked implementation (verified by
reverting the production change and running it), and passes once delivery moved outside the lock, which is a
stronger check than a throughput benchmark for a correctness contract.
