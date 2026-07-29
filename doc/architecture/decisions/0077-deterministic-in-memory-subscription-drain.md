# 77. A deterministic drain for the in-memory subscription model, and no position watermark

Date: 2026-07-29

## Status

Accepted. Adds one method (plus a convenience overload) to the released `InMemorySubscriptionModel`. Additive, so no migration.

## Context

Every test of an asynchronous projection ended by polling an assertion:

```kotlin
await untilAsserted { assertThat(store["johan"]).isEqualTo("Johan Haleby") }
```

Nothing let a test ask whether the projection had processed the events it just wrote, so a test had to poll until an assertion passed or a timeout expired. Writing the documentation's Testing chapter forced that shape to be documented as the recommendation, which is what prompted issue #451.

Two costs are real. A genuine regression waits out the whole timeout and then reports the last assertion failure rather than saying the projection never advanced. And polling hides the difference between synchronous and asynchronous: a projection registered `SYNCHRONOUS` is meant to be updated inside `execute(...)`, but a polling test passes whether the update was synchronous or merely fast, so a mode misconfigured to `ASYNC` is invisible. `SynchronousSubscriptionModelTest` already asserts with no polling at all, and that is the shape an in-memory test should be able to have.

Delivery is genuinely asynchronous in the model tests use. `InMemorySubscriptionModel.accept(...)` offers each matching event onto a per-subscription `BlockingQueue` on the caller's thread, and a thread from a cached pool later polls that queue and invokes the handler. So a write returns before the read model is updated, with no signal in between.

`Subscription.waitUntilStarted()` does not help. Every implementation of it reports that the read side opened, a thread launched or a cursor acquired or a `Flux` subscribed. None of them says anything about an event having been handled.

## Decision

**`InMemorySubscriptionModel` gains `waitUntilAllEventsProcessed(Duration)`,** with a no-argument convenience defaulting to 10 seconds. It returns whether everything drained rather than throwing, mirroring `Subscription.waitUntilStarted(Duration)`, which is the shape this stack already uses for a bounded wait.

**Done is counted, not inferred from the queue.** `InMemorySubscription` keeps a count of outstanding events, incremented before an event becomes visible to the consumer thread and decremented in a `finally` after the handler returns. A subscription is idle when that count is zero.

Reading `queue.isEmpty()` instead is wrong, and this was proven rather than reasoned: the run loop takes an event off the queue and only then calls the handler, so an event being handled right now is in neither the queue nor any flag set afterwards. A queue-only implementation was written first, and the slow-handler test failed against it exactly there. The count also has no such window, because the increment happens before the event can be seen and the decrement after the handler completes. Incrementing after the offer would be a different bug, since the consumer could poll, handle and decrement before the increment landed.

The `finally` is load-bearing. A handler that exhausts its retries and throws would otherwise leave the count stuck above zero and hang every later wait.

**Pausing does not exclude a subscription from the wait.** The first version skipped paused subscriptions, on the stated ground that anything queued before a pause could never drain. That ground is false, and Copilot caught it on review. `pauseSubscription` only records a flag that stops `accept(...)` from queueing anything new, and `InMemorySubscription`'s run loop checks only `shutdown`, so a paused subscription's own thread keeps draining whatever was already queued.

Once the reason was gone the filter had nothing left to justify it, so it was removed rather than reworded. A backlog from before a pause does finish and is worth waiting for, and the only case the filter actually covered was a paused subscription stuck in its handler, where reporting success would have been a lie. That case now reports the timeout instead, which is the honest answer and needs no caveat in the javadoc.

**A throwing handler is waited out.** The count stays raised across the retry strategy's attempts, so the wait blocks until it gives up. That is the wanted behaviour and it is the reason the wait is bounded by a timeout rather than blocking forever.

## Rejected: waiting until a subscription has processed up to a position

Issue #451 proposed this first, and it is unsound. ADRs 7 and 21 establish that the global position is reserved outside the write transaction, so positions are strictly monotonic but never dense: a rolled-back write leaves a permanent gap, and a lower position can commit after a higher one. A wait for "processed everything up to P" is therefore the gap-free frontier those ADRs already rule out. On MongoDB it would report caught-up while a late-committing lower-position event was still pending, which is worse than polling because it fails silently rather than slowly.

The issue also claimed `WriteResult` already carries what a caller would need to name such a point. It does not. `WriteResult` is `(String streamId, long oldStreamVersion, long newStreamVersion)` and carries no global position at all, in every store implementation.

A per-stream version would be sound, since stream versions are dense and ordered within a stream, but a subscription spanning several streams has no single version to wait for, so it does not answer the question a projection test asks.

## Rejected for now: waiting for a specific event id, on any model

Sound, because an event id carries no ordering assumption, and it would work against a real MongoDB. Deferred because every model would have to track handled ids per subscription, which is memory and bookkeeping in production code bought entirely for a test-time benefit. Revisit when someone genuinely needs determinism against a real store rather than in memory.

## Consequences

- An in-memory projection test drains once and then makes a plain assertion, so it needs no Awaitility and reports a real failure immediately.
- The synchronous versus asynchronous distinction becomes visible. The synchronous idiom is no wait at all, so a projection whose mode is misconfigured to `ASYNC` now fails the test instead of passing slowly.
- This is in-memory only by design, and that is not a gap in the change-stream models. A change stream tails an unbounded cursor with no end-of-stream signal, so "everything written has arrived" has no definition there, and a test against a real MongoDB keeps polling.
- The timeout is measured as elapsed against a budget rather than as now against a precomputed deadline, because a large `Duration` overflows such a deadline to a negative value and would report an immediate timeout. Also found on review.
- The wait polls its own condition internally on a short interval. The determinism comes from the condition being exact, not from avoiding a sleep, and the alternative (a lock and condition signalled per completed handler) is more machinery than the guarantee needs.
- Only the projection tests that use the in-memory model were converted. The roughly 90 other Awaitility test files are change-stream or Spring integration tests where polling is still the correct thing, so sweeping them would be a large diff that mostly made things worse.
- Awaitility was never a user-facing problem, contrary to the issue. Every module declares it in test scope and Maven never propagates test scope, so a user depending on the Projection DSL never received it. The gain here is determinism and a better failure, not dependency hygiene.
