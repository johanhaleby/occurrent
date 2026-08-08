# 114. A lease expires on the database's clock, not the asking node's

Date: 2026-08-08

## Status

Accepted. Fixes #659. Filed separately by [ADR 113](0113-a-competing-consumers-status-and-its-lease-call-are-one-step.md), which fixed a different weakness in the same lease and left this one alone on purpose.

## Context

`MongoListenerLockService` wrote a lease's `expiresAt` from the holder node's own `Clock`, `clock.instant().plus(leaseTime)`, in both `acquireOrRefreshFor` and `commit`. Whether that lease had expired was decided in `lockIsExpired` by comparing `expiresAt` to `clock.instant()` on whichever node was asking. Two different wall clocks ended up in one comparison.

With the default 20 second lease, a node whose clock ran ahead of the holder's by more than what remained of the lease took the lock while the holder still believed it had it, and both consumed until the holder's next refresh noticed the loss. A node whose clock ran behind waited out the skew on top of the lease time before it took over from a dead holder. Nothing in the code or the documentation stated the assumption that the cluster's clocks agreed to within a lease.

## Decision

**Both the write and the read judge `expiresAt` against `$$NOW`, MongoDB's own aggregation variable for the current time on the server, not against any node's `Clock`.** `$$NOW` is fixed once per operation and is the same value on every member of the deployment, so acquiring, refreshing and judging a lease all agree with each other regardless of clock skew between nodes.

**`acquireOrRefreshFor` became a single upsert matching only on `_id`, with every decision moved into the update pipeline.** The original filter was `and(eq("_id", subscriptionId), or(lockIsExpired(clock), eq("subscriberId", subscriberId)))`, and MongoDB refuses `$expr`, which `$$NOW` needs, inside an upsert's filter. Splitting the filter and the upsert into two calls does not work either. A second attempt whose filter also mentions `subscriberId` gets that field seeded into a freshly upserted document before the update pipeline ever runs, which erases the very "is this subscriber already the holder" distinction the pipeline depends on to decide whether a take is a refresh or a fresh acquisition. Confirmed against a live MongoDB 8.0 replica set. A filter of `{_id: "s", subscriberId: "holder"}` upserts a document where `$subscriberId` already reads `"holder"` inside the pipeline, before any stage sets it.

Matching on `_id` alone sidesteps both problems. The update pipeline computes one condition, `isAllowedFor(subscriberId)`, true when nobody holds the lease yet, when `subscriberId` already holds it, or when the current lease is expired, and every field it sets is conditional on it: `subscriberId` moves to the caller's id only if allowed, `expiresAt` moves to `$$NOW` plus the lease time only if allowed, and `version` stays put on a refresh, increments on a genuine takeover, and stays put again when the caller was not entitled to touch it. A document already held by someone else with time left on the lease comes back unchanged. The caller tells the two outcomes apart by comparing the returned `subscriberId` to its own, not by whether the call matched, so the method's return type and its two-line contract (`Optional` present when the lock is held, empty otherwise) are unchanged.

**`commit` writes `expiresAt` the same way, still as `set("expiresAt", $$NOW + leaseTime)`, now on a single-stage update pipeline instead of a plain update.** Its match filter, `_id` and `subscriberId`, is untouched, since which node currently holds the lease is a different question from which clock judges its expiry.

**Matching an upsert on `_id` alone, against its unique index, is the shape MongoDB itself documents as immune to a duplicate-key race**, unlike the original multi-clause filter, which is why `acquireOrRefreshFor` needed to catch `DUPLICATE_KEY` as its primary signal that someone else held the lock. That signal is now `Optional.empty()` from the `subscriberId` comparison instead. The `DUPLICATE_KEY` catch stays as a defensive fallback rather than a path this method relies on.

**The `Clock` parameter is gone everywhere it existed only to serve this comparison.** `MongoListenerLockService.acquireOrRefreshFor` and `commit` no longer take one. `MongoLeaseCompetingConsumerStrategySupport`'s constructors no longer take one either, since its own two call sites into `MongoListenerLockService` were the only use it had. The public `.clock(Clock)` builder method on both `NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy` stays, since removing a public method is a breaking change with its own migration path, but it is now a no-op kept for source compatibility, and its javadoc says so.

**Tests that used to move a `Clock` past the lease now seed `expiresAt` on the lock document directly, through the raw `MongoCollection`, using the real wall clock.** A database clock cannot be moved by a test, so `MongoLeaseTimingTest` and `MongoLeaseRaceTest` construct "close to expiring" and "already expired" by writing a real `Instant` a small margin from `Instant.now()` straight into the document, then let the code under test judge it against MongoDB's actual current time exactly as production does. This needs no test-only addition to production code. `MutableClock` is deleted.

## Consequences

`NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy` no longer expose any way to make lease timing diverge from the database's own clock, including through the `.clock(Clock)` builder method, which now does nothing.

The four acquisition outcomes are unchanged. A free lock is taken, an already-held lock is refreshed, an expired lock is taken over, and a lock held by someone else with time left is refused. What changed is which clock decides the third and fourth of those.

`MongoLeaseTimingTest`'s and `MongoLeaseRaceTest`'s margin around `Instant.now()` when seeding a lock document has to be large enough that the round trip to seed it and the round trip the test makes afterwards cannot cross it on a loaded CI runner. Both tests use 2 seconds against a 10 minute lease, which is long enough for that and still close enough to the boundary each test is exercising.
