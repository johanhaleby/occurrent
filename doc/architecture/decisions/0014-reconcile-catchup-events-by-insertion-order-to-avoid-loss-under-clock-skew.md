# 14. Reconcile catch-up events by insertion order to avoid loss under clock skew

Date: 2026-06-18

## Status

Accepted

## Context

`CatchupSubscriptionModel` replays historic events from `EventStoreQueries` and then hands over to a live, position-aware subscription. Events written while the replay is running are picked up by a "delta" reconciliation step that runs after the bulk replay and before the live subscription takes over.

The bulk replay reads events sorted by the configurable `catchupPhaseSortBy`, which defaults to `SortBy.ascending(TIME, STREAM_VERSION)`. The delta step then re-queried the same sort with `skip = numberOfEventsBeforeStartingCatchupSubscription` and `limit = delta`, expecting the skipped prefix to be the events already replayed and the remainder to be the events written during the replay.

That assumption only holds when the sort order matches insertion order, and it does not under clock skew. Take an event `E` written during the replay whose CloudEvent `time` is earlier than the replay cursor's current position (a writer whose clock runs behind):

1. `E` sorts before the `skip` boundary in `(time, streamversion)` order, so the delta re-query skips it.
2. The replay cursor already passed that sort position, so the bulk replay never read `E`.
3. `E` was written before the live subscription's resume position was captured, so the live subscription (which resumes by database position, for example the MongoDB oplog, not by `time`) never delivers it.

`E` is delivered by none of the three paths, so it is silently lost. Because it is a miss and not a duplicate, consumer-side idempotency cannot recover it. The window is the tail of the replay, when the cursor reaches roughly "now" and a concurrent write with a slightly behind clock lands just behind it.

We considered three other directions and rejected them:

- **Capture the live resume position before the bulk replay instead of after.** This would let the live subscription cover everything written during the replay. But the resume position is a token into the database change stream (the oplog in MongoDB), and the oplog is a capped collection. On a long replay over a large store, a token captured before the replay can age out before the handover, leaving the live subscription unable to resume at all. That trades a rare silent miss for a hard failure on exactly the long replays the fix is meant to protect.
- **Sort the catch-up by a monotonic, server-assigned global position.** This is the clean answer, but Occurrent has no such field today. ADR 0007 proposed one and was rejected, ADR 0008 specifies it and is still a proposal. Building it for this fix is out of scope.
- **Document a contract requiring a monotonic sort key.** The default `(time, streamversion)` is not monotonic with insertion, and the config Javadoc already explains why a store-specific key like MongoDB's `_id` cannot be the default in a store-agnostic API. A documentation-only change does not fix the default behavior.

## Decision

Reconcile the during-catch-up events by insertion order rather than by the time-based sort.

The events written during the replay are, by definition, the most recently inserted events. So the delta step now reads the newest `numberOfEventsNotConsumed` events in insertion order and reverses them back to insertion order for delivery:

```java
eventStoreQueries.query(catchupFilter, 0, numberOfEventsNotConsumed, SortBy.natural(DESCENDING))
```

`SortBy.natural` is insertion order. It is monotonic with insertion regardless of the events' `time`, so a clock-skewed event written during the replay is still among the newest events and is no longer skipped. The live resume position stays captured after the bulk replay, so its token stays fresh and the oplog problem above does not arise. The bulk replay keeps `catchupPhaseSortBy` unchanged, so its time-ordered delivery and its index requirements are the same as before.

`SortBy.natural` has to mean the same thing on both event stores for this to be correct. On MongoDB it maps to `$natural`, which is global insertion order. On `InMemoryEventStore` it iterated the per-stream map, so an event appended to an existing stream was grouped with that stream rather than placed at the global end. That is now fixed: `InMemoryEventStore` assigns each event a global insertion sequence at write time and sorts `SortBy.natural` by it, matching MongoDB. The existing test `sort_by_natural_asc_sorts_by_insertion_order` already described this intent, it only passed before because it wrote one event per stream.

### Performance

This matters on production-scale stores (the motivating case is a rebuild over roughly 113 million events), so the query cost is part of the decision.

The delta query reads the newest events with a `limit` and no `skip`. On MongoDB, `find(filter).sort({$natural: -1}).limit(delta)` is a reverse collection scan that stops once it has `delta` matches. It reads the recent tail, never the backlog. Measured with `explain` against a 100,000-document collection with `delta = 5`:

- Unfiltered (a beginning-of-time catch-up): `totalDocsExamined = 5`.
- Filtered to a type matching every other document: `totalDocsExamined = 10`.

So the cost scales with how many events were written during the replay window, not with the size of the store. This is also cheaper than the previous delta, which passed `skip = numberOfEventsBeforeStartingCatchupSubscription` and so walked the entire prefix (on a 113M store, roughly 113M index entries) on every catch-up that had any concurrent writes.

`$natural` cannot use an index. With a selective filter on a busy store, the reverse scan reads recent non-matching writes until it has collected `delta` matches, still bounded by the replay-window write volume and never the backlog. There is no index-backed and skew-safe option without a global position field, so an index-backed reconciliation is the future optimization path if ADR 0008 lands.

The bulk replay still needs the caller's `(time, streamversion)` (or time) index, exactly as before. This change does not add or remove any index requirement.

## Consequences

Positive:

- The catch-up to live handover no longer loses events under clock skew.
- The delta reads only the recent tail, so it is faster than the previous `skip`-based reconciliation on large stores.
- It removes the spurious duplicate the old delta produced, where a skewed write shifted the sort window and an already-replayed event was delivered again.
- `SortBy.natural` now means global insertion order on both the in-memory and MongoDB stores, so the two behave the same.

Negative:

- The delta's reverse `$natural` scan is not index-backed. With a selective filter on a busy store it reads recent non-matching writes until it has `delta` matches. For large rebuilds, keep the replay window low-write so this stays small. This is the same operational posture the motivating rebuild already uses.
- Closing the loss fully still needs the global position from ADR 0008. Until then, the no-live case (an in-memory projection that replays from beginning of time on every restart and never starts the wrapped subscription) relies on the same insertion-order delta and is correct on stores whose natural order is global insertion order.
