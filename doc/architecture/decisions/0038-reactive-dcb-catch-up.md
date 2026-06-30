# 38. Reactive DCB catch-up

Date: 2026-06-30

## Status

Accepted

## Context

Live reactive DCB subscriptions shipped (ADR 37), but a `DcbStartAt.beginning()` started live rather than replaying, so a reactive read model could not be rebuilt from history. The blocking stack has DCB catch-up in `CatchupSubscriptionModel`. This adds the reactive equivalent. There was no reactive catch-up of any kind before this, not even for streams, so this is the first.

It is scoped to DCB only. `dcbposition` is a monotonic server-assigned counter, so replay pages by position with no time sort and no count, which sidesteps the clock-skew loss and the count undercount the stream catch-up has to defend against. The reactive stream catch-up, if it is ever needed, is a separate piece.

## Decision

Add `ReactorDcbCatchupSubscriptionModel` in a new module `subscription/util/reactor/dcb-catchup-subscription`. Its `Flux<CloudEvent> subscribe(DcbQuery, DcbStartAt)` mirrors the blocking DCB catch-up: when the start is a DCB position it replays history and hands over to live, otherwise it goes straight to live through the reactive `DcbSubscriptionModel` facade.

The catch-up `Flux` is `concat(bulk, reconcile, live)`:

- The live resume token is captured with `globalSubscriptionPosition()` BEFORE any replay read, threaded through `flatMapMany`. This is the load-bearing invariant: an event that commits during the replay is still delivered, because live resumes from a token taken before the replay started.
- `bulk` pages the matched events from the start position to the head observed at the start, in `dcbposition` windows.
- `reconcile` keeps paging until the head stops advancing, covering events written during the bulk replay.
- `live` subscribes from the captured token, filtered to `dcbposition > 0`, matches the query, and is not already delivered.

### The one divergence from blocking: id-based dedup across both phases

The blocking catch-up caches only the reconciliation tail, because its live subscription resumes after the bulk head. The reactive `globalSubscriptionPosition()` token resumes inclusively, so the live change stream re-delivers boundary events the replay already emitted. The fix is to dedup the handover by event id (not by a position watermark) and to feed the dedup cache from the bulk phase as well as reconcile. Id-based dedup also preserves in-flight-hole recovery: an event below the replay head that was never seen during the replay is still delivered once by live, because it is not in the cache.

The cache is bounded (default 1000 ids, evicts eldest). It is insertion-ordered, so the most recently replayed ids are retained, which are exactly the ones live can re-deliver (the boundary and the during-replay commits near the head). Bulk events far below the token are evicted but are never re-delivered, so eviction is safe. This is the same bounded-cache assumption the blocking catch-up makes.

## Consequences

- A reactive read model can be rebuilt from the beginning by subscribing with `DcbStartAt.beginning()` or any `afterPosition(...)`, then continue live. This closes the last reactive DCB gap, so reactive DCB now matches the blocking stack at the store, application-service, query, and subscription layers.
- The replay-longer-than-oplog trade-off carries over: if the replay outlasts the change stream history, the captured token ages out and the live resume fails loudly rather than dropping events.
- Position-storage and durable resume are left to the existing `ReactorDurableSubscriptionModel` composing on top, so this module is the catch-up-then-live `Flux` and nothing more.
