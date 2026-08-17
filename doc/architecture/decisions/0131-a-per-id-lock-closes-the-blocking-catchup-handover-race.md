# 131. A per-id lock closes the blocking catch-up handover race

Date: 2026-08-17

## Status

Accepted. Resolves #827.

## Context

`AbstractCatchupSubscriptionModel`'s per-attempt identity check (#737, PR 823) made the ownership decision atomic.
`endReplayIfStillCurrent` compares a `CatchupAttempt` against the map entry it registered under
`currentAttempt.put` in `startCatchupAsync`, so a cancelled or superseded attempt can tell it is no longer current
instead of clobbering whichever attempt took the id over. That fix closed every removal site inside the replay
loop. It did not close what happens after the decision.

Once `endReplayIfStillCurrent` returns and removes the map entry, the finishing attempt still has work left scoped
to that id, an optional temporary checkpoint delete, then the delegate `subscribe` call (or the cancelled-cleanup
branch). Nothing held ownership across that tail, so two different things could interleave with it:

- `cancelSubscription(id)` landing after the decision but before the delegate `subscribe` call found nothing in
  `currentAttempt` to flag (the entry was already gone) and the delegate did not know the id yet either, so the
  cancellation was silently lost and the delegate went live anyway. This was the issue's original scope.
- A fresh attempt registering for the same id in that same gap could save its own temporary checkpoint before the
  finishing attempt's delayed `cfg.storage().delete(subscriptionId)` ran, and the stale delete then removed the
  fresh attempt's position instead of its own. PR 823's review surfaced this in both `StreamCatchupSubscriptionModel`
  finishing tails (the time-based and the position-ordered path) and in the DCB catch-up model's, and it was
  deferred here rather than patched three times independently.

Both come from the same structural gap. The identity check covers the map entry, not every id-scoped side effect
after it.

The reactor catch-up models (`ReactorStreamCatchupSubscriptionModel`, `ReactorDcbCatchupSubscriptionModel`, both via
shared `NamedCatchupSupport`) do not have this gap. `NamedCatchupSupport.handOver` holds `synchronized (state)`
across the whole span, from the ownership check through the delegate `subscribe` call, and `cancelSubscription`
takes the same per-attempt `state` monitor before flagging cancellation or, if handover has already released the
monitor, before falling through to the delegate's own `cancelSubscription`, which by then has something real to
cancel. A race resolves to "the flag is set before handover checks it" or "handover fully completes and the
delegate cancel afterward genuinely cancels it", never the silent-loss window this issue describes. The reactor
models also
never persist a temporary checkpoint of their own, "this model does not persist subscription positions, layer a
durable model on top," so the checkpoint-delete manifestation has no reactor equivalent to close either. This is a
blocking-only fix, not a case for patching both sides identically.

PR 812's `Disposables.swap()` in `ReactorDurableSubscriptionModel` is a related but distinct precedent. It gives a
map entry a stable identity so an error handler's removal is unambiguous, a narrower problem (which entry to
remove) than the one here (which operations must not interleave at all).

## Decision

Add one `ReentrantLock` per `subscriptionId` to `AbstractCatchupSubscriptionModel`, exposed through a small
`HandoverLock` (an `AutoCloseable` whose `close()` declares no checked exception, so a try-with-resources releasing
one needs no catch clause). Every caller that changes who owns an id for a moment takes it:

- `startCatchupAsync`'s registration (`runningCatchupSubscriptions.put` and `currentAttempt.put`).
- Each finishing tail, from `endReplayIfStillCurrent` through the checkpoint delete (when it runs) and the delegate
  `subscribe` call or the cancelled-cleanup branch. This is three separate call sites, the time-based and the
  position-ordered path in `StreamCatchupSubscriptionModel`, and the DCB catch-up model's single path.
- `cancelRunningCatchup`'s flag-and-cleanup body.

The lock is held only across these short transitions, never across an in-flight replay, so a long catch-up is never
serialized by it, only the moments its ownership actually changes hands are. That is what makes the two races
impossible together instead of independently patched:

- A cancellation racing a finishing attempt now either flags cancellation before the attempt reaches its
  decision (the attempt then skips both the delegate `subscribe` and its own checkpoint cleanup, exactly as
  `wasCancelled()` already gated), or it blocks until the attempt's whole tail, delegate `subscribe` included, has
  completed, and then the outer `cancelSubscription`'s call to the delegate genuinely cancels what just went live.
- A fresh attempt's registration cannot begin until a still-finishing attempt for the same id has fully released
  the lock, checkpoint cleanup included, so a fresh attempt can never write anything for a stale delete to race.

A `ReentrantLock`, not `synchronized`, because a handover span can call into storage or the delegate, and every
caller here runs on the dedicated virtual thread `startCatchupAsync` starts for each attempt. Blocking inside a
`synchronized` block pins the underlying platform thread for as long as the virtual thread is blocked, so no other
virtual thread can use that platform thread meanwhile. A plain lock does not have this effect.

The lock registry never evicts entries. A `subscriptionId` is application-defined and low-cardinality in every
documented usage of this model, unlike a per-event or per-request key, so the registry's own slow, unbounded growth
over a model's lifetime is the cheaper trade against a reference-counted eviction scheme built for a key space that
does not need it here.

## Consequences

Both manifestations are closed by one mechanism instead of three independent point patches, and the shape matches
what `NamedCatchupSupport` already proves works for the reactor side, a per-attempt critical section spanning the
decision and its delegate handoff, adapted to a lock a virtual thread can safely block on.

The lock registry grows for the life of a model instance, one entry per distinct `subscriptionId` it has ever run a
catch-up for, never reclaimed. Acceptable for the documented cardinality, but it would need revisiting if a caller
ever minted `subscriptionId` values per request or per event rather than per named subscription.

A `cancelSubscription` call for an id whose finishing tail is in flight now blocks for that tail's duration
(identity decision, optional checkpoint delete, delegate `subscribe`) instead of returning immediately with a
sometimes-silently-lost effect. That tail does a fixed amount of work, one identity check, at most one storage
delete, one delegate `subscribe` call, so the wait ends quickly rather than growing with anything else in the
system. It is what makes the cancellation's effect reliable rather than racy.
