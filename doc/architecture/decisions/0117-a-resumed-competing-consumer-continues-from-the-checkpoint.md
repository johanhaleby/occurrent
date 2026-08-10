# 117. A resumed competing consumer continues from the checkpoint

Date: 2026-08-09

## Status

Accepted. Fixes #668. Found and deliberately left out of [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) (#665), which named the same node's own regain as a different defect and filed it separately.

## Context

A competing consumer that loses its lease is paused, and a competing consumer that wins the lease back is resumed. Resuming used to continue the paused subscription itself, from the change-stream position it had already read, with nothing re-reading the checkpoint. Whichever node held the lease in the meantime kept moving the checkpoint forward, so the node coming back was behind where the subscription actually was. It redelivered everything the other node had already handled and wrote the checkpoint backward as it went.

Delivery stayed at-least-once either way, so nothing was lost, and the redelivered events corrected themselves as the node caught back up. [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) named exactly this limit and filed it here rather than fixing it, since it needed its own look at what a fix does to the catch-up models sitting above a subscription.

## Decision

**`DurableSubscriptionModel.resumeSubscription(id)` re-reads the stored checkpoint and asks the wrapped model to reopen there, instead of asking it to continue from its own tracked position.** A new capability interface, `RepositionableSubscriptions`, adds a two-argument `resumeSubscription(id, startAt)` next to the existing one-argument form. `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` both implement it, and both set the same `currentStartAt` reference the existing resume already reuses to the caller's position first, then run the rest of that resume path unchanged.

**The fallback, when there is nothing to improve on, is the wrapped model's own resume, never `StartAt.subscriptionModelDefault()`.** It applies when no checkpoint is stored yet, when the wrapped model does not implement `RepositionableSubscriptions`, and when the subscription opted out of this model's checkpoint management from the start. The default resolves to the present at the moment it is resolved, so using it here would silently drop whatever was published while the subscription was paused, the very loss [ADR 94](0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md) already ruled unacceptable for a pause. The wrapped model's own tracked position is what every resume already used before this fix, so the fallback changes nothing for the cases this fix does not reach.

**A checkpoint write with `EveryN(n)` still leaves a window of up to `n - 1` events the resumed node redelivers, zero with the default `everyEvent()`.** The stored checkpoint is only as recent as the last write, and a node writing less often than every event resumes from whatever it last wrote, not from the position it would have reached by the next write. This is the same tradeoff `EveryN(n)` already makes for a crash, not a new one this fix introduces.

**`CatchupSubscriptionModel` implements the interface as a plain forward to whatever the model it wraps resolves to.** Its own one-argument resume already bypasses its catch-up children, going straight to the durable model underneath, and the two-argument form does the same. A lease regain is a resume, not a fresh subscription, so it must never re-trigger a catch-up replay, and this keeps it that way whether or not a caller reaches the composition through `CatchupSubscriptionModel` directly.

**`CompetingConsumerSubscriptionModel` needs no change.** It already resumes a regained lease through the one-argument `resumeSubscription(id)` on whatever it wraps, and that call now reaches `DurableSubscriptionModel`'s corrected resume unchanged, wherever `DurableSubscriptionModel` sits in the composition.

**Blocking only.** No reactor competing consumer subscription model exists, so this fix has nothing to reach on that stack.

## Consequences

This amends what [ADR 94](0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md)'s pause section implied about a resumed subscription continuing from the position it had read. That is still what the wrapped Mongo model does on its own, but a `DurableSubscriptionModel` sitting above it now resumes from the stored checkpoint instead whenever one is available and the model underneath can be repositioned.

The limit [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) named and filed separately, "a node that lost the lease and later takes it back resumes its own subscription from the position it had, not from the checkpoint the other node advanced", is what this ADR fixes. That sentence is no longer true of the current code, and this is the record of why.

`CompetingConsumerSubscriptionModelFixture.deliversEventsPublishedWhilePaused()` still answers `true` for the same outcome as before, reopening from a position that covers the paused window, but the position it reopens from is now the checkpoint another node may have advanced, not always the position this node's own delegate last read.
