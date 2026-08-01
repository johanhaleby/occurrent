# 85. Every subscription model can be stopped

Date: 2026-08-01

## Status

Accepted. This is the first slice of #481. The `occurrent.subscription.mode` property that motivates it
follows separately.

## Context

ADR 82 shipped deny-by-default subscription testing: stop every subscription, then let each test name the
ones it needs. It works for the change-stream models and the in-memory model, and silently does not work
for four others.

`RegisteringSubscribable` has a blocking and a reactor twin, and four subclasses between them, the
synchronous models and the push models. None implemented `SubscriptionModelLifeCycle`, so
`OccurrentSubscriptionsExtension.stoppedByDefault(...)` could not stop them, `startAll()` could not start
them, and a test that believed it had stopped everything still had synchronous projections running.

The absence was deliberate, and both classes said so:

> no `SubscriptionModelLifeCycle`: there is nothing to start or stop when the events arrive from the
> caller, and a pause would drop them rather than defer them, since there is no feed holding them back.

## Decision

**The four models get a lifecycle, on `RegisteringSubscribable` rather than on each subclass.** The base
class already owns the id registry and `route(...)`, which are exactly the two things a lifecycle needs,
so one change per twin covers synchronous and push on both stacks and any future subclass.

**The stated reason for not having one does not hold.** "A pause would drop events rather than defer them"
is true, but it is not a reason to omit the lifecycle, because `InMemorySubscriptionModel` has the same
property and implements the lifecycle anyway. Its `accept(List)` returns early when the model is stopped,
so events handed to a stopped in-memory model are dropped exactly the way events handed to a stopped
synchronous model now are. The precedent decided this already, in the opposite direction from the javadoc.

**Dropped, not deferred, is the documented contract.** An event fed to a stopped model, or matching a
paused subscription, never reaches that handler, and resuming does not replay it. That is the honest
description of what a register-only model can offer, and it is what a test wants. Both class javadocs now
say it in those words rather than using it as an argument against having a lifecycle at all.

**Say plainly that this is sharper for a synchronous projection than for an async one.** The point of a
synchronous projection is that it updates in the same transaction as the write. A stopped synchronous
subscription means the write succeeds and the projection does not run, so an application that stops
subscriptions and then accepts traffic has writes landing with no projection. That is the intended
behaviour in a test and a foot-gun in production. It is documented rather than prevented, because
preventing it, by failing the write, would break the case the feature exists for.

**Registering on a stopped model yields a paused subscription.** This mirrors
`SpringMongoSubscriptionModel.subscribe` and `InMemorySubscriptionModel.subscribe`, and it is what will
let the `mode=manual` property stop a model before the annotation bean post-processors register anything.
Without it the property could not work for these models.

**The blocking twin also implements `IntrospectableSubscriptionModel` (ADR 83).** The registry is already
there, so `subscriptionIds()` is one line, and without it `startAll()` would skip exactly the models this
ADR is adding. There is no reactor twin of that capability, for the reason ADR 83 gives.

## Consequences

`RegisteringSubscribable` now implements `SubscriptionModelLifeCycle` instead of
`CancellableSubscriptions`. The lifecycle interface extends the cancellation one, so nothing a caller
could already do stops compiling, and the new methods are `final` on the base class so no subclass can
weaken them.

`route(...)` gained two checks on the dispatch path, one volatile read and one set lookup per
registration. That is the cost of the feature and it is paid on every synchronous write. The set is empty
in the default running case, so the lookup is cheap, but it is not free and it is worth knowing about if a
synchronous projection ever shows up in a profile.

On the reactor twin the running check sits inside a `Mono.defer`, so it is evaluated when the returned
`Mono` is subscribed rather than when it is assembled. Assembling a dispatch while running and subscribing
after a stop delivers nothing, which is the behaviour a caller would expect and is covered by a test.

The subscription conformance suite planned in #395 should pin the lifecycle across all models, including
the drop-not-defer semantics, which currently hold because every implementation agrees rather than because
anything enforces it.
