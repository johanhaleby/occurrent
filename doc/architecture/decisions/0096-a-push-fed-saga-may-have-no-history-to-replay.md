# 96. A push-fed saga may have no history to replay

Date: 2026-08-04

## Status

Accepted. Implements #349, the `@Saga(source = PUSH)` half that [ADR 91](0091-a-push-catch-up-replays-off-the-startup-path.md)
deferred.

## Context

Running a saga from a push feed already worked programmatically: `SagaRunner.agnostic(pushModel, converter).run(...)`
takes any `Subscribable`, and both `PushSubscriptionModel` and `CatchupThenPushSubscriptionModel` are one. What was
missing was the declarative path. `@Saga` had no `source`, so a Spring application could only feed a saga from the event
store, while `@Projection` has had `source = PUSH` since [ADR 62](0062-pluggable-projection-event-source.md).

Mirroring `@Projection` covers most of it: add `source`, `subscriptionModel` and `subscriptionModelName`, resolve the
feed bean, wrap it in a `CatchupThenPushSubscriptionModel` so a saga that has never run is folded up from history before
it goes live, and register the saga on that.

Two things do not carry over.

The first is what a push feed is allowed to be. `@Projection(source = PUSH)` accepts a `PushSubscriptionModel` or a
`DomainEventFeed`. A saga cannot take the second. The saga executor recognises a redelivered event by its
`streamid`/`streamversion` or `position` extension, and a domain-event feed carries no such metadata, so a saga bound to
one would re-fold every redelivery and issue its commands again. Push delivery is at-least-once, so that is not an edge
case.

The second is the assumption that there is history to replay at all. The reason to feed a saga from a broker is usually
that another application writes the events. That application's events are not in this application's event store, so the
catch-up in front of the feed has nothing to replay, and the beans it needs (`PositionOrderedReader`,
`CheckpointStorage`) may not exist in the context at all. Worse than failing, a store that does hold unrelated events
would fold those in.

## Decision

`@Saga` gains `source`, `subscriptionModel` and `subscriptionModelName`, matching `@Projection`, plus a `catchup`
attribute that `@Projection` does not have.

**Only a `PushSubscriptionModel` is accepted.** A resolved bean of any other type is refused at startup, naming the
redelivery metadata as the reason, rather than binding a feed that costs the saga its redelivery protection.

**`catchup = FROM_EVENT_STORE` is the default, `catchup = NONE` opts out.** `NONE` takes live events only and touches no
event store, which is what a saga fed by a foreign broker needs. Under the default, a missing `PositionOrderedReader` or
`CheckpointStorage` fails with a message naming `catchup = NONE`, because the application most likely to hit it is
precisely the one that should be setting it, and a bare `NoSuchBeanDefinitionException` would send it looking for a
missing event store instead.

The `Catchup` enum lives in `framework/annotations`, not in the saga module, because the argument for it is about push
sources and not about sagas. `@Projection(source = PUSH)` has the same gap and no way to say so yet; closing that is
additive from here and is deliberately not part of this decision.

**The four start-position attributes are refused, with one exception.** `startAt`, `startAtGlobalPosition` and
`resumeBehavior` do nothing on a push saga: a catch-up always replays from the beginning, and where a live feed resumes
after a restart is the broker's business. `startupMode` is the exception, and only under the default `catchup`, where
the replay is real work on the startup path for `BACKGROUND` to move off it, following ADR 91's
`pushCatchUpShouldWaitUntilStarted` rather than the `shouldWaitUntilStarted` an event-store saga uses. With
`catchup = NONE` there is no replay, so `startupMode` is refused there too. Setting `catchup` on an `EVENT_STORE` saga is
refused as well: ignoring it would leave the saga reading the whole history it was asked to skip.

**A push saga is withheld by `occurrent.subscription.mode=manual`, through the same registry as a push projection.** A
push feed is a bean the application supplies, so the withholding that mode applies to Occurrent's own
`SubscriptionModel` never reaches it, and a saga issuing commands at boot after being told to wait is the exact failure
manual mode exists to prevent — a saga behind a leader election reacting on every node. `ManualStartProjections` was
therefore renamed to `ManualStartPushSources` and now holds both kinds. One registry rather than one per annotation,
because what lands a registration there is the push feed and not what is on the other end of it, and because an
application bringing its push sources up behind a leader election wants one `startAll()` rather than one per kind. Ids
cannot collide, since a `@Projection` and a `@Saga` already cannot share a subscription id.

**The timer gate needs the handover, not `isRunning`.** A saga's timer poller is gated on its subscription running, so
that a paused or withheld saga does not issue commands. `CatchupThenPushSubscriptionModel.isRunning(id)` is `true` for
the whole replay, matching what an event-store catch-up model reports, so reusing it would let a timeout fire against
state that is only half folded up — the one failure catching up before going live exists to prevent. The model therefore
gained a public `isCatchingUp(String)`, and the gate is `isRunning(id) && !isCatchingUp(id)`. ADR 91 predicted this
signal would be needed and deliberately left it to this decision. It is public rather than internal because a caller
outside this repository driving that model has the same question and no way to answer it.

The saga's observation view is **not** withheld with it. `SagaInstances` reads the state store and needs no
subscription, and an application deciding whether to start a saga is exactly the caller that wants to see the instances
it already has.

## Consequences

`@Saga` and `@Projection` now diverge on two attributes, in both directions: `@Saga` has `catchup` and `@Projection`
does not, `@Projection` accepts a `DomainEventFeed` and `@Saga` does not. Both are argued above rather than incidental,
but they are real drift in a pair of annotations otherwise kept in step, and the first of them is a gap to close.

The `ManualStartPushSources` rename is free only because the type is unreleased: it was added in #481 and 0.32.0 has not
shipped. Its reactor twin was renamed with it even though `@Saga` is blocking-only, so the two stacks do not diverge
over a difference that is not theirs.

A withheld push saga's registration runs on whichever thread calls `start`, after refresh, which is why the registrar's
list of subscriptions is a `CopyOnWriteArrayList`: `close()` may read it while that happens.

`catchup = NONE` gives up the one thing a catch-up buys, which is a saga folded up from history before it reacts. A saga
that has run before is unaffected, since its per-instance state lives in the `SagaStateStore` either way. The difference
shows on a first run against an existing history: the saga starts from nothing and reacts only to what arrives from
there on. That is the correct trade when the history is not in the local store, and the wrong one when it is, which is
why the default is to catch up.
