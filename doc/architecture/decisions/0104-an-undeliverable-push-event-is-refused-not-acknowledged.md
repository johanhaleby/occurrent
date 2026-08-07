# 104. An undeliverable push event is refused, not acknowledged

Date: 2026-08-06

## Status

Accepted. Wave A of the post-0.31.0 API review remediation arc (#578), which gates the 0.32.0 tag.

Numbered 104 rather than 103 because `main` carries a duplicated `0102` pair and #591 renumbers the
dispatcher one to 0103, so 103 is spoken for.

## Context

Three code paths on the push stack answered the same question differently, and none of the three answers
was written down anywhere. The question is what happens to an event a push feed cannot deliver right now.

On a push stack that question has teeth, because the listener acknowledges the broker message once
`accept(..)` returns. An `accept` that returns normally without delivering is therefore not a no-op. It is
an acknowledgement of an event nothing consumed, and the broker then discards it. **Returning normally is
the loss.** `AGENTS.md` makes "no design may lose events" a hard rule, so this is the standard the three
paths have to be measured against, not a preference.

The three, each verified on both the blocking and the reactor stack:

1. **Nothing registered.** `DomainEventFeed.accept` returned normally with no projection registered, and the
   feed offered no way to ask whether one was. Under `occurrent.subscription.mode=manual` registration is
   deferred into `ManualStartPushSources`, so ordinary configuration reaches this, not just a bug.
2. **Catch-up failed.** `CatchupThenPushSubscriptionModel` cancelled the registration, so every later
   `accept` routed to nothing and returned normally. The `DomainEventFeed` plus `CatchupProjectionFeed`
   pairing instead recorded the failure and threw from every later `accept`. Same annotation, opposite
   outcomes, decided by which bean type the application injected.
3. **Stop during a replay.** `stop()` cancelled the registration for good. `start()` restarted only the live
   feed, so the subscription never came back, while `isRunning()` answered true and `isRunning(id)` answered
   false.

ADR 90 saw the second of these and named it without settling it, calling the two answers "the shape of a
question that was never really decided". This is that question, decided.

## Decision

**An event a push feed cannot deliver is refused, not acknowledged, except when an operator stopped the
feed, because a stop has a start on the other side.**

That single sentence decides all three, and the exception is what keeps it from swallowing ADR 85.

**Nothing registered: refuse (`DomainEventFeed`).** Nobody chose this state. It is either a misconfiguration
or `mode=manual` before `startAll()`. In the manual case refusing is not merely the safer default, it is the
only way the push stack can honour ADR 86's *registered, not started, means withheld not lost*. The broker is
the only thing holding a backlog, and it holds one only while the listener declines to acknowledge. A
`hasProjection()` accessor comes with it, so a listener that wants to branch can ask instead of catching. That
mirrors `RegisteringSubscribable.hasSubscriptions()`, which the push model already had. `catchUpAll()` refuses
on an empty feed too, matching `catchUp(String)`, which already did. A "successful" catch-up of nothing was
never a useful answer. `stopCatchUp()` stays a no-op, because a shutdown verb that throws when there was
nothing to shut down is a nuisance rather than a safeguard.

**Catch-up failed: refuse, on both flavours.** Nobody chose this either. The fix was a deletion. The shared
handover already recorded the failure and already *was* the action registered on the live feed, so the
subscription model had been cancelling the registration specifically to route around its own engine. Removing
that one line makes the two flavours agree because they run the same engine, rather than because two
implementations were talked into matching.

**Recovery becomes explicit.** The failed subscription keeps its id until `cancelSubscription(id)` releases
it, and then a fresh `subscribe` sets it up again. That is a real cost, because the id is held and a restart no
longer quietly clears it. It buys the thing that matters, which is that nobody discards live events on the
application's behalf as a side effect of a failure the application has not been told about. ADR 90 already
requires the sole registration to be a clearable `AtomicReference` rather than a one-way latch, so the
recovery path needed no new mechanism.

**Model stopped: keep dropping (ADR 85).** A stopped model still drops live events and returns normally. This
is the exception the decision sentence carves out, and it is not a compromise. A stop is an explicit operator
act with a `start()` on the other side, so the window is bounded by something a human chose. Refusing instead
would also break the deny-by-default subscription testing that ADR 82 and ADR 85 exist for, since
`OccurrentSubscriptionsExtension.stoppedByDefault(..)` stops every model and tests then write events through
them. Recorded decisions are not re-opened on a derived argument.

**Stop during a replay: reversible.** A stopped replay keeps its registration on the live feed, and
`start(true)` replays the whole history again. `CatchupProjectionFeed.stopCatchUp()` had already worked this
out and written it down, a stop is not a failure and the feed stays usable, and both handover engines already
cooperate by clearing their stopped flag at the top of `catchUp`. So this ports a recorded decision rather
than deriving a new one. It is also what makes the paragraph above honest, because dropping while stopped is only
acceptable when the window closes.

`start(false)` does **not** restart them. "Do not resume subscriptions automatically" would otherwise be
ignored for exactly the subscriptions whose catch-up is the thing that was stopped, so the interrupted replay
is left for `resumeSubscription(id)` to pick up one at a time. Resuming means replaying from the beginning,
because this model persists no replay cursor and never has.

A **failed** replay is not restartable, unlike a stopped one. Restarting it from `start()` would turn a loud
refusal into a restart loop, and the two states are not the same, since one was chosen and the other was not.

**`PushSubscriptionModel` does not refuse, and that asymmetry is deliberate.** It is fed from the write path
as well as from a broker, since `new InMemoryEventStore(pushModel::accept)` is public API and both the TCK
fixture and the catch-up model's own tests use it. There the event is already durably stored, and a later catch-up
replays it from the store, so refusing would fail the *write* while protecting nothing. This is the same
hazard ADR 85 documented for the synchronous models rather than preventing, and for the same reason.
`DomainEventFeed` has no such second role, since it is domain-typed and therefore cannot be an event-store
listener at all, which is why it can refuse and the model cannot. The model's javadoc now states the caveat and points
at `hasSubscriptions()`, which is the ask-first answer available to a broker listener.

The first draft of this change did refuse on both, on a symmetry argument. The evidence killed it. Symmetry
between two types is worth less than each type matching what it is actually wired to.

**Both handover engines report the refusal the same way.** The blocking engine wrapped its terminal failure
in `HandoverMessages.catchUpFailed(noun)` while the reactor engine propagated the raw cause, so the identical
refusal read as a terminal condition on one stack and as an ordinary handler error on the other. Since the
recovery differs from a retry, that difference mattered, and `ReactiveHandover` now takes the same `noun` its
blocking twin does. `BlockingHandover` also records an `Error`, not only a `RuntimeException`. That gap was
survivable while the model released the registration on failure, and became a live event-loss hole the moment
it stopped.

## Consequences

**`accept` can now throw where it used to succeed, on published API.** All four push types shipped in 0.31.0,
verified against the tag rather than the changelog heading. There is no OpenRewrite recipe, because these are
behaviour changes at unchanged call sites and there is no source edit for a recipe to make. ADR 90 set that
precedent in this same area. The migration guide carries it instead.

**`isRunning(id)` answers `true` after a failed catch-up**, where it used to answer `false`. That is more
coherent, not less, because the subscription exists and is refusing. The `isRunning()` / `isRunning(id)` disagreement
after a stop is gone for the same reason, since the registration no longer vanishes.

**A `Subscription` handle tracks the one replay it was created for.** A replay that `stop()` interrupted
answers `false` from `waitUntilStarted` and keeps answering `false` after `start(true)` launches a fresh one,
because the handle cannot see it. `isRunning(id)`, `isCatchingUp(id)` on the blocking model, and the handle
`resumeSubscription(id)` hands back are the ways to ask about a restarted replay. Threading a live handle
through a restart would mean a mutable subscription handle, which is a worse trade than documenting this.

**Restarting a stopped reactor replay depends on the live sink being untouched.** `ReactiveHandover`
subscribes its unicast `liveSink` only after the marker phase, and a stop errors the pipeline before that, so
an interrupted replay never subscribed it and a second `catchUp` can. A replay that *finished* is never
relaunched, which is the case that would fail. The reactor `CatchupProjectionFeed.catchUp()` already relied on
exactly this, and a test now checks it from the subscription-model side too.

**Under `startupMode = BACKGROUND` the failure signal improves.** Nobody waits, so the failure was previously
only logged and recorded in `PushCatchupStatus`. Now the queue backs up as well, so the health
indicator and the transport agree instead of the transport quietly draining.

**The de-dup cache is still not a redelivery guarantee.** It suppresses the replay-to-live overlap and nothing
more, which matters most for `catchup = NONE`, where there is no replay and therefore no overlap window at
all. The fold has to tolerate broker redelivery regardless. This was checked as part of the same round and is
a javadoc note rather than a change, because the difference between the two feed flavours here is narrower
than it first appeared.

**Unit A3 (#580) owns the exception type.** `IllegalStateException` is used throughout, matching the refusals
the handovers already threw. If A3 lands a sealed refusal family, these move with it.

> **Amended on 2026-08-07 by [ADR 106](0106-a-refused-subscription-call-says-which-condition-it-hit.md).** The sealed
> family landed, and these refusals did not move into it. A failed catch-up, a feed with no projection and a cancelled
> replay stay `IllegalStateException`, because the rule ADR 106 settled on asks whether the caller can fix it by
> passing something else, and none of these can be. They are a failure or another thread's state, not an argument the
> caller got wrong.
