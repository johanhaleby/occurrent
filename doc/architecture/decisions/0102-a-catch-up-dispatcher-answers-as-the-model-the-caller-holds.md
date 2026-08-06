# 102. A catch-up dispatcher answers as the model the caller holds

Date: 2026-08-06

## Status

Accepted. Resolves #557.

Builds on [ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md) and
[ADR 101](0101-a-durable-reactor-subscription-delegates-when-the-model-it-wraps-is-named.md), which promoted the three
reactor catch-up models to full subscription models. Amends neither.

## Context

`ReactorCatchupSubscriptionModel` is a dispatcher. It holds up to three inner catch-up models, a stream one, a DCB one
and a capability-agnostic one, routes each subscription to whichever fits the filter and start position, and hands
every one of them the same wrapped model. It is also the only one of the four types a caller normally holds:
`ReactorDcbCatchupSubscriptionModel` is package-private, the agnostic model has no type of its own, and the
dispatcher's constructors are the only public way to get the combination.

Reviewing #556 surfaced two places where it stops behaving like the model the caller holds. Both are older than #556
and were left out of it deliberately to keep that change reviewable.

**It told a caller's `StartAt.dynamic` the wrong type.** Each inner model resolved the caller's `StartAt` against its
own class, so a start position written as `context.hasSubscriptionModelType(ReactorCatchupSubscriptionModel.class)`
never matched once the subscription went through the dispatcher, on the cold path and the named path alike. The
blocking `CatchupSubscriptionModel` has never had this problem, because it passes its own class to each inner model at
construction and `AbstractCatchupSubscriptionModel` carries a comment saying exactly why. The rule was real on one
stack and unwritten on both, which is how the other stack came to miss it.

**It chose an inner model to answer for a subscription id it did not create.** `ownerOf` looked the id up in the
record it writes when it routes a named subscription, and otherwise returned "any inner model". Reading that at the
call site, it looks like a guess, and it was reported as one. It is not, but only because of an invariant that is
nowhere near the code that depends on it: the inner models are private to the dispatcher, so nothing outside can
subscribe on one, and the dispatcher records the owner before it routes, so an id reaching the fallback is always an
id no inner model is replaying. The reported failure, a life cycle call landing on an inner model while a different
one still replays that id, is therefore not reachable through the dispatcher's public API. A second catch-up model
built by hand over the same wrapped model could replay an id this dispatcher cannot see, but interrogating its own
inner models would not find that one either.

The remaining question was whether the wrapper should refuse instead, which is the shape ADR 101 chose when a catch-up
model cannot manage named subscriptions at all.

## Decision

**A subscription routed through the dispatcher reports the dispatcher's own class to a caller's `StartAt.dynamic`,
on both the cold and the named path.** The type a caller matches on is the type it holds. Which mode-specific model
runs the catch-up is an internal routing decision, and a start position that changes meaning depending on it would be
unusable.

**The type is injected at construction rather than hardcoded, exactly as on the blocking side.** Each inner model
takes a `subscriptionModelContextType`, defaulting to its own class through the public constructors and set to
`ReactorCatchupSubscriptionModel.class` by the dispatcher's package-private ones. A stream catch-up model wired
directly, which its javadoc offers as the DCB-free variant, therefore still reports itself. The same class is what the
inner model names when it refuses a named subscription over a cold-only wrapped model, so that message also names the
type the caller holds.

**The dispatcher does not refuse a subscription id it did not create.** It can answer honestly, so ADR 101's precedent
does not apply: what that ADR refuses is a capability the composition does not have, a named subscription over a model
that only offers the cold primitive, not a question it could pass on. Refusing here would break the calls that are
explicitly allowed to name an unknown id, since cancelling one is an idempotent no-op and `isRunning` on one answers
false, and those are what a Spring context close and a health check reach.

**A per-subscription life cycle call is routed in three steps, and none of them is a guess.** The dispatcher's own
record answers for every id it created. An id it did not create may still be replaying on an inner model, which is the
one state the wrapped model cannot answer for, so each inner model is asked whether it holds that replay. What is left
is an id no replay owns, and since every inner model forwards to the same wrapped model, going through any one of them
returns that model's own answer. The third step keeps its meaning without the reader having to reconstruct the
invariant above, and the second step keeps it true if the invariant ever stops holding.

**The forward goes through exactly one inner model, which is where this dispatcher has to differ from the blocking
one.** The blocking `CatchupSubscriptionModel` answers `isRunning(String)` and `isPaused(String)` by asking every
inner model and OR-ing the results, which is safe there because each of those already ORs in the shared delegate. The
reactor inner models throw when asked to pause an already paused subscription, so the same fan-out would ask the
wrapped model to pause the same id three times and fail on the second.

**Only the reported type is a behavior change; the routing change is defence in depth.** The interrogation step fixes
no reachable defect today, and no test can be written against the public API that fails without it. It is in because
the alternative leaves the fallback's correctness resting on an invariant a reviewer already misread once.

## Consequences

**A start position that branches on the subscription model type now behaves differently on a released path.** The
dispatcher's cold `subscribe(SubscriptionFilter, StartAt)` shipped before this release, so a caller who worked around
the old behavior by matching on `ReactorStreamCatchupSubscriptionModel` or by not matching at all sees a change. The
direction is the one the blocking stack has always had, and the workaround was a match on a type the reactive starter
never hands out, so the exposure is small. It is in the changelog rather than the migration guide for that reason.

**An inner model built by the dispatcher answers with the dispatcher's name in more places than the start position.**
Its refusal message for a cold-only wrapped model names `ReactorCatchupSubscriptionModel` too. That is intended: the
message tells a caller which of its own objects to change, and the caller has no reference to the inner model.

**The blocking dispatcher has the mirror image of the ownership gap, and it is not fixed here.** Its
`pauseSubscription` and `resumeSubscription` go straight to the shared delegate, so a pause issued while a replay is
still in flight never reaches the inner model's `pauseRequestedDuringCatchup`, while `isPaused` does consult it. The
read side and the write side therefore disagree, and only when a `StreamCatchupSubscriptionModel` is used standalone
does the pending pause work at all. It is a real defect rather than a deliberate difference, but it is on the blocking
stack and belongs to its own change.

**Two constructors now exist per inner model where one did before.** The extra one is package-private and exists only
so the dispatcher can inject its own type, which is the same shape and the same cost the blocking side already pays.
