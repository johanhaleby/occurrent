# 101. A durable reactor subscription delegates when the model it wraps is named

Date: 2026-08-05

## Status

Accepted. Partially answers #547, which stays open for the question this ADR deliberately leaves unanswered.

Amends [ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md) in one place, noted under
Decision. Does not amend [ADR 94](0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md).

## Context

`ReactorDurableSubscriptionModel` drove the wrapped model's cold `FluxSubscriptionModel` primitive through a
`concatMap` of its own and called that a named subscription. The blocking `DurableSubscriptionModel` instead hands the
subscription to the model it wraps and adds only the checkpoint. The reactor version therefore inherited nothing, and
phase 7 of the subscription TCK measured exactly what that costs, on the `Durable(Catchup(Mongo))` composition:

- `refuses_a_subscription_filter_it_does_not_understand` failed, because an unsupported `SubscriptionFilter` was
  reported when the change stream started rather than when `subscribe(..)` was called.
- `is_retried_or_propagates_as_the_fixture_declares` failed, because a failing action neither reached the publisher nor
  was retried. It ended the subscription silently.
- `an_action_whose_mono_errors_fails_through_the_model_and_leaves_it_running` failed for the same root cause.

#547 recorded two ways out, its own retry configuration or delegation, and the maintainer chose delegation.

Implementing it surfaced a constraint the issue did not have. Delegation needs the wrapped model to offer a named
`subscribe(..)`, and in the composition the reactive starter wires for a store that writes `position`, the wrapped model
is `ReactorCatchupSubscriptionModel`, which offers only the cold primitive and has no lifecycle at all. The only
checkpoint-aware reactor model with a named `subscribe(..)` is `ReactorMongoSubscriptionModel`, one layer below it. So
`ReactorDurableSubscriptionModel` was the only thing in the reactor stack turning a cold `Flux` into a named,
lifecycle-managed subscription, and telling it to stop doing that requires something below it to start.

That is a larger change than #547 describes. It moves three released catch-up models onto the combining interface, needs
a named counterpart of `PositionCatchupPipeline`, which is written entirely around the cold shape, and has to preserve
the durable model's capture of the feed position for a subscription registered while the model is stopped, which the
blocking twin has no equivalent of.

## Decision

**The durable model delegates when the model it wraps is a reactor `SubscriptionModel`, and keeps driving the cold
primitive when it is not.** Which path a given instance takes is fixed when it is constructed, by whether the wrapped
model carries the combining interface.

On the delegating path the model does what the blocking twin does. It resolves the start position, hands the
subscription to the wrapped model with the checkpoint save wrapped around the caller's action, and forwards its whole
life cycle. Everything the wrapped model already does for a named subscription therefore applies, which is what makes
an unsupported filter refused in `subscribe(..)` and a failing action retried with the wrapped model's configured
backoff. **The retry surface is the wrapped model's own configuration and no new one is introduced**, which is the
answer #547's follow-up comment asked for.

**This covers every shipped composition in which the wrapped model is named**, which is more than it sounds: the
reactive starter's own branch for a store that writes no `position` (`OccurrentReactiveMongoAutoConfiguration`), the
`mongodb-subscription-to-spring-event` forwarder example, and `ReactorDurableSubscriptionModelTest`.

**The start position is resolved through a dynamic `StartAt` rather than before the call.** The wrapped model's
`subscribe(..)` has to run inside this model's `subscribe(..)`, or its filter validation could not reach the caller,
and the reactive checkpoint read cannot be awaited there without blocking the calling thread first. A dynamic `StartAt`
moves the read to the moment the wrapped model subscribes, which is the same mechanism the already-shipped
`ResumeStartPositions.replayThenResume` uses and documents.

**That resolution is remembered after the first attempt, unlike the blocking twin, which re-reads storage every time.**
The wrapped model re-resolves the position when it restarts a change stream, and that happens on a scheduler thread
where awaiting a reactive read is refused outright. Reusing the first answer is safe here because the only writer of a
subscription's checkpoint is that subscription, so a restart before the first delivery finds storage exactly as the
first attempt left it.

**A subscription registered while the wrapped model is stopped still reads the feed position at registration.** Without
it, starting that subscription later would begin wherever the feed had reached by then and skip everything written
while it waited, which the no-loss rule does not allow. The cold path already did this and the wrapped model does not,
so the delegating path keeps doing it.

**This amends ADR 98 in one respect and leaves the rest standing.** ADR 98 put the combining interface on the four
models that carry a subscription id and recorded that the three reactor catch-up models expose only a cold `Flux`.
That placement is unchanged here, and no catch-up model is touched. What changes is the consequence ADR 98 did not
draw, that the placement decides whether the durable model can delegate at all. **ADR 94 is not amended.** Its
objection is to inventing a lifecycle a type does not have, and nothing here invents one.

**The `Durable(Catchup(Mongo))` composition therefore keeps the old behaviour and the old defects, and that is
deliberate rather than overlooked.** Whether the catch-up models should be promoted so that composition can delegate
too is a released-API question of its own, filed as #550, and it is the maintainer's to answer. Until it is answered
the three conformance wirings over that composition stay out of the tree, where phase 7 left them.

## Consequences

The two paths are the cost. One model now answers the TCK differently depending on what it wraps, so a reader has to
know which composition they have before they can say whether a failing action is retried. The `Durable(Mongo)`
conformance wirings added here and the `Durable(Catchup(Mongo))` wirings still held back are the two halves of that,
and the second half is the reason the split is a stage rather than an end state.

A fixture declaration now differs between the two compositions for the same model class. `retriesAFailingHandler()` is
true for `Durable(Mongo)` and false for `Durable(Catchup(Mongo))`. That is a real difference in behaviour rather than a
knob holding a bug, and the suite asserts both directions, but it is only tolerable while the promotion question is
open.

Delegating the life cycle means the wrapped model's answers become this model's answers, including for `isRunning`,
`isPaused` and `subscriptionIds`. A wrapped model that is named but not introspectable cannot be asked for its
subscription ids, so the ids handed to it through this model are used instead.
