# 109. A saga refuses an event it cannot recognise a redelivery of

Date: 2026-08-07

## Status

Accepted. Resolves #583, from the post-0.31.0 API review. Amends
[ADR 96](0096-a-push-fed-saga-may-have-no-history-to-replay.md), which decided which feeds a `@Saga` accepts but left
what happens per event unchanged.

## Context

A saga tells a redelivered event from a new one by its `streamid` together with its `streamversion`, or by its
`position`. `SagaExecutionSupport.isRedelivery` compares those against a per-instance watermark. An event carrying none
of them leaves nothing to compare, so the check answers "not a redelivery" for every delivery of the same event, and
the reaction runs again and issues its commands again. A broker hands the same message over more than once as a matter
of course, so that is the normal case rather than an edge case.

ADR 96 closed the half of this that is decidable at startup. A `DomainEventFeed` carries no such metadata at all, so a
saga refuses to be bound to one. What it left open is the half that is only decidable per event. A
`PushSubscriptionModel` is accepted, but what arrives on it is whatever the application's listener forwarded, and a
listener that builds a CloudEvent from a broker message without carrying the Occurrent extensions across produces
exactly the keyless event above.

The behaviour until now was a warning, logged once per runner, saying that the reaction will run again and issue its
commands again. The warning is accurate and it does not help. It scrolls past during startup, it is emitted once for
the life of the process rather than once per duplicate, and nothing downstream of it changes. Meanwhile the same
release fails an application context outright when a push sink is shared by two consumers, on the reasoning that the
dangerous configuration is the one that has to announce itself ([ADR 90](0090-a-push-sink-feeds-one-consumer.md)), and
refuses an undeliverable push event rather than acknowledging it ([ADR 104](0104-an-undeliverable-push-event-is-refused-not-acknowledged.md)).
Duplicate commands against a domain model is a worse outcome than either, and it is the one that only warns.

The obvious answer, failing on the first keyless event with nothing to configure, does not survive contact with the
reason `catchup = NONE` exists. That mode is for a feed carrying another application's events. Those events are the
least likely of any to carry Occurrent's extensions, because the application writing them is not running Occurrent.
Refusing them unconditionally would not make that configuration loud, it would make it unusable, because the way ADR 96
blessed for running a saga off another application's broker would stop working with no way to say "yes, I know". A
feed with no such metadata on it, chosen deliberately, has to
stay expressible. It just must not stay silent.

## Decision

**An event carrying no redelivery key is refused, and refusing is the default.** `SagaExecution` throws
`SagaRedeliveryDetectionException` before the reaction runs. The throw reaches the subscription model, so under ADR 104
the saga does not acknowledge the event, and whether it is offered again is that model's own business. A
`PushSubscriptionModel` hands the acknowledge-or-redeliver decision to the listener that called `accept`, and on a
consume-side broker bridge `DeliveryFailurePolicy` is where the choice is configured. Where the event is offered again
the saga refuses it again and the application stays stuck on it. Where it is not, the duplicate commands are still not
issued, and that is the part this decision owns. A saga whose feed drops the metadata is broken in a way that costs
correctness, and the failure is visible from the first event rather than from a warning nobody read.

**The opt-out is one attribute, `redeliveryDetection = REQUIRED | BEST_EFFORT`.** `BEST_EFFORT` restores the previous
behaviour, warning once and taking the event. It exists for the other application's broker above, and its javadoc says
what it costs, which is that every redelivery runs every reaction again, so each command the saga issues has to be safe
to receive more than once. Naming it for the
property of the feed rather than for a log level follows `Catchup.NONE` and `Consumers.MANY`, and keeps the reader's
attention on what is being given up rather than on how loudly it is reported.

**Two enum types, not one.** The declarative half lives in `framework/annotations` next to `Catchup`, the runtime half
in `org.occurrent.dsl.saga.blocking` next to `SagaRunnerConfig`, and `SagaAnnotationRegistrar` translates between them.
The annotations module is a leaf that nothing depends on but the framework, and the saga DSL is deliberately usable
without it, so sharing one type would mean a dependency edge in one direction or the other purely to spare two
constants. `Catchup`, `StartupMode` and `ResumeBehavior` are already declarative-only types the registrar translates;
this one differs only in that the runtime needs a value of its own to carry.

**`SagaRunnerConfig` carries it, so the programmatic path has it too.** `SagaRunner` is public API and takes any
`Subscribable`, so a hand-wired saga over a push model has always had this exposure. It gets the same default and the
same opt-out, through `withRedeliveryDetection(..)`. The record gained a component, so the three-argument constructor
stays as a delegating overload rather than being replaced.

**Setting the attribute on a `source = EVENT_STORE` saga is refused.** Those events always carry a `streamid` with a
`streamversion`, so `BEST_EFFORT` there would relax a check that never fires while reading as protection deliberately
given up. This follows the same reasoning that refuses `catchup` on an event-store saga.

## Consequences

This is a behaviour change for code that shipped. `SagaRunner.agnostic(pushModel, ..)` has been public since before
0.31.0, and a saga wired that way over a feed with no extensions used to duplicate commands silently and now fails.
That is the intended outcome, and it is why the change carries a breaking-changes entry and a section in the 0.32.0
upgrade guide rather than being written off as a refinement of the unreleased `@Saga(source = PUSH)` work. An
application relying on the old behaviour sets `BEST_EFFORT` and keeps it, having said so once.

The refusal happens per event, and only after the saga has decided the event belongs to one of its instances. An event
it does not correlate to any instance is ignored as before, so a feed with no metadata on it only announces itself once
it delivers something the saga would have reacted to. That is deliberate. A saga sharing a broker topic with unrelated
traffic should not fail on the traffic it was always going to skip.

The saga acknowledges no refused event, and whether the event comes back is the subscription model's own business.
Where it does come back, on a queue with no dead-letter policy, meaning no separate destination for messages that keep
failing, it blocks the events behind it. That is the same trade ADR 104 made, for the same reason, that an event a saga
cannot handle correctly is better stuck and visible than consumed and wrong. A consume-side broker bridge configured
with `DeliveryFailurePolicy.PARK` takes the other side of it, republishing the refused event to the parking destination
and acknowledging it out of the source queue once that republish is confirmed, so there the event is normally gone from
the source on the first refusal rather than stuck in front of the events behind it. Where the trade is not wanted at
all, `BEST_EFFORT` is the answer, and it is the answer precisely because it makes the choice visible in the code rather
than in a log file.

Nothing changes for the catch-up leg of a push saga or for an event-store saga. Both read from the event store, whose
events always carry the metadata, so neither can reach the refusal. The check costs one already-computed null test per
delivered event.

## Amendment (2026-09-18): a refused event does not come back on its own

Two sentences in this record said a refused event is offered again. They were wrong for different reasons, so this
amendment keeps them apart rather than telling one story about both.

The Decision said "A push feed offers it again, the saga refuses it again, and the application stays stuck on it".
That was wrong on the day it was written. `PushSubscriptionModel`'s javadoc said, in the same commit `8a9311fd6` of
2026-08-10, that a handler exception propagates to the caller so the listener can decide whether to acknowledge or
redeliver, and the javadoc is the one the code implements. The model behaved that way at `occurrent-0.33.0` too, so
no later work falsified this sentence. It was never true.

The Consequence said "A refused event is not acknowledged, so a broker will keep offering it", hedged only by a queue
with a dead-letter policy. That one was true when it was written, because no broker bridge existed on 2026-08-10.
0.34.0 is what makes it false. A bridge configured with `DeliveryFailurePolicy.PARK` republishes the refused event to
the parking destination and acknowledges it out of the source queue once that republish is confirmed, so the event is
normally gone from the source on the first refusal. A park publish that fails redelivers the original instead, which
is one way `PARK` can still end in a redelivery. The dead-letter hedge does not cover parking, because dead-lettering
is the broker's own policy and parking is the bridge's.

The second one costs the argument for `REQUIRED` being the default. That argument is that an event the saga cannot
handle correctly is better stuck and visible than consumed and wrong, and on a parking bridge it is neither stuck nor
visible where a reader of this record would look for it. Someone choosing `REQUIRED` there is promised a trade they do
not get. Both sentences now say what the saga does and leave what happens to the event afterwards to the
subscription model. See [#1079](https://github.com/johanhaleby/occurrent/issues/1079).
