# 89. Manual subscription mode on the reactive stack

Date: 2026-08-02

## Status

Accepted. This is the last slice of #481, after ADR 86. Only the OpenRewrite recipe for the renamed
property is still outstanding.

## Context

ADR 86 shipped `occurrent.subscription.mode` for the blocking stack and said the reactive stack "already
worked this way, by accident of layer order". That was half right, and the half that was wrong shipped as
a bug.

The reactive durable model does decline to subscribe while stopped, so it withholds delivery. What it did
not do was pin where the subscription starts from, because the branch that parks a subscription returned
before ever resolving a start position. A subscription registered while the model was stopped therefore
began wherever the feed had reached by the time it was started, silently skipping everything written in
between. That was fixed on its own, ahead of this, so the guarantee ADR 86 records is true on both stacks
before anything depends on it.

The other half of the sentence was simply not implemented. Under `manual` the reactive starter stopped the
synchronous model and did nothing about the asynchronous one, so synchronous projections stopped running
while asynchronous subscriptions kept going. Half-applying is worse than not applying, because an
application reading the property's own documentation was told none of its subscriptions would run.

## Decision

**No wrapper, unlike the blocking stack.** The reactive chain is `ReactorDurableSubscriptionModel` over
`ReactorCatchupSubscriptionModel` over `ReactorMongoSubscriptionModel`, with the durable model outermost
and the only `Subscribable` the annotation registrars resolve. There is no competing consumer model on
this stack at all. So `manual` is a matter of handing back a model that is already stopped, and the bean
method calls `stop()` on it. Nothing starts it again, because no reactor model implements Spring's
`Lifecycle`.

That is the whole reason this slice is smaller than the blocking one. The blocking stack needed
`ManualStartSubscriptionModel` because its layer order puts catch-up above the model that knows how to
withhold, and catch-up replays out of the event store rather than reading a change stream. The reactive
order already puts the withholding model on the outside.

**Stopping the model and skipping the startup wait are one change, not two.** The reactor registrars call
`waitUntilStarted().block()` in eight places with no timeout, and a paused reactive subscription's
`waitUntilStarted()` returns a `Mono` that never completes. Stop the model without also skipping those
waits and context startup never finishes. Worse, it never fails either: the wait happens while Spring
builds the context, so a JUnit `@Timeout` on the test method does not cover it and the build hangs. Seven
of the eight are the same `shouldWaitUntilStarted(...)` pairing the blocking registrars already use. The
eighth, a push projection's catch-up, blocks unconditionally and is handled below.

**A push projection is withheld by a registry, not by the model.** A `@Projection(source = PUSH)` is fed
by a `PushSubscriptionModel` or `DomainEventFeed` bean the application supplies, so stopping Occurrent's
own model never reaches it. A reactor `ManualStartPushSources` records the startup work instead of running
it, and the application calls `start(id)` or `startAll()` when it is ready. (Named `ManualStartProjections`
when this was written; renamed in [ADR 96](0096-a-push-fed-saga-may-have-no-history-to-replay.md) when a
push-fed saga began using the same registry.) Same shape as the blocking
twin, except the work runs when the returned `Mono` is subscribed rather than when the method is called,
and the id is claimed at that same moment so a `Mono` that is built and never subscribed leaves the
projection withheld rather than dropping it.

**For a domain feed the registration is withheld too, not only the catch-up.** Registering alone puts the
feed into buffering mode, so deferring only the catch-up would let a live event fill a bounded buffer
rather than be folded, and eventually overflow it. Deferring both together also needs a per-id catch-up,
since `catchUpAll()` would re-run the catch-up of projections already live on the same feed, so the
reactive `DomainEventFeed` gains the `catchUp(String)` the blocking one got in #497.

**There is no reactive `startAll()` on the model, and the testing module stays blocking only.** There is
no reactor `IntrospectableSubscriptionModel`, so nothing can enumerate a reactive model's subscription
ids, and `occurrent-testing-junit-jupiter` depends only on the blocking subscription API. A reactive
application under `manual` resumes subscriptions by id. Worth stating rather than leaving someone to
discover it.

> **Amended for 0.32.0, under #395.** The reactor `IntrospectableSubscriptionModel` now exists, so the
> premise above no longer holds and the reactive twin of the testing module (#530) is unblocked. Two
> decisions in it are worth recording here rather than rediscovering.
>
> `subscriptionIds()` returns a plain `Set<String>` rather than a `Mono`, because it is answered from a
> registry the model already holds. Every method on the reactive `SubscriptionModelLifeCycle` returns a
> plain value for the same reason.
>
> It has no `of(..)` unwrapping helper, unlike the blocking twin, because that one walks a
> `DelegatingSubscriptionModel` chain and the reactive stack has no such interface: the chain is composed
> through constructors, as this ADR already describes. A caller reaches the interface with `instanceof` on
> the subscription model itself. That is also why the reactive twin can offer `startAll()` while the model
> has none: enumerating ids and starting them is the extension's job, not the model's.
>
> **What that does not reach, on either stack.** The subscription DSL wrappers (`Subscriptions`,
> `StreamSubscriptions`, `DcbSubscriptions`) hold their model in a private field and expose no accessor, and
> `DcbSubscriptionModelAdapter` forwards lifecycle calls without forwarding introspection. So a caller who
> injects only a DSL wrapper cannot reach the ids through it, and `of(..)` would not have helped: the
> blocking adapter is not a `DelegatingSubscriptionModel` either, so the blocking helper cannot unwrap it
> today. Inject the subscription model bean when you need the ids. Making the wrappers forward is a separate
> change on both stacks, and nothing needs it yet.

**Sagas need nothing.** `@Saga` is blocking only, so the timer gating ADR 86 added has no reactive twin to
keep in step.

## Consequences

`SubscriptionMode.MANUAL` now means what it always said it meant. No documentation changed to make that
true, the code did.

Everything ADR 86 lists under consequences applies here too, for the same reasons: boot no longer
validates subscription wiring, a bad filter fails at the first resume instead of during context refresh,
and an application that defines its own subscription model bean does not get this at all, since the
starter's bean is `@ConditionalOnMissingBean`.

One difference is worth naming. The blocking wrapper reports withheld ids from `subscriptionIds()` and
fails a duplicate id at registration, because nothing below it would catch either. The reactive stack has
neither, since it has no wrapper: a withheld reactive subscription is a paused subscription in the durable
model, which already knows about it, and duplicate ids are caught where they always were.

A missed wait is the failure mode to watch for if a registrar grows a new subscription path. It hangs the
build rather than failing a test, which is a bad way to find out. The reactive mode tests exist mostly to
find it, and they only find it by hanging.
