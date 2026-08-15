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

**Amended for 0.33.0: `IntrospectableSubscriptionModel` is renamed to `IntrospectableSubscriptions`, and
`DelegatingSubscriptionModel` to `SubscriptionModelWrapper`.** Neither capability changed. Both names moved
because neither interface ever extended `SubscriptionModel`.

> **Amended again, 2026-08-14, before 0.33.0 shipped, for #738.** This record says the reactive stack needs no
> wrapper because its layer order already puts the withholding model outermost, and that is still true. What it did
> not say is where the reactive stack therefore records a subscription's first start position, which is
> `ReactorDurableSubscriptionModel.resolveStartAt`. That method read storage, found nothing, read a position and
> wrote it with no condition attached, so two nodes registering a subscription for the very first time at the same
> moment both found nothing, both wrote, and the second write won without anybody being told. The events between the
> two positions then reached neither. It is the defect ADR 86's fourth amendment describes on the blocking stack, and
> #771 says whichever direction wins is decided once and applied to both, so the answer here is the same answer.
>
> **The write is conditional now, with `ifAbsent()`, and a registration that loses it is refused with
> `StartPositionAlreadyPinnedException`.** Everything ADR 86's fourth amendment records about that refusal holds
> here too, including the one exception inside it. The stored position is read back, and a registration whose own
> position is what comes back completes rather than failing. A read back that differs, that fails, or that finds
> nothing is refused, the last two for the weaker reason that nothing here can show the two agree. The exception
> lives in `subscription/core`, which both stacks already depend on, so no reactive twin of it exists.
>
> **Two differences from the blocking stack are worth stating rather than leaving to be found.**
>
> The first is scope. On the blocking stack this lives in `ManualStartSubscriptionModel`, so only
> `occurrent.subscription.mode=manual` reaches it, and `DurableSubscriptionModel` still records a first position
> with `any()`. The reactive stack has no such wrapper, by this record's own decision, so the conditional write and
> the refusal sit in the only durable model there is. Any reactive durable subscription whose start position resolves
> to the subscription model default can therefore be refused on its first run, not only one registered under
> `manual`. A registration naming a position of its own still records nothing and is never refused, which is the same
> exclusion the blocking model makes. That is a wider change than the blocking one and it was taken
> deliberately, because the alternative is a model that records a first position two ways depending on how it was
> configured, and because the rule in `AGENTS.md` says a loss window is a loss whatever its width.
>
> The second is where the refusal comes out. The blocking model throws from `subscribe(..)` on every path. The
> reactive model throws from `subscribe(..)` only when the wrapped model manages named subscriptions of its own,
> which is the path that already waits for the position inside that call. When this model drives the cold primitive
> itself it cannot throw there, because resolving the position waits on storage and that call holds the model's
> monitor, which this model refuses to block for the reason its own comment gives. So the refusal is signalled on
> `Subscription.waitUntilStarted()` and logged at `ERROR`, which is where that path already reports a start it could
> not make. A caller that never asks whether the subscription started sees only the log line. A refused subscription
> is also dropped from the model rather than left registered, so starting again means registering again, not
> resuming.
>
> **A position that could not be read at registration joins the refusal.** A subscription registered while the model
> is stopped reads where the feed is at that moment, and that read used to be allowed to fail quietly and be taken
> again when the subscription started. A read taken then answers with wherever the feed has reached by then, so the
> subscription started past everything written while it waited, which is the same loss this amendment refuses
> everywhere else. It refuses now, and a read that answers nothing refuses with it, since answering nothing is how
> `CheckpointAwareSubscriptionModel.globalCheckpoint()` reports a problem it cannot resolve rather than a position,
> and an Atlas cluster that prohibits `hostInfo` reaches it. There is no original failure to carry for that one, so
> it is an `IllegalStateException` naming the subscription and the way past it, an explicit `StartAt`, which records
> no position and promises nothing about where the subscription starts. A read that failed carries its own failure
> unwrapped. Both surface where this amendment already says a refusal surfaces, and a refused subscription is dropped
> the same way.
>
> **A storage that cannot evaluate the condition keeps the write it had.** `CheckpointStorage.evaluatesWriteConditions()`
> defaults to `false`, and this model is the only durable model the reactive stack has, so requiring the capability
> would fail every application whose own storage never overrode that method, at the first write and with no startup
> check to catch it first. The model asks `evaluatesWriteConditionsFor(String)` instead, and a storage answering
> `false` gets the unconditional write 0.32.0 made, with a `WARN` naming the storage class and what closes the
> window. Both storages Occurrent ships on this stack answer `true`, so this only reaches a storage written
> elsewhere. It is a window Occurrent cannot close from here, since the storage is the thing that would have to
> evaluate the condition, and the way out is for it to.
>
> **Two bounds this does not cover.** A subscription registered while the model is stopped reads its position at
> registration and reads storage when it is started, so a checkpoint written between those two is accepted in
> silence even though it may hold a later position. That is the same window #771 owns, between one node reading a
> position and its own write reaching storage, widened here by however long the subscription waits to be started,
> and closing it needs what #771 needs. And the
> refusal cannot tell a second node from a storage answering from a reader that has not seen the write, which is
> why the exception is named for the position rather than for whoever wrote it.
