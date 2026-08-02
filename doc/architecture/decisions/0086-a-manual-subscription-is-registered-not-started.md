# 86. A manual subscription is registered, not started

Date: 2026-08-02

## Status

Accepted. This is the second slice of #481, after ADR 85. The OpenRewrite recipe for the renamed property
and the reactive stack follow separately.

## Context

ADR 82 shipped deny-by-default subscription testing, and ADR 85 gave the last four models a life cycle so
that stopping one really does stop everything. What neither could reach is the cost of starting in the
first place. A JUnit extension runs after the context is refreshed, by which time the annotation bean post
processors have registered and started every subscription, so a test still pays for a change stream it
immediately closes. #481 asked for a way to prevent that, and it is a production capability rather than a
test trick: an application bringing subscriptions up behind a leader election wants exactly the same thing.

The obvious implementation is to stop the model once the context is up. It was built that way first, and
it does not hold. A subscription resuming from a stored checkpoint does not go straight to live delivery,
it goes through the catch-up model, which replays out of the event store on a background thread rather
than reading the change stream. So events reached handlers while the model reported the subscription
paused. It only showed up across a restart, which is to say on the second run of any test suite.

Setting the catch-up model's stopped flag before registration is worse than the bug. `shouldKeepReplaying`
then returns false on entry, and the subscription comes back as a `CancelledSubscription` that was never
registered with the delegate. It is lost rather than paused, and nothing can resume it.

## Decision

**Stopping is the wrong verb.** A stopped model has already been handed every subscription, so any layer
that reads history rather than a live feed can still deliver. Withholding has to happen before the model
is told anything at all.

**`ManualStartSubscriptionModel` wraps the model and defers the whole `subscribe` call.** It records the
subscription id, filter, start position and handler, hands back a placeholder, and passes nothing down.
`resumeSubscription(id)` performs the real subscribe. Until then no competing consumer lock is taken, no
history is replayed and no change stream is opened, which is the startup cost #481 was about.

**The reactive stack already worked this way, by accident of layer order.** `ReactorMongoSubscriptionModel`
and `ReactorDurableSubscriptionModel` both decline to subscribe while stopped, and the reactive starter
happens to put the durable model outermost, above catch-up. The blocking starter stacks the same layers in
the opposite order and its durable model is a pure delegate. The wrapper gives the blocking stack the
property the reactive one gets for free, without reordering anything.

**It lives in `occurrent-subscription-api-blocking` and knows nothing about Spring.** That module already
holds concrete wrappers with no dependencies of their own, and this one needs none either, so it costs no
new Maven coordinate. The Boot property is wiring, which is how the rest of Occurrent is built.

**A first run starts from where it was registered, not from where it was started.** This is the part worth
reading twice. `DurableSubscriptionModel` pins a subscription's resume position the first time its start
position is resolved, and `SpringMongoSubscriptionModel` forces that resolution inside `subscribe`. Under
`auto` that happens at boot. Deferring `subscribe` would move it to resume, so a subscription running for
the very first time would silently skip everything written while it waited. So the wrapper captures the
position when the subscription is registered and writes it when the subscription starts, if nothing is
stored yet. Waiting withholds events rather than losing them, which is the only promise worth making.

A subscription that has run before is unaffected either way: it resumes from its own checkpoint. And the
capture is optional, because it needs a checkpoint storage and something able to report a position. Given
neither, the wrapper still works and a first run starts from the moment it is started.

**`isPaused(id)` is true for a subscription that is registered and not started.** It is the question a
caller is really asking, and `OccurrentSubscriptionsExtension.startAll()` filters on it. For the same
reason the wrapper reports its own ids from `subscriptionIds()`, merged with the delegate's, since
`IntrospectableSubscriptionModel.of` stops at the first model that implements it.

**`isRunning()` is false while withholding, even though the listener container and the lease refresh thread
below it are running.** It answers whether this model is delivering, not whether its resources exist.

**Registering the same id twice fails here.** Nothing else catches two `@Subscription`s sharing an id
except the innermost Mongo model, which under this mode is never reached. Without the check a duplicate
would quietly replace the first handler and the application would boot green with a subscription missing.

**Starting a subscription resumes it if the delegate parked it.** Subscribing to a stopped model registers
a paused subscription rather than a running one, so a plain subscribe would hand back something that never
delivers. This is what makes stop-then-start work, which is exactly what the JUnit extension does between
tests.

**The three paths that never went through the subscription model are closed too.** A saga's timer poller
could dispatch commands and write events on its first tick regardless of whether the saga's subscription
was running, and two projection paths replayed history during registration. They were already doing this
before this change, so `manual` merely made them visible.

## Consequences

Boot no longer validates subscription wiring. A bad filter or an unsupported start position used to fail
during context refresh and now fails at the first resume, which for a leader election means in production,
some time after deploy. The duplicate id check and the eager argument checks recover the cheapest of those.
The rest cannot be recovered without subscribing, which is the thing being avoided.

Nothing below the wrapper knows a withheld subscription exists. No lease is held for it, so an operator
looking at who is consuming what sees nothing until the subscription is started. In a mixed deployment
that is an improvement, since a manual node no longer takes a lock it then refuses to act on.

State has two authorities, split by id: the wrapper answers for a withheld subscription and the delegate
for a started one. That is safe only because a started subscription is pure pass-through. The delegate
changes state on its own when a lease is lost or change stream history is lost, so caching an answer here
would go stale.

The bean's runtime type differs by mode, so code reaching for the concrete
`CompetingConsumerSubscriptionModel` works under `auto` and fails under `manual`. The starter's bean is
also `@ConditionalOnMissingBean`, so an application defining its own subscription model does not get this
and `manual` silently does not apply to it.

This does not pay the underlying debt. `CatchupSubscriptionModel` still cannot pause, resume or list a
subscription whose replay is in flight, and its dispatcher still does not fan those calls out to the
children holding the state. `manual` routes around that rather than fixing it, and the gap is live again
for exactly the subscriptions the application has just started. The subscription conformance suite in #395
should pin it.

`manual` is blocking only until the reactive starter gets the same wiring.
