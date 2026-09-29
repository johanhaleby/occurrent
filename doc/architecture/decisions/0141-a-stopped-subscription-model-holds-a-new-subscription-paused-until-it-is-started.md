# 141. A stopped subscription model holds a new subscription paused until it is started

Date: 2026-09-29

## Status

Accepted. Resolves part of [#1166](https://github.com/johanhaleby/occurrent/issues/1166). Amends
[ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md), which says the blocking
catch-up model abandons a replay that `stop()` cuts short.

## Context

`CompetingConsumerSubscriptionModel` wraps another subscription model, and after `stop()` it has to keep a new
subscription from delivering anything until the node is started again and wins the lease. It can only do that if the
model it wraps behaves the same way for a subscription made while that model is stopped.

`DurableSubscriptionModel` decides where a new subscription starts in `subscribe(..)`, when it has no stored position
for it. `ManualStartSubscriptionModel` stores that position when a subscription is registered for the same reason
(ADR 130). A competing consumer model that passed the subscription on only once the node had won the lease therefore
started it from the position at that time, and skipped every event written between `subscribe(..)` and `start()`.

Passing it on straight away is only safe when the wrapped model holds it paused. Two models did not.
`NativeMongoSubscriptionModel` opened a change stream for it straight away, until
[#1171](https://github.com/johanhaleby/occurrent/pull/1171) changed that. The blocking
`CatchupSubscriptionModel` ended the replay of a subscription made while it was stopped as soon as the replay began,
and did the same to a replay that `stop()` cut short. In both cases the subscription still counted as running a
catch-up, so `isRunning()` returned `true` for good, the replay never went on to the live model, and nothing ran it
again.

Behind a `CompetingConsumerSubscriptionModel` that also broke the next subscription. The competing consumer model
starts the wrapped model before it hands over a subscription whose lease the node won, but only when the wrapped model
says it is not running. After such a stop it never started it again, so a subscription whose lease the node won later
went to a stopped live model and received nothing, while the node kept refreshing the lease. Every `start(true)` also
threw `SubscriptionAlreadyRunningException` for the subscription whose replay was cut short.

## Decision

**Every subscription model that `CompetingConsumerSubscriptionModel` wraps holds a subscription made while it is
stopped paused.** It registers the subscription, decides its start position as it would while running, delivers
nothing, returns `false` from `isRunning(id)` and `true` from `isPaused(id)`, and runs it once when it is started.
Its own `isRunning()` says whether it runs.

**`CompetingConsumerSubscriptionModel` hands a subscription made while it is stopped to the wrapped model in
`subscribe(..)`, and does not register it with the lease strategy.** A lease won while stopped would lock every other
node out of a subscription this node does not serve. `start()` makes it compete for the lease whether or not it resumes
subscriptions automatically, since nobody paused it, and winning the lease resumes it in the wrapped model.

**The blocking catch-up model keeps a replay it cannot run, instead of ending it.** That covers a replay subscribed
while the model is stopped and one that `stop()` cuts short. The model keeps the replay together with the handle its
subscriber already holds, stores no further position for it, and counts the subscription as paused. `start(true)` runs every
kept replay again from where it started, and `resumeSubscription(id)` runs that one, first starting the model without
resuming anything else when it is stopped. `start(false)` does not run them, the same way the live model it wraps
does not resume its paused subscriptions on `start(false)`. Cancelling the subscription or shutting the model down
drops the replay, and `waitUntilStarted()` on its handle then returns `false`.

## Consequences

A replay that `stop()` cut short runs again from the position it started at when the subscription was made, so the
events it delivered before the stop are delivered again.

The blocking catch-up model runs a kept replay again only on `start(true)` or a resume, where the reactor catch-up
models in ADR 98 run it on any `start(..)`.

When a `resumeSubscription(..)` since `stop()` has started the wrapped model again, `CompetingConsumerSubscriptionModel`
cannot hand it a subscription made in the meantime without it running straight away. That subscription goes to the
wrapped model once the node wins its lease, and starts from the position at that time.

A subscription model of your own that you wrap in a `CompetingConsumerSubscriptionModel` has to hold a subscription
made while it is stopped paused. One that delivers it straight away delivers it on a node that holds no lease for it.
