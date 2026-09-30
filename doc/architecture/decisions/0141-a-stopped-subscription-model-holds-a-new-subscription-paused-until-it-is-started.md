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
stopped paused.** It registers the subscription, delivers nothing, returns `false` from `isRunning(id)` and `true` from
`isPaused(id)`, and runs it once when it is started. Its own `isRunning()` says whether it runs.

Where such a subscription starts is up to each model:

| Model | Where a subscription made while it is stopped starts, for `StartAt.now()` or the model default |
|---|---|
| `DurableSubscriptionModel` | The position it records in `subscribe(..)` when it has none stored |
| `NativeMongoSubscriptionModel` | The operation time MongoDB answers with, asked for when `subscribe(..)` is called |
| `SpringMongoSubscriptionModel` | Wherever the change stream is when it opens, after `start()` or a resume |
| `InMemorySubscriptionModel` | The first event fed to it after it is resumed |
| The blocking catch-up models | The replay's own start position, and the live model's for a subscription with no replay |

**`SubscriptionModel.subscribePaused(..)` holds a new subscription paused the same way, whether or not the model
runs.** `SpringMongoSubscriptionModel`, `NativeMongoSubscriptionModel`, `InMemorySubscriptionModel`,
`DurableSubscriptionModel`, `ManualStartSubscriptionModel`, the blocking push, synchronous and catch-up-then-push
models, and the blocking catch-up models implement it. The default implementation calls
`subscribe(..)` while the model is stopped and throws `UnsupportedOperationException` while it runs, since pausing a
subscription after subscribing it could deliver an event first.

**`CompetingConsumerSubscriptionModel` hands a subscription made while it is stopped to the wrapped model in
`subscribe(..)`, through `subscribePaused(..)`, and does not register it with the lease strategy.** The wrapped model
then holds it paused also when a `resumeSubscription(..)` since `stop()` has started it again, or when its own `stop()`
threw. A lease won while stopped would lock every other node out of a subscription this node does not serve. `start()`
makes it compete for the lease whether or not it resumes subscriptions automatically, since nobody paused it, and
winning the lease resumes it in the wrapped model.

**A running wrapped model that refuses `subscribePaused(..)` with `UnsupportedOperationException` gets the subscription
the way it did in 0.33.0, the one case where a subscription made while the node is stopped delivers events before
`start()` without being resumed.**
The node competes for the lease straight away. When it wins, it subscribes the subscription in the running wrapped
model and records it as running, so it delivers events before `start()`, and `start()` finds it started. From then on
it competes as a subscription the user resumed since `stop()` does. A lease the node loses pauses it and keeps it
competing, and winning the lease back resumes it. One that loses the lease in `subscribe(..)`, or one the wrapped model
does not run after all, gives up its registration and waits for `start()`, since a stopped node competes for nothing it
does not deliver. Waiting for `start()` also after winning would start it where the wrapped model starts a subscription at
that moment, and lose every event written in between. Losing no event ranks above what `stop()` promises, so the rule
for this case is that for any wrapped model, nothing that works in 0.33.0 throws or delivers fewer events, and nothing is
delivered without the lease.

**`CompetingConsumerSubscriptionModel.stop()` pauses a subscription in the wrapped model when that model still runs it
before it gives up the lease.** A wrapped model that threw from its own `stop()` can still run every subscription, and
a node delivers only while it holds the lease. A subscription the wrapped model still runs after
`pauseSubscription(..)` has returned keeps its lease and stays running, and `stop()` throws. When the wrapped model's
own `stop()` threw, `stop()` throws an `IllegalStateException` with that failure as its cause, which names the
subscriptions it paused and says that `start(true)` resumes them while `start(false)` keeps them paused.

**`subscribe(..)` registers with the lease strategy and subscribes in the wrapped model without holding anything that
`stop()`, `start(..)`, `cancelSubscription(..)` or `shutdown()` waits for, and it records the subscription only when none
of them ran in the meantime.** The MongoDB lease strategies retry a registration for as long as MongoDB cannot be
reached, and subscribing in the wrapped model can take as long as opening a change stream. A lifecycle call that waited
for either would hang for as long, `shutdown()` included. Each of the four calls counts as a new lifecycle state, and
`subscribe(..)` records what it made under the model's monitor only while the state it made it for still holds.
Otherwise it makes the subscription again for the new state. After a `stop()` it pauses what it made in the wrapped
model and gives up its registration, and after a `start(..)` it competes for the lease. `shutdown()` ends a registration
that is retrying, and a `subscribe(..)` that `shutdown()` overtook pauses what it made, gives up its registration and
throws `IllegalStateException`. So once `subscribe(..)` has returned, a subscription that a `stop()` overtook is paused
in the wrapped model and holds no lease.

A lease callback does not wait for `subscribe(..)` either. A callback for the subscription being subscribed finds
nothing recorded yet and does nothing. So once the wrapped model has it, `subscribe(..)` asks the strategy again, and
pauses it when the lease is gone, or starts it when a grant came in the meantime. `start(..)` still registers under the
monitor, as it does in 0.33.0, so a `shutdown()` waits for a `start(..)` whose registration retries while MongoDB cannot
be reached.

**The blocking catch-up model keeps a replay it cannot run, instead of ending it.** That covers a replay subscribed
while the model is stopped and one that `stop()` cuts short. The model keeps the replay together with the handle its
subscriber already holds, and counts the subscription as paused. An event whose action completed before the stop may
still have its position stored, and nothing past it is. `start(true)` runs every kept replay again from the last
position it stored, or from where it started when it stored none, and `resumeSubscription(id)` runs that one, first starting the model without
resuming anything else when it is stopped. `start(false)` does not run them, the same way the live model it wraps
does not resume its paused subscriptions on `start(false)`. Cancelling the subscription or shutting the model down
drops the replay, and `waitUntilStarted()` on its handle then returns `false`. Neither stores the present as the
position of a replay it cut short, since that position lies past the history the replay has not read.

## Consequences

A replay that `stop()` cut short runs again from the last position it stored, so the events it delivered after that
position are delivered again, and the stored position never moves back. For a time position the replay also delivers
the events stored at that exact time again, since other events can have the same time down to the millisecond.

The blocking catch-up model runs a kept replay again only on `start(true)` or a resume, where the reactor catch-up
models in ADR 98 run it on any `start(..)`.

A subscription made on a stopped `CompetingConsumerSubscriptionModel` over a bare `SpringMongoSubscriptionModel`, with
`StartAt.now()` or the model default, starts where the change stream is once the node wins the lease. An event written
between `subscribe(..)` and then is not delivered to it. Over a `DurableSubscriptionModel` it is, since that model
records the position in `subscribe(..)`.

A subscription model of your own that you wrap in a `CompetingConsumerSubscriptionModel` has to hold a subscription
made while it is stopped paused. One that delivers it straight away delivers it on a node that holds no lease for it.
Unless it implements `subscribePaused(..)`, a subscription made while the competing consumer model is stopped and the
wrapped model runs reaches your model as soon as the node wins its lease, and delivers events before `start()`. A model whose subscription still runs after `pauseSubscription(..)` has returned makes
`stop()` throw, and the node keeps that subscription's lease.
