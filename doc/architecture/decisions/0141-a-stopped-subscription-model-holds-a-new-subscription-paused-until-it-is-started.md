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
models, and the blocking catch-up models implement it. The default implementation throws
`UnsupportedOperationException`. A default that subscribed while the model is stopped would ask `isRunning()` first and
subscribe after, and a `resumeSubscription(..)` or a lease grant on another thread can start the model between the two
calls, so the subscription would run on a node that holds no lease for it. Subscribing and pausing straight after fails
the same way, since the model can deliver an event between the two calls.

**`CompetingConsumerSubscriptionModel` hands a subscription made while it is stopped to the wrapped model in
`subscribe(..)`, through `subscribePaused(..)`, and does not register it with the lease strategy.** The wrapped model
then holds it paused also when a `resumeSubscription(..)` since `stop()` has started it again, or when its own `stop()`
threw. A lease won while stopped would lock every other node out of a subscription this node does not serve. `start()`
makes it compete for the lease whether or not it resumes subscriptions automatically, since nobody paused it, and
winning the lease resumes it in the wrapped model.

**A wrapped model that refuses `subscribePaused(..)` with `UnsupportedOperationException` gets the subscription the
way it did in 0.33.0, the one case where a subscription made while the node is stopped can deliver events before
`start()` without being resumed.** The node competes for the lease straight away, and when it wins, it subscribes the
subscription in the wrapped model without starting that model. When the wrapped model runs it, `subscribe(..)` records
it as running, so it delivers events before `start()`, and `start()` finds it started. From then on it competes as a
subscription the user resumed since `stop()` does. A lease the node loses pauses it and keeps it competing, and winning
the lease back resumes it. When the node loses the lease in `subscribe(..)`, or when the wrapped model holds the
subscription paused because that model is stopped, it gives up its registration and waits for `start()`, since a
stopped node competes for nothing it does not deliver. Waiting for `start()` also after winning would start it where the
wrapped model starts a subscription at that moment, and lose every event written in between. Losing no event ranks above
what `stop()` promises, so the rule for this case is that for any wrapped model, nothing that works in 0.33.0 throws or
delivers fewer events, and nothing is delivered without the lease.

**`CompetingConsumerSubscriptionModel.stop()` pauses a subscription in the wrapped model when that model still runs it
before it gives up the lease.** A wrapped model that threw from its own `stop()` can still run every subscription, and
a node delivers only while it holds the lease. A subscription the wrapped model still runs after
`pauseSubscription(..)` has returned keeps its lease and stays running, and `stop()` throws. When the wrapped model's
own `stop()` threw, `stop()` throws an `IllegalStateException` with that failure as its cause, which names the
subscriptions it paused and says that `start(true)` resumes them while `start(false)` keeps them paused.

**`CompetingConsumerSubscriptionModel` changes what it records, and asks the wrapped model to change a subscription,
only while it holds its monitor, except for two steps of `subscribe(..)`.** These threads act on it:

| Thread | Outside the monitor | Under the monitor |
|---|---|---|
| A user's `stop()`, `start(..)`, `pauseSubscription(..)`, `resumeSubscription(..)` and `cancelSubscription(..)`, also when an action calls one on the wrapped model's delivery thread | Nothing | Everything, including registering with the lease strategy in `start(..)` and `resumeSubscription(..)` |
| `onConsumeGranted(..)` and `onConsumeProhibited(..)`, which the lease strategy calls on its notifier thread or on the thread that registers | Nothing | Everything, including subscribing, resuming or pausing in the wrapped model |
| `subscribe(..)` | Registering with the lease strategy, and making the subscription in the wrapped model | Deciding the next step from what holds at that moment, and recording the subscription |
| `shutdown()` | Shutting the lease strategy down, first | Everything else |

The MongoDB lease strategies retry a registration for as long as MongoDB cannot be reached, and making a subscription
in the wrapped model can take as long as opening a change stream. A lifecycle call or a lease callback that waited for
either would hang for as long, so `subscribe(..)` holds nothing while it does them. After each of the two steps it takes
the monitor and decides the next one from what holds then, whatever held when it began. That is whether the model is
shut down or stopped, whether the node holds the lease, and whether the wrapped model runs what it made or holds it
paused.

- Shut down, or the id cancelled with `cancelSubscription(..)`: it cancels what it made in the wrapped model, gives up
  its registration and throws `IllegalStateException`. A `subscribe(..)` after `shutdown()` throws the same exception
  before it does anything.
- Stopped, nothing made yet: it makes the subscription with `subscribePaused(..)`. A wrapped model that refuses gets it
  as described above.
- Stopped, something made: it pauses the subscription when the wrapped model runs it, gives up its registration, and
  records it as waiting for `start()`. One the wrapped model still runs after the pause keeps its lease and is recorded
  as running. So is one that a model refusing `subscribePaused(..)` runs.
- Started, not registered: it registers.
- Started, lease held: it starts the wrapped model when that model is stopped, then subscribes there, or resumes what the
  wrapped model holds paused. A resume that throws cancels the subscription in the wrapped model and gives up the
  registration, and `subscribe(..)` throws, so the same id can be subscribed again.
- Started, lease not held: it records the subscription as waiting for a grant, still registered. When the wrapped model
  runs what it made, since the lease went while it was being made, it pauses it there and records it as paused by the
  system, which the next grant resumes.

A lease callback for a subscription that `subscribe(..)` has not recorded yet finds nothing and does nothing, and the
step after it asks the strategy whether the node holds the lease. The wrapped model is started only under the monitor,
so a `subscribe(..)` that a `stop()` overtook never starts the wrapped model after that `stop()` has returned.

These rules follow:

1. A subscription delivers only while the node holds its lease. The one exception is a lease lost while the wrapped
   model makes the subscription, which delivers until the step after it pauses the subscription. 0.33.0 never paused it.
2. Once `subscribe(..)` has returned or thrown, what the model records for the id matches what the wrapped model holds,
   whatever ran on other threads in the meantime.
3. No lifecycle call and no lease callback waits for a registration or a subscribe in the wrapped model that
   `subscribe(..)` does on another thread.
4. A failure that goes away does not keep a subscription from running for good. A `subscribe(..)` that throws takes
   back what it made, so the same id can be subscribed again. When cancelling it in the wrapped model throws too, the subscription stays recorded
   as waiting with its lease given back, and the next grant tries it again.

`start(..)` and `resumeSubscription(..)` still register under the monitor, as they do in 0.33.0, so a `stop()` waits for
one whose registration retries while MongoDB cannot be reached. `shutdown()` does not wait for it, since shutting the
lease strategy down ends that registration before `shutdown()` takes the monitor.

The alternative is one serial executor that runs every change of state, calls the wrapped model and the lease strategy
outside it, and applies each result only when nothing changed while the call ran. I did not choose it. Every call moved
outside the serial path would need its own answer to a stop, a start, a cancel or a lease change that came while it
ran, for the lease callbacks and the lifecycle calls as much as for `subscribe(..)`. Keeping the wrapped model's calls
under the monitor means that `stop()`, `start(..)`, a pause, a resume and a lease callback never overlap. Only the two
steps of `subscribe(..)` that can take as long as an outage go outside, and one rule answers for both of them, which is
to decide again from what holds now.

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
Unless it implements `subscribePaused(..)`, a subscription made while the competing consumer model is stopped reaches
your model as soon as the node wins its lease, and when your model runs, it delivers events before `start()`. A model
whose subscription still runs after `pauseSubscription(..)` has returned makes `stop()` throw, and the node keeps that
subscription's lease.

A lease lost while the wrapped model makes a subscription in `subscribe(..)` lets that subscription deliver until
`subscribe(..)` takes the monitor again and pauses it.
