# 161. A held event is withdrawn instead of delivered without the lease

Date: 2026-11-14

## Status

Accepted. Resolves [#1195](https://github.com/johanhaleby/occurrent/issues/1195). Amends
[ADR 156](0156-a-refresh-round-tells-the-listeners-what-they-have-not-heard.md) in one place, since `hasLock` on the
MongoDB lease strategies now answers no while a prohibition is owed, and that no isn't counted as a lapse. One case of
[ADR 117](0117-a-resumed-competing-consumer-continues-from-the-checkpoint.md) stays open as
[#1514](https://github.com/johanhaleby/occurrent/issues/1514).

## Context

`CompetingConsumerSubscriptionModel` asks `CompetingConsumerStrategy.hasLock` before it runs the action of a competing
subscription for an event. While the answer is no, it holds the event on the thread the wrapped model delivers it on.
In 0.34.0 these calls into the wrapped model ended that wait and ran the action without the lease:

- a pause of the subscription when the lease is lost, or a cancel
- a stop or a shutdown of the wrapped model
- a call that only asks whether the wrapped model runs the subscription

The model let the event through so that a wrapped model that waits in such a call for the delivering thread never
holds the call up. The node that held the lease could deliver the same event, so it reached the handler on two nodes.

Two more paths ran the action without the lease. `hasLock` on the MongoDB lease strategies could answer yes while the
strategy still had to tell its listeners about a prohibition, also once the node held a newer lease. And when the lease
came back before a later try of the competing consumer model had made the pause that prohibition called for, the held
event ran under the new lease, although another node may have delivered it in between.

## Decision

**A subscription model can declare that it delivers an event again after its action threw `AttemptNotMade`.** The new
capability `RedeliveringSubscriptions` has one method, `redeliversAnAttemptNotMade()`. A model that answers `true`
moves no position past such an event, doesn't retry it or report it as a failure, and delivers it again when the
subscription is resumed or started. It never waits for the delivering thread in a call that only asks about a
subscription or starts the model, and waits for it only for a limited time in a pause, cancel, stop or shutdown.
`NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` answer `true`, and a pause or stop of either waits at
most a second for a running action. `DurableSubscriptionModel`, the blocking catch-up models and
`ManualStartSubscriptionModel` answer what the model they wrap answers. `InMemorySubscriptionModel` doesn't declare it.
`findIn` doesn't look past a wrapper, as for the capabilities in
[ADR 151](0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md), because a wrapper that stores a
position past the event breaks the promise. So a wrapper is found only when it implements the capability itself.

**Over a model that answers `true`, the competing consumer model withdraws a held event instead of running it without
the lease.** The action throws `AttemptNotMade`, which moves from the nested `RetryExecution.AttemptNotMade` in an
internal package to the public `org.occurrent.retry.AttemptNotMade`, since a model of your own has to recognise it.
The event is withdrawn in these cases:

- once a pause or cancel of the subscription, or a stop of the wrapped model, has returned. A call that throws
  withdraws nothing, and the event waits for the lease again.
- once `shutdown()` has begun, as it calls `shutdown()` on the wrapped model, without waiting for that call to return
- when an interrupt ends the wait once `shutdown()` has begun

A call that only asks, or starts the wrapped model, leaves the event waiting. Three rare cases still run the action
without the lease and log a WARN each time. They are a subscription the model has forgotten while no pause, cancel or
stop returned, an interrupt before `shutdown()` has begun, and an event delivered on a thread that is inside a call the
competing consumer model makes into the wrapped model, since that call can't return while the event waits.

Over any other model the event runs as in 0.34.0, and the first time that happens the competing consumer model logs a
WARN that names `RedeliveringSubscriptions`.

**`hasLock` answers no while a prohibition is owed.** The `CompetingConsumerStrategy.hasLock` contract now asks a
strategy to answer `false` while it still owes its listeners an `onConsumeProhibited` for a lock the subscriber lost,
also when the subscriber holds the lock again. `MongoLeaseCompetingConsumerStrategySupport` answers no when the
comparison of ADR 156 would tell a prohibition, until each listener has returned from `onConsumeProhibited`. That no
isn't counted as a lapse, so the next round doesn't tell the grant again because of it. `SagaRunner` asks `hasLock`
before each poll of the saga timers, so a node that owes that prohibition fires no timer either.

**A prohibition left to a try keeps the event waiting until the subscription has been paused.** The competing consumer
model leaves a prohibition to a try on a thread of its own when another call holds the lock of the subscription. Over a
model that redelivers it does that also when an event of the subscription waits for the lease, since the pause can wait
for the delivering thread and the strategy's notifier thread shouldn't wait with it. Over such a model it then records
the consumer, and while it is recorded, a held event waits as without the lease, also when `hasLock` answers yes again.
The try pauses the subscription in the wrapped model, which withdraws the event, and resumes it once paused.

**A blocking catch-up replay that a subscribe started runs again after a withdrawn event.** Only a replay that a resume
started ran again, since the live model holds that subscription paused. A replay that a subscribe started ended with
"the subscription did not go live" logged at ERROR, and the withdrawn event never came back on this node. Now that
replay is held and runs again from the last position it stored, once the subscription is resumed or `start(true)` is
called, also when nothing paused or stopped it. It doesn't run again by itself after a backoff, since a model that
declares `RedeliveringSubscriptions` delivers a withdrawn event again only on a resume or a start. The withdrawal is
logged at DEBUG.

I rejected four other designs.

- **Checking the fencing token on the held event.** The event would wait for a prohibition once the token changed while
  it waited. A plain grant changes the token too, and for a consumer whose listeners were last told it wasn't granted
  and that has lost no lease since, `whatToTell` answers only `GRANT`. The event would wait for a prohibition that
  never comes.
- **Restarting `ChangeStreamSubscriptions.runUntilStopped` when the action throws `AttemptNotMade`.** That changes what
  every user of the MongoDB models gets, and a bare change stream would deliver the event again on the node without the
  lease.
- **Reading the stored checkpoint when a catch-up replay runs again.** Left to #1514.
- **Waiting for a running action before giving up the lease.** Left to
  [#1513](https://github.com/johanhaleby/occurrent/issues/1513).

## Consequences

- **The fix holds only on a stack that resumes from the stored checkpoint.** That is a stack with a
  `DurableSubscriptionModel`, directly or under a catch-up model, as ADR 117 decided. The Spring Boot starter always puts
  one under its `CompetingConsumerSubscriptionModel`. Over a bare `NativeMongoSubscriptionModel` or
  `SpringMongoSubscriptionModel` the event is withdrawn, but the node resumes from its own position in memory, so it
  delivers again what the node that held the lease delivered meanwhile.
- **A catch-up replay that loses the lease while it reads history can still run an event on two nodes.** A pause
  during a replay that a subscribe started takes effect only at the handover
  ([#1256](https://github.com/johanhaleby/occurrent/issues/1256)), so the replay hands the next event
  to the action after the pause has returned. Nothing withdraws that event, it waits for the lease, and it runs once
  this node gets the lease back, also when another node delivered it meanwhile. The duplicate is older than this
  decision. That is [#1528](https://github.com/johanhaleby/occurrent/issues/1528).
- **A catch-up replay that runs again starts from this node's own last stored position, not the stored checkpoint.**
  After a failover it delivers again what another node delivered meanwhile. Nothing is lost. That is #1514. Without a
  failover, this node also delivers again the events it replayed since it last stored a position, up to 999 with the
  Spring Boot starter, which stores one every 1,000 events. A replay built with `dontUseCheckpointStorage()` has stored
  no position, so it starts again from where the subscription asked to start, and this node delivers again the events
  it replayed before the withdrawn one.
- **Some cases stay at-least-once.** A node that stalls after `hasLock` answered yes, for example in a long GC pause,
  runs the action once it wakes up, also when its lease expired meanwhile. A MongoDB clock that runs ahead of the node's
  can make `hasLock` answer yes after another node took the lease, as the `hasLock` javadoc of
  `MongoLeaseCompetingConsumerStrategySupport` describes. An action still running when the lease is given up runs on,
  since the change stream pause waits at most a second for it. That is #1513, and
  [#1482](https://github.com/johanhaleby/occurrent/issues/1482) is the same one-second wait on one node.
  With checkpoint fencing on, see [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md), a
  checkpoint write from the node that lost the lease is refused, so the stored checkpoint doesn't move backwards. The
  fencing doesn't stop the action.
- **A `CompetingConsumerStrategy` of your own that answers yes from `hasLock` while it owes a prohibition** keeps the
  second path open.
- **Code that named `RetryExecution.AttemptNotMade` stops compiling.** Changing the import fixes it.
- **A wrapped model can log one failed run at shutdown.** That happens when the event is withdrawn before the wrapped
  model has marked itself shut down.
