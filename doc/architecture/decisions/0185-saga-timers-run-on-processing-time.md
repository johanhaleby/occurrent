# 185. Saga timers run on processing time

Date: 2026-11-20

## Status

Accepted. Part of [#918](https://github.com/johanhaleby/occurrent/issues/918) and
[#952](https://github.com/johanhaleby/occurrent/issues/952), which let a quarantined saga instance hold the events it
skips and apply them when it is released, as
[ADR 178](0178-a-quarantined-saga-instance-records-what-it-skips-and-is-released-by-replaying-it.md) describes. Holding
inputs for hours means a timer can come due while inputs are held, and ADR 178 Decision point 6 points here for the
order the two run in. The rule applies to a `Saga` written by hand, a flow saga and an `@Saga` alike.

## Context

A saga timer fires on the clock of the node whose poller finds it due. The executor arms a
`StartTimeout(timerName, after)` at the time it applies the reaction that returned it, plus `after`, and a
`StartTimeoutAt(timerName, at)` at `at`. The poller fires a timer once its due time is at or before the poller's clock.
0.34.0 worked the same way.

No time from the input takes part in this. `EventMeta`, which is what the executor knows about an input besides the
event itself, has the stream id and version, the subscription position, and the event's type, id and source. It has no
time. The CloudEvent `time` attribute is optional and set by whoever wrote the event, so two writers with different
clocks can give two events in the same stream times that are out of order.

While inputs arrive live, the difference rarely shows. Quarantine makes it show. A quarantined instance holds every
input it skips until someone releases it, which can be hours later, and the release applies the held inputs on the
releasing node's clock. Take an order saga that arms a 30 minute payment timeout and cancels the order when it fires.
The instance is quarantined at minute 10, the payment arrives at minute 40 and is held, and the instance is released at
minute 90. Live, the timeout would have fired at minute 30, before the payment arrived. After the release, the payment
is applied first, and a reaction that cancels the timer on payment cancels it, so the timeout never fires.

Ordering the two by event time would mean firing the timer between two held inputs, at the place in the held list where
its due time falls. That needs a time for each held input that every node orders the same way, and the CloudEvent
`time` attribute is not such a time.

## Decision

### 1. Timers run on processing time, across the framework

A timer is due at a time on the polling node's clock, and the time of an input plays no part. A `StartTimeout` in the
reaction to a held input is armed at the time the release applies that input, plus its duration, not at the time the
input arrived.

### 2. Held inputs are applied before the instance's timers fire

`SagaStateStore.findWithDueTimers` returns `ACTIVE` instances only. An instance stays `QUARANTINED` while it is
releasing, until `compareAndSaveQuarantineEnded` makes it `ACTIVE` in the write that deletes its last skipped-input
records. `SagaExecution.hasDueTimer` checks again when it fires a timer and refuses a quarantined instance, for one that
was quarantined between the poll and the fire.

So a timer that came due while the instance was quarantined or releasing fires on the first poll after the instance is
`ACTIVE`, after every held input. When a held input completes the instance, the release ends it as `COMPLETED` and its
timers never fire.

### 3. Timers wait for the subscription

`SagaRunner.run` waits for the subscription to start by default, so the poller doesn't begin before a catch-up replay
ends. The overload that takes `waitUntilStarted` as `false` hands that to the `timersEnabled` gate the caller passes,
which the poller asks before every poll.

The Spring Boot starter passes `SagaAnnotationRegistrar.timersEnabledFor`. It gives `isRunning` and not `isCatchingUp`
for a model with `ReplayAwareSubscriptions`, `isRunning` alone for a model without it, and `true` for a `Subscribable`
that is not a `SubscriptionModelLifeCycle`. In this repository both `SubscriptionModel` interfaces extend
`SubscriptionModelLifeCycle`, so `true` only reaches an application's own `Subscribable`. A `PushSubscriptionModel`
gets `isRunning` alone, since it has no replay.

### 4. A deadline ordered against inputs by event time is the saga's job

An application that needs one puts the deadline in its event data and arms the timer with `StartTimeoutAt` at that
instant. A flow saga does the same with the step `timeout` that takes a function of the received events. The reaction
to a later input compares the time in that input's data with the deadline kept in the saga's state, and treats an input
past the deadline as late, whether the timer has fired yet or not.

The timer still fires on processing time. What changes is that an instant taken from data stays the same when a release
replays the reaction that arms it, while a `StartTimeout` moves by as long as the instance was quarantined.

### 5. A store-assigned append time is the later path for SQL

A time the event store assigns when it appends an event is ordered the same way on every node that reads it. With one,
the executor could fire a held instance's due timers at their place among its held inputs. That is the path for the SQL
event store, and 0.35.0 adds no such time.

## Consequences

- A saga that uses a timer as a deadline can see an input from after the deadline before the timeout fires, when the
  instance was quarantined in between. ADR 178 Decision point 6, the 0.35.0 migration guide and the quarantine runbook
  say so.
- A `StartTimeout` armed by a held input fires later than it would have live, by as long as the instance was
  quarantined.
- No released behaviour changes. 0.34.0 armed a `StartTimeout` against the executor's clock too, and kept a quarantined
  instance's timers armed without firing them.
- The order is the same on every store, because it depends on the instance's status and on the poller, and not on a
  time the store records.
- Occurrent gives an application that needs event-time deadlines `StartTimeoutAt` and nothing more. The comparison of
  times lives in the application's reactions.
