# 177. A lifecycle call takes effect at one instant, as in some serial order

Date: 2026-11-14

## Status

Accepted. Part of [#1257](https://github.com/johanhaleby/occurrent/issues/1257). Amends
[ADR 131](0131-a-per-id-lock-closes-the-blocking-catchup-handover-race.md), whose handover lock registry is never
reclaimed, and [ADR 146](0146-a-catch-up-takes-over-a-live-start-the-wrapped-model-loses-as-it-opens.md), whose
takeover resume fails when the wrapped model holds the subscription paused again. Applies to the blocking and reactor
catch-up models, to `ManualStartSubscriptionModel` when it wraps one, and to the two handover engines,
`BlockingHandover` and `ReactiveHandover`, with the push models and projection feeds built on them.

## Context

Many 0.35.0 issues on the catch-up models and the handover engines describe the same bug. Two lifecycle calls, or a
call and the thread that runs a replay, interleave and leave a state that no single-threaded run of the same calls
could leave. Four examples:

* [#1237](https://github.com/johanhaleby/occurrent/issues/1237): a `stop()` during a blocking catch-up `start(..)` can
  leave the wrapped model running and every new catch-up subscription paused.
* [#1255](https://github.com/johanhaleby/occurrent/issues/1255): `isRunning(id)` and `isPaused(id)` are both false while
  a subscribe-started catch-up hands over.
* [#1239](https://github.com/johanhaleby/occurrent/issues/1239): a reactor catch-up action can handle the same event
  twice at once when `start(true)` follows a `stop()`.
* [#893](https://github.com/johanhaleby/occurrent/issues/893) item 32: a stop that comes while a handover delivers what
  it buffered during the replay still lets the catch-up report success, and the reactive one delivers those events
  after `stop()` returned.

The fixes so far each closed the window their issue found. On the blocking catch-up that added `lifecycleGeneration`,
`liveDelegateResumes` and `stoppedByFailedStart` to `AbstractCatchupSubscriptionModel`, next to the per-id claim and
the handover lock from ADR 131. Each fix was tested against its own window, because nothing wrote down what a correct end
state is when calls overlap.

Three implementation units in 0.35.0 change this code next. L2 changes the blocking catch-up, L3 the reactor catch-up
and HO1 the handover engines. They need one rule to build against and a test that checks it.

## Decision

### The rule

A lifecycle call on a subscription id X, or on the model, takes effect at one instant between the moment it's called
and the moment it returns. A step that a replay thread or a handover engine takes on its own also takes effect at one
instant. Once every call has returned and every replay has finished, X's observables are the ones that some serial
order of those calls and steps gives, an order that keeps each thread's calls in the order the thread made them.

A read (`isRunning`, `isPaused`, `isCatchingUp`, `isHeldPausedFor`) answers the state at some instant during the
read.

A call that times out also has a stated serial result. It took effect as a stated first part of itself, usually
nothing, and the exception message says which part. These are the four results `AbstractCatchupSubscriptionModel`
already reports after a 10 second wait:

| Result | Meaning |
| --- | --- |
| `NOTHING_CHANGED` | the call took no effect |
| `STARTED_BUT_NOT_RESUMED` | the model started, X was not resumed |
| `STARTED_BUT_NOT_RUN_AGAIN` | the model started, a parked replay was not run again |
| `STOPPED_ALL_THE_SAME` | the model stopped, a replay it waited for had not finished |

### Two safety properties

Two properties are true at every instant, not only at the end:

* **S1.** At most one call to X's action is running at a time, counting the replay thread and the wrapped model's
  live delivery together.
* **S2.** No call to X's action starts after `cancelSubscription(X)`, `stop()` or `pauseSubscription(X)` returned.

S2 covers pause in every phase. A `pauseSubscription(X)` that returns while X still replays its history stops the
replay before the next event, and X is then held paused. Today a pause during a subscribe-started catch-up takes effect
only at the handover ([#1256](https://github.com/johanhaleby/occurrent/issues/1256)).

### Blocking catch-up

What the rule compares for each id:

| Observable | Values |
| --- | --- |
| phase | absent, replaying, parked (stopped model, ADR 141), held paused (including held because its live start was lost), live and running, live and paused |
| stored checkpoint | none, a catch-up checkpoint, or the live position |
| the handle | started, waiting, or failed with which exception |
| delivered events | which events reached the action, and how often |

For the model, the rule compares whether it's running and whether it was shut down.

The serial result of each call, as one step on that state:

| Call | From | Result |
| --- | --- | --- |
| `subscribe(X)` | absent, model running | replaying, then live and running after the handover |
| `subscribe(X)` | absent, model stopped | parked |
| `subscribe(X)` | any other phase | `DuplicateSubscriptionIdException`, nothing changes |
| `cancelSubscription(X)` | any | absent, stored checkpoint removed |
| `pauseSubscription(X)` | live and running | live and paused |
| `pauseSubscription(X)` | replaying | held paused, and the replay delivers nothing more |
| `resumeSubscription(X)` | parked or held paused | replaying, then live and running |
| `resumeSubscription(X)` | live and paused | live and running |
| `resumeSubscription(X, startAt)` | known, not running | live and running from `startAt` |
| `resumeSubscription(X, startAt)` | running | `SubscriptionAlreadyRunningException`, nothing changes |
| `stop()` | any | model stopped, a replay becomes parked, a live subscription becomes paused |
| `start(false)` | any | model running, nothing resumed |
| `start(true)` | any | model running, parked replays and paused subscriptions run again |

Three more rows decide what a failure means:

* A `start(..)` whose wrapped model throws before it started leaves the model stopped, as if the call never happened.
  One that throws after the wrapped model started leaves everything as a successful `start(..)` would, and the
  caller still gets the exception.
* A replay whose live start is lost is held paused. A resume of it with an origin above the beginning refuses with
  `CatchupCannotResumeException` until it is cancelled or resumed at a given position.
* A takeover that the wrapped model holds paused again takes over again. Each takeover counts toward the 3 replays
  `MAX_REPLAYS_AGAIN` allows, and once those are used, the resume fails as a takeover fails today when every replay lost
  its live start. This replaces the ADR 146 rule that the resume fails at once
  ([#1269](https://github.com/johanhaleby/occurrent/issues/1269)).

A `cancelSubscription(X)` or `stop()` that takes effect while the replay thread retries `canResumeFrom` or
`globalCheckpoint` ends the retry, and the replay ends as cancelled or parked. For that the two methods on the blocking
`CheckpointAwareSubscriptionModel` get an overload that takes a `BooleanSupplier stillWanted`. Its default
implementation calls the existing method, so a model written outside Occurrent keeps compiling and gets no early end.
The MongoDB models check `stillWanted` before each new attempt
([#1266](https://github.com/johanhaleby/occurrent/issues/1266) item 1).

```java
default boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
    return canResumeFrom(checkpoint);
}
```

`ManualStartSubscriptionModel` is a layer of its own around the catch-up. Its `start(..)` and `stop()` take effect at
one instant across its own state and the delegate's `start(..)` or `stop()`, and its registration reads
`globalCheckpoint()` inside that same instant.

### Blocking locks

The rule needs one lock for each scope, taken in a fixed order:

| Order | Lock | Taken by |
| --- | --- | --- |
| 1 | `ManualStartSubscriptionModel` model lock | its `start(..)`, `stop()` and registration |
| 2 | catch-up model lock, read and write | `start(..)`, `stop()` and `shutdown()` exclusively, across the loop over every id and the wrapped model's own `start(..)` or `stop()`. Every per-id call shared, around its claim. Replay threads never take it |
| 3 | per-id claim (`lifecycleClaims`) | each lifecycle call on X, as today |
| 4 | per-id handover lock | the replay thread at every step it takes on its own, including the check and the write of a catch-up checkpoint |
| 5 | `lifecycleLock` | short reads and writes of shared fields, nothing taken under it |

The model lock replaces the reasoning behind `lifecycleGeneration`, `liveDelegateResumes` and `stoppedByFailedStart`.
A `start(..)` or `stop()` waits for the per-id calls in flight and then runs alone. A per-id call waits for a
`start(..)` or `stop()` in flight.

The exclusive wait has no deadline of its own. Each shared holder waits at most 10 seconds for its claim, so the
exclusive wait ends too. A `start(..)` or `stop()` called from an action on a replay thread, or from a thread that
holds a claim, throws at once, because it would wait for a per-id call that can only finish on that thread.

Handover locks are counted and removed like claims. ADR 131 says its registry of handover locks is never reclaimed.
Under this rule it keeps an entry only while a call or a replay thread uses it ([#1257](https://github.com/johanhaleby/occurrent/issues/1257)).

### Reactor catch-up

The rule, the observables and the serial results are those of the blocking catch-up. Only the locks differ, because a
Reactor thread must not wait on a lock that a caller thread holds across a call to the wrapped model.

* Per id, `NamedCatchupSupport` runs every lifecycle step for X one at a time, in arrival order, without blocking.
  A step from a Reactor thread is queued and the thread moves on. A public call on a caller's thread queues its step and
  waits for that step only. This replaces both `synchronized(state)` across the wrapped model's calls in `handOver` and
  the blocking id lock that Reactor-thread paths take today.
* For the model, `start(..)`, `stop()` and `shutdown()` take `startLock`, above the per-id queues. `stop()`
  takes no `startLock` today.
* A cancel or stop reaches a `canResumeFrom` in flight by disposing the replay's subscription, so the reactor API
  needs no new overload.

### Handover engines

What the rule compares for each engine:

* its phase, which is idle, replaying, delivering what it buffered during the replay, live, stopped, failing or failed
* whether the catch-up marker exists
* the answer each accepted payload got, which is delivered, refused with a reason, or stopped
* the order in which payloads reached the `deliver` callback the engine was created with

The calls are `catchUp`, the three accept calls (`accept`, `acceptReportingDelivery`, `acceptIfLive`),
`stopIfNotCatchingUp`, the stop and cancel calls of the models and feeds built on the engine, a feed's `register`, a
registrar's `close()`, and the reset [#1291](https://github.com/johanhaleby/occurrent/issues/1291) asks for.

The catch-up marker is written in this order on both stacks:

1. Read the marker, and skip the replay when it's there.
2. Replay the history.
3. Deliver the payloads buffered during the replay.
4. Write the marker.

`ReactiveHandover` writes the marker before step 3 today
([#893](https://github.com/johanhaleby/occurrent/issues/893) item 31). A stop or cancel that takes effect before step 4
means no marker is written and the catch-up ends as stopped, not as done (#893 item 32). A failure is permanent, and
on `ReactiveHandover` it also forgets the marker. Keeping a late store write from bringing the marker back after that is
[#1150](https://github.com/johanhaleby/occurrent/issues/1150)'s.

Every accepted payload gets an answer. The replay turn, which lets one catch-up at a time replay on an engine, is
given back on every path. That includes a JVM-fatal error
from `deliver`, a payload the buffer discards, a recording callback that throws and a replay thread that fails to start.

The engine's lock and its replay turn are the only locks the rule needs here. Step 4 runs inside the replay turn and
checks under the engine lock that the turn is still its own. The push models' `markerLocks` are replaced by that turn. Their
`synchronized(this)` monitor becomes a lock that nothing waits under, stores nothing and calls no engine method
([#893](https://github.com/johanhaleby/occurrent/issues/893) item 33).

A `DomainEventFeed` registration is a lifecycle call on the feed's id. A failed engine leaves the failed phase only by a
reset ([#1291](https://github.com/johanhaleby/occurrent/issues/1291)). A registrar `close()` takes effect before or
after each manual-start registration, and a registration that takes effect after it is undone
([#1032](https://github.com/johanhaleby/occurrent/issues/1032)).

### The property harness

A jqwik harness checks the rule against the code, in two test trees:

* `subscription/util/blocking/catchup-subscription`, package `org.occurrent.subscription.blocking.durable.catchup.lifecycle`,
  for `CatchupSubscriptionModel` and `StreamCatchupSubscriptionModel`.
* `subscription/api/blocking`, next to `BlockingHandoverTest`, for `BlockingHandover`.

For the catch-up models, jqwik generates 2 or 3 threads, each with 1 to 3 calls, over 1 or 2 ids. A runner starts the
threads together, waits until every call returned and every replay finished, and reads the observables. A reference
model in the test tree tries every order of the same calls that keeps each thread's order. The run passes when one of
those orders gives the same call results, the same `isRunning()` of the model, and the same `isRunning(id)` and
`isPaused(id)` for each id. It doesn't compare the stored checkpoint, the handle or the delivered events.

The reference model covers `subscribe`, `cancelSubscription`, `pauseSubscription`, `resumeSubscription(id)`,
`start(false)`, `start(true)` and `stop()`, and a start that fails before or after it takes effect. It doesn't cover
`shutdown()`, `subscribePaused`, `resumeSubscription(id, startAt)`, a live start that is gone or a takeover, so the
harness checks none of those rows yet.

S1 and S2 are checked while the run is going, by a wrapper around the action. The S2 check skips a pause or a stop
that a resume of the same id or a `start(true)` overlaps, because that call can take effect after it. The harness
doesn't catch an action that starts in that window.

For `BlockingHandover`, a runner steps one replay thread through its history reads and the drain, and runs each accept
call on a thread of its own. There is no reference model of the handover. Its properties check what holds in every
order. The marker is written only for a catch-up that nothing cancelled. A live payload is delivered once, and a
history payload at most once for each replay that read the history. Every accept call gets an answer that agrees with
what was delivered, except an `acceptReportingDelivery` in a run that injects a failure, which can report a buffered
payload that is then lost. A later catch-up gets the replay turn.

A property the code breaks today stays in the harness, disabled with the number of the issue that owns it. The unit
that fixes that issue enables it. The timeout results are not exercised, because the 10 second wait is a constant.

These are disabled today:

* [#1256](https://github.com/johanhaleby/occurrent/issues/1256), a pause during a replay that a subscribe started
  returns, and the replay keeps delivering to the action until the handover. S2 with a pause, both serial-order
  properties with a pause, and 2 examples.
* [#1515](https://github.com/johanhaleby/occurrent/issues/1515), an action starts after a cancel of its id returned,
  although the subscribe had returned before the cancel began. S2 without a pause.
* [#1516](https://github.com/johanhaleby/occurrent/issues/1516), `resumeSubscription(id)` after `stop()` returns
  normally and the model stays stopped, when the subscribe it resumes overlapped the stop. The serial-order property
  without a pause, and 1 example.
* [#1517](https://github.com/johanhaleby/occurrent/issues/1517), a cancel returns while the action of the subscription
  it cancels still runs, and a new subscribe of the same id enters a second call to the action. S1.
* [#1520](https://github.com/johanhaleby/occurrent/issues/1520), a `start(true)` whose wrapped model throws after it
  started keeps the parked replays parked. The serial property on scripts that can hold such a call, and 1 example.
  The serial property on all other scripts stays enabled.
* #893 item 32, a cancel before the marker step still writes the marker. 1 handover property and 1 example.

The reactor harness comes with L3.

### Issues the rule closes

An issue closes by construction when any change that meets the rule and passes the harness closes it.

| Unit | Closes by construction | Doesn't |
| --- | --- | --- |
| L2, blocking catch-up | [#1225](https://github.com/johanhaleby/occurrent/issues/1225), [#1226](https://github.com/johanhaleby/occurrent/issues/1226), [#1237](https://github.com/johanhaleby/occurrent/issues/1237), [#1252](https://github.com/johanhaleby/occurrent/issues/1252), [#1255](https://github.com/johanhaleby/occurrent/issues/1255), [#1256](https://github.com/johanhaleby/occurrent/issues/1256), [#1257](https://github.com/johanhaleby/occurrent/issues/1257), [#1258](https://github.com/johanhaleby/occurrent/issues/1258), [#1269](https://github.com/johanhaleby/occurrent/issues/1269), [#1271](https://github.com/johanhaleby/occurrent/issues/1271), [#1515](https://github.com/johanhaleby/occurrent/issues/1515), [#1516](https://github.com/johanhaleby/occurrent/issues/1516), [#1517](https://github.com/johanhaleby/occurrent/issues/1517), [#1520](https://github.com/johanhaleby/occurrent/issues/1520), [#1266](https://github.com/johanhaleby/occurrent/issues/1266) item 1 | [#946](https://github.com/johanhaleby/occurrent/issues/946) (javadoc), [#1272](https://github.com/johanhaleby/occurrent/issues/1272) (test gaps), #1266 items 2 to 5 |
| L3, reactor catch-up | [#1156](https://github.com/johanhaleby/occurrent/issues/1156), [#1239](https://github.com/johanhaleby/occurrent/issues/1239), [#1268](https://github.com/johanhaleby/occurrent/issues/1268), [#1270](https://github.com/johanhaleby/occurrent/issues/1270) | |
| HO1, handover engines | #893 items 31, 32 and 33, [#1058](https://github.com/johanhaleby/occurrent/issues/1058), [#1062](https://github.com/johanhaleby/occurrent/issues/1062), [#1105](https://github.com/johanhaleby/occurrent/issues/1105), [#1134](https://github.com/johanhaleby/occurrent/issues/1134), [#1240](https://github.com/johanhaleby/occurrent/issues/1240), [#1278](https://github.com/johanhaleby/occurrent/issues/1278), [#1280](https://github.com/johanhaleby/occurrent/issues/1280), the `replayOpen` item of [#1107](https://github.com/johanhaleby/occurrent/issues/1107) | #893 item 29 (an API change), [#1057](https://github.com/johanhaleby/occurrent/issues/1057) (test timing), #893 item 11 (a test) |

Four more issues are owned outside these units and built against the rule. #1291 and #1032 are the feed's, #1262 is
`ManualStartSubscriptionModel`'s ([#1262](https://github.com/johanhaleby/occurrent/issues/1262)), and #1264 uses the
`stillWanted` overload ([#1264](https://github.com/johanhaleby/occurrent/issues/1264)).

## Alternatives considered

* **An epoch counter instead of the model lock.** Each `start(..)` and `stop()` would bump a counter, and a per-id call
  would check it before and after its work. That is the `lifecycleGeneration` approach generalised. It needs a check at
  every place a call can be overtaken, and a missed check is the kind of bug this ADR exists to stop.
* **Only order `start(..)` against `stop()`.** That closes #1237 but not #1225, #1226 or #1271, where the race
  condition is between a model call and a per-id call.
* **Interrupt the replay thread to end a `canResumeFrom` retry.** An interrupt reaches every blocking call the replay
  thread is in, the action and the event store read included, not only the retry.
* **Change the `canResumeFrom` signature.** Every model written outside Occurrent would stop compiling, for a signal it
  can ignore.
* **Let a pause during a replay take effect at the handover.** That keeps #1256, and the action can still run after
  `pauseSubscription(X)` returned.
* **Keep the ADR 146 refusal.** A wrapped model that pauses a takeover's resume once more would then need a manual
  resume, although the takeover could take the new live start over the same way it took the first.

## Consequences

* L2, L3 and HO1 are checked against the harness, not only against the window each issue found. A property that a
  unit's issue owns is enabled in that unit's PR.
* A blocking `start(..)` or `stop()` waits for every per-id call in flight, up to 10 seconds for each. A per-id call
  waits for a `start(..)` or `stop()` in flight, which includes the wrapped model's own `start(..)` or `stop()`.
* The handover lock registry stops growing with every id a model ever ran a catch-up for.
* The `stillWanted` overloads are new public API on `CheckpointAwareSubscriptionModel`. A model written outside
  Occurrent that doesn't override them keeps today's behaviour, where the retry runs on after a cancel until it gives up.
* A reactor catch-up no longer blocks a Reactor thread on a lifecycle lock.
* The harness doesn't test the timeout results. L2 can test them if it makes the 10 second wait configurable in tests.
* The catch-up reference model has no rows for `shutdown()`, `subscribePaused`, `resumeSubscription(id, startAt)`, a
  live start that is gone or a takeover, so no property checks #1269 yet. No property checks #1252, #1255, #1257 or
  #1258 either. They need a replay thread that fails to start, a read of `isRunning(id)` during the handover, a look
  at the handover lock registry and a look at the checkpoint store. L2 adds what its issues need before it claims them
  closed.
