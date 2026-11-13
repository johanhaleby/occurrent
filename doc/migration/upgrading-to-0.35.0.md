# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change, what the `UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and
what you have to do by hand. Find the parts of Occurrent you use in the lists below and read those sections.

The sections that can break the build are listed under "Changes that can break the build", and each bullet says when.
The compiler finds those breaks for you, but a section in that list can also describe a change that still compiles. The
other sections compile fine and behave differently, so read them first.

### Changes that can break the build

- [A saga applies a stream version that arrives after a higher one](#a-saga-applies-a-stream-version-that-arrives-after-a-higher-one).
  `SagaEnvelope` gains a 14th record component and `SagaRunnerConfig` a 6th. **Breaks the build** if you use a record
  pattern over either.
- [The deprecated `accept(..)` on the projection feeds is removed](#the-deprecated-accept-on-the-projection-feeds-is-removed). `DomainEventFeed` and
  `CatchupProjectionFeed` lose both `accept(..)` overloads, on the blocking and the reactor stack. **Breaks the build**
  if you still call one of them.
- [The reactive `executeAndReturnState` emits a non-null state](#the-reactive-executeandreturnstate-emits-a-non-null-state).
  `ReactiveSnapshotDeciderApplicationService` and `ReactiveSnapshotDcbDeciderApplicationService` return
  `Mono<@NonNull S>`. **Breaks the build** for Kotlin code that writes the result type as `Mono<State?>`.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from).
  `DcbStartAt` gains the case `DcbStartAt.AtCheckpoint`. **Breaks the build** if you `switch` exhaustively over
  `DcbStartAt` without a `default`.
- [DCB snapshots resume from marker versions](#dcb-snapshots-resume-from-marker-versions). `Snapshot` gains a 4th and a 5th
  record component. **Breaks the build** if you use a record pattern over it.
- [The reactor `HistoryLossReportingSubscriptions` gains a method](#the-reactor-historylossreportingsubscriptions-gains-a-method).
  **Breaks the build** if you implement the interface yourself.
- [A record pattern over `NativeMongoSubscription` stops compiling](#a-record-pattern-over-nativemongosubscription-stops-compiling).
  `NativeMongoSubscription` gains a third record component. **Breaks the build** if you use a record pattern over it.
- [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
  `DcbConsistencyToken` becomes a class, and `DcbEventStream(List, long)` and `DcbDomainEventStream(List, long)` are
  removed. **Breaks the build** if you create a token, call one of those constructors or call `value()`, which a
  `DcbEventStore` of your own usually does.
- [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). **Breaks the build** if you
  use a record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions`, or call `new DcbEventStream(events, head)`.
- [`ComposedDefaultStartPosition` and its bean are removed](#composeddefaultstartposition-and-its-bean-are-removed).
  **Breaks the build** if you inject the `occurrentComposedDefaultStartPosition` bean, name the type, or call or
  override `occurrentCompetingDurableSubscriptionModel(..)`.

### Sagas

- [A global position is no longer a saga redelivery key](#a-global-position-is-no-longer-a-saga-redelivery-key). A
  saga recognises a redelivery by `streamid` with `streamversion` only. An event that has just a `position` is refused
  under the default `REQUIRED`, and an unreadable `streamid` or `streamversion` fails the delivery. Compiles unchanged.
- [A saga applies a stream version that arrives after a higher one](#a-saga-applies-a-stream-version-that-arrives-after-a-higher-one).
  A saga keeps the highest applied version per stream and event type, and applies a late version instead of skipping
  it. Saga instances store a new `streamMarks` field. Breaks the build only for a record pattern over `SagaEnvelope` or
  `SagaRunnerConfig`.
- [A saga's quarantine budget runs per failing event](#a-sagas-quarantine-budget-runs-per-failing-event). Each failing
  event keeps its own clock. Compiles unchanged.

### DCB

- [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). **Breaks the build** if you
  use a record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions`, or call `new DcbEventStream(events, head)`.
- [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
  A `DcbAppendCondition` with a token read with other criteria throws `IllegalArgumentException`, on every store. A
  token value saved under 0.34.0 isn't valid. **Breaks the build** if you create a token, call
  `new DcbEventStream(events, head)` or `new DcbDomainEventStream(events, head)`, or call `value()`, which a
  `DcbEventStore` of your own usually does.
- [Criteria with tags conflict on their tags](#criteria-with-tags-conflict-on-their-tags). On MongoDB, sharing a type
  with a criteria item that has tags is no longer a reason for a condition to refuse.
- [An append on MongoDB waits for majority on its position reservation](#an-append-on-mongodb-waits-for-majority-on-its-position-reservation).
  This overrides a w:1 on that one write, which on the Spring and reactor stores now runs outside a transaction of yours
  too. On a set of a primary, a secondary and an arbiter with the secondary down, an append throws after
  `markerVersionsCommitTimeout`.
- [The default transaction executors retry on a MongoDB error label](#the-default-transaction-executors-retry-on-a-mongodb-error-label).
  A duplicate key from a write made outside the transaction is no longer retried.

### Subscriptions and catch-up

- [A read by position makes no promise about resuming](#a-read-by-position-makes-no-promise-about-resuming). A loop that
  keeps a position and reads after it through `readInPositionOrder(..)` or `afterPosition(..)` still compiles, and can
  skip an event.
- [A resume at a position through a `DurableSubscriptionModel` stores the checkpoint](#a-resume-at-a-position-through-a-durablesubscriptionmodel-stores-the-checkpoint).
  `RepositionableSubscriptions.findIn(durableModel)` finds the durable model, which stores the position before it
  resumes there. Compiles unchanged.
- [A capability that acts on a subscription is not found past a wrapper](#a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper).
  `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. Compiles unchanged.
- [A `CompetingConsumerListener` can be told the same thing twice, and after the call returns](#a-competingconsumerlistener-can-be-told-the-same-thing-twice-and-after-the-call-returns).
  A listener of your own on a MongoDB lease strategy can hear the same answer twice, a prohibition with no grant
  before it, and the change of a register, release or unregister after that call returned. Compiles unchanged.
- [The deprecated `accept(..)` on the projection feeds is removed](#the-deprecated-accept-on-the-projection-feeds-is-removed). Call `acceptAndWait(..)` instead, which
  `accept(..)` already forwarded to. `UpgradeToOccurrent_0_34` renames the calls, but only while the project still
  depends on 0.33.0 or 0.34.0. Breaks the build.
- [A catch-up needs the event store itself](#a-catch-up-needs-the-event-store-itself). A catch-up subscription model
  throws `IllegalArgumentException` when you construct it over a decorator, a test double or your own implementation of
  the event store interfaces. Compiles unchanged.

- [A catch-up keeps its live start in the checkpoints it stores](#a-catch-up-keeps-its-live-start-in-the-checkpoints-it-stores).
  0.34 refuses the new checkpoints, so a rolling upgrade or a rollback needs care. A catch-up that loses its live
  start on every replay now throws `CatchupCannotResumeException`. Compiles unchanged.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from).
  A blocking catch-up delivers each replayed event as a `CheckpointAwareCloudEvent`, which equals only another one
  with the same checkpoint. Store the checkpoint with your state to resume from it. **Breaks the build** if you
  `switch` exhaustively over `DcbStartAt` without a `default`.
- [A catch-up over both a stream and a DCB model refuses a position start without a filter](#a-catch-up-over-both-a-stream-and-a-dcb-model-refuses-a-position-start-without-a-filter).
  A subscription with no filter that starts at a position checkpoint throws `IllegalArgumentException`. Compiles
  unchanged.
- [A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it).
  A start after a position above 0 with no live start, `DcbStartAt.afterPosition(n)` among them, and a replay from
  above the beginning whose live start is lost throw `CatchupCannotResumeException`. Compiles with a removal warning.
- [A stream catch-up over an event store without positions replays from position 0](#a-stream-catch-up-over-an-event-store-without-positions-replays-from-position-0).
  `StartAt.checkpoint(GlobalCheckpoint.of(0))` replays the history where it used to go live, and a position above 0
  throws `UnsupportedStartAtException`. Compiles unchanged.
- [A MongoDB subscription model refuses a catch-up's checkpoint](#a-mongodb-subscription-model-refuses-a-catch-ups-checkpoint).
  A subscribe at a `GlobalCheckpoint` or a `CatchupTimeCheckpoint`, or at a dynamic start that returns one, is refused
  with `UnsupportedStartAtException` instead of opening at the present. Compiles unchanged, except a record pattern
  over `NativeMongoSubscription`.
- [A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).
  Every subscription a catch-up subscription runs on MongoDB, which in Spring Boot is every subscription by default.
  Compiles unchanged.
- [A blocking cancel doesn't wait for a subscribe whose `StartAt.dynamic` function runs](#a-blocking-cancel-doesnt-wait-for-a-subscribe-whose-startatdynamic-function-runs).
  A cancel made while the function runs returns first, and that subscribe subscribes nothing. Compiles unchanged.
- [A dynamic `StartAt.now()` over `ReactorDurableSubscriptionModel` fails when the wrapped model answers nothing](#a-dynamic-startatnow-over-reactordurablesubscriptionmodel-fails-when-the-wrapped-model-answers-nothing).
  `waitUntilStarted()` fails where 0.34.0 started the subscription from wherever the feed had reached. Over
  `ReactorMongoSubscriptionModel` it fails only when MongoDB's reply to `hello` has no `localTime`, where 0.34.0 didn't
  start it either. Compiles unchanged.
- [A dynamic `StartAt.now()` that the wrapped model refuses fails `waitUntilStarted()` instead of `subscribe(..)`](#a-dynamic-startatnow-that-the-wrapped-model-refuses-fails-waituntilstarted-instead-of-subscribe).
  A refusal of the subscribe, for an unsupported filter for example, no longer throws from `subscribe(..)`. Compiles
  unchanged.
- [A resumed MongoDB subscription waits for the action that ran when it was paused](#a-resumed-mongodb-subscription-waits-for-the-action-that-ran-when-it-was-paused).
  A resumed `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` subscription calls its action only once the
  call from before the pause has returned. Compiles unchanged.
- [A MongoDB subscription at the present is refused when the server gives it no position](#a-mongodb-subscription-at-the-present-is-refused-when-the-server-gives-it-no-position).
  On a server that sends no operation time, a `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel`
  subscription at the present reads the change stream once to record its position, and `waitUntilStarted(..)` and
  `start(true)` throw `UnsupportedStartAtException` when that read gives none. Compiles unchanged.
- [The reactor `HistoryLossReportingSubscriptions` gains a method](#the-reactor-historylossreportingsubscriptions-gains-a-method).
  **Breaks the build** if you implement the interface yourself.
- [A failure after the write doesn't run the command again](#a-failure-after-the-write-doesnt-run-the-command-again). A
  handler of a synchronous subscription that fails now makes `execute` throw, where a retry could run the command again.

### Spring Boot starter

- [A catch-up needs the event store itself](#a-catch-up-needs-the-event-store-itself). A context whose
  `EventStoreQueries`, event store or `DcbEventStore` bean is a decorator, a mock or your own implementation fails at
  startup when a catch-up runs over it. Compiles unchanged.

- [A catch-up keeps its live start in the checkpoints it stores](#a-catch-up-keeps-its-live-start-in-the-checkpoints-it-stores).
  A subscription with `resumeBehavior = SAME_AS_START_AT` goes live from the live start its catch-up read. Compiles
  unchanged.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from). A
  `SubscriptionResumePoints` bean says where each annotated subscription resumes. Compiles unchanged.
- [A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it).
  `startAtGlobalPosition` and `startAtDcbPosition` above 0 are refused at start, and deprecated for removal. Compiles
  with a removal warning.
- [A stream catch-up over an event store without positions replays from position 0](#a-stream-catch-up-over-an-event-store-without-positions-replays-from-position-0).
  `startAt = StartPosition.BEGINNING` replays the history over a store with stream position off, where from 0.30.0 to
  0.34.0 it got none of it. Compiles unchanged.
- [A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).
  `restart-on-change-stream-history-lost` no longer affects a subscription the starter runs through a catch-up
  subscription, which is every subscription by default. Compiles unchanged.
- [A Spring Mongo event store sends a commit again when MongoDB can't say whether it succeeded](#a-spring-mongo-event-store-sends-a-commit-again-when-mongodb-cant-say-whether-it-succeeded).
  The starters now create `CommitRetryingMongoTransactionManager` and `CommitRetryingReactiveMongoTransactionManager`,
  and a blocking event store built by hand, or a reactive one built from a `ReactiveTransactionManager`, logs a warning
  until it uses one. Compiles unchanged.
- [A restart follows `resumeBehavior` for `startAt = BEGINNING_OF_TIME` and `startAt = NOW`](#a-restart-follows-resumebehavior-for-startat--beginning_of_time-and-startat--now).
  On the reactive stack a restart no longer replays the history of a `BEGINNING_OF_TIME` `@StreamSubscription`, and a
  `NOW` subscription no longer misses the events written while the application was down. On the blocking stack `NOW` with
  `SAME_AS_START_AT` starts at the present on every restart and no longer competes with the other instances, so every
  instance is delivered every event. A reactive `NOW` subscription with the default `resumeBehavior` also asks the
  server where the feed is each time it registers and needs the answer only when no checkpoint is stored, so when it
  waits for its start, which is the default in `auto` subscription mode, the application does not start when no
  checkpoint is stored and the MongoDB server cannot answer. Compiles unchanged.
- [A subscription with no start position resumes from a catch-up's checkpoint](#a-subscription-with-no-start-position-resumes-from-a-catch-ups-checkpoint).
  On the blocking stack a subscription with `startAt` unset no longer skips the events written while the application
  was down when a catch-up's replay stopped before it went live. Compiles unchanged.

### Brokers

- [A catch-up over both a stream and a DCB model refuses a position start without a filter](#a-catch-up-over-both-a-stream-and-a-dcb-model-refuses-a-position-start-without-a-filter).
  `CloudEventForwarder.forward(subscriptionId, startAt)` and `DomainEventForwarder.forward(subscriptionId, startAt)` are
  refused at a position checkpoint over such a model. Compiles unchanged.

#### RabbitMQ

- [A RabbitMQ source consumes again after a recovery without topology recovery](#a-rabbitmq-source-consumes-again-after-a-recovery-without-topology-recovery).
  A source on a connection with topology recovery turned off registers a new consumer after a recovery. A source stops
  in three cases where it kept running in 0.34.0. Two log lines change their text. Compiles unchanged.

#### Kafka

### Snapshots

- [DCB snapshots resume from marker versions](#dcb-snapshots-resume-from-marker-versions). The DCB deciders and
  a `@Snapshot` DCB view save at marker versions, and ignore a snapshot 0.34.0 saved at a position. `Snapshot` gains two
  components. **Breaks the build** if you use a record pattern over `Snapshot`. A DCB application service with
  synchronous subscriptions on `noTransaction()` or a Spring executor no longer runs your domain function in its
  transaction executor's transaction outside a Spring transaction scope of yours. On the blocking service that holds only
  for a command that asks for marker versions, as a snapshot decider's does.
- [The reactive `executeAndReturnState` emits a non-null state](#the-reactive-executeandreturnstate-emits-a-non-null-state).
  The emitted state is declared non-null. Breaks the build only for a Kotlin `Mono<State?>` written by hand.

### The rewrite recipe

- `org.occurrent.MigrateDcbMarkerVersionsRecordPattern_0_35` adds the new bindings to a record pattern over
  `DcbAppendResult`, `DcbEventStream`, `DcbReadOptions` or `Snapshot`. See
  [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions) and
  [DCB snapshots resume from marker versions](#dcb-snapshots-resume-from-marker-versions).
- `org.occurrent.MigrateNativeMongoSubscriptionRecordPattern_0_35` adds the new binding to a record pattern over
  `NativeMongoSubscription`. See
  [A record pattern over `NativeMongoSubscription` stops compiling](#a-record-pattern-over-nativemongosubscription-stops-compiling).
- No recipe rewrites a `DcbConsistencyToken` or the streams that create one. See
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).

## A global position is no longer a saga redelivery key

A saga recognises a redelivered event by its `streamid` together with its `streamversion`, and by nothing else. From
0.31.0 to 0.34.0 it also recognised an event without a stream key by its `position`, and that skipped events it had
never applied. The reason is in [ADR 150](../architecture/decisions/0150-a-global-position-is-not-a-saga-redelivery-key.md),
and the report is [#1298](https://github.com/johanhaleby/occurrent/issues/1298).

### Who is affected

A saga that receives events with a `position` and no `streamid` and `streamversion`. Events read from an Occurrent
event store have both, so a saga fed only from the store is not affected.

A custom feed that drops the stream key is affected, and so is an event whose `streamid` or `streamversion` cannot be
read. 0.34.0 treated an unreadable value as absent.

### What changes

| The event | `REQUIRED` (the default) | `BEST_EFFORT` |
| --- | --- | --- |
| Has `streamid` and `streamversion` | Applied, a redelivery is skipped | Applied, a redelivery is skipped |
| Has only a `position`, or nothing | Refused with `SagaRedeliveryDetectionException` | Applied with a warning, and a redelivery runs the reactions again |
| Has a `streamid` or `streamversion` that cannot be read | Refused with `SagaRedeliveryDetectionException` | Refused with `SagaRedeliveryDetectionException` |

An event the saga applies without a key can no longer be quarantined, since quarantine identifies the event it stopped
on by its key.

### What to do

Forward `streamid` and `streamversion` from the feed, and the saga recognises its redeliveries.

If the feed cannot supply them, set `RedeliveryDetection.BEST_EFFORT` with `@Saga(redeliveryDetection = ..)` or
`SagaRunnerConfig.withRedeliveryDetection(..)`. Do that only when every command the saga issues is safe to receive
more than once.

If the exception reports an unreadable `streamid` or `streamversion`, fix the feed that wrote the value. Neither setting
lets the event through.

### Stored saga state

A saga instance saved by 0.31.0 to 0.34.0 may hold a `positionWatermark`. 0.35.0 loads it and ignores it, and the field
is gone from that instance's document after its next save. A document without the field reads as `null`, and
`SagaEnvelope.positionWatermark()` is deprecated.

A 0.34.0 node reads a document that 0.35.0 saved, and the other way round. A rolling upgrade works, and so does going
back to 0.34.0.

A failure record whose input is `position:N` was written for a position-only event. On an `ACTIVE` instance 0.35.0
treats it as no record, so the next failure starts a fresh budget and the next successful delivery clears it.

A `QUARANTINED` instance keeps its record and stays quarantined. This finds the ones that hold such a record:

```javascript
db.<collection>.find({ failureInput: /^position:\d+$/ })
```

The [runbook for a quarantined saga instance](../runbooks/quarantined-saga-instance.md) says what to do with them.

### Why there is no recipe

No signature changes, so there is nothing for `UpgradeToOccurrent_0_35` to rewrite. Whether a feed can supply the
stream key, or should use `BEST_EFFORT`, depends on the feed.

## A resume at a position through a `DurableSubscriptionModel` stores the checkpoint

In 0.33.0 and 0.34.0, `RepositionableSubscriptions.findIn(durableModel)` answered the model the
`DurableSubscriptionModel` wraps, since the durable model didn't implement `RepositionableSubscriptions` and the
lookup went past it. A `resumeSubscription(subscriptionId, startAt)` through that answer resumed the subscription at
`startAt` and stored nothing, so a restart before the next event resumed from the checkpoint stored earlier.

From 0.35.0 the lookup answers the `DurableSubscriptionModel` itself when the model it wraps can resume at a position,
and empty otherwise. Its `resumeSubscription(subscriptionId, startAt)` stores the position as the subscription's
checkpoint, then resumes the wrapped model there.

* `StartAt.checkpoint(..)` stores that checkpoint.
* `StartAt.now()` stores the position the wrapped model's `globalCheckpoint()` answers. When it answers `null`, the
  call throws `IllegalStateException`, nothing is stored and the subscription stays paused.
* `StartAt.subscriptionModelDefault()` resumes from the stored checkpoint, as `resumeSubscription(subscriptionId)`
  does.
* A subscription subscribed without checkpointing, or one the wrapped model doesn't report as paused, is passed on to
  the wrapped model and nothing is stored.
* A checkpoint the wrapped model's new `restartsAt(..)` answers `false` for is passed on to the wrapped model and
  nothing is stored. The method answers `false` unless the model overrides it. The MongoDB subscription models answer
  `false` for a `GlobalCheckpoint`, a `CatchupTimeCheckpoint`, a time and every other checkpoint their change stream
  can't open at. They open at the present when they subscribe at one, so a stored one would make a restart skip
  events.

The checkpoint is written with the same write condition as every other checkpoint the durable model writes. A node
whose write the checkpoint storage refuses, because another node has written with a newer lease, gets
`CheckpointWriteConditionNotFulfilledException` and the subscription stays paused. When the wrapped model's resume
throws after the write, the checkpoint stored before is put back with the write condition the position was stored
with and the exception is thrown, so a failed reposition leaves the stored position as it was before the call. When
nothing was stored before, the new checkpoint stays.

A catch-up model whose live delegate is a `DurableSubscriptionModel` now has the position stored when it resumes the
durable model at one. [Section 29 of the 0.34.0 upgrade guide](upgrading-to-0.34.0.md#29-a-blocking-subscription-that-resumes-from-another-nodes-catch-up-position-replays-from-it) says such a resume stores nothing until the first live
position or the quiet save. That section stays as it was released, and over a durable model it no longer holds.

There is nothing to change in your code unless it depends on the checkpoint staying where it was after a resume at a
position. A `DurableSubscriptionModel` over a repositionable subscription model you wrote yourself stores nothing on a
resume at a position, as in 0.34.0. To have the position stored, override `RepositionableSubscriptions.restartsAt(..)`
in that model to answer `true` for every checkpoint a subscription it subscribes there starts reading at. Nothing stored changes format, and there is no OpenRewrite recipe for this. See
[ADR 151](../architecture/decisions/0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md) and
[#1253](https://github.com/johanhaleby/occurrent/issues/1253).

## A capability that acts on a subscription is not found past a wrapper

In 0.33.0 and 0.34.0 the blocking `capability(type)` answered the model itself when it implemented `type`, and
otherwise unwrapped a `SubscriptionModelWrapper` and asked the model beneath. Every `findIn(..)` method calls it. So
`RepositionableSubscriptions.findIn(competingConsumerModel)` answered the model the `CompetingConsumerSubscriptionModel`
wraps, and a `resumeSubscription(subscriptionId, startAt)` through that answer ran the subscription while the competing
consumer still had it as paused and held no lease for it. A node that didn't hold the lease could then run the
subscription next to the node that did.

From 0.35.0 a wrapper is unwrapped only for a capability that a wrapper's own state doesn't depend on:

* `IntrospectableSubscriptions`, `ReplayAwareSubscriptions` and `HistoryRetainingSubscriptions`, which answer questions
  about subscriptions.
* `QuietPositionReportingSubscriptions`, which adds and removes a listener that is told positions.
* `Pushable`, which takes events in at the bottom of the stack.
* `HistoryLossReportingSubscriptions`, which a catch-up model reaches under a durable model.
* `StartRefusalReportingSubscriptions`, which a competing consumer model reaches under a durable or catch-up model,
  so it can give back the lease of a subscription whose start the MongoDB model refused.

Every other capability is found only on a model that implements it. That is `RepositionableSubscriptions`,
`Subscribable`, `CancellableSubscriptions`, `SubscriptionModelLifeCycle`, `SubscriptionModel`,
`CheckpointAwareSubscriptionModel`, `StreamSubscriptionModel` and `DcbSubscriptionModel`.

* `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. There is no way to resume a competing subscription at a position yet.
  Pause and resume it through the `CompetingConsumerSubscriptionModel`. Over a `DurableSubscriptionModel` the resume
  starts from the stored checkpoint. [#1304](https://github.com/johanhaleby/occurrent/issues/1304) decides how a resume at
  a position through a competing consumer should work.
* `capability(CheckpointAwareSubscriptionModel.class)`, or another model type, on a wrapper that doesn't implement it
  answers empty where it used to answer the model beneath. On the stack the Spring Boot starter builds, a
  `CompetingConsumerSubscriptionModel` over a `CatchupSubscriptionModel` over a `DurableSubscriptionModel`, it used to
  answer the `DurableSubscriptionModel`, and a `subscribe` through it went around both the catch-up and the competing
  consumer. Subscribe through the outermost model instead.
* A `SubscriptionModelWrapper` of your own is found for an acting capability only if it implements it. Implement it on
  the wrapper, update the wrapper's own state for the subscription and then forward to the model it wraps. The
  observing capabilities are still found through your wrapper without any change. A wrapper of your own that a
  `CatchupSubscriptionModel` or a `StreamCatchupSubscriptionModel` wraps has to implement `RepositionableSubscriptions`
  and forward to the model beneath. Otherwise the catch-up model treats it as a model that can't be resumed at a
  position, and resuming a subscription from a position the catch-up model stored throws `IllegalStateException`.

[Section 29 of the 0.34.0 upgrade guide](upgrading-to-0.34.0.md#29-a-blocking-subscription-that-resumes-from-another-nodes-catch-up-position-replays-from-it) says a resume at a position reached through `RepositionableSubscriptions.findIn(..)`
on a `CompetingConsumerSubscriptionModel` goes to the catch-up model under it. That section stays as it was released, and
from 0.35.0 there is no such resume.

The code compiles unchanged, and there is no OpenRewrite recipe, because the method signatures are the same and only
what the lookup answers changes. Nothing stored changes. See
[ADR 151](../architecture/decisions/0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md) and
[#1251](https://github.com/johanhaleby/occurrent/issues/1251).

## A `CompetingConsumerListener` can be told the same thing twice, and after the call returns

In 0.34.0 a `CompetingConsumerListener` added to `SpringMongoLeaseCompetingConsumerStrategy` or
`NativeMongoLeaseCompetingConsumerStrategy` was told each change of a lease once. A `registerCompetingConsumer`,
`releaseCompetingConsumer` or `unregisterCompetingConsumer` told the listeners of the change it made before it
returned, and threw what they threw. An `OutOfMemoryError` at the wrong moment could lose a grant or a prohibition for
good, which [#1349](https://github.com/johanhaleby/occurrent/issues/1349) fixes, and the fix needs both promises to go.

### Who is affected

Code that implements `CompetingConsumerStrategy.CompetingConsumerListener` itself and adds it to one of the two MongoDB
lease strategies with `addListener`. `CompetingConsumerSubscriptionModel` adds itself as a listener and handles
everything below, so a competing consumer built on it needs no change.

### What changes

* A listener can be told the same thing twice in a row. That can happen, for example, after an `OutOfMemoryError` cut a
  call short, when a lease is lost while the strategy works out what to tell, and in every round for as long as a
  listener keeps throwing an `Error`.
* A listener can be told a prohibition with no grant before it. A grant that was undone before the strategy got to tell
  it may never be told, but the prohibition after it is.
* A register, release or unregister may call the listeners before it returns, and throw what they throw, but only for
  the change it made itself and only while nothing else is calling them about that consumer, and anything else they
  are told, which can include that change, comes on a notifier thread, before or after the call returns, where what
  they throw is logged. A listener's `Error` that came out of `subscribe`, `resume` or `cancel` in 0.34.0 can end up
  only in the log, at ERROR.

### What to do

* Act on each call as "this is now true" rather than as "this just changed". On `onConsumeGranted`, start consuming
  unless the subscriber already does. On `onConsumeProhibited`, stop unless it has already stopped. A second call with
  the same answer then does nothing.
* Don't expect the listener to have been called once `registerCompetingConsumer`, `releaseCompetingConsumer` or
  `unregisterCompetingConsumer` returns. Use the `boolean` that `registerCompetingConsumer` returns, or ask
  `hasLock(subscriptionId, subscriberId)`, when the caller needs the answer at once.
* Don't count on catching what the listener throws around one of those calls. Handle the failure inside the listener,
  since on a notifier thread the strategy only logs it.

The code compiles unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.
See [ADR 156](../architecture/decisions/0156-a-refresh-round-tells-the-listeners-what-they-have-not-heard.md).

## A saga applies a stream version that arrives after a higher one

Up to 0.34.0 a saga kept the highest stream version it had applied for each stream, and skipped every later event at or
below it. That loses an event whenever a stream's events reach the saga out of order. `KafkaTopicPerTypeDestinationResolver`
orders one stream's events of one type and nothing across types, so a saga that listens to two event types of a stream
can see version 1000 of one type before version 2 of the other. The reason is in
[ADR 154](../architecture/decisions/0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md), and the
report is [#1305](https://github.com/johanhaleby/occurrent/issues/1305).

### Who is affected

A saga fed through a broker or a partitioner that does not keep a stream's events in version order. A saga fed straight
from an Occurrent event store, in order, behaves as before.

### What changes

The saga keeps the highest applied version per stream and event type. For an event of a stream:

| The event's stream version | What the saga does |
| --- | --- |
| Above the highest applied version of its event type | Applies it |
| Equal to that version, or one the saga recorded as applied | Skips it as a redelivery |
| Below it by less than the reorder window and not recorded | Applies it, since it arrived late |
| Below it by the reorder window or more, and not pending | Skips it and logs a warning once per stream |
| Pending, however far below it | Applies it and removes it from pending |

A reaction that ran for a late event runs once. A redelivery of that event is skipped.

An event is pending when it was handed to the saga and failed. The instance records its stream version before the
exception reaches the subscription, whether or not quarantine is on, so a broker that parks the event afterwards does
not lose it. An operator who replays the parked event after the instance has applied more than a window of later
versions still gets it applied, once. An instance keeps every pending version until it applies, and logs a warning
naming the stream and the type when a failure takes one stream's pending versions of one type past the window.

An instance that failed on an event under 0.34.0 or earlier holds that failure in its failure record, which names the
event but not its type. If another event fails before the first is offered again, the failure record is replaced, and the
instance keeps the first event's stream version in `legacyFailed` for its stream. The version is applied when it comes
back however far behind it is, and it is removed from the list once it is applied or fails again with its type known.

### The reorder window

The window is 256 by default. Change it with `SagaRunnerConfig.withReorderWindow(int)` when the feed reorders further:

```java
SagaRunnerConfig config = SagaRunnerConfig.defaults().withReorderWindow(1024);
```

A saga declared with `@Saga` always uses the default, since there is no annotation attribute or Spring property for it.
An event further behind than the window that did not fail in the saga is lost in the way it was lost before 0.35.0, and
the warning says which stream it came from. That is the precondition for exactly-once. The feed keeps a stream's order to
within the window per event type, or the event failed in the saga. Feeds that can reorder deeper than 256 versions are a
listener that delivers from several threads, a Kafka topic whose partition count was raised after events were produced,
and a RabbitMQ `prefetchCount` larger than the window. A larger window keeps more numbers in every instance. Raising the window later does not bring back what an
instance has already forgotten, because the `horizon` keeps every version at or below it skipped. The larger window
applies to the versions that arrive after the change.

### Stored saga state

A saga instance document gains a `streamMarks` field. It holds, per stream, a `floor` and a list of event types, and
each type has its highest applied `mark`, the `applied` versions below it and a `horizon`, the highest version it has
stopped remembering, and a `pending` list of event types with the `versions` of each that failed. A document without
`pending` reads as nothing pending. A `legacyFailed` list of stream versions, kept per stream beside `floor`, holds the
failures described above, and a document without it reads as an empty list. `pending` is a list rather than a map because
stream ids and event types can contain dots. An instance holds at most 256 `applied` versions per stream and event
type, so a document grows by some kilobytes for an instance that listens to a few streams, and every save rewrites them.

A document saved by 0.31.0 to 0.34.0, or by an earlier 0.35.0 build, has no `streamMarks`. It reads as a floor equal to
each stream's watermark, so everything the instance has already seen stays skipped, and a version above the watermark
is applied as before. Late versions are recognised from the instance's next save on.

`streamWatermarks` is still written with the highest version of each stream. A 0.34.0 node reads a document that
0.35.0 saved, ignores `streamMarks`, and replaces the document without it on its next save. The next 0.35.0 node then
reads it as an old document. A rolling upgrade works, and so does going back to 0.34.0, which loses late events on that
stretch as it did before.

### A record pattern over `SagaEnvelope` or `SagaRunnerConfig` stops compiling

A record pattern has to name every component of the canonical constructor. `SagaEnvelope` gains `streamMarks` as its
14th component and `SagaRunnerConfig` gains `reorderWindow` as its 6th:

```java
// Before, stops compiling
case SagaRunnerConfig(var pollInterval, var batchLimit, var maxCas, var detection, var quarantineAfter) -> ...

// After
case SagaRunnerConfig(var pollInterval, var batchLimit, var maxCas, var detection, var quarantineAfter, var reorderWindow) -> ...
```

The same applies to `SagaEnvelope`, which needs a `var streamMarks` after its `failure` binding, and to an
`instanceof` pattern as much as a `switch` case. `SagaEnvelope.StreamMarks` and `SagaEnvelope.TypeMark` are new types
and only matter to a `SagaStateStore` implemented outside this repository, which has to persist `streamMarks`. A store
that ignores it keeps working, and every instance then reads as it did before 0.35.0, so late versions are not
recognised.

Constructor calls are not affected. The 13-argument `SagaEnvelope` constructor and the five-argument
`SagaRunnerConfig` constructor stay, and fill in no marks and a window of 256. `SagaExecutionSupport.EventMeta` gains an
`eventType` component too, and it lives in an `internal` package.

### Why there is no recipe

`UpgradeToOccurrent_0_35` does not rewrite these patterns. A record pattern's arity is a compiler-enforced fact, so
the way `UpgradeToOccurrent_0_34` appends `var appendId` to a `WriteResult` pattern could be extended to these two
records, but the 0.34.0 change to the same two records got a guide entry and no recipe, and this follows that.

## A saga's quarantine budget runs per failing event

Up to 0.34.0 the quarantine budget belonged to the instance. The first failure wrote a `firstFailedAt`, and a later
failure of a different event kept it. Under a broker's PARK action a failure parked and forgotten days ago then made the
first failure of an unrelated event quarantine the instance. 0.35.0 gives each failing event its own clock. The reason
is in [ADR 157](../architecture/decisions/0157-a-sagas-quarantine-budget-runs-per-input.md), and the report is
[#1318](https://github.com/johanhaleby/occurrent/issues/1318).

### Who is affected

A saga with quarantine turned on through `SagaRunnerConfig.withQuarantineAfter(Duration)`, and a saga you turn it on for
while some of its events are failing. An event that failed while quarantine was off keeps the instant it first failed.

### What changes

The saga measures how long an event has been failing from the instant on its pending entry, or from the failure record's
`firstFailedAt` when the record names that event, or from now. So `SagaFailure.firstFailedAt()` is when the event the
record names first failed.

| Sequence | 0.34.0 | 0.35.0 |
| --- | --- | --- |
| Versions 7, 8, 7, 8 fail in turn | measured from the first failure | each measured from its own first failure, so 7 still reaches the budget |
| Version 7 fails and is parked, version 8 fails once a week later | 8 quarantines the instance on its first failure | 8 starts its own clock |
| Version 7 is parked, replayed after `quarantineAfter`, and fails again | measured from the first failure | quarantines the instance on that first replay failure |
| Version 7 fails while quarantine is off, quarantine is turned on a day later, and 7 fails again | a fresh budget from that failure | quarantines the instance on that failure when a day is past `quarantineAfter` |

The last two rows are intended, since the event has been failing for longer than the budget. Fix the cause before
replaying a parked event on an instance with quarantine on, or before turning quarantine on for a saga with failing
events.

### Stored saga state for the clock

Each pending version records when it first failed, in a `pendingSince` list beside `pending` in `streamMarks`. A
document without it falls back to the failure record's `firstFailedAt` for the event the record names, and any other
event starts its clock on its next failure.

A `SagaStateStore` implemented outside this repository has to save `StreamMarks.pendingSince()` and read it back in
`compareAndSave` and `find`, and in `compareAndSaveWithoutState` and `findWithoutState` too when it overrides them. A store that saves only the
other marks, or rebuilds them with the four-argument `StreamMarks` constructor, loses the instants. An instance whose
failures alternate between two events, such as versions 7, 8, 7, 8 of one stream, then starts each event's clock again
on every failure and never reaches its budget.

## The deprecated `accept(..)` on the projection feeds is removed

`accept(E)` and `accept(EventMetadata, E)` are gone from the blocking and the reactor `DomainEventFeed` and
`CatchupProjectionFeed`. 0.34.0 deprecated them for removal and made each forward to `acceptAndWait(..)`, as
[section 12 of the 0.34.0 guide](upgrading-to-0.34.0.md#12-a-projection-feeds-accept-waits-until-the-event-is-applied-and-fails-when-it-is-not)
describes. The removal is [#1276](https://github.com/johanhaleby/occurrent/issues/1276).

### Who is affected

Code that calls `accept(..)` on one of those four types. `accept(..)` on a `PushSubscriptionModel` or a
`CatchupThenPushSubscriptionModel` is a different method and stays.

### What to do

Run `UpgradeToOccurrent_0_34` while the project still depends on 0.33.0 or 0.34.0, and then move the dependency to
0.35.0. The 0.34.0 guide shows how to run the recipe. It renames both `accept(..)` overloads on the four types to
`acceptAndWait(..)`, and does not touch an `accept(..)` on any other type. The renamed call does what the deprecated one
did in 0.34.0. Section 12 of the 0.34.0 guide covers how that differs from 0.33.0.

`acceptAndWait(..)` first appears in 0.34.0. A project on 0.33.0 stops compiling after the rename until the dependency
moves up, so change the dependency right after the recipe has run.

If the dependency is already 0.35.0, the build fails on each `accept(..)` call and the recipe no longer finds it, so
rename each call to `acceptAndWait(..)` by hand. A broker listener that can have a message delivered again can call
`acceptOrDefer(..)` instead, which the same section covers.

### Why there is no recipe in `UpgradeToOccurrent_0_35`

`UpgradeToOccurrent_0_34` renames a call only when the compiler has resolved it to `accept(..)` on one of the four
feeds. Once the type has no `accept(..)`, the call resolves to nothing and the recipe does not change it.
`ProjectionFeedAcceptRenameTest` tests this against a feed that declares only `acceptAndWait(..)` and
`acceptOrDefer(..)`. A recipe that runs after the upgrade has no type to match on, so `UpgradeToOccurrent_0_35` has
nothing to add, and the four entries stay in `UpgradeToOccurrent_0_34`.

## A RabbitMQ source consumes again after a recovery without topology recovery

A `RabbitMqCloudEventSource` or `RabbitMqDomainEventSource` on a connection with automatic recovery and topology
recovery turned off stopped consuming after a recovery in 0.34.0. From 0.35.0 it registers a new consumer
once the recovery has finished and consumes again. The reason is in
[ADR 153](../architecture/decisions/0153-a-rabbitmq-source-registers-a-new-consumer-when-a-recovery-leaves-it-without-one.md),
and the report is [#1066](https://github.com/johanhaleby/occurrent/issues/1066).

Nothing in your code changes. If you restart the source after a recovery, or turned topology recovery on only to keep
the source consuming, you can stop doing that. With topology recovery on, the source keeps the consumer the client
registered and starts no second one.

A source now stops, with a line at `error`, in three cases where 0.34.0 kept it running. Closing its channel puts the
delivery back on the queue.

- **A `TrafficListener` or `MetricsCollector` throws while the source sends a negative acknowledgement for a held
  delivery.** The source can't tell whether it went out, and sending it twice makes the broker close the channel. In
  0.34.0 the next poll sent it again.
- **A negative acknowledgement or an acknowledgement fails with an `IOException` on a connection with automatic
  recovery, and a poll interval later the connection still hasn't dropped.** A `TrafficListener` or `MetricsCollector`
  written in Kotlin can throw one while the connection is fine. So can NIO, which throws `Frame enqueuing failed` on a
  connection a memory alarm has blocked, for example one a source shares with a sink that publishes during the alarm.
  In 0.34.0 the source kept running. It sent the negative
  acknowledgement again on the next poll, and left a delivery whose acknowledgement never went out unanswered, so at
  the default `prefetchCount` of one the broker sent the source nothing further.
- **Its channel is closed for good.** The broker closed the channel on its own, for example with
  `406 PRECONDITION_FAILED` after a delivery outlived `consumer_timeout`, or a connection without automatic recovery
  closed the client's own channel. In 0.34.0 a source holding a delivery tried to release it again and logged a warning
  on every poll, and one holding none consumed nothing and logged nothing. A source holding none stops too, and when the
  application closed the channel or its connection, the line is at `warn`. On a channel that wraps the client's own and
  isn't `Recoverable`, a connection close that amqp-client's default recovery-triggering condition accepts doesn't stop
  the source, as in 0.34.0, whether or not the connection under the wrapper recovers, since a recovery there may reopen
  the channel. The source logs a warning when it's built on such a channel, and if no recovery reopens it, the source
  stays up and consumes nothing.

An `IOException` because the connection dropped doesn't stop the source. It keeps running and consumes again after the
recovery.

The source judges recovery by amqp-client's default recovery-triggering condition, since the client doesn't expose the
one a connection was built with. If you give a `ConnectionFactory` a condition of your own with
`setConnectionRecoveryTriggeringCondition`, the source stays up and consumes nothing after a close your condition
rejects and the default accepts. [#1353](https://github.com/johanhaleby/occurrent/issues/1353) proposes letting you give
the source your condition.

Two log lines of the delivery failure policy have new text, and a search or an alert on the old one stops matching.

| Before | From 0.35.0 |
|---|---|
| `Parked delivery tag {} to exchange "{}" routing key "{}" and acknowledged the original; nothing consumed it.` | `Parked a copy of delivery tag {} to exchange "{}" routing key "{}" and sent the acknowledgement for the original.` |
| `Redelivered delivery tag {} with requeue; nothing consumed it.` | `Sent a negative acknowledgement with requeue for delivery tag {} after it failed.` |

Both are still logged at `warn`. After a recovery the client ignores an acknowledgement for a tag from before it, so the
old text said more than the source knew.

## The reactive `executeAndReturnState` emits a non-null state

`executeAndReturnState` on `ReactiveSnapshotDeciderApplicationService` and `ReactiveSnapshotDcbDeciderApplicationService`
returns `Mono<@NonNull S>` instead of `Mono<S>`. The state type `S` may still be nullable. The new signature
changes nothing at runtime. A `null` state still writes none of the command's events and fails with a
`NullPointerException`. See
[#1119](https://github.com/johanhaleby/occurrent/issues/1119).

### Who is affected

Kotlin code that writes the result type of the call by hand as `Mono<State?>`. Kotlin reads the annotation, and a
`Mono<State>` can't be assigned to a `Mono<State?>`. Java code, and Kotlin code that lets the compiler infer the type,
compile unchanged.

### What to do

Drop the `?` from the hand-written `Mono<State?>`. There is no OpenRewrite recipe, since the compiler reports every
place that needs it.

## DCB reads can resume from marker versions

On MongoDB a DCB append reserves its position before its transaction commits, so an append holding 41 can commit after
the append at 42. A read after position 42 never returns 41, and a decider or view that resumes from a position it saw
loses that append ([#1230](https://github.com/johanhaleby/occurrent/issues/1230)). 0.35.0 lets a read resume from
marker versions instead, as [ADR 148](../architecture/decisions/0148-dcb-snapshots-resume-from-marker-versions-not-positions.md)
describes.

What is new:

- `read(criteria, DcbReadOptions.withVersions())` reports `DcbEventStream.markerVersions()`, the epoch and version of
  each marker of the criteria, read at the same point in time as the events. It reports them only for a full forward
  read, with no limit, skip or position bound, and criteria other than `MatchAll`.
- A conditional append with a consistency token reports the same in `DcbAppendResult.markerVersions()`.
- `read(criteria, DcbReadOptions.afterVersions(saved))` returns the matching events appended after `saved`, including
  one that committed late below a position an earlier read saw. A failover can make it miss an event, as [What else a
  token holds on MongoDB](#what-else-a-token-holds-on-mongodb) says. When the store can't compare `saved` with its own
  markers, for example after they were pruned, it throws `DcbSnapshotInvalidException`, and you read the boundary in
  full. The versions hold the criteria they were read with, and a read on other criteria throws the same exception.
  Criteria are the same when they hold the same alternatives in any order, the rule a consistency token uses, so
  versions from `tags(a)` are refused on `tags(a).excludingTypes("Y")`, and versions from `types("A", "B")` on
  `anyOf(type("A"), type("B"))`.
- `DcbAppendConfirmation.isConfirmed(result)` answers whether an append is committed at majority, read outside any
  transaction of yours. The MongoDB stores and `InMemoryEventStore` implement it.

What changes on MongoDB without you doing anything:

- Every DCB append counts its marker increments in a new field `s`, gives each marker document an `epoch`, and writes an
  `mv` field into each event with the epoch and version of each of its markers. A read never returns `mv` as part of a
  CloudEvent. The store creates an index on `mv` at start, together with the other DCB indexes.
- A read that asks for versions runs in a short read-only transaction of its own on the primary. Its commit waits at
  most `markerVersionsCommitTimeout`, 5 seconds by default, a new setting in each MongoDB `EventStoreConfig`. When the commit fails,
  `withVersions()` returns the events without versions and `afterVersions(..)` throws `DcbSnapshotInvalidException`.
- `resumeFromMarkerVersions(false)` turns resuming off. No read or append reports versions, `afterVersions(..)` throws,
  and the store logs a warning at start. `s`, `epoch` and `mv` are still written, so turning it back on is safe.
- Inside a session or transaction of yours, such as Spring's `@Transactional` or a native ambient session, a read
  reports no versions and `afterVersions(..)` throws `DcbSnapshotInvalidException`. Your transaction may still abort,
  and versions that counted its increments would skip the append that takes those versions next. Read the boundary in
  full there.

The upgrade needs no manual step, provided nothing replaces the document of an event a 0.35 node wrote. That rules out
a 0.34 node's `updateEvent`, a 0.34 tool, or your own code replacing event documents, during a rolling upgrade, after a
rollback to 0.34 followed by a new upgrade, and from a tool that shares the event collection. Such an event loses its
`mv` and is no longer returned by `afterVersions(..)`. A full read still returns it. On a replica set, if that event
matches an append condition and was committed after the condition's read, the append no longer fails, though it should.
Replacing an event document isn't supported, and no setting turns this off. A store built on
`MongoTemplate.withSession(..)` or `ReactiveMongoTemplate.withSession(..)` doesn't support reading versions.

Three things break, and two of them are compile errors.

- A record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions` with the old number of components stops
  compiling. `UpgradeToOccurrent_0_35` adds `var markerVersions` to the first two, and `var versionsRequested`,
  `var afterVersions`, `var confirmsAppendBeforeSaving` and `var firstOperationOfTransaction` to the third.
- `DcbEventStream(List, long)` is removed, so a store or a test double that builds a stream with it stops compiling.
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with)
  says what to call instead.
- An `isEqualTo(...)` on a whole `DcbAppendResult` or `DcbEventStream` that a store returned still compiles, and fails
  when the store reported versions. Compare the components you mean instead. Every other existing constructor stays,
  and sets the new components to empty.

## A consistency token holds the criteria it was read with

A `DcbConsistencyToken` holds the criteria of the read that returned it, and a `DcbAppendCondition` throws
`IllegalArgumentException` when it's created with a token read with other criteria. In 0.34.0 the token was a bare
number that a condition could pair with any criteria. On MongoDB that number was a sum of marker versions, so a
condition with other criteria than the read compared sums over other markers, and could let an append through after an
event matching its criteria had committed. The in-memory store refused the same code. The change is
[#1228](https://github.com/johanhaleby/occurrent/issues/1228), and
[ADR 147](../architecture/decisions/0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md)
describes it.

### What changes

- `DcbConsistencyToken` is a final class instead of a record. A store creates one with
  `DcbConsistencyToken.of(criteria, version)`, and `criteria()` and `version()` read it back. The public constructor,
  `DcbConsistencyToken.of(long)` and `value()` are removed.
- `DcbEventStream(List, long)` and `DcbDomainEventStream(List, long)` are removed, since they create a token without
  criteria.
- `DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken)` takes its criteria from the token.
- `failIfEventsMatch(DcbCriteria, DcbConsistencyToken)` is deprecated. It still works, and throws
  `IllegalArgumentException` when the criteria aren't the same as the token's.
- `wholeStoreLock(DcbConsistencyToken)` throws `IllegalArgumentException` for a token not read with `DcbCriteria.all()`.

Two criteria are the same when they hold the same set of alternatives. `anyOf(a, b)` is the same as `anyOf(b, a)`, and
a `DcbCriterion` is the same as an `anyOf(..)` with only that criterion in it. Criteria with different alternatives
aren't the same, even when they select the same events, so `types("A", "B")` isn't the same as
`anyOf(type("A"), type("B"))`.

The check runs in the condition's constructor, before any store is called, so it works the same on every store, a
`DcbEventStore` of your own included. `GenericDcbApplicationService` reads and appends with the same criteria on both
stacks, so it isn't affected on a store that returns the criteria it was given.

### Who is affected

- **A `DcbEventStore` of your own.** It stops compiling where it calls `new DcbConsistencyToken(..)`,
  `DcbConsistencyToken.of(long)`, `value()` or `new DcbEventStream(events, head)`.
- **Code that creates a token, or a stream with a two-argument constructor**, such as a test double that returns a
  `DcbEventStream` or a `DcbDomainEventStream`. It stops compiling the same way. A stream created with a token you
  already hold, `new DcbEventStream(events, head, token)`, still compiles. A call to `value()` stops compiling too, and
  so does a record pattern over `DcbConsistencyToken`, since the token is no longer a record.
- **A `DcbEventStore` decorator that rewrites criteria**, adding a tenant tag for example, and passes the inner store's
  token through. It compiles, but the token holds the rewritten criteria, so a condition on the caller's criteria throws
  `IllegalArgumentException`. In `read`, return
  `new DcbEventStream(inner.events(), inner.lastSequencePosition(), DcbConsistencyToken.of(criteria, innerToken.version(), innerToken.markers()), inner.markerVersions())`,
  since the three-argument constructor sets `markerVersions` to empty. Those versions hold the rewritten criteria's
  markers, the added tag's included, so pass them back to the inner store unchanged with the rewritten criteria. A store
  reports no versions for `all()`, on a read or on a conditional append, and throws `DcbSnapshotInvalidException` on a
  read of `all()` with `afterVersions`. So when `all()` becomes a tag, use the three-argument constructor on that read,
  drop the versions from that append's result, and throw on that read yourself. In `append`, rewrite the criteria the
  same way, and give the inner store a condition with
  `DcbConsistencyToken.of(rewrittenCriteria, token.version(), token.markers())`. On a sharded cluster the added tag then
  counts in the check like any other tag, so appending any event with that tag makes the check refuse, whether or not
  the event matches.
- **Code that reads with one criteria and appends with another**, passing the token the read returned. Creating the
  condition throws `IllegalArgumentException` on every store. `failIfEventsMatch(criteria, token)` compiles with a
  deprecation warning, and `wholeStoreLock(token)` and `new DcbAppendCondition(criteria, Optional.of(token))` compile
  with no warning, so the compiler doesn't find all of this code.
- **Code that saved a token value**, in an ETag or a queued command for example. A token value saved under 0.34.0 isn't
  valid on 0.35.0.

### What to do

In a store, pass the read's criteria when you create the token, and read `version()` where you read `value()`:

```java
return new DcbEventStream(events, head, DcbConsistencyToken.of(criteria, version));
```

Compute the version the same way you computed the value for `DcbConsistencyToken.of(long)`. `DcbEventStream(List, long)`
used the head as the token, so `new DcbEventStream(events, head, DcbConsistencyToken.of(criteria, head))` does what it
did. For a `DcbDomainEventStream`, pass the token of the `DcbEventStream` it was built from. A test double with no
`DcbEventStream` behind it creates the token with `DcbConsistencyToken.of(criteria, version)`.

Build a conditional append from the token alone:

```java
DcbEventStream stream = eventStore.read(criteria);
// decide on stream.events()
eventStore.append(newEvents, DcbAppendCondition.failIfEventsMatch(stream.consistencyToken()));
```

Where you passed the criteria the read was given and the token separately, drop the criteria. If the append really needs
other criteria than the read, read with those criteria before you decide, and decide from that read. A token read after
the decision can hide an event committed in between, so don't swap in a fresh token for the one the decision was made with.

Don't save a token value to use later. Read again when the command runs, and decide from that read. That includes a
client's ETag. Replacing a stale ETag with a fresh token makes the append check against a read the client's decision
wasn't made from. To refuse a client's stale write, keep a version of your own in your events, and compare the client's
ETag against it in the decision.

### Why there is no recipe

A recipe could rewrite `value()` to `version()`. That rename would compile, and it would keep a token value saved under
0.34.0 in use, although such a value isn't valid on 0.35.0. `DcbConsistencyToken.of(criteria, version)`
needs the criteria the read was given, and a recipe can't tell from the call site which criteria those were. Rewriting
`failIfEventsMatch(criteria, token)` to `failIfEventsMatch(token)` would compile too, but a condition that used other
criteria than its read would then append on the read's criteria instead of throwing.

### What else a token holds on MongoDB

On MongoDB, a token also holds what the read saw of each marker. On a replica set, an event a 0.35.0 node appended that
doesn't match a condition's criteria no longer fails it, so you'll see fewer `DcbAppendConditionNotFulfilledException`s
on a busy tag. A condition can still fail with no matching event, for example after a marker was deleted, when a
0.34.0 node incremented a marker, or when the read saw a marker only 0.34.0 had written. On anything else, such as a
sharded cluster, this doesn't apply, and any event that moves a marker of the criteria still fails it.

A condition can also miss a matching event when the read's token covers that event and the read's events don't hold it.
Through a mongos, a plain read can do this when the markers and the events are on different shards, and a read with
`withVersions()` doesn't when its stream reports `markerVersions()`, because it read both in one snapshot transaction.
On any deployment, a read whose events are behind its token can do this, for
example when a read preference or a read concern sends the events read to a lagging secondary or to an older snapshot. A
token also doesn't depend on a read's limit, skip or position range, so after such a read a condition doesn't fail for a
matching event the read left out. 0.34.0 has the gap this paragraph describes too, except that it refused the append
when the read caught only some of the event's markers, or when a later event moved a marker of the criteria, even one
that didn't match. On 0.34.0 a failover can cause it as well, when it loses a position reservation made below majority.
So can a rollback of your transaction on the Spring and reactor stores, when the store's own transaction manager uses
the same database factory as yours and its transaction is set to `REQUIRES_NEW` or `NOT_SUPPORTED`. The event then
commits above the last reserved position. 0.35.0 reserves positions outside every transaction and waits for majority on
the reservation, as [the next section](#an-append-on-mongodb-waits-for-majority-on-its-position-reservation) says.

If you delete marker documents from the `_dcb_checkpoints` collection, delete one only in a transaction that also
finds no event with its tag or type, and not during a rolling upgrade. On a sharded cluster, run that transaction at
read concern snapshot. Earlier versions said pruning during quiescence was harmless. It isn't, because a token can be
held across the prune.

A `DcbEventStore` of your own needs no change, and `DcbConsistencyToken.of(criteria, version)` keeps working for it.
A MongoDB store throws `IllegalArgumentException` for a token built that way.

## An append on MongoDB waits for majority on its position reservation

An append on a MongoDB event store reserves its positions with an update of a counter. That update now runs outside
every transaction, one of yours included, and waits for write concern majority, whatever write concern you configured.
A failover could lose an update made below majority while the append's event committed above the counter, and reads
then missed the event.

### What changes

A w:1 on the client, on the database or as the server's default no longer applies to this update, and neither does a
`wtimeout` you configured. The update waits for majority for at most `markerVersionsCommitTimeout`, 5 seconds by
default, the setting that also bounds the commit of a read that asks for versions. When a majority doesn't acknowledge
the update in that time, the append throws and writes no event, and the positions it reserved stay unused. On a client
with a `timeoutMS` above 0, where the driver sends no `wtimeout`, your `timeoutMS` bounds the update instead. A
`timeoutMS` of 0 waits forever, so there `markerVersionsCommitTimeout` bounds the whole update, the wait to select a
primary included. The rest of the append keeps the write concern it had.

Every append waits for as long as a majority takes to replicate the update. On MongoDB 5.0 or later, on a set without an
arbiter and with no write concern configured, the update already waited for majority. What changes there, unless your
client sets a `timeoutMS` above 0, is that a wait that used to have no limit now fails the append after
`markerVersionsCommitTimeout`. On a local three-member set at w:1, a single writer's latency stayed within the noise
between runs. With 8 writers, throughput went from about 990 to 1030 appends a second on the native store, 1050 to 875
on the Spring store and 990 to 670 on the reactor store. These appends ran outside a transaction, and all members ran on
one host, so the numbers show no network. Across a network, expect each append to wait about one more round trip to a
secondary.

### Inside a transaction of yours

On the Spring and reactor stores, the update used to join a transaction already open when the append started, and
committed with it. It now commits on its own, before the append writes its event.

- After a rollback of your transaction, the reserved positions stay unused. Positions already have gaps, so nothing that
  reads them has to change.
- Two appends in transactions of yours no longer conflict on the counter, only on a marker or a partition stream they
  share. An append that loses throws a `DataIntegrityViolationException` or a `TransientDataAccessException`, and your
  transaction has to be retried. `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` retry it when they
  opened it outside a Spring transaction scope of yours.
- In a Spring scope of yours with no transaction, a read through the store's template can bind a MongoDB session to
  that scope. A stream `write` or a DCB append in that session no longer retries a conflict, since the conflict aborted
  the session's transaction. It fails at once, as in your transaction.
- On a replica set, with transaction synchronization active, as it is by default, a read in your transaction reports, in
  `lastSequencePosition()`, the head as of the first operation in your transaction's MongoDB session on the store's
  database factory. It also returns your transaction's own appends, and those it started after that operation are at
  positions above that head. If you keep that position and read after it later, you get those events again. On a sharded
  cluster, unless your transaction reads at read concern snapshot, the shards aren't guaranteed to be read from one
  snapshot, so the head and the events can come from different points in time.
- An append joins your transaction when that's a Spring-managed MongoDB transaction on the template's database
  factory, and the `TransactionTemplate` or `TransactionalOperator` you give the store, if any, keeps the default
  propagation, `REQUIRED`.
  [ADR 149](../architecture/decisions/0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md)
  lists what happens in other setups.

### What to do

On a replica set of a primary, a secondary and an arbiter with the secondary down, a majority can't acknowledge the
update. In a probe on all three stores, appends there used to complete in 6 to 18 ms. The store now bounds the update
by `markerVersionsCommitTimeout`, so such an append throws after 5 seconds by default. To wait longer or
shorter, set `markerVersionsCommitTimeout` in the store's `EventStoreConfig`. On a client with a `timeoutMS` above 0
that setting doesn't bound the update, and the client's `timeoutMS` does.

## Criteria with tags conflict on their tags

On the MongoDB stores, the check of a conditional append with a token reads only the tag markers of an item with tags,
and no longer the markers of its types. Sharing a type with such an item is then no longer a reason for the check to
refuse. Such an item also no longer adds its type markers to what a conditional append increments.
Say you read `DcbCriteria.type("SeatReserved").tags(Tag.of("show", "1"))`, and another command appends a
`SeatReserved` tagged `show:2`. A conditional append with the read's token used to throw
`DcbAppendConditionNotFulfilledException`, and now succeeds. The in-memory store already answered that way for this
example. On a sharded cluster the MongoDB stores can still refuse an append the in-memory store accepts, for example
after an event of another type that has one of the criteria's tags. On a replica set that event no longer refuses
it, as [What else a token holds on MongoDB](#what-else-a-token-holds-on-mongodb) says. With a token, an item without
tags still reads its type markers. The change is the first part of [#1229](https://github.com/johanhaleby/occurrent/issues/1229), and
[ADR 147](../architecture/decisions/0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md)
describes it.

### What stays the same

- A conditional append on criteria with tags is still refused when an event that matches them committed after the
  read. On a replica set the check reads the events as they are when the append runs, so an event deleted by then, or
  changed so that it no longer matches, may not fail it. A read can also miss such an event and let the append through,
  as [What else a token holds on MongoDB](#what-else-a-token-holds-on-mongodb) says.
- Every append still increments a marker for each type and each tag of its events, so two appends of one type still
  can't commit at the same time on MongoDB, whatever their tags.
- This change adds nothing on disk, so it doesn't stop 0.34.0 and 0.35.0 nodes appending to the same store, within the
  limits in [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). A conditional
  append from a 0.34.0 node still increments the type markers of its criteria, also for an item with tags. A conditional
  append from a 0.35.0 node doesn't add them for such an item. A 0.35.0 check reads a type marker only for an item
  without tags, so a 0.34.0 increment can make that check refuse an append and the command retry, and it can't let an
  append through. This change doesn't stop you going back to 0.34.0 either, within the same limits.

### Who is affected

- **Code that counted on the refusal.** A command whose conditional append failed because an event of the same type
  with other tags was appended can now succeed. If the decision depends on every event of that type, read with criteria
  that hold only the type. A conditional append on `type("X").tags(a)` that appends an event of another type also no
  longer makes an append with a token read on `type("X")` alone refuse, since it no longer increments that type marker.
  When many commands append one type with different tags, an append that used to be refused can now wait for seconds in
  the store's retries of a write conflict. When those retries run out, it fails with the store's write conflict exception,
  `DataIntegrityViolationException` on the Spring stores, and not with `DcbAppendConditionNotFulfilledException`, so the
  default retry of `GenericDcbApplicationService` doesn't retry it.
- **A `DcbEventStore` of your own** that checks conditions on markers, as the MongoDB stores do. The TCK reads
  `type("X").tags(a)`, appends an `X` tagged only `b`, and then appends with the read's token. That append has to
  succeed, so a store that still reads the type marker for an item with tags fails the case.
- **Code that switches over the TCK's `DcbAppendConditionModel`.** The enum gains `MARKERS_THEN_EVENTS`, which the
  fixtures of the MongoDB stores declare, so a `switch` that lists every case and has no `default` stops compiling until
  it handles the new one. A fixture that returns `EXACT_CRITERIA` or `TAG_MARKER` still compiles.
- **Code that saved a token value.** For an item with both types and tags, a 0.34.0 token value is a sum over its tag
  and type markers, and 0.35.0 no longer counts the type markers of that item. An old value can then let an append through
  that should have been refused. Read again instead, as
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with)
  says.
- **Code or an operator that deletes marker documents.** Follow the rule at the end of
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).

## The default transaction executors retry on a MongoDB error label

The default `SpringTransactionExecutor` and `SpringReactiveTransactionExecutor` retry a transaction they opened outside
a Spring transaction scope of yours when it loses a conflict. In 0.34.0 that meant any `DataIntegrityViolationException`, because Spring translates MongoDB's write
conflict to it. That also matched a duplicate key and a validation failure, which no new transaction can get past, so
the unit of work ran 15 times on the blocking executor and 16 on the reactive one before the error reached the caller. The 0.35.0 budget is 240 attempts, so the same
predicate would have run it for two minutes.

0.35.0 retries a `TransientDataAccessException`, or any exception with the `TransientTransactionError` label anywhere
in its cause chain. MongoDB puts that label on a write conflict, and the driver's own `withTransaction` retries on it.
A duplicate key and a validation failure inside the transaction now run the unit of work once and reach the caller.
A commit that fails with the `UnknownTransactionCommitResult` label is not retried either, since the commit may have
gone through, even when the same error also has the `TransientTransactionError` label. The 0.34.0 predicate did not
retry it.

The two executor modules now depend on `mongodb-driver-core` as an optional dependency and look for the label only when
`com.mongodb.MongoException` is on the classpath. Without the driver you still get the executor, and it still retries a
`TransientDataAccessException`. You add nothing.

### Who is affected

Two cases changed. The first retried in 0.34.0 and doesn't now. A unit of work that writes outside the transaction, for example through a
`MongoTemplate` with `SessionSynchronization.NEVER`, can get a duplicate key that MongoDB does not label. Say it checks
whether a document exists, a rival inserts it, and the write then fails with the duplicate. The second run saw the
rival's document and skipped the write, so the command succeeded. In 0.35.0 it fails with the `DuplicateKeyException`
after the first run.

The second did not retry in 0.34.0 and does now. A commit that fails with the `TransientTransactionError` label reaches
the executor as a `TransactionSystemException` with a `MongoException` cause, which 0.34.0 did not match. The executor
now runs the unit of work again, up to three runs in all, which is safe when the transaction was rolled back, as that
label says it was. A failover and a transaction that outlives `transactionLifetimeLimitSeconds` fail the commit this way,
and a second run gets past both.
The case to watch is a unit of work that catches a duplicate key from a write inside the transaction and returns normally.
MongoDB has aborted the transaction by then, so the commit fails with the label and the executor runs the unit of work
three times, where 0.34.0 failed once. The third failure is a `TransactionSystemException` whose message says the commit
failed with `NoSuchTransaction` after 3 failed commits.

An executor you build with a retry of your own uses that retry as given, as before, so it has no cap on failed commits.
An executor built on a template or operator that opens no transaction has no commit to fail, so the cap never applies to it.

### What to do

Handle the duplicate in the unit of work only when its write is outside the transaction, and let a duplicate from a write inside the transaction reach the caller instead of catching it. Or make the write through a template that joins the transaction, so that a
rival's insert surfaces as a labelled conflict and is retried.

### In a Spring scope of yours with no transaction

In a Spring transaction scope of yours that has no transaction, such as a `TransactionTemplate` on
`PROPAGATION_SUPPORTS`, `SpringTransactionExecutor` no longer runs a unit of work again after it ran in your scope's
synchronization. The conflict reaches you, as it already did with `SpringReactiveTransactionExecutor`. A MongoDB session
that a read binds to your scope stays bound until your scope ends, so a second run would use the session the conflict
aborted. With a retry policy that isn't a `RetryStrategy.Retry`, the executor runs the unit of work once whenever your
scope is active. To get the retry back, retry around your scope, so the next attempt binds a new session.

## DCB snapshots resume from marker versions

`SnapshotDcbDeciderApplicationService`, `ReactiveSnapshotDcbDeciderApplicationService` and a `@Snapshot` view of a
`DcbSnapshotView` save a DCB snapshot at the marker versions of its boundary, and then read only the events appended
after them. In 0.34.0 they saved the position of the last event and lost an append that committed late below it
([#1230](https://github.com/johanhaleby/occurrent/issues/1230)). The reads they make are the ones
[DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions) describes.

### What changes

- A decider executes with `DcbExecuteOptions.fromVersions(saved, position)`, or `withVersions()` when it has no snapshot,
  and saves at the versions and the last position its append reported. It saves only when `DcbApplicationService.isAppendConfirmed(..)` returns
  `true`. `GenericDcbApplicationService` asks the event store, and returns `false` when the store doesn't implement
  `DcbAppendConfirmation`.
- A `@Snapshot` DCB view reads the events after the saved versions on each matching event, and saves at the versions of
  that read. `everyNEvents` counts the events of that read.
- When the store refuses to read after the saved versions, for example because a marker was pruned, the decider and the
  view read the whole boundary from the initial state and save a new snapshot.
- They do the same when that read returns an event below the highest position folded into the snapshot. It committed
  late, and folding it last could give another state than a read in position order.
- When the store reported no versions, or didn't confirm the append, the snapshot isn't saved. The first skip for each
  reason logs a warning, and later ones log at debug.
- A boundary reports no versions until each tag of an item with tags, and each type of an item without tags, has a
  marker. A decider reads the whole boundary, an attempt whose read reported no versions saves nothing, and its append creates the missing markers
  and gives the others an epoch, so its next command can save. A `@Snapshot` view never appends, so it reads the whole
  boundary until other appends have done that for every marker. 0.34.0 saved at a position there.
- A `@Snapshot` view over `DcbCriteria.all()` keeps no snapshot and logs a warning at start, since a read of every event
  has no marker versions. A snapshot 0.34.0 saved for it stays in the store and stops changing, so delete it or give the
  view a boundary. `dcbSnapshotView { }` in Kotlin uses `DcbCriteria.all()` unless you set tags or criteria. A decider over
  `DcbCriteria.all()` saves no snapshot either, so every command reads every event of the store, and the first skip logs
  a warning.
- A custom key function on a decider should stay keyed on the criteria. If you widen the criteria and keep the key, and
  the marker keys stay the same, for example when you change only the types of an item with tags, the next execute
  resumes after the saved versions and doesn't read the older events the wider criteria now matches, and nothing reports
  it. A widening that changes the marker keys is refused, and the decider reads the whole boundary.
- Inside a transaction of yours a read reports no versions, so a decider that runs in one reads the whole
  boundary and saves no snapshot. This is a known limitation, tracked in
  [#1307](https://github.com/johanhaleby/occurrent/issues/1307).
- A DCB application service with synchronous subscriptions reads and decides inside its transaction executor, as in
  0.34.0. When the executor opens the transaction, the service tells the store through `DcbReadOptions` that its read
  is the first operation of that transaction, and a MongoDB store reports the marker versions, so a decider there saves
  snapshots. On a replica set the store reads them in that transaction. On a sharded cluster it reads them in a
  read-only transaction of its own, on a second session, and the decider's append still runs in the executor's
  transaction.
- On a Spring executor that opens no transaction, outside a Spring transaction scope of yours, a Spring MongoDB store
  on a replica set reads them in the session transaction its read binds. On a sharded cluster it reads them in a
  transaction of its own and then binds a session transaction. The decider's append joins the bound transaction, so a
  failing handler undoes the append.
- In a Spring scope of yours with no transaction, on an executor that opens none, the service doesn't mark the read,
  so with no session bound for the store's database factory yet, a decider there reads in a transaction of the
  store's own, and its append runs in one too, which the store retries on a conflict and which doesn't
  join your scope, so a failing handler doesn't undo it.
- In a Spring scope of yours with no transaction and a session already bound there, a read that asks for marker
  versions, such as a snapshot decider's, reads in a transaction of the store's own until the store writes in that
  session, with an append for example. After that it reads in that session and gets no versions, and a read after
  marker versions is refused there. The store doesn't see writes to event or marker documents that go through your
  template in that session, so it relies on there being none.
- A Spring MongoDB store no longer retries a DCB append or a stream write in a session bound to the synchronization. A
  conflict fails it at once, as in your transaction, instead of a retry in the transaction the conflict aborted.
- `DcbReadOptions.confirmingAppendBeforeSaving()` and `asFirstOperationOfTransaction()` say something about the caller
  that the store can't check. Set them on a read of your own only when you keep the contract their javadoc states.
- In a transaction you own, on a blocking service, an exception from your domain function no longer marks that
  transaction rollback-only when the executor joins it and its `isTransactional()` answers `true` there.

### Stored snapshots

- `Snapshot` gains two components, `Optional<DcbMarkerVersions> markerVersions` and `long highestFoldedPosition`. The
  three-argument constructor stays and sets them to empty and 0, and `Snapshot.atMarkerVersions(..)` creates a DCB
  snapshot, whose `version` is 0.
- `SpringMongoSnapshotStore` and `ReactiveSpringMongoSnapshotStore` store a DCB snapshot with a `markerVersions` document,
  which holds the criteria the versions were read with and their entries, a `highestFoldedPosition` field and no
  `version` field. A stream snapshot is stored as before.
- A DCB snapshot that 0.34.0 saved at a position is never used. After the upgrade an execute or delivery reads the whole
  boundary until one can save over it, so nothing needs migrating. On a boundary whose markers 0.34.0 wrote, a decider's
  append gives them an epoch, and its next command can save.
- A 0.34.0 node that reads a snapshot 0.35.0 saved, during a rolling upgrade or after a rollback, finds no `version`. It
  reads the whole boundary and saves a snapshot at a position, which 0.35.0 then doesn't use.
- A snapshot store of your own has to persist `Snapshot.markerVersions()` and `Snapshot.highestFoldedPosition()`. If it
  drops the versions, every execute and delivery reads the whole boundary. If it loads a lower position than it was
  given, a resumed state can differ from a read in position order.

### What to do

- If you have a `DcbApplicationService` of your own that wraps another one, override `isAppendConfirmed(..)` and
  delegate it. Without that the deciders save no snapshot. Pass the domain function on as it is, and to that one service
  only, since a decider knows a refusal of its own read by that function. If you wrap it, a refused read after a conflict
  reaches the caller instead of reading in full. If you give it to a second service as well, a refused read there can
  run the command again.
- Replace `DcbExecuteOptions.fromPosition(..)`, which is deprecated for removal, with `fromVersions(..)`. Save the
  `DcbAppendResult.markerVersions()` and `lastSequencePosition()` of an execute with `withVersions()` and pass both on
  the next one.
- `DomainEventQueries.afterPosition(..)` is deprecated, since a position isn't a safe place to resume from on MongoDB.
  To process every event, resume a catch-up subscription from the checkpoint it stores.
- A record pattern over `Snapshot` with three bindings stops compiling. `UpgradeToOccurrent_0_35` adds
  `var markerVersions, var highestFoldedPosition`.
- A `SnapshotPolicy` of your own that reads `SnapshotDecision.newVersion()` gets 0 from a DCB decider, where 0.34.0 gave
  it the position of the append. Use `eventsSinceSnapshot()`.

## A failure after the write doesn't run the command again

With `noTransaction()` and no transaction of yours, a synchronous subscription runs after the write has committed. In 0.34.0, a handler that threw what
the application service's retry strategy retries on made the service read, decide and write again. By default that's
`WriteConditionNotFulfilledException` or `DcbAppendConditionNotFulfilledException`, so a handler that failed once stored the
events twice and `execute` returned normally. A retry strategy that matches `RuntimeException` did the same for any handler
failure and for a failed re-read of the written events. It applies to the stream and the DCB `GenericApplicationService`,
blocking and reactive.

### Who is affected

You use synchronous subscriptions and the attempt doesn't open a transaction. That's `noTransaction()`, the default, an
executor of your own that overrides neither `isTransactional()` nor `inTransactionTellingIfOpened`, or that answers a fixed
`true`, a Spring executor on `PROPAGATION_NOT_SUPPORTED`, `PROPAGATION_SUPPORTS` or `PROPAGATION_NEVER`, and an executor that
joins a transaction already open, whether yours or an outer service's. A Spring executor that opens its own transaction
under yours, on `PROPAGATION_REQUIRES_NEW` or on another transaction manager, isn't affected. A service without synchronous
subscriptions behaves as before.

### What changes

- `execute` fails, and the service's retry strategy doesn't run the command again. On the reactive stack the caller
  gets the original failure. On the blocking stack a strategy made by the factory methods of `RetryStrategy` still
  gives the failure to its `onError` listener and throws it as its `mapError` maps it, as for a failure its predicate
  doesn't match. A strategy of your own that ignores the shutdown predicate gets an empty attempt from any later run,
  before it reads or writes. When that strategy returns, the failure is thrown, and when it throws, the caller gets
  what it throws.
- The `sideEffect` doesn't run.
- The retry strategy can still run the command again for a failure of the write itself, and for any failure when the
  attempt opened a transaction. The
  service asks through `inTransactionTellingIfOpened`, a new default method on `TransactionExecutor` and
  `ReactiveTransactionExecutor`. The Spring executors answer from the transaction status, and the default infers it from
  `isTransactional()` answering `false` before the executor and `true` inside it. The service can't see whether the store
  joined, so a store that doesn't join keeps the first write and the retry writes again. The native MongoDB store under
  `SpringTransactionExecutor` does that.
- The service asks `isTransactional()` outside the executor too. An executor whose answer fails when no transaction is open
  makes an `execute` with synchronous subscriptions fail before the write.
- An executor that opens a transaction and overrides neither `isTransactional()`, which answers `false` by default, nor
  `inTransactionTellingIfOpened` still sees an exception pass through and can roll back, but the command no longer runs again. A rollback rule keyed on the
  handler's exception type sees a different exception now.
- `NativeMongoTransactionExecutor.isTransactional()` answers whether the session bound to the thread has a transaction in
  force, where it used to answer `true` always.

### What to do

- If you catch nothing around `execute` because a retry used to hide a handler's failure, handle it now. What's stored
  by then depends on your executor, its propagation and the store, so read it back before you run the command again.
  With `noTransaction()` and no transaction of yours the events are stored. With a Spring executor over the Spring
  MongoDB stores a write to a stream often rolled back, even on `PROPAGATION_NOT_SUPPORTED`, `PROPAGATION_SUPPORTS` or
  `PROPAGATION_NEVER`, and there a retry that used to work is gone. When `execute` runs outside every Spring transaction
  scope of yours, a DCB append on `PROPAGATION_NOT_SUPPORTED` rolls back as a write to a stream does, and a snapshot
  decider's does too. In a Spring scope of yours on `PROPAGATION_SUPPORTS` or `PROPAGATION_NOT_SUPPORTED`, which opens
  no transaction, the append of a plain `execute` rolls back too when you don't catch the failure inside that scope.
  [ADR 165](../architecture/decisions/0165-a-failure-after-the-write-is-retried-only-in-a-transaction-the-attempt-opened.md) has a table of the cases I ran.
- If your executor opens a transaction, override `isTransactional()` so the retry stays. Answer for the moment of the call,
  so `false` until the transaction is open. An executor that answers a fixed `true` counts as joining one. If it can open
  one with another already open, override `inTransactionTellingIfOpened` too, since the default counts that as joining.
- If a retry predicate was how a handler's work got done again, retry inside the handler, or use a transaction so the
  write and the handler commit together.
- If a handler on a service with no transaction writes to a stream through a service with a Spring executor on one of
  those three propagations, over a Spring MongoDB store, that handler can lose its reaction. The first command's events
  are stored, the handler's write rolls back, nothing runs the handler's command again, and a synchronous subscription
  has no replay. Use an executor that opens a transaction on the handler's service, which keeps the retry, or retry
  inside the handler.

## A handler failure in a joined transaction doesn't stop the other handlers

In 0.34.0 a synchronous dispatch stopped at the first handler that threw whenever the transaction executor reported a
transaction, including one it had only joined. With `NativeMongoTransactionExecutor` or `SpringReactiveTransactionExecutor`
inside a transaction of yours, a caller that caught the failure and committed kept the events, and the handlers behind the
failing one never got them. It applies to the stream and the DCB `GenericApplicationService`, blocking and reactive.

### Who is affected

You use synchronous subscriptions, and `execute` runs in a transaction the executor joins instead of opening its own:

- `NativeMongoTransactionExecutor` with a session already bound to the thread, yours or an outer service's.
- `SpringReactiveTransactionExecutor` in a transaction of yours, on a propagation that joins it such as `PROPAGATION_REQUIRED`.
- An executor of your own that answers `true` from `isTransactional()` before it opens its transaction, for example one
  that answers a fixed `true`. The default `inTransactionTellingIfOpened(..)` then reports every transaction as joined,
  also one the executor opened itself.

`SpringTransactionExecutor` isn't affected unless its transaction manager has `setGlobalRollbackOnParticipationFailure(false)`
or isn't an `AbstractPlatformTransactionManager`.

### What changes

- In a transaction the attempt joined, every handler gets the event and the failures are reported together, unless the
  executor's `rollsBackAJoinedTransactionOnFailure()` answers `true`. It's a new default method on `TransactionExecutor`
  and `ReactiveTransactionExecutor`, and it answers `false`.
- `SpringTransactionExecutor` answers `true` when its transaction manager is an `AbstractPlatformTransactionManager`
  that isn't set with `setGlobalRollbackOnParticipationFailure(false)`, and `false` for any other manager.
- In a transaction the attempt opened, the dispatch still stops at the first failure.

### What to do

- If your executor opens its own transaction but answers `true` from `isTransactional()` before it does, override
  `inTransactionTellingIfOpened(..)` to tell the action whether this call opened the transaction. The dispatch then stops
  at the first failure in a transaction it opened, and a failure after the write reaches the retry strategy.
- If your executor joins a transaction that a failure marks rollback-only, so the caller can't commit it, override
  `rollsBackAJoinedTransactionOnFailure()` to return `true`. The dispatch then stops at the first failure as it did in
  0.34.0, and the handlers behind it don't do work that the rollback discards.
- Otherwise keep the default. If the transaction rolls back after all, the handlers behind the failing one did work that's
  discarded, and a side effect outside the transaction, an HTTP call say, still happened.

## A read by position makes no promise about resuming

`readInPositionOrder(..)` on `PositionOrderedReader` and `DomainEventQueries` is a range query. It returns the events in
the range it can see when it runs. On MongoDB an append takes its position before its transaction commits, so
an event can reserve a position below the one you kept and commit after you read it, and a read after your position
never returns it. See [#1297](https://github.com/johanhaleby/occurrent/issues/1297).

### Who is affected

A loop that keeps the last position it read, or the value of `currentPosition()`, and reads after it the next time
round. It compiles and runs on 0.35.0 as it did on 0.34, and it can skip an event the same way:

```java
try (Stream<DomainEvent> events = domainEventQueries.afterPosition(lastPosition)) {
    events.forEach(this::handle);
}
```

`DomainEventQueries.afterPosition(..)` gives you a deprecation warning and nothing more. The same loop written with
`readInPositionOrder(filter, PositionRange.afterPosition(lastPosition))` gives you no warning at all. The store can't
tell where a position came from, so it can't refuse the read the way a catch-up refuses a start after a position.

A read that doesn't continue from an earlier one, such as a report over a range, isn't affected.

### What to do

Replace the loop with a catch-up subscription and resume from the checkpoint it stores, as
[A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from) describes.
A read model the loop has already built can be missing such an event, and only a rebuild from the beginning finds it.
Keep `readInPositionOrder(filter, PositionRange.afterPosition(p))` for range reads.

## A catch-up needs the event store itself

A catch-up subscription reads the point where its live subscription takes over, and every event it replays, through
the event store, which on MongoDB runs those reads in one session. An event that commits late, below a position the
replay already read, is then replayed or delivered live, and not skipped. [ADR 155](../architecture/decisions/0155-a-catch-up-reads-its-live-start-from-the-event-store-and-refuses-to-resume-where-it-could-skip-an-event.md)
has the reasons. Only the event store itself can do that, so the catch-up checks what it was given when you construct
it.

### Who is affected

The four catch-up subscription models and what they check:

| Model | Checks |
| --- | --- |
| `StreamCatchupSubscriptionModel` | the `EventStoreQueries` |
| `DcbCatchupSubscriptionModel` | the `DcbEventStore` |
| `ReactorStreamCatchupSubscriptionModel` | the `PositionOrderedReader` |
| `ReactorDcbCatchupSubscriptionModel` | the `DcbEventStore` |

`MongoEventStore`, `SpringMongoEventStore` and `InMemoryEventStore` pass the two blocking checks, and
`ReactorMongoEventStore` passes the two reactor ones. A class that wraps one of them, a mock, and your own implementation of the interface do not, and the constructor throws an
`IllegalArgumentException` naming `CatchupReads` or `DcbCatchupReads`.

Both Spring Boot starters run a subscription through a catch-up by default, and build the catch-up over the event store
bean in the context. A context whose bean is such a class fails at startup with the same exception.

### What to do

Hand the catch-up the event store itself. Put a decorator around the subscription model, or around the code the
subscription calls, and no longer around the event store the catch-up reads. In a test, run the catch-up over
`InMemoryEventStore` or a MongoDB event store instead of a mock.

`CatchupReads` and `DcbCatchupReads` are in packages named `internal`. They are public only because the stores and the
catch-ups live in different modules, and they can change in any release, so implementing them yourself is not supported.

## A catch-up keeps its live start in the checkpoints it stores

A catch-up subscription reads its live start, the point where its live subscription takes over, before it replays,
and goes live from it once the replay is done. An event that commits while the replay runs, below what the replay
already read, is then delivered live. A catch-up that runs without the live delegate now does that too, and when it
restarts in the middle of its replay, it goes live from the live start its earlier run stored.

### What changes

The checkpoint a replay stores holds the live start in a new form when the catch-up runs without the live delegate, or
when the replay started after position 0 or after the beginning of time:

| Replay | Stored as |
| --- | --- |
| By position | `position:<position>;v:2;origin:<position>;replayTo:<position>;liveFrom:<live start>` |
| By time | `position:time;v:2;at:<time>;origin:<time>;liveFrom:<live start>` |

The other checkpoints a catch-up stores keep the form they had in 0.34.

A catch-up runs without the live delegate when the model it wraps answers `null` to its start. Under the Spring Boot
starter that is the case for `resumeBehavior = SAME_AS_START_AT`, where `DurableSubscriptionModel` and
`CompetingConsumerSubscriptionModel` answer `null`. Such a catch-up now hands the innermost subscription model, the
MongoDB one, the live start, and stores nothing when it goes live. It fails with an `IllegalStateException` when neither
the event store nor the subscription model reports a live start.

### Rolling upgrades and rolling back

0.34 refuses a checkpoint in the new form. It throws when it reads one and writes nothing, so nothing is lost, but the
subscription doesn't start on that node. During a rolling upgrade, a 0.34 node that resumes a subscription a 0.35 node
stored in the new form fails to start it until it runs 0.35 too.

To roll back to 0.34, first delete the stored checkpoint of every subscription whose checkpoint starts with
`position:` and has `;v:2;` after its first value. Each of those subscriptions then starts from the start it is
configured with.

### When a catch-up loses its live start on every replay

After its replay, a catch-up checks that its live start is still in the change stream history. When it isn't, and the
replay started at position 0 or at the beginning of time, the catch-up replays again with a new live start, up to three
times. A replay that started later is refused the first time, as
[A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it)
describes. When it loses the live start after each of those
replays too, it fails with `CatchupCannotResumeException`, an `IllegalStateException`, and the message names the longest
replay, from reading its live start to checking it. Size the change stream history to reach back at least that far. On
MongoDB that is the oplog, which you can keep for a minimum time with the `oplogMinRetentionHours` setting.

A blocking catch-up whose replay a resume or a takeover started then holds the replay and logs the failure once, and
runs it again when the subscription is resumed or `start(true)` is called. A replay that `subscribe(..)` started ends
with the exception, which `waitUntilStarted()` throws.

A DCB catch-up and the reactor catch-ups replay only by position, and throw the same exception when you start them from
a time replay's checkpoint in the new form.

## A replayed event holds the checkpoint to resume from

`StreamCatchupSubscriptionModel` and `DcbCatchupSubscriptionModel` now deliver each event they replay as a
`CheckpointAwareCloudEvent`, holding the checkpoint the catch-up stores after it. The live subscription and the reactor
catch-ups already delivered their events that way. An application that keeps its own state can store the checkpoint
with that state, in the same transaction, and resume from it. The state and the point the subscription resumes from
then never disagree. See [#1297](https://github.com/johanhaleby/occurrent/issues/1297).

### Who is affected

An action or a converter that compares a replayed event with the `CloudEvent` that was written, or checks its class. A
`CheckpointAwareCloudEvent` equals only another one with the same checkpoint, so such a comparison now fails. Compare
`getOriginalCloudEvent()` instead. `EventMetadata` equality doesn't include the checkpoint, so the metadata of an event from
the replay and of the same event from live compare equal.

### Resuming from a checkpoint you store

Read the checkpoint where your handler gets the event:

| Handler | Read the checkpoint with |
| --- | --- |
| The DCB DSL, blocking and reactor | `DcbEventMetadata.checkpoint()` |
| The Kotlin stream DSL, a `@Subscription` method with an `EventMetadata` parameter, a `@Projection` view | `EventMetadata.getCheckpoint()` |
| An action that takes the `CloudEvent` | `EventMetadata.from(cloudEvent).getCheckpoint()` |

Each returns an empty `Optional` for an event delivered with no checkpoint. Store the string with your state, in the
same transaction, and start the subscription from it again with
`StartAt.checkpoint(new StringBasedCheckpoint(checkpoint))`, or `DcbStartAt.checkpoint(checkpoint)` for a DCB
subscription.

From the checkpoint of a live event, or of an event that a catch-up replays by position, the subscription resumes
after that event. A catch-up that replays by time resumes at the event's time instead, so it delivers the events with
that time again, that event included. An event without a time holds the checkpoint of the event before it, and resumes
where that one does. When its `catchupPhaseSortBy` doesn't start with the time ascending, an event from its first read
resumes the whole replay from where it started.

To rewind a read model to a point you know is good, keep the checkpoint of the event at that point, and start the
subscription from it.

The checkpoint of a replayed event means something only to a catch-up subscription. A MongoDB subscription model with
no catch-up wrapping it refuses that checkpoint, in a `StartAt` that names it or as a position a `StartAt.dynamic(..)`
returns before its change stream has opened, as
[A MongoDB subscription model refuses a catch-up's checkpoint](#a-mongodb-subscription-model-refuses-a-catch-ups-checkpoint)
describes.

The checkpoint of a live event holds a change stream resume token, so a restart from it needs the change stream
history to still reach back to it. On MongoDB that is the oplog. Once it no longer does, a catch-up subscription
doesn't restart the subscription from the present, as
[A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now)
describes. A MongoDB subscription model with no catch-up subscription wrapping it stops the subscription with an error
in the log, or restarts it from the present when it is set to restart after lost history, which skips the events in
between.

### Under a competing consumer

`CompetingConsumerSubscriptionModel` hands your start to the model it wraps when the node first gets the lease, not
when you subscribe. A `StartAt.checkpoint(..)` you build when you subscribe is as old as the node's start, so a node
that takes over an hour later delivers that hour's events again. Read the checkpoint when the node takes over instead:

```java
competingConsumerSubscriptionModel.subscribe("my-read-model", filter, StartAt.dynamic(context -> {
    String checkpoint = myStore.checkpoint();
    return checkpoint == null ? StartAt.subscriptionModelDefault() : StartAt.checkpoint(new StringBasedCheckpoint(checkpoint));
}), action);
```

Return a start of your own, such as `StartAt.subscriptionModelDefault()`, while your store holds no checkpoint yet,
since `StringBasedCheckpoint` refuses `null`.

When a node gets the lease back after losing it, the model it wraps resumes from where that node last ran. Events can
be delivered again. None are skipped, unless the change stream history no longer reaches back to that position and the
model is set to restart after lost history, which restarts the subscription from the present.

### Spring Boot

Declare a `SubscriptionResumePoints` bean, and Spring Boot asks it where each `@Subscription`, `@StreamSubscription`,
`@DcbSubscription`, `@Projection`, `@Saga` and `@Snapshot` subscription resumes, whatever start position the
annotation gives, `NOW` and `DEFAULT` included:

```java
@Bean
SubscriptionResumePoints subscriptionResumePoints(MyReadModelRepository repository) {
    return subscriptionId -> repository.findCheckpoint(subscriptionId).orElse(null);
}
```

A non-null answer is the checkpoint the subscription resumes from, ahead of the one Spring Boot's `CheckpointStorage`
holds. When that stored checkpoint is newer than the answer, the events between the two are delivered again. `null`
starts the subscription from the annotation's start, as it starts without the bean. Spring Boot reads the bean each time
a subscription model reads the start, also when a node of a competing consumer takes over, and can ask more than once
for one start, so return the same answer each time.

With `resumeBehavior = SAME_AS_START_AT` on a start that replays, `BEGINNING` or an explicit position, or on `NOW`, the
answer only moves where the subscription starts. The subscription keeps no checkpoint in `CheckpointStorage` once it is live, and the
competing consumer model does not compete for it. A catch-up that stores checkpoints during its replay still does, and
deletes the last one when the replay goes live.

Spring Boot decides from the annotation, not from the answer, whether application startup waits for the subscription.
When `occurrent.subscription.mode` is `manual`, nothing waits. Otherwise, under the default `startupMode`, a subscription whose annotation starts at `NOW` or `DEFAULT` waits, also when the
answer makes it replay. One whose annotation replays, from the beginning, a position or a time, starts in the
background, also when the answer makes it go live. Set `startupMode` on the annotation to choose.

### An exhaustive `switch` over `DcbStartAt`

`DcbStartAt.checkpoint(..)` returns the new `DcbStartAt.AtCheckpoint`. `DcbStartAt` is sealed, so a `switch` over it
that lists every case and has no `default` no longer compiles. Add a case for `DcbStartAt.AtCheckpoint`.

### Why there is no recipe

What the new case should do depends on what the `switch` does with the others, so there is nothing for
`UpgradeToOccurrent_0_35` to write into it.

## A catch-up over both a stream and a DCB model refuses a position start without a filter

The blocking `CatchupSubscriptionModel` and `ReactorCatchupSubscriptionModel` hold a stream and a DCB catch-up when you
use both, as the Spring Boot starters do. They route a subscription with no filter by its start. A time or beginning
start goes to the stream catch-up, and a position checkpoint went to the DCB one, so a stream subscription restored from
its own checkpoint replayed only DCB events and skipped its stream events. Both models now throw an
`IllegalArgumentException` before they subscribe anything.

### Who is affected

Code that subscribes with no filter, or with a `null` one, at a position checkpoint over a model that holds both
catch-ups. That includes `subscribe(subscriptionId, startAt, action)`, which passes no filter, and the broker
`CloudEventForwarder.forward(subscriptionId, startAt)` and `DomainEventForwarder.forward(subscriptionId, startAt)`. The
DSLs and the Spring Boot annotations pass a filter, and a time start, a beginning start and a model with only one
catch-up are not affected.

Under a `CompetingConsumerSubscriptionModel`, `subscribe` throws when the node gets the lease straight away or the
competing consumer model is stopped. A node that gets the lease later logs a warning, gives the lease back, and fails the
same way every time it gets the lease.

### What to do

Pass a `StreamSubscriptionFilter` for stream events, a `DcbSubscriptionFilter` for DCB events, or an
`AgnosticSubscriptionFilter` for both. For a broker forwarder, use the `forward` overload that takes a filter.

## A catch-up refuses a start that could skip an event committed below it

On MongoDB an append takes its position before its transaction commits, so an event can commit below a position a
reader has already read past. A catch-up that went live from a live start read before that position still delivers
such an event, from the change stream. A catch-up started after a position with no such live start never delivers it.
A catch-up now refuses every start that could skip an event that way, before it delivers or stores anything, and fails
with `CatchupCannotResumeException`. A replay that loses its live start fails with the same exception, but only after
its handler has received the events the replay delivered, in this run or before a restart, and after any checkpoint
storage it uses has kept the progress. So the exception doesn't mean that nothing reached the handler. See
[#1230](https://github.com/johanhaleby/occurrent/issues/1230).

### Who is affected

| Start | 0.34 | 0.35 |
| --- | --- | --- |
| `startAtGlobalPosition` on `@Subscription`, `@Projection`, `@Saga` or `@Snapshot`, or `startAtDcbPosition` on `@DcbSubscription`, above 0 | Replays after the position | Refused |
| `DcbStartAt.afterPosition(n)` or `StartAt.checkpoint(GlobalCheckpoint.of(n))`, with `n` above 0 | Replays after `n` | Refused |
| A stored `position:<n>` checkpoint that holds no live start, with `n` above 0 | Resumes after `n` | Refused |
| A time checkpoint, `TimeBasedCheckpoint`, that a blocking catch-up resumes from and that holds no live start | Resumes from the time and logs a warning | Refused |
| A replay whose live start the change stream history no longer reaches, when it started above position 0 or after the beginning of time | Replays again from where it started, with a live start read then, up to three times | Refused the first time |

The table's last row also covers a catch-up from a specific time, `StartAt.checkpoint(TimeBasedCheckpoint)` or a time
start in Spring Boot. When its replay outlasts the change stream history, it now fails at once instead of replaying
again.

The same applies to a checkpoint 0.34 stored in the middle of a replay that started above position 0. When the
application is down for the upgrade for so long that the change stream history no longer reaches back to the live start
in that checkpoint, 0.35 refuses it.

A start at position 0, `StartPosition.BEGINNING`, `DcbStartAt.beginning()` or the beginning of time is not refused. A
replay from there whose live start is lost still replays again, as
[When a catch-up loses its live start on every replay](#when-a-catch-up-loses-its-live-start-on-every-replay)
describes.

On an event store that writes no positions, a blocking stream catch-up throws `UnsupportedStartAtException` instead for
`StartAt.checkpoint(GlobalCheckpoint.of(n))`, `startAtGlobalPosition` or a stored `position:<n>`, with `n` above 0, and
for a stored one also when it holds a live start. Over such a store none of them replayed from 0.30.0 to 0.34.0. They
went live at the present, as
[A stream catch-up over an event store without positions replays from position 0](#a-stream-catch-up-over-an-event-store-without-positions-replays-from-position-0)
describes.

The blocking catch-up throws `CatchupCannotResumeException` from `subscribe(..)` or from `waitUntilStarted()`, and a
replay a resume or a takeover started is held, as for a lost live start. The reactor catch-up fails its `Flux` with it.
The message names the subscription and what to do.

### What to do

To resume after an event, keep the checkpoint the subscription delivers with it, and start from that, as
[A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from)
describes. In Spring Boot, return it from a `SubscriptionResumePoints` bean. That checkpoint holds the live start, so a
start from it is not refused while the change stream history still reaches back to that live start. Once it no longer
does, a start from it is refused when the replay that delivered the event started at a specific time, or is a 0.34
replay that started above position 0. A checkpoint from a replay that started at position 0 or at the beginning
replays again from there instead.

A subscription that is refused, and has no such checkpoint stored with its state, can only start from the beginning
again: `StartPosition.BEGINNING`, `DcbStartAt.beginning()` or `StartAt.checkpoint(GlobalCheckpoint.of(0))`. Cancel it
first with `cancelSubscription(id)`, which deletes its stored checkpoint. A blocking catch-up holds a refused resume,
and holds the stored checkpoint it read with it, so deleting the checkpoint and resuming is refused again. With every
instance of the application stopped, deleting the stored checkpoint is enough. From then on it delivers a checkpoint
with every event. For a refused replay whose live start was lost, size the change stream history to reach back across
the replay. On MongoDB that is the oplog.

`startAtGlobalPosition`, `startAtDcbPosition` and `DcbStartAt.afterPosition(long)` are deprecated for removal, since
every value above 0 is refused. Use `StartPosition.BEGINNING` or `DcbStartAt.beginning()` instead of 0.

## A stream catch-up over an event store without positions replays from position 0

A blocking `StreamCatchupSubscriptionModel` over an event store that writes no positions handed
`StartAt.checkpoint(GlobalCheckpoint.of(n))` to the subscription model it wraps, without a replay. The MongoDB
subscription model opened the change stream at the present for it, and skipped every event before it with no error.
Every release from 0.30.0 to 0.34.0 did that. Position 0 now replays from the beginning of time and then goes live. A
position above 0, whether passed, from `startAtGlobalPosition`, or read from the stored checkpoint, throws
`UnsupportedStartAtException` from `subscribe(..)`, since the store has no position to replay after.

### Who is affected

A Spring Boot application on the blocking MongoDB starter with `startAt = StartPosition.BEGINNING` or
`startAtGlobalPosition = 0` on `@Subscription`, `@Projection`, `@Saga` or `@Snapshot`, which Spring Boot starts at
position 0, over an event store that writes no stream positions. A store writes none when
`occurrent.event-store.stream.position` is `false`. It also writes none when stream position is on only by default and
the store started against a collection that already held events without a `position`, since it then turns stream
position off for itself and logs how to turn it on. On any release from 0.30.0 to 0.34.0 such a subscription got none
of the history, only the events written after it started. On 0.30.0 that is `@Subscription` alone, since `@Projection`,
`@Saga` and `@Snapshot` arrived in 0.31.0. An application that upgrades straight from 0.30, 0.31, 0.32 or 0.33 is
affected in the same way.

The same goes for code that starts a blocking stream catch-up at `StartAt.checkpoint(GlobalCheckpoint.of(n))` over such
a store. A DCB event store always writes positions.

### What to do

Under `resumeBehavior = SAME_AS_START_AT` there is nothing to do. The subscription keeps no checkpoint, and replays the
whole history the next time it starts.

Under the default `resumeBehavior`, a subscription that went live this way before 0.35 stored a checkpoint as it handled
events, and resumes from that checkpoint in 0.35, so it still never gets the history. To get it, stop every instance of
the application, delete the checkpoint stored for the subscription, and start the application again. The subscription
then replays the whole history, including the events it already handled after it went live, so rebuild the read
model it feeds first, or make sure its handler can take an event it already handled.

For a start above position 0 in your own code, start from `StartAtTime.beginningOfTime()` or from a time instead.

## A MongoDB subscription model refuses a catch-up's checkpoint

`NativeMongoSubscriptionModel`, `SpringMongoSubscriptionModel` and `ReactorMongoSubscriptionModel` opened the change
stream at the present when you subscribed at a `GlobalCheckpoint` or a `CatchupTimeCheckpoint`, the checkpoints a
catch-up stores, and skipped every event before it with no error. Their `subscribe(..)` now throws an
`UnsupportedStartAtException`, and the reactor model's unnamed `subscribe(..)` fails its `Flux` with it. See
[#1336](https://github.com/johanhaleby/occurrent/issues/1336).

A `StartAt.dynamic(..)` whose function returns one of these checkpoints opened the change stream at the present too,
in 0.34.0 and before. `DcbStartAt.dynamic(..)` returning `DcbStartAt.beginning()` is one. The models now refuse that
as well. They call the function only as the change stream opens, so `subscribe(..)` returns, and the subscription then
stops with the `UnsupportedStartAtException` instead of delivering events. `waitUntilStarted(..)` throws it on the
blocking models, which also log it at `ERROR`, and `waitUntilStarted()` fails with it on the reactor model, except on
the handle `subscribe(..)` returns while the model is stopped, which never completes. A reactor handle that has answered
that it started keeps that answer. It answers once the change stream is subscribed to, so a refusal on a retry after a
failed opening stops the subscription and is only logged at `ERROR`. On a subscription that reads through the
driver's cursor, a resume straight after the handle answered can be refused too, when the function returns one of these
checkpoints, since the cursor may not have opened. The handle that resume returns fails with it. See
[#1371](https://github.com/johanhaleby/occurrent/issues/1371).

The function is checked until a change stream of the subscription has opened. On the blocking models that means a run
has reported its cursor open, so an answer from an opening that a pause or a failure stopped doesn't end the check. The
reactor model counts a driver cursor that opened for a subscription with an id. A reactor `subscribe(..)` without an id,
and a subscription with an id when the model can't read through the driver's cursor, count the first document the
change stream reads instead.

Once a change stream has opened, the model opens it again at the same position and doesn't call the function again.
That covers a pause and a resume, an action that throws, and a failure reading the change stream before the first
event. A reactor subscription that counts the first document takes the position the function answered when its `Flux`
was subscribed to. It gives that position back to the function only when the change stream fails before it reads a
document with error code 9, 280 or 286, the codes the server rejects a start position with. That holds also when a
pause or another failure opened the change stream again at that position first. After a 9 or a 280 the model opens the
change stream again, and that opening calls the function again. After a 286 a subscription with an id first asks the
listeners added with `addHistoryLossListener`, with the checkpoint the change stream opened at. One of them can take the
subscription over. When one throws, the model logs it and opens the change stream again after its backoff, which calls
the function again and asks the listeners again if the history is still lost. Otherwise, with
`restartSubscriptionsOnChangeStreamHistoryLost(true)` the model restarts the subscription at the present without
calling the function, and with the default the subscription fails. A `Flux` from `subscribeFailingOnHistoryLoss`
fails with a `HistoryLostException` that names that checkpoint, whatever the configuration. In 0.34.0 the models could call the function again at an opening
before the first event, and a catch-up's checkpoint from
that later call opened at the present and skipped the events written in between.

Three cases still lose events, as they did in 0.34.0:

- A resume token of another collection as the function's first answer, on the blocking models and on a reactor
  subscription that reads through the driver's cursor. The server opens the change stream at it and rejects it only on
  the first read after an event is written to the watched collection. The subscription keeps running and can retry at
  that token without delivering anything.
- An operation time ahead of the server's clock as the function's answer. The server opens the change stream at it, and
  the events written before that time aren't delivered.
- A position the server rejects only after an event is written, such as a resume token of another collection, on a
  reactor subscription that counts the first document. The model calls the function again, and an answer that comes
  after that event skips it.

### Who is affected

Code that subscribes a MongoDB subscription model directly at a checkpoint a catch-up stored, or delivered with a
replayed event, in a `StartAt` that names it or from a dynamic start that returns it. A
resume at such a checkpoint, `resumeSubscription(subscriptionId, startAt)`, isn't refused. The models go on from the
position the subscription has read to, for such a checkpoint and each time a dynamic position returns one. In 0.34.0
the reactor model opened at the moment of the resume instead, and skipped what was written while the subscription was
paused.

The function `subscribe(..)` was given is another matter. When no change stream has opened yet, a resume can still ask
that function, and it's refused when the function returns one of these checkpoints. That includes
`resumeSubscription(subscriptionId, startAt)` at such a checkpoint, or at a dynamic position that returns one.

A blocking `DurableSubscriptionModel` hands the MongoDB model a dynamic start that returns the checkpoint in its
`CheckpointStorage`. A subscription whose storage holds a catch-up's checkpoint now stops in the same
way, where it skipped the history before. A blocking Spring Boot subscription with `startAt` unset doesn't get there,
since it resumes from such a checkpoint, see
[A subscription with no start position resumes from a catch-up's checkpoint](#a-subscription-with-no-start-position-resumes-from-a-catch-ups-checkpoint).
`ReactorDurableSubscriptionModel` hands the
reactor model the stored checkpoint itself, so a subscription at the model default whose storage holds one of these
stops too, its handle fails with the `UnsupportedStartAtException`, and the checkpoint stays in storage.

A `TimeBasedCheckpoint` isn't refused. A catch-up writes one when it can't get a global position, and reads it back. A
`DurableSubscriptionModel` with no catch-up around it and one of these in its storage still opens at the present.

A `CompetingConsumerSubscriptionModel` wrapping `NativeMongoSubscriptionModel` or `SpringMongoSubscriptionModel` hears
of a refusal of a dynamic start through `StartRefusalReportingSubscriptions`, also through a `DurableSubscriptionModel`
in between, although the refusal comes after `subscribe(..)` has returned. The node that got the lease gives it back,
frees the subscription id and logs the refusal at `ERROR`. The handle that node's `subscribe(..)` returned answers
`true` from `waitUntilStarted(..)` while it waits for the lease, as a waiting competing consumer always does, and throws
the `UnsupportedStartAtException` once the refusal has come. A refused node doesn't compete for the subscription again,
so once every node that subscribed it has been refused, the subscription runs on no node and its lease stays free.
Subscribe it again with the same id, on any node, with a start position the model accepts.

### What to do

Subscribe through the catch-up subscription model that stored the checkpoint. For a `DcbStartAt.dynamic(..)` that
returns `DcbStartAt.beginning()`, subscribe through a model composed with `DcbCatchupSubscriptionModel` or
`ReactorDcbCatchupSubscriptionModel`, which replays the history before going live, or return `DcbStartAt.now()` when
the subscription doesn't need the history.

For a subscription behind a catch-up whose `CheckpointStorage` holds a catch-up's checkpoint, restore the start it had
when the replay stopped, so the catch-up finishes the replay, or remove the stored checkpoint to start from the new
start position without the history. A Spring Boot starter subscription at `DEFAULT` waits for its start by default, so
the exception can stop the application from starting.

### A record pattern over `NativeMongoSubscription` stops compiling

The `Subscription` that `NativeMongoSubscriptionModel.subscribe(..)` returns is a `NativeMongoSubscription`, and it
now holds a supplier of the failure that `waitUntilStarted(..)` throws, as a third record component called
`startFailure`. A record pattern has to name every component of the canonical constructor:

```java
// Before, stops compiling
case NativeMongoSubscription(var id, var latch) -> ...

// After
case NativeMongoSubscription(var id, var latch, var startFailure) -> ...
```

`org.occurrent.MigrateNativeMongoSubscriptionRecordPattern_0_35`, which `UpgradeToOccurrent_0_35` runs, adds the
binding to a `switch` case and an `instanceof` pattern alike. Constructor calls aren't affected. The two-argument
constructor stays, and `waitUntilStarted(..)` on an instance built with it only waits.

## A subscription whose resume token is gone stops instead of restarting from now

A MongoDB subscription continues from a resume token, a position in the oplog, which is MongoDB's log of recent writes.
When a subscription has been stopped or paused for longer than the oplog keeps its writes, the token is older than the
oldest entry and MongoDB can't resume from it. Before 0.35.0 the Spring Boot starters then restarted the subscription
from now, since `restart-on-change-stream-history-lost` defaults to `true`, and every event committed between the
token and the restart was never delivered. A subscription that a catch-up subscription runs now stops instead.

### Who is affected

Every subscription made through `CatchupSubscriptionModel`, `StreamCatchupSubscriptionModel`,
`DcbCatchupSubscriptionModel` or their reactor counterparts on MongoDB, including subscriptions that start at `NOW` or
`DEFAULT` and never replay. Both starters run every subscription through one by default:

- the blocking starter when `occurrent.event-store.capabilities` contains `STREAM`, or contains `DCB` and the context
  has a `DcbEventStore`
- the reactive starter when `occurrent.event-store.capabilities` contains `DCB` and the context has a DCB store, or
  when it contains `STREAM` and the stream store writes positions, which is the default

`restart-on-change-stream-history-lost` keeps its meaning only where the starter composes no catch-up subscription.

A catch-up subscription you build yourself is affected too, also when the MongoDB subscription model under it doesn't
restart, which is how that model is built by default. There the subscription used to end with an error in the log,
and the `Flux` of an unnamed reactor subscription failed with MongoDB's own error, code 286. It now stops as below.

On the reactor stack, an action that a catch-up subscription model runs, a `doOnCancel` on the `Mono` an action
returns and a handler on `waitUntilStarted()` can subscribe, cancel, stop or shut down through that model, from the
thread that runs the code or from another thread. A subscribe of an id throws `DuplicateSubscriptionIdException` until
the model the catch-up wraps has completed its cancel of that id. `ReactorMongoSubscriptionModel` completes it before
`cancelSubscription` returns.

A `StartAt.dynamic(..)` function that answers `null` to the catch-up can call the catch-up model too. The catch-up then
goes straight to live and hands the function to the model it wraps, and `ReactorMongoSubscriptionModel` can call it
again inside its own `subscribe(..)`, on the thread that called the catch-up. While that `subscribe(..)` runs, a
subscribe of the id throws `DuplicateSubscriptionIdException` until the id is cancelled. A cancel of the id then, from
that thread or another, goes to the wrapped model at once, and the action gets no event after it.

When that cancel reaches `ReactorMongoSubscriptionModel` or `ReactorDurableSubscriptionModel` before the cancelled
`subscribe(..)` took the id there, the model registers nothing for it, and `waitUntilStarted()` on the
`Subscription` that `subscribe(..)` returned fails with `IllegalStateException`. In 0.34.0 that subscription
ran and `waitUntilStarted()` completed. When the model was shut down by the time it asks whether the `subscribe(..)` was
cancelled, `subscribe(..)` throws `SubscriptionModelShutdownException` instead and returns no `Subscription`. Both models take the id before they call the function, so only a cancel from
another thread can get there first. Only an instance of either exact class takes this path. A subclass or a proxy of
either model, a Spring AOP proxy for example, registers the cancelled `subscribe(..)` as 0.34.0 did, and the catch-up
then cancels the id there again, as described below. The catch-up can't tell whether a subclass overrides
`subscribe(String, SubscriptionFilter, StartAt, Function)` or whether a proxy advises it, so it calls that method on a
subclass or a proxy. A Mockito spy of either model is an instance of the exact class with Mockito's default inline mock
maker, so it takes this path, and a stub or a `verify` on its `subscribe(String, SubscriptionFilter, StartAt, Function)`
doesn't see a catch-up subscribe that goes straight to live.

If another thread can cancel a subscribe whose start you wait for, `waitUntilStarted()` can fail with an
`IllegalStateException` because of that cancel. That happens when:

- the cancel reached a `ReactorMongoSubscriptionModel` or `ReactorDurableSubscriptionModel` of that exact class before
  it took the id
- the cancel, or the catch-up's second cancel of a subclass or a proxy, reached `ReactorMongoSubscriptionModel` while
  the subscription was starting there
- the subscribe replays history first and the cancel came before the replay handed over to the wrapped model

When the cancel reached `ReactorDurableSubscriptionModel` after it took the id and before it started the subscription
or handed it to the model it wraps, the exception is `CancellationException`, which extends `IllegalStateException`.

`IllegalStateException` also comes from failures that have nothing to do with a cancel, so treat it as the cancel only
when your application cancelled the id.

Another wrapped model can still register the cancelled `subscribe(..)` after the cancel. So can a subclass or a proxy
of `ReactorMongoSubscriptionModel` or `ReactorDurableSubscriptionModel`, whether or not it overrides or advises
`subscribe(String, SubscriptionFilter, StartAt, Function)`. When the
wrapped `subscribe(..)` returns holding the id, the catch-up cancels the id there again, unless a subscribe of the id
made after the cancel took it there. That cancel waits until no other subscribe of the id is under way. Until that
second cancel has completed, a subscribe of the id throws `DuplicateSubscriptionIdException` whenever the wrapped model
holds the id for the cancelled `subscribe(..)`, also after the first cancel has completed. When the cancel came before
the wrapped model had taken the id for the cancelled `subscribe(..)`, the wrapped model can refuse a subscribe of the id
made after the cancel with `DuplicateSubscriptionIdException`, since the cancelled one can take the id there first. A
subscribe that replays history first gets that exception from `waitUntilStarted()` once the replay hands over. The
catch-up then cancels the id there again. A wrapped model that takes the id only after it called the function makes that
likely.

A `CatchupListener` given to `listenForCatchup` can still run while the catch-up holds a lock, so don't call the
catch-up model from it. The listener is told that a catch-up started from inside an operation on a map the catch-up
keeps.

### What happens

- A subscription that has gone live stops. That is every subscription that started at `NOW`, `DEFAULT`, a time or a
  position, whether it replayed first or not, and every subscription that resumed from a stored resume token after the
  application restarted.
- A replay whose live start is already gone when the subscription opens the change stream from it runs again from
  where it started, as it has since 0.34.0. A handler that sends mail or calls another service can then get events of
  the unfinished first replay again.

The catch-up doesn't replay from the lost token instead. On MongoDB an append takes its position before its
transaction commits, so an event can still commit below a position the subscription has already read. A replay from
any position other than the beginning could skip that event.

### What you see when a subscription stops

- An error in the log each time MongoDB reports the loss, naming the subscription and the resume token that is gone.
  For a subscription with an id, the MongoDB subscription model logs a warning next to it, that it paused the
  subscription and a listener took it over, as it has since 0.34.0. On a blocking catch-up subscription model you call
  directly, the warning can be all you get when a `resumeSubscription(id, startAt)` at another position runs before the
  error is logged. When such a resume clears a recorded loss, an information line says that an error logged earlier
  for the subscription no longer applies to the position it lost. When you paused or resumed the subscription just
  before MongoDB reported the loss, neither is logged and `resumeSubscription(id)` isn't refused. A subscription you
  resume with `resumeSubscription(id)` opens at the lost token again, MongoDB reports the loss again, and the
  subscription stops as described here, without skipping an event. One you resume with
  `resumeSubscription(id, startAt)` opens at `startAt` when it is a resume token.
- On the reactor stack `waitUntilStarted()` of a subscription with an id completes before the MongoDB subscription
  model finds the token gone, so it doesn't tell you the subscription stopped. Check `isPaused(id)` instead.
- The subscription is paused, `isPaused(id)` answers `true`, and it delivers nothing.
- After the error, `resumeSubscription(id)` throws `CatchupCannotResumeException` until you cancel the subscription,
  see [How to recover](#how-to-recover). On a blocking catch-up subscription model you call directly, a
  `resumeSubscription(id, startAt)` can end that refusal too, even at a position whose history is gone, and even when
  the error is logged while it runs. A `resumeSubscription(id)` after the pause but before the error isn't refused
  yet. A resume that isn't refused never delivers from a position whose history is gone. When it opens the
  subscription there, MongoDB reports the loss again and the subscription is paused again with another error, so no
  event is skipped. On the blocking stack `start(true)` throws `CatchupCannotResumeException` too. On the reactor
  stack `start(true)` resumes the subscription in the MongoDB subscription model, which finds the token gone again and
  pauses it again with one more error.
- On the blocking stack, a subscription you start from a resume token that is already gone throws
  `CatchupCannotResumeException` from `subscribe`, and nothing is stored. Under competing consumers the start runs
  when a node acquires the lease. When it fails there, that node pauses the subscription, logs an error that names
  it, and gives up the lease. The node doesn't compete for the lease again until you resume the subscription, with `resumeSubscription(id)` or
  `start(true)`.
  Another node that waits for the lease can then get it, and pauses the subscription the same way. On the reactor stack such a subscription with an id is paused as
  above once the MongoDB subscription model opens it, and nothing is stored.
- In Spring Boot the blocking starter throws `CatchupCannotResumeException` when it registers a subscription whose
  start reads the stored resume token and that token is gone, which can keep the application from starting. Once a
  subscription has gone live and stored a resume token, these starts read it:
  - `startAt = BEGINNING` and `startAtGlobalPosition` on `@Subscription`, `@Projection`, `@Saga` and `@Snapshot`,
    under the default `resumeBehavior`. `@Snapshot` starts at `BEGINNING` when you don't set `startAt`, so a
    `@Snapshot` with no start attribute is one of them.
  - `startAt = BEGINNING_OF_TIME`, `startAtISO8601` and `startAtTimeEpochMillis` on `@StreamSubscription`, under the
    default `resumeBehavior`.
  - `startAt = DEFAULT` on every annotation, whatever the `resumeBehavior`. So every subscription with no start
    attribute is one of them, apart from a `@Snapshot`, which starts at `BEGINNING` and is in the first item. On a
    `@DcbSubscription`, a `@Projection` that returns a `DcbProjection` and a `@Snapshot` that returns a
    `DcbSnapshotView`, under the default `resumeBehavior`, `startAt = BEGINNING` reads it too, and so do
    `startAtDcbPosition` on `@DcbSubscription` and `startAtGlobalPosition` on the other two.
  - Every start, `NOW` and `DEFAULT` included, when a `SubscriptionResumePoints` bean answers a resume token for the
    subscription.

  A `NOW` start that no `SubscriptionResumePoints` bean answers for doesn't read the stored token and opens at the
  present. Every reactive subscription is paused as above.

  A subscription with `startupMode = BACKGROUND` on the annotation whose stored resume token is gone is paused
  instead, with an error in the log that names it, and the rest of the application starts. With
  `occurrent.subscription.mode=manual` that happens in `start(true)`, which goes on to start the other subscriptions,
  or in the `resumeSubscription(id)` that first starts the subscription, which doesn't throw.
  The node doesn't compete for the lease again until you resume the subscription, as the competing consumers item
  above describes. A later `resumeSubscription(id)` competes again, and throws `CatchupCannotResumeException` when this node
  gets the lease during the call and the token is still gone. With `startupMode`
  unset the application doesn't start on a node that holds the lease when the subscription registers. While the
  subscription model runs, a node that waits for the lease starts in either mode, and pauses the subscription once it
  gets the lease, as above.
- The `Flux` of an unnamed reactor subscription fails with `CatchupCannotResumeException`.

### How to recover

Cancelling a subscription whose checkpoint Spring Boot stores deletes the checkpoint. When the subscription is
registered again, it starts where its annotation says. So cancelling and subscribing again with `NOW` or `DEFAULT`
skips every event since the lost token. For a `NOW` or `DEFAULT` subscription the only ways back without a gap are to
switch it to `BEGINNING` or to resume it from a checkpoint the application kept.

When the application doesn't start, or you would rather not call `cancelSubscription`, delete the stored checkpoint
instead, with every instance of the application stopped. Spring Boot stores it in the collection
`occurrent.subscription.mongodb.collection` names, `subscriptions` by default, as the document whose `_id` is the
subscription id. Every step below that cancels the subscription can delete that document instead.

A subscription that the blocking starter paused under `startupMode = BACKGROUND` recovers the same way. With every
instance of the application stopped, delete its stored checkpoint, or replace it with a resume token the oplog still
has, and start the application again. It then starts where its annotation says, or from the resume token you stored.

To replay from the beginning, when the read model or saga can take every event again:

1. Set `startAt = BEGINNING` on the annotation. In code, subscribe from the beginning, for example
   `DcbStartAt.beginning()` for a DCB subscription.
2. Cancel the subscription with `cancelSubscription(id)`, which deletes its stored checkpoint.
3. Register it again, for example by restarting the application. With nothing stored, it replays from the beginning
   and then goes live.

To rebuild the read model, when it can't take an event twice:

1. Stop the application, or cancel the subscription.
2. Delete the read model's data and the subscription's stored checkpoint. Stopping the application keeps the
   checkpoint, and the next start would refuse it again.
3. Replay from the beginning, as above.

To resume from a checkpoint the application kept, when it stores the checkpoint delivered with each event:

1. Cancel the subscription.
2. Subscribe again from that checkpoint, or answer it from a `SubscriptionResumePoints` bean. A checkpoint from a
   replayed event always resumes without a gap. A resume token from a live event resumes only while the oplog still
   has it.

On a blocking catch-up subscription model you call directly, you can keep the subscription instead of cancelling it,
when the token you kept is from a live event and the oplog still has it. Call
`resumeSubscription(id, StartAt.checkpoint(new StringBasedCheckpoint(token)))` on the catch-up model. It runs the
paused subscription again from that token, and a later `resumeSubscription(id)` no longer throws. This resume never
replays, so a checkpoint from a replayed event doesn't work here. When the catch-up model wraps a
`DurableSubscriptionModel`, that model stores the token as the subscription's checkpoint.

The blocking Spring Boot starter puts a `CompetingConsumerSubscriptionModel` around the catch-up model, and that model
has no resume at a position, see [#1304](https://github.com/johanhaleby/occurrent/issues/1304). So in Spring Boot,
cancel the subscription as above.

To continue from now, when skipping the events since the lost token is acceptable:

1. Cancel the subscription.
2. Subscribe again with `StartAt.now()`, or register it again with `startAt = NOW` or the default. The events
   committed between the lost token and now are never delivered.

### How to keep it from happening

Keep the oplog window longer than the longest time a subscription can be stopped, paused or replaying. On the
blocking stack a subscription with an id that has handled an event since it subscribed saves its position about once a
minute by default while it matches no events, so for it the time that counts is the time it is down. One that resumed
from the position stored for it after the application restarted, or whose replay ended, saves no position while it
matches no events until it handles an event, so the position stored for it stays where it was. For it the time that
counts is the time since that position was written. On the reactor stack a subscription with an id saves its position
while it matches no events only while the MongoDB subscription model reads quiet positions. When it can't, it logs a
warning saying that a subscription that matches no event keeps the position of the last event it handled. An unnamed
reactor `Flux` saves no position, and its position moves only when it reads an event.

## A Spring Mongo event store sends a commit again when MongoDB can't say whether it succeeded

MongoDB labels a commit it could not answer `UnknownTransactionCommitResult`: the commit reached the server, or may
have, and the reply was lost. The native `MongoEventStore` sends such a commit again for up to two minutes, because it
runs through the driver's `withTransaction`. `SpringMongoEventStore` and `ReactorMongoEventStore` ran through Spring's
`MongoTransactionManager` and `ReactiveMongoTransactionManager`, which retry nothing, so an append that may have been
saved was reported as a failure. A caller that sent it again could write its events twice.

0.35.0 adds `CommitRetryingMongoTransactionManager` in `org.occurrent.eventstore.mongodb.spring.blocking` and
`CommitRetryingReactiveMongoTransactionManager` in `org.occurrent.eventstore.mongodb.spring.reactor`. Each extends
Spring's manager and sends the commit again for up to two minutes from the first attempt. It does not retry a
top-level `MaxTimeMSExpired` error, as the driver doesn't, and neither does it retry a write concern error with that
code, which the driver raises as the same exception.

### Who is affected

- With the Spring Boot starters and no `MongoTransactionManager` or `ReactiveMongoTransactionManager` bean of your own,
  nothing to do. The starters create the new managers.
- With a bean of your own, or an event store you wire by hand, the retry is off until you switch the manager. The blocking
  store logs a warning when it is created, and the reactive one when `EventStoreConfig.Builder.transactionConfig(..)` is
  called with a `ReactiveTransactionManager` that is not the new one. `transactionConfig(TransactionalOperator)` logs
  nothing, because the operator does not expose its manager.

### What to do

Replace the manager and keep its other arguments. Each new class has the constructors of the one it replaces that take a
database factory, with transaction options or with an options resolver and default options:

```java
// Before
MongoTransactionManager transactionManager = new MongoTransactionManager(databaseFactory, transactionOptions);
// After
MongoTransactionManager transactionManager = new CommitRetryingMongoTransactionManager(databaseFactory, transactionOptions);
```

```java
// Before
ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
// After
ReactiveMongoTransactionManager transactionManager = new CommitRetryingReactiveMongoTransactionManager(databaseFactory);
```

When your code starts a transaction and the event store joins it, the commit belongs to your transaction manager, so
that one has to be the new manager for the retry to apply.

An append whose commit is still unknown after two minutes fails as before, and it may or may not be saved.

## A restart follows `resumeBehavior` for `startAt = BEGINNING_OF_TIME` and `startAt = NOW`

Some subscriptions did not start where `resumeBehavior` says on a restart. On the reactive Spring Boot starter, a
`@StreamSubscription` ignored `resumeBehavior`. With `startAt = BEGINNING_OF_TIME` it started at position 0 on every
restart, and with `startAt = NOW` it started at the present on every restart. A reactive `@Subscription`,
`@DcbSubscription`, `@Projection` or `@Snapshot` with `startAt = NOW` did the same, while their replay starts already
followed `resumeBehavior`. On the blocking starter, `startAt = NOW` with `resumeBehavior = SAME_AS_START_AT` continued
from the checkpoint the subscription had stored, where the documentation says it starts at the present again. Both
starters now do what the javadoc and the documentation say.

### Who is affected

- Every application on the reactive starter with a `@StreamSubscription` that sets `startAt` to `BEGINNING_OF_TIME` or
  `NOW`. That includes one that sets `resumeBehavior = DEFAULT` explicitly. A `@StreamSubscription` that does not set
  `startAt` is not affected, and neither is one that sets `resumeBehavior = SAME_AS_START_AT`, except that
  `BEGINNING_OF_TIME` no longer stores a checkpoint.
- Every application on the reactive starter with a `@Subscription`, `@DcbSubscription`, `@Projection` or `@Snapshot`
  that sets `startAt = NOW`, including one that sets `resumeBehavior = DEFAULT` explicitly. One that does not set
  `startAt`, or sets `resumeBehavior = SAME_AS_START_AT`, is not affected.
- Every application on the blocking starter with `startAt = NOW` and `resumeBehavior = SAME_AS_START_AT` on a
  `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@Projection`, `@Saga` or `@Snapshot`.

### What changes

On the reactive starter:

- `BEGINNING_OF_TIME` with `DEFAULT` replays the history the first time. After a restart it continues from the
  checkpoint it stored, so the handler no longer receives every event again.
- `NOW` with `DEFAULT` starts at the present the first time. After a restart it continues from the checkpoint it
  stored, so the handler receives the events written while the application was down. Before, it never received them.
- `SAME_AS_START_AT` starts at `startAt` on every restart, as the documentation says. `BEGINNING_OF_TIME` with
  `SAME_AS_START_AT` now stores no checkpoint, the way a reactive `@Subscription` already behaves.

`NOW` with `SAME_AS_START_AT` still stores a checkpoint after each event, as before, and ignores it on restart. Changing
that subscription to `DEFAULT` later continues from the checkpoint it stored.

The checkpoint is the one the starter's durable subscription model stores. An application that supplies its own
`FluxSubscriptionModel` or `Subscribable` gets no durable model from the starter. A `RegisteringSubscribable`, such as
`SynchronousSubscriptionModel`, does not count. With its own model, the restart behaviour above is that model's: `NOW`
with `DEFAULT` starts where its default start does, and whether `BEGINNING_OF_TIME` with `DEFAULT` continues or replays
again depends on whether it stores a checkpoint.

With the starter's durable model, `NOW` with `DEFAULT` also records where the feed is when a subscription with no stored
checkpoint registers, the way any start from the model default does. The reactive MongoDB model reads it by asking the
server. It asks on every restart, and ignores the answer when a checkpoint is stored. A server that cannot be asked, or
that replies without a clock, therefore makes the model refuse the registration only when no checkpoint is stored. Such
a subscription waits for its start by default when `occurrent.subscription.mode` is `auto`, so the application does not
start. With `startupMode = BACKGROUND`, with mode `manual`, or on a bean built after startup,
the refusal does not hold up startup, and the subscription is not running.
`occurrent.subscription.start-when-no-start-position-can-be-recorded=true` does not change this for the reactive MongoDB
model, which fails when it cannot be asked or replies without a clock. The property is described in
[section 7 of the 0.34.0 guide](upgrading-to-0.34.0.md#7-durablesubscriptionmodel-refuses-a-first-subscription-when-no-start-position-can-be-recorded).

A subscription that handled an event before the upgrade normally has a checkpoint stored already, because the old start
stored one after each event too. The first restart after the upgrade continues from it, and `startAt` is not looked at
again for that subscription id. So changing `startAt` on an existing id has no effect until the id is new or its
checkpoint is deleted. If the position of that checkpoint is no longer in the oplog, the subscription stops, see
[a subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).

On the blocking starter, `NOW` with `SAME_AS_START_AT` starts at the present on every restart and stores no
checkpoint, so the events written while the application was down are not delivered. Before, it delivered them once it
had stored a checkpoint. When it's paused and resumed with no `SubscriptionResumePoints` bean answering for it, it
doesn't replay from a catch-up checkpoint in storage. As for any `NOW` start, an event written shortly before the
subscription starts can still be delivered, and a `SubscriptionResumePoints` bean that answers for the subscription id
still decides where it starts.
It also no longer competes with the other instances of the application, so every instance is delivered every event, as the
documentation says for `SAME_AS_START_AT`. A checkpoint it stored before the upgrade stays in storage and is not used.
`NOW` with `DEFAULT` is unchanged.

### What to do

Nothing, if the subscription is meant to behave as the documentation describes.

On the reactive starter, set `resumeBehavior = SAME_AS_START_AT` on a subscription that feeds an in-memory read model
that has to be rebuilt on every boot. Without it the read model now starts empty and receives only the events after the
last checkpoint. A `NOW` subscription that has to skip the events written during downtime also needs
`SAME_AS_START_AT`.

A reactive `NOW` subscription with the default `resumeBehavior` now needs the MongoDB server to answer when it
registers with no stored checkpoint, and refuses the registration when it cannot be asked.

On the blocking starter, set `resumeBehavior = DEFAULT` on a `NOW` subscription that relied on resuming from its
checkpoint after a restart, or on competing with the other instances. Without it the subscription now misses the events
written while the application was down, and runs on every instance. Delete the old checkpoint of a subscription you
later switch to `DEFAULT`, or it resumes from that position.

## A subscription with no start position resumes from a catch-up's checkpoint

On the blocking starter, a subscription with `startAt` unset didn't resume from a checkpoint that a catch-up stored. A
catch-up stores a `GlobalCheckpoint` or a `CatchupTimeCheckpoint` while it replays, and that checkpoint stays in storage
when the replay stops before it goes live. In 0.34.0 the subscription then opened the change stream at the present, and
skipped every event written before it, the ones written while the application was down included, with no error. It now
resumes the replay from that checkpoint and goes live after it.

### Who is affected

Every application on the blocking starter with one of these, when a catch-up stored its checkpoint:

- a `@Subscription` or `@StreamSubscription` with no `startAt`
- a `@Saga` with no `startAt` that reads the event store rather than a push feed
- a `@Projection` with no `startAt` that reads the event store and doesn't return a `DcbProjection`
- a `@Snapshot` with `startAt = DEFAULT` that doesn't return a `DcbSnapshotView`

Storage holds such a checkpoint when the subscription ran from `startAt = BEGINNING`, a global position or a time, the
application stopped before the replay went live, and the start was changed to `DEFAULT` before the next boot. A
competing-consumer node that takes such a subscription over reads the same checkpoint.

The reactive starter, a `@DcbSubscription`, and a projection or snapshot over DCB already resumed from it.

### What changes

- With a catch-up's checkpoint stored, the subscription replays from it and then goes live, so it's delivered the
  events written since. The catch-up refuses this start in the same cases as a `BEGINNING` start that restarts from
  that checkpoint. In `auto` subscription mode, the default, a `BEGINNING` start replays in the background, but a
  subscription with `startAt` unset can replay before the application finishes starting, unless the annotation sets
  `startupMode = BACKGROUND`. A `resumeSubscription(id)` of such a subscription after a pause replays from that
  checkpoint too, where it ran no replay before.
- With a `TimeBasedCheckpoint` stored that holds no live start, the start is refused, as
  [A catch-up refuses a start that could skip an event committed below it](#a-catch-up-refuses-a-start-that-could-skip-an-event-committed-below-it)
  describes.
- With nothing stored, it starts at the present without a replay, as before.
- With a live checkpoint stored, it resumes from it, as before. When MongoDB no longer has the history back to that
  checkpoint, the start throws `CatchupCannotResumeException` and the application doesn't start, the same as a
  `BEGINNING` start that restarts from its checkpoint. With `startupMode = BACKGROUND` the subscription is paused
  instead, see
  [A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now).
- A `@Projection` with `recordAppliedAppends = true` and no `startAt` no longer gets the startup warning that its
  recordings never reset, since it replays when a catch-up stored its checkpoint.

### What to do

Nothing, unless the handler can't take the events the replay delivers. The replay can run long when the checkpoint is
far behind, and the application doesn't finish starting until it's done. Set `startupMode = BACKGROUND` on the
annotation to start the application first. To start at the present instead, delete the stored checkpoint before the
next boot.

A subscription whose live checkpoint MongoDB no longer has stops the application from starting. Set
`startupMode = BACKGROUND` to have it paused instead, and see
[A subscription whose resume token is gone stops instead of restarting from now](#a-subscription-whose-resume-token-is-gone-stops-instead-of-restarting-from-now)
for how to recover it.

## `ComposedDefaultStartPosition` and its bean are removed

The blocking Spring Boot starter no longer has `ComposedDefaultStartPosition` or the `occurrentComposedDefaultStartPosition`
bean. The bean recorded that a subscription with no start position never replays on the subscription model the starter
builds. The `@Projection(recordAppliedAppends = true)` registrar read it to warn at startup that such a projection's
recordings never reset. Such a subscription now replays when a catch-up stored its checkpoint, see
[A subscription with no start position resumes from a catch-up's checkpoint](#a-subscription-with-no-start-position-resumes-from-a-catch-ups-checkpoint),
so the fact the bean reported is no longer true, and nothing in Occurrent reads it.

### Who is affected

Code that injects the bean or names `ComposedDefaultStartPosition`, and code that calls or overrides
`OccurrentMongoAutoConfiguration.occurrentCompetingDurableSubscriptionModel(..)`, which loses its last parameter. That
code stops compiling.

### What to do

Remove the reference. Nothing replaces it. Code that asked `isDefaultKnownLiveOnlyFor(..)` whether a projection never
replays should treat a projection with no `startAt` as one that can replay.

## A blocking cancel doesn't wait for a subscribe whose `StartAt.dynamic` function runs

In 0.34.0 the blocking `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel`, `CatchupSubscriptionModel` and
`DurableSubscriptionModel` asked a `StartAt.dynamic(..)` function while they held the lock for the subscription id, and
`cancelSubscription` for that id waited for the lock. When the function waited for a cancel of its own id on another
thread, that cancel threw `IllegalStateException` after 10 seconds on the catch-up models, and on
`DurableSubscriptionModel` it waited for as long as the function did. These models now ask the function for their own
model with no lock for the id held.

### Who is affected

Code that subscribes through one of these models with a `StartAt.dynamic(..)` function and cancels the same id from
another thread while the function can still be running. Code that subscribes the same id from two threads at once with
such a function is affected too, and so is code that resumes the id on `DurableSubscriptionModel` from another thread
while the function runs.

### What changes

* A `cancelSubscription(id)` made on another thread while the function of a subscribe of `id` runs returns without
  waiting for that subscribe. The subscribe returns once the function has, subscribes nothing, and the `Subscription` it
  returns answers `false` from `waitUntilStarted(..)`. A cancel the function makes on the subscribing thread doesn't
  stop the subscribe, as in 0.34.0, and neither does a cancel that throws.
* Two subscribes of the same id made at once on `StreamCatchupSubscriptionModel`, `DcbCatchupSubscriptionModel` or
  `CatchupSubscriptionModel` can both ask their function before either has registered. The one that is refused throws
  `DuplicateSubscriptionIdException` once its function has returned. Until then a `pauseSubscription(id)` or
  `resumeSubscription(id)` on another thread waits for it for up to 10 seconds, and then throws
  `IllegalStateException`. A subscribe of an id these models already run throws before it asks the function, as in 0.34.0.
  A `CatchupSubscriptionModel` with both catch-ups still asks the function first to pick the catch-up model, as in
  0.34.0.
* A subscribe on one of these catch-up models waits for the lock for the id before it asks the function, as in 0.34.0,
  and throws `IllegalStateException` when another call for the id holds that lock for longer than 10 seconds. It waits
  for that lock again once the function has returned, and can throw the same way then.
* `DurableSubscriptionModel` asks the function before the model it wraps throws `DuplicateSubscriptionIdException`, as
  in 0.34.0. A `cancelSubscription(id)` made on another thread while that function runs now cancels the subscription
  that runs, and the duplicate then returns a `Subscription` that never starts instead of throwing. A
  `resumeSubscription(id)` of the subscription that runs waits for that function, as the next bullet says.
* A `resumeSubscription(id)` or the new `resumeSubscription(id, startAt)` on `DurableSubscriptionModel` made on another
  thread while the function of a subscribe of `id` runs still waits for that subscribe, but throws
  `IllegalStateException` when the function doesn't return within 10 seconds. In 0.34.0 `resumeSubscription(id)` waited
  for as long as the function did. Its `pauseSubscription(id)` and `stop()` don't wait for the subscribe, as in 0.34.0.

### What to do

* Don't read a returned `cancelSubscription(id)` as a sign that a subscribe of `id` on another thread has returned. Call
  `waitUntilStarted(..)` on the `Subscription` that subscribe returns when you need to know whether it started.
* Keep effects that must happen only for a subscribe that goes ahead out of the function, since a subscribe that then
  throws `DuplicateSubscriptionIdException` or `IllegalStateException` asks it too.
* Don't wait in the function for a `pauseSubscription` or `resumeSubscription` of the same id on a catch-up model, or
  for a `resumeSubscription` on `DurableSubscriptionModel`, on another thread. That call throws `IllegalStateException`
  when the function doesn't return within 10 seconds.
* Wait for the cancel only the first time a subscribe asks the function. A catch-up model asks it again while it holds
  the lock for the id on a resume, on its replay thread during the handover to live delivery, and for the model it
  wraps when that model is another wrapper such as `DurableSubscriptionModel` and the subscribe goes straight to live
  delivery. A cancel made during one of those asks throws `IllegalStateException` after 10 seconds, or during the
  handover waits for as long as the function does.
* A `CatchupSubscriptionModel` with both a stream and a DCB catch-up asks the function first to choose between them for
  a subscribe with no filter, or with a filter that is not a stream, DCB or agnostic one. A cancel made during that ask
  returns, and the subscribe still goes ahead.

The code compiles unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.

## A dynamic `StartAt.now()` over `ReactorDurableSubscriptionModel` fails when the wrapped model answers nothing

`ReactorDurableSubscriptionModel` starts a subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()`
from where the feed was when `subscribe(..)` was called, as `globalCheckpointAsOfNow()` of the wrapped model answers it.
When the read answers nothing, the subscription doesn't start. `waitUntilStarted()` of the subscription `subscribe(..)`
returned fails with an `IllegalStateException`, and an `ERROR` is logged. In 0.34.0 the subscription started from
wherever the feed had reached when the wrapped model got it, and skipped what was written before that.

A read that fails is read again, with a `WARN` for each failed read, and the subscription waits without starting until a
read answers or a cancel of the id or a `shutdown()` ends the wait. When the read has failed before the hand-over to
the wrapped model waits for it, the hand-over reads again at once. After each failed read that the hand-over waited
for, it waits before it reads again. The first wait is between 100 and 150 milliseconds, and the waits about double
from there, up to 5 seconds, which the seventh wait is the first that can reach. When `globalCheckpointAsOfNow()`
throws instead of returning a `Mono`, nothing is read, and `waitUntilStarted()` fails with what it threw.

### Who is affected

A `ReactorDurableSubscriptionModel` over a model that manages named subscriptions and whose `globalCheckpointAsOfNow()`
can answer empty, with a subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()`. That includes a
subscribe that waits for a checkpoint that a cancel of the id deleted to be written back.

A wrapped model of your own that breaks the contract of `CheckpointAwareSubscriptionModel.globalCheckpointAsOfNow()`
is affected too:

* When it throws a `RuntimeException` instead of returning a `Mono`, `waitUntilStarted()` now fails with what it threw.
* When the `Mono` it returns fails on every read, the subscription never starts, and a `WARN` is logged for each
  failed read.

In 0.34.0 the subscription started in both cases, unless the subscribe waited for such a write back.

`ReactorMongoSubscriptionModel` answers empty when MongoDB's reply to `hello` has no `localTime`, so such a subscription
doesn't start over it. It didn't start in 0.34.0 either. The model opened its change stream at the present with the same
request, and stopped the subscription with an `IllegalStateException` that says the reply has no `localTime`. A
subscribe that waited for such a write back read the position again after each failure instead, and neither started nor
failed. With `startWhenNoStartPositionCanBeRecorded(true)` the subscription is handed to the model as `StartAt.now()`.
It stops with that `IllegalStateException` when the model's own request gets a reply without `localTime` too, and
otherwise opens where the feed is then and skips what was written since `subscribe(..)`, as that setting accepts. When
MongoDB can't be reached, the read fails and is read again until MongoDB answers, and the same request to MongoDB failed
the subscription's opening at the present in 0.34.0 too. The model doesn't throw from `globalCheckpointAsOfNow()`. The
Spring Boot starter's `ReactorDurableSubscriptionModel` asks `ReactorMongoSubscriptionModel`, directly or through a
`ReactorCatchupSubscriptionModel`, so the same holds there.

### What to do

To keep the 0.34.0 behaviour, and accept that the subscription skips what was written between `subscribe(..)` and the
moment the wrapped model gets it, set `startWhenNoStartPositionCanBeRecorded(true)` on the config you pass:

```java
new ReactorDurableSubscriptionModel(wrappedModel, checkpointStorage,
        new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
```

The same setting lets a subscription from the subscription-model default start when no checkpoint is stored and the
read answers nothing.

Otherwise, let the subscribe fail and subscribe the id again once the wrapped model can answer. The id is free once
`waitUntilStarted()` has failed, so you don't need to call `cancelSubscription(..)` first.

The setting doesn't start a subscription over a model that throws, or whose `Mono` fails on every read. Make such a
model return a `Mono` instead of throwing. When no read again can give the position, complete the `Mono` empty. The
contract has that answer for a problem the model can't resolve, and over it the setting starts the subscription as
0.34.0 did. Fail the `Mono` only for a problem that a read again can resolve. For a subscription from a
`StartAt.dynamic(..)` function that answers `StartAt.now()`, `ReactorDurableSubscriptionModel` reads again after each
failure. For one from the subscription-model default, or from a function that answers
`StartAt.subscriptionModelDefault()`, it doesn't, so one failure fails its start when no checkpoint is stored.

### Why there is no recipe

The change is in what happens at runtime, and the code compiles unchanged. A recipe that turned on
`startWhenNoStartPositionCanBeRecorded` would accept the skipped events for you, for the subscription-model default
too, on every subscription.

## A dynamic `StartAt.now()` that the wrapped model refuses fails `waitUntilStarted()` instead of `subscribe(..)`

`ReactorDurableSubscriptionModel` hands a subscription whose `StartAt.dynamic(..)` function answers `StartAt.now()` to
a wrapped model that manages named subscriptions once it has read where the feed was at the call, which is after
`subscribe(..)` has returned. So when the wrapped model refuses the subscribe, for a filter it doesn't support for
example, `subscribe(..)` no longer throws. `waitUntilStarted()` of the subscription it returned fails with what the
wrapped model threw, and an `ERROR` is logged. A subscribe of an id that the wrapped model already reports still throws
`DuplicateSubscriptionIdException` at the call.

### Who is affected

Code that catches the wrapped model's refusal around `subscribe(..)` for such a subscription. A subscribe that waits
for a checkpoint that a cancel of the id deleted to be written back was handed over after the call in 0.34.0 already.

### What to do

Handle the refusal where you wait for `waitUntilStarted()`, as 0.34.0 already required for a subscription from the
subscription-model default. Keep handling `DuplicateSubscriptionIdException` at the call too, since it can come from
either place.

## A resumed MongoDB subscription waits for the action that ran when it was paused

`pauseSubscription(id)` and `stop()` on `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` wait at most a
second for a running action. In 0.34.0 a `resumeSubscription(id)` or a `start(true)` after that could call the action
for the next events while the call from before the pause still ran, so two calls for one subscription ran at once and a
later event could be handled before an earlier one. A resumed subscription now opens its change stream and then waits,
with no deadline, until that call returns, before it calls the action again.

### Who is affected

Code that pauses or stops one of these models while an action can run for longer than a second, and then resumes it. A
`CompetingConsumerSubscriptionModel` over one of them pauses and resumes its subscriptions as its lease moves, so it is
affected too.

### What changes

* While the earlier call runs, the resumed subscription calls the action for no event. An action that never returns
  holds the resumed subscription back for as long as it runs, where 0.34.0 delivered the events after it.
* `resumeSubscription(id)` and `start(true)` return, and `waitUntilStarted(..)` answers `true`, once the change stream
  has opened, without waiting for that call.
* The pause still returns after at most a second.

### What to do

Give an action that can block for good a timeout of its own, since a resume no longer gets past it. The code compiles
unchanged, and there is no OpenRewrite recipe, because no signature changes. Nothing stored changes.

## A MongoDB subscription at the present is refused when the server gives it no position

`NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` record a position for a subscription started at the
present before its change stream opens. A replica set and a sharded cluster send MongoDB's operation time with every
reply, and the models record that. On a server whose reply to `ping` has no operation time, the model logs a `WARN`,
reads the change stream once, and records the commit time of the event that read returns, or the resume token of an
empty read. A pause and resume, or a restart, before the first event then opens at that position, where 0.34.0 opened
at a later present and skipped what was written in between.

When that read returns neither an event nor a resume token, the subscription can't start at the present:

* `subscribe(..)` returns without waiting for MongoDB, as in 0.34.0.
* `waitUntilStarted(..)` on the handle it returned throws an `UnsupportedStartAtException`, and so does `start(true)`
  when it starts the subscription. The message says MongoDB gave neither an operation time nor a change stream
  position. The subscription is removed from the model, and the listeners added through
  `StartRefusalReportingSubscriptions` are told, as for a catch-up's checkpoint.
* A `resumeSubscription(id, StartAt.now())` does the read when the resumed change stream opens, and is refused the same
  way. A subscription that had already run is then removed from the model too, and has to be subscribed again.
* With `restartSubscriptionsOnChangeStreamHistoryLost` on, a restart from the present after the history was lost that
  gets neither holds the subscription paused. `isPaused(id)` answers `true`, an error in the log names it, and
  `resumeSubscription(id)` reads the change stream again. Under a `CompetingConsumerSubscriptionModel` the node that
  holds it keeps the lease, so no other node runs the subscription until it is resumed on that node.

In 0.34.0 such a subscription opened at the present.

### Who is affected

Code that subscribes one of these models at `StartAt.now()`, at the model default, or at a `StartAt.dynamic(..)` that
resolves to one of those, on a MongoDB server that sends no operation time. A `StartAt.dynamic(..)` start does the read
at the subscribe whatever it resolves to, and is refused only when it resolves to the present. A replica set and a
sharded cluster aren't affected.

### What to do

Subscribe at a checkpoint instead, such as a `MongoOperationTimeCheckpoint` for the time to start from, and handle the
`UnsupportedStartAtException` where you call `waitUntilStarted(..)` or `start(true)`. The code compiles unchanged, and
there is no OpenRewrite recipe, because no signature changes.

## The reactor `HistoryLossReportingSubscriptions` gains a method

`HistoryLossReportingSubscriptions` in `org.occurrent.subscription.api.reactor` gains an abstract method:

```java
Flux<CloudEvent> subscribeFailingOnHistoryLoss(@Nullable SubscriptionFilter filter, StartAt startAt);
```

A class that implements the interface doesn't compile until it implements the method. The reactor catch-up
subscriptions read their live phase through it, so they stop instead of skipping events when the history is lost
after the subscription has read events.

### What to do

Implement it so that the returned `Flux` fails with `HistoryLostException` whenever your model loses the history it
reads from, as it opens or later, and so that your model never restarts the subscription from the current time.
Retrying another error from the position the subscription has read to is fine. Set `lostFrom` to
the position the subscription had read to, or to the position it opened at when it has read nothing. There is no
default implementation, because a model that can't report a later loss would let a catch-up subscription skip events
without saying so. The rewrite recipe doesn't add the method, since it has to be written for your model.
