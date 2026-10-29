# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change, what the `UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and
what you have to do by hand. Find the parts of Occurrent you use in the lists below and read those sections.

The sections that can break the build are listed under "Changes that can break the build", and each bullet says when.
The compiler finds those for you. The other sections compile fine and behave differently, so read them first.

### Changes that can break the build

- [A saga applies a stream version that arrives after a higher one](#a-saga-applies-a-stream-version-that-arrives-after-a-higher-one).
  `SagaEnvelope` gains a 14th record component and `SagaRunnerConfig` a 6th. **Breaks the build** if you use a record
  pattern over either.
- [The deprecated `accept(..)` on the projection feeds is removed](#the-deprecated-accept-on-the-projection-feeds-is-removed). `DomainEventFeed` and
  `CatchupProjectionFeed` lose both `accept(..)` overloads, on the blocking and the reactor stack. **Breaks the build**
  if you still call one of them.
- [A replayed event holds the checkpoint to resume from](#a-replayed-event-holds-the-checkpoint-to-resume-from).
  `DcbStartAt` gains the case `DcbStartAt.AtCheckpoint`. **Breaks the build** if you `switch` exhaustively over
  `DcbStartAt` without a `default`.
- [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
  `DcbConsistencyToken` becomes a class, and `DcbEventStream(List, long)` and `DcbDomainEventStream(List, long)` are
  removed. **Breaks the build** if you create a token or a stream, or implement a `DcbEventStore`.

### Sagas

- [A global position is no longer a saga redelivery key](#a-global-position-is-no-longer-a-saga-redelivery-key). A
  saga recognises a redelivery by `streamid` with `streamversion` only. An event that has just a `position` is refused
  under the default `REQUIRED`, and an unreadable `streamid` or `streamversion` fails the delivery. Compiles unchanged.
- [A saga applies a stream version that arrives after a higher one](#a-saga-applies-a-stream-version-that-arrives-after-a-higher-one).
  A saga keeps the highest applied version per stream and event type, and applies a late version instead of skipping
  it. Saga instances store a new `streamMarks` field. Breaks the build only for a record pattern over `SagaEnvelope` or
  `SagaRunnerConfig`.

### DCB

- [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions). **Breaks the build** if you
  use a record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions`.
- [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with).
  A `DcbAppendCondition` with a token read with other criteria throws `IllegalArgumentException`, on every store. A
  token value saved under 0.34.0 isn't valid. **Breaks the build** if you create a token or a stream, or implement a
  `DcbEventStore`.
- [Criteria with tags conflict on their tags](#criteria-with-tags-conflict-on-their-tags). On MongoDB, an append that
  only shares a type with a condition's tagged criteria no longer makes the condition refuse.

### Subscriptions and catch-up

- [A resume at a position through a `DurableSubscriptionModel` stores the checkpoint](#a-resume-at-a-position-through-a-durablesubscriptionmodel-stores-the-checkpoint).
  `RepositionableSubscriptions.findIn(durableModel)` finds the durable model, which stores the position before it
  resumes there. Compiles unchanged.
- [A capability that acts on a subscription is not found past a wrapper](#a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper).
  `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. Compiles unchanged.
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
  A subscribe at a `GlobalCheckpoint` or a `CatchupTimeCheckpoint` throws `UnsupportedStartAtException` instead of
  opening at the present. Compiles unchanged.

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

### Brokers

- [A catch-up over both a stream and a DCB model refuses a position start without a filter](#a-catch-up-over-both-a-stream-and-a-dcb-model-refuses-a-position-start-without-a-filter).
  `CloudEventForwarder.forward(subscriptionId, startAt)` and `DomainEventForwarder.forward(subscriptionId, startAt)` are
  refused at a position checkpoint over such a model. Compiles unchanged.

#### RabbitMQ

#### Kafka

### Snapshots

### The rewrite recipe

- `org.occurrent.MigrateDcbMarkerVersionsRecordPattern_0_35` adds the new bindings to a record pattern over
  `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions`. See
  [DCB reads can resume from marker versions](#dcb-reads-can-resume-from-marker-versions).
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
versions still gets it applied, once. An instance keeps at most as many pending versions per stream and event type as the
window, and when one more fails it drops the smallest and logs an error naming the stream.

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
- `read(criteria, DcbReadOptions.afterVersions(saved))` returns every matching event appended after `saved`, including
  one that committed late below a position an earlier read saw. When the store can't compare `saved` with its own
  markers, for example after they were pruned, it throws `DcbSnapshotInvalidException`, and you read the boundary in
  full.
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
`mv` and is no longer returned by `afterVersions(..)`. A full read still returns it. A store built on
`MongoTemplate.withSession(..)` or `ReactiveMongoTemplate.withSession(..)` doesn't support reading versions.

Three things break, and two of them are compile errors.

- A record pattern over `DcbAppendResult`, `DcbEventStream` or `DcbReadOptions` with the old number of components stops
  compiling. `UpgradeToOccurrent_0_35` adds `var markerVersions` to the first two, and `var versionsRequested` and
  `var afterVersions` to the third.
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
a `DcbCriterion` is the same as an `anyOf(..)` with only that criterion in it. Criteria that select the same events but
are built differently aren't the same, so `types("A", "B")` isn't the same as `anyOf(type("A"), type("B"))`.

The check runs in the condition's constructor, before any store is called, so it works the same on every store, a
`DcbEventStore` of your own included. `GenericDcbApplicationService` reads and appends with the same criteria on both
stacks, so it isn't affected.

### Who is affected

- **A `DcbEventStore` of your own.** It stops compiling where it calls `new DcbConsistencyToken(..)`,
  `DcbConsistencyToken.of(long)`, `value()` or `new DcbEventStream(events, head)`.
- **Code that creates tokens or streams**, such as a test double that returns a `DcbEventStream` or a
  `DcbDomainEventStream`. It stops compiling the same way. A record pattern over `DcbConsistencyToken` stops compiling
  too, since the token is no longer a record.
- **Code that reads with one criteria and appends with another**, passing the token the read returned. It compiles with
  a deprecation warning, and the append throws `IllegalArgumentException` on every store.
- **Code that saved a token value**, in an ETag or a queued command for example. A token value saved under 0.34.0 isn't
  valid on 0.35.0.

### What to do

In a store, pass the read's criteria when you create the token, and read `version()` where you read `value()`:

```java
return new DcbEventStream(events, head, DcbConsistencyToken.of(criteria, version));
```

Compute the version the same way you computed the value for `DcbConsistencyToken.of(long)`. `DcbEventStream(List, long)`
used the head as the token, so `new DcbEventStream(events, head, DcbConsistencyToken.of(criteria, head))` does what it
did. For a `DcbDomainEventStream`, pass the token of the `DcbEventStream` it was built from.

Build a conditional append from the token alone:

```java
DcbEventStream stream = eventStore.read(criteria);
// decide on stream.events()
eventStore.append(newEvents, DcbAppendCondition.failIfEventsMatch(stream.consistencyToken()));
```

Where you passed the criteria and the token separately, drop the criteria. If the append really needs other criteria
than the read, read again with those criteria and use that token.

Don't save a token value to use later. Read again when the command runs.

### Why there is no recipe

The only part a recipe could rewrite is `value()` to `version()`. That rename would compile, and it would keep a token
value saved under 0.34.0 in use, although such a value isn't valid on 0.35.0. `DcbConsistencyToken.of(criteria, version)`
needs the criteria the read was given, and a recipe can't tell from the call site which criteria those were. Rewriting
`failIfEventsMatch(criteria, token)` to `failIfEventsMatch(token)` would compile too, but a condition that used other
criteria than its read would then append on the read's criteria instead of throwing.

## Criteria with tags conflict on their tags

On the MongoDB stores, the check of a conditional append reads only the tag markers of an item with tags, and no longer
the markers of its types. An append that only shares a type with the criteria then no longer makes the check refuse.
Say you read `DcbCriteria.type("SeatReserved").tags(Tag.of("show", "1"))`, and another command appends a
`SeatReserved` tagged `show:2`. A conditional append with the read's token used to throw
`DcbAppendConditionNotFulfilledException`, and now succeeds. The in-memory store already answered that way, so the
stores agree. An item without tags is checked against its type markers as before. The change is the first part of
[#1229](https://github.com/johanhaleby/occurrent/issues/1229), and
[ADR 147](../architecture/decisions/0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md)
describes it.

### What stays the same

- An append of an event that matches the criteria still makes the check refuse, since such an event has every tag of
  the item it matches.
- Every append still increments a marker for each type and each tag of its events. Two appends of one type that run at
  the same time still conflict on MongoDB, whatever their tags.
- Nothing changes on disk, so 0.34.0 and 0.35.0 nodes can append to the same store. A conditional append from a 0.34.0
  node still increments type markers that 0.35.0 no longer reads for criteria with tags. That can make a 0.35.0 check
  refuse an append and the command retry, and it can't let an append through. Going back to 0.34.0 is safe for the
  data.

### Who is affected

- **Code that counted on the refusal.** A command whose conditional append failed because an event of the same type
  with other tags was appended now succeeds. If the decision depends on every event of that type, read with criteria
  that hold only the type.
- **A `DcbEventStore` of your own** that checks conditions on markers, as the MongoDB stores do. The TCK reads
  `type("X").tags(a)`, appends an `X` tagged only `b`, and then appends with the read's token. That append has to
  succeed, so a store that still reads the type marker for an item with tags fails the case.
- **Code that saved a token value.** For criteria with tags, a 0.34.0 token value is a sum over tag and type markers,
  and 0.35.0 compares it against the tag markers only. An old value can then let an append through that should have
  been refused. Read again instead, as
  [A consistency token holds the criteria it was read with](#a-consistency-token-holds-the-criteria-it-was-read-with)
  says.

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

The string means something only to a catch-up subscription. A MongoDB subscription model with no catch-up wrapping it
refuses the checkpoint of a replayed event in a `StartAt` that names it, as
[A MongoDB subscription model refuses a catch-up's checkpoint](#a-mongodb-subscription-model-refuses-a-catch-ups-checkpoint)
describes. From a `StartAt.dynamic(..)` it still opens the change stream at the present.

The checkpoint of a live event holds a change stream resume token, so a restart from it needs the change stream
history to still reach back to it. On MongoDB that is the oplog. Once it no longer does, the catch-up does not take the
subscription over, and the subscription model it wraps decides. A blocking MongoDB subscription model then stops the
subscription and logs an error, or restarts it from the present when it is set to restart after lost history, which
skips the events in between.

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

With `resumeBehavior = SAME_AS_START_AT` on a start that replays, `BEGINNING` or an explicit position, the answer only
moves where the replay starts. The subscription keeps no checkpoint in `CheckpointStorage` once it is live, and the
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

Only a `StartAt` that names the checkpoint is refused. A `StartAt.dynamic(..)` that answers one still opens the change
stream at the present. A blocking `DurableSubscriptionModel` does that when it reads one from its storage for a dynamic
start, which [#1346](https://github.com/johanhaleby/occurrent/issues/1346) tracks.

### Who is affected

Code that subscribes a MongoDB subscription model directly at a checkpoint a catch-up stored, or delivered with a
replayed event. The Spring Boot starters put a catch-up in front of the MongoDB model, so they are not affected. A
resume at such a checkpoint, `resumeSubscription(subscriptionId, startAt)`, still goes on from the position the
subscription has read to.

### What to do

Subscribe through the catch-up subscription model that stored the checkpoint.
