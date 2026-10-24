# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change, what the `UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and
what you have to do by hand. Find the parts of Occurrent you use in the lists below and read those sections.

The sections that can break the build are listed under "Changes that can break the build", and each bullet says when.
The compiler finds those for you. The other sections compile fine and behave differently, so read them first.

### Changes that can break the build

### Sagas

- [A global position is no longer a saga redelivery key](#a-global-position-is-no-longer-a-saga-redelivery-key). A
  saga recognises a redelivery by `streamid` with `streamversion` only. An event that has just a `position` is refused
  under the default `REQUIRED`, and an unreadable `streamid` or `streamversion` fails the delivery. Compiles unchanged.

### DCB

### Subscriptions and catch-up

- [A resume at a position through a `DurableSubscriptionModel` stores the checkpoint](#a-resume-at-a-position-through-a-durablesubscriptionmodel-stores-the-checkpoint).
  `RepositionableSubscriptions.findIn(durableModel)` finds the durable model, which stores the position before it
  resumes there. Compiles unchanged.
- [A capability that acts on a subscription is not found past a wrapper](#a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper).
  `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. Compiles unchanged.

### Spring Boot starter

### Brokers

#### RabbitMQ

#### Kafka

### Snapshots

### The rewrite recipe

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
  nothing is stored. The MongoDB subscription models answer `false` for a `GlobalCheckpoint`, a
  `CatchupTimeCheckpoint`, a time and every other checkpoint their change stream can't open at. They open at the
  present when they subscribe at one, so a stored one would make a restart skip events.

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
position, or you wrote a repositionable subscription model that opens a subscription somewhere else for some
checkpoint. Override `RepositionableSubscriptions.restartsAt(..)` in that model to answer `false` for those, or a
`DurableSubscriptionModel` over it stores them. Nothing stored changes format, and there is no OpenRewrite recipe for this. See
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
