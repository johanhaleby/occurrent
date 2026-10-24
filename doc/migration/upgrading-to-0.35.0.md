# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change, what the `UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and
what you have to do by hand. Find the parts of Occurrent you use in the lists below and read those sections.

The sections that can break the build are listed under "Changes that can break the build", and each bullet says when.
The compiler finds those for you. The other sections compile fine and behave differently, so read them first.

### Changes that can break the build

### DCB

### Subscriptions and catch-up

#### A resume at a position through a `DurableSubscriptionModel` stores the checkpoint

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

The checkpoint is written with the same write condition as every other checkpoint the durable model writes. A node
whose write the checkpoint storage refuses, because another node has written with a newer lease, gets
`CheckpointWriteConditionNotFulfilledException` and the subscription stays paused. When the wrapped model's resume
throws after the write, the checkpoint stored before is put back, or removed when none was, and the exception is
thrown.

A catch-up model whose live delegate is a `DurableSubscriptionModel` now has the position stored when it resumes the
durable model at one. Section 29 of the 0.34.0 upgrade guide says such a resume stores nothing until the first live
position or the quiet save. That section stays as it was released, and over a durable model it no longer holds.

There is nothing to change in your code unless it depends on the checkpoint staying where it was after a resume at a
position. Nothing stored changes format, and there is no OpenRewrite recipe for this. See
[ADR 151](../architecture/decisions/0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md) and
[#1253](https://github.com/johanhaleby/occurrent/issues/1253).

### Spring Boot starter

### Brokers

#### RabbitMQ

#### Kafka

### Snapshots

### The rewrite recipe
