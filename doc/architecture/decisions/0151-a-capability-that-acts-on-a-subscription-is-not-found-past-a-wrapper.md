# 151. A capability that acts on a subscription is not found past a wrapper

Date: 2026-10-24

## Status

Accepted. #1251 and #1253.

Amends [ADR 118](0118-a-subscription-model-capability-marker-replaces-object-in-the-of-lookups.md) and
[ADR 119](0119-subscription-model-capability-gains-instance-side-lookups.md), where both
say the blocking lookups walk a `SubscriptionModelWrapper` chain for every capability.

## Context

Since 0.33.0 the blocking `SubscriptionModelCapability.capability(type)` answers `this` when the model implements
`type`, and otherwise unwraps one `SubscriptionModelWrapper` and asks the wrapped model. The static `findIn` methods,
`RepositionableSubscriptions.findIn` among them, call it. So a lookup on a wrapper that doesn't implement a capability
answers the model beneath the wrapper, and the caller then acts on that model as if the wrapper weren't there.

For a capability that only reads, that is what the caller wants. `IntrospectableSubscriptions.subscriptionIds()`
asked of a `DurableSubscriptionModel` is answered correctly by the model it wraps. For a capability that acts on a
subscription it is wrong, because a wrapper that keeps state for a subscription never hears about the action:

* `RepositionableSubscriptions.findIn(competingConsumerModel)` answers the model the
  `CompetingConsumerSubscriptionModel` wraps. A resume at a position through it resumes the subscription while the
  competing consumer still has it recorded as paused and holds no lease for it (#1251).
* `RepositionableSubscriptions.findIn(durableModel)` answers the MongoDB model under the
  `DurableSubscriptionModel`. A resume at a position through it moves the subscription without storing a checkpoint,
  so a restart before the next event resumes from the older stored checkpoint (#1253).
* `capability(CheckpointAwareSubscriptionModel.class)` on the stack the Spring Boot starter builds, a
  `CompetingConsumerSubscriptionModel` over a `CatchupSubscriptionModel` over a `DurableSubscriptionModel`, answers the
  `DurableSubscriptionModel`. A `subscribe` through that answer goes around both the catch-up and the competing
  consumer.

The reactor stack has no `SubscriptionModelWrapper`, so its `capability(type)` never unwraps and none of this applies
to it.

## Decision

### 1. The blocking default unwraps a wrapper only for a capability that observes

`capability(type)` answers `this` when the model implements `type`, as before. Past that, it unwraps a
`SubscriptionModelWrapper` only when `type` is one of these:

* `IntrospectableSubscriptions`, `ReplayAwareSubscriptions` and `HistoryRetainingSubscriptions`, which answer
  questions about subscriptions.
* `QuietPositionReportingSubscriptions`, which adds and removes a listener that is told positions. Another listener
  changes no wrapper's state.
* `Pushable`, which takes events in at the bottom of the stack. They reach every wrapper's action on the way up as
  usual.
* `HistoryLossReportingSubscriptions`. Its listener can answer `TakeOver` and so act on a subscription, which makes it
  the exception to the rule. It stays unwrapped because it is the SPI a catch-up model uses to hear about lost history
  from the MongoDB model under a `DurableSubscriptionModel`, and the `DurableSubscriptionModel` keeps its own state
  through its own listener in the same list. Making it answer only for itself would mean every wrapper forwarding it,
  and would change how a catch-up model takes over a subscription.

Every other capability acts on a subscription and is answered only by a model that implements it:
`Subscribable`, `CancellableSubscriptions` and every type built on it (`SubscriptionModelLifeCycle`,
`SubscriptionModel`, `CheckpointAwareSubscriptionModel`, `StreamSubscriptionModel`, `DcbSubscriptionModel`), and
`RepositionableSubscriptions`. `findIn` keeps calling `capability(type)` and follows the same rule.

A wrapper that keeps state for a subscription is then never bypassed by default. A wrapper is found for an acting
capability by implementing it, and its implementation updates its own state before forwarding.

The rule goes in the default method rather than in a `capability` override on each wrapper that keeps state. Overrides
on `CompetingConsumerSubscriptionModel` and `ManualStartSubscriptionModel` would fix the two shipped wrappers and leave
the same lookup going around every wrapper written later, in Occurrent or in an application.

### 2. `CompetingConsumerSubscriptionModel` answers empty for `RepositionableSubscriptions`

This follows from decision 1 with no code in `CompetingConsumerSubscriptionModel`. A resume at a position through a
competing consumer has to decide what a node without the lease does, and [ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md)
decision 7 names the same question as open work for releasing a quarantined saga across nodes. Both get one answer, in
#1304. Going from empty to an implementation later adds a capability and breaks no caller.

### 3. `DurableSubscriptionModel` implements `RepositionableSubscriptions`

`resumeSubscription(subscriptionId, startAt)` stores the position and then resumes the wrapped model at it. The whole
call holds the lock `DurableSubscriptionModel` already takes per subscription id for its plain `resumeSubscription`.

* A subscription subscribed without checkpointing is forwarded to the wrapped model and nothing is stored.
* A subscription the wrapped model doesn't report as paused is forwarded too, and nothing is stored. The wrapped model
  then throws its own exception for an id it doesn't know or one that is already running, and no checkpoint is stored
  for either.
* `StartAt.checkpoint(..)` stores that checkpoint, then resumes there.
* `StartAt.now()` asks the wrapped model's `globalCheckpoint()` for the current position, stores it, then resumes
  there, so the stored and the actual start agree. When the wrapped model answers `null`, nothing is stored, the
  subscription stays paused and the call throws `IllegalStateException`.
* `StartAt.subscriptionModelDefault()` does what the plain `resumeSubscription(subscriptionId)` does, since the stored
  checkpoint is this model's default.
* A dynamic `StartAt` is resolved once and then handled as above. One that resolves to `null` throws
  `IllegalArgumentException`.
* A checkpoint the wrapped model can't subscribe at is forwarded to the wrapped model and nothing is stored. A restart
  subscribes the wrapped model at the stored checkpoint, and a MongoDB model opens at the present for a checkpoint its
  change stream can't open at, such as a `GlobalCheckpoint`, a `CatchupTimeCheckpoint`, a time or one an application
  made up. Storing it would make a restart skip the events written in between.

The durable model can't tell those checkpoints apart itself, because only the wrapped model knows what it can open at.
So blocking `RepositionableSubscriptions` gets a default method, `restartsAt(Checkpoint)`, that answers `false`. The
MongoDB models override it and answer `true` for every checkpoint their change stream can open at, and the durable
model stores a position only when the wrapped model answers `true`. A wrong `false` makes a restart resume from the
checkpoint stored before, which delivers events again, and a wrong `true` makes it skip events, so a model that doesn't
override the method has nothing stored, as in 0.34.0. The durable model answers what its wrapped model answers. The
reactor `RepositionableSubscriptions` doesn't get the method, because `ReactorDurableSubscriptionModel` stores nothing
on a resume at a position.

The store comes first. The subscription is paused and the id lock is held, so no event's checkpoint can be written in
between. Resuming first would let an event's checkpoint be written and then overwritten with an older position.
Resuming first and then storing only when the stored checkpoint is still the one read before would avoid that, but
`CheckpointWriteCondition` has no such case. It has `any()`, `notOlderThan(..)` and `ifAbsent()`, and a fourth case
would have to be evaluated by every `CheckpointStorage`, the four Occurrent ships and every one an application wrote.

The store uses the same write condition as every other checkpoint write in the model (ADR 116, ADR 139). A refused
write throws `CheckpointWriteConditionNotFulfilledException`, nothing is resumed and the subscription stays paused,
because a node another node has fenced out must not move the position.

When the wrapped resume throws after the store, the model puts back the checkpoint stored before, under the same lock
and with the write condition the new position was stored with, and the exception propagates. A checkpoint another
lease holder wrote in the meantime then refuses the put-back. A failed reposition leaves the stored position as it
was before the call. A catch-up model that shares the checkpoint storage, as it does in the Spring Boot starter, replays
from the stored checkpoint. Had the new position stayed, a later resume would make the catch-up model go live at it
and skip the events written between its replay position and the new one. The MongoDB models pause the subscription
again before they throw, so no event was delivered from the new position when the old checkpoint goes back. A failure
to put the checkpoint back is added to the thrown exception as a suppressed exception.

When nothing was stored before, the new checkpoint stays. Deleting it could remove a checkpoint that another node, or
the resumed subscription's own events, wrote in the meantime. With nothing stored, a restart subscribes the wrapped
model at its default, which for the MongoDB models is the present.

`capability(RepositionableSubscriptions.class)` on a `DurableSubscriptionModel` answers the model itself when the
wrapped model can reposition and empty otherwise, as `ReactorDurableSubscriptionModel` and the catch-up models already
do. `resumeSubscription(subscriptionId, startAt)` throws `UnsupportedOperationException` when the wrapped model can't.

## Consequences

* `RepositionableSubscriptions.findIn(..)` on a `CompetingConsumerSubscriptionModel` or a
  `ManualStartSubscriptionModel` answers empty. In 0.33.0 and 0.34.0 it answered the model beneath. There is no way to
  resume a competing-consumer subscription at a position until #1304 is done. Over a `DurableSubscriptionModel`,
  pausing through the competing consumer and resuming through it starts from the stored checkpoint.
* `RepositionableSubscriptions.findIn(..)` on a `DurableSubscriptionModel` answers the durable model, and a resume at
  a position through it stores the checkpoint first, unless the wrapped model's `restartsAt(..)` answers
  `false` for it. A catch-up model over a durable model therefore stores the position at each live hand-over
  it repositions, which is the position it already stores for itself when both share one checkpoint storage, as they
  do in the Spring Boot starter.
* `capability(..)` for `Subscribable`, `CancellableSubscriptions` or a model type built on them answers empty on a
  wrapper that doesn't implement it, where it used to answer the model beneath.
* A `SubscriptionModelWrapper` of an application's own that should be found for an acting capability has to implement
  it. Observing capabilities are still found through it.
* ADR 119 called these lookups purely additive because they hadn't shipped. They shipped in 0.33.0, so this is a
  breaking change, with a changelog entry and a section in the 0.35.0 upgrade guide. The only new method is a default one,
  so there is no OpenRewrite recipe.
* A `DurableSubscriptionModel` over a repositionable subscription model of an application's own stores nothing on a
  resume at a position, as in 0.34.0, until that model overrides `restartsAt(..)` to answer `true` for the
  checkpoints it starts reading at.
* No stored document changes format. The `DurableSubscriptionModel` writes the same checkpoint with the same write
  condition, only earlier. A 0.34.0 node reads a checkpoint stored at a resume as an ordinary checkpoint.
* The lock order on the starter's stack is the catch-up model's claim, then its hand-over lock, then the durable
  model's id lock, then the checkpoint storage or the MongoDB model. A catch-up hand-over took the durable model's id
  lock under its own locks before this change too, through the plain resume.
