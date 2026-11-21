# 151. A capability that acts on a subscription is not found past a wrapper

Date: 2026-10-24

## Status

Accepted. #1251 and #1253. Amended by #1430, which adds `StartRefusalReportingSubscriptions` to the list.

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
* `StartRefusalReportingSubscriptions`, added in #1430, which adds and removes a listener that is told about a start
  that was already refused. The listener can't change what the refusing model did. The
  `CompetingConsumerSubscriptionModel` gives back its own lease when it is told, so the state that changes belongs to
  the model that added the listener. It has to hear the refusal from the MongoDB model under a
  `DurableSubscriptionModel`. A blocking catch-up model implements the capability itself, so `findIn` stops at it. It
  passes the listener on to the MongoDB model under it, and tells the listener through `handleHeldPaused` when it holds a
  subscription paused because the live start of its replay is gone.
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
  subscribes the wrapped model at the stored checkpoint. A MongoDB model refuses a `GlobalCheckpoint` or a
  `CatchupTimeCheckpoint` there, and opens at the present for a time or one an application made up. Storing it would
  make a restart stop, or skip the events written in between.

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

A failed resume can put back a checkpoint older than the one a delivery stored, so the model saves a quiet position only
when it knows which checkpoint is stored and that checkpoint is at or after the position of the latest delivery that
ended. The model notes what every write did, under the lock all checkpoint writes take:

* after a delivery's save or a quiet save returns, its checkpoint is stored
* after a put-back writes, the checkpoint read before the resume is stored
* after a restart position, the position a subscription restarts from once the wrapped model lost its history, is
  written, that position is stored
* after a read, the checkpoint it read is stored
* after a write throws, a refused one included, the stored checkpoint is unknown, since the write may have gone
  through

The stored checkpoint is at or after the latest delivery when it is the checkpoint that delivery stored, a quiet
position saved after it, a restart position stored after it, or the position of a resume made after the delivery read
its event. The model compares when the delivery ends, with the checkpoint stored at that moment. A put-back that wrote
an older checkpoint over the delivery's own then holds quiet saves, and one that wrote the delivery's own checkpoint
again doesn't. A delivery whose action stored no checkpoint, or one that hasn't ended yet, holds quiet saves until a
later delivery stores its checkpoint.

A restart position is at or after every delivery that ended before the model wrote it, also one that stored no
checkpoint. The wrapped model restarts the subscription from that position, so the restarted run delivers only events
after it, and every event before it was either delivered already or is no longer in the history the wrapped model keeps.
The model writes it under the same locks as a put-back, so no delivery ends and no quiet position is saved before the
model has noted what the write left stored.

When a put-back throws, the model reads the stored checkpoint right away. It holds the deliveries' lock from the
put-back to the decision, so no delivery ends in between. While the stored checkpoint is unknown, after that read threw
or after any other write that threw, every quiet save reads it before it writes. When the read returns the last
checkpoint known to be at or after the latest delivery, or the quiet position or restart position whose write threw,
the model saves the quiet position in the same attempt.

The model remembers 2 positions, the last checkpoint known to be at or after the latest delivery and the latest quiet
position or restart position whose write threw. One is enough after a quiet save throws, because the next quiet save
reads before it writes. A restart position whose write threw replaces a quiet position whose save threw before it, with
no read in between. When a read then finds that quiet position stored, quiet saves stay off until a later write stores a
position known to be at or after the latest delivery. The run that lost the history saves no quiet position meanwhile,
since a listener that throws fails the restart, and the next attempt writes a restart position again.

A run of the subscription that a pause stopped can still be in its action for an event, since the MongoDB models stop
waiting for a running handler after one second. It can also still hold a quiet position the wrapped model handed it
before the pause, since `QuietPositionReportingSubscriptions` doesn't promise to wait for a replaced run. One rule
decides every save of an event's checkpoint and of a quiet position, under the lock all checkpoint writes take. The
model numbers each read of the wrapped model and notes how many times the subscription was repositioned when it was
made. A delivery belongs to the read its thread made last, and a quiet position to the read it was handed out before.
The save is dropped when one of these came after that read:

* a resume at a position, a put-back or a restart position, whether its write returned or threw, since each one stores
  where the subscription reads from next
* a save from a later read, or from the same read and a delivery started later, since a run reads and delivers one
  event at a time on one thread, so only a run that a resume replaced saves after a later read has

`HistoryLossListener` doesn't say which thread it's called on, so the rule doesn't depend on it. The save of a delivery
or a quiet position whose read was made before the restart position was written is dropped whichever thread it runs
on, and one whose read was made after it saves. The MongoDB models end the run that lost the history and read again
before they deliver, so the restarted run loses no save. When a delivery that started before the restart position was
written ends, the model goes on comparing the stored checkpoint with the restart position. Without the rule, a stopped
run that returns from its action for e1 after the resumed run stored e2 writes e1 over e2, and a restart would deliver
e2 again while quiet saves wait for the next event. A quiet position handed out before a restart position was stored
would write over it, and once the history no longer holds that quiet position, a restart from it skips the events
written after the restart position.

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
* The save of an event's checkpoint or of a quiet position from a run that a pause stopped and a resume replaced is
  dropped once a resume at a position, a put-back or a restart position came after the read it belongs to, or once the
  resumed run has started saving. It used to write the older checkpoint over the newer one, and a quiet position handed
  out before a restart position was stored could write over the restart position. This holds after the plain
  `resumeSubscription(subscriptionId)` as well as after a resume at a position.
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
