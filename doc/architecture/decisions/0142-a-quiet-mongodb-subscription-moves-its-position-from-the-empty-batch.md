# 142. A quiet MongoDB subscription moves its position from the empty batch

Date: 2026-09-30

## Status

Accepted. Resolves [#1168](https://github.com/johanhaleby/occurrent/issues/1168). Applies to
`NativeMongoSubscriptionModel`, `SpringMongoSubscriptionModel` and `DurableSubscriptionModel`. Changes one row of
[ADR 141](0141-a-stopped-subscription-model-holds-a-new-subscription-paused-until-it-is-started.md), which that ADR
now shows. `ReactorMongoSubscriptionModel` is [#1169](https://github.com/johanhaleby/occurrent/issues/1169).

## Context

A MongoDB subscription's position moved only when an event matched its filter. A subscription that matched nothing
for a while kept the resume token of the last event that did match, or the operation time it started at. MongoDB keeps
the oplog for a limited time. Once the oplog had dropped that position, a pause and a resume, a process restart or a
lease handover opened the change stream at a position that was gone, and MongoDB answered with
`ChangeStreamHistoryLost`. The subscription then restarted from the present or stopped, depending on
`restartSubscriptionsOnChangeStreamHistoryLost`, although it had missed nothing up to then.

MongoDB sends a resume token with every batch of a change stream, also with a batch that has no document for the
subscription. That token is a position after every event the change stream has looked at, so it moves while the
subscription matches nothing.

`SpringMongoSubscriptionModel` read its change streams through Spring Data's `MessageListenerContainer`, which hands
over documents and nothing for an empty batch. The container was also behind the defects listed under Consequences.

## Decision

**`NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` read a change stream with the same cursor loop.**
The loop is `ChangeStreamSubscriptions` in the new `occurrent-subscription-mongodb-common-blocking-change-stream`
module, in an `internal` package. It holds what the native model did before, which is the subscriptions and their
positions, pause, resume, stop, start, cancel, the restart with the `RetryStrategy` and the handling of lost history.
Each model passes its calls on to it and supplies what differs. For the Spring model that is `MongoTemplate` for
opening a change stream and running a command, the pipeline Spring Data builds for a filter, the executor, and
`SmartLifecycle`. `SpringMongoSubscriptionModel` no longer uses `MessageListenerContainer`.

**The loop reads with `tryNext()`, and a read that returns no document moves the subscription's position to the
cursor's resume token.** The position is the one a pause and a resume, or a restart of the change stream, open at. It
moves only while the run that read it is still open, so a run that a pause has closed cannot replace the position a
resume was given.

**`pauseSubscription(..)` waits up to a second for an action that is running, and not for a read.** A read that
returns nothing waits on the server for up to `maxAwaitTime`, and closing the cursor does not end it sooner. The wait
also covers handing a quiet position to a listener. Once the pause has returned, the run starts no action, and a
document the read returns after that is left to the resume. `stop()` closes every subscription before it waits for
any of them. A pause called from inside the action does not wait, since the action cannot return while it waits.

**A model tells the quiet position to a listener through the new `QuietPositionReportingSubscriptions`.** The model
asks each listener before a read whether it wants the position, and the listener answers with a consumer or with
nothing. After a read that returned no document, the model calls the consumers with the position. A listener that
writes the position under a condition reads that condition when it is asked, which is before the read.

**`DurableSubscriptionModel` saves the quiet position as the subscription's checkpoint.**

- It saves only for a subscription it stores checkpoints for, and only for the registration that was current when it
  was asked. A cancel followed by a new subscribe of the same id is another registration.
- It saves at most once per interval per subscription. The interval starts at `subscribe(..)` and starts again with
  every checkpoint saved for an event and with every attempt to save a quiet position, so a subscription that
  receives events gets no extra write. The default is one minute, `saveQuietPositionEvery(Duration)` changes it and
  `neverSaveQuietPosition()` turns the save off.
- It writes with the same `CheckpointWriteCondition` as a checkpoint for an event, read before the read that
  returned the position.
- The save holds the lock per subscription id that `subscribe(..)`, `resumeSubscription(..)`, `cancelSubscription(..)`
  and the save after lost history also take. So a save is never written after a cancel has deleted the checkpoint, or
  after a new subscribe of the id has replaced the registration. The checkpoint write for an event takes no lock.
- A write the condition refuses is thrown to the wrapped model, which ends delivery for that subscription on that
  node, as it does when the write for an event is refused. Any other failure is logged as a warning and tried again
  after the interval, since nothing is lost by a quiet position that was not saved.

**`SpringMongoSubscriptionModel` records the present when `subscribe(..)` is called, as the native model does.** A
subscription at `StartAt.now()`, or with the model default, that is made while the model is stopped or created with
`autoStartup(false)` starts at the operation time MongoDB answers with when `subscribe(..)` asks. Before, it started
wherever the change stream was when `start()` opened it, and missed the events written in between.

### The rule for the saved position

A quiet position is stored only when all of these hold:

1. It is the resume token of a read that returned no document, from a run that no pause, cancel or shutdown had
   closed when the position was taken. The same thread runs the action for a document before it reads again, so the
   action has returned for every matching event before that position.
2. The write condition was read before that read, so a node whose lease moved during the read writes with the token
   it held ([ADR 139](0139-a-node-that-gave-up-a-lease-writes-with-the-token-it-held.md)). The store refuses that
   write once the node that holds the lease now has written a checkpoint of its own, and accepts it before then.
3. The interval has passed since the last checkpoint write or attempt for that subscription.

A pause that comes while a quiet position is being written waits up to a second for the write, as it does for an
action. A write that takes longer can finish after the pause has returned, and after another node has taken the lease.
With a fencing token the store refuses it once that node has written a checkpoint, and accepts it before then. Without
a token, the write can replace a newer checkpoint with the older quiet position. Either way the stored position never
moves past an event whose action has not returned, so a subscription that resumes from it can receive events again
but skips none.

The resume token of an empty batch can come before an event written at the same cluster time as the last event the
change stream read. A subscription that opens at it can then receive that event a second time. That is a duplicate,
since every event the action has not returned for still comes after the token.

### What I did not choose

Saving the quiet position on every empty read would write once per `maxAwaitTime` for every quiet subscription, which
is once a second with the driver's default.

Asking MongoDB for its operation time on a timer, and saving that, needs no access to the cursor. The operation time
can be later than what the change stream has delivered, so a subscription restarted from it can miss an event.

Waiting for a quiet position's write for as long as it takes would close the gap described above. A checkpoint store
that hangs would then block a pause of that subscription, and a pause is what takes a subscription away from a node
that lost its lease.

Keeping `MessageListenerContainer` and opening a second change stream per subscription only for its resume token
doubles the change streams, and the token of the second one says nothing about what the first has delivered.

## Consequences

A quiet subscription behind a `DurableSubscriptionModel` costs one checkpoint write per interval. Keep the interval
well below the oplog window.

A subscription that matches nothing resumes and restarts from a position the oplog still has, as long as the process
is down, or the subscription paused, for less than the oplog window. Longer than that still ends in lost history.

These change for `SpringMongoSubscriptionModel`, and each was a defect:

- An event whose action still throws after the `RetryStrategy` gave up is no longer skipped. The change stream is
  restarted from the event before it, so the event is delivered again. With a strategy that gives up, the restart
  gives up the same way, and the subscription then stops until it is paused and resumed.
- A pause or a cancel that comes before the change stream has opened stops it.
- After lost history with `restartSubscriptionsOnChangeStreamHistoryLost` turned off, the model forgets the
  subscription, as the native model does. `isRunning(id)` and `isPaused(id)` return `false`, and the id can be
  subscribed again.
- The executor the model makes by default, or for `useVirtualThreads()`, is made per model and shut down with it.
- A subscription at the present made while the model is stopped receives the events written before `start()`.
- No action starts after `pauseSubscription(..)` or `cancelSubscription(..)` has returned, and an event the change
  stream has already returned by then is left to the resume. `pauseSubscription(..)` and `stop()` also wait up to a
  second for an action that is running. Before, they did not wait for the action, and an event could be delivered
  after they had returned.

`SpringMongoSubscription` no longer wraps a Spring Data `Subscription`. Its `protected` constructor is gone, and
neither it nor `SpringMongoSubscriptionModel` has an `equals` and `hashCode` of its own. `SpringMongoSubscriptionModel.subscribe(..)` after `shutdown()` throws
`IllegalStateException`.

`NativeMongoSubscriptionModel.pauseSubscription(..)` and `stop()` no longer wait for a read that returns nothing.
Before, each pause waited up to a second for it, so stopping a model with many quiet subscriptions took up to a second
per subscription.

`NativeMongoSubscriptionModel` restarts a change stream whose cursor the driver reports as no longer open when the
model did not close it. Before, the subscription ended without a log line above debug and stayed listed as running.

Both models evaluate a `StartAt.dynamic(..)` position once for each opening of the change stream. The native model
also evaluated it right after `subscribe(..)`, to find out whether it was the present, and the supplier a
`DurableSubscriptionModel` passes then read the checkpoint store twice. Now the present is recorded without
evaluating the supplier, and the supplier uses the recorded present when it answers `StartAt.now()`.

A subscription model of your own that a `DurableSubscriptionModel` wraps gets no quiet position saved unless it
implements `QuietPositionReportingSubscriptions`.
