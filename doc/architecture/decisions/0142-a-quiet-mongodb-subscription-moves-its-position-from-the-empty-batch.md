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
also covers handing a quiet position to a listener. A pause called from inside the action does not wait, since the
action cannot return while it waits.

- Once a pause or a cancel has closed the run, no attempt of the action starts on it, a retry included. An attempt
  that started before can still be running when they return, since a cancel doesn't wait and a pause waits a second
  at most. A document the read returns after the close is left to the resume. A retry that finds the run closed
  ends without calling the `RetryStrategy`'s `onError`, `onRetryableError` or `onAfterRetry` for the attempt it
  skipped, since the action did not fail.
- `DurableSubscriptionModel` reads the version to write an event's checkpoint with before it calls your action, and
  that read can outlast a pause or a cancel. Both models therefore implement the new `DeliveryCheckingSubscriptions`,
  and `DurableSubscriptionModel` calls its `checkStillDelivering()` between the read and your action. The loop keeps
  the run that called the action in a thread-local while the action runs, and the call throws once that run is
  closed. The loop treats that like the check before the attempt, so your action doesn't start and the position
  doesn't move past the event. A wrapped model that doesn't implement it, or a call from a thread the model didn't
  call the action on, gets no check.
- `stop()` closes every subscription before it waits, and waits one second for all of them together.
- The pause is recorded before the wait, and the wait runs after the model has let go of its monitor. So a call for
  another subscription, a pause, a resume, a cancel or `subscriptionIds()`, doesn't wait for the paused subscription's action. The
  loop waits for an action, and a model for its executor to shut down, only without the monitor and without any lock
  of the loop. A virtual thread that waits while it holds a monitor keeps its carrier thread on JDK 21 to 23, and
  every other virtual thread that needs that carrier waits with it. A pause, a cancel and a shutdown still close the
  cursor with the monitor held.
- An interrupt doesn't end the wait. The subscription is paused when `pauseSubscription(..)` or `stop()` returns, and
  the interrupt is set on the thread again. `stop()` pauses every subscription even when one pause throws, and then
  throws the first failure.

**A resume the executor rejects is handed to it again.** The subscription counts as running, and a thread of its own
hands the run to the executor again, 100 ms and then up to 2 seconds apart, until the executor takes it, a pause or a
cancel closes the run, or the model or the executor shuts down. A warning is logged on the first rejection and on
every fifth try after it. A resume can come from `start(true)` or from a lease handover, and nothing calls either
again, so a rejection thrown to the caller would leave the subscription paused for good. A `subscribe(..)` the executor
rejects still throws, since its caller gets the exception and the id is not registered. A closed run keeps its thread
until its read returns, which takes up to `maxAwaitTime`, and for as long as an action that outlived the pause still
runs. Every pause and resume, and every stop and start, can add such a run, so no multiple of the number of
subscriptions is always enough. An executor with too few threads delays a resume, and the subscription is not left
paused. An executor the model can't ask whether it is shut down counts as running, so a resume it rejects after it
was shut down is handed to it again until the subscription is paused or cancelled or the model shuts down.

**A run that has been closed never changes the position a later run of the subscription opens at, and never
stores a checkpoint that a later run, or a later subscribe of the same id, starts from, with one exception in
`DurableSubscriptionModel` that can deliver an event again but skips none.** A pause, a cancel and a stop close a run,
and a resume or a start makes a new run of the same subscription.

- An action that returns after its run was closed still moves the position while no new run exists, so a plain
  resume goes on after the event. Once a resume has made the new run, the closed run's write is refused, and a
  subscription resumed at an earlier position receives the event again.
- `DurableSubscriptionModel` deletes the checkpoint in a cancel under the same lock as the checkpoint write for an
  event, so an action that returns after the cancel stores nothing.
- After lost history the model asks MongoDB for the present, and `DurableSubscriptionModel` stores it as the
  checkpoint to restart from. A resume, or a cancel and a new subscribe of the id, can come while the model asks.
  `HistoryLossListener` is therefore also given a `BooleanSupplier` that returns `false` once either has come, and
  `DurableSubscriptionModel` calls it under the lock that `resumeSubscription(..)` and `cancelSubscription(..)` take.
  Once the new run or registration exists, the closed run stores nothing. Before, it could store a present later
  than where the new run started, and a crash then restarted the subscription past events the new run had not
  delivered.
- A pause alone leaves that `BooleanSupplier` returning `true`. Without the stored restart position, the resume would
  open at the position MongoDB no longer has and restart from the present again, skipping the events written during
  the pause.
- Whether a quiet position may be saved depends on whether the persist predicate stored the last event delivered.
  `DurableSubscriptionModel` numbers the deliveries of every run of a subscribe, and only the latest one decides it.
  An action that returns after a resume has delivered later events doesn't change it, so it can't let the resumed run
  save a quiet position past an event the predicate declined.
- The exception is a checkpoint written after a resume has come. That is after the pause has stopped waiting, or
  while it still waits when another thread resumes the subscription then, since the pause is recorded before its
  wait. A
  `DurableSubscriptionModel` action that returns that late still saves the checkpoint of its event, and a quiet
  position whose save starts that late is still saved. The model can't tell which run the action or the save belongs
  to. Every event up to either position has had its action return, so a subscription that restarts from one can
  receive events again but skips none. Closing it needs the wrapped model to tell the checkpoint write for an event
  which run it belongs to.

**A model tells the quiet position to a listener through the new `QuietPositionReportingSubscriptions`.** The model
asks each listener before a read whether it wants the position, and the listener answers with a consumer or with
nothing. After a read that returned no document, the model calls the consumers with the position. A listener that
writes the position under a condition reads that condition when it is asked, which is before the read.

**`DurableSubscriptionModel` saves the quiet position as the subscription's checkpoint.**

- It saves only for a subscription it stores checkpoints for, and only for the registration that was current when it
  was asked. A cancel followed by a new subscribe of the same id is another registration.
- It saves at most once per interval per subscription. The interval starts at `subscribe(..)`. It starts again when a
  checkpoint for an event has been written, when a quiet position's save goes ahead because the registration is still
  current and the save still allowed, whether or not the write then succeeds, and when the write condition for a
  quiet position can't be read. So a subscription that stores a checkpoint for an event at least once per interval
  gets no extra write. The default is one minute,
  `saveQuietPositionEvery(Duration)` changes it and `neverSaveQuietPosition()` turns the save off.
- It saves nothing while the last event delivered is one the persist predicate declined to store, since the quiet
  position comes after that event. A predicate can decline events until a batch the action keeps in memory is
  written, and a restart from a position after those events would lose the batch.
- Before the first event after a subscribe it saves whatever the predicate is. A read that returns nothing then comes
  after no event the subscription hasn't been given. After a restart, the events the predicate declined come after the
  stored checkpoint, so the change stream returns them before any empty read, and the first one the predicate declines
  stops the save again.
- It writes with the same `CheckpointWriteCondition` as a checkpoint for an event, read before the read that
  returned the position.
- The save holds the lock per subscription id that `subscribe(..)`, `resumeSubscription(..)`, `cancelSubscription(..)`
  and the save after lost history also take. So a save is never written after a cancel has deleted the checkpoint, or
  after a new subscribe of the id has replaced the registration. The checkpoint write for an event takes only a lock
  of its own for each subscribe, which a cancel also takes before it deletes the checkpoint. The lock is one per id,
  and exists only while a call holds it or waits for it, so a checkpoint store that hangs during a save makes only
  calls for that id wait. Neither lock is a monitor, for the reason the loop waits without one. A virtual thread that
  waits for the checkpoint store while it holds a monitor keeps its carrier thread on JDK 21 to 23.
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
3. The interval has passed since the subscribe, the last checkpoint written for an event, the last quiet position
   save that went ahead, or the last failed read of the write condition for one.
4. The persist predicate stored the last event delivered, or no event has been delivered since the subscribe.

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
well below the oplog window. A subscription whose persist predicate declines some events, such as an `EveryN` with
`n` above 1, gets no quiet position saved after a declined event until the predicate stores one. If it stays quiet
for longer than the oplog window after that, a restart still ends in lost history.

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
- No attempt of the action starts after `pauseSubscription(..)` or `cancelSubscription(..)` has returned, a retry
  included, and an event the change stream has already returned by then is left to the resume. Through a
  `DurableSubscriptionModel` the same holds for your action. `pauseSubscription(..)`
  also waits up to a second for an action that is running, and `stop()` one second for all of them. Before, they did
  not wait for the action, and an event could be delivered after they had returned.

`SpringMongoSubscription` no longer wraps a Spring Data `Subscription`. Its `protected` constructor is gone, and
neither it nor `SpringMongoSubscriptionModel` has an `equals` and `hashCode` of its own. `SpringMongoSubscriptionModel.subscribe(..)` after `shutdown()` throws
`IllegalStateException`.

`NativeMongoSubscriptionModel.pauseSubscription(..)` and `stop()` no longer wait for a read that returns nothing.
Before, each pause waited up to a second for it, so stopping a model with many quiet subscriptions took up to a second
per subscription.

`CompetingConsumerSubscriptionModel` calls the wrapped model's `pauseSubscription(..)` while it holds its own monitor.
So when it pauses a subscription whose action is running, its calls for other subscriptions still wait up to a second.

`NativeMongoSubscriptionModel` restarts a change stream whose cursor the driver reports as no longer open when the
model did not close it. Before, the subscription ended without a log line above debug and stayed listed as running.

Both models evaluate a `StartAt.dynamic(..)` position once for each opening of the change stream. The native model
also evaluated it right after `subscribe(..)`, to find out whether it was the present, and the supplier a
`DurableSubscriptionModel` passes then read the checkpoint store twice. Now the present is recorded without
evaluating the supplier, and the supplier uses the recorded present when it answers `StartAt.now()`.

A subscription model of your own that a `DurableSubscriptionModel` wraps gets no quiet position saved unless it
implements `QuietPositionReportingSubscriptions`. Through such a model your action can still start after a pause or a
cancel has returned, while `DurableSubscriptionModel` reads the write version, unless the model implements
`DeliveryCheckingSubscriptions`.
