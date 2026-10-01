# 142. A quiet MongoDB subscription moves its position from the empty batch

Date: 2026-09-30

## Status

Accepted. Resolves [#1168](https://github.com/johanhaleby/occurrent/issues/1168). Applies to
`NativeMongoSubscriptionModel`, `SpringMongoSubscriptionModel` and `DurableSubscriptionModel`. Changes one row of
[ADR 141](0141-a-stopped-subscription-model-holds-a-new-subscription-paused-until-it-is-started.md), which that ADR
now shows. `ReactorMongoSubscriptionModel` does the same through
[#1169](https://github.com/johanhaleby/occurrent/issues/1169), and `ReactorDurableSubscriptionModel` doesn't save its
quiet position yet.

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

- Right before each attempt of the action, a retry included, the loop reads whether a pause or a cancel has closed
  the run, and it makes no attempt once it has read that. The read and the call hold no lock, so an attempt that read
  the run open can still start just after a cancel has returned. An attempt that started before can still be running
  when they return, since a cancel doesn't wait and a pause waits a second at most. A document the read returns after the close is left to the resume. A retry that finds the run closed
  ends without calling the `RetryStrategy`'s `onError`, `onRetryableError` or `onAfterRetry` for the attempt it
  skipped, since the action did not fail.
- `DurableSubscriptionModel` reads the version to write an event's checkpoint with after that check and before it
  calls your action, and that read can outlast a pause or a cancel. So through it your action can still be called
  once after a cancel has returned, or after a pause has stopped waiting. No event is lost this way. After a cancel
  the checkpoint of that call is not saved, since the save and the cancel's delete take the same lock. Checking the
  run again between the read and your action needs the wrapped model to tell `DurableSubscriptionModel` whether the
  run that called it is still open. That is new public API, and the call it would stop loses no event.
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
- `DurableSubscriptionModel` saves a quiet position only when no delivery of the subscription is under way in any run
  and the current delivery that started last stored the checkpoint of its event. Item 4 of the rule below states it in
  full, with what makes a delivery current. A delivery counts as under way from before the read of the write version
  until it has finished, whether it stored its checkpoint, declined to or failed, and the quiet save checks this and
  writes under the same lock that a delivery takes when it starts. So a closed run whose read of the write version
  outlasts a pause can't let the resumed run save a quiet position past an event the predicate declined, and a quiet
  position that an earlier run read is not saved while a later run, opened at an earlier position, delivers an event.
  A closed run's action that never returns keeps the quiet save off for as long as it hangs.
- The exception is a checkpoint written after a resume has come. That is after the pause has stopped waiting, or while
  it still waits when another thread resumes the subscription then, since the pause is recorded before its wait. A
  `DurableSubscriptionModel` action that returns that late still saves the checkpoint of its event, and a quiet
  position whose save starts that late is still saved when no delivery is under way and the current one that started
  last stored its checkpoint. The model doesn't know whether a run is still open, only which read each delivery came
  from. Every event up to either position has had its action return, so a subscription that restarts from one can
  receive events again but skips none. Closing it needs the wrapped model to give the checkpoint write for an event
  the same `BooleanSupplier`, which is a change to the public subscribe API.

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
- It saves nothing while the current delivery that started last is of an event the persist predicate declined to
  store, since the quiet position comes after that event. It also saves nothing while an event is being delivered in
  any run, so an action a pause stopped waiting for keeps the save off until it returns. A predicate can decline
  events until a batch the action keeps in memory is written, and a restart from a position after those events would
  lose the batch.
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

A quiet position is saved when all of these hold, and only then:

1. It is the resume token of a read that returned no document, from a run that no pause, cancel or shutdown had
   closed when the position was taken. The same thread runs the action for a document before it reads again, so the
   action has returned for every matching event before that position.
2. The write condition was read before that read, so a node whose lease moved during the read writes with the token
   it held ([ADR 139](0139-a-node-that-gave-up-a-lease-writes-with-the-token-it-held.md)). The store refuses that
   write once the node that holds the lease now has written a checkpoint of its own, and accepts it before then.
3. The interval has passed since the subscribe, the last checkpoint written for an event, the last quiet position
   save that went ahead, or the last failed read of the write condition for one.
4. No delivery of the subscription is under way in any run, and the current delivery that started last since the
   subscribe stored the checkpoint of its event, or none has started. A delivery is current unless a current delivery
   that started before it came from a later read. A delivery stores it when the persist predicate accepts the event,
   no cancel has come and the write succeeds. This must hold when the model asks before the read, and again when the
   position is written.
5. The registration the save was asked for is still current, so no cancel, and no new subscribe of the id, has come
   since.

Item 4 asks about the current delivery that started last rather than the one that finished last. A quiet position
comes from an open run that delivered every event before it on its own thread. An action a pause stopped waiting for,
returning after the resumed run has delivered later events, is for an event the resumed run delivers too before any
quiet position it reads, so it can only repeat that event, and what it stored says nothing about the events before the
quiet position. While such an action runs, its delivery is under way, so a closed run's action that never returns
keeps the quiet save off for as long as it hangs.

A delivery can also start late. The loop checks that the run is open before it calls the action, and
`DurableSubscriptionModel` counts the delivery as started only once its action is called. A closed run's thread can
stall in between, and its action is then called after the resumed run has delivered later events. If the last of those
is one the predicate declines and the late delivery stores its checkpoint, a model that took the late delivery as the
one that started last would allow the quiet save again, and a quiet position the resumed run reads next would be saved
past the declined event. A restart from it loses what a batching action kept in memory for that event.

So the model numbers the reads. Each time the wrapped model asks it before a read, it takes the next number and keeps
it for the thread that asked. A delivery comes from the last read on its own thread, since a run reads and calls the
action for what it read on the same thread. That rests on how a run is executed:

- A run is one task on the executor. A restart after an error, or after lost history, retries inside that task on the
  same thread.
- A resume makes a new run and hands it to the executor as a new task, and a resume is only accepted for a paused,
  so closed, run.
- A closed run's thread that has stalled is still running its task, so an executor that runs each task on a thread
  of its own can't hand that thread to the resumed run. A thread pool, a thread per task and virtual threads all do
  that, and so does the `ThreadPoolTaskExecutor` the Spring model makes when none is given, with or without virtual
  threads. The number is kept on the virtual thread, not on its carrier. The native model takes any
  `ExecutorService`. One that runs a task on the thread that hands it over could run a resumed run on a run's thread
  only from inside an action, whose delivery has then already started and stays under way until the nested run ends.

A closed run reads the event it delivers late before the pause closes it, and the resumed run reads only after the
resume, so every read of the resumed run has a higher number. Every delivery of the open run is therefore current, and
a late delivery of a closed run is not current once the open run has delivered anything. A late delivery that is not
current still counts as under way, and still stores its checkpoint, which can move the stored position back but never
past an event whose action has not returned. It never decides whether the quiet save is allowed. Before the open run
has delivered anything, the late delivery is current. The open run opened at the stored checkpoint, or where the
closed run had read to, and both come before the late event, so the open run delivers that event again before any
read that returns nothing after it, and that delivery is current.

A closed run's thread can also read once more after the resume, before it sees that the run was closed, and then
delivers nothing. A delivery of the open run that comes after such a read still came from a later read than every
current delivery before it, so it is current.

Without a quiet position interval the model is never asked before a read, so no read is numbered and every delivery
is current. Nothing then reads whether the quiet save is allowed.

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

Waiting for a quiet position's write for as long as it takes would close the window described above, for a write that
outlasts the pause. A checkpoint store
that hangs would then block a pause of that subscription, and a pause is what takes a subscription away from a node
that lost its lease.

Taking the last thread that read as the open run, and treating a delivery on any other thread as late, would need no
numbers. A closed run's thread that reads once more after the resume would then make the open run's next delivery
late, and if the delivery before that was of an event the predicate declined, a quiet subscription would get no quiet
position saved until its next event.

Keeping `MessageListenerContainer` and opening a second change stream per subscription only for its resume token
doubles the change streams, and the token of the second one says nothing about what the first has delivered.

### `ReactorMongoSubscriptionModel`

**A subscription with an id reads its change stream through the driver's change stream cursor, one batch at a time,
and takes the resume token from the driver's own cursor.** `ChangeStreamPublisher` has no method that returns the
`postBatchResumeToken`, but the cursor behind it has one. The model opens the change stream with
`BatchCursorPublisher.batchCursor(int)`, the method the driver itself calls when something subscribes to the
publisher, and reads the driver's `AsyncAggregateResponseBatchCursor` from the private field `wrapped` of the
`BatchCursor` it gets back. It only reads that field. Every batch, and the close, go through the public
`BatchCursor.next()` and `BatchCursor.close()`. So the commands MongoDB gets, the server each one goes to and the
driver's own resume after a failover or a network error are the same as with `ReactiveMongoTemplate.changeStream(..)`.

**The model asks for the next batch only once the action's `Mono` has completed for every event of the batch before
it, and looks at the token once a second while it waits.** Within one call to `next()` the driver sends another
`getMore` only after a reply with no document, and the reply that ends the call comes last. So a token that a later
look during the same call finds replaced came with a reply that had no document, and the model moves the
subscription's position to it. The driver decodes a token object of its own from every reply, which lets a look tell
two replies apart even when MongoDB sends the same token twice. The model never moves to the token it finds at the
latest look, because the driver stores a reply before it hands over that reply's documents, so that token can belong
to events the action hasn't had yet.

The driver writes the reply to a plain field, and the model reads it from another thread without synchronization. The
Java memory model promises that a later read sees a write at least as new as an earlier read did only for a volatile
or an opaque read. I rely on HotSpot and the hardware keeping that order for a single field, and no test here proves
it.

**At construction the model checks that the field exists, that it can be read, and that both methods exist, and for
every cursor it checks that the field holds an `AsyncAggregateResponseBatchCursor`.** When a check fails, the model
logs one warning with the reason and reads through `ReactiveMongoTemplate.changeStream(..)` as before. The events
delivered are the same either way, and only the position of a quiet subscription stays at its last event. A test in
the build fails when the route is off on the driver version the build uses.

**The field can be read when the application runs on the module path too.** The 5.8.0 jars of the driver have no
`module-info`, only an `Automatic-Module-Name`, so they are automatic modules, and an automatic module opens every
package. I checked it with a named module on the module path on Temurin 21.0.12.1, where both
`MethodHandles.privateLookupIn(..)` and `setAccessible(true)` work on `BatchCursor.wrapped`. A driver version with a
`module-info` that doesn't open `com.mongodb.reactivestreams.client.internal` makes the check fail, and the model
then logs the warning.

**The reads of a subscription with an id from a subscribe, a resume or a start until a pause, a cancel or a shutdown
are one run, and a new run for the same id reads nothing until every earlier run for that id has ended.** A run has
ended once it is closed and the work it started between two reads, an action's `Mono` or a listener's, has completed
or been cancelled. Closing a run cancels that work. The position moves only while the run is open or that work is
under way. So the `Mono` of a paused run never runs next to a later run's, and the later run opens at a position that
comes after every event an earlier run's action completed for.

**A listener gets the quiet position through the reactor `QuietPositionReportingSubscriptions`.** It mirrors the
blocking capability, with a `Mono` in place of a blocking call. Before each look the model asks each listener for a
function, calls it when the look finds a new quiet position, and hands the subscription nothing more until the `Mono`
it returns has completed. A `CheckpointWriteConditionNotFulfilledException` from that `Mono` ends delivery for the
subscription on this node, as it does in the blocking models.

The `Flux` that `subscribe(filter, startAt)` returns reads through `ReactiveMongoTemplate.changeStream(..)` as before.
Nothing listens for its quiet position.

### What I did not choose for `ReactorMongoSubscriptionModel`

Sending the `aggregate` and `getMore` commands from the model on a cursor of its own fails on a sharded cluster
behind several `mongos` routers. A `getMore` has to reach the `mongos` that opened the cursor, and the model's
commands went to whichever one the driver picked. Against two `mongos` over 20 seconds, that model sent 8 `aggregate`
commands, and 8 of its 21 `getMore` commands failed with `CursorNotFound`. The driver's change stream on the same
cluster sent 1 `aggregate` and 20 `getMore` commands, and none failed. Reading its own cursor also gave up the
driver's resume after a failover.

Moving the position to the token found at the latest look, without waiting to see it replaced, can move past an event
the action hasn't had. The driver stores the reply of a `getMore` before it hands over the documents in it.

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
- The model makes no attempt of the action, a retry included, once it has read that `pauseSubscription(..)` or
  `cancelSubscription(..)` closed the subscription, and an event the change stream has already returned by then is
  left to the resume. An attempt that passed that check can still start just after a cancel returns, and through a
  `DurableSubscriptionModel` your action can still be called once after a pause or a cancel. `pauseSubscription(..)`
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
implements `QuietPositionReportingSubscriptions`.

A subscription with an id on `ReactorMongoSubscriptionModel` handles one batch at a time, so each batch costs a round
trip to MongoDB on top of the time its actions take. `ReactiveMongoTemplate.changeStream(..)` fetched the next batch
while the action ran.

`ReactorMongoSubscriptionModel` reads a private field of the driver, which a driver release can remove. The model
then logs a warning, and a quiet subscription keeps the position of its last event, which the oplog can drop. The test
of the route fails the build on such a driver version, but an application that runs a newer driver than the build
gets only the warning.

While a subscription with an id waits for a batch, the model looks at the token once a second.

A test that stubs `changeStream(..)` on a mocked `ReactiveMongoOperations` no longer reaches a subscription with an id
on `ReactorMongoSubscriptionModel`, since the model opens its change stream from `getCollection(..)`.
