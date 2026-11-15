# Runbook: a quarantined saga instance

## Who this is for

You run a saga on 0.34.0 or later, an instance of it has stopped, and you want to know what it stopped on and what to
do with it. The `SagaStatus.QUARANTINED` state is new in 0.34.0, so nothing before that release can produce one.

If you are deciding whether to keep quarantine on, and how to upgrade a cluster from 0.34.0, that question is in
[Upgrading to 0.35.0](../migration/upgrading-to-0.35.0.md#a-quarantined-saga-instance-can-be-released-and-quarantine-is-on-by-default).
[Section 8 of Upgrading to 0.34.0](../migration/upgrading-to-0.34.0.md#8-a-saga-instance-that-keeps-failing-can-be-quarantined-and-four-saga-types-change-with-it)
covers the two cases where a saga never quarantines anything. This runbook starts from the point where you already
have one.

## Why an instance stops

Each saga gets one subscription, and that subscription delivers the events for all of its instances. When handling an event for one instance
throws, the executor rethrows, and on a subscription model that offers the event again the instance tries again. Usually that is
the instance's `evolve`, its `react`, or its command dispatcher, and it can also be the read that loads the instance,
which is how an instance whose state no longer decodes ends up here. Up to 0.33.0 that went on without limit.

What the failing event holds up in the meantime is decided by whatever feeds the subscription, and the javadoc on
`SagaStatus.QUARANTINED` says what that can be.

Quarantine is off by default in 0.34.0, so there an instance can only be quarantined if you turned it on, with
`SagaRunnerConfig.withQuarantineAfter(..)` or `occurrent.saga.quarantine-after` on the annotation path. From 0.35.0 it
is on by default, after 5 minutes, and `SagaRunnerConfig.disableQuarantine()` or
`occurrent.saga.quarantine-enabled=false` turns it off. With quarantine on, the executor times how long the event it is
failing on has been failing. Once that reaches the budget, the instance can move to
`SagaStatus.QUARANTINED`, and when it does the executor stops rethrowing.

Reaching the budget is not enough on its own. The javadoc on `SagaStatus.QUARANTINED` lists what else has to hold, so
an instance past its budget can still be `ACTIVE`. Read its status rather than working it out from the time.

A quarantined instance applies no further events and fires no timers, and its redelivery watermarks stop moving, so
nothing it skipped is recorded as handled. The subscription still delivers those events, and the runner still reads
the instance before skipping each one, which step 6 comes back to.

From 0.35.0 the runner also records the key of each input it skips, in the order they arrive, as a record of its own
outside the instance's document, and step 5 releases the instance by replaying them. In 0.34.0 nothing brings an instance out of quarantine, and deleting it in
step 6 is the only ending.

### When no instance stopped at all

One thing that looks like a stuck instance is not one. The saga reads the CloudEvent and asks its id extractor which
instance the event belongs to before anything else happens, so a converter or an id extractor that throws gives the
runner no instance to stop. The runner never lets the subscription past that event, whatever the budget. An event the saga cannot route may still belong to an
instance, and once the subscription moved past it the next event for that instance would mark the instance as having
handled it, so it could not be fed to the saga again.

Nothing is written for it, so `findByStatus(QUARANTINED, ..)` does not list it and the rest of this runbook does not
apply. What you get is a `WARN` from `SagaExecution` on the first failure and an `ERROR` once per interval after that,
each saying the saga could not work out which instance the event belongs to and logging what stopped it. That interval
is the quarantine budget when one is configured, and a fixed five minutes when it is not, so this `ERROR` still
fires on a subscription model this saga cannot quarantine anything on, such as a push feed, including one a broker
source feeds, for as long as each offer of the event comes within twice that interval of the one before. A model that
offers it further apart gets a `WARN` on every offer, because the saga forgets an event it hasn't been offered for
that long. A model that does not offer a refused delivery again gets only the first `WARN`, and `DeliveryFailurePolicy` is where a broker source's choice is configured. The event is named by its
redelivery key when it has one, and otherwise by its CloudEvent
id and `source` attribute, which stay the same from one delivery of it to the next. Where the event is offered again, repair the
converter or the id extractor and the saga applies it in the order it was written, with nothing to feed to it again.

`OutOfMemoryError` is the other thing that never quarantines. It says the JVM ran out of heap while some instance held
the thread rather than anything about that instance, so it is rethrown and the instance keeps its state. Every other
failure counts, including an `Error`, so a recursive `evolve` raising `StackOverflowError` quarantines like any other
failure does.

## How you find out

Three log lines from `org.occurrent.dsl.saga.blocking.SagaExecution`, all naming the saga's subscription id and the
instance's saga id, and all logging the exception with its stack trace. There is no metric and no health indicator for
any of this, in 0.34.0 or 0.35.0.

The first `WARN` says the instance failed on an event and leaves it to whatever feeds the subscription whether the
event is offered again. The duration in that line is the budget it has to exhaust, not how long it has been failing so far.

That line does not repeat for every redelivery. The runner only logs it when it writes a failure record, and the same
input failing again inside the budget records nothing new, so the redeliveries after the first are silent. A different
input failing usually does write a record and log again, though not if that write loses a compare-and-set to something
else touching the instance, in which case the runner discards it and says nothing.

So silence after that first line tells you nothing on its own. The instance may still be failing on the same event, or
a later delivery may have succeeded and cleared the record, which the runner does without logging anything. Read the
instance's status with step 1 rather than reading the quiet either way.

When an instance is quarantined, the same logger logs an `ERROR` saying it is now `QUARANTINED`. That line is the one
to alert on. It names two durations, how long the event had been failing and then the budget, in that order.

A release in step 5 logs lines of its own from the same logger. The one to alert on is an `ERROR` saying the instance
is `QUARANTINED` again, and step 5 lists the others.

The third line is a `WARN` for one of the ways an instance past its budget is not quarantined, where the subscription
could not confirm it still holds the failing event. That instance stays `ACTIVE`, so it does not appear in step 1,
and this line is the only log line that says so.

The other ways log nothing about the quarantine. When the store read or write behind it throws, or the write loses a
compare-and-set to another writer, no quarantine line appears. After a lost compare-and-set the runner cannot tell what
the other writer did either. Read the instance's status with step 1 or step 2 rather than working it out from the time
or from which lines you saw.

That refusal is not final. The runner asks the subscription again on every redelivery, so a check that failed because
a store was briefly unreachable can succeed later, and the same instance can be quarantined then. That recovery needs a later
delivery to the instance, which for the event it stopped on means the subscription model offering that event again.
Keep looking for it in step 1 rather than treating this warning as the end of the story.

That third line is not logged on every redelivery. The runner holds the instance's id in memory and logs it once per
run of refusals, so it says nothing on the redeliveries that follow. It says it again after any event that instance
handles successfully, and again after a restart, since that memory does not survive one. An alert on it should
therefore neither expect one line per redelivery nor treat a second line as a second instance.

The elapsed duration is how long the named event has been failing, measured from when it first failed. Each failing
event keeps its own clock, so an instance where two events fail in turn renames the record to whichever event failed
last, with that event's own instant.

## The sequence

Steps are all marked **[you]**, since there is no tool for this. Nothing in steps 1 through 3 writes to the store,
and steps 5 and 6 are the ones that change it.

### 1. [you] List the quarantined instances

`SagaInstances` is the read-only view over one saga's instances. On the Spring stack the `@Saga` registrar publishes
one per saga under the bean name `sagaInstances-<id>`. Running a saga programmatically, `SagaSubscription.instances()`
hands you the same thing.

```java
SagaInstances instances = applicationContext.getBean("sagaInstances-order-fulfilment", SagaInstances.class);
List<SagaInstance> quarantined = instances.findByStatus(SagaStatus.QUARANTINED, Instant.now(), 100);
```

Three things about that query. `Instant.now()` as the second argument means every instance in the status, and passing
`Instant.now().minus(threshold)` instead restricts it to the ones that have not been updated for longer than
`threshold`. The results come back least recently updated first, so the instance that has been stopped the longest is
first. And the third argument is a bound rather than a page, with no cursor behind it, so a saga with more quarantined
instances than that needs a higher number rather than a second call.

`findByStatus` throws `UnsupportedOperationException` on a store that does not implement `SagaStateStoreQueries`.
`SpringMongoSagaStateStore` and the in-memory store both do. If yours does not, go to the store directly, which for
MongoDB is:

```javascript
db.getCollection("saga-order-fulfilment")
  .find({ status: { $in: ["QUARANTINED", "QUARANTINED_RECORDING"] } }, { state: 0 })
  .sort({ updatedAt: 1 })
  .limit(100)
```

Match both strings. `SpringMongoSagaStateStore` stores an instance that 0.35.0 quarantined with the status
`QUARANTINED_RECORDING`, and one that 0.34.0 quarantined with `QUARANTINED`. Both read back as
`SagaStatus.QUARANTINED`, and `findByStatus` matches both.

`saga-<sagaId>` is the collection the Spring Boot starter uses when you never named one. If you built the
`SpringMongoSagaStateStore` yourself, use the name you passed it.

Keep the limit, and raise it when you have to. An index the store creates for itself on `status` and `updatedAt`
serves the match and the sort, so this never scans the collection, but without a bound it still reads and returns
every quarantined instance, which during an incident is exactly when there are most of them.

Keep the `{ state: 0 }` too. `findByStatus` projects the state away for the same reason, and a flow saga's state holds
the events it has retained, so printing a hundred of them is a lot of data none of this step needs. `updatedAt` is
stored as epoch milliseconds rather than a date.

### 2. [you] Read what the instance stopped on

`SagaInstance.failure()` answers a `SagaFailure` for a quarantined instance, and `null` for an instance that has no
failure recorded.

A `null` does not mean the instance is not failing. Several ways of failing record nothing, a failing timeout among
them, so an instance can fail on every delivery and every timer poll and still answer `null` with an `ACTIVE` status.
The javadoc on `SagaStatus.QUARANTINED` lists which ways those are.

```java
SagaInstance instance = quarantined.getFirst();
SagaFailure failure = instance.failure();
// failure.input()          the failing event's redelivery key, a stream id with its stream version
// failure.position()       the event's global position when the store assigns one, otherwise null
// failure.firstFailedAt()  when the event the record names first failed, strictly when that failure
//                          was first recorded, on the record or on the event's pending entry, which is
//                          later than the first failure itself if an earlier failure recorded nothing
// failure.failureType()    the class name of the exception that stopped the processing. Usually one the
//                          saga or its command dispatcher threw, and it can also be a store or
//                          converter failure, since the runner catches the whole path including the
//                          read that decodes the state. EventMissingFromStore when a release in
//                          step 5 found the event missing from the event store
// failure.failureMessage() that exception's message, or null when it had none. Cut to the first
//                          1000 characters with "... (truncated)" appended when it was longer,
//                          so the log lines are where a long message survives whole
// failure.releasedAt()     when a release of the instance started, or null when none is running
```

Read `firstFailedAt()` as how long `failure.input()` has been failing. Each failing event keeps its own clock, so
another event that failed on the same instance before it does not move it. A record written before 0.35.0 can hold an
earlier instant, from when the instance started failing on some other event.

It is a floor rather than an exact incident start. The value is when the failure was first recorded, and an earlier
failure of the same event that recorded nothing, one whose writes all lost their compare-and-set for instance, makes
that later than when the event started failing. So the event may have been failing for longer than the difference
between `firstFailedAt()` and now.

The record holds the exception's class name and message, and not its stack trace. Take the stack trace from the log
lines under "How you find out" above, which are the only place it exists.

`SagaInstance.currentStep()` tells you which step a flow saga's instance was on. `updatedAt()` is when the quarantine
was written.

Reading one instance by id works on any store, including one whose `findByStatus` refused you in step 1, since
enumeration is the optional capability and a by-id lookup is not.

```java
Optional<SagaInstance> one = instances.find("order-4711");
```

It reads through `SagaStateStore.findWithoutState`, so on a store that overrides that member it answers for an
instance whose state no longer decodes as well. On a store that does not override it, that member inherits to `find`,
which decodes, so this throws for such an instance and `findByStatus` in step 1 is the read that still answers.

### 3. [you] Read the state, if you need it and it still decodes

Nothing `SagaInstance` answers comes from the saga's own state, which is why step 1 works on the instance you are most
likely to be looking at. `findByStatus` reads no state on any store, since that is what the `SagaStateStoreQueries`
contract requires of it. Step 2's by-id read is state-free only on a store that overrides `findWithoutState`, as above.

An instance whose state can no longer be decoded, after an event class was renamed or a converter changed, is
quarantined like any other. Its state is still stored, untouched, so repairing the converter and reading it again
gets the state back. The instance stays quarantined.

`SagaStateStore.find(sagaId)` is the read that does decode the state, and it is the one that throws on such an
instance. Use it when the state itself is what you need:

```java
Optional<SagaEnvelope<OrderFulfilment>> envelope = stateStore.find("order-4711");
```

Two members make the state-free reads work, `SagaStateStore.findWithoutState` and
`SagaStateStore.compareAndSaveWithoutState`. Both are `default` methods that inherit to `find` and `compareAndSave`,
so a store written against 0.33.0 keeps working without them. `SpringMongoSagaStateStore` overrides both.
`SagaStateStore.inMemory()` does not and does not need to, since it holds each envelope as an object rather than a
document, so nothing there can fail to decode.

A store of your own that overrides neither cannot quarantine an instance whose state does not decode, because the
executor's own read throws for the same reason yours does, so that instance never reaches its budget. It keeps
failing for as long as the subscription model offers the failing event again, the way it did in 0.33.0. The contract for an override is in
[section 8 of the upgrade guide](../migration/upgrading-to-0.34.0.md#the-five-breaks).

### 4. [you] Fix the cause, or decide there is nothing to fix

Fixing the cause does not release the instance on its own. It is what lets the release in step 5 succeed, since a
release replays the event the instance stopped on first. Three common causes, and what each one tells you.

**The saga's own code is wrong.** Deploy the fix, then release the instance in step 5. Until then it stays
quarantined, and the input it stopped on is never handed to the saga again no matter how many later events reach the
runner. On 0.34.0 nothing replays it, and step 6 is what ends it.

**The state cannot be decoded.** That shows up as a store or converter class in `failureType()` rather than
anything of your saga's. Repair the converter or restore the event class, deploy, and read the instance again with
`SagaStateStore.find`. If it decodes now, you know what the instance held, and step 5 can release it. A release reads
the instance with its state, so while the state still doesn't decode, `release` throws an `IllegalStateException`
that names the decoding failure.

**A downstream service was refusing the command.** The instance stopped on an event it would handle correctly today,
and step 5 replays it. On 0.34.0 nothing does.

In every case the instance stays where it is until step 5 or step 6. What you have gained is knowing whether the
process it was running finished some other way, which decides between releasing it and deleting it.

**Replaying a parked event costs its whole clock.** A parked event's clock keeps running while it is parked. When you
replay it on an instance with quarantine on and it fails again after `quarantineAfter` has passed since its first
failure, that first replay failure quarantines the instance, and the `ERROR` line comes with no `WARN` before it. Fix
the cause before you replay. Turning quarantine on works the same way for an event that failed while it was off, since
that event has been failing since then.

### 5. [you] Release the instance

From 0.35.0 `SagaSubscription.release(sagaId)` brings a quarantined instance back. It reads the event the instance
stopped on from the event store and applies it the way a delivery is applied, and then does the same for every input
the instance skipped while it was quarantined, in the order they arrived. The instance stays `QUARANTINED` while that
runs and becomes `ACTIVE` once no recorded input is left, so an event that arrives during the release is recorded and
replayed too. Once it is `ACTIVE`, its timers fire again.

A timer that came due while the instance was quarantined fires after every recorded input has been replayed,
including inputs that arrived after its deadline. For a saga that uses a timer as a deadline, a replayed input from
after the deadline that cancels or re-arms the timer means the timeout never fires, where live delivery would have
fired it before that input.

On the Spring stack the `@Saga` registrar publishes the saga's `SagaSubscription` under the bean name
`sagaSubscription-<id>`. Running a saga programmatically, `SagaRunner.run` returns it.

```java
SagaSubscription subscription = applicationContext.getBean("sagaSubscription-order-fulfilment", SagaSubscription.class);
SagaSubscription.ReleaseResult result = subscription.release("order-4711");
```

A release takes no lease and pauses nothing, so you can call it on any node. It dispatches the commands the replayed
inputs produce, the same commands those events would have produced had the instance never stopped. Make sure the
process has not finished some other way in the meantime, which is what step 4 was for.

`release` reads the events with the `EventStoreQueries` the runner was given through `SagaRunner.eventStoreQueries(..)`.
The Spring Boot starter passes its own `EventStoreQueries` bean when the context has exactly one, or several with one
of them primary. Point it at the store the saga's subscription reads from.

What `release` returns, and what to do next:

| Result | What it means | What to do |
|---|---|---|
| `RELEASED` | The instance replayed every input and is `ACTIVE`, or `COMPLETED` when one of them completed it. | Go to step 7. |
| `STILL_RELEASING` | An input failed again during the release. The instance is still `QUARANTINED`, with the release under way. | See below. The timer poller tries the input again. |
| `QUARANTINED_AGAIN` | An input kept failing during the release until its quarantine budget ran out, or the event store handed back an input's event without a stream id and stream version, which only an `EventStoreQueries` other than Occurrent's own event stores can do. The instance is quarantined on that input, and the release has stopped. | Go back to step 2. The failure record names the new input, which stays recorded with the inputs after it, so the next release starts from it. For an event without a stream id and version, give the runner an `EventStoreQueries` that returns them and release again, or delete the instance in step 6. |
| `QUARANTINED_EVENT_MISSING` | The event store no longer has the event of one of the inputs. The instance is quarantined on that input at once, and the release has stopped. | See below. |
| `NOT_QUARANTINED` | The instance is not quarantined, so there was nothing to release. | Nothing. Another call or node may have released it already. |
| `NOT_FOUND` | No instance has that id. | Check the saga id and the bean name. |

**When an input fails again.** `release` logs a `WARN` that the instance failed on the event while it was releasing,
and returns `STILL_RELEASING`. After every poll the timer poller goes
on with each instance that is releasing, which with a competing-consumer strategy happens only on the node that holds
the saga's timer lease, and without one on every node. The poller also goes on with a release that a crash or a
restart interrupted. You can call `release` again too, and it goes on where the release stands.

The poller polls only while the saga's `timersEnabled` returns `true`, so a release that is under way waits with the
timers while that gate is closed. Going on with a release dispatches commands. On a subscription model that reports
whether it runs, the Spring Boot starter keeps the gate closed while the saga's subscription isn't running, which with
`occurrent.subscription.mode=manual` lasts until the application resumes the subscription. On a model that also
reports a replay, the gate stays closed while the subscription is still catching up. The first poll after the gate
opens goes on with the release. A `release` call doesn't wait for the gate.

One poll replays at most `timerBatchLimit` records, 100 by default, shared evenly among the releasing instances it
lists, at least one each, and the next poll goes on where the last one stopped. A `release` call has no such bound and
replays on the calling thread until it returns one of the results above.

Fix the cause the
same way as in step 4. If the input keeps failing until its quarantine budget runs out, measured from the start of the
release for the input the instance stopped on, the instance is quarantined again and the release stops, which is
`QUARANTINED_AGAIN`. With quarantine off the poller keeps trying the input until it succeeds.

When a store can't list the instances that are releasing, the poller logs a `WARN` once that it cannot go on with
releases on its own, and stops asking. A release that failed on an input or was interrupted then goes on only when you
call `release` again.

**When an event is missing.** The instance's failure record names the input whose event is gone, and its
`failureType()` is `EventMissingFromStore`. The `ERROR` line says the same. That input stays recorded with the inputs after it, and the next release starts
from it.
Three ways on from there:

- Restore the event in the event store and call `release` again.
- If the event is in the store, `release` is reading another one. Check that the runner's `EventStoreQueries` reads the
  store the saga's subscription reads from, and call `release` again.
- Delete the instance in step 6, once you have decided not to recover it.

**When `release` throws.** It throws an `IllegalStateException` before it writes anything when:

- The instance was quarantined by 0.34.0, which recorded none of the inputs it skipped.
- The instance's state doesn't decode. The message names the decoding failure.
- The runner has no `EventStoreQueries`. The message names `SagaRunner.eventStoreQueries(..)`.

The first can't be released at all. Its message names `SagaStateStore.delete(sagaId)`, and step 6 is what is left.
For the second, fix how the state decodes, as in step 4, and release the instance again, or delete it in step 6.

An instance whose state stops decoding while it is releasing stays releasing. On a store that can list the releasing
instances, each poll logs the failure at `WARN` and tries again, so the release goes on once the state decodes.

`release` also throws an `IllegalStateException` when the state store didn't persist `SagaFailure.releasedAt`,
`SagaFailure.quarantineId` or `SagaEnvelope.quarantineSkipCount` as the release started. The message names the store.
A `SagaStateStore` of your own has to save all three, as
[the 0.35.0 upgrade guide](../migration/upgrading-to-0.35.0.md#stored-saga-state-for-a-release) describes.

**Checking on a release.** `SagaInstance.failure().isReleasing()` is `true` while a release is under way, and
`failure().releasedAt()` says when it started. Against MongoDB:

```javascript
db.getCollection("saga-order-fulfilment")
  .find({ status: { $in: ["QUARANTINED", "QUARANTINED_RECORDING"] }, failureReleasedAt: { $exists: true } }, { state: 0 })
  .limit(100)
```

`failureReleasedAt` is stored as epoch milliseconds. The inputs still to replay are documents in the collection named
after the saga collection with `_skipped_inputs` appended, one per skipped input, each with `sagaId`, `quarantineId`,
`seq` and `key`. A release replays them in order of `seq`, and only those under the instance's `failureQuarantineId`:

```javascript
db.getCollection("saga-order-fulfilment_skipped_inputs")
  .find({ sagaId: "order-4711", quarantineId: "<failureQuarantineId>" })
  .sort({ seq: 1 })
  .limit(100)
```

A key is `<streamId>@<streamVersion>` for an event from a stream.

**What a release logs.** All from the same logger as the quarantine `ERROR`:

- `INFO` when the release starts, with the event it replays first.
- `WARN` when an input fails again and the instance stays releasing.
- `ERROR` when the instance is `QUARANTINED` again, either because an input ran out of budget or because an event is
  missing from the event store.

### 6. [you] Delete the instance, once you have decided not to recover it

`SagaStateStore.delete(sagaId)` is the call, and whether you have anything to call it on depends on how the store was
wired.

If your application defines its own `SagaStateStore` bean, which is also the case when a `@Saga` names one through
`store` or `storeName`, inject that bean and call it:

```java
stateStore.delete("order-4711");
```

If you let the Spring Boot starter default the store, there is nothing to inject. It builds a
`SpringMongoSagaStateStore` over `saga-<sagaId>` inside the provider and never publishes it as a bean, so
`SagaInstances` is the only handle you were given and it is deliberately read-only. Delete the document instead:

```javascript
db.getCollection("saga-order-fulfilment").deleteOne({ _id: "order-4711" })
```

That removes the same document, since the instance is one document keyed by its saga id. From 0.35.0 the Java call
also deletes the records of the inputs the instance skipped, so delete those too:

```javascript
db.getCollection("saga-order-fulfilment_skipped_inputs").deleteMany({ sagaId: "order-4711" })
```

The Java call also drops the saga id from the store's in-memory latch for the retained-event warning a flow saga gets
when it crosses the size threshold, and nothing in MongoDB does that. The consequence is that if this saga id comes
back and crosses that threshold again, the warning stays suppressed until the entry is evicted or the application
restarts. Everything below applies to both routes.

Read `SagaStateStore.delete`'s own javadoc before you run either. Deleting an instance discards its redelivery
watermarks along with its status, so if the event source can still redeliver an event this instance already consumed,
a delete that races that redelivery lets the event recreate the instance and run the process a second time. A
subscription replay, a reset checkpoint, and a redelivery after a crash are all ways that happens.

So delete an instance only once its source can no longer redeliver any of its events. Leaving it in `QUARANTINED`
until then is safe, because the runner skips every event addressed to it. It is not free, though. Every later event
addressed to that instance is still delivered and still reads the instance from the state store before the runner
skips it. From 0.35.0 the skip also records the event's key, which takes two writes to the instance and one record
between them, so a correlation id that keeps receiving events costs a read and three writes for each one.

The business process the instance was running is a separate question, and Occurrent has no answer for it. An order
half way through fulfilment when its instance stopped is still half way through it after you delete the row. Finish or
compensate it through whatever your application uses for that, before or after the delete, and not by expecting the
saga to pick it up again.

### 7. [you] Verify

After a release, the instance is `ACTIVE`, or `COMPLETED` when a replayed input completed it, and has no failure
record:

```java
instances.find("order-4711").map(SagaInstance::status);
```

Or against MongoDB, where a released instance no longer matches:

```javascript
db.getCollection("saga-order-fulfilment").countDocuments({ _id: "order-4711", status: { $in: ["QUARANTINED", "QUARANTINED_RECORDING"] } })
```

The document of a released `ACTIVE` instance usually reads `ACTIVE_MARKED` in MongoDB, which `find` reads back as
`ACTIVE`. The
[0.35.0 upgrade guide](../migration/upgrading-to-0.35.0.md#stored-saga-state-1) says why.

After a delete, the instance is gone:

```java
instances.find("order-4711").isEmpty();
```

Or against MongoDB:

```javascript
db.getCollection("saga-order-fulfilment").countDocuments({ _id: "order-4711" })
```

No `ERROR` follows for a deleted instance, because there is no longer an instance or a failure record for the runner
to find. A new `ERROR` naming the same id means a redelivery recreated the instance and it was quarantined again,
which is the race in step 6.

## Preventing the next one

`SagaRunnerConfig.quarantineAfter` is how long one event has to keep failing before its instance can be quarantined
on it. `SagaRunnerConfig.defaults()` sets it to 5 minutes from 0.35.0, and in 0.34.0 it was empty, which means never.
On the annotation path it is `occurrent.saga.quarantine-after` instead, which is 5 minutes when it is left out or
blank.

With quarantine on, from the defaults or set yourself, a `SagaRunner` on a subscription model that retains every
event it delivers refuses to start without an `EventStoreQueries`, since `release` in step 5 reads the skipped inputs
back with it. Pass the event store with `SagaRunner.eventStoreQueries(..)`, or call
`SagaRunnerConfig.disableQuarantine()`. On the annotation path, no `EventStoreQueries` bean, or several with none of
them `@Primary`, fails at startup the same way, so define the event store as a bean, mark one `@Primary`, or set
`occurrent.saga.quarantine-enabled=false`. On a
model that doesn't retain every event, or on a push feed, `SagaRunner` turns quarantine off and logs a `WARN` each time
the saga starts, with the default configuration too. `SagaRunnerConfig.disableQuarantine()` or
`occurrent.saga.quarantine-enabled=false` silences it.

Lower it when you would rather find out sooner and are willing to quarantine an instance whose downstream service was
only briefly unavailable. Raise it when your dispatcher talks to something that is routinely down for longer than five
minutes, so an instance is not quarantined for an outage that would have resolved.

Turning quarantine off restores the 0.33.0 behaviour, where the saga is never quarantined. How you say that differs
by path. On the annotation path set `occurrent.saga.quarantine-enabled=false`, and otherwise call
`SagaRunnerConfig.disableQuarantine()`. Removing `occurrent.saga.quarantine-after`, or leaving it blank, gives the
default of 5 minutes, not off. Zero is refused on both paths,
because a reader can take it to mean "never" or "immediately". The property stops the application from starting when
it has at least one `@Saga`, and `withQuarantineAfter(Duration.ZERO)` throws an `IllegalArgumentException`.

Either way this stops the next instance being quarantined. It does not bring back one you already have, and step 5
still can.

Whether an instance should be quarantined at all is a question about your saga rather than about the budget. An
`evolve` that throws on an event it does not recognise, rather than ignoring it, quarantines an instance for something
that was never a failure. `SagaRunnerConfig.redeliveryDetection` under `REQUIRED`, which is its default, already
refuses an event with no redelivery key before the saga sees it.

## Rollback considerations

Downgrading to 0.33.0 does not remove the quarantined instances from the store, and 0.33.0 does not know the status.
What that does depends on your store. `SpringMongoSagaStateStore` on 0.33.0 reads `status` with `SagaStatus.valueOf`, which
throws `IllegalArgumentException` on the string `QUARANTINED`, so every read of such an instance fails and the
subscription blocks on it exactly as it did before quarantine existed. Delete the quarantined instances before you
downgrade, under the same rule as step 6, or accept that.

A 0.34.0 node throws on every event addressed to an instance that 0.35.0 stored as `QUARANTINED_RECORDING`, and
`findByStatus(QUARANTINED, ..)` on 0.34.0 doesn't find such an instance. The node stores no position past that event,
so no input is lost. On a live subscription it retries the event in place and keeps the saga's competing-consumer
lease while it does, so the saga's subscription stalls. The stall holds up every later event of that saga
subscription, not only the events of the quarantined instance. It ends when a 0.35.0 node takes the lease, or when a
0.35.0 node deletes the instance. A release doesn't end it, because a released instance is stored as `ACTIVE_MARKED`,
which 0.34.0 can't decode either. A node whose catch-up replay failed on the event also has to restart before its
subscription goes live. After a downgrade no 0.35.0 node is left to take the lease, so the stall lasts until the
instance is deleted. To downgrade without that stall, do steps 1 to 4 on 0.35.0:

1. Turn quarantine off on every node, with `SagaRunnerConfig.disableQuarantine()` or
   `occurrent.saga.quarantine-enabled=false`, and have that running on every node before step 2. A node that still has
   quarantine on can quarantine another instance after you have listed them.
2. Find the quarantined instances with `findByStatus(QUARANTINED, ..)` and call `release` on each whose failure
   record has a quarantine id.
3. Delete with `SagaStateStore.delete(sagaId)` each one whose release returns `QUARANTINED_AGAIN`,
   `QUARANTINED_EVENT_MISSING` or `STILL_RELEASING`.
4. When a release throws, the instance stays quarantined. Fix the cause the exception names, such as a state that no
   longer decodes or an event store read that failed, and release the instance again, or delete it.
5. Stop every 0.35.0 node, and set every `ACTIVE_MARKED` instance back to `ACTIVE`, released ones included, as
   [the 0.35.0 upgrade guide](../migration/upgrading-to-0.35.0.md#stored-saga-state-1) describes. It also says what
   0.34.0 then loses.
6. Start the 0.34.0 nodes.

An instance 0.34.0 quarantined needs nothing, and an instance whose release returns `NOT_QUARANTINED` is already
released. The
[0.35.0 upgrade guide](../migration/upgrading-to-0.35.0.md#rolling-upgrades-from-0340-and-rolling-back-to-it) also
covers a rolling upgrade from 0.34.0.

Deleting an instance is not reversible from inside Occurrent. Take a backup of the collection first if you want one.
