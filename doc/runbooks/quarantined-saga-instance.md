# Runbook: a quarantined saga instance

## Who this is for

You run a saga on 0.34.0 or later, an instance of it has stopped, and you want to know what it stopped on and what to
do with it. The `SagaStatus.QUARANTINED` state is new in 0.34.0, so nothing before that release can produce one.

If you are deciding whether to turn quarantine on at all, that question is in
[section 8 of Upgrading to 0.34.0](../migration/upgrading-to-0.34.0.md#8-a-saga-instance-that-keeps-failing-is-quarantined-and-four-saga-types-change-with-it),
which also covers the two cases where a saga never quarantines anything. This runbook starts from the point where you
already have one.

## Why an instance stops

A saga has one subscription and every instance of that saga is fed by it. When handling an event for one instance
throws, the executor rethrows, the subscription redelivers the event, and the instance tries again. Usually that is
the instance's `evolve`, its `react`, or its command dispatcher, and it can also be the read that loads the instance,
which is how an instance whose state no longer decodes ends up here. Up to 0.33.0 that went on without limit, so one correlation id that could never make progress stopped every
other correlation id behind it.

From 0.34.0 the executor times how long the instance has been failing. Once that reaches
`SagaRunnerConfig.quarantineAfter`, five minutes by default, the instance moves to `SagaStatus.QUARANTINED` and the
executor stops rethrowing, so the subscription acknowledges the event and goes on delivering to everybody else.

A quarantined instance applies no further events and fires no timers, and its redelivery watermarks stop moving, so
nothing it skipped is recorded as handled. The subscription still delivers those events, and the runner still reads
the instance before skipping each one, which step 5 comes back to.

Nothing in 0.34.0 brings an instance out of quarantine. Deleting it is the only ending this release offers, and step 5
is what that costs.

## How you find out

Three log lines from `org.occurrent.dsl.saga.blocking.SagaExecution`, all naming the saga's subscription id and the
instance's saga id, and all logging the exception with its stack trace. There is no metric and no health indicator for
any of this in 0.34.0.

The first `WARN` says the instance failed on an event and is being retried. The duration in that line is the budget it
has to exhaust, not how long it has been failing so far.

That line does not repeat for every redelivery. The runner only logs it when it writes a failure record, and the same
input failing again inside the budget records nothing new, so the redeliveries after the first are silent. A different
input failing usually does write a record and log again, though not if that write loses a compare-and-set to something
else touching the instance, in which case the runner discards it and says nothing.

So silence after that first line tells you nothing on its own. The instance may still be failing on the same event, or
a later delivery may have succeeded and cleared the record, which the runner does without logging anything. Read the
instance's status with step 1 rather than reading the quiet either way.

When the budget elapses, the same logger logs an `ERROR` saying the instance is now `QUARANTINED`. That line is the
one to alert on. It names two durations, how long the instance had been failing and then the budget, in that order.

The third line is a `WARN` for the case where the budget elapsed and the instance was not quarantined, because the
subscription could not confirm it still holds the failing event. That instance goes on blocking the saga's other
instances, and while that lasts it does not appear in step 1, so this line is the only thing that says so.

That refusal is not final. The runner asks the subscription again on every redelivery, so a check that failed because
a store was briefly unreachable can succeed later and quarantine the same instance then. Keep looking for it in step 1
rather than treating this warning as the end of the story.

That third line is not logged on every redelivery. The runner holds the instance's id in memory and logs it once per
run of refusals, so it says nothing on the redeliveries that follow. It says it again after any event that instance
handles successfully, and again after a restart, since that memory does not survive one. An alert on it should
therefore neither expect one line per redelivery nor treat a second line as a second instance.

The elapsed duration is how long the instance has been failing, which can be longer than the named event has. The
clock belongs to the instance rather than to one event, so an instance where two events both fail keeps the instant it
started failing and renames the record to whichever event failed last.

## The sequence

Steps are all marked **[you]**, since there is no tool for this. Nothing in steps 1 through 3 writes to the store,
and step 5 is the only one that changes it.

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
  .find({ status: "QUARANTINED" }, { state: 0 })
  .sort({ updatedAt: 1 })
  .limit(100)
```

`saga-<sagaId>` is the collection the Spring Boot starter uses when you never named one. If you built the
`SpringMongoSagaStateStore` yourself, use the name you passed it.

Keep the limit, and raise it when you have to. An index the store creates for itself on `status` and `updatedAt`
serves the match and the sort, so this never scans the collection, but without a bound it still reads and returns
every quarantined instance, which during an incident is exactly when there are most of them.

Keep the `{ state: 0 }` too. `findByStatus` projects the state away for the same reason, and a flow saga's state holds
the events it has retained, so printing a hundred of them is a lot of data none of this step needs. `updatedAt` is
stored as epoch milliseconds rather than a date.

### 2. [you] Read what the instance stopped on

`SagaInstance.failure()` answers a `SagaFailure` for a quarantined instance, and `null` for an instance that is failing
on nothing.

```java
SagaInstance instance = quarantined.getFirst();
SagaFailure failure = instance.failure();
// failure.input()          the failing event's redelivery key, a stream id with its stream version,
//                          or the global position when the event has no stream metadata
// failure.position()       that global position beside it when the store assigns one, otherwise null
// failure.firstFailedAt()  when this instance started failing, strictly when its first failure record
//                          was written, which is later than the first failure itself if that write
//                          lost a compare-and-set
// failure.failureType()    the class name of the exception that stopped the processing. Usually one the
//                          saga or its command dispatcher threw, and it can also be a store or
//                          converter failure, since the runner catches the whole path including the
//                          read that decodes the state
// failure.failureMessage() that exception's message, or null when it had none. Cut to the first
//                          1000 characters with "... (truncated)" appended when it was longer,
//                          so the log lines are where a long message survives whole
```

Read `firstFailedAt()` as the start of the instance's current run of failing, and not as the first time
`failure.input()` itself failed. Reading it the other way under-reports how long the instance has been stuck.

It is a floor rather than an exact incident start. The value is when the first failure record was written, which is
the same moment unless that write lost its compare-and-set, so the instance may have been failing for longer than the
difference between `firstFailedAt()` and now.

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
quarantined like any other. Its state is still stored, untouched, so repairing the converter and reading it again is
what gets it back.

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
executor's own read throws for the same reason yours does, so that instance goes on blocking the saga's other
instances the way it did in 0.33.0. The contract for an override is in
[section 8 of the upgrade guide](../migration/upgrading-to-0.34.0.md#the-five-breaks).

### 4. [you] Fix the cause, or decide there is nothing to fix

Whatever the cause, fixing it does not release the instance, so this step is about what you learn rather than about
getting the instance moving. Three common ones, and what each one tells you.

**The saga's own code is wrong.** Deploy the fix. The instance stays quarantined anyway. `QUARANTINED` is absorbing,
so the input it stopped on is never handed to the saga again no matter how many later events reach the runner, and
step 5 is still what ends it.

**The state cannot be decoded.** That shows up as a store or converter class in `failureType()` rather than
anything of your saga's. Repair the converter or restore the event class, deploy, and read the instance again with
`SagaStateStore.find`. If it decodes now, you know what the instance held. It is still quarantined.

**A downstream service was refusing the command.** The instance stopped on an event it would handle correctly today.
It is still quarantined, and 0.34.0 has nothing that replays it.

In every case the instance stays where it is. What you have gained is knowing whether the process it was running
finished some other way, which is what you need before step 5.

### 5. [you] Delete the instance, once you have decided not to recover it

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

That removes exactly what `delete` removes, since the instance is one document keyed by its saga id. Everything below
applies to both routes.

Read `SagaStateStore.delete`'s own javadoc before you run either. Deleting an instance discards its redelivery
watermarks along with its status, so if the event source can still redeliver an event this instance already consumed,
a delete that races that redelivery lets the event recreate the instance and run the process a second time. A
subscription replay, a reset checkpoint, and a redelivery after a crash are all ways that happens.

So delete an instance only once its source can no longer redeliver any of its events. Leaving it in `QUARANTINED`
until then is safe, because the saga's other instances are not waiting behind it, which is the guarantee that matters
here. It is not free, though. Every later event addressed to that instance is still delivered and still reads the
instance from the state store before the runner skips it, so a correlation id that keeps receiving events keeps
paying a lookup for each one.

The business process the instance was running is a separate question, and Occurrent has no answer for it. An order
half way through fulfilment when its instance stopped is still half way through it after you delete the row. Finish or
compensate it through whatever your application uses for that, before or after the delete, and not by expecting the
saga to pick it up again.

### 6. [you] Verify

```java
instances.find("order-4711").isEmpty();
```

Or against MongoDB:

```javascript
db.getCollection("saga-order-fulfilment").countDocuments({ _id: "order-4711" })
```

No `ERROR` follows for a deleted instance, because there is no longer an instance or a failure record for the runner
to find. A new `ERROR` naming the same id means a redelivery recreated the instance and it was quarantined again,
which is the race in step 5.

## Preventing the next one

`SagaRunnerConfig.quarantineAfter` is how long an instance may keep failing before it is quarantined, five minutes by
default. On the annotation path it is `occurrent.saga.quarantine-after` instead.

Lower it when you would rather find out sooner and are willing to quarantine an instance whose downstream service was
only briefly unavailable. Raise it when your dispatcher talks to something that is routinely down for longer than five
minutes, so an instance is not quarantined for an outage that would have resolved.

Turning quarantine off restores the 0.33.0 behaviour, where the event is retried forever and every other instance of
that saga waits behind it. How you say that differs by path. Set the property to zero, and pass `null` for
`SagaRunnerConfig.quarantineAfter`. `Duration.ZERO` is refused there with an `IllegalArgumentException`, deliberately,
so that one literal does not mean opposite things on the two paths.

Either way this stops the next instance being quarantined. It does not bring back one you already have.

Whether an instance should be quarantined at all is a question about your saga rather than about the budget. An
`evolve` that throws on an event it does not recognise, rather than ignoring it, quarantines an instance for something
that was never a failure. `SagaRunnerConfig.redeliveryDetection` under `REQUIRED`, which is its default, already
refuses an event with no redelivery key before the saga sees it.

## Rollback considerations

Downgrading to 0.33.0 does not remove the quarantined instances from the store, and 0.33.0 does not know the status.
What that does depends on your store. `SpringMongoSagaStateStore` on 0.33.0 reads `status` with `SagaStatus.valueOf`, which
throws `IllegalArgumentException` on the string `QUARANTINED`, so every read of such an instance fails and the
subscription blocks on it exactly as it did before quarantine existed. Delete the quarantined instances before you
downgrade, under the same rule as step 5, or accept that.

Deleting an instance is not reversible from inside Occurrent. Take a backup of the collection first if you want one.
