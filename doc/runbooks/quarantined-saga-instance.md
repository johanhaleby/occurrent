# Runbook: a quarantined saga instance

## Who this is for

You run a saga on 0.34.0 or later, an instance of it has stopped, and you want to know what it stopped on and what to
do with it. The `SagaStatus.QUARANTINED` state is new in 0.34.0, so nothing before that release can produce one.

If you are deciding whether to turn quarantine on at all, that question is in
[section 8 of Upgrading to 0.34.0](../migration/upgrading-to-0.34.0.md#8-a-saga-instance-that-keeps-failing-is-quarantined-and-four-saga-types-change-with-it),
which also covers the two cases where a saga never quarantines anything. This runbook starts from the point where you
already have one.

## Why an instance stops

A saga has one subscription and every instance of that saga is fed by it. When an instance's `evolve`, its `react`, or
its command dispatcher throws, the executor rethrows, the subscription redelivers the event, and the instance tries
again. Up to 0.33.0 that went on without limit, so one correlation id that could never make progress stopped every
other correlation id behind it.

From 0.34.0 the executor times how long the instance has been failing. Once that reaches
`SagaRunnerConfig.quarantineAfter`, five minutes by default, the instance moves to `SagaStatus.QUARANTINED` and the
executor stops rethrowing, so the subscription acknowledges the event and goes on delivering to everybody else.

A quarantined instance receives no further events and fires no timers, and its redelivery watermarks stop moving, so
nothing it skipped is recorded as handled.

Nothing in 0.34.0 brings an instance out of quarantine. Deleting it is the only ending this release offers, and step 5
is what that costs.

## How you find out

Two log lines from `org.occurrent.dsl.saga.blocking.SagaExecution`, both naming the saga's subscription id and the
instance's saga id, and both logging the exception with its stack trace.

While the instance is still inside its budget, every failure logs a `WARN` saying the instance failed on an event and
is being retried, and naming the budget it has to exhaust before it is quarantined.

When the budget elapses, the same logger logs an `ERROR` saying the instance is now `QUARANTINED`, how long it had
been failing, and which event it stopped on. That line is the one to alert on. There is no metric and no health
indicator for this in 0.34.0.

The duration in both lines is how long the instance has been failing, which can be longer than the named event has.
The clock belongs to the instance rather than to one event, so an instance where two events both fail keeps the
instant it started failing and renames the record to whichever event failed last.

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
db.sagaInstances.find({ status: "QUARANTINED" }).sort({ updatedAt: 1 })
```

Replace `sagaInstances` with the collection name you built your `SpringMongoSagaStateStore` with. That query is served
by an index the store creates for itself on `status` and `updatedAt`, so it is cheap on a large collection.

### 2. [you] Read what the instance stopped on

`SagaInstance.failure()` answers a `SagaFailure` for a quarantined instance, and `null` for an instance that is failing
on nothing.

```java
SagaInstance instance = quarantined.getFirst();
SagaFailure failure = instance.failure();
// failure.input()          the failing event's redelivery key, a stream id with its stream version,
//                          or the global position when the event has no stream metadata
// failure.position()       that global position beside it when the store assigns one, otherwise null
// failure.firstFailedAt()  when this instance started failing
// failure.failureType()    the class name of the exception the saga or its dispatcher threw
// failure.failureMessage() that exception's message, or null when it had none
```

Read `firstFailedAt()` as the start of the instance's current run of failing, and not as the first time
`failure.input()` itself failed. Reading it the other way under-reports how long the instance has been stuck.

The record holds the exception's class name and message, and not its stack trace. Take the stack trace from the log
lines under "How you find out" above, which are the only place it exists.

`SagaInstance.currentStep()` tells you which step a flow saga's instance was on. `updatedAt()` is when the quarantine
was written.

Reading the instance by id works on any store, including one whose `findByStatus` refused you in step 1:

```java
Optional<SagaInstance> one = instances.find("order-4711");
```

### 3. [you] Read the state, if you need it and it still decodes

Nothing on `SagaInstance` comes from the saga's own state, so steps 1 and 2 never decode it. That is deliberate, and it
is what makes them work on the instance you are most likely to be looking at.

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
so a store written against 0.33.0 keeps working without them. `SpringMongoSagaStateStore` overrides both. A store of
your own that overrides neither cannot report an instance whose state does not decode, and cannot quarantine one
either, because the executor's own read throws for the same reason yours does. Overriding them is
[section 8 of the upgrade guide](../migration/upgrading-to-0.34.0.md#the-five-breaks).

### 4. [you] Fix the cause, or decide there is nothing to fix

There are three causes, and which one you have decides step 5.

**The saga's own code is wrong.** Deploy the fix. The instance stays quarantined, because nothing rereads it, so it
still needs step 5 afterwards.

**The state cannot be decoded.** Repair the converter or restore the event class, deploy, and read the instance again
with `SagaStateStore.find`. If it decodes now, you know what the instance held. It is still quarantined.

**A downstream service was refusing the command.** The instance stopped on an event it would handle correctly today.
It is still quarantined, and 0.34.0 has nothing that replays it.

In all three the instance stays where it is. What you have gained is knowing whether the process it was running
finished some other way, which is what you need before step 5.

### 5. [you] Delete the instance, once you have decided not to recover it

```java
stateStore.delete("order-4711");
```

Read `SagaStateStore.delete`'s own javadoc before you run this. Deleting an instance discards its redelivery
watermarks along with its status, so if the event source can still redeliver an event this instance already consumed,
a delete that races that redelivery lets the event recreate the instance and run the process a second time. A
subscription replay, a reset checkpoint, and a redelivery after a crash are all ways that happens.

So delete an instance only once its source can no longer redeliver any of its events. Until then, the instance sitting
in `QUARANTINED` costs you nothing beyond a row, since it already skips every event addressed to it and the saga's
other instances are not waiting behind it.

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
db.sagaInstances.countDocuments({ _id: "order-4711" })
```

The `ERROR` line does not repeat for a deleted instance, since nothing rereads a quarantine. A new `ERROR` naming the
same id means the instance was recreated by a redelivery and quarantined again, which is the race in step 5.

## Preventing the next one

`SagaRunnerConfig.quarantineAfter` is how long an instance may keep failing before it is quarantined, five minutes by
default. On the annotation path it is `occurrent.saga.quarantine-after` instead.

Lower it when you would rather find out sooner and are willing to quarantine an instance whose downstream service was
only briefly unavailable. Raise it when your dispatcher talks to something that is routinely down for longer than five
minutes, so an instance is not quarantined for an outage that would have resolved.

Setting it to zero, or to `null` in `SagaRunnerConfig`, turns quarantine off and restores the 0.33.0 behaviour where
the event is retried forever and every other instance of that saga waits behind it. That is a way to keep an
already-quarantined instance from happening again, and not a way to bring back one you already have.

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
