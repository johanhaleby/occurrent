# 158. A saga holds an event that arrives before its instance starts, unless its subscription delivers in commit order

Date: 2026-10-29

## Status

Proposed. Resolves [#1317](https://github.com/johanhaleby/occurrent/issues/1317), and with
[ADR 154](0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md) as amended in place, the half of
[#1330](https://github.com/johanhaleby/occurrent/issues/1330) about a failed version the instance cannot list. ADR 154 has
not shipped, so it changes in place and points here. [ADR 128](0128-a-renamed-or-removed-step-refuses-its-parked-instances.md)
shipped in 0.34.0, and its `delete` remedy reads differently on a subscription that can hold, so it gets a pointer in its
Status section.

Which subscription models deliver in commit order is up to each model, through the `CommitOrderedSubscriptions`
capability that [#1366](https://github.com/johanhaleby/occurrent/pull/1366) adds. No model implements it yet, so for
now every subscription holds.

## Context

A saga instance starts on its start event. Up to 0.34.0, a non-start event for an instance that has not started is
skipped and acknowledged. That is right when nothing can arrive before the start, and the event is lost when something
can.

Several feeds deliver out of commit order. A Kafka topic with several partitions, a RabbitMQ queue with several
consumers or a prefetch above one, a catch-up subscription handing over to its live feed, and a start whose first
delivery failed while a later event of the same instance went through. In each, order 1 starts the instance and order 2
is a later event of it, and order 2 can arrive first. 0.34.0 skips order 2, the instance starts on order 1, and the
reaction to order 2 never runs.

Waiting for the start cannot be done by refusing the event, since a broker redelivers a refused event at once and a
start that never comes would stall the consumer. Storing the event and applying it once the start arrives keeps it
without stalling anything.

## Decision

### Which subscriptions hold

**A subscription holds unless its model declares that it delivers every subscription in commit order.** The saga
asks `CommitOrderedSubscriptions.findIn(subscriptionModel)` once at `run` and keeps one answer, `holds`, which is true
unless the model answers `deliversEveryEventInCommitOrder()`. That answer is true only when the model delivers every
subscription in commit order at every moment, so reading it once is enough. A model that does not implement the
capability holds. `holds` decides whether an event that arrives before its start is held or skipped, whether a failed
version the instance cannot list is held, and whether `run` refuses a state store that cannot hold.

Commit order here means what the `CommitOrderedSubscriptions` contract says. No event is delivered before every earlier
event matching the subscription was delivered and its listener returned normally, where an event is earlier when it
committed earlier or was written earlier in the same commit. A delivery whose listener throws is retried in place before
anything after it. On such a feed only an event that came before its start in that order can arrive before the start,
also when both were written in one commit. Nothing is held, and that event is skipped as in 0.34.0. A model that answers
true and breaks that contract is that model's defect, and the ERROR for an event overtaken in flight below is where it
shows.

The capability is not on `SubscriptionModelCapability`'s unwrap list, so a wrapper declares commit order only by
implementing it. Any wrapper can break commit order, with a thread pool, a catch-up handover or a parking listener, and
a wrapper that inherited a "yes" from the model below it would skip on a feed that reorders. An undeclared wrapper is
treated as holding, which costs storage, never an event. When the model below a wrapper declares commit order and the
wrapper does not, `run` logs one WARN naming both classes. A model below that implements the capability and answers
false declares nothing, so it gets no WARN.

**`run` refuses a store that cannot hold.** When `holds` is true and the state store does not implement
`SagaHeldEvents`, `run` throws an `IllegalStateException` naming the subscription, the model and the store, and saying
the store must implement `SagaHeldEvents` or the saga must run on a model that declares commit order. Both shipped
stores, `InMemorySagaStateStore` and `SpringMongoSagaStateStore`, implement it.

### What every attempt does

Each attempt of an input to a started instance, a delivery or a timer, reads the instance and then the held index, each
held copy without its body. The read leaves out a copy that waits, using `heldIndexWithoutWaiting`, unless the instance
has no `startMark` or has completed. The attempt decides each copy it read before it handles the input:

| The copy | What the attempt does |
|---|---|
| the instance is completed | releases it with a WARN |
| the marks prove it applied | releases it |
| anything else | applies it as an input of its own, whatever the marks say, then releases it |

This walk is the sweep. A copy applied in it can lower the start or complete the instance, and then the sweep reads the
index again, so the copies that were waiting apply in version order or are released in that same attempt. The sweep runs
whatever `holds` is, whenever the state store can hold, so a copy held before a deploy
moved the saga to a commit-order model is still applied. A quarantined instance has no sweep and keeps every copy. An
instance that has not started has no sweep either, and the attempt doesn't read the index.

The marks prove a copy applied when its version is a mark, or among the `applied` versions, of any type of its stream,
since a stream version names one event. A copy with no redelivery key has no version, so nothing proves it applied and
it is always applied.

The order is by `heldAt`, with each stream's versions sorted within that stream's slots, so a stream applies in version
order. A copy that would end the instance while another stream still has copies waits until the rest of the sweep has
run, along with the later copies of its own stream. The copies applied this way each go through `process` with their own
compare-and-set, which a lost save retries from the first read.

**Why that order of reads stops a double apply.** The instance is read first, so a copy in the index that the instance
doesn't show applied was not forgotten by that read. A save can only forget a version by trimming it from the marks, and
the attempt that makes the save has swept on the instance it read, or its compare-and-set fails because that instance
changed. So a copy is released before the marks that prove it can go. A release that fails in the sweep fails the
attempt and its input isn't saved. An apply save the sweep already made stands, and the next attempt releases the copy
on the marks.

A copy can apply behind the reorder window, and the marks don't remember a version behind the window. So the save that
applies a version keeps it in `applied`, even behind the window, until the next save of its type. That next save's
attempt swept first and released the copy, or its compare-and-set failed.

### The hold and the fence

`process` checks completed, then quarantined, then not started. A timer of an instance that has not started is skipped.
A non-start event of an instance that has not started is held when `holds` is true, and skipped otherwise. To hold it,
the executor:

1. inserts a copy with `SagaHeldEvents.hold`. The key is the event's redelivery key, which ends in `@` and a version.
   An event without one is keyed by its CloudEvent id and source, followed by a suffix no redelivery key ends in. A
   source is a URI and holds no space, so two held events share a key only when they are the same event. A duplicate key is a
   redelivery of an event already held.
2. reads the instance from the primary, and sweeps when it is started. Then the event is acknowledged.

A hold creates no instance, and nothing that lists instances sees a held event.

The fence is the order of the two writes on each side. The hold reads after its insert, and a save that changes what
held copies do lists them after the save. That is the save that starts an instance, the one that lowers its start and
the one that completes it. A hold whose read comes before that save inserted before the list, so the list finds it. A
hold whose read comes after sees what the save wrote and sweeps itself. Both reads go to the primary. A save's list that
loses a compare-and-set while applying a copy leaves the rest to the next input, and logs at DEBUG. The write that won
can be an input's failure write, which reads no held events, so those copies wait for the next input.

### An event below the start on the start's own stream waits

The save that starts an instance records `startMark`, the stream id and version of the start. `startMark` is
the lowest start event applied on that stream, not the first one delivered. A start with no stream key leaves no
`startMark`, so nothing waits on that instance.

**An event on the start's stream with a version below `startMark`, which is not a start event, is stored and does
nothing yet, on both paths.** Delivered before the start, it is held and its copy waits once the start arrives. Delivered
after the start, it is held and acknowledged, and its copy waits. "Start event" means what `startEventTypes()` would start
on, tested with `isInstance`, so one start type, several, abstract, sealed and subclass cases all follow this one rule. A
held copy records the answer in `startEvent` when it is held. A body that cannot be converted counts as a start event, so
the copy applies and its failure shows.

**A start event below the mark lowers it.** It is classified like any other event. When it applies, `startMark` moves
down to it, and that save lists the held copies, so the waiting copies above it apply in version order. With start type
`A`, another type `C` and the stream `A@3`, `C@4`, `A@5`, an instance that receives `A@5` first starts there. `C@4` waits.
`A@3` arrives, applies, lowers the mark and `C@4` applies after it.

**When no lower start arrives, the event never reacts,** and completing the instance releases its copy with the WARN from
the table. That is right for an event that committed before the real start, which is what a version below the start
usually is. The instance can't tell the two apart, because telling them apart means reading the stream.

On a feed in commit order, an event below the start is skipped with a DEBUG log, as in 0.34.0. Nothing is stored.

**A copy from another stream than the start's is applied, wherever it committed.** Telling a copy that committed before
the start from one that committed after needs positions that become visible in commit order. The MongoDB event store
reserves a position block before its transaction starts, so a lower position can commit after a higher one, and
comparing positions would release an event that committed after the start. Applying the copy instead costs at most one
reaction to an event that committed before the start. That is the same cost a holding feed already pays for such an
event delivered after the start, because classification compares versions and marks, not positions. Holding such a copy
forever instead would keep its bytes and never run its reaction.

**Limits of the wait rule.**

- A start below the mark by the reorder window of its type or more is behind the window, so it is skipped. With window 2,
  start type `A`, another type `C` and the stream `A@3`, `C@4`, `A@8`, an instance that receives `A@8` first starts there.
  `A@3` is skipped and logs the behind-the-window warning, `startMark` stays at 8, and `C@4` never reacts. The end state
  matches a start that never arrived, and the cause differs, because here the start did arrive and was skipped. This is
  ADR 154's limit on an event behind the window, reached through the start.
- `startEvent` is set when the copy is held, so a deploy that changes `startEventTypes()` leaves waiting copies with the
  old answer. A copy that the new definition would start on but that was held as a non-start event waits. If it is
  delivered again it applies as a start event, lowers the start and is released, and otherwise a lower start applies it
  or completion releases it unapplied. A copy held as a start event that the new definition no longer starts on applies on the next attempt without waiting, and
  a lower start that arrives afterwards also runs once. Neither loses an event by itself or fails forever. The second
  doesn't keep the stream's order.
- A failure record or a pending entry naming a waiting event stays until a lower start applies the copy or the instance
  completes.

### A failing copy

A copy that fails in a sweep is not released. The clock for it is on the copy, in `failingSince`, which the first failure
writes with `SagaHeldEvents.failing` and no later failure moves. A copy has no failure record and no pending entry of its
own while it fails.

**The delivery that drove the sweep is held before it is refused**, so a delivery is never refused for another event's
failure with nothing keeping it. A delivery that isn't itself the copy that failed, and that the instance doesn't already
count as handled, is held as a plain copy, and the next sweep applies it. When that hold itself fails, an ERROR names the event and the store
failure, and the delivery is refused.

Then the failure is rethrown, and the subscription's retry or parking runs as for any failing input. When quarantine is on
and the copy's clock has run past `quarantineAfter`, the instance is quarantined with the copy's key on its failure
record and the delivery is acknowledged, unless the delivery could not be held. That charge reads the instance and the
index again first, and quarantines nothing when the copy is gone or the marks prove it applied. A timer's attempt that fails on a copy
charges the same clock, and the timer stays due. [ADR 157](0157-a-sagas-quarantine-budget-runs-per-input.md) has the budget
per input, and the copy's clock is how a held event takes part in it.

### A failed delivery the instance cannot list

ADR 154, as amended, never drops a pending version. A delivery whose own reaction failed is listed as pending on the
instance. Where that can't be done and the delivery holds, a plain copy keeps it, and the failure is rethrown:

1. the instance is at its cap of 10 000 pending versions,
2. the pending write loses its compare-and-set on every attempt,
3. the pending write fails in the store,
4. the quarantine write fails in the store,
5. the version was overtaken in flight. A read made after this delivery classified the version new finds it behind the
   reorder window.

A copy applies on the next sweep, whatever the marks' window says, so nothing marks it as outstanding. When the instance
proves the version applied or lists it pending, no copy is written. An instance that has not started, or that is no
longer active, gets none either. A copy kept while the instance has the version behind the window logs a WARN, since
nothing proves it applied, and if it did, its reaction runs again.

Cause 5 has three entrances. The failure path's own read is the first. A later compare-and-set attempt of the same
delivery is the second. There, attempt 1 classifies the version new, dispatches and loses its save, and attempt 2 finds the version
behind the window. 0.34.0 skips and acknowledges it there, and the executor now throws `OvertakenInFlight`, which takes
the failure path. The first two need deliverers that run in parallel to apply a window of the version's stream and type
in that gap. The third is the delivery's own sweep. The attempt's read classifies the version new, the copies it applies
move the instance past the version, and the same exception takes the failure path, on one thread.

The evidence for cause 5 is per delivery and keyed. Each call of `onCloudEvent` keeps the keys it classified new by
position alone, meaning a classification that would still say new with every marker that keeps a version outstanding
ignored, and that it has not yet seen applied by a save that returned true. Cause 5 applies only when the failing
event's key is in that set. A delivery whose first successful read already finds the version behind the window has an
empty set, so it is an arrival behind the window, ADR 154's own row, and gets no copy. A read that fails on the failure
path can't tell, so the copy is written.

Where the delivery does not hold, causes 1 to 4 rethrow as in 0.34.0, and cause 5 logs an ERROR naming the key and the
instance, then rethrows. On a feed that keeps the commit-order contract, a window can pass a version only after some
deliverer applied it, so that ERROR shows a model that broke the contract.

### Completion, quarantine and delete

- The save that completes an instance lists the held copies after it, and each is released with a WARN. A copy inserted
  after that list, or one whose release failed, stays as storage until a later input reaches the completed instance,
  whose sweep releases it, or until the instance is deleted. See the last consequence below.
- A delivery whose sweep applies the copy that completes the instance is then skipped with no log, as one that arrives
  just after completion is. Its reaction doesn't run, where the same event held a moment earlier would have run before
  the completing copy.
- A quarantined instance skips everything, the sweep included, and keeps every held copy.
- `SagaStateStore.delete(sagaId)`, on a store that also implements `SagaHeldEvents`, removes the instance's held events
  first and then the instance. A hold written between the two is applied if a start ever arrives for that id.

## Consequences

- **Behaviour change for 0.34.0 users.** On a subscription that does not declare commit order, a non-start event that
  arrives before its instance starts is held and applied after the start, instead of being skipped. `run` refuses a
  state store that does not implement `SagaHeldEvents` on such a subscription. The changelog lists both under Breaking
  changes.
- **An event below the start reacts as it does on 0.34.0, unless a lower start arrives.** On 0.34.0 such an event finds
  the stream's watermark above it and is skipped. Here it waits, which stores a copy and runs no reaction, so this is not
  a Breaking changes entry. A lower start applying, and the event above it applying after that, belong to ADR 154's late
  versions.
- **The held events are not bounded.** Where a subscription holds, every acknowledged non-start event of an id whose
  instance never starts stays held until someone removes it. A saga deployed onto a live topic holds the later events of
  instances that began before it existed. A replay of a stream's history from before its start stores a waiting copy per
  event until the instance completes. One INFO per subscription and node, the first time it holds, names the store. The
  runbook gives a MongoDB query for the count and age of held events per saga id, on the `{sagaId: 1, heldAt: 1}` index.
  I chose no loss over a bound, since no fixed bound exists without either stalling or losing.
- **Every attempt on a started instance reads the held index.** That is the sweep above. It stays on a model that declares
  commit order, since a copy held before the saga moved to that model may still be waiting. A delivery doesn't get the copies that wait, since the store filters them
  out. On a local MongoDB, with 10 000 waiting copies on an instance, the median delivery took 8 to 10 ms against 4 to 7 ms
  with none. Without the filter it took 27 to 36 ms and read 2.6 MB. The server still examines every copy of the instance,
  because the only index is `{sagaId: 1, heldAt: 1}`. I didn't add a second index, since it costs every hold a write to
  save a few milliseconds on an instance with thousands of copies. A network adds transfer to the unfiltered read only, so
  these numbers don't show what the filter saves against a remote server.
- **Removing an instance changes on a holding subscription.** [ADR 128](0128-a-renamed-or-removed-step-refuses-its-parked-instances.md)
  gives `delete` as a remedy for an instance parked in a renamed step. After `delete`, a later event of that id is held,
  not skipped. To abandon an instance on such a subscription, keep a terminal envelope and remove its held events, as
  the runbook shows.
- The MongoDB store creates one new collection per saga, named after the saga's collection with `-held-events`, keyed
  by saga id and key. Deployments with restricted roles need to allow that.
- **Preconditions.** Held copies are written with the store's write concern and read from the primary, so the fence
  holds when the write concern survives a failover. A held event whose document would pass the store's size limit cannot
  be held, so its delivery fails as a failing input does.
- **Upgrading.** A 0.34 node skips early events and its whole-document replace drops `startMark`. For a saga whose
  subscription can hold, stop its consumers on every node and start them on 0.35.0 together. A saga on a commit-order
  subscription holds nothing and upgrades as before.

### What this does not close

- **An event overtaken in flight whose copy can't be written.** Take window 2, where `I2` fails, `I5` applies in flight
  and the plain-copy hold throws. The delivery is refused with an ERROR naming the event and the store failure, and its
  redelivery is then skipped as behind the window, so its reaction never runs. It takes two faults at once, a failing
  reaction and a failing store write, and the ERROR is what's left to go on. A read that fails before the hold doesn't
  count, because the hold is still attempted. A crash between classifying a version new and writing its copy is followed
  by a redelivery whose first read finds it behind the window, which is ADR 154's row for an arrival behind the window and
  [#1320](https://github.com/johanhaleby/occurrent/issues/1320)'s.
- **A copy can apply twice.** A copy with no redelivery key has no marks to prove it, so it applies again
  when a crash falls between its apply save and its release, and when another input lists it between that save and that
  release. That is at-least-once, as a crash in a dispatch already is. A hold that loses to a release can insert a copy again after the event applied, which the next sweep releases while the marks still prove
  it, and applies once more when a reorder window has passed it. And the plain copy of a version that applied and was then
  passed by a full reorder window while this delivery of it was being handled applies again, and the WARN above says so.
  A replay of a version the marks have forgotten applies again when its reads fail on the failure path too, because
  nothing then says it is behind the window, and there is no WARN.
- **A copy left on a completed instance is storage only.** It can never apply. The next input to that instance releases
  it, and `delete` removes it. A TTL on the instance collection doesn't remove it, and the runbook's query finds it.
