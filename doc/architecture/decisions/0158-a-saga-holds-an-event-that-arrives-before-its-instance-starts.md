# 158. A saga holds an event that arrives before its instance starts, unless its subscription delivers in commit order

Date: 2026-10-29

## Status

Proposed. Resolves [#1317](https://github.com/johanhaleby/occurrent/issues/1317), and with
[ADR 154](0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md) as amended in place, the self-hold
half of [#1330](https://github.com/johanhaleby/occurrent/issues/1330). ADR 154 has not shipped, so it changes in place
and points here. [ADR 128](0128-a-renamed-or-removed-step-refuses-its-parked-instances.md) shipped in 0.34.0, and its
`delete` remedy reads differently on a subscription that can hold, so it gets a pointer in its Status section.

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
version the instance cannot list is self-held, and whether `run` refuses a state store that cannot hold.

Commit order here means what the `CommitOrderedSubscriptions` contract says. No event is delivered before every earlier
event matching the subscription was delivered and its listener returned normally, where an event is earlier when it
committed earlier or was written earlier in the same commit. A delivery whose listener throws is retried in place before
anything after it. On such a feed only an event that came before its start in that order can arrive before the start,
also when both were written in one commit. Nothing is held, and that event is skipped as in 0.34.0. A model that answers true and breaks that
contract is that model's defect, and the ERROR of cause 5 below is where it shows.

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

### The hold and the fence

`process` checks completed, then quarantined, then not started. A timer of an instance that has not started is skipped.
A non-start event of an instance that has not started is held when `holds` is true, and skipped otherwise. To hold it,
the executor:

1. inserts a copy with `SagaHeldEvents.hold`. The key is the event's redelivery key, which ends in `@` and a version.
   An event without one is keyed by its CloudEvent id and source, followed by a suffix no redelivery key ends in. A
   source is a URI and holds no space, so two held events share a key only when they are the same event. A duplicate key is a
   redelivery of an event already held.
2. reads the instance from the primary.
   - When the instance is absent or not started, the event is acknowledged and the start's delivery applies it.
   - When it is started and active, `markHeldEvents(sagaId, null)` marks the instance as having events to drain. A
     delivery with a redelivery key then runs again on the started instance, which drains the held events and looks up
     its own copy. Other deliveries can move the instance past the event's reorder window before the hold, and a drain
     does not apply such a copy but waits for the next delivery of its event, which this delivery is. A delivery without a redelivery key
     only drains, since running it again would apply the event a second time beside its copy.
   - When it is completed, the copy is released with a WARN, which is what 0.34.0 does with an event that arrives after
     completion.
   - When it is quarantined, the copy stays and the event is acknowledged.

A hold creates no instance, and nothing that lists instances sees a held event.

The fence is the order of the two writes on each side. The hold reads after its insert, and the start lists the held
events after its save. A hold whose read comes before the start's save inserted before the start's list, so the list
finds it. A hold whose read comes after sees the instance started and drains it itself. Both reads go to the primary,
and so does the read of the whole instance that the drain starts from.

### The drain

**The flag.** The save that starts an instance writes `heldEventsChecked` as false and records `startMark`, the start's
stream id, version and position when the start has one. A drain is due while the flag is false. It runs on the start's own
delivery after its save, on a hold that finds the instance started, and on any input to a started instance with a drain
due, on every node whatever that node's subscription answers. The drain runs inside the executor's compare-and-set
retry, before the input that drove it, and the save of that input writes the flag true. A mark made meanwhile raises
the version, that save loses, and the retry drains again.

**Proof that a version applied** means positive evidence on the instance only. The version is a mark, or in `applied`
of a type mark of its stream, or its key is in `drainedHeldKeys`. A save that applies a held copy adds its key to
`drainedHeldKeys` before the copy is released, so the proof survives a crash between the two. Each later save in the
same drain drops the keys whose copies the drain has released since, so the set holds the keys whose release has not
happened yet rather than every key the drain applied, and a drain of many copies does not grow the instance towards the
store's document limit. The drain's ending save empties the set, except for the keys it applied itself. A save outside
a drain that adds a key, the lookup below applying a copy or a redelivery applying a self-held version, marks a drain
due, and that drain runs right after the save and its ending save forgets the key. A key stays only while that drain has
not ended, which is when the proof is still needed.

**The whole set.** The drain reads `heldIndex(sagaId)`, every held copy without its body, orders it by `heldAt` with
each stream's versions sorted within that stream's slots, and fetches the bodies in pages of `timerBatchLimit`. A body
gone at fetch was released by another deliverer and is passed over. One pass in that order decides each copy:

| The copy | What the drain does |
|---|---|
| proven applied | release |
| its key is in `selfHeldKeys`, or the copy is marked `outstanding` | apply, however far behind the reorder window, except that `process` skips a copy of another type below the start and the drain releases it |
| held before the start, on the start's stream, below the start's version, on a saga whose one start event type is neither abstract nor sealed, and the instance lists no earlier event of the start's type below it | release, DEBUG |
| self-held, its key in neither set | plain classification, apply when new, otherwise leave it with a WARN |
| behind the reorder window | leave it with a WARN. The lookup below applies it on its event's next delivery |
| a redelivery only by the pre-0.35 floor (ADR 154) | apply, with an ERROR naming the key, since a held copy was never applied |
| new, and applying it ends the instance while another stream has copies not yet decided | defer it and its stream's later copies |
| new | apply |

The deferred copies are applied after the pass, in walk order, without deferring again. Once the instance is terminal,
every copy left is released with a WARN.

**A copy from another stream than the start's is applied, wherever it committed.** Telling a copy that committed before
the start from one that committed after needs positions that become visible in commit order. The MongoDB event store
reserves a position block before its transaction starts, so a lower position can commit after a higher one, and
comparing positions would release an event that committed after the start. Applying the copy instead costs at most one
reaction to an event that committed before the start. That is the same cost a holding feed already pays for such an
event delivered after the start, because classification compares versions and marks, not positions. Holding such a copy
forever instead would keep its bytes and never run its reaction.

**On a saga whose one start event type is a class that is neither abstract nor sealed, an event of another type on the
start's own stream below the start's version is skipped.** A lower version of that stream committed before the start, so
a delivery of such an event after the start is skipped with a DEBUG log, the same as the drain releases its copy.
Classification alone would apply it, because ADR 154 compares a version with the marks of its own type, and a type with
no mark has nothing to compare with. An event of the start type itself is still classified. An instance that names the
skipped event, in a failure record, a pending entry or a self-held key, saves itself without it, because only a save that
applies an event takes it off otherwise.

**A saga with several start event types classifies an event below the start like any other, on both paths.**
`startMark` is the first start event delivered, not the first one on the stream. With start types `A` and `B`, another
type `C` and the stream `B@3`, `C@4`, `A@5`, an instance that receives `A@5` first starts there. `C@4` committed after
the real first event, so skipping it would lose it, and classifying it costs at most one extra reaction to an event
that committed before the start. An abstract class, a sealed class or an interface as the only start event type counts as several, because
`startsOn(Opening.class)` lets every event type below it start the instance, and the `Class` that `startEventTypes()`
returns says so without reading the stream.

**A start event type that appears twice on one stream is a limit of this decision while the instance knows only the
later one.** With one start type `A`,
another type `C` and the stream `A@3`, `C@4`, `A@5`, an instance that receives `A@5` first starts there. `C@4` is then
skipped when it is delivered after `A@5` and released by the drain when it was held before it, although it committed
after `A@3`. `A@3` is applied, since it is of the start type. When the instance already lists `A@3`, as pending because it
failed or as applied after the start, `C@4` is classified on both paths, so `A@3` failing, `C@4` being held and `A@5`
starting loses nothing. The instance finds the start's type by looking for the start's version among its marks, so once it
has moved the reorder window or more past the start it no longer knows the type, and `C@4` is skipped again. A class
that is neither abstract nor sealed, with a subclass on the stream, can lose an event the same way, and an earlier event of
the subclass does not change that even when the instance lists it, because it sits under the subclass's own type. The
builder refuses such a class unless `replacementFilter(...)` is set, so only that or a `Saga` of your own reaches it.
`SagaHeldEventTest` records these cases. Telling `C@4` from an event before the real start while `A@3` has not arrived
would mean reading the stream, which the saga does not do.

A held copy is applied through `process`, with its own meta and its own failure handling. When it fails, the driving
input is refused as any failing input is, the failed version is listed pending or self-held as below, and REDELIVER
retries the driving input, which drains first. When the held copy's failure quarantines the instance instead, the
driving input is acknowledged without being applied, as every later input of a quarantined instance is. The retention
check asks about the driving input, since that is the event the subscription moves past, and the held copy stays in the
store. A held copy that fails in a drain a timer ran is kept and measured the same way, and the timer stays due until the
held copy applies. A timer's attempt asks whether the timer is still due after the drain, because a held copy can cancel
or re-arm it. A held copy whose conversion or `evolve` throws while the drain checks whether it ends the instance
is applied at once, so that failure is its own as well. A held copy with no redelivery key has no clock of its own, so the
budget runs on the keyed delivery it stops, and a quarantine names that delivery. A drain a timer ran has no such delivery,
so that failure is only logged. A copy the instance counts as handled or behind the window that fails on the lookup below
gets its key in `selfHeldKeys`, which makes it new, so a failure record names it, its budget runs and the next drain
applies it.

**The lookup on every keyed delivery.** Whenever the state store can hold, every keyed delivery to a started instance
reads the held copy with its own key on each attempt, after the drain and before classifying the delivery, whatever `holds` is. So a lost compare-and-set reads it again, and so does the run after a hold. A
copy held while the saga ran on a model that reorders is then still found when its event comes back on a model that
declares commit order, as after a deploy that moves the saga to another model. A self-held copy not marked
`outstanding` is left to the drain. Any other copy is released without being applied when the marks show its event
applied or when the delivery skips the event as committed before the start. Otherwise the delivery applies it, adds its
key to `drainedHeldKeys` and releases it, also when it would have skipped the event as behind the window or as a
redelivery from the pre-0.35 floor. The key means a release that fails cannot apply the copy a second time. The floor case
logs the same ERROR as the drain's, because it makes the same trade.

### The self-hold of a failed version

ADR 154, as amended, never drops a pending version. A failed version that cannot be listed pending is kept a second
way, by holding it. There are five causes:

1. the instance is at its cap of 10 000 pending versions,
2. the pending write loses its compare-and-set on every attempt,
3. the pending write fails in the store,
4. the quarantine write fails in the store,
5. the version was overtaken in flight. A read made after this delivery classified the version new finds it behind the
   reorder window.

Cause 5 has two entrances. The failure path's own read is one. A later compare-and-set attempt of the same delivery is
the other. There, attempt 1 classifies the version new, dispatches and loses its save, and attempt 2 finds the version
behind the window. 0.34.0 skips and acknowledges it there, and the executor now throws `OvertakenInFlight`, which takes
the failure path. Both entrances need deliverers that run in parallel to apply a window of the version's stream and type
in that gap.

**The evidence for cause 5 is per delivery and keyed.** Each call of `onCloudEvent` keeps the keys it classified new by
position alone, meaning a classification that would still say new with every marker that keeps a version outstanding
ignored, and that it has not yet seen applied by a save that returned true. Cause 5 applies only when the failing
event's key is in that set. A delivery whose first successful read already finds the version behind the window has an
empty set, so it is an arrival behind the window, ADR 154's own row, not cause 5.

**Who self-holds.** A held copy that failed in a drain is self-held on every node, whatever the driving delivery's
answer, because the copy exists and the node read it through `SagaHeldEvents`. The delivery's own event is self-held
where its delivery holds. Where it does not, causes 1 to 4 rethrow as in 0.34.0, and cause 5 logs an ERROR naming the
key and the instance, then rethrows. On a feed that keeps the commit-order contract, a window can pass a version only
after some deliverer applied it, so that ERROR shows a model that broke the contract.

**The steps.**

1. `markHeldEvents(sagaId, key)` adds the key to `selfHeldKeys`, marks a drain due and raises the version, in one write
   that succeeds on any active started instance whatever its version. It answers the instance as it was before the write.
   Empty means nothing matched, so nothing is held and the failure is rethrown.
2. The decision is taken on that answer with the key itself ignored, since a concurrent self-hold or a store's retry of
   a write that succeeded may have added it already. When the answer proves the version applied or lists it pending,
   `unmarkSelfHeld` pulls the key and the failure is rethrown. Otherwise the key stays. A version kept while the answer
   has it behind the window logs a WARN, since nothing proves it applied, and if it did, its reaction runs again.
3. For the delivery's own event, `hold` inserts the copy with `selfHeld` true, and a duplicate key is a copy already
   held. A held copy that failed is never inserted again. When it is gone, it was applied or proven applied since it
   failed, so the key is pulled and nothing is held.
4. `markHeldEvents(sagaId, null)` makes a drain due once more. A drain that read the instance after step 1 and ended
   before step 3 found no copy and wrote the flag true, and without this mark the copy would wait for a drain that is
   never due.
5. The original failure is rethrown, so the delivery is refused and never acknowledged.

Both marks raise the version, so a whole-instance save that read before them loses and reads them back. While the key
is in `selfHeldKeys`, classification treats the version as outstanding however far behind the window it is, so the
redelivery applies it once, and that save pulls the key and adds it to `drainedHeldKeys`. A version proven applied on a
later read has its key pruned before anything is classified. When one of the self-hold's own writes fails, one ERROR
names the event, the instance and the store failure, the store failure is added to the original as suppressed, and the
original is rethrown.

**The document size limit.** When the mark is refused because the instance is at the store's document size limit, the
key cannot be added, so the marker moves to the held copy. The fallback marks a drain due without a key, decides as in
step 2, and marks the copy `outstanding`. For the delivery's own event that is a hold with `outstanding` true, or
`markOutstanding` on a copy already held. For a held copy it is `markOutstanding` only. A held copy that is gone was
applied or proven applied, and inserting it again would apply it a second time once that proof fades. The fallback marks
a drain due once more, logs an ERROR and rethrows. The marker is a boolean already on the held document, so setting it
never grows a document. It is needed because the event's redelivery may arrive on a delivery that does not hold, a
competing consumer's takeover or a catch-up handover, where the lookup does not run and the version reads behind the
window. With the marker, any node's drain applies the copy.

**The premise.** A self-held copy whose key is in neither `selfHeldKeys` nor `drainedHeldKeys` is either applied or
still pending, and plain classification tells which. That holds because a kept key names a version step 2 did not prove
applied or find pending, every 0.35 write keeps the sets as it read them, every apply pulls the key, and the only other
removals are completion, a whole-document replace by a 0.34 node, and a concurrent self-hold's pull, which happens only
on proof or a pending entry.

### Completion, quarantine and delete

- The save that completes an instance, and the ending save of a drain that found it terminal, empty `selfHeldKeys`. A
  held copy that completion overtakes is released with a WARN. A self-held copy inserted after the completing drain read
  the index stays as storage on a completed instance, since a completed instance never drains, and the runbook's query
  finds it.
- A quarantined instance skips everything, the drain included, and keeps every held copy.
- `SagaStateStore.delete(sagaId)`, on a store that also implements `SagaHeldEvents`, removes the instance's held events
  first and then the instance. A hold written between the two is applied if a start ever arrives for that id.

## Consequences

- **Behaviour change for 0.34.0 users.** On a subscription that does not declare commit order, a non-start event that
  arrives before its instance starts is held and applied after the start, instead of being skipped. `run` refuses a
  state store that does not implement `SagaHeldEvents` on such a subscription. The changelog lists both under Breaking
  changes.
- **The held events are not bounded.** Where a subscription holds, every acknowledged non-start event of an id whose
  instance never starts stays held until someone removes it. A saga deployed onto a live topic holds the later events of
  instances that began before it existed. One INFO per subscription and node, the first time it holds, names the store.
  The runbook gives a MongoDB query for the count and age of held events per saga id, on the `{sagaId: 1, heldAt: 1}`
  index. I chose no loss over a bound, since no fixed bound exists without either stalling or losing.
- **Removing an instance changes on a holding subscription.** [ADR 128](0128-a-renamed-or-removed-step-refuses-its-parked-instances.md)
  gives `delete` as a remedy for an instance parked in a renamed step. After `delete`, a later event of that id is held,
  not skipped. To abandon an instance on such a subscription, keep a terminal envelope and remove its held events, as
  the runbook shows.
- **An event between two start events of one stream can be lost on a saga whose one start event type is neither abstract
  nor sealed**, when the later start event reaches the instance first and the instance does not list the earlier one yet.
  The decision above describes the case, and closing it would mean reading the stream.
- **Every attempt of a keyed delivery reads the held collection once when the state store can hold.** That is the lookup above. It
  stays on a model that declares commit order, since a copy held before the saga moved to that model may still be
  waiting for its event.
- The MongoDB store creates one new collection per saga, named after the saga's collection with `-held-events`, keyed
  by saga id and key. Deployments with restricted roles need to allow that.
- **Preconditions.** Held copies are written with the store's write concern and read from the primary, so the fence
  holds when the write concern survives a failover. A held event whose document would pass the store's size limit cannot
  be held, so its delivery fails as a failing input does.
- **What this does not close.** A held copy can be applied twice when it applied and a full reorder window of its type
  passed it while its own failure was handled, and the WARN above says so. A crash between classifying a version new and
  the self-hold's mark is followed by a redelivery whose first read finds the version behind the window, which is ADR 154's row
  for an arrival behind the window and [#1320](https://github.com/johanhaleby/occurrent/issues/1320)'s.
- **Upgrading.** A 0.34 node skips early events and its whole-document replace drops `startMark`, `drainedHeldKeys` and
  `selfHeldKeys`. For a saga whose subscription can hold, stop its consumers on every node and start them on 0.35.0
  together. A saga on a commit-order subscription holds nothing and upgrades as before.
