# 178. A quarantined saga instance records what it skips and is released by replaying it

Date: 2026-11-14

## Status

Accepted at the design gate for [#1259](https://github.com/johanhaleby/occurrent/issues/1259). Amends
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md) by reference, since ADR 134
shipped in 0.34.0. It replaces how Decision point 7 releases an instance and turns quarantine on by default again for
every `SagaRunner`. It also corrects three sentences in ADR 134, two of them ones that
[#1088](https://github.com/johanhaleby/occurrent/issues/1088) found. ADR 134 gets a pointer in its Status section.

## Context

ADR 134 quarantines a saga instance once one input has kept failing for longer than `quarantineAfter`. A quarantined
instance is inert. It skips every input addressed to it, fires no timers, and its redelivery watermarks stay where they
were. In 0.34.0 nothing brings it back, so `SagaStateStore.delete(sagaId)` is the only way out, and that throws the
instance's state and its business process away. That is why ADR 134 was amended before 0.34.0 shipped to make
quarantine off by default.

ADR 134 Decision point 7 described a release that clears the failure record and restarts the saga's subscription at the
position the instance stopped on. The replayed events would reach every instance of the saga, the others would skip
them as redeliveries, and the released instance would apply them.

That restart can't be done safely on the stack the Spring Boot starter builds, which is
`CompetingConsumerSubscriptionModel` over `CatchupSubscriptionModel` over a durable MongoDB model:

- `CompetingConsumerSubscriptionModel` can't be resumed at a position ([ADR 151](0151-a-capability-that-acts-on-a-subscription-is-not-found-past-a-wrapper.md)).
- Pausing a subscription through it gives up the lease, so a "pause, store a position, resume" sequence hands the
  subscription to another node partway through.
- `SagaFailure.position` is `null` for an event from a store that assigns no global position, so there is no position
  to restart such an instance from.
- A restart replays every event since the position for every instance of the saga, and all of them wait for it.

A replay the saga runs itself, reading the event store by global position from where the instance stopped, avoids the
restart. `PositionOrderedReader` documents why that read can't be trusted on MongoDB. An event can reserve a position
below the one the read reached and commit after it, so a read after a position never returns that event, and it would
arrive live while the instance still waits for its replay.

## Decision

### 1. A quarantined instance records the key of every input it skips

Every input a quarantined instance skips has its key recorded, in the order the inputs arrive.
The key is the stream id with the stream version when the event has both, which is the redelivery key
`SagaFailure.input` holds for an input that failed live. An event without them is keyed by its CloudEvent id and source,
which `Filter.cloudEvent(id, source)` reads back. After a release finds a recorded input's event missing,
`SagaFailure.input` holds the key that input was recorded under, so it can be a CloudEvent key, see point 5. A key
already recorded isn't recorded again, and neither is the key of the input the failure record names, so a redelivered
input adds no record. An instance quarantined before 0.35.0 has no
quarantine id, see point 2, so its skips record nothing, as in 0.34.0.

The rule is keyed on the instance being quarantined, not on one branch of the executor. Three places skip an input for
a quarantined instance, and all three record it:

- the quarantined skip in `SagaExecutionSupport.process`.
- the skip for an instance that never started, which runs first when the instance was quarantined on its start event.
- `SagaExecution`'s load-failure path, which skips an input when the state no longer decodes and the instance is
  quarantined. It records through `compareAndSaveWithoutState`, so the undecodable state is never written.

A skip used to write nothing. It now makes the three writes point 2 describes, two compare-and-sets on the instance with
the key's record between them, and each compare-and-set is retried on a lost write like any other save. The second one
changes the instance's version after the record exists, which point 3 relies on. A redelivered input whose key a
skip recorded takes a number that goes unused, since its record write keeps the record already there. The watermarks
still don't move, which is what ADR 134 Decision point 5 requires.

Events from a DCB store are stored with a stream id and a stream version on MongoDB, so they are recorded by stream key
like any other.

### 2. The records are kept outside the instance's document

Each recorded key is a record of its own in the state store, holding the saga id, a quarantine id, a sequence number and
the key. A quarantine can record any number of inputs without growing the instance's document toward MongoDB's 16 MB
limit, so there is no limit on how many it records.

The write that quarantines an `ACTIVE` instance gives its failure record a new quarantine id and sets the skip counter
to 0. The id names the whole quarantine until the instance is `ACTIVE` again, so a release that quarantines the instance
again keeps the id, and keeps the counter for the records that stay. Records are stored under the saga id and the
quarantine id, and a release reads only those under the current id. A record left over from an earlier quarantine of the
same instance is never replayed.

A skip reads the instance's skip counter and raises it by one in a compare-and-set on the instance. The value it read is
the record's sequence number. Then it writes the record with that number, and then it changes the instance's version
again in a second compare-and-set. Each number the skip counter gives out is taken once, so no two skipped inputs of a
quarantine share one, and release reads the records in order of sequence number, which is the order the inputs were
skipped in. The one number written again is -1, which every release writes for its head record, see point 3 step 1.

A crash or a lost race between those writes can skip a number, but never gives two records the same one. When the first
write succeeds and the record isn't written, the input was never acknowledged, so it is offered again and takes a new
number. A redelivered input whose key a skip recorded takes a number that goes unused. Its record write keeps the record
already there, and it still makes the second write, so when the record was written and the second write wasn't, the
redelivery finishes the skip. When two skips of one key race, the record written first stays and the other's number goes
unused in the same way. The record that stays can hold the higher of the two numbers, so the input is replayed after an
input of another stream that took a number between them. Both orders are ones live delivery offered, since the input
arrived before that other input and again after it. A release that finds a record whose skip never finished replays an
input the instance never applied.

Release deletes a record it replayed with `deleteSkippedInput(sagaId, quarantineId, record)`, which deletes the record
of that key only when its sequence number is the one release read. So a delete never removes another record written
under the same key in the meantime. `deleteSkippedInputByKey` deletes by key alone, and release uses it only when it
starts, to make the input it starts from the first record it replays, in point 3 step 1.

`SagaStateStore.delete(sagaId)` also deletes the instance's records. It deletes the instance first and the records after
it. When a delete fails between the two, what remains is records that no instance reads, never a quarantined instance
without the records of what it skipped. When a replayed input completes the instance, release deletes every record of
the instance, under every quarantine id, with `deleteSkippedInputs(sagaId)`, since a completed instance skips every
input and none of them can be replayed. On a store that doesn't implement `deleteSkippedInputs`, release deletes only
the record it applied. After a crash between the write that completes the instance and that delete, the records stay
until `SagaStateStore.delete(sagaId)`.

### 3. Release replays the input the instance stopped on, then the records

`SagaSubscription.release(String sagaId)` releases one instance. K is the input the instance was quarantined on.

1. Release records K first, as a record with the sequence number -1, below every other record's. A record of K's key
   left from an earlier attempt is deleted by its key before that, whatever its number, so K's record moves to the
   head, the first record a release replays. Then release sets `releasedAt` on the failure record in one
   compare-and-set. The same write restarts K's quarantine budget at the release instant, on the failure record, and
   on K's pending entry when K is a stream key with one, so K gets a full budget again before it can quarantine the
   instance again. An instance with `releasedAt` set is releasing.
2. Release replays the records in order of sequence number, K's first. For each one it reads the event from the event
   store by the record's key and applies it through the executor's normal processing, with the quarantined skip
   bypassed for that one input, and deletes the record after the write that applies its input. K is applied once, as
   its record. A record whose input the stream marks show as applied already, after a crash between that write and the
   delete or behind another node's release, is deleted without being applied again.
3. The instance becomes `ACTIVE`, with the failure record and the skip counter cleared, only in a compare-and-set that
   expects the version release read before it found no records left. Release reads the instance first and the records
   after it, so a skip that finishes between the two reads has changed the version, and the write loses.

Release applies the event it read back by that event's stream id and version, with the same budget, failure record and
stream marks as any other input. A record's CloudEvent key is used only to find the event. Every event the MongoDB event
stores write has a stream id and version. An event read back without them, which only a custom `EventStoreQueries` can
return, quarantines the instance again at once, as a missing event does in point 5, with a failure type of its own.

An input that arrives live while the instance is releasing reaches the quarantined skip and is recorded. The skip's
second compare-and-set, after the record, changes the instance's version, so a release that read the records before the
record existed loses its `ACTIVE` write, reads again, and finds the new record. When that second write loses
instead, because the instance went `ACTIVE` first, the input is processed again against the `ACTIVE` instance and applied
like any live input, and its record stays under a quarantine id no release reads again. Either way no input is skipped
without a record that a release reads.

A lost compare-and-set while releasing is either such a skip or another node's release of the same instance. Release
follows the at-least-once dispatch contract of live inputs. A reaction is dispatched before the write that applies its
input, so a node whose write loses has dispatched it too, and two nodes releasing the same instance at once can each
dispatch a record's reaction. When the instance read again holds the same state, the same first record and the same
watermarks, only skips happened. Release then writes the result it computed onto the instance it read again, with that
instance's skip counter and version, and doesn't dispatch the reaction a second time. Anything else, it computes
again. The state is compared with `equals`, so for a state type that doesn't override it, release always
computes again and dispatches the reaction a second time.

An event that commits late below a position another read reached isn't a problem here. Nothing reads a range of
positions. The late event is delivered live, is skipped, and is recorded like any other.

Release reads the store through an `EventStoreQueries` that `SagaRunner` is given. The Spring Boot starter passes its
own bean. A `SagaRunner` built without one refuses to start while quarantine is on, as point 7 says. When such a runner
starts because quarantine is off, `release` throws and names `SagaRunner.eventStoreQueries(..)`.

Release returns what it did. The instance is released and `ACTIVE`, or it is still releasing because an input failed
again inside its budget, or it is quarantined again because that input's budget ran out, or it is quarantined again
because the event of K or of a recorded input is missing from the store or has no stream id and version, or it isn't
quarantined, or no instance has that id. The refusals in point 4 throw instead.

### 4. What release refuses

Release throws `IllegalStateException` for an instance quarantined before 0.35.0. Its failure record has no quarantine
id, so it has no records to replay. The exception message names the way out, `SagaStateStore.delete(sagaId)`. A restart
of the saga's subscription isn't one, since a quarantined instance skips every event a restart offers it.

Release also throws `IllegalStateException` for an instance whose state no longer decodes, and the message names the
decoding failure. To go on, fix how the state decodes and release the instance again, or delete the instance. An
instance whose state stops decoding while it is releasing stays releasing. On a store that answers the poller's query,
each poll logs the failure at `WARN` and tries again, so the release goes on once the state decodes.

### 5. Release runs on any node, and a crashed release is resumed

Release takes no lease and pauses nothing. Every step that changes the instance is a compare-and-set on its document,
so release can run on any node, including a node that doesn't hold the saga's subscription lease. Two nodes releasing
the same instance at once compete for each record. Each step applies the first record it has just read, so the node that
loses reads the instance again, finds that the stream marks show the record's input as applied, and deletes the record
without applying it.

When a recorded input fails again while releasing, the failure is recorded the way any failure is, and the instance
stays releasing. The timer poller also asks for releasing instances on every poll and continues their replay. With a
competing-consumer strategy only the node holding the saga's timer lease polls, which is the Spring Boot starter's
setup, and without one every node does. `SagaStateStore` gains a query for them, a `default` method that throws
`UnsupportedOperationException`, so a custom store keeps compiling. The poller logs that once at `WARN` and stops asking,
and on such a store a failed or interrupted release goes on only when `release` is called again. The poller is what
retries the failed input until it succeeds or its budget runs out, and it also resumes a release that a crash
interrupted.

One poll replays at most `timerBatchLimit` records, 100 by default. Each instance the poll lists gets a share of
`max(1, timerBatchLimit / instances listed)` records, so an instance with many records doesn't hold back the others.
The poll lists at most `timerBatchLimit` releasing instances, so the shares add up to at most `timerBatchLimit`. The
batch is `timerBatchLimit` because that is the bound a poll already puts on the due timers it fires, so no new setting
is needed for it.

The poll lists the releasing instances with `findReleasingSagaIds(after, limit)`, in saga id order after a cursor, and
the next poll lists the ones after the last id this poll listed. A poll that lists fewer than `timerBatchLimit` reached
the end, so the poll after it starts from the first id again. Every releasing instance gets a turn, however many come
before it in saga id order.

The poll fires due timers before it continues releases, and the next poll starts a poll interval after the previous
poll ends, so a long release delays the saga's timers by at most one batch of records per poll. An instance whose live
inputs arrive faster than the poller replays its share doesn't finish on the poller. `release` called directly has no
bound and replays on the calling thread until it reaches one of the outcomes point 3 lists.

When the budget runs out, the instance is quarantined again under the same quarantine id. The input that failed becomes
the failure record's input, `releasedAt` is cleared, and the records after it stay in order. A later release replays the
failed input first and then those records.

When release reads K or a recorded input and its event no longer exists in the store, the instance is quarantined again
at once, with no budget, since retrying doesn't bring a deleted event back. That input becomes the failure record's
input under the same quarantine id, by the key it was recorded under, `releasedAt` is cleared, and the records after it
stay in order. The failure record's type and message say that the event is missing from the store.

To go on, restore the event in the store and release the instance again, or delete the instance with
`SagaStateStore.delete(sagaId)`. When the event is in the store and release was given an `EventStoreQueries` for another
store, fix the configuration and release the instance again.

An event read back without a stream id and version is handled the same way, with a failure type and message of its own.
To go on, give release an `EventStoreQueries` that returns the event with them and release the instance again, or delete
the instance.

In all three cases the instance keeps the record of the input that failed, since neither the write for a used-up
budget nor the write for an event that can't be replayed deletes it. The next release starts by deleting the record of
the input the failure record names, by key alone, and writing it again with the sequence number -1. So that record
moves to the head whatever its number was.

The record that stays does no harm:

- When its key is the input the failure record names, it is the record the next release moves to the head and replays
  first. That is always so for an event that is missing or has no stream id and version, and for a used-up budget when
  the input was recorded by its stream key.
- When it was recorded under a CloudEvent key, a used-up budget names the input by its stream key instead. The next
  release replays the input under its stream key first, reaches the record that stayed after it, finds the input
  applied already, and deletes the record without applying it again. When the record that stayed is K's own at -1,
  two records share -1, and since both name the same input, whichever is replayed first applies it and the other is
  deleted as applied already.

When the read itself fails, the release throws and the instance stays releasing. On a store that answers the poller's
query, the poller tries again on its next poll.

### 6. Timers stay held until the instance is ACTIVE

The status stays `QUARANTINED` while an instance is releasing, and `findWithDueTimers` returns `ACTIVE` instances only.
A timer that came due while the instance was quarantined or releasing fires once it is `ACTIVE`.

So such a timer fires only after every recorded input has been replayed, including inputs that arrived after its
deadline. For a saga that uses a timer as a deadline, a recorded input from after the deadline that cancels or re-arms
the timer means the timeout never fires, where live delivery would have fired it before that input. This record keeps
that order.

### 7. Quarantine is on by default again

`SagaRunnerConfig.defaults()` quarantines after 5 minutes, and so does the Spring Boot starter when
`occurrent.saga.quarantine-after` is left out or blank. ADR 134 turned it off only because nothing brought a quarantined
instance back. Five minutes is the budget ADR 134 Decision point 3 argues for.

It still applies only to subscription models that retain every event they deliver. On any other model `SagaRunner`
turns quarantine off and logs a `WARN`, as it did in 0.34.0.

The default applies to every `SagaRunner`. On a model that retains every event, `SagaRunner.run` throws an
`IllegalStateException` when quarantine is on and the runner has no `EventStoreQueries`, whether quarantine comes from
the defaults or was configured explicitly. The message names `SagaRunner.eventStoreQueries(..)` and
`SagaRunnerConfig.disableQuarantine()` as the two ways out. A runner that quarantines without an `EventStoreQueries`
can't release anything, so `SagaStateStore.delete(sagaId)` would be the only way out, which is the situation ADR 134
turned quarantine off for.

That is a startup failure for a `SagaRunner` built in plain Java without an `EventStoreQueries` that runs on the
defaults, from `SagaRunnerConfig.defaults()` or from the `run` overloads that take no config, which pass
`SagaRunnerConfig.defaults()` themselves. It started in 0.34.0, since the defaults kept quarantine off then.

The Spring Boot starter gives `SagaRunner` the application's `EventStoreQueries` bean when there is one, or several
with one of them `@Primary`. With quarantine on and a model that retains every event, a `@Saga` fails at startup when
there is no `EventStoreQueries` bean, or several and none of them `@Primary`. The message names the saga id and
`occurrent.saga.quarantine-enabled=false`.

`disableQuarantine()` turns it off. In Spring Boot, `occurrent.saga.quarantine-enabled=false` does. A blank
`occurrent.saga.quarantine-after` kept quarantine off in 0.34.0, and it now means the default of 5 minutes.

### 8. Persisted format

- `SagaFailure` gains `releasedAt` and a quarantine id, both nullable. Its five-argument constructor stays. On MongoDB
  they are the top-level `failureReleasedAt` and `failureQuarantineId` fields, next to the other flattened `failure*`
  fields.
- `SagaEnvelope` gains the skip counter. It is delivery bookkeeping like the stream marks, not lifecycle, so
  `SagaFailure` and `SagaInstance` don't hold it. On MongoDB it is a top-level field of the instance's document.
- On MongoDB the records are documents in a collection of their own, named after the saga collection with
  `_skipped_inputs` appended. A unique index on the
  saga id, the quarantine id and the key keeps a key from being recorded twice, and an index on the saga id, the
  quarantine id and the sequence number serves the order release reads them in.
- A document written before 0.35.0 has none of these fields and reads as not releasing, with no quarantine id.

On MongoDB, an instance whose failure record has a quarantine id is stored with the status `QUARANTINED_RECORDING`
instead of `QUARANTINED`. `SagaStatus` doesn't change. The 0.35.0 store reads both strings as `QUARANTINED`, and its
queries for quarantined instances match both. The string is there for 0.34.0, which decodes the status with
`SagaStatus.valueOf` and throws on a value it doesn't know.

Nothing outside the 0.35.0 store matches the new string. A query written by hand for `status: "QUARANTINED"`, like the
one the runbook gives for 0.34.0, doesn't find an instance that 0.35.0 quarantined, and neither does an alert built on
such a query or `findByStatus` in the 0.34.0 store. A query by hand has to match both strings, with `$in`.

So a 0.34.0 node in a mixed cluster throws on every event addressed to an instance that 0.35.0 quarantined, instead of
skipping the input without a record. Quarantine is on by default during a rolling upgrade too, so a cluster upgraded
one node at a time can meet such an instance. Every subscription model a saga can quarantine on writes its position or
checkpoint only after the action returns, so the event the 0.34.0 node fails on, and everything after it, is delivered
again. No input is lost.

On a live MongoDB subscription the 0.34.0 node retries the event in place with the subscription model's retry
strategy, which by default backs off from 100 ms to 2 s and never gives up. In a catch-up replay that a subscribe
started, the replay fails and the subscription doesn't go live on that node until the node restarts, which its upgrade
does. A replay that a resume started runs again from its last stored checkpoint with the same backoff.

The 0.34.0 node keeps the saga's competing-consumer lease while it retries, because the lease is refreshed on a
schedule of its own whatever the subscription is doing, so the saga's subscription stalls. The node gives the lease up
when it is stopped for its upgrade, but the next node to take it may still run 0.34.0 and stall on the same event, so
the stall ends once a 0.35.0 node holds the lease. It also ends when a 0.35.0 node releases or deletes the instance, so it
is no longer quarantined. A released instance is `ACTIVE` and is stored with that plain status, which 0.34.0
decodes.

Without a competing-consumer strategy every node runs the saga's subscription. The 0.35.0 nodes handle the event, and
only the 0.34.0 node's own subscription stalls, until the node is upgraded or the instance is no longer quarantined.

The subscription delivers one event at a time, so the stall holds up every later event of that saga subscription, for
every instance of the saga, not only the events of the quarantined instance.

After a rollback to 0.34.0 there is no 0.35.0 node left to take the lease or to release the instance, so the stall
lasts until the instance is deleted, with competing consumers or without. A node whose catch-up stopped on the event
also has to restart before its subscription goes live. A downgrade that doesn't stall anything goes like this, on
0.35.0:

1. Turn quarantine off on every node, and have that running on every node before step 2. A node that still has
   quarantine on can quarantine another instance after the instances are listed.
2. Find the quarantined instances with `findByStatus(QUARANTINED, ..)` and call `release` on each whose failure record
   has a quarantine id.
3. Delete with `SagaStateStore.delete(sagaId)` each one whose release returns `QUARANTINED_AGAIN`,
   `QUARANTINED_EVENT_MISSING` or `STILL_RELEASING`.
4. When a release throws, the instance stays quarantined. Fix the cause the exception names, such as a state that no
   longer decodes or an event store read that failed, and release the instance again, or delete it.
5. Downgrade.

An instance 0.34.0 quarantined needs nothing, and an instance whose release returns `NOT_QUARANTINED` is already
released. Of what this record adds, only the `QUARANTINED_RECORDING` status is something 0.34.0 can't decode, and no
instance has it after these steps.

<!-- Q2: downgrade sentence pending ruling -->

The upgrade guide has the procedure for operators, in
[Rolling upgrades from 0.34.0 and rolling back to it](../../migration/upgrading-to-0.35.0.md#rolling-upgrades-from-0340-and-rolling-back-to-it).

### 9. Three corrections to ADR 134

- ADR 134's Status section says the quarantine budget is off on "push feeds and the broker sources". `SagaRunner` also
  turns it off for `CatchupThenPushSubscriptionModel` and for the in-memory subscription model, so that list is
  incomplete. Quarantine runs only on a model that retains every event, and point 7 of this record keeps that rule.
- ADR 134 Decision point 3 calls the budget "the earliest a failing instance can be quarantined". The budget runs from
  the first time an event failed, not from its latest failure. So an event whose earlier failure the instance still
  records, from longer ago than the budget, quarantines the instance the first time it fails again. Replaying a parked
  event, turning quarantine on for an event that failed while it was off, and moving a MongoDB subscription back by hand
  to before a failed event all produce such an event.
- ADR 134 Decision point 7 says that keying the failure record by its redelivery key means "release can be added on top
  of instances written by 0.34.0 without migrating them". Release refuses those instances, as point 4 says. An instance
  that 0.34.0 quarantined recorded none of the inputs it skipped, and its failure record has no quarantine id.

## Consequences

- A quarantined instance can be brought back without stopping the saga and without a redeploy. While the poller
  continues a release, the saga's timers wait for at most one batch of records per poll.
- A skip by a quarantined instance is now three writes, two compare-and-sets on the instance and the record between
  them.
- A quarantined instance keeps a record of every input it skips, with no limit. The records take space in the store
  until a release replays them, a replayed input completes the instance, or the 0.35.0 store deletes the instance.
  Records left under an earlier quarantine id, see points 2 and 3, stay until the instance is deleted, or until a
  replayed input completes it on a store that implements `deleteSkippedInputs`, see point 2. A 0.34.0 `delete` after a
  downgrade removes none of them.
- An instance whose K or recorded input refers to an event that was deleted from the store stays quarantined until the
  event is restored or the instance is deleted.
- A 0.34.0 node, in a mixed cluster or after a downgrade, stalls the subscription of a saga with an instance that
  0.35.0 quarantined, for every later event of that subscription. It loses no input. In a mixed cluster the stall ends
  once a 0.35.0 node holds the saga's competing-consumer lease, or once a 0.35.0 node releases or deletes the
  instance. Without competing consumers it ends when the 0.34.0 node is upgraded. After a downgrade only a delete ends
  it.
  A downgrade that first turns quarantine off on every node, and then releases those instances and deletes the ones
  that stay quarantined, stalls nothing.
- An application that never configured quarantine starts quarantining on upgrade, and so does one that set
  `occurrent.saga.quarantine-after` blank to keep it off. A blank value meant quarantine off in 0.34.0 and means the
  5-minute default in 0.35.0.
- A `SagaRunner` with quarantine on, on a model that retains every event, and without an `EventStoreQueries` fails at
  startup. That includes a runner built in plain Java on the defaults, from `SagaRunnerConfig.defaults()` or from a
  `run` overload without a config, which started in 0.34.0. A
  `@Saga` in Spring Boot fails at startup the same way when there is no `EventStoreQueries` bean, or several and none
  of them `@Primary`. The changelog and the upgrade guide name `disableQuarantine()` and
  `occurrent.saga.quarantine-enabled=false` as the ways to turn quarantine off.
- `SagaFailure` gains two components, so a record pattern over it stops compiling. `SagaEnvelope` changes in 0.35.0
  already, and gains one more component.
- A custom `SagaStateStore` has to persist the new fields, keep the records, implement `deleteSkippedInput`,
  `deleteSkippedInputByKey` and `deleteSkippedInputs`, delete the records in `delete(sagaId)`, and implement the query
  for releasing instances.
  A release reads the instance back after setting `releasedAt` and throws
  when `releasedAt`, the quarantine id or the skip counter didn't persist, rather than replaying into an instance that
  would forget it is releasing.

## Rejected alternatives

- **Restart the saga's subscription at K's position**, as ADR 134 Decision point 7 described. The Context above has the
  reasons. It needs a pause that keeps the competing-consumer lease, which ADR 151 rules out, it can't release an
  instance with no position, and every instance of the saga waits for the replay.
- **Replay from the event store by global position**, filtered to the instance. Reading after a position misses an event
  that commits late below it, and closing that gap means a second replay-to-live handover next to the one
  `CatchupSubscriptionModel` already has.
- **A second, temporary subscription per release**, starting at K's position and feeding only the instance. The
  competing-consumer model would give it its own lease, but deciding when the instance may go `ACTIVE` and the temporary
  subscription may stop is the same handover problem between two subscriptions.
- **Keep the records in a list on the instance's document.** The list grows the document toward MongoDB's 16 MB limit,
  so it needs a limit, and an instance that skips more inputs than the limit can't be released.
- **Log an input whose event is missing and drop its record.** That loses the input. A wrong `EventStoreQueries`, one
  pointed at another database for example, would find no event for any record, drop them all, and set the instance
  `ACTIVE` without any of the inputs it skipped.
- **A new `SagaStatus.RELEASING`.** Every exhaustive `switch` over `SagaStatus` would stop compiling again. A 0.34.0 node
  throwing on a status it doesn't know is what point 8 wants, and the store-only `QUARANTINED_RECORDING` gets that
  without a public enum value.
