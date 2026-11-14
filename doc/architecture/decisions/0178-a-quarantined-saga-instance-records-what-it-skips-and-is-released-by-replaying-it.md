# 178. A quarantined saga instance records what it skips and is released by replaying it

Date: 2026-11-14

## Status

Accepted at the design gate for [#1259](https://github.com/johanhaleby/occurrent/issues/1259). Amends
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md) by reference, since ADR 134
shipped in 0.34.0. It replaces how Decision point 7 releases an instance and turns quarantine on by default again. It
also corrects two sentences in ADR 134 that [#1088](https://github.com/johanhaleby/occurrent/issues/1088) found. ADR 134
gets a pointer in its Status section.

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
`SagaFailure.input` holds. An event without them is keyed by its CloudEvent id and source, which
`Filter.cloudEvent(id, source)` reads back. A key already recorded isn't recorded again, and neither is the key of the
input the failure record names, so a redelivered input adds no record. An instance quarantined before 0.35.0 has no
quarantine id, see point 2, so its skips record nothing, as in 0.34.0.

The rule is keyed on the instance being quarantined, not on one branch of the executor. Three places skip an input for
a quarantined instance, and all three record it:

- the quarantined skip in `SagaExecutionSupport.process`.
- the skip for an instance that never started, which runs first when the instance was quarantined on its start event.
- `SagaExecution`'s load-failure path, which skips an input when the state no longer decodes and the instance is
  quarantined. It records through `compareAndSaveWithoutState`, so the undecodable state is never written.

A skip used to write nothing. It now writes the key's record, and then a compare-and-set on the instance that raises a
skip counter on it, retried on a lost write like any other save. A skip whose key is recorded already still raises the
counter. That write changes the instance's version, which point 3
relies on. The watermarks still don't move, which is what ADR 134 Decision point 5 requires.

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

A record takes the skip counter's value as its sequence number, and the instance write after it raises the counter.
When that instance write fails, after a crash or a lost compare-and-set, the record stays and the next skip can
take the same number. Records are read in order of sequence number and then key, so the order is fixed either way. The
input of such a record was never acknowledged, so it is offered again before any later input of its stream, and its
skip raises the counter then. So two inputs of one stream never share a number, and a release that finds the record
replays an input the instance never applied.

`SagaStateStore.delete(sagaId)` also deletes the instance's records.

### 3. Release replays the input the instance stopped on, then the records

`SagaSubscription.release(String sagaId)` releases one instance. K is the input the instance was quarantined on.

1. Release sets `releasedAt` on the failure record in one compare-and-set. The same write restarts K's quarantine
   budget at the release instant, on the failure record and on K's pending entry, so a release that fails again gets a
   full budget before it is quarantined again. An instance with `releasedAt` set is releasing.
   Before that write, release records K with a sequence number below every other record's, replacing a record of
   K's key if one is left.
2. Release reads K's event from the event store by its key and applies it through the executor's normal processing,
   with the quarantined skip bypassed for that one input. Then it does the same for each record, in order, and deletes
   the record after the write that applies its input. A record whose input the stream marks show as applied already,
   after a crash between that write and the delete or behind another node's release, is deleted without being applied
   again.
3. The instance becomes `ACTIVE`, with the failure record and the skip counter cleared, only in a compare-and-set that
   expects the version release read together with no records left.

Release applies the event it read back by that event's stream id and version, with the same budget, failure record and
stream marks as any other input. A record's CloudEvent key is used only to find the event. Every event the MongoDB event
stores write has a stream id and version. An event read back without them, which only a custom `EventStoreQueries` can
return, quarantines the instance again at once, as a missing event does in point 5, with a failure type of its own.

An input that arrives live while the instance is releasing reaches the quarantined skip and is recorded. The
compare-and-set that raises the skip counter after the record changes the instance's version, so a release that read the
records before that input loses its `ACTIVE` write, reads again, and finds the new record. When the counter write loses
instead, because the instance went `ACTIVE` first, the input is processed again against the `ACTIVE` instance and applied
like any live input, and its record stays under a quarantine id no release reads again. Either way no input is skipped
without a record that a release reads.

A lost compare-and-set while releasing is either such a skip or another node's release of the same instance. Release
follows the at-least-once dispatch contract of live inputs, so a reaction is dispatched once for each time its input is
applied. When the instance read again holds the same state, the same first record and the same watermarks, only skips
happened, and release writes the result it computed again without dispatching the reaction a second time. Anything else,
it computes again.

An event that commits late below a position another read reached isn't a problem here. Nothing reads a range of
positions. The late event is delivered live, is skipped, and is recorded like any other.

Release reads the store through an `EventStoreQueries` that `SagaRunner` is given. The Spring Boot starter passes its
own bean. A `SagaRunner` built without one still records skipped inputs, and `release` throws, naming the missing
`EventStoreQueries`.

Release returns what it did. The instance is released and `ACTIVE`, or it is still releasing because an input failed
again inside its budget, or it is quarantined again because that input's budget ran out, or it is quarantined again
because the event of K or of a recorded input is missing from the store or has no stream id and version, or it isn't
quarantined, or no instance has that id. The refusal in point 4 throws instead.

### 4. What release refuses

Release throws `IllegalStateException` for an instance quarantined before 0.35.0. Its failure record has no quarantine
id, so it has no records to replay. The exception message names the way out, `SagaStateStore.delete(sagaId)`. A restart
of the saga's subscription isn't one, since a quarantined instance skips every event a restart offers it.

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

When the budget runs out, the instance is quarantined again under the same quarantine id. The input that failed becomes
the failure record's input and its record is deleted after that write, `releasedAt` is cleared, and the records after
it stay in order. A later release replays the failed input first and then those records.

When release reads K or a recorded input and its event no longer exists in the store, the instance is quarantined again
at once, with no budget, since retrying doesn't bring a deleted event back. That input becomes the failure record's
input under the same quarantine id, its record is deleted after that write, `releasedAt` is cleared, and the records
after it stay in order, so nothing is lost. The failure record's type and message say that the event is missing from
the store.

To go on, restore the event in the store and release the instance again, or delete the instance with
`SagaStateStore.delete(sagaId)`. When the event is in the store and release was given an `EventStoreQueries` for another
store, fix the configuration and release the instance again.

An event read back without a stream id and version is handled the same way, with a failure type and message of its own.
To go on, give release an `EventStoreQueries` that returns the event with them and release the instance again, or delete
the instance.

When the read itself fails, the release throws and the instance stays releasing. On a store that answers the poller's
query, the poller tries again on its next poll.

### 6. Timers stay held until the instance is ACTIVE

The status stays `QUARANTINED` while an instance is releasing, and `findWithDueTimers` returns `ACTIVE` instances only.
A timer that came due while the instance was quarantined or releasing fires once it is `ACTIVE`.

### 7. Quarantine is on by default again

`SagaRunnerConfig.defaults()` quarantines after 5 minutes, and so does the Spring Boot starter when
`occurrent.saga.quarantine-after` is left out. ADR 134 turned it off only because nothing brought a quarantined instance
back. Five minutes is the budget ADR 134 Decision point 3 argues for.

It still applies only to subscription models that retain every event they deliver. `SagaRunner` turns quarantine off on
any other model, as it did in 0.34.0.

`disableQuarantine()` turns it off. In Spring Boot, `occurrent.saga.quarantine-enabled=false` does, since a blank or
missing `occurrent.saga.quarantine-after` now means the default.

### 8. Persisted format

- `SagaFailure` gains `releasedAt` and a quarantine id, both nullable. Its five-argument constructor stays. On MongoDB
  they are the top-level `failureReleasedAt` and `failureQuarantineId` fields, next to the other flattened `failure*`
  fields.
- `SagaEnvelope` gains the skip counter. It is delivery bookkeeping like the stream marks, not lifecycle, so
  `SagaFailure` and `SagaInstance` don't hold it. On MongoDB it is a top-level field of the instance's document.
- On MongoDB the records are documents in a collection of their own next to the saga collection. A unique index on the
  saga id, the quarantine id and the key keeps a key from being recorded twice, and an index on the saga id, the
  quarantine id, the sequence number and the key serves the order release reads them in.
- A document written before 0.35.0 has none of these fields and reads as not releasing, with no quarantine id.

On MongoDB, an instance whose failure record has a quarantine id is stored with the status `QUARANTINED_RECORDING`
instead of `QUARANTINED`. `SagaStatus` doesn't change. The store reads both strings as `QUARANTINED`, and every query
for quarantined instances matches both. The string is there for 0.34.0, which decodes the status with
`SagaStatus.valueOf` and throws on a value it doesn't know.

So a 0.34.0 node in a mixed cluster throws on an input of an instance that 0.35.0 quarantined, instead of skipping the
input without a record. It doesn't store the subscription's position past that input, so no input is lost. On a live
MongoDB subscription the node retries the input with backoff, and the saga's subscription stalls on it, like the stall
in [section 3 of the 0.33.0 upgrade guide](../../migration/upgrading-to-0.33.0.md#3-a-0320-node-during-a-rolling-upgrade).
It ends when the node is upgraded, or when a release or a delete means the instance is no longer quarantined. In a
catch-up that a subscribe started, the replay ends with an error, and the saga's subscription doesn't go live on that
node until the node restarts, which the upgrade does. A catch-up that a resume started retries with backoff.

In every one of these cases the node keeps the saga's competing-consumer lease. Nothing tells
`CompetingConsumerSubscriptionModel` that the subscription it wraps has ended, and the lease strategy refreshes the
lease on a schedule of its own. So no other node runs the saga's subscription until this node shuts down and gives the
lease up, or until the lease expires after a shutdown that doesn't give it up.

The upgrade guide still says to keep quarantine off with `disableQuarantine()` or
`occurrent.saga.quarantine-enabled=false` until every node runs 0.35.0, and to turn it on in a deploy after that. With
quarantine off, no 0.35.0 node quarantines anything, so no 0.34.0 node meets such an instance. An instance a 0.34.0 node
quarantined has no quarantine id, and release refuses it.

After a downgrade, a 0.34.0 node throws on every input of an instance that 0.35.0 quarantined, releasing or not. It
stalls the saga's subscription when the input comes live and stops it when the input comes in a catch-up, as above,
until the node runs 0.35.0 again, or until the instance is deleted and the node restarts. No input is lost.

### 9. Two corrections to ADR 134

- ADR 134's Status section says the quarantine budget is off on "push feeds and the broker sources". `SagaRunner` also
  turns it off for `CatchupThenPushSubscriptionModel` and for the in-memory subscription model, so that list is
  incomplete. Quarantine runs only on a model that retains every event, and point 7 of this record keeps that rule.
- ADR 134 Decision point 3 calls the budget "the earliest a failing instance can be quarantined". The budget runs from
  the first time an event failed, not from its latest failure. So an event whose earlier failure the instance still
  records, from longer ago than the budget, quarantines the instance the first time it fails again. Replaying a parked
  event, turning quarantine on for an event that failed while it was off, and moving a MongoDB subscription back by hand
  to before a failed event all produce such an event.

## Consequences

- A quarantined instance can be brought back without stopping the saga, without a redeploy, and without the other
  instances of the saga waiting for anything.
- A skip by a quarantined instance is now two writes, the record and the compare-and-set on the instance.
- A quarantined instance keeps a record of every input it skips, with no limit. The records take space in the store
  until a release replays them or the instance is deleted.
- An instance whose K or recorded input refers to an event that was deleted from the store stays quarantined until the
  event is restored or the instance is deleted.
- A rolling upgrade from 0.34.0 that follows the upgrade guide takes two deploys, one with quarantine off and one that
  turns it on.
- A 0.34.0 node, in a mixed cluster or after a downgrade, stalls or stops the subscription of a saga with an instance
  that 0.35.0 quarantined, until it runs 0.35.0. It loses no input.
- An application that never configured quarantine starts quarantining on upgrade. The changelog and the upgrade guide
  say so and name `disableQuarantine()` and `occurrent.saga.quarantine-enabled=false`.
- `SagaFailure` gains two components, so a record pattern over it stops compiling. `SagaEnvelope` changes in 0.35.0
  already, and gains one more component.
- A custom `SagaStateStore` has to persist the new fields, keep the records, delete them in `delete(sagaId)`, and
  implement the query for releasing instances. A release reads the instance back after setting `releasedAt` and throws
  when the field didn't persist, rather than replaying into an instance that would forget it is releasing.

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
