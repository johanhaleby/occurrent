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

Every input a quarantined instance skips has its key appended to a list on the instance, in the order the inputs arrive.
The key is the stream id with the stream version when the event has both, which is the redelivery key
`SagaFailure.input` holds. An event without them is keyed by its CloudEvent id and source, which
`Filter.cloudEvent(id, source)` reads back. A key already in the list isn't appended again, and neither is the key of
the input the failure record names, so a redelivered input adds nothing.

The rule is keyed on the instance being quarantined, not on one branch of the executor. Three places skip an input for
a quarantined instance, and all three record it:

- the quarantined skip in `SagaExecutionSupport.process`.
- the skip for an instance that never started, which runs first when the instance was quarantined on its start event.
- `SagaExecution`'s load-failure path, which skips an input when the state no longer decodes and the instance is
  quarantined. It records through `compareAndSaveWithoutState`, so the undecodable state is never written.

A skip used to write nothing. It is now a compare-and-set on the instance, retried on a lost write like any other save.
The watermarks still don't move, which is what ADR 134 Decision point 5 requires.

Events from a DCB store are stored with a stream id and a stream version on MongoDB, so they are recorded by stream key
like any other.

### 2. The list has a limit

`SagaRunnerConfig.withQuarantineRecordLimit(int)` sets how many keys one instance records, 1000 by default, and
`occurrent.saga.quarantine-record-limit` sets it in Spring Boot. The keys are stored in the instance's own document, so
the limit bounds how much a quarantine can grow it, below MongoDB's 16 MB document limit. 1000 is a starting point, not a
measured number.

When the list is full, the instance stops recording, sets an overflow flag in the same write, and keeps skipping.

### 3. Release replays the input the instance stopped on, then the list

`SagaSubscription.release(String sagaId)` releases one instance. K is the input the instance was quarantined on.

1. Release sets `releasedAt` on the failure record in one compare-and-set. The same write restarts K's quarantine
   budget at the release instant, on the failure record and on K's pending entry, so a release that fails again gets a
   full budget before it is quarantined again. An instance with `releasedAt` set is releasing.
2. Release reads K from the event store by its key and applies it through the executor's normal processing, with the
   quarantined skip bypassed for that one input. Then it does the same for each recorded key, in recorded order. Each
   applied input is removed from the list in the write that applies it.
3. The write that applies the last entry also sets the instance `ACTIVE` and clears the failure record.

An input that arrives live while the instance is releasing reaches the quarantined skip and is appended to the list. That
write changes the instance's version, so release loses its next compare-and-set, reads the instance again, and finds the
new entry. The instance becomes `ACTIVE` only in a write that empties the list, so no input is skipped after that.

An event that commits late below a position another read reached isn't a problem here. Nothing reads a range of
positions. The late event is delivered live, is skipped, and is appended like any other.

Release reads the store through an `EventStoreQueries` that `SagaRunner` is given. The Spring Boot starter passes its
own bean. A `SagaRunner` built without one still records skipped inputs, and `release` throws, naming the missing
`EventStoreQueries`.

Release returns what it did. The instance is released and `ACTIVE`, or it is still releasing because an input failed
again, or it isn't quarantined, or no instance has that id. The refusals in point 4 throw instead.

### 4. What release refuses

Release throws `IllegalStateException` for an instance whose list overflowed, and for an instance quarantined
before 0.35.0, which has no list. The exception message names the two ways out, a restart of the saga's
subscription from `SagaFailure.position`, or `SagaStateStore.delete(sagaId)`.

### 5. Release runs on any node, and a crashed release is resumed

Release takes no lease and pauses nothing. Every step is a compare-and-set on one document, so it can run on any node,
including a node that doesn't hold the saga's subscription lease. Two nodes releasing the same instance at once compete
for each entry. Each step applies the first entry of the list it has just read, so the node that loses reads the
instance again and finds the entry gone from the list. That also holds for a keyless entry, which the stream marks
can't recognise as applied.

When a recorded input fails again while releasing, the failure is recorded the way any failure is, and the instance
stays releasing. The timer poller also asks for releasing instances on every poll and continues their replay. With a
competing-consumer strategy only the node holding the saga's timer lease polls, which is the Spring Boot starter's
setup, and without one every node does. `SagaStateStore` gains a query for them, a `default` method that throws
`UnsupportedOperationException`, so a custom store keeps compiling. The poller logs that once at `WARN` and stops asking,
and on such a store a failed or interrupted release goes on only when `release` is called again. The poller is what
retries the failed input until it succeeds or its budget runs out, and it also resumes a release that a crash
interrupted. Without it, live inputs would keep filling the list of an interrupted
release until it overflowed.

When the budget runs out, the instance is quarantined again. The input that failed becomes the failure record's input
and is removed from the list in that write, `releasedAt` is cleared, and the entries after it stay in the list in
order. A later release replays the failed input first and then those entries.

An input whose event no longer exists in the store is logged at `ERROR`, dropped from the list, and the replay goes on.

### 6. Timers stay held until the instance is ACTIVE

The status stays `QUARANTINED` while an instance is releasing, and `findWithDueTimers` returns `ACTIVE` instances only.
A timer that came due while the instance was quarantined or releasing fires once it is `ACTIVE`.

### 7. Quarantine is on by default again

`SagaRunnerConfig.defaults()` quarantines after 5 minutes, and so does the Spring Boot starter when
`occurrent.saga.quarantine-after` is left out. ADR 134 turned it off only because nothing brought a quarantined instance
back. Five minutes is the budget ADR 134 Decision point 3 argues for.

It still applies only to subscription models that retain every event they deliver. `SagaRunner` turns quarantine off on
any other model, as it does today.

`disableQuarantine()` turns it off. In Spring Boot, `occurrent.saga.quarantine-enabled=false` does, since a blank or
missing `occurrent.saga.quarantine-after` now means the default.

### 8. Persisted format

- `SagaFailure` gains `releasedAt`, nullable. Its five-argument constructor stays. On MongoDB it is the top-level
  `failureReleasedAt` field, next to the other flattened `failure*` fields.
- `SagaEnvelope` gains the list of skipped keys and the overflow flag. They are delivery bookkeeping like the stream
  marks, not lifecycle, so `SagaFailure` and `SagaInstance` don't hold them. On MongoDB they are the top-level
  `quarantineSkipped` and `quarantineSkippedOverflow` fields.
- A document written before 0.35.0 has neither field and reads as not releasing, with no list.

A 0.34.0 node decodes only the fields it knows. It reads a releasing instance as quarantined, skips its inputs and writes
nothing. That is also the gap in a mixed cluster. A 0.34.0 node skips an input of a quarantined instance without
recording it, and a release can't tell later that the input is missing from the list. So the upgrade guide says to keep
quarantine off with `disableQuarantine()` or `occurrent.saga.quarantine-enabled=false` until every node runs 0.35.0,
and to turn it on in a deploy after that. With quarantine off, no 0.35.0 node quarantines anything while a 0.34.0 node
might skip for it. An instance a 0.34.0 node quarantined has no list, and release refuses it.

After a downgrade, a releasing instance stays quarantined, and a 0.34.0 node doesn't continue its replay. Release
continues it after an upgrade again.

### 9. Two corrections to ADR 134

- ADR 134's Status section says the quarantine budget is off on "push feeds and the broker sources". `SagaRunner` also
  turns it off for `CatchupThenPushSubscriptionModel` and for the in-memory subscription model, so that list is
  incomplete. Quarantine runs only on a model that retains every event, and point 7 of this record keeps that rule.
- ADR 134 Decision point 3 calls the budget "the earliest a failing instance can be quarantined". A failure record
  whose input was later skipped, which only repositioning a MongoDB subscription by hand produces, quarantines on the
  next failure, sooner than the budget.

## Consequences

- A quarantined instance can be brought back without stopping the saga, without a redeploy, and without the other
  instances of the saga waiting for anything.
- A skip by a quarantined instance is now a write. An instance that receives many inputs while quarantined writes once
  for each until the list is full.
- An instance that receives more than the record limit while quarantined can't be released, and the operator is back to
  a subscription restart or a delete. A longer quarantine needs a higher limit.
- A rolling upgrade from 0.34.0 that follows the upgrade guide takes two deploys, one with quarantine off and one that
  turns it on.
- An application that never configured quarantine starts quarantining on upgrade. The changelog and the upgrade guide
  say so and name `disableQuarantine()` and `occurrent.saga.quarantine-enabled=false`.
- `SagaFailure` gains a component, so a record pattern over it stops compiling. `SagaEnvelope` changes in 0.35.0
  already, and gains two more components.
- A custom `SagaStateStore` has to persist the new fields and implement the query for releasing instances. A release
  reads the instance back after setting `releasedAt` and throws when the field didn't persist, rather than replaying
  into an instance that would forget it is releasing.

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
- **A new `SagaStatus.RELEASING`.** 0.34.0 decodes the status with `SagaStatus.valueOf`, so a 0.34.0 node would throw on
  every delivery for a releasing instance during a rolling upgrade, and every exhaustive `switch` over `SagaStatus`
  would stop compiling again.
