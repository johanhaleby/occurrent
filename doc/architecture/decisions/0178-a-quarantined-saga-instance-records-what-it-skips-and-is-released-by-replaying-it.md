# 178. A quarantined saga instance records what it skips and is released by replaying it

Date: 2026-11-14

## Status

Accepted at the design gate for [#1259](https://github.com/johanhaleby/occurrent/issues/1259). Amends
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md) by reference, since ADR 134
shipped in 0.34.0. It replaces how Decision point 7 releases an instance and turns quarantine on by default again for
every `SagaRunner`. It also corrects three sentences in ADR 134, two of them ones that
[#1088](https://github.com/johanhaleby/occurrent/issues/1088) found. ADR 134 gets a pointer in its Status section.

It refers to three things that later changes in 0.35.0 add. They are
[ADR 180](0180-occurrent-owns-the-stored-form-of-a-saga-instance.md), which moves the stored form of an instance into
Occurrent, the `ACTIVE_MARKED` status in
[ADR 154](0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md), and the upgrade guide's section
on upgrading from 0.34.0 and rolling back to it.

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

### 1. A quarantined instance records the key of every event it skips

Every event a quarantined instance skips has its key recorded, in the order the events arrive. A timeout records
nothing, because its timer stays on the instance, see point 6.
The key is the stream id with the stream version when the event has both, which is the redelivery key
`SagaFailure.input` holds for an input that failed live. An event without them is keyed by its CloudEvent id and source,
which `Filter.cloudEvent(id, source)` reads back. After a release finds a recorded input's event missing,
`SagaFailure.input` holds the key that input was recorded under, so it can be a CloudEvent key, see point 5. A key
already recorded isn't recorded again, and neither is the key of the input the failure record names, so a redelivered
input adds no record. An instance a 0.34.0 node quarantined or last wrote has no quarantine id, see point 2, so its
skips record nothing, as in 0.34.0.

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

The methods that read and write the records are on an interface of their own, `SagaStateStoreSkippedInputs`, which a
store implements next to `SagaStateStore`, the way it implements `SagaStateStoreQueries`. It has no state type
parameter, since none of its methods reads the state, and none of them has a default. The in-memory store and
`SpringMongoSagaStateStore` implement it. Point 7 says what the runner does with a store that doesn't.

The write that quarantines an `ACTIVE` instance gives its failure record a new quarantine id and sets the skip counter
to 0. The quarantine id and the skip counter have to be written with the status that says the instance is
quarantined. Once [ADR 180](0180-occurrent-owns-the-stored-form-of-a-saga-instance.md) has Occurrent encode every
field of the instance itself, and a store write every field the encoding holds, they are, and the saga doesn't read the
instance back to check them.

The id names the whole quarantine until the instance is `ACTIVE` again, so a release that quarantines the instance
again keeps the id, and keeps the counter for the records that stay. Records are stored under the saga id and the
quarantine id, and a release reads only those under the current id. A record left over from an earlier quarantine of the
same instance is never replayed.

A skip reads the instance's skip counter and raises it by one in a compare-and-set on the instance. The value it read is
the record's sequence number. Then it writes the record with that number, and then it changes the instance's version
again in a second compare-and-set. Each number the skip counter gives out is taken once, so no two skipped inputs of a
quarantine share one. Release reads the records it finds in order of sequence number, which is the order those inputs
took their numbers in. A skip that took its number and hasn't written its record when the release reads the records
isn't replayed. Its second compare-and-set loses because the release changed the instance. It reads the instance again,
and when the instance is no longer quarantined under that quarantine id, it deletes its own record, by its sequence
number, and applies its input live to the `ACTIVE` instance. That puts it after the inputs the release
replayed, even when it took a lower number than some of them. It needs two inputs of one instance skipped at the same
time, which one subscription doesn't do, since it delivers one event at a time. Two subscriptions that deliver into one
state store can, and live delivery applies two inputs in flight at once in whichever order their compare-and-sets
succeed too. The one number written again is -1, which every release writes for its head record, see point 3 step 1.

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
under the same key in the meantime. The one write that moves a record is `recordSkippedInputAtHead`, which release
makes when it starts, to make the input it starts from the first record it replays, in point 3 step 1.

`SagaStateStore.delete(sagaId)` also deletes the records under the quarantine id the instance had. It deletes the
instance first and those records after it. When a delete fails between the two, what remains is records that no
instance reads, never a quarantined instance without the records of what it skipped. It deletes by that quarantine id
and not by saga id, because an event can start a new instance with the same saga id between the two deletes. Once that
instance is quarantined, it records under a quarantine id of its own, and a delete by saga id would remove records of
inputs that were acknowledged. When a replayed input completes the instance, release deletes the records under its
quarantine id with `deleteSkippedInputs(sagaId, quarantineId)`, since a completed instance skips every input and none of
them can be replayed. After a crash between the write that completes the instance and that delete, the records stay,
and nothing reads or deletes them, since a completed instance has no quarantine id. Records also stay where nothing
reads them in five other cases. The first is a skip's record after a crash between its write and the delete that
follows a lost second compare-and-set. The second is a skip's record when the skip read the instance as still
quarantined just before a release turned it active, see point 3. The third is every record of an instance whose delete
`SpringMongoSagaStateStore` retried after the first attempt had removed the instance, since the retry finds no instance
to read the quarantine id from. The fourth is K's record when a release that lost the start compare-and-set wrote it
after the winning release had replayed K, deleted its record and turned the instance active. The fifth is K's record
when `SagaStateStore.delete(sagaId)` removed the instance and its records after a release read the instance and before
the release wrote K's record, since the release's compare-and-set then loses and it finds no instance. Such records
take up storage and nothing else.

`SpringMongoSagaStateStore` reads the instance and its records from the primary, whatever read preference the
`MongoOperations` it is given has. A release turns the instance `ACTIVE` once its read of the records finds none, and
the compare-and-set of that write checks only the version of the instance's document. A read from a secondary that lags can find no
record while one is written, so the release would turn the instance `ACTIVE` with an input that was acknowledged when
it was skipped and is never applied.

Every write and delete of a record uses `WriteConcern.MAJORITY` with `j: true`, whatever write concern the
`MongoOperations` or its collection has. A skip acknowledges its input once the record is written, so a record
written with `w: 1` that a failover rolled back would leave an input that no release replays. With `j: true` a majority
has the record in its on-disk journal before it acknowledges, whatever `writeConcernMajorityJournalDefault` says. A server that runs without journaling
refuses `j: true`, and the write then fails after one attempt, whatever the store's `RetryStrategy` retries, with an
`IllegalStateException` that says the store needs journaling. The
writes to the instance's own document keep the caller's write concern.

### 3. Release replays the input the instance stopped on, then the records

`SagaSubscription.release(String sagaId)` releases one instance. K is the input the instance was quarantined on.

1. Release records K first, as a record with the sequence number -1, below every other record's. It does that with
   `recordSkippedInputAtHead`, which in one write per key sets the number of a record of K's key left from an earlier
   attempt to -1, whatever it was, or writes the record when there is none. So K's record moves to the head, the first
   record a release replays, and a release that reads the records in between never finds K without one. Then release sets `releasedAt` on the failure record in one
   compare-and-set. The same write restarts K's quarantine budget at the release instant, on the failure record, and
   on K's pending entry when K is a stream key with one, so K gets a full budget again before it can quarantine the
   instance again. An instance with `releasedAt` set is releasing.
2. Release replays the records in order of sequence number, K's first. For each one it reads the event from the event
   store by the record's key and applies it through the executor's normal processing, with the quarantined skip
   bypassed for that one input, and deletes the record after the write that applies its input. That write keeps the
   instance `QUARANTINED`, with its failure record and `releasedAt`, while a live write that applies the input the
   failure record names clears the record and writes `ACTIVE`. So while the instance is releasing, its failure record
   says nothing about whether the input it names was applied, and the stream marks alone decide whether a record's
   input was applied. K is applied once, as its record. A record whose input the stream marks show as applied already,
   after a crash between that write and the delete or behind another node's release, is deleted without being applied
   again, also when it is K's or the failure record names its input. Release reads and writes the stream marks with no
   reorder window, so every version it applies stays among the `applied` ones until the release ends, also one at or
   below the horizon of its type. With a window of 1, a record of version 3 replayed after a record of version 5 of the
   same type would read as behind the window and be skipped without being applied. A record of an input with a stream
   id and version is written only while its input is new, so a record whose input the marks read this way list as
   applied names an input the instance applied.
   Every write that ends the release trims the marks back to the window: the one that makes the instance `ACTIVE`, the
   write of a replayed input that completes the instance, and the write that quarantines it again. The instance is
   quarantined again only on the first record left, and release applies the records in order, so every record that
   stays names an input no release applied. When the trim puts such a record behind the window, the next release
   applies it all the same, since release skips only an input the marks list as applied. An input without a stream id
   and version, which only `RedeliveryDetection.BEST_EFFORT` takes (ADR 109), is recorded without being checked against
   the marks, so when the instance applied the same event before the quarantine, the release applies it again. That
   is what `BEST_EFFORT` already does with any redelivery.
3. The instance becomes `ACTIVE`, with the failure record and the skip counter cleared, only in a compare-and-set that
   expects the version release read before it found no records left. Release reads the instance first and the records
   after it, so a skip that finishes between the two reads has changed the version, and the write loses. A skip that
   raised the skip counter before release read the instance, and writes its record after release found none, loses its
   second compare-and-set, to this write or to another skip's. It then reads the instance again. When the instance is no
   longer quarantined under that quarantine id, the skip deletes its own record again, by its sequence number, and its
   input, which was never acknowledged, is applied to the `ACTIVE` instance like any live input. When the instance is
   still quarantined, the record stays and the skip starts again from its first compare-and-set. That takes a new
   number, which goes unused, since the record write keeps the record already there. If this write turns the instance
   `ACTIVE` before the skip starts again, the input is applied like any live input, and its record stays where nothing
   reads it.

Release applies the event it read back by that event's stream id and version, with the same budget and stream marks as
any other input. A failure while releasing replaces the failure record differently from a live one, see point 5. A
record's CloudEvent key is used only to find the event. Every event the MongoDB event
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
dispatch a record's reaction. After a lost write, release reads the instance again. Only skips happened when the
first record is still the one it replayed and the instance read again matches the one it read before in the state,
the status, the timers, the stream watermarks and marks, `createdAt`, `updatedAt`, `completedAt`, whether it started
and the whole failure record. Release then writes the result it computed onto the instance it read again, with that
instance's skip counter and version, and doesn't dispatch the reaction again. It tries that write up to
`maxCasAttempts` times. When anything else changed, or those attempts all lose, it reads the instance and the records
again and computes again, which dispatches the reaction again. After `maxCasAttempts` rounds in a row that end in a
lost write, release throws a `SagaConcurrencyException`, as live processing does. The state is compared with
`equals`, so for a state type that doesn't override it, release always computes again and dispatches the reaction
again.

An event that commits late below a position another read reached isn't a problem here. Nothing reads a range of
positions. The late event is delivered live, is skipped, and is recorded like any other.

Release reads the store through an `EventStoreQueries` that `SagaRunner` is given. The Spring Boot starter passes its
own bean. A `SagaRunner` built without one refuses to start while quarantine is on, as point 7 says. When such a runner
starts because quarantine is off, `release` throws and names `SagaRunner.eventStoreQueries(..)`.

Release returns a `ReleaseResult` that says what it did:

- `RELEASED`: every record was replayed and the instance is `ACTIVE`, or `COMPLETED` when a replayed input completed
  it.
- `STILL_RELEASING`: an input failed again inside its budget, or with quarantine off, and the instance stays
  releasing.
- `QUARANTINED_AGAIN`: an input's budget ran out, or the event of K or of a recorded input was read back without a
  stream id and version.
- `QUARANTINED_EVENT_MISSING`: the event of K or of a recorded input is missing from the store.
- `NOT_QUARANTINED`: the instance isn't quarantined.
- `NOT_FOUND`: no instance has that id.

A `release` on an instance that is releasing already goes on with its replay and doesn't restart K's budget. The
refusals in point 4 throw instead.

### 4. What release refuses

Release throws `IllegalStateException` for a quarantined instance that a 0.34.0 node quarantined or last wrote. Its
failure record has no quarantine id, so it has no records to replay. The exception message names the way out, `SagaStateStore.delete(sagaId)`. A restart
of the saga's subscription isn't one, since a quarantined instance skips every event a restart offers it.

Release also throws `IllegalStateException` for an instance whose state no longer decodes, and the message names the
decoding failure. To go on, fix how the state decodes and release the instance again, or delete the instance. An
instance whose state stops decoding while it is releasing stays releasing. Each poll logs the failure at `WARN` and
tries again, so the release goes on once the state decodes.

### 5. Release runs on any node, and a crashed release is resumed

Release takes no lease and pauses nothing. Every step that changes the instance is a compare-and-set on its document,
so release can run on any node, including a node that doesn't hold the saga's subscription lease. Two nodes releasing
the same instance at once compete for each record. Each step applies the first record it has just read, so the node that
loses reads the instance again, finds that the stream marks show the record's input as applied, and deletes the record
without applying it. The failure record plays no part in that, as point 3 step 2 says.

When a recorded input fails again while releasing, its failure replaces the failure record, its version is pending on
the stream marks, and the instance stays releasing. A live failure that replaces a failure record keeps the input the
replaced record named on the stream marks as one that failed, so a redelivery of it is applied. A failure while
releasing keeps nothing of the replaced record on the marks, since the release has applied that input by then, and
keeping it would make an applied input count as not applied and apply it twice.

The timer poller also asks for releasing instances on every poll and continues their replay. With a
competing-consumer strategy only the node holding the saga's timer lease polls, which is the Spring Boot starter's
setup, and without one every node does. The query is `findReleasingSagaIds` on `SagaStateStoreSkippedInputs`, and the
last paragraph of this point says how a poll pages through it. The poller is what
retries the failed input until it succeeds or its budget runs out, and it also resumes a release that a crash
interrupted.

The poller goes on with an instance only when its `updatedAt` is at least `timerPollInterval` old. A release writes
the instance for every input it applies and the first time an input fails, and a skip doesn't change `updatedAt`, so an
instance whose release is still running was written within that time, and the poller skips it, whichever node runs
the release. Without that, a release that runs longer than one poll interval has the poller replay the same
records beside it, and both dispatch each record's commands. The poller starts each poll a `timerPollInterval` after
the last one ended, so a release it went on with itself is old enough by the next poll.

In five cases a release and the poller, or two releases, can still replay the same record and both dispatch its
commands, which delivery at least once allows:

- A release that takes longer than `timerPollInterval` over one input.
- A poller whose clock is `timerPollInterval` or more ahead of the clock of the node that wrote.
- A `release` call on an instance that the poller or another call is replaying. The instance is releasing already, so
  the call goes on with the replay without the check, beside the other one.
- A release working through records whose inputs the stream marks show as applied. It deletes them without writing the
  instance, so after `timerPollInterval` of that the poller goes on beside it. Neither dispatches anything for those
  records, so only the first record that isn't applied can have its commands dispatched by both, the same at-least-once
  dispatch a live input has.
- Two polls that read the instance at the same time, once its last write is `timerPollInterval` old. Without a
  competing-consumer strategy every node polls, and with one, two nodes can poll at once while the timer lease moves
  from one to the other. A retry that fails again on the same input within its budget writes nothing, so while that
  input keeps failing, every node that polls retries it.

A claim stored on the instance would rule all five out, at the cost of a write to take and give back for every release
and a way to take over the claim of a node that crashed.

The poller polls only while the `timersEnabled` that `SagaRunner.run` takes returns `true`, so that gate holds back the
continuation of a release as well as the saga's timers. On a subscription model that reports whether it runs, the
Spring Boot starter keeps the gate closed while the saga's subscription isn't running, and on one that also reports a
replay, while it is still catching up. The gate exists for timers. A timer fires on the clock, so while the
subscription isn't running or is still catching up, it can decide against state that lacks inputs the subscription
hasn't delivered yet. A release applies only inputs the subscription has delivered and the instance recorded, each on
the state the inputs before it produced. An input the subscription still holds back comes after all of them, where
live delivery would also put it. So a call to `release` doesn't wait for the gate and dispatches on whichever node it's
called on. Continuing a release waits for the gate only because the poller runs it, and the first poll after the gate
opens goes on with it.

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
bound and replays on the calling thread until it reaches one of the outcomes point 3 lists, so it finishes such an
instance. A live input skipped while it runs only makes it write its result again on top of the skip, without
dispatching it again, as point 3 says.

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
budget nor the write for an event that can't be replayed deletes it. The next release starts by setting the number of
the record of the input the failure record names to -1, in one write. So that record moves to the head whatever its
number was.

The record that stays does no harm:

- When its key is the input the failure record names, it is the record the next release moves to the head and replays
  first. That is always so for an event that is missing or has no stream id and version, and for a used-up budget when
  the input was recorded by its stream key.
- When it was recorded under a CloudEvent key, a used-up budget names the input by its stream key instead. The next
  release replays the input under its stream key first and reaches the record that stayed after it. The stream marks
  show that input as applied by then, so the release deletes the record without applying it again, whatever the
  failure record says, see point 3 step 2. When the record that stayed is K's own at -1, two records share -1. Both
  name the same input, so whichever is replayed first applies it, and the stream marks show it as applied when the
  other is reached, so that one is deleted.

When the read itself fails, the release throws and the instance stays releasing. On a store that answers the poller's
query, the poller tries again on its next poll.

### 6. Timers stay held until the instance is ACTIVE

The status stays `QUARANTINED` while an instance is releasing, and `findWithDueTimers` returns `ACTIVE` instances only.
A timer that came due while the instance was quarantined or releasing fires once it is `ACTIVE`.

So such a timer fires only after every recorded input has been replayed, including inputs that arrived after its
deadline, because a saga's timers run on processing time. For a saga that uses a timer as a deadline, a recorded input
from after the deadline that cancels or re-arms the timer means the timeout never fires. This record keeps that order.

### 7. Quarantine is on by default again

`SagaRunnerConfig.defaults()` quarantines after 5 minutes, and so does the Spring Boot starter when
`occurrent.saga.quarantine-after` is left out. ADR 134 turned it off only because nothing brought a quarantined
instance back. Five minutes is the budget ADR 134 Decision point 3 argues for.

It still applies only to subscription models that retain every event they deliver. On any other model `SagaRunner`
turns quarantine off and logs that at startup. A budget you set, with `withQuarantineAfter(..)`, a `SagaRunnerConfig`
constructor or `occurrent.saga.quarantine-after`, gets a `WARN`, as in 0.34.0. The default budget gets a `DEBUG`, so an
application that never asked for quarantine doesn't get a new warning each time the saga starts.
`SagaRunnerConfig` records which of the two it holds in `quarantineAfterDefaulted`.

The default applies to a `SagaRunner.run` given no config or `SagaRunnerConfig.defaults()`, and to every `@Saga` in
Spring Boot. The 3- and 4-argument `SagaRunnerConfig` constructors keep quarantine off. On a model that retains every
event, `SagaRunner.run` throws an
`IllegalStateException` when quarantine is on and the runner has no `EventStoreQueries`, whether quarantine comes from
the defaults or was configured explicitly. The message names `SagaRunner.eventStoreQueries(..)` and
`SagaRunnerConfig.disableQuarantine()` as the two ways out. A runner that quarantines without an `EventStoreQueries`
can't release anything, so `SagaStateStore.delete(sagaId)` would be the only way out, which is the situation ADR 134
turned quarantine off for.

For the same reason `SagaRunner.run` throws an `IllegalStateException` when quarantine is on and the saga's state store
doesn't implement `SagaStateStoreSkippedInputs`. The message names the interface and `disableQuarantine()`. Such a
store would quarantine an instance and skip its inputs without recording any of them.

That is a startup failure for a `SagaRunner` built in plain Java without an `EventStoreQueries` that runs on the
defaults, from `SagaRunnerConfig.defaults()` or from the `run` overloads that take no config, which pass
`SagaRunnerConfig.defaults()` themselves. It started in 0.34.0, since the defaults kept quarantine off then.

The Spring Boot starter gives `SagaRunner` the application's `EventStoreQueries` bean when there is one, or several
with one of them `@Primary`. With quarantine on and a model that retains every event, a `@Saga` fails at startup when
there is no `EventStoreQueries` bean, or several and none of them `@Primary`. The message names the saga id and
`occurrent.saga.quarantine-enabled=false`.

`disableQuarantine()` turns it off. In Spring Boot, `occurrent.saga.quarantine-enabled=false` does. A blank
`occurrent.saga.quarantine-after` kept quarantine off in 0.34.0, so the starter refuses to start on one, and the
message names `occurrent.saga.quarantine-enabled=false`. An application that turned quarantine off that way finds out
on its first deploy, instead of quarantining without knowing it.

### 8. Persisted format

- `SagaFailure` gains `releasedAt` and a quarantine id, both nullable. Its five-argument constructor goes, see ADR 180. On MongoDB
  they are the top-level `failureReleasedAt` and `failureQuarantineId` fields, next to the other flattened `failure*`
  fields. Starting a release rewrites the record's `firstFailedAt`, and K's pending entry when K has one, to the
  release instant, which is how K's budget restarts.
- `SagaEnvelope` gains the skip counter. It is delivery bookkeeping like the stream marks, not lifecycle, so
  `SagaFailure` and `SagaInstance` don't hold it. On MongoDB it is the top-level `quarantineSkipCount` field of the
  instance's document.
- A key is stored as a string, in a record document and in the failure record's input. A stream key is
  `<streamId>@<streamVersion>`, as in 0.34.0, and a CloudEvent key is
  `cloudevent:<length of id>:<id>:<source>@cloudevent`. The length lets an id that contains a colon be read back.
- On MongoDB the records are documents in a collection of their own, named after the saga collection with
  `_skipped_inputs` appended. A unique index on the
  saga id, the quarantine id and the key keeps a key from being recorded twice, and an index on the saga id, the
  quarantine id and the sequence number serves the order release reads them in.
- A document a 0.34.0 node wrote has none of these fields and reads as not releasing, with no quarantine id.

On MongoDB, an instance whose failure record has a quarantine id is stored with the status `QUARANTINED_RECORDING`
instead of `QUARANTINED`. `SagaStatus` doesn't change. The 0.35.0 store reads both strings as `QUARANTINED`, and its
queries for quarantined instances match both. The string is there for 0.34.0, which decodes the status with
`SagaStatus.valueOf` and throws on a value it doesn't know.

Nothing outside the 0.35.0 store matches the new string. A query written by hand for `status: "QUARANTINED"`, like the
one the runbook gives for 0.34.0, doesn't find an instance that 0.35.0 quarantined, and neither does an alert built on
such a query or `findByStatus` in the 0.34.0 store. A query by hand has to match both strings, with `$in`.

So a 0.34.0 node throws on every event addressed to an instance that 0.35.0 quarantined, instead of skipping the input
without a record. That makes a deploy that runs a 0.34.0 node next to a 0.35.0 node fail loudly, but it doesn't make
that deploy safe. When the instance gets its events from a broker source that parks an event a 0.34.0 node fails on,
the source acknowledges the event, and a push feed whose caller doesn't push the event again loses it. No node records
such an input, so a release never replays it, and nothing a 0.35.0 node does can stop that. Every 0.34.0 node that
runs sagas therefore stops before any 0.35.0 node starts, which takes the service that hosts the sagas down for the
deploy, as the upgrade guide says.

On a subscription model that writes its position or checkpoint only after the action returns, which every model a
saga can quarantine on does, the event the 0.34.0 node fails on, and everything after it, is delivered again. The next
paragraphs say how a 0.34.0 node that runs anyway stalls on it.

On a live MongoDB subscription the 0.34.0 node retries the event in place with the subscription model's retry
strategy, which by default backs off from 100 ms to 2 s and never gives up. In a catch-up replay that a subscribe
started, the replay fails and the subscription doesn't go live on that node until the node restarts, which its upgrade
does. A replay that a resume started runs again from its last stored checkpoint with the same backoff.

The 0.34.0 node keeps the saga's competing-consumer lease while it retries, because the lease is refreshed on a
schedule of its own whatever the subscription is doing, so the saga's subscription stalls. The node gives the lease up
when it is stopped for its upgrade, but the next node to take it may still run 0.34.0 and stall on the same event, so
the stall ends once a 0.35.0 node holds the lease. It also ends when a 0.35.0 node deletes the instance, because the
0.34.0 node reads the instance again on every retry and a deleted one is not there to decode. A release doesn't end it,
because a released instance keeps the stream marks of the events it has handled, so it is stored as `ACTIVE_MARKED`
(ADR 154), which 0.34.0 can't decode either. A catch-up replay that a subscribe
started is not retried, so a node whose replay failed doesn't go live until it restarts, whatever happens to the
instance.

Without a competing-consumer strategy every node runs the saga's subscription. The 0.35.0 nodes handle the event, and
only the 0.34.0 node's own subscription stalls, until the node is upgraded or the instance is deleted.

The subscription delivers one event at a time, so the stall holds up every later event of that saga subscription, for
every instance of the saga, not only the events of the quarantined instance.

After a rollback to 0.34.0 there is no 0.35.0 node left to take the lease, so the stall lasts until the instance is
stored with a status 0.34.0 reads, or deleted, with competing consumers or without. The 0.34.0 node reads the instance
again on every retry, so the stall on an `ACTIVE_MARKED` instance ends once it is stored as `ACTIVE`, since no 0.35.0
node is left to store it as `ACTIVE_MARKED` again. A node whose catch-up stopped on the event also has to restart
before its subscription goes live. A downgrade that doesn't stall anything goes like this, with steps 1 to 5 on 0.35.0
nodes:

1. Turn quarantine off on every node, and have that running on every node before step 2. A node that still has
   quarantine on can quarantine another instance after the instances are listed.
2. List the quarantined instances and call `release` on each whose failure record has a quarantine id. On MongoDB,
   `find({ status: "QUARANTINED_RECORDING" }, { _id: 1 })` on the saga's collection lists exactly those, and its cursor
   reaches every one. On another store, `findByStatus(QUARANTINED, ..)` returns at most `limit` instances and has no
   cursor, so you know a call listed them all only when it returns fewer than its `limit`. Raise the limit until it does.
3. When a release returns `STILL_RELEASING`, an input failed again. With quarantine off there is no budget to run out,
   so the instance stays releasing and the poller retries the input until it succeeds. Fix what makes the input fail
   and call `release` again. Don't delete the instance while a 0.35.0 node runs. `SagaStateStore.delete(sagaId)`
   takes no expected version, so a delete that reaches the store after the poller has finished the release deletes
   the released instance.
4. When a release throws, the instance stays quarantined. Fix the cause the exception names, such as a state that no
   longer decodes or an event store read that failed, and release the instance again.
5. Repeat steps 2 to 4 until every instance step 2 lists has a release that returned `QUARANTINED_AGAIN` or
   `QUARANTINED_EVENT_MISSING`, or that keeps failing for a cause you can't fix.
6. Stop every 0.35.0 node. Then, from a process that runs no `SagaRunner`, store every `QUARANTINED_RECORDING`
   instance as `QUARANTINED` without its quarantine id, skip count and release time, and every `ACTIVE_MARKED` instance,
   released ones included, as `ACTIVE`, with the update ADR 154 describes. On MongoDB, on each saga collection:

   ```
   db.getCollection("<sagaCollection>").updateMany(
     { status: "QUARANTINED_RECORDING" },
     { $set: { status: "QUARANTINED" }, $unset: { failureQuarantineId: "", quarantineSkipCount: "", failureReleasedAt: "" } })
   db.getCollection("<sagaCollection>").updateMany({ status: "ACTIVE_MARKED" }, { $set: { status: "ACTIVE" } })
   ```

   With no `SagaRunner` running, nothing releases an instance or delivers it an input while the update runs.
7. Start the 0.34.0 nodes.

No step deletes an instance. Of what this record adds, only the `QUARANTINED_RECORDING` status is something 0.34.0
can't decode, and step 6 stores every instance that has it as `QUARANTINED`. 0.34.0 decodes the status with
`SagaStatus.valueOf` and reads only the fields it names, so it reads such an instance as one it quarantined itself,
with its state and the input it stopped on. It skips every input addressed to it and fires none of its timers, and only
a delete takes it out of quarantine, as for any instance 0.34.0 quarantined.

Step 6 removes the quarantine id because 0.34.0 skips inputs without recording them. A release after a later upgrade
would replay the inputs recorded before the downgrade and miss the ones skipped after it. Without a quarantine id, a
0.35.0 node reads the instance as one 0.34.0 quarantined, which `release` refuses, as point 4 says. The records of the
inputs it skipped on 0.35.0 stay in the store, where nothing reads them once the quarantine id is gone. They take up
storage and nothing else.

ADR 154 stores an instance with stream marks as `ACTIVE_MARKED` for the same reason this record stores
`QUARANTINED_RECORDING`, and says what setting it back to `ACTIVE` loses.

The upgrade guide has the procedure for operators, in
[Upgrading from 0.34.0 and rolling back to it](../../migration/upgrading-to-0.35.0.md#upgrading-from-0340-and-rolling-back-to-it).

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

- A quarantined instance can be brought back without stopping the saga, and without a redeploy once the application
  has code that calls `SagaSubscription.release`. An application with no such code needs a deploy that adds it. While the poller
  continues a release, the saga's timers wait for at most one batch of records per poll.
- A skip by a quarantined instance is now three writes, two compare-and-sets on the instance and the record between
  them.
- On MongoDB every write and delete of a skipped-input record waits for a majority of the replica set to journal it,
  up to 10 seconds an attempt, or the client's `timeoutMS` when it sets one above 0. That slows a skip, the start of a release and every record a release deletes by the
  time a majority takes, and a replica set without a majority available fails them where `w: 1` would have succeeded.
- A release keeps every stream version it applies in the instance's stream marks until the write that ends it, see
  point 2, so the instance's document grows by one number per replayed input during one release. A release whose
  backlog takes the document past MongoDB's 16 MB limit fails on every write from that record on. The instance stays
  releasing, the failure is never recorded, so its budget never runs out, and every poll applies that record again
  and dispatches its commands again before the write fails. Deleting the instance is the only way out.
- A quarantined instance keeps a record of every input it skips, with no limit. The records take space in the store
  until a release replays them, a replayed input completes the instance, or the 0.35.0 store deletes the instance.
  Records left under an earlier quarantine id, see points 2 and 3, stay in the store, and nothing reads or deletes
  them, since a release, a completion and a delete all go by the instance's current quarantine id. They take up
  storage and nothing else. A 0.34.0 `delete` after a downgrade removes none of the records.
- An instance whose K or recorded input refers to an event that was deleted from the store stays quarantined until the
  event is restored or the instance is deleted.
- Every 0.34.0 node that runs sagas stops before any 0.35.0 node starts, so the service that hosts the sagas is down
  for the deploy.
- A 0.34.0 node that runs next to a 0.35.0 node anyway, or after a downgrade, fails on every event for an instance
  that 0.35.0 quarantined. A broker source that parks such an event acknowledges it, a push feed whose caller doesn't
  push it again loses it, and a release never replays either. On a subscription model that delivers the event again,
  the node stalls the saga's subscription for every later event. In a mixed cluster the stall ends once a 0.35.0 node
  holds the saga's competing-consumer lease, or once a 0.35.0 node deletes the instance. A release doesn't end it,
  since a released instance is stored as `ACTIVE_MARKED` when it holds stream marks (ADR 154). Without competing
  consumers it ends when the 0.34.0 node stops. After a downgrade it ends when the instance is stored as `QUARANTINED`
  again, or as `ACTIVE` for a released instance, or deleted.
  A downgrade that first turns quarantine off on every node and releases those instances, then stops every 0.35.0
  node, stores the ones that stay quarantined as `QUARANTINED` and sets `ACTIVE_MARKED` back to `ACTIVE`, stalls
  nothing and deletes no instance.
- An application that never configured quarantine starts quarantining on upgrade. One that set
  `occurrent.saga.quarantine-after` blank to keep it off fails at startup instead, with a message that names
  `occurrent.saga.quarantine-enabled=false`.
- A `SagaRunner` with quarantine on, on a model that retains every event, and without an `EventStoreQueries` fails at
  startup. That includes a runner built in plain Java on the defaults, from `SagaRunnerConfig.defaults()` or from a
  `run` overload without a config, which started in 0.34.0. A
  `@Saga` in Spring Boot fails at startup the same way when there is no `EventStoreQueries` bean, or several and none
  of them `@Primary`. The changelog and the upgrade guide name `disableQuarantine()` and
  `occurrent.saga.quarantine-enabled=false` as the ways to turn quarantine off.
- `SagaFailure` gains two components, so a record pattern over it stops compiling. `SagaEnvelope` changes in 0.35.0
  already, and gains one more component. `SagaRunnerConfig` gains `quarantineAfterDefaulted` as its 7th component, so
  a record pattern over it stops compiling too.
- A custom `SagaStateStore` with quarantine on has to implement `SagaStateStoreSkippedInputs` and delete the records in
  `delete(sagaId)`, or turn quarantine off. Without the interface the runner refuses to start, see point 7. The new
  fields ask nothing of the store once ADR 180 has Occurrent encode them with the rest of the instance.

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
- **The record methods as `default` methods on `SagaStateStore` that throw `UnsupportedOperationException`**, so a
  custom store keeps compiling. Such a store quarantines an instance and skips its inputs without recording any of
  them, so the instance can't be released, and the saga could only warn about it once it happened. A store of its own
  without the interface is refused at startup instead, before anything is quarantined.
- **Keep the records in a list on the instance's document.** The list grows the document toward MongoDB's 16 MB limit,
  so it needs a limit, and an instance that skips more inputs than the limit can't be released. A release grows the
  stream marks the same way, see the consequences, but by one number per input instead of a whole record, and only
  until the release ends.
- **Log an input whose event is missing and drop its record.** That loses the input. A wrong `EventStoreQueries`, one
  pointed at another database for example, would find no event for any record, drop them all, and set the instance
  `ACTIVE` without any of the inputs it skipped.
- **A new `SagaStatus.RELEASING`.** Every exhaustive `switch` over `SagaStatus` would stop compiling again. A 0.34.0 node
  throwing on a status it doesn't know is what point 8 wants, and the store-only `QUARANTINED_RECORDING` gets that
  without a public enum value.
