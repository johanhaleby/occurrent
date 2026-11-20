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

Amended by [ADR 182](0182-a-saga-instance-and-its-skipped-input-records-change-in-one-atomic-write.md), which has
every write that changes a record change the instance in the same atomic write, at write concern `majority` with
`j: true` on MongoDB. Points 1, 2, 3 and 5 and the consequences below are corrected in place to that rule, and ADR 182
says why the separate writes went.

Amended in place for [#918](https://github.com/johanhaleby/occurrent/issues/918) and
[#952](https://github.com/johanhaleby/occurrent/issues/952), before 0.35.0 shipped. #952 found quarantine off for every
push saga, including one whose catch-up reads the event store. #918 found that a saga on a source that can't hand an
event back can't have both instance isolation and no lost events. A record now holds the event of the input it records,
and the write that quarantines an instance records the input it was quarantined on as the first record. So a release
replays the held events in order and reads nothing from the event store, and every push saga quarantines and releases
like any other saga. The runner refuses quarantine only for a store that doesn't implement
`SagaStateStoreSkippedInputs`, or one that can't write a transition atomically. Points 1 to 5 and 7 to 9, the
consequences and the rejected alternatives are corrected in place. Point 6 points at
[ADR 185](0185-saga-timers-run-on-processing-time.md) for how timers are ordered against held inputs.

The amendment reverses ADR 134's "not moved to a holding area" in its Decision point 1 for a quarantined instance. A
skipped event is now copied into the saga's store. ADR 134 argued against that copy because the event store still had
the first one, and a push source may have no event store behind it, which is the case #918 is about. `HistoryRetainingSubscriptions`, which
0.34.0 shipped for the retention check this amendment removes, is removed in 0.35.0 and nothing replaces it.

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

### 1. A quarantined instance records the events it skips, and holds them

An event a quarantined instance skips is recorded with its key and the event itself, in the order the events
arrive, unless the instance counts it as applied already. That is a redelivery of an input it applied before it was
quarantined, or an event further behind than the reorder window, which an active instance skips too. The write that quarantines an instance also records the input it was quarantined on, K, as the first record of
the quarantine. A timeout records nothing, because its timer stays on the instance, see point 6. A timeout never
quarantines an instance either, since it has no redelivery key to quarantine on.

The key is the stream id with the stream version when the event has both, which is the redelivery key
`SagaFailure.input` holds for an input that failed. An event without them is keyed by its CloudEvent id and source.
Only `RedeliveryDetection.BEST_EFFORT` lets such an event reach an instance (ADR 109). The key tells two deliveries of
one event apart, and a release doesn't use it to find the event, since the record holds the event. A key already
recorded isn't recorded again, so a redelivered input, K included, adds no record. An instance a 0.34.0 node
quarantined or last wrote has no quarantine id, see point 2, so its skips record nothing, as in 0.34.0.

K is recorded in the write that quarantines the instance, `compareAndSaveSkipped`, which inserts the instance at
version 0 when K is its first input. K gets the sequence number 0 and the skip counter starts at 1, so K comes before
every input the instance records after it. K's event is the one the failing delivery has in hand. On a push source that delivery
may be the only time the saga sees it, so the quarantining write is the one place it can be kept. When that write
fails, the delivery throws, K isn't acknowledged, and the instance stays as its last write left it, so a redelivery of
K that quarantines the instance records K then.

The rule is keyed on the instance being quarantined, not on one branch of the executor. Three places skip an input for
a quarantined instance, and all three record it:

- the quarantined skip in `SagaExecutionSupport.process`.
- the skip for an instance that never started, which runs first when the instance was quarantined on its start event.
- `SagaExecution`'s load-failure path, which skips an input when the state no longer decodes and the instance is
  quarantined. It records through `compareAndSaveSkipped`, which changes the skip counter or the status but not the
  state, so the state that no longer decodes is never written back.

A skip used to write nothing. It now records its input and its event in one compare-and-set on the instance that raises
the skip counter and writes the record together, as point 2 describes, and the compare-and-set is retried on a lost write like
any other save. That write changes the instance's version together with the record, which point 3 relies on. A
redelivered input whose key a skip recorded takes a number that goes unused, since its write keeps the record already
there. The watermarks still don't move, which is what ADR 134 Decision point 5 requires.

Events from a DCB store are stored with a stream id and a stream version on MongoDB, so they are recorded by stream key
like any other.

### 2. The records are kept outside the instance's document

Each recorded input is a record of its own in the state store, holding the saga id, a quarantine id, a sequence number,
the key and the event. A quarantine can record any number of inputs without growing the instance's document toward
MongoDB's 16 MB limit, so there is no limit on how many it records.

A store keeps a record's event and returns it from `findSkippedInputs`. The saga keeps no other copy of a skipped
event. `SpringMongoSagaStateStore` writes it as a CloudEvents JSON string in the record's `event` field, so a number
that isn't an `Integer`, such as a `Long`, and a URI, a timestamp or a binary extension come back as a `String`.

The methods that read and write the records are on an interface of their own, `SagaStateStoreSkippedInputs`, which a
store implements next to `SagaStateStore`, the way it implements `SagaStateStoreQueries`. It has no state type
parameter, since none of its methods reads the state, and none of them has a default. The in-memory store and
`SpringMongoSagaStateStore` implement it. Point 7 says what the runner does with a store that doesn't.

The write that quarantines an `ACTIVE` instance gives its failure record a new quarantine id, records K with the
sequence number 0 and sets the skip counter to 1, see point 1. The quarantine id and the skip counter have to be written with the status that says the instance is
quarantined. Once [ADR 180](0180-occurrent-owns-the-stored-form-of-a-saga-instance.md) has Occurrent encode every
field of the instance itself, and a store write every field the encoding holds, they are, and the saga doesn't read the
instance back to check them.

The id names the whole quarantine until the instance is `ACTIVE` again, so a release that quarantines the instance
again keeps the id, and keeps the counter for the records that stay. Records are stored under the saga id and the
quarantine id, and a release reads only those under the current id. A record left over from an earlier quarantine of the
same instance is never replayed.

A skip reads the instance's skip counter, and in one compare-and-set on the instance raises it by one and writes the
record with the value it read as the record's sequence number. Each number the skip counter gives out is taken once, so
no two skipped inputs of a quarantine share one. Release reads the records it finds in order of sequence number, which
is the order those inputs took their numbers in. A skip that loses its compare-and-set has written nothing. It reads
the instance again, records its input when the instance is still quarantined, and applies the input live when the
instance is `ACTIVE`.

After a crash the store holds either the raised counter and the record, or neither, so an input is acknowledged only
once its record is written. When the write doesn't happen, the input was never acknowledged, so it is offered again and
takes a new number. A redelivered input whose key a skip recorded takes a number that goes unused, since its write keeps
the record already there. When two skips of one key race, the first write keeps its record and the other's number goes
unused in the same way.

Release deletes a record it replayed in the write that applies its input, `compareAndSaveReplayed`, which writes
only while the record of that key still has the sequence number release read. So a release never removes another record
written under the same key in the meantime. Two releases that read the same record apply it once, since the second
finds it gone and writes nothing, and then reads the instance again. Without that check an event with no stream id and
version would be applied twice, because nothing on the instance says it was applied. No write moves a record. A release starts in a write to the instance alone,
see point 3 step 1.

`SagaStateStore.delete(sagaId)` deletes the instance and the records under the quarantine id it had in one atomic
write. When a replayed input completes the instance, the write that completes it, `compareAndSaveReplayed`, deletes
every record under its quarantine id, since a completed instance skips every input and none of them can be replayed.
It writes only while the replayed record is still there, as for any replayed input, so a release that replays a record
another release applied already can't complete the instance and delete records no release applied. So in the stores Occurrent ships, the records of a quarantine exist only while the instance is quarantined
under its quarantine id, until the downgrade in point 8 takes the quarantine id off the instances that stay
quarantined, and nothing reads their records after that.

`SpringMongoSagaStateStore` reads the instance and its records from the primary at read concern `majority`, whatever
read preference the `MongoOperations` it is given has. A read from a secondary that lags can find no record while one
is written, and a release turns the instance `ACTIVE` once its read of the records finds none. The write that does that
also checks inside its transaction that no record is under the quarantine id, as ADR 182 point 3 says, so a release
whose read missed a record writes nothing and replays the record on its next attempt.

Every write that changes a record, and every other compare-and-set on the instance, uses `WriteConcern.MAJORITY` with
`j: true`, whatever write concern the `MongoOperations` or its collection has, as ADR 182 point 3 says. A skip
acknowledges its input once the record is written, so a record written with `w: 1` that a failover rolled back would
leave an input that no release replays. With `j: true` a majority has the record in its on-disk journal before it
acknowledges, whatever `writeConcernMajorityJournalDefault` says. A server that runs without journaling refuses
`j: true`, and the write then fails after one attempt, whatever the store's `RetryStrategy` retries, with an
`IllegalStateException` that says the store needs journaling.

### 3. Release replays the held events in order, the input the instance stopped on first

`SagaSubscription.release(String sagaId)` releases one instance. K is the input the instance was quarantined on, the
one its failure record names.

1. Release sets `releasedAt` on the failure record in one compare-and-set on the instance,
   `compareAndSaveWithoutState`, and writes no record. K's record is the first of the quarantine from the write that
   quarantined the instance, and an instance quarantined again during a release stops on its first record left, see
   point 5. So the input the failure record names always has the first record. The same write restarts K's quarantine
   budget at the release instant, on the failure record, and on K's pending entry when it has one, so K gets a full
   budget again before it can quarantine the instance again. An instance with `releasedAt` set is releasing.
2. Release replays the records in order of sequence number, K's first. For each one it applies the event the record
   holds through the executor's normal processing, with the quarantined skip
   bypassed for that one input, and deletes the record in the write that applies its input. That write keeps the
   instance `QUARANTINED`, with its failure record and `releasedAt`, while a live write that applies the input the
   failure record names clears the record and writes `ACTIVE`. So while the instance is releasing, its failure record
   says nothing about whether the input it names was applied, and the stream marks alone decide whether a record's
   input was applied. K is applied once, as its record. A record whose input the stream marks show as applied already
   is deleted without being applied again, in a write that raises only the instance's version, also when it is K's or
   the failure record names its input. Release reads and writes the stream marks with no
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
   the marks, so when the instance applied the same event before the quarantine, the release applies it again. A
   redelivery of it that arrives after a release applied and deleted its record is recorded and applied again too.
   That is what `BEST_EFFORT` already does with any redelivery. Two releases that overlap on such a record, two
   `release` calls or a call and the timer poller, can each apply it as well. The second one reads the records before
   the first one's write and the instance after it, and the marks don't list the input, so it replays the record onto
   a state that took it already and dispatches the commands computed from that state. Its write then finds the record
   gone and writes nothing, so the stored state takes the input once. Dispatch stays before the write, as ADR 63 has
   it, and reading the record again before the replay would only make the gap between the two reads smaller.
3. The instance becomes `ACTIVE`, with the failure record and the skip counter cleared, only in a compare-and-set that
   expects the version release read before it found no records left. Release reads the instance first and the records
   after it, so a skip that records an input between the two reads has changed the version, and the write loses. The
   write also takes place only while no record is under the quarantine id, which the store checks inside the write, so
   a record the release's read missed stays and is replayed. A skip that loses its compare-and-set to this write reads
   the instance again and applies its input, which was never acknowledged, to the `ACTIVE` instance like any live
   input.

Release counts a held event's failure under the key the event was recorded under, with the same budget as any other
input, and on the stream marks when the event has a stream id and version. A failure while releasing replaces the
failure record differently from a live one, see point 5.

An input that arrives live while the instance is releasing reaches the quarantined skip and is recorded. The skip's
write changes the instance's version together with the record, so a release that read the records before the record
existed loses its `ACTIVE` write, reads again, and finds the new record. When the skip's write loses instead, because
the instance went `ACTIVE` first, it has written nothing, and the input is processed again against the `ACTIVE`
instance and applied like any live input. Either way no input is skipped without a record that a release reads.

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

Release reads nothing from the event store, so it needs no `EventStoreQueries`, and it works the same whichever
subscription model delivered the inputs, a push model included.

Release returns a `ReleaseResult` that says what it did:

- `RELEASED`: every record was replayed and the instance is `ACTIVE`, or `COMPLETED` when a replayed input completed
  it.
- `STILL_RELEASING`: an input failed again inside its budget, or with quarantine off, and the instance stays
  releasing.
- `QUARANTINED_AGAIN`: an input's budget ran out.
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
the same instance at once compete for each record. The write that applies a record deletes it, so the node that loses
reads again and finds the next record first. A node that read the record before that write and the instance after it
finds that the stream marks show the record's input as applied, and writes the instance one version up without
applying it. The failure record plays no part in that, as point 3 step 2 says.

When a recorded input fails again while releasing, its failure replaces the failure record, its version is pending on
the stream marks, and the instance stays releasing. A live failure that replaces a failure record keeps the input the
replaced record named on the stream marks as one that failed, so a redelivery of it is applied. A failure while
releasing keeps nothing of the replaced record on the marks, since the release has applied that input by then, and
keeping it would make an applied input count as not applied and apply it twice.

A held input without a stream id and version, which only `RedeliveryDetection.BEST_EFFORT` records, has its failure
counted under its record's CloudEvent key. Every retry replays the same record, so that key matches from one failure
to the next. The stream marks can't hold such an input, so the failure record alone holds its clock, and the marks
stay as they are. A failure inside the budget writes nothing once the failure record names the input, and when the
budget runs out the instance is quarantined again on it. A live failure of such an input doesn't quarantine an
instance.

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
- A release working through records whose inputs the stream marks show as applied. It deletes them in writes that
  raise only the instance's version and leave its `updatedAt` as it is, so after `timerPollInterval` of that the
  poller goes on beside it. Neither dispatches anything for those records, so only the first record that isn't
  applied can have its commands dispatched by both, the same at-least-once dispatch a live input has.
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

The instance keeps the record of the input that failed, since the write for a used-up budget doesn't delete it. Release
applies the records in order, so the input that failed has the first record left, and the next release replays it
first without moving anything. The failure record names the input by the key its record is under, a stream key or a
CloudEvent key.

When a read of the store fails, the release throws and the instance stays releasing. On a store that answers the
poller's query, the poller tries again on its next poll.

### 6. Timers stay held until the instance is ACTIVE

The status stays `QUARANTINED` while an instance is releasing, and `findWithDueTimers` returns `ACTIVE` instances only.
A timer that came due while the instance was quarantined or releasing fires once it is `ACTIVE`.

So such a timer fires only after every held input has been replayed, including inputs that arrived after its deadline.
For a saga that uses a timer as a deadline, a held input from after the deadline that cancels or re-arms the timer
means the timeout never fires. A saga's timers run on processing time across the framework, and
[ADR 185](0185-saga-timers-run-on-processing-time.md) records that rule, this order, and what an application that needs
a deadline ordered against its inputs by event time does instead.

### 7. Quarantine is on by default again

`SagaRunnerConfig.defaults()` quarantines after 5 minutes, and so does the Spring Boot starter when
`occurrent.saga.quarantine-after` is left out. ADR 134 turned it off only because nothing brought a quarantined
instance back. Five minutes is the budget ADR 134 Decision point 3 argues for.

It applies to every saga, whichever subscription model feeds it, a push model included, since a release replays the
events the instance holds and asks the model for nothing. 0.34.0 quarantined only on a model that retains every event
it delivers, and turned quarantine off on any other with a log line at startup. That check and its log lines are gone,
and so is `HistoryRetainingSubscriptions`, the capability a model implemented to say it retains every event. 0.34.0
shipped it, and 0.35.0 removes it with nothing in its place.

The default applies to a `SagaRunner.run` given no config or `SagaRunnerConfig.defaults()`, and to every `@Saga` in
Spring Boot. The 3- and 4-argument `SagaRunnerConfig` constructors keep quarantine off.

`SagaRunner.run` throws an `IllegalStateException` when quarantine is on and the saga's state store doesn't implement
`SagaStateStoreSkippedInputs`, whether quarantine comes from the defaults or was configured explicitly. The message
names the interface and `disableQuarantine()`. Such a store would quarantine an instance and skip its inputs without
recording any of them, so `SagaStateStore.delete(sagaId)` would be the only way out, which is the situation ADR 134
turned quarantine off for. The Spring Boot starter refuses a `@Saga` the same way on every subscription model, and its
message names `occurrent.saga.quarantine-enabled=false`.

Right after that check, `SagaRunner.run` calls `requireAtomicTransitions()` on the store, which throws when the store
can't write a transition atomically in the database it is connected to. `SpringMongoSagaStateStore` throws on a
standalone MongoDB server, which refuses transactions, and the message names
`occurrent.saga.quarantine-enabled=false` and `SagaRunnerConfig.disableQuarantine()`. A saga on a standalone server
runs with quarantine off, and
[ADR 182](0182-a-saga-instance-and-its-skipped-input-records-change-in-one-atomic-write.md) has the details. Both
refusals happen at startup, so a saga fails before it receives its first input rather than when an instance is first
quarantined.

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
- A record document holds the event of its input as a CloudEvents JSON string, in its `event` field. Every record has
  one, K's record included, which the write that quarantines the instance writes. The collection is new in 0.35.0, so
  no released document changes.
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
deploy, as the upgrade guide says. 0.35.0 quarantines a push saga too, which 0.34.0 never did, so a push saga's
instances are among those a 0.34.0 node throws on.

On a subscription model that writes its position or checkpoint only after the action returns, such as the MongoDB
subscription models, the event the 0.34.0 node fails on, and everything after it, is delivered again. The next
paragraphs say how a 0.34.0 node that runs anyway stalls on it. A push feed delivers the event again only when its
caller pushes it again.

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
   longer decodes or a read of the store that failed, and release the instance again.
5. Repeat steps 2 to 4 until every instance step 2 lists has a release that returned `QUARANTINED_AGAIN`, or that keeps
   failing for a cause you can't fix.
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
inputs it skipped on 0.35.0 stay in the store, where nothing reads them once the quarantine id is gone. Each holds the
event it recorded, which on a push source is the only copy there is. The records take up storage, and they keep that
copy until they are deleted by hand.

ADR 154 stores an instance with stream marks as `ACTIVE_MARKED` for the same reason this record stores
`QUARANTINED_RECORDING`, and says what setting it back to `ACTIVE` loses.

The upgrade guide has the procedure for operators, in
[Upgrading from 0.34.0 and rolling back to it](../../migration/upgrading-to-0.35.0.md#upgrading-from-0340-and-rolling-back-to-it).

### 9. Three corrections to ADR 134

- ADR 134's Status section says the quarantine budget is off on "push feeds and the broker sources". `SagaRunner` also
  turns it off for `CatchupThenPushSubscriptionModel` and for the in-memory subscription model, so that list is
  incomplete. That rule is gone since the amendment for #918 and #952, and quarantine runs on every model, see point
  7.
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
- A skip by a quarantined instance now makes one write, a compare-and-set that raises the skip counter and records the
  input with its event. The write that quarantines an instance records K the same way.
- Every saga quarantines and releases, a push saga included, and a release reads nothing from the event store.
- On MongoDB every saga write waits for a majority of the replica set to journal it, up to 10 seconds an attempt, or the
  client's `timeoutMS` when it sets one above 0, as ADR 182 says. With a `timeoutMS` of 0 the commit of a transaction
  the store runs waits without limit. That slows a quarantine, a skip, the start of a release and every record a
  release replays by
  the time a majority takes, and a replica set without a majority available fails them where `w: 1` would have
  succeeded.
- A release keeps every stream version it applies in the instance's stream marks until the write that ends it, see
  point 2, so the instance's document grows by one number per replayed input during one release. Before a release
  applies a record it counts the versions the marks keep, and at 500,000 it stops without applying the record or
  dispatching its commands. The instance is quarantined again on that record with its marks trimmed to the reorder
  window, the release returns `QUARANTINED_AGAIN`, and an ERROR names the instance. A further release goes on from that
  record. On MongoDB 500,000 versions take about 8 MB of the 16 MB a document may hold, which leaves the rest for the
  state. A longer backlog takes more than one release. Keeping the
  versions a release applies outside the instance's document would remove the bound, and is left for later.
- A quarantined instance keeps a record of the inputs it skips, with no limit on how many, and each record holds a whole event.
  The records take space in the store until a release replays them, a replayed input completes the instance, or the
  0.35.0 store deletes the instance. The downgrade in point 8 keeps the records of the instances that stay quarantined
  in the store, and nothing reads or deletes them, since a release, a completion and a delete all go by the instance's
  current quarantine id. A 0.34.0 `delete` after a downgrade removes none of the records.
- Deleting a quarantined instance deletes the only copy the saga has of the events it skipped. On a push source that
  can be the only copy there is.
- An event erased from the event store while an instance holds it, for a GDPR request for example, stays in its record
  until a release replays it or the instance is deleted. Erasing an event means releasing or deleting the quarantined
  instances that hold it as well. The same goes for an event redacted in place with `EventStoreOperations.updateEvent`.
  The record keeps the event as it was when the instance skipped it, and a release replays that copy, not the redacted
  one.
- A record of an input without a stream id and version, taken under `RedeliveryDetection.BEST_EFFORT`, is still
  replayed after a switch to `REQUIRED`, since `REQUIRED` refuses such an event only when it's delivered live. The
  release applies the record once, because the write that applies it takes place only while the record is there.
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
- A saga with quarantine on fails at startup when its state store doesn't implement `SagaStateStoreSkippedInputs`, or
  can't write a transition atomically. That includes a `SagaRunner` built in plain Java on the defaults, from
  `SagaRunnerConfig.defaults()` or from a `run` overload without a config, which started in 0.34.0, and a saga on a
  standalone MongoDB server. The changelog and the upgrade guide name `disableQuarantine()` and
  `occurrent.saga.quarantine-enabled=false` as the ways to turn quarantine off.
- `HistoryRetainingSubscriptions`, which 0.34.0 shipped, is removed. A subscription model of your own that implements
  it stops compiling until the `implements` clause goes, together with its `retains` and `retainsEveryEvent`
  overrides.
- `SagaFailure` gains two components, so a record pattern over it stops compiling. `SagaEnvelope` changes in 0.35.0
  already, and gains one more component.
- A custom `SagaStateStore` with quarantine on has to implement `SagaStateStoreSkippedInputs`, keep each record's event
  and return it unchanged, and delete the records in `delete(sagaId)`, or turn quarantine off. Without the interface the runner refuses to start, see point 7. The new
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
- **Record only the key, and read each record's event back from the event store at release**, as this record first
  decided. That needs a subscription model that keeps every event it delivers, so a push saga couldn't quarantine
  (#952, #918), and it needs an `EventStoreQueries` on the runner. An event erased from the store, or a runner given an
  `EventStoreQueries` for another database, left an input release couldn't replay, which quarantined the instance
  again.
- **Record K when a release starts, at a sequence number below every other record's.** It needs a fourth transition
  that moves K's record to the head. On a push source the event K failed on is in hand only during the delivery that
  quarantines the instance, so a release that starts later has nothing to record K from.
- **A new `SagaStatus.RELEASING`.** Every exhaustive `switch` over `SagaStatus` would stop compiling again. A 0.34.0 node
  throwing on a status it doesn't know is what point 8 wants, and the store-only `QUARANTINED_RECORDING` gets that
  without a public enum value.
