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
`SagaStateStoreSkippedInputs`, or one that can't write a transition atomically. A release reads the instance and its
first record with `findWithFirstSkippedInput`, as one write left them, instead of reading the instance and then its
records. Points 1 to 5 and 7 to 9, the
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

A store keeps a record's event and returns it from `findSkippedInputs` and `findWithFirstSkippedInput`. The saga
keeps no other copy of a skipped
event. A release converts the event again, so `SpringMongoSagaStateStore` holds an event so that a release hands the
converter the event exactly as it was delivered. That means the same attributes, the same `CloudEventData` class,
the same data values with the same Java classes at any depth, the same bytes, and the extensions with the same values
and the same Java classes. The one exception is an extension that is a `Number` of a class the store doesn't hold as
that class, such as an `AtomicLong`, which comes back with its value as a `Long`, a `BigDecimal` or a `Double`, or a
subclass of `BigInteger` or `BigDecimal`, which comes back as a plain `BigInteger` or `BigDecimal`. A map or a list inside the
data may come back as another implementation of `Map` or `List`, such as a `Document` for a `LinkedHashMap`. Data the
record can't hold that way is held as its bytes and its class, and a record too large for one document is split
across several, so an event a quarantined instance skips stops the subscription only when the record can't hold its
attributes, an extension such as a `UUID` that a `CloudEventExtension` sets, or its key.

In the record, `event` holds the event's attributes as a CloudEvents JSON string, and `extensions` its extensions. A
`URI`, an `OffsetDateTime`, a `Byte`, a `Short`, a `Float`, a `BigInteger` and a `BigDecimal` extension is stored as a
string with a tag that names the class, since BSON would read it back as another class. A `String` with an unpaired
surrogate, which BSON's UTF-8 can't hold, is stored as its UTF-16 code units in BSON binary with the tag `utf16`, and
comes back as the same `String`. Any other extension is stored as it is. The store reads each extension once and
holds a copy of a `byte[]`, so an array its producer changes while the store checks the record is checked and stored
as it was first read. Every other class a release hands back is immutable.

A `Number` of any other class than `Byte`, `Short`, `Integer`, `Long`, `Float`, `Double`, `BigInteger`, `BigDecimal`
and `Decimal128` comes back as another class. The store reads its value once, so a number its producer changes while
the store checks the record is held and checked as one value. A subclass of `BigInteger` or `BigDecimal` is stored with
the `bigInteger` or `bigDecimal` tag and comes back as a plain `BigInteger` or `BigDecimal` with its value, and a
`BigDecimal` keeps its scale. Any other is stored with the tag `number`, the
name of its class, and its value. A release hands it back as a `Long` when its value is an integer that fits in one,
and otherwise as a `BigDecimal` with its value and scale, which the record stores as its decimal string. Its value is
the decimal its `toString` writes, which is how Jackson writes such a number, and a `Number` whose `toString` isn't a
decimal, such as `NaN` or an infinity, comes back as its `doubleValue`, a `Double`. Once the record is written, the
store logs a `WARN` that names the instance, the input, the extension, the delivered class and the class a release
hands back, and not the value. It logs nothing for an event it refuses, a write that loses its compare-and-set, or an
event whose key it has recorded already. Inside a MongoDB transaction of the caller's, it logs once the record is
written in that transaction, before the caller commits it. When the caller then rolls back, the `WARN` is for a record
that was never kept, and the store logs one again only when a redelivery records the event. Logging after the commit instead would need a hook that a transaction on a
session from `MongoTemplate.withSession` doesn't have, and the `WARN` only goes to the log. Outside such a transaction,
the store logs once its own transaction commits. When MongoDB can't tell whether that commit took effect, and the
commit still fails after the retries the store's `RetryStrategy` allows, the store throws and logs no `WARN`. When that
commit took effect, a redelivery finds the key recorded and logs none either. A release can't hand back the delivered class,
since `Number` has no way to build one from a value, and refusing the event would stop the subscription for every
instance of the saga. The MongoDB event store never delivers such an extension, since it hands back every extension as
an `Integer`, a `Long`, a `Boolean` or a `String`, so such an event comes from an in-memory event store or one of your
own.

`InMemorySagaStateStore` holds such a `Number` by the same rule and logs the same `WARN`, for any class other than
`Byte`, `Short`, `Integer`, `Long`, `Float`, `Double`, `BigInteger`, `BigDecimal` and `Decimal128`, so a release hands
back the same class from either store. The saga DSL's common module doesn't depend on BSON, so it knows a `Decimal128`
by its class name, as it already does for a `Decimal128` in data, and holds it as it is, since the class is final and
immutable.

Data delivered as a `PojoCloudEventData` that holds a map, which is how the MongoDB event store delivers a JSON object,
is held in `dataDocument`, with its bytes in `data` unless they are the document's own JSON, and a release hands the
converter a map again. Other data is held in `data` as its bytes in BSON binary, and a release hands the converter a
`BytesCloudEventData`. Written as CloudEvents JSON, binary data would grow by a third as base64, so a 13 MiB binary
event would take more than 17 MiB. JSON data parsed into a document would read a field such as
`{"$numberLong": "42"}` as MongoDB Extended JSON and come back as `42`. A map released as its bytes would hand the
converter the MongoDB event store's JSON of it, where a date is `{"$date": ...}`, instead of the map it converted on
delivery.

`compareAndSaveSkipped` checks the record with a round trip before it writes anything. It encodes the record to BSON
the way MongoDB stores it, decodes it, and reads the event back with the method `findSkippedInputs` and
`findWithFirstSkippedInput` read a record with, so the check and a release read the record the same way. It writes
the copy of the record it checked, decoded from the record's BSON, so a change the application makes to the delivered
bytes, maps or extensions after the check doesn't reach the record.

When the data read back differs from the delivered data in a value at any depth, in its value or its Java class, or
encoding, decoding or reading it back throws, the record holds the data's bytes in `data` instead, as BSON binary, and
the name of the data's class in `dataClass`. A `BigDecimal` in a map comes back as a `Decimal128`, a map at any depth
with a key that isn't a `String` makes the BSON codec throw a `ClassCastException`, and the `JsonCloudEventData` the
CloudEvents JSON format reads a JSON object into comes back as a `BytesCloudEventData`, so all three are held this way.
A release hands the converter a `JsonCloudEventData` read from those bytes when `dataClass` names one, and a
`BytesCloudEventData` with the same bytes for data of any other class. The bytes are read the way a `JsonFormat` with
its default options reads the data of an event, not by the format the application registered with
`EventFormatProvider`, since a format made with `withForceJsonDataToBase64()` reads them back as a
`BytesCloudEventData`. That read turns a decimal number into a `double`, so when a `double` can't hold one of the
decimal numbers in the data, such as `1234567890.123456789`, every decimal number in it is read as a `BigDecimal`
instead. A `JsonCloudEventData` built from a node rather than read by the format comes back as the node its JSON reads
as, such as a `DoubleNode` of `1.1` for a `DecimalNode` of `1.10`, which is the same number.

The check compares the data it reads back from the bytes with the data a release promises, its class and, for a
`JsonCloudEventData`, its JSON value at every depth. A number equals a number of the same value whatever its node
class, and any other value equals only an equal value of the same node class. Once the record is written, the store
logs a warning that names the instance, the input and the difference, since the converter then gets bytes where it was
delivered a map or an object. It logs that warning at the same point as the `WARN` for a number, inside a MongoDB
transaction of the caller's and outside one, and logs neither when it throws on a commit of its own whose result MongoDB can't
tell.

A record that doesn't fit in one document, at most 16 MiB, or in the write that sends it, at most 16 MiB and 16 KiB
with its filter, is encoded to BSON and split into chunks of 8 MiB. Each chunk is a document in a collection named
after the saga collection with `_skipped_input_chunks` appended, under the record's saga id, quarantine id and key and
the chunk's index. The record then holds only `seq`, the number of chunks in `chunks` and their total size in `size`.
A read takes the chunks below the record's count in the order of their index, and throws an `IllegalStateException`
when an index is missing or the bytes don't add up to `size`, instead of reading another event.

The chunks are written before the record that points at them and deleted after it, and every one of those writes is in
the transaction that writes the instance. A failure between two of them rolls all of them back, and a Spring
transaction of the caller's that catches the failure can't commit either, since the store marks it rollback-only. A
transaction whose session its owner bound to the `MongoDatabaseFactory` is one the store can't abort, and its owner
can commit it after a failed write. That leaves chunks no record points at, when the record's write failed, or when
the chunks' delete failed after the record's. It never leaves a record without its chunks, since the chunks are
written first and deleted last. A read never reaches a chunk no record points at, the next skip of the same key
overwrites it, and it is deleted with the records of its key or its quarantine id, by a replay, by the end of the
quarantine, or by `delete(sagaId)`.

`compareAndSaveSkipped` throws an `IllegalStateException`, and the instance and its records stay as they were, only
when:

- The event read back differs from the delivered one in an attribute or an extension, in its value or its Java class,
  other than for a `Number` the record holds with the tag `number`.
- Encoding, decoding or reading the record back throws even with the data held as its bytes, such as for an extension
  of a class BSON has no codec for.
- The data is a `JsonCloudEventData` whose bytes read back as another JSON value. A `BinaryNode` is written as base64
  and reads back as text, a `POJONode` reads back as the JSON its object is written as, and a `DoubleNode` of `NaN` is
  written as the text `"NaN"`.
- The saga id, the quarantine id and the key take so much room that a chunk doesn't fit beside them in one document,
  which takes about 8 MiB between them.

The message names the instance, the input and, when it's known, where in the event the difference is. A subscription
that retries such an event stops on it.

Neither that message nor the warning quotes the event. They name an attribute or an extension by its name, and a value
in the data by its position, such as `entry 2 > element 0`, and its class. A key in a map, a field name in JSON and a
value are left out, since they can be personal data, and a log line keeps them after the event is erased from the
event store.

The methods that read and write the records are on an interface of their own, `SagaStateStoreSkippedInputs`, which a
store implements next to `SagaStateStore`, the way it implements `SagaStateStoreQueries`. It has no state type
parameter, since its methods take and return an instance as a `StoredSaga`, with the state encoded already, and none
of its methods has a default, `fits` and `fitsWithoutState` included, so a store with a size limit doesn't answer `true`
by leaving it out.
The in-memory store and
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
finds it gone and writes nothing, and then reads the instance and its first record again. The check holds whatever the
read before it returned, so an event with no stream id and version is applied once even on a store whose
`findWithFirstSkippedInput` returns an instance with a record of an earlier version of it. Nothing on the instance says
such an event was applied. No write moves a record. A release starts in a write to the instance alone,
see point 3 step 1.

`SagaStateStore.delete(sagaId)` deletes the instance and the records under the quarantine id it had in one atomic
write. When a replayed input completes the instance, the write that completes it, `compareAndSaveReplayed`, deletes
every record under its quarantine id, since a completed instance skips every input and none of them can be replayed.
It writes only while the replayed record is still there, as for any replayed input, so a release that replays a record
another release applied already can't complete the instance and delete records no release applied. So in the stores Occurrent ships, the records of a quarantine exist only while the instance is quarantined
under its quarantine id, until the downgrade in point 8 takes the quarantine id off the instances that stay
quarantined, and nothing reads their records after that.

`SpringMongoSagaStateStore` reads the instance and its records from the primary, whatever read preference the
`MongoOperations` it is given has. A release reads the instance and its first record with `findWithFirstSkippedInput`,
in one transaction at read concern `snapshot`, as ADR 182 point 3 says, and every other read is at read concern
`majority`. It refuses to run inside a MongoDB transaction of the caller's, or inside a Spring transaction of another
resource that synchronizes its resources, as ADR 182 point 4 says, so a release runs outside both. A read from a secondary that lags can find no record while one is written, and a release turns the instance
`ACTIVE` once its read finds no record. The write that does that
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
   `release` calls or a call and the timer poller, don't apply it twice. A release reads the instance and its first
   record with `findWithFirstSkippedInput`, which returns both as one write left them, so a release that reads after
   another one applied the record finds it gone. Two releases that both read before that write each dispatch the
   record's commands, as for any record, see the paragraph on a lost compare-and-set below. The second one's write
   loses on the version, so the stored state takes the input once. Dispatch stays before the write, as ADR 63 has it.
3. The instance becomes `ACTIVE`, with the failure record and the skip counter cleared, only in a compare-and-set that
   expects the version of the read that found no record left. That read returns the instance and its first record as
   one write left them, so a skip that records an input after it has changed the version, and the write loses. The
   write also takes place only while no record is under the quarantine id, which the store checks inside the write, so
   a record written after the read stays and is replayed. A skip that loses its compare-and-set to this write reads
   the instance again and applies its input, which was never acknowledged, to the `ACTIVE` instance like any live
   input.

Release counts a held event's failure under the key the event was recorded under, with the same budget as any other
input, and on the stream marks when the event has a stream id and version. A failure while releasing replaces the
failure record differently from a live one, see point 5.

Before release writes that failure, it reads the instance and its first record again, in one read, and checks that
the record it replayed is still the first record of the quarantine. It compares the quarantine id, the `seq` and the key, and never the event. `InMemorySagaStateStore` and
`SpringMongoSagaStateStore` both read a record back as a new event, and a byte array in an extension or in the data
equals no byte array of an earlier read. Comparing events would find the record changed on every attempt, so release
would never write the failure, the input's budget would never run out, and the instance would stay releasing for good.

An input that arrives live while the instance is releasing reaches the quarantined skip and is recorded. The skip's
write changes the instance's version together with the record, so a release that read before the record existed
loses its `ACTIVE` write, reads again, and finds the new record. When the skip's write loses instead, because
the instance went `ACTIVE` first, it has written nothing, and the input is processed again against the `ACTIVE`
instance and applied like any live input. Either way no input is skipped without a record that a release reads.

A lost compare-and-set while releasing is either such a skip or another node's release of the same instance. Release
follows the at-least-once dispatch contract of live inputs. A reaction is dispatched before the write that applies its
input, so a node whose write loses has dispatched it too, and two nodes releasing the same instance at once can each
dispatch a record's reaction. After a lost write, release reads the instance and its first record again. Only skips
happened when the
first record is still the one it replayed and the instance read again matches the one it read before in the state,
the status, the timers, the stream watermarks and marks, `createdAt`, `updatedAt`, `completedAt`, whether it started
and the whole failure record. Release then writes the result it computed onto the instance it read again, with that
instance's skip counter and version, and doesn't dispatch the reaction again. It tries that write up to
`maxCasAttempts` times. When anything else changed, or those attempts all lose, it reads the instance and its first
record again and computes again, which dispatches the reaction again. After `maxCasAttempts` rounds in a row that end in a
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
decoding failure. To go on, fix how the state decodes and release the instance again, or delete the instance.

An instance whose state stops decoding while it is releasing, such as on a state a replay saved, stays releasing, and
each read that fails counts against the budget of the record the release is on, the way a replay that fails does. A
`release` call on it returns `STILL_RELEASING` instead of throwing. Once the budget runs out, the instance is
quarantined again on that record, and a later release throws the decoding failure until the state decodes.

When the replay that saved that state applied the last record, no record is left to count the failure against, so the
release ends the quarantine and returns `RELEASED`. The write that ends it writes no state, so the stored state stays as
the replay saved it. The next input to the instance then fails on the decoding the way it fails for any `ACTIVE` instance.

A held record whose event no longer decodes counts against its own budget the same way, and the failure record names it
by its key. Once its budget runs out, the instance is quarantined again on that record, and a later release replays it
first.

### 5. Release runs on any node, and a crashed release is resumed

Release takes no lease and pauses nothing. Every step that changes the instance is a compare-and-set on its document,
so release can run on any node, including a node that doesn't hold the saga's subscription lease. Two nodes releasing
the same instance at once compete for each record. The write that applies a record deletes it, so the node that loses
reads again and finds the next record first. Each node reads the instance and its first record in one read, so no node
replays a record against an instance that applied it already. The failure record plays no part in that, as point 3
step 2 says.

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

When the read of the instance and its first record fails, the release reads the instance and the key of its first
record again with `findWithFirstSkippedInputKey`, which decodes neither the state nor the record's event. It records the
failure against that record, as above. When no record is left, the release ends the quarantine as in point 4. Nothing
it reads or checks from then on decodes the stored state. The size checks measure the instance as the store holds it,
and the failure record names the input from the record read alone, by its event when that decodes and by its key when
it doesn't. On MongoDB, a flow saga whose received events no longer decode has its failure counted against the
record, or is released, the same way. When the read with `findWithFirstSkippedInputKey`
fails too, the release throws and the instance stays releasing. On a store that answers the poller's query, the
poller tries again on its next poll.

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

An instance quarantined while its data was on a store that records skips, and then moved to one that can't, isn't
refused at startup, since quarantine is off there. It can't record the inputs it skips, so the delivery of an input it
has to record as skipped fails and the input isn't acknowledged, since skipping it without a record would leave an
input that no release replays. An input it needs no record for, such as a redelivery of one it applied before it was
quarantined, is skipped and acknowledged as on a store that records skips. A subscription that delivers an event again
before any later one, as a MongoDB subscription does, holds up every later event of that saga subscription meanwhile,
for every instance of the saga, not only the events of the quarantined instance. That lasts until the instance's data
is moved back to a store that records skips and it is released there, or until it is deleted by hand.

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
- A record document holds the attributes of its input's event as a CloudEvents JSON string, in its `event` field,
  its extensions in `extensions`, and its data in `dataDocument` when it was delivered as a map and in `data` as
  bytes otherwise, with `dataClass` naming the data's class when the record holds as bytes data it couldn't hold as
  delivered, see section 2. A record too large for one document holds `chunks` and `size` in place of those fields,
  and its BSON is split across documents in a collection named after the saga collection with
  `_skipped_input_chunks` appended, each holding `sagaId`, `quarantineId`, `key`, `chunk` and `bytes`, with a unique
  index on the first four. Every
  record has an `event`, K's record included, which the write that quarantines the instance writes. The collection is new in 0.35.0, so
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
  point 2, so the instance's document grows by one number per replayed input during one release. A release asks the
  store before every write it makes whether it can take it, through `SagaStateStoreSkippedInputs.fits`, and for the
  write that applies a record it asks before it dispatches the record's commands. For a write that leaves the stored
  state where it is, which starts a release, records a failure, quarantines the instance again or ends the quarantine,
  it asks through `fitsWithoutState(saga, expectedVersion)`. `SpringMongoSagaStateStore` reads the instance as it holds
  it, replaces the fields that write sets and measures the result, without decoding the state, so a state that no
  longer decodes doesn't fail the answer. The in-memory store answers `true`, as it does for `fits`. When the store can't take the
  instance with the record applied, or with the record's failure recorded, the release doesn't apply the record. An
  attempt the store refuses that way dispatches none of the record's commands, but an earlier attempt can have dispatched
  them already with a write that then lost to another writer, and the ERROR says how many. The instance is quarantined again on that record with its marks trimmed to the reorder window, the release
  returns `QUARANTINED_AGAIN`, and an ERROR names the instance. When the record applied to the trimmed instance fits,
  the ERROR says to release the instance again, and a further release goes on from that record. Otherwise the ERROR
  says a further release stops on the record too. When the store can't take the write that quarantines the instance
  again, even with no pending entry for the record and no failure message, the release writes nothing, returns
  `STILL_RELEASING`, and an ERROR names the instance and says to delete it, and the timer poller logs that again on
  every poll. `SpringMongoSagaStateStore` refuses an instance of more than 16 MiB, and a write of more than 16 MiB and 16 KiB with its compare-and-set filter, which holds the saga id. A store whose `fits` answers `true` to every instance refuses
  nothing, and a release on it dispatches the commands of a record whose write the store then fails. A backlog long enough takes more
  than one release. Keeping the versions a release applies outside the instance's document would remove the limit, and
  is left for later.
- A quarantined instance keeps a record of the inputs it skips, with no limit on how many, and each record holds a whole event.
  The records take space in the store until a release replays them, a replayed input completes the instance, or the
  0.35.0 store deletes the instance. The downgrade in point 8 keeps the records of the instances that stay quarantined
  in the store, and nothing reads or deletes them, since a release, a completion and a delete all go by the instance's
  current quarantine id. A 0.34.0 `delete` after a downgrade removes none of the records.
- Deleting a quarantined instance deletes the only copy the saga has of the events it skipped. On a push source that
  can be the only copy there is. The MongoDB store logs a WARN with the number of records it deletes. Inside a MongoDB
  transaction of the caller's, it logs it before the caller commits, so a rollback can leave a WARN for a delete that
  was never kept. When the store throws on a commit of its own whose result MongoDB can't tell, it logs none. A TTL index or a
  job that removes instances in the database deletes none of their records. The records and their chunks stay in the
  database with no instance, where no release can replay them and nothing deletes them.
- An event erased from the event store while an instance holds it, for a GDPR request for example, stays in its record
  until a release replays it or the instance is deleted. Erasing an event means releasing or deleting the quarantined
  instances that hold it as well. The same goes for an event redacted in place with `EventStoreOperations.updateEvent`.
  The record keeps the event as it was when the instance skipped it, and a release replays that copy, not the redacted
  one. Since quarantine is on by default, an application that erases or redacts events has this copy after an upgrade
  from 0.34.0 without having changed anything.
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
- **Check a held event against a list of the classes the record can hold**, instead of with a round trip. Such a list
  has to know every way BSON changes a value, and two it didn't know about were a `String` with an unpaired surrogate
  and a key that isn't a `String` in a nested map or a map inside a list. The round trip finds both without knowing
  about either.
- **Refuse an event the record can't hold as delivered, or one over 16 MiB.** A subscription that delivers the event
  again before any later one, as a MongoDB subscription does, would stop on it for every instance of the saga, and the
  only way out would be to change the event or the converter. Holding the data's bytes and splitting a large record
  keep the subscription going, at the cost of a release handing the converter bytes where it was delivered a map or an
  object.
