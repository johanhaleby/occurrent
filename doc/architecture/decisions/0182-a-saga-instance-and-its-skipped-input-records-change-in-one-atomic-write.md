# 182. A saga instance and its skipped-input records change in one atomic write

Date: 2026-11-19

## Status

Accepted, for [#1259](https://github.com/johanhaleby/occurrent/issues/1259). Amends
[ADR 178](0178-a-quarantined-saga-instance-records-what-it-skips-and-is-released-by-replaying-it.md). It replaces the
separate writes of the instance and its records in ADR 178's points 1, 2 and 3, the rules for the gaps between them,
the write concerns of point 2, and the consequences that follow from them. ADR 178 gets a pointer in its Status section,
and its points are corrected in place to this rule.

It restores a rule from [ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md),
which ADR 178 dropped when it moved the records out of the instance's document. A record is written in the same
compare-and-set as the decision not to advance.

Amended in place for [#918](https://github.com/johanhaleby/occurrent/issues/918) and
[#952](https://github.com/johanhaleby/occurrent/issues/952), before 0.35.0 shipped, together with ADR 178. A record now
holds the event of the input it records, and the write that quarantines an instance records that input as the first
record. So a release has no head record to write, and `compareAndSaveReleaseStarted` is gone. Three
transitions are left. Every push saga now quarantines too, and a push saga needs no replica set, so a saga with quarantine on
can meet a standalone MongoDB server, which a saga on Occurrent's MongoDB subscription models never did. A release
reads the instance and its first record with `findWithFirstSkippedInput`, which returns both as one write left them,
instead of reading the instance and then its records. Points 1, 2 and 3 and the consequences are corrected in place,
and point 3 says what the store does on a standalone server.

## Context

ADR 178 keeps each skipped input as a record of its own, next to the instance, so a quarantine can record any number of
inputs without growing the instance's document toward MongoDB's 16 MB limit. The instance and its records were then
written separately.

A skip took three writes: a compare-and-set that raised the skip counter, the record, and a second compare-and-set on
the instance. A release wrote its head record before the compare-and-set that started it, and deleted each record after
the write that applied its input. Each gap between two of those writes needed a rule of its own. ADR 178 point 2 lists
five kinds of record that a crash or a lost race left behind where nothing reads them. Point 3 step 3 describes a
skip that loses its last write to a release, deletes its own record again and applies its input live.

The two halves also had different write concerns in `SpringMongoSagaStateStore`. Records were written at
`WriteConcern.MAJORITY` with `j: true`. The instance was written at the `MongoTemplate`'s write concern, which can be
`w: 1`. A release applies a record's input to the instance and then deletes the record. With the instance write at
`w: 1` and the delete at majority, a failover between the two can roll the instance write back and keep the delete.
The instance is then releasing without the input and without its record, and the input is lost although the source
was told it was handled.

`SagaStateStoreSkippedInputs` hasn't shipped, so its methods can change without a migration path.

## Decision

### 1. Every write that changes records changes the instance in the same atomic write

`SagaStateStoreSkippedInputs` has no method that writes or deletes a record on its own. It has three transitions, each
a compare-and-set on the instance's version that writes the instance and its records together, or neither:

| Transition | Instance | Records under the quarantine id |
| --- | --- | --- |
| `compareAndSaveSkipped` | raises the skip counter, or quarantines the instance, state left alone | records the input with its event, keeps a record of the same key that is there already |
| `compareAndSaveReplayed` | the whole instance after the replayed input, or one version up when the input was applied already | deletes the replayed record, or every record when the input completes the instance, and writes neither unless that record is still there with the sequence number the release read |
| `compareAndSaveQuarantineEnded` | the whole instance, `ACTIVE` after the last record | deletes every record, and writes neither while a record is there |

The write that quarantines an instance is a `compareAndSaveSkipped` that records the input it was quarantined on, and
inserts the instance at version 0 when that input is its first. A release starts with a
`compareAndSaveWithoutState` that sets `releasedAt` and writes no record, since the input the release starts from has
the first record already.

`SagaStateStore.delete(sagaId)` deletes the instance and the records under its quarantine id in one atomic write too.
Each transition returns `false` on a version conflict and has then written nothing. The reads
`findSkippedInputs` and `findReleasingSagaIds` stay as they were. A release reads with a third,
`findWithFirstSkippedInput`, which returns the instance and the first record under its quarantine id as one write left
them.

So records under a quarantine id change only in a write that also changes the instance's version. Every write a
release makes for the instance and record it read expects the version that read returned, so a write that wins was
made on the records as they were at that version. A read of the instance and a separate read of its records don't give
that, since each can miss a different write, see point 3. The release's last write turns the instance `ACTIVE` only
when no record was there, and checks that again inside its atomic write.

### 2. What this removes

- The three writes of a skip are one transition. The second compare-and-set, the delete of its own record after it
  lost, and point 3 step 3's case of a skip that loses to a release are gone. A skip that loses writes nothing, reads the
  instance again and applies its input live when the instance is `ACTIVE`.
- None of the five kinds of leftover record in ADR 178 point 2 can be written. A crash leaves either both halves of a
  transition or neither, a release writes no record when it starts, a completion deletes the records with
  the write that completes the instance, and a delete removes the instance and its records together.

A release reads the instance and its first record once for every record it replays, and replays the record against
that instance. The commands of the replay go out before the write that applies the record. A release that read before
another release applied the record sends them again, and its write then loses on the version.

A record whose input the stream marks show as applied is deleted in a write that raises only the instance's version and
leaves its `updatedAt` as it is. So the fourth case in ADR 178 point 5 stays. The timer poller goes on beside a release
that spends longer than `timerPollInterval` on such records, and neither dispatches anything for them.

The skip counter stays the source of each record's sequence number, and a redelivered input whose key is recorded
already still takes a number that goes unused. `SkippedInput` is a value of a sequence number, a key and the input's
event, and a store keeps the event with the record and returns it from `findSkippedInputs` and
`findWithFirstSkippedInput`, so a release replays the
record without reading the event store.

### 3. A transition that returns normally survives a failover

The executor acknowledges an input right after the store call that skipped or applied it returns. So a call that
returns normally has made its write durable.

On MongoDB, `SpringMongoSagaStateStore` runs each transition and a delete of a quarantined instance in a transaction of
its own, which reads at read concern `snapshot` on the primary and commits at `WriteConcern.MAJORITY` with `j: true`.
A commit that a majority doesn't acknowledge fails after 10 seconds. On a client with a `timeoutMS` above 0, where the
driver sends no `wtimeout`, that `timeoutMS` bounds it instead, and on a client with a `timeoutMS` of 0 the commit waits
without limit. `MongoTransactionManager` passes no transaction timeout to the driver, and under a `timeoutMS` of 0 the
driver sends no `maxCommitTime`, so the store can't bound that commit the way it bounds a single write.
A majority of the replica set has
the write in its on-disk journal before the call returns, so a new primary after a failover has it. The single-document
compare-and-sets of the live path, `compareAndSave` and `compareAndSaveWithoutState`, write at the same concern,
whatever write concern the `MongoTemplate` has. Every other read of the store goes to the primary at read concern
`majority`, so the executor never acts on a write that a failover can still roll back, its own included, such as a
write whose majority acknowledgement timed out.

A read at `majority` can still miss a write acknowledged before it. Right after a failover, a majority read on the new
primary can miss a write the old primary acknowledged at `majority`. The executor acts on an instance or a record it
finds through a compare-and-set on the version, which a read that missed a later write loses. Two decisions act on
something a read did not find, and each reads again where nothing is missed:

- `find` and `findWithoutState` read an instance they don't find again at read concern `linearizable`, which returns
  every write a majority acknowledged before the read started, as
  [MongoDB documents](https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/). The executor skips an input addressed to no instance
  without writing anything, so an instance the read missed would lose that input. Point 4 covers the read inside a
  caller's transaction. A standalone server refuses read concern `linearizable`, and has no failover, so a majority
  read there already returns every write it acknowledged. The store asks the server once, with `hello` when it is
  created, and skips the second read on a standalone server.
- `compareAndSaveQuarantineEnded` reads the records again inside its transaction and writes nothing while one is
  there. A release ends the quarantine after `findWithFirstSkippedInput` returns no record, and the write deletes every
  record under the quarantine id, so a record the read missed would be deleted before it was replayed. The transaction
  reads everything the primary has, and a new primary has every write a majority acknowledged.

Two reads at `majority`, one of the instance and one of its records, can each go to a different primary across a
failover, and each can miss a different write. A record read after the instance can then be one a later version of the
instance deleted, and a release that acts on it sends the commands of a record another release applied already. When
that replay fails, the release quarantines the instance again on a record that is gone, and its compare-and-set on
the version it read wins. So `findWithFirstSkippedInput` reads the instance and its first record in one transaction of
the store's at read concern `snapshot`, and both are what the primary had at the same moment. MongoDB documents that
only `snapshot` reads every shard of a sharded cluster at the same moment in a transaction, so that holds when the
instance and its records are on different shards too. A standalone server refuses the transaction, and every write of
a record with it, so its records don't change between two reads, and the store reads the instance and then its first
record at `majority`.

An instance that transaction doesn't find is read again at `linearizable`, as `find` does. When that read finds it, the
store reads both again in a new transaction. A new primary right after an election can hand out a snapshot from before
writes a majority already has, and the linearizable read runs in a session of its own. So the store passes the
cluster time and operation time of that session to the session of the new transaction before its first read, and the
driver asks for a snapshot at or after that time (`afterClusterTime`). That snapshot has every write the linearizable
read saw. When it doesn't find the instance either, the instance was deleted after the linearizable read, which started
after the call did.

The session of the linearizable read isn't causally consistent. The driver takes the operation time of every reply
into the session, an error reply included, and a causally consistent session sends it as `afterClusterTime` on its
next read whatever the read concern. MongoDB refuses `afterClusterTime` at `linearizable` with `InvalidOptions`, so
after one failed attempt every retry of the read would fail, also once the server is back. A session that isn't
causally consistent still keeps the latest operation time, which is at or after the read that succeeded.

`findWithFirstSkippedInput` refuses a caller's transaction, see point 4.

A transaction aborted with the `TransientTransactionError` label runs again through the store's `RetryStrategy`. A
commit that ends with `UnknownTransactionCommitResult` is committed again through the same strategy. The transaction
needs a replica set or a sharded cluster, and a standalone server refuses it. A server without journaling refuses
`j: true`. The store turns
each refusal into an `IllegalStateException` that says what the server lacks. The store throws any other failure of
the transaction at once, and throws each refusal on the first attempt, since every later attempt would be refused too.
That holds for a strategy implemented another way, which runs its own loop and retries what it retries. The attempt
returns a failure the store doesn't retry instead of throwing it, and the store throws it once the strategy has
returned, so the strategy never sees it.

The store needs the `MongoDatabaseFactory` to run a transaction, and only a `MongoTemplate` exposes it. The constructors
keep their `MongoOperations` parameter, which 0.34.0 shipped, and refuse anything else than a `MongoTemplate` with an
`IllegalArgumentException` that names the fix.

`SagaStateStoreSkippedInputs.requireAtomicTransitions()` throws when a store can't write the three transitions
atomically, and `SagaRunner.run` calls it at startup when quarantine is on. On a standalone server
`SpringMongoSagaStateStore` throws there, with a message that names `occurrent.saga.quarantine-enabled=false` and
`SagaRunnerConfig.disableQuarantine()`, so a saga fails at startup instead of at its first quarantine. With quarantine
off a saga runs on a standalone server that journals its writes, since the runner then makes none of the three
transitions. Every saga write asks for `j: true`, which MongoDB answers with an error on a server without journaling.
An instance quarantined earlier, such as one in data restored from a replica set, can't record the inputs it skips
there. The delivery of an input it has to record as skipped fails, so the input isn't acknowledged and is delivered
again. Skipping it without a record would acknowledge an input that no release replays. An input it needs no record
for, such as a redelivery of one it applied before it was quarantined, is skipped and acknowledged as on a replica set.
The instance
holds up its inputs until its data is moved back to a replica set and it is released there, or until it is deleted by
hand in the database. `SagaSubscription.release(sagaId)` throws on the standalone server before it reads or writes
anything. The write that starts a release asks for no transaction, so without that refusal a release there would mark
the instance releasing and dispatch the commands of the input it stopped on, and the server would then refuse the
transaction that records that input as applied. Every timer poll would dispatch those commands again. The timer poller
replays nothing on a store whose `requireAtomicTransitions()` throws, so an instance already marked releasing, such as
one in data restored from a replica set, stays as it is, and the first poll that lists one logs an `ERROR` that names
it. A `delete(sagaId)` of such an instance on the standalone server writes in a transaction, which the server refuses.
On a replica set and through a mongos nothing changes.

`InMemorySagaStateStore` keeps an instance and its records as one value and replaces it in one write, so a read
sees the instance and its records as one write left them.

### 4. Inside a caller's transaction, the caller's commit decides

The store takes its database from `MongoDatabaseUtils.getDatabase`, and picks how it reads and writes from that
database, not from the template. It joins the caller's transaction exactly when that database is bound to a session
with an active MongoDB transaction. That holds in three cases:

- a Spring transaction that synchronizes its resources, as a `MongoTransactionManager` does by default,
- a Spring transaction of another resource in which a template with `SessionSynchronization.ALWAYS` has started a
  MongoDB transaction,
- a transaction on the session of a template from `MongoTemplate.withSession`.

A transition that joins runs its writes in that transaction. Under a `MongoTransactionManager` the store's own
transaction propagates as `REQUIRED` and takes part in the caller's. On a template from `withSession`, the store's
transaction manager would start a second transaction on a new session, so the store runs the writes directly on the
database bound to the caller's session. The instance and its records then change together because they are in that
transaction, and they are durable only as far as the commit's write concern makes them. The store doesn't retry a
transition it joined, and a failure reaches the caller, whose transaction should then abort. A commit after the failure
fails under a `MongoTransactionManager`, which the store marks rollback-only, and may keep the writes made before the
failure in the other two cases, where it can't mark the transaction.

Nor does the store retry any other read or write while a MongoDB transaction is bound to the factory. MongoDB aborts a
transaction when an operation in it fails, so the same operation sent again in it fails too, and the caller would get
that later failure in place of the first. The store checks for a bound transaction when a call starts, and again
after a failure, since a template with `SessionSynchronization.ALWAYS` starts the transaction on the operation that
fails. The attempt returns a failure inside a transaction instead of throwing it, as in point 3, so a strategy
implemented another way doesn't send the operation again either. 0.34.0 retried every read and write, in a caller's transaction
too.

A MongoDB transaction bound to the factory while transaction synchronization is off, as a `MongoTransactionManager`
with `SYNCHRONIZATION_NEVER` runs it, gives a database bound to no session, from `MongoDatabaseUtils` and from the
template alike. A transition would still join it, since the store's transaction manager takes part in it and turns
synchronization on, but `find`, `findWithoutState` and the single-document writes would run outside it. So in that case
every call of the store runs inside the store's own transaction, which takes part in the caller's and turns
synchronization on for the call, and every database the call takes is bound to the caller's session. A failure inside
the call marks the caller's transaction rollback-only.
In a Spring transaction of another resource where no MongoDB transaction has started, the store reads and writes as it
does outside a transaction. It reads through the driver rather than the template there, since a template with
`SessionSynchronization.ALWAYS` would start a MongoDB transaction for the read, and the read would see its snapshot.

Inside a transaction the store joins, `find` and `findWithoutState` read at the transaction's snapshot, which misses an
instance written after the snapshot began. A found instance needs nothing more, since the executor writes it back through a compare-and-set, and a write inside the
transaction to an instance changed after its snapshot fails with a write conflict. An absence has no such write, and no
read in the transaction can see past its snapshot. So on an absence the store inserts a document with the
saga's id and deletes it again in the same transaction, and the server decides whether that insert conflicts.

| In the transaction, for one saga id | The read | The insert | `find` |
| --- | --- | --- | --- |
| another writer stored the instance after the snapshot began | absent | `WriteConflict` | throws, the caller's next attempt reads it |
| another writer is storing it, not yet committed | absent | `WriteConflict` | throws, the next attempt reads it or its absence |
| another writer deleted it before the snapshot began | absent | succeeds | empty |
| another writer deleted it after the snapshot began | found | not run | the instance, and a write-back of it conflicts |
| the transaction stored or updated it | found | not run | the instance |
| the transaction deleted it | absent | succeeds | empty |

The `WriteConflict` has the `TransientTransactionError` label, so a caller that runs its transaction again on that
label gets a new snapshot. The two writes leave no document behind when the transaction commits. The read and both
writes go through the database the store took from `MongoDatabaseUtils`, so they share the caller's session whatever
session synchronization the template has. The collection must accept a document that holds only `_id`, so a validator
that requires other fields makes such a read of an absent instance fail.

Two alternatives fell short. A second read outside the session can't be had on a template from `withSession`, whose
factory binds every database it hands out to the session, and it throws on an absence the transaction made by its own
delete, on every attempt. Refusing a caller's transaction outright would break every saga run inside one, the case this
point is about.

The store doesn't check that write concern. Spring keeps the transaction's `ClientSession` in a package-private
`MongoResourceHolder`, so the store can tell that a transaction is active but can't read the concern it commits at. A
refusal would break every saga run inside a caller's transaction, and a warning would fire on every such call whatever
the concern is. The SPI and the store's javadoc say that the caller's commit decides.

`findWithFirstSkippedInput` throws a `SagaReadRefusedException`, an `IllegalStateException`, inside a caller's
MongoDB transaction, in all three cases above and under `SYNCHRONIZATION_NEVER`, before it reads anything. A transaction at read concern `local` or `majority`
on a sharded cluster reads each shard at its own moment, so it could return an instance that applied a replay together
with the record that replay deleted, the pair the read exists to rule out. Only `snapshot` reads the shards at one
moment. The store can't tell the read concern of the transaction for the same reason it can't tell the write concern,
so it refuses a transaction at `snapshot` too, on a replica set as well as through a mongos. The same rule on every
deployment means a release that works against a test replica set doesn't start failing on a sharded cluster in
production. In Occurrent only a release reads with it, from `SagaSubscription.release(sagaId)` and from the timer
poller. The poller runs on a thread of its own, so it's never inside a caller's transaction, and the delivery of an
input never reads with it. A caller that released inside a transaction releases outside it instead.

`SagaSubscription.release(sagaId)` throws the `SagaReadRefusedException` as it is. For any other failure of the read
it reads the instance again, without its state and then whole, to tell a state that doesn't decode from another
failure. Inside the caller's transaction those reads would run in it, and for an absent instance the store inserts and
deletes a document there to read it, so a validator on the collection or a concurrent insert of the same id could abort
the caller's transaction before the refusal reached it. A store of your own that throws a plain
`IllegalStateException` instead gets those reads.

## Consequences

- A quarantined instance's records exist only while it is quarantined under that quarantine id. Nothing is left behind
  to clean up, which settles [#1546](https://github.com/johanhaleby/occurrent/issues/1546) for the stores Occurrent ships.
- Every saga write on MongoDB waits for a majority of the replica set to journal it, up to 10 seconds an attempt, or the
  client's `timeoutMS` when it sets one above 0. On a client with a `timeoutMS` of 0 the commit of the store's own
  transaction waits without limit. On a primary, secondary and arbiter replica set with one data-bearing member down,
  there is no majority to journal to, so saga writes fail until that member is back, where `w: 1` would have succeeded.
- A `MongoTemplate` configured with `w: 1` no longer decides how a saga instance is written. 0.34.0 wrote the instance at
  the template's concern, so this is a breaking change, with a section in the upgrade guide.
- Quarantine on MongoDB needs a replica set or a sharded cluster, because a quarantine and a skip run in a
  transaction. Occurrent's MongoDB subscription models need one already for change streams, but a push saga doesn't,
  so the store refuses quarantine at startup on a standalone server, and a saga there runs with quarantine off.
- A `find` or `findWithoutState` that finds no instance reads a second time on a replica set or through a mongos, and
  that read waits for a majority as a write does. The first input of a new instance waits for it, and a found
  instance costs one read as before. A standalone server gets one read.
- A transition costs a transaction, a few more round trips than a single-document write. Only a quarantined instance and
  a release run them. The live path of an `ACTIVE` instance stays one write. A release also reads in a transaction, once
  for every record it replays.
- A `SpringMongoSagaStateStore` built on a `MongoOperations` that isn't a `MongoTemplate`, such as a wrapper or a mock,
  fails at construction.
- A custom store that implements `SagaStateStoreSkippedInputs` has to write the instance and its records atomically. One
  without transactions across two tables or collections can keep the records in the instance's row or document, with
  the size limit ADR 178 rejected for MongoDB.

## Rejected alternatives

- **Keep the separate writes and raise the instance writes to majority.** That closes the failover loss in the Context,
  and keeps the three-write skip, the five kinds of leftover record and the race of point 3 step 3, each with a rule
  that only reading the protocol shows to be correct.
- **Put the records back in a list on the instance's document.** One document is atomic without a transaction, and it
  grows toward MongoDB's 16 MB limit, which ADR 178 rejected.
- **Refuse or warn inside a caller's transaction.** See point 4. That holds for the reads and writes a saga makes, and
  not for the release's paired read, which point 4 refuses.
- **Check the read concern of a caller's transaction, and refuse only one that isn't `snapshot`.** Spring keeps the
  `ClientSession` where the store can't read it on a template from `withSession`, so the check would need a refusal for
  that case anyway, and it would pass on a test replica set for a transaction that reads two moments on a sharded
  cluster.
- **Read the pair outside the caller's transaction, in a session of the store's.** That read can't see what the
  caller's transaction wrote and hasn't committed, a release's own write for the record before included, so the next
  write of the release expects a version the transaction already moved past and loses every attempt.
- **Read every empty snapshot again at `linearizable`, a bounded number of times, and throw when the attempts run
  out.** Each new snapshot can be from before the linearizable read for the same reason as the first one. Passing the
  time of the linearizable read to the new transaction gives a snapshot after it on the first attempt.
- **A new session for every attempt of the linearizable read.** It also keeps the time of a failed attempt out of the
  next one, but the store would have to keep the session of the attempt that succeeded and close every other one. One
  session that isn't causally consistent gives the same reads.
- **Tell the store's refusal from a decoding failure by its message.** A message is for a person, and a store of your
  own words its own. A type the SPI names is what a store throws and what the release checks.
