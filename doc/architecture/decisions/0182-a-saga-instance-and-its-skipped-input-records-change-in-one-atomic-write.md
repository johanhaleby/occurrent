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

`SagaStateStoreSkippedInputs` has no method that writes or deletes a record on its own. It has four transitions, each
a compare-and-set on the instance's version that writes the instance and its records together, or neither:

| Transition | Instance | Records under the quarantine id |
| --- | --- | --- |
| `compareAndSaveSkipped` | raises the skip counter, state left alone | records the input, keeps a record of the same key that is there already |
| `compareAndSaveReleaseStarted` | sets `releasedAt`, state left alone | writes the head record at -1, moving a record of its key |
| `compareAndSaveReplayed` | the whole instance after the replayed input, or one version up when the input was applied already | deletes the replayed record, only while its sequence number is the one the release read |
| `compareAndSaveQuarantineEnded` | the whole instance, `ACTIVE` after the last record or `COMPLETED` by a replayed input | deletes every record |

`SagaStateStore.delete(sagaId)` deletes the instance and the records under its quarantine id in one atomic write too.
Each transition returns `false` on a version conflict and has then written nothing. The reads,
`findSkippedInputs` and `findReleasingSagaIds`, stay as they were.

So records under a quarantine id change only in a write that also changes the instance's version. A release that reads
the instance, then its records, and wins a compare-and-set on the version it read, saw the records as they were at that
version. The release's last write relies on that. It turns the instance `ACTIVE` only when no record was there.

### 2. What this removes

- The three writes of a skip are one transition. The second compare-and-set, the delete of its own record after it
  lost, and point 3 step 3's case of a skip that loses to a release are gone. A skip that loses writes nothing, reads the
  instance again and applies its input live when the instance is `ACTIVE`.
- None of the five kinds of leftover record in ADR 178 point 2 can be written. A crash leaves either both halves of a
  transition or neither, a release that loses its start writes no head record, a completion deletes the records with
  the write that completes the instance, and a delete removes the instance and its records together.

A release still reads the instance again after it reads the first record, and replays that record against the second
read. The compare-and-set would refuse a write based on the first read once another release has applied a record in
between, but the commands of the replay go out before that write.

A record whose input the stream marks show as applied is deleted in a write that raises only the instance's version and
leaves its `updatedAt` as it is. So the fourth case in ADR 178 point 5 stays. The timer poller goes on beside a release
that spends longer than `timerPollInterval` on such records, and neither dispatches anything for them.

The skip counter stays the source of each record's sequence number, and a redelivered input whose key is recorded
already still takes a number that goes unused. `SkippedInput` stays a value of a sequence number and a key, so it can
hold the event's body later without a change to the SPI's methods.

### 3. A transition that returns normally survives a failover

The executor acknowledges an input right after the store call that skipped or applied it returns. So a call that
returns normally has made its write durable.

On MongoDB, `SpringMongoSagaStateStore` runs each transition and a delete of a quarantined instance in a transaction of
its own, which reads at read concern `snapshot` on the primary and commits at `WriteConcern.MAJORITY` with `j: true`.
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
  caller's transaction.
- `compareAndSaveQuarantineEnded`, for an instance the release doesn't complete, reads the records again inside its
  transaction and writes nothing while one is there. A release ends the quarantine after a read of the records finds
  none, and the write deletes every record under the quarantine id, so a record the read missed would be deleted
  before it was replayed. The transaction reads everything the primary has, and a new primary has every write a
  majority acknowledged.

A transaction aborted with the `TransientTransactionError` label runs again through the store's `RetryStrategy`. A
commit that ends with `UnknownTransactionCommitResult` is committed again through the same strategy. The transaction
needs a replica set, and a standalone server refuses it. A server without journaling refuses `j: true`. The store turns
each refusal into an `IllegalStateException` that says what the server lacks. With a strategy built from
`RetryStrategy.retry()`, as the default is, the store throws any other failure of the transaction at once, and throws
each refusal on the first attempt, since every later attempt would be refused too. A strategy implemented another way
runs its own loop and decides what it retries.

The store needs the `MongoDatabaseFactory` to run a transaction, and only a `MongoTemplate` exposes it. The constructors
keep their `MongoOperations` parameter, which 0.34.0 shipped, and refuse anything else than a `MongoTemplate` with an
`IllegalArgumentException` that names the fix.

`InMemorySagaStateStore` keeps an instance and its records as one value and replaces it in one write, so a read
sees the instance and its records as one write left them.

### 4. Inside a caller's transaction, the caller's commit decides

A transition called while a Spring MongoDB transaction on the same `MongoDatabaseFactory` is active joins it, since
the store's transaction propagates as `REQUIRED`. The instance and its records then change together because they are in
that transaction, and they are durable only as far as the commit's write concern makes them. The store doesn't retry a
transition it joined, and a failure reaches the caller's transaction.

Inside that transaction, `find` and `findWithoutState` read at its snapshot, which misses an instance inserted after
the snapshot began. A found instance needs nothing more, since the executor writes it back through a compare-and-set,
and a write inside the transaction to an instance changed after its snapshot fails with a write conflict. An absence
has no such write, so when neither read finds the instance, the store reads it once more outside the transaction, at
read concern `linearizable`. If it is there, the store throws a `MongoException` with the `TransientTransactionError`
label instead of returning empty, and the executor doesn't skip the input. A caller that runs its transaction again on
that label gets a snapshot that has the instance. Refusing a caller's transaction outright would break every saga run
inside one, the case this point is about.

The store doesn't check that write concern. Spring keeps the transaction's `ClientSession` in a package-private
`MongoResourceHolder`, so the store can tell that a transaction is active but can't read the concern it commits at. A
refusal would break every saga run inside a caller's transaction, and a warning would fire on every such call whatever
the concern is. The SPI and the store's javadoc say that the caller's commit decides.

## Consequences

- A quarantined instance's records exist only while it is quarantined under that quarantine id. Nothing is left behind
  to clean up, which settles [#1546](https://github.com/johanhaleby/occurrent/issues/1546) for the stores Occurrent ships.
- Every saga write on MongoDB waits for a majority of the replica set to journal it, up to 10 seconds an attempt. On a
  primary, secondary and arbiter replica set with one data-bearing member down, there is no majority to journal to, so
  saga writes fail until that member is back, where `w: 1` would have succeeded.
- A `MongoTemplate` configured with `w: 1` no longer decides how a saga instance is written. 0.34.0 wrote the instance at
  the template's concern, so this is a breaking change, with a section in the upgrade guide.
- Quarantine on MongoDB needs a replica set, because a skip runs in a transaction. Occurrent's MongoDB subscription
  models need one already for change streams.
- A `find` or `findWithoutState` that finds no instance reads a second time, and that read waits for a majority as a
  write does. The first input of a new instance pays for it, and a found instance costs one read as before.
- A transition costs a transaction, a few more round trips than a single-document write. Only a quarantined instance and
  a release run them. The live path of an `ACTIVE` instance stays one write.
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
- **Refuse or warn inside a caller's transaction.** See point 4.
