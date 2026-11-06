# 167. A DCB read reports marker versions as the first operation of a transaction the service opened

Date: 2026-11-06

## Status

Accepted. Amends [ADR 148](0148-dcb-snapshots-resume-from-marker-versions-not-positions.md), which hasn't shipped in a
release, so its section on the read before the executor's transaction points here.

## Context

[ADR 148](0148-dcb-snapshots-resume-from-marker-versions-not-positions.md) lets a DCB read report marker versions, and a
snapshot decider saves at them. A Mongo store reports none when the read runs in a transaction. The Spring store checks
`isActualTransactionActive()`, the native store checks the `ClientSession` bound on the thread, and the reactor store
checks the transaction context of the subscriber. Inside one, a read after saved versions throws
`DcbSnapshotInvalidException`, and a read without a base reports no versions.

The store refuses because it can't tell what ran in the transaction before the read. A read in a transaction sees that
transaction's own uncommitted writes. Versions that count an increment the transaction later aborts are handed out
again to other events, and a snapshot saved at them skips those events. ADR 148 shows that sequence for a caller's
transaction that aborts after the decider saved. The `@Snapshot` view saves at the versions of its read with no append
of its own, and a read in the dispatch transaction of a synchronous subscription would count the events that
transaction just appended.

So a `GenericDcbApplicationService` with synchronous subscriptions reads and decides before its executor's transaction
when the command asks for marker versions, which a snapshot decider's always does. That has three costs a plain command
no longer pays:

- A write the domain function makes before it throws stays stored, since no transaction holds it.
- A transient failure of the read doesn't get the executor's retry.
- On an executor that opens no transaction, a handler that fails doesn't undo the append.

A transaction the service's executor just opened has had nothing run in it when the service reads. That is the case
this decision handles. A transaction the caller owns stays a known limitation, tracked in
[#1307](https://github.com/johanhaleby/occurrent/issues/1307).

## Decision

### The service tells the store

`DcbReadOptions` gets two more components, and both imply that the read asks for versions.

- `confirmsAppendBeforeSaving`, set with `confirmingAppendBeforeSaving()`. The caller saves no snapshot at the versions
  this read reports, and saves at an append's versions only after `DcbAppendConfirmation` confirms that append. A
  snapshot decider does that, as ADR 148 describes. The `@Snapshot` view saves at the versions of its read, so it
  doesn't set this.
- `firstOperationOfTransaction`, set with `asFirstOperationOfTransaction()`. The caller opened the transaction the
  read runs in and nothing has run in that transaction yet. It implies `confirmsAppendBeforeSaving`, since the safety
  argument below rests on the confirmation too.

When `firstOperationOfTransaction` is set and the store finds a transaction active on a replica set, the store reads
the marker documents, the consistency token, the position counter and the events in that transaction, and reports
versions from them. It doesn't open a session of its own and doesn't commit anything, and the transaction commits with
the options the executor uses. Without it, a read in a transaction reports no versions as today. Anywhere but a replica
set, `mongos` for example, a read in a transaction reports no versions with it too, since the safety argument below
rests on how a replica set reads a transaction.

The blocking and the reactive `GenericDcbApplicationService` read and decide in the executor for every command, as in
0.34.0. With synchronous subscriptions they set `confirmsAppendBeforeSaving` on every read that asks for versions, and
`firstOperationOfTransaction` as well when `inTransactionTellingIfOpened` says the attempt opened its transaction. The
read is the first thing the action does, and an executor's conflict retry runs the action again in a new transaction,
so the read is the first operation of each attempt. A command whose executor joins a transaction the caller owns reads
without `firstOperationOfTransaction` and gets no versions, as today. An executor on `PROPAGATION_REQUIRES_NEW` in the
caller's transaction opens one of its own, so its read gets it.

Nothing else sets either component. The `@Snapshot` view, the catch-up, the re-read of the written block and the read
inside `appendedVersions` don't.

### A Spring scope with synchronization and no transaction

A Spring executor on `PROPAGATION_SUPPORTS`, `PROPAGATION_NOT_SUPPORTED` or `PROPAGATION_NEVER` with no transaction of
the caller's opens no transaction, so the service doesn't set `firstOperationOfTransaction` there. Its
`TransactionTemplate` makes transaction synchronization active, though. The Spring MongoDB stores set
`SessionSynchronization.ALWAYS`, so a read through their template there binds a session with a transaction to the
synchronization. An append in the same synchronization joins it, and the synchronization commits or rolls it back
when the executor returns. A plain command has read that way since 0.34.0, so a failing handler undoes its append.

The Spring stores now read through the template when `confirmsAppendBeforeSaving` is set, synchronization is active,
no transaction is, no session is bound for the store's database factory yet, and the store runs on a replica set. The
read is then the first operation of the session transaction it binds, and it reports versions from it. When a session
is already bound, for example by an earlier read in the same scope, such a read reports no versions, or is refused
after saved versions, as inside a transaction. A read on a session of its own wouldn't see that session's writes, and
an append checked against its token could fail its condition on them.

A read without `confirmsAppendBeforeSaving` takes ADR 148's path in that scope. That covers the `@Snapshot` view and a
read of the caller's in a method on `PROPAGATION_SUPPORTS`. Their versions come from a read-only transaction committed
at majority, and they may save at them. With a session already bound, such a read doesn't see that session's writes,
and an append in the session checked against its token can fail its condition, as it could before this change.
Versions from the session transaction come from a transaction that hasn't
committed when the read returns, at the session's default read concern.

In a scope of the caller's with no transaction, an executor that opens none takes part in that scope's
synchronization, so the session transaction the read binds commits only when the caller's scope ends, after `execute`
has returned. The confirmation doesn't find the append then, and a snapshot decider there saves no snapshot. The
session stays bound until that scope ends, as after a plain command, and a MongoDB transaction manager on the same
database factory counts it as an existing transaction there.

The Spring stores retry a write only in a transaction they own. A session bound for their database factory counts as
one they don't, so a DCB append in it that conflicts fails at once instead of being retried in the transaction the
conflict aborted.

### Why the versions are safe to save

1. **No write of the transaction is counted.** The read is the transaction's first operation, so its snapshot holds no
   increment the transaction itself made. What it reports comes from other transactions only.
2. **The read is one point in time.** On a replica set WiredTiger reads every operation of a transaction from one
   snapshot, taken at its first operation, so the marker documents, the token, the counter and the events describe the
   same state. The store's own read-only transaction reads at `snapshot`, where MongoDB promises that. At `local`, which
   the executor's transaction can use, the documentation says only that reads might come from one snapshot. If an event
   newer than the markers did reach the read, it matches the criteria and so shares a query marker with it. The append
   increments every query marker, so its write conflicts with that event's increment and the attempt aborts.
3. **The decider saves only at an append's versions, after confirming the append at majority.** It saves at the
   versions the append reported, which come from a read inside the append's transaction after the append's own writes.
   ADR 148 already accepts that, because the decider reads its append back at read concern majority outside any session
   before it saves, and skips the save when the append isn't there. A read in the same transaction, before the append,
   adds no state the append's own read of its versions didn't already have.
4. **A snapshot that wasn't majority-committed can't be saved.** The executor's transaction can read at read concern
   `local`, so its snapshot may hold a write that a failover rolls back. The append commits later in the same
   transaction. MongoDB aborts a transaction that is open when its primary steps down, so the snapshot and the commit
   come from one primary's oplog, and the commit's entry is after the snapshot's point. When the confirmation finds the
   append at majority, every entry up to its commit is majority-committed, the snapshot's point included. When it
   doesn't, nothing is saved.
5. **What the safety needs is no DCB write of this transaction before the read.** The read being the first operation
   gives that, and so does an executor that does no work of its own in the transaction before the action. The positions
   of a DCB write are reserved outside the transaction, so a read after such a write sees a counter below them, misses
   its events and counts its marker increments. An executor that reports opening a transaction it joined is caught, since the
   append in that transaction isn't committed when `execute` returns, so the confirmation doesn't find it and the
   decider skips the save. An executor that opens the transaction, appends to the boundary before the action and
   commits before `execute` returns is not caught, and the snapshot skips that append's events. The Spring executors
   and `NativeMongoTransactionExecutor` run nothing before the action.

Point 2 rests on how WiredTiger reads a transaction, and on the write conflict when that fails, and point 4 on MongoDB
aborting open transactions on a stepdown, which its documentation says for `rs.stepDown`, and replicating the oplog in
order. Neither is tested here. Point 3 is the argument ADR 148 already relies on.

### The read before the executor goes

With the store reporting versions in a transaction the service opened, nothing needs the read before the executor.
`allowsReadBeforeTransaction()` on `TransactionExecutor` and `ReactiveTransactionExecutor`, which isn't in 0.34.0, is
removed with its overrides in `noTransaction()`, `NativeMongoTransactionExecutor`, `SpringTransactionExecutor` and
`SpringReactiveTransactionExecutor`. The catch in the blocking service's action keeps its role from ADR 148 for a
transaction the executor joined.

### Tests

- `DcbReadOptionsTest` covers what sets each component, that `firstOperationOfTransaction` implies
  `confirmsAppendBeforeSaving`, that the withers keep them, and that each is refused without what it needs.
- The marker-version test of the native, the Spring and the reactor MongoDB store gets a read with
  `firstOperationOfTransaction` as the first operation of a transaction the test opens. It reports versions, and a read
  after them returns only the newer events.
- Each of those stores gets a test that this read runs in the transaction. The test appends in a transaction it opens
  and then reads with the option, and the versions count the append's marker increment, which a read on a session of
  the store's own doesn't see. That breaks the option's contract on purpose.
- Each Spring store gets a test for the scope with synchronization and no transaction. A read with
  `confirmsAppendBeforeSaving` reports versions, and an append after it in the scope is gone when the scope throws. A
  read without it reports versions too, and the append stays. A second read with it, after an append in the scope, reports
  no versions, and an append with its token succeeds.
- `SnapshotDcbDeciderInCallerTransactionTest` and its reactive twin keep the caller's transaction cases. Their case where
  the executor owns the transaction asserts the snapshot is saved with the read in the transaction.
- One test per gap over a Spring MongoDB store, for a snapshot decider. On the blocking stack a write of the domain
  function rolls back. On both stacks a transient read failure gets the executor's retry and the snapshot is saved, and
  a failing handler on `PROPAGATION_NOT_SUPPORTED` undoes the append. The reactive service's domain function is
  synchronous, so the first gap has no reactive test.

## Consequences

- A snapshot decider on a service with synchronous subscriptions reads and decides in the executor's transaction, as a
  plain command does. On a replica set it still saves snapshots when the executor opened that transaction, and on a
  Spring executor that opens none outside a scope of the caller's. It pays none of the three costs in the context that a plain command
  doesn't pay.
- On a replica set, in a scope of the caller's with no transaction, on an executor that opens none, a snapshot decider
  saves no snapshot.
- On a sharded cluster a read in the executor's transaction reports no versions, so a snapshot decider there saves no
  snapshot and the store refuses a snapshot saved earlier.
- The versions of such a read come from the executor's transaction, not from a read-only transaction committed at
  majority. The bound `markerVersionsCommitTimeout` doesn't apply to it. Whether the save is safe now rests on the
  confirmation, as the save after an append always did.
- In a caller's transaction nothing changes, and #1307 stays open.
- `DcbReadOptions` has two public components a caller of the store can set wrongly. The javadoc states their contract.
  The confirmation catches a decider's read in a transaction it only joined, and not one in a transaction that committed a DCB write before the read, or a caller that saves at the versions without a
  confirmation.
- `allowsReadBeforeTransaction()` is gone before it shipped, and the paragraphs of ADR 148 and the changelog about the
  read before the executor are replaced.
