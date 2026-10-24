# 148. A position reader stops at the complete-through position

Date: 2026-10-23

## Status

Accepted. Part of [#1230](https://github.com/johanhaleby/occurrent/issues/1230) and
[#1297](https://github.com/johanhaleby/occurrent/issues/1297). Applies to the three MongoDB event stores
(`SpringMongoEventStore`, `MongoEventStore` and `ReactorMongoEventStore`), to the snapshot DSL on both stacks, to the DCB
`@Snapshot` view and to the position catch-ups. Amends [ADR 61](0061-first-class-snapshot-support.md), whose DCB snapshot
is saved at the last position of the command's own append. Extends [ADR 84](0084-what-a-position-guarantees.md) with the
one position a reader may use as a watermark.

## Context

[ADR 84](0084-what-a-position-guarantees.md) says a position is not commit order. The MongoDB stores reserve a block of
positions with a `findAndModify` on the counter document before the append transaction starts, so append A can reserve
41, append B can reserve 42, and B can commit first. A reader that reads at that moment sees 42 and not 41.

That's harmless for a reader that only orders events. It loses events for a reader that stores the highest position it
has seen and later reads only what comes after it:

- `SnapshotDcbDeciderApplicationService` and its reactive twin save the DCB snapshot at
  `DcbAppendResult.lastSequencePosition()` and resume with `fromPosition(version)`. In the scenario above the snapshot is
  saved at 42 and the next command never reads 41.
- The DCB `@Snapshot` view treats every delivered event at or below its saved version as a redelivery, so 41, delivered
  after 42, is skipped.
- A position catch-up that resumes from a position-only checkpoint (a user-supplied `GlobalCheckpoint.of(p)` or
  `DcbStartAt.afterPosition(p)`, a 0.34 checkpoint without a live start, or a no-live catch-up restarted after a crash)
  replays from p and never sees an event below p that committed after p was stored. #1297 is the stream side of this.

#1230 links a reproduction that holds one append between its reservation and its transaction. The held append commits
below a head a reader already saw, on all three stores, and the snapshot decider ends with a balance of 7 where 12 is
right.

The append condition is not affected. Its consistency token is made of marker versions bumped inside the append
transaction ([ADR 21](0021-dcb-write-path-query-scoped-concurrency.md)), not of positions.

The property the fix has to give is: **no event commits at or below a position that a reader was told is complete.**
I call that position the complete-through position, CT.

## Decision

**The store keeps a list of reservations that may still commit, and a reader's CT is just below the lowest of them.**
The counter document gets an `inflight` array next to `position`. The reservation is one pipeline `findAndModify` that
increments `position` by n and appends `{last, n, at}` to `inflight` in the same write. An entry's first position is
`last - n + 1`.

**A reservation is resolved when a fence exists for its first position.** A new collection, named after the position
collection with a `-fences` suffix, holds documents `{_id: <first position>, kind: "commit" | "abort", at}` with a unique
`_id`.

- The append transaction inserts the `commit` fence as its first write, so the fence commits together with the events or
  not at all.
- Every failure exit (a failed append condition, a failed write condition, exhausted retries, an unknown commit result, a
  cancelled reactor append) inserts an `abort` fence outside the transaction and outside any caller session, with
  write concern majority.
- Once either fence exists, no transaction can insert the other, because the `_id` is unique. A block whose `abort` fence
  exists can never commit.

**CT is computed from the counter document and the fences.** A reader reads the counter document, then the fences for the
first positions of its entries. CT is `first - 1` of the lowest entry without a fence, or `position` when every entry has
one. When an entry has no fence, the reader reads the counter document again and treats an entry that is gone by then as
resolved. These reads, and the events read that follows them, run with read concern majority in one causally consistent
session, so no read sees an older state than the one before it, even when they go to different replica set members.
Without the session, the second counter read could come from a member that hasn't seen an entry's reservation yet, and
the reader would count that entry as gone. An events read from such a member could miss events below CT. Reading CT never
writes.

**An append learns its own CT inside its transaction.** In the same snapshot as the append condition check, the
transaction reads the counter document and the fences of its entries. If its own entry is missing, it aborts and takes a
new block. `DcbAppendResult.completeThroughPosition()` is the append's last position when every entry below its own block
has a fence in that snapshot, and the CT that snapshot gives otherwise. Each attempt, including a retry with the same
block, starts over with the fence insert and this check.

**Only a reservation removes an entry. Only appenders and `reapStaleReservations()` write fences.**

- Each reservation also drops from `inflight` the entries its appender saw fenced in its previous in-transaction read. That
  keeps the counter document at one write per append, as today.
- An appender with no previous read, on its first append or after a restart, reads the counter document and the fences
  with read concern majority before it reserves, and that reservation drops what the read saw fenced. Without it, a store
  whose writers each append once would never drop an entry, and the counter document would grow toward MongoDB's 16 MB
  document limit.
- After its own commit, an appender inserts an `abort` fence for any entry without a fence that is older than 120 seconds
  (server time). A duplicate key means that append committed.
- After its own commit, an appender also deletes fences that no entry can need any more. It reads the counter document
  with read concern majority, takes `position` as P and the entries' first positions as F, and runs
  `deleteMany({_id: {$lte: P, $nin: F}})`. Fences have no TTL.
- `reapStaleReservations()` on the store does the 120-second step on its own, outside any transaction and session. It
  needs write permission.
- A write on a fence `_id` that an open transaction has already inserted waits for that transaction to end. On a replica
  set the transaction lifetime limit bounds that wait, but a prepared cross-shard transaction can hold it for as long as
  its coordinator takes. So the 120-second step and the fence delete run with a client-side timeout and are skipped when
  it expires. Skipping is safe, because an entry without a fence only holds CT back.

**A fence that already exists makes the owner take a new block.** The stores retry an append with the same block for a long
time. The DCB paths make up to 15 attempts, and each one can retry inside the driver's `withTransaction`. The stream
any-version retry has no limit on the Spring and native stores. So a live appender can be fenced at 120 seconds. Its next
attempt then gets a duplicate key on its `commit` fence. That duplicate key is checked before every same-block retry, and
the append takes a new block. After 3 new blocks in one call, the call throws an exception saying nothing was written.

**An unknown commit result is settled by the fence, the entry and the events.** After an unknown commit result or a
cancellation, the store inserts the `abort` fence and then reads, all in one causally consistent session with write
concern and read concern majority. Every read then waits until the majority-committed state covers everything the insert
saw.

- When the insert gets a duplicate key, the store reads that fence. A `commit` fence means the events were written. An
  `abort` fence means nothing was written, and the unique `_id` means nothing can be.
- When the insert succeeds and the own entry is still in `inflight`, nothing was written. The entry was never dropped, so
  the fence cleanup never deleted a fence for it, so no `commit` fence existed, and the `abort` fence now stops one.
- When the insert succeeds and the entry is gone, a fence existed and the cleanup deleted it, either a `commit` fence or
  an `abort` from the 120-second step. An event with the append's own `appendId` anywhere in the block means the events
  were written. Without one, the outcome is unknown, because `deleteEvent` or `delete(Filter)` may have removed them.

Without the session, a commit the primary has applied but not yet replicated could be missing from these reads, and a
`commit` fence the primary has applied can still roll back after a failover. When the store can't settle the outcome
within a bound, including when the `abort` insert itself times out behind the transaction, it throws an exception saying
the outcome is unknown, and the 120-second step fences the entry later.

**In a caller's transaction, nothing changes but the session.** The reservation is a plain `$inc` inside the caller's
transaction, with no entry and no fence, so the counter write commits together with the events. The native store now
reserves through the ambient session too, as the Spring store already does. Concurrent appends in native ambient mode then
conflict on the counter document and abort the caller's transaction.

**The store refuses to run without majority read concern.** Everything above depends on reads at read concern majority
not seeing writes that later roll back. MongoDB 5.0 and later always have it. Before 5.0 it can be turned off. On first
use a store that writes positions runs `buildInfo`. Below 5.0 it also requires `storageEngine.supportsCommittedReads` from
`serverStatus` to be `true`. If that's false, or `serverStatus` is not permitted, the store throws an exception naming the
precondition before any append or read runs.

**Every reader that resumes from a position uses CT.**

| Reader | Before | After |
| --- | --- | --- |
| Snapshot DSL decider, both stacks | saves at `lastSequencePosition()` | saves at `completeThroughPosition()` when it equals the last position, and skips the best-effort save otherwise |
| DCB `@Snapshot` view, both stacks | applies events up to the delivered position and saves there | reads `between(version, delivered position)`, takes CT from that read, applies the events up to the lower of CT and the delivered position, and saves there when that is above the saved version |
| Position catch-up, no live phase | replays to the head and stores `position:N` | still replays to the head, but reads each window together with CT in one call, caps it at CT and waits for CT to pass the cursor |
| Position catch-up, with a stored live start | unchanged ([ADR 144](0144-a-catch-up-checkpoint-keeps-the-live-start-it-read-before-the-replay.md)) | unchanged |

A no-live catch-up waits at most 20 minutes for CT to move. Before it gives up it calls `reapStaleReservations()` once,
and then it throws an exception naming the entry that holds CT back.

`readInPositionOrder` keeps clamping at the head. Clamping it at CT would let a live-mode replay move its cursor past the
events between CT and the head and never read them.

**CT and the events it bounds come from one call.** A caller that reads `completeThroughPosition()` and then reads events
makes two calls, and the second can go to a member that is further behind and miss events at or below CT. So
`PositionOrderedReader` gets a read that returns a window capped at CT together with that CT, from one causally consistent
session, and the position catch-ups read through it. A DCB read already returns its CT in `DcbEventStream`, from the same
session as its events, and the decider uses the value from its own transaction. The standalone `completeThroughPosition()`
is for resume points and for waiting, and its javadoc says it doesn't bound a separate read.

**The public API gets one new component and two new methods.** `DcbEventStream` and `DcbAppendResult` gain
`completeThroughPosition`, and `PositionOrderedReader` on both stacks gains `completeThroughPosition()` and the capped
read. All three types
shipped in 0.34, so a store outside this repository may construct or implement them, and a default can't know whether that
store commits in position order. So the defaults claim nothing:

- The `DcbAppendResult` and `DcbEventStream` compatibility constructors set the new component to 0. The snapshot decider
  then skips its save, and nothing that resumes from a `DcbEventStream` moves.
- `completeThroughPosition()` and the capped read throw `UnsupportedOperationException` unless the store overrides them.

The MongoDB stores, `InMemoryEventStore` (which assigns and commits positions under one lock) and the adapters in this
repository override both. The event store TCK gets a conformance test that holds a lower append while a higher one commits,
and checks that CT stays below the held append until it commits or aborts.

## Alternatives considered

**Treat an old entry as resolved.** The first version of this design let a reader count an entry older than 10 minutes as
resolved, with the appender aborting itself past 5 minutes. That only holds while `transactionLifetimeLimitSeconds` plus
the clock skew between primaries stays under 5 minutes. The setting is per node and can change at runtime, and a prepared
cross-shard transaction is exempt from it. A prepared transaction that stalls for 6 minutes commits after a reader aged its
entry out, and that event is lost. I didn't want safety to depend on a setting the store can't control.

**Remove the entry after commit.** A second `$pull` on the counter document after every commit doubles the writes on the
one document every append already writes, makes appends in a caller's transaction conflict with it, and can't work in
native ambient mode, where the caller commits after `append` returns.

**Reserve inside the transaction.** Commit order then equals position order, but every append write-conflicts on the
counter document and the appends run one at a time. That's the contention [ADR 84](0084-what-a-position-guarantees.md)
reserves outside the transaction to avoid.

**A post-commit finalizer.** The append writes events without a position and a finalizer assigns positions in commit
order. That changes what a subscription receives (the position arrives in an update, not the insert) and what
`DcbAppendResult.lastSequencePosition()` means.

**What other event stores do.** I compared with three, from research done for this release:

- Axon Framework's JPA and JDBC stores track gaps with `GapAwareTrackingToken` and give up on one once a later event's
  timestamp, set by the client, is older than a timeout (60 seconds by default in `JpaEventStorageEngine`). That's the
  age rule above, on the reader side only, and an event that commits after the timeout is skipped without an error.
  AxonFramework#4829 reported this and was closed as not planned.
- Axon Server has one sequencer per replication group, so it has no gaps to resolve.
- Marten replaced a 3-second stale-gap skip with transaction-evidence gap skipping (`UseTransactionEvidenceForGapSkipping`).
  That's the idea of resolving a gap by evidence, as here, but nothing stops a late commit.

A PostgreSQL store can get an exact watermark from `pg_snapshot_xmin`. MongoDB has nothing like it, which is why this
design writes the fence itself.

## Consequences

- No event commits at or below a CT a reader observed. The fence insert and the own-entry check guarantee this on every
  store, in every mode, without a timeout or a server setting. The one precondition is majority read concern, which the
  store checks.
- Per append, the counter document still gets one write, but a heavier one, since the pipeline rewrites and returns the
  `inflight` array. That's a handful of entries normally. The append also does one fence insert, two reads inside the
  transaction and a share of the batched fence deletes. A failed append adds one fence insert. An instance's first
  append, and its first after a restart, adds a counter read and a fence query. Nothing writes a shared document that
  isn't written today.
- A reader call adds one fence query, and one more counter read when an entry has no fence.
- A crashed append holds CT back for 120 seconds while other appenders run. With no appender running, it holds CT back
  until `reapStaleReservations()` runs. A no-live catch-up calls that itself, so it only stays stuck where it may not
  write, and then it fails with an exception instead of waiting for ever.
- A DCB snapshot can be saved at a lower position than before, or not at all for one command. The next command then
  reads a few more events. A DCB `@Snapshot` may not include its newest event until the next delivery.
- Concurrent appends in native ambient mode now conflict on the counter document, as Spring appends in a caller's
  transaction already do.
- An append whose commit result the store can't settle now throws an exception saying the outcome is unknown. The caller
  has to find out from the events whether it was written before it retries.
- A store outside this repository that implements `PositionOrderedReader` and doesn't override the two new methods can't
  run a no-live position catch-up any more. The catch-up stops with the `UnsupportedOperationException` until the store
  implements them. A DCB store outside this repository that builds `DcbEventStream` with a compatibility constructor gets
  CT 0, so a DCB `@Snapshot` view on it never saves. The 0.35.0 changelog and migration guide have to name both.
- A 0.34 writer still reserves with a plain `$inc` and adds no entry, so CT can move above one of its appends before
  that append commits. The guarantee holds once every writer runs 0.35. DCB snapshots and position-only checkpoints written by 0.34 may already be past a lost event. The migration
  guide says to delete DCB snapshots, and that the store can't repair a position-only checkpoint.
- The constants are fixed and named here, so making them configurable later only adds options: 120 seconds before an
  entry is fenced as abandoned, 20 minutes for a no-live catch-up to wait, and 3 new blocks per append call.
