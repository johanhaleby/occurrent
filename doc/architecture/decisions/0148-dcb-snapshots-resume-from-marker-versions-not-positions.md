# 148. DCB snapshots resume from marker versions, not positions

Date: 2026-10-24

## Status

Accepted. Fixes [#1230](https://github.com/johanhaleby/occurrent/issues/1230) and settles the 0.35 contract for
[#1297](https://github.com/johanhaleby/occurrent/issues/1297). Applies to the three MongoDB event stores
(`SpringMongoEventStore`, `MongoEventStore` and `ReactorMongoEventStore`), `InMemoryEventStore`, the snapshot DSL on
both stacks and the DCB `@Snapshot` view. Amends [ADR 61](0061-first-class-snapshot-support.md), whose DCB snapshot
version is a position. Amends [ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) on what pruning markers costs.
Extends [ADR 84](0084-what-a-position-guarantees.md) with what a position can't be used for.

## Context

### The defect

The MongoDB stores reserve a block of positions before the append transaction starts. The reservation is one
`findOneAndUpdate` with `$inc` on a counter document, outside the transaction (`MongoEventStore.java:471` reserves,
`:485-487` starts the transaction, and `SpringMongoEventStore.java:426` and `ReactorMongoEventStore.java:359` do the
same). The block is reused when the transaction is retried. So an append can own a position for a long time before its
events are visible, and an append that reserved later can commit first.

[ADR 84](0084-what-a-position-guarantees.md) states this as the contract:

> **A position is not commit order.** Reservation happens before commit, so two concurrent writers can commit in the
> opposite order to their positions.

That's harmless for a reader that only sorts events. It loses events for a reader that stores the highest position it
has seen and later reads only what comes after it. Below, I call such a stored position a watermark.

[#1230](https://github.com/johanhaleby/occurrent/issues/1230) is the DCB case. Take the boundary
`types(Deposited, Withdrawn).tags(account:a1)`:

1. A deposit appends `Deposited(a1, 5)` without an append condition. It reserves position 41. Its first transaction
   attempt aborts on a transient error, and it backs off before it retries.
2. A withdraw runs `Withdraw(a1, 3)` through `SnapshotDcbDeciderApplicationService`. It reads the events after the old
   snapshot version, decides, reserves 42 and commits. Its append condition passes, because nothing matching had
   committed since its read.
3. The service saves the new snapshot with version 42, which is `DcbAppendResult.lastSequencePosition()`
   (`SnapshotDcbDeciderApplicationService.java:161`).
4. The deposit retries and commits at 41.
5. The next command reads with `fromPosition(42)` (`SnapshotDcbDeciderApplicationService.java:145`) and never gets 41.
   Its consistency token doesn't help, because 41 committed before that read.

From then on every decision on `account:a1` uses a balance that is 5 too low, and every later snapshot starts from that
state. A reproduction that holds one append between its reservation and its transaction ends with a balance of 7 where
12 is right. The reactive service saves the same way (`ReactiveSnapshotDcbDeciderApplicationService.java:166`, `:188`).

The DCB `@Snapshot` view has the same defect from the other side. It treats every delivered event at or below its saved
version as a redelivery (`SnapshotAnnotationRegistrar.java:186`), so the deposit at 41, delivered after the withdraw at
42, is skipped as if it had already been applied to the view's state.

[#1297](https://github.com/johanhaleby/occurrent/issues/1297) is the store-wide case, for catch-up subscriptions that
resume from a position:

1. A catch-up subscription reads past 42 while a stream write that reserved 41 is still delayed, and the application
   stores 42 as `GlobalCheckpoint.of(42)`.
2. The stream write commits at 41.
3. The application restarts and resumes from that checkpoint. The catch-up reads a new live start, which is the point in
   the change stream it will follow once the replay is done, and replays the events after 42.
4. The replay doesn't read 41, because 41 is below 42. The change stream doesn't deliver it either, because 41 committed
   before the new live start.

A catch-up that stored its live start together with the position
([ADR 144](0144-a-catch-up-checkpoint-keeps-the-live-start-it-read-before-the-replay.md)) doesn't have this problem,
because it keeps the live start the earlier attempt read before 41 committed. It happens when the position is stored
alone. That's a position the application stored itself, a checkpoint written by 0.34 or earlier, or a checkpoint stored
during a catch-up that only replays and never goes live.

[#1298](https://github.com/johanhaleby/occurrent/issues/1298) has the same defect in the saga position watermark. It's
fixed separately and this ADR doesn't change it.

The append condition is not affected. Its consistency token is the sum of the versions of marker documents that the
append increments inside its transaction ([ADR 21](0021-dcb-write-path-query-scoped-concurrency.md),
`MongoEventStore.java:524-550`, `:580-605`), not a position. ADR 21 already explained why a condition phrased over
positions is unsound. The snapshot decider and the view were written later and used a position as their version anyway.

### Designs we tried

Seven designs came before the decision. Each one is described with how it works, the interleaving that broke it or the
cost that ruled it out, and why I dropped it.

#### 1. Reserve the position outside the transaction (the behaviour this ADR fixes)

Before ADR 21, the DCB append reserved its block inside the transaction. Concurrent appends then all wrote the one
counter document, and MongoDB raised a `TransientTransactionError` that aborted the whole transaction. ADR 21 moved the
reservation out, so the counter became one atomic update that MongoDB serializes without a transaction conflict, and the
positions of a failed append are never used. [ADR 84](0084-what-a-position-guarantees.md) then wrote down what that
costs. A position is not commit order, and `currentPosition()` "is a high-watermark, not a fence".

ADR 21 replaced positions with marker versions for the append condition. The snapshot decider and the `@Snapshot` view
([ADR 61](0061-first-class-snapshot-support.md)) kept using a position as their version. ADR 61 even argued that a DCB
snapshot needs no check for being ahead of the head, because a DCB version "is a global, monotonic position that never
resets". That's true for the head of the store. The defect is a position below the version whose event isn't visible yet
when the snapshot is saved. The two walkthroughs above are what that costs.

#### 2. Re-read an overlap behind the saved position

The cheapest fix is to resume from the snapshot version minus some margin, say 100 positions, so a late commit just
below the version is still read.

That fails in two ways. The first is that no margin is long enough. The deposit reserves 41 and its process stalls (a
long GC pause, a network partition, a transaction stuck in its retries). 150 other appends commit meanwhile, and the
withdraw saves its snapshot at 192. The next command resumes from 92. The deposit then commits at 41, and every later
command misses it. The stores retry an append with the same block up to 15 times with backoff between 10 and 500 ms
(`MongoEventStore.java:664-669`), and a paused process adds any amount of time on top.

The second is that the snapshot state doesn't record which events it already applied. Resuming from 92 reads the events
at 93 to 192 again, and the decider applies them a second time. Fixing that needs a set of applied event ids in every
snapshot.

I dropped it because it makes the loss less likely without removing it, and I won't accept a design that can still lose
an event here.

#### 3. Reserve the position inside the transaction

If every append, DCB and stream, writes the counter inside its transaction, positions become commit order. A reader that
sees 42 then knows that 41 either committed before it or never will. Spring's outer-transaction mode already works this
way, since the counter update joins the caller's transaction.

The cost is throughput under contention. Inside a transaction, MongoDB doesn't wait for a document another transaction
has modified. It aborts the second transaction with a `WriteConflict`. Outside a transaction the server retries the same
write internally until it succeeds, which is why today's counter is a busy document but not a source of aborts. So with
the reservation inside, appends A and B both start, A increments the counter, B's increment conflicts and B aborts into
the 10 to 500 ms backoff, and when B retries it may conflict with C. Roughly one append gets through per transaction
latency, and the rest spend their time in backoff. The design search (design 7 below) estimated a ceiling of tens to low
hundreds of appends per second. That estimate is reasoned from commit latency and not measured.

This is the contention ADR 21 removed, and it would also make every append in the store conflict with every other, not
only appends on the same boundary. I rejected it for performance.

#### 4. A complete-through position with an age rule

This was the first design that went through review, and the next three are revisions of it. The idea is a position the
store can promise, called the complete-through position or CT. It is the highest position below which every reserved
block has either committed or can never commit. A reader that only ever stores CT as its watermark can't miss a late
commit.

The store learns which blocks are still open like this:

- The counter document gets an `inflight` array next to `position`. The reservation becomes one pipeline
  `findOneAndUpdate` that increments `position` by n and appends `{last, n, at}` to `inflight` in the same write. An
  entry's first position is `last - n + 1`.
- A new fence collection holds one document per block, with the block's first position as its unique `_id` and a `kind`
  of `commit` or `abort`.
- The append transaction inserts the `commit` fence as its first write, so the fence commits with the events or not at
  all. Every failure exit inserts an `abort` fence outside the transaction. Once either fence exists the other can't be
  inserted, so a block with an `abort` fence can never commit.
- A reader reads the counter document, then the fences for the entries in `inflight`. CT is `first - 1` of the lowest
  entry without a fence, or `position` when every entry has one.
- An appender inserts an `abort` fence for an entry older than 120 seconds that still has no fence, so a crashed process
  can't keep CT down for ever.

The decider had to learn its own CT. A first version saved the snapshot when the CT read after its own commit had
reached its last position. The first review broke that. Append X reserves 41, its marker write loses the write conflict
against the withdraw, the withdraw commits at 42, X retries with the same block and commits at 41 after it. Then CT
reaches 42, and the withdraw saves at 42 without X's event. The fix read the counter and the fences inside the append
transaction, in the same snapshot as the append condition, and saved at the last position only when every entry below
its own block had a fence in that snapshot.

The second review removed a step that deleted the append's own entry with a `$pull` after commit. That doubled the
writes on the counter document, made appends in a caller's transaction conflict on it, and couldn't work in native
ambient mode, where the caller commits after `append` returns. It also found that the native store reserves outside the
caller's ambient transaction (`MongoEventStore.java:471` runs before the ambient session check at `:476-482`).

Fences expired after an hour, so the fence collection wouldn't grow for ever. That hour comes from a draft I no longer
have, so it's unchecked. The third review found that once a fence expires, its entry looks open again. CT then drops
back below it, and on a quiet store a reader waits for ever. The fix was the age rule:

- An entry older than X = 10 minutes counts as resolved, fenced or not.
- An appender whose own entry is older than X - M, with M = 5 minutes, aborts and takes a new block.
- MongoDB aborts a transaction that runs longer than `transactionLifetimeLimitSeconds`, 60 seconds by default. As long
  as that limit plus the clock skew between primaries stays under M, a block whose own check passed commits or dies
  before it turns 10 minutes old.

The fourth review broke the age rule. `transactionLifetimeLimitSeconds` is set per node, can be changed at runtime, and
can't be read on some managed deployments. The review also said that prepared transactions, the two-phase commits
MongoDB uses across shards, are exempt from it, which I haven't checked against the MongoDB documentation. Occurrent
supports `mongos`. The interleaving:

1. Append A reserves 41 and passes its own-entry check at 4 minutes 59 seconds.
2. A's transaction spans two shards, is prepared, and stalls for 6 minutes.
3. At 10 minutes 1 second a reader counts A's entry as resolved and moves CT past 41.
4. A commits at 11 minutes, below a CT the reader already stored.

I dropped it because its safety rested on a server setting the store can't control.

#### 5. Resolve a block by its fence or its first event

The fourth review compared the age rule with this design, called B at the time. An entry counts as resolved when a fence
exists for its first position, or when an event exists at that position (the events collection has a unique index on
position). That removes the age rule, because a committed block whose `commit` fence expired still has its first event
as evidence.

It had two problems of its own. An aborted block has no event, so its `abort` fence must never expire, or CT goes back
below it. B solved that with a conditional `$pull` of the entry on every failure exit and every reaped crash, which is a
new write on the counter document at the failure rate. The second problem is deletes. `deleteEvent` and `delete(Filter)`
exist, so the first event of committed block 41 can be deleted after its `commit` fence expired while its entry is still
in `inflight`. Then 41 has neither fence nor event, and CT drops back to 40.

The fourth review chose B in its fence form, and that became B':

- An entry is resolved only by a fence. The event lookup goes.
- Fences never expire. An appender deletes them in ranges once no entry can need them, which removes both the `$pull`
  and the delete problem.
- `abort` fences are inserted with write concern majority, because a fence written at w:1 can be rolled back after a
  failover, after a reader already used it.
- CT reads always run with read concern majority.
- An appender never counts its own entry as resolved unless its commit returned success, since a commit with an unknown
  result can still go through.

#### 6. B', the fenced complete-through position

The last draft of B' is ADR 148 at commit 1fabe07cb on the branch `rel35/d3-bprime-final`. It kept the `inflight` array,
the fences and the in-transaction check from design 4, without the age rule, and added:

- **Drops.** Each reservation removes from `inflight` the entries its appender saw fenced in its previous in-transaction
  read, so the counter document still gets one write per append.
- **Fence cleanup.** After its own commit an appender reads the counter document at majority, takes `position` as P and
  the entries' first positions as F, and runs `deleteMany({_id: {$lte: P, $nin: F}})` on the fences.
- **A reaper.** After its own commit an appender fences any entry older than 120 seconds, and `reapStaleReservations()`
  does the same on request.
- **Settlement.** After an unknown commit result the store inserts the `abort` fence and reads the fence, the entry and
  the events in one causally consistent session at majority to find out whether the block was written.
- **Timeouts** on the reaper insert and the cleanup, because a non-transactional write on a fence `_id` that an open
  transaction already inserted waits for that transaction.
- **A composite read** on `PositionOrderedReader` that returns a range of events and its CT from one session.
- **Compatibility defaults.** Constructors and default methods that claim nothing for stores outside this repository.
- **A startup check** that refuses to run without majority read concern.

The fifth review looked at the fence cleanup only. It found that after an unknown commit result the `kind` of a fence
can't tell the appender whether it committed, because the cleanup may have deleted its `commit` fence and let a new
`abort` fence in. The store had to look for its own `appendId` in the events instead. It found that a reader's CT could
come out lower than the CT the same reader saw a moment earlier, when a drop and a cleanup ran between its counter read
and its fence read, which costs no event but breaks any caller that assumes CT only grows. It found that the own-entry
check had to run again on every retry of the same block. And it found that MongoDB before 5.0 can run with majority read
concern turned off, so the store needed the startup check.

The first round of review on the pull request found four more:

- The ADR had dropped the requirement that the counter read, the fence read, the second counter read and the events read
  share one causally consistent session. Without it, the second counter read can go to a member that hasn't seen a
  reservation yet and count the entry as gone, and the events read can go to a member that is further behind and miss
  events below CT.
- An `abort` fence insert on a block whose transaction is open waits for that transaction. On a replica set the
  transaction lifetime limit caps the wait, but a prepared cross-shard transaction can make it wait as long as its
  coordinator takes.
- Settling an unknown commit with a majority read can miss a commit the primary has written but hasn't replicated yet,
  and the store then reports "nothing written" for a block that later becomes visible. The mirror case loses events. A
  `commit` fence the primary has written can roll back after a failover, while the store already reported success.
- The compatibility defaults claimed a CT. A store outside this repository that commits out of position order would have
  reported its head as complete.

The second round found four more:

- An entry is dropped only by an appender that saw it fenced in its previous read, and a process's first append has no
  previous read. With writers that append once and exit, nothing is ever dropped, and the counter document grows toward
  MongoDB's 16 MB document limit.
- When an append's commit response is lost, `deleteEvent` can remove the block's events and the cleanup can delete its
  `commit` fence. The settlement's `abort` insert then succeeds, the event lookup is empty, and the store says nothing
  was written for a block that was.
- CT was a bare `long`. A caller that reads CT and then reads events makes two calls with no shared session, so the
  second call can go to a lagging member and miss events at or below CT.
- My fix for the compatibility default in the first round set CT to `firstSequencePosition - 1`. Append A holds 41,
  append B commits at 42 through a store that uses the compatibility constructor, and B reports CT 41 while A can still
  commit at 41.

The sixth review stopped B'. Its blocker was that drops rested on fences seen in reads inside the append transaction,
and those reads aren't durable unless the transaction commits with write concern majority. An append's own entry wasn't
excluded from what a reservation drops either. Two interleavings lose an event:

1. Append A's commit at 41 is prepared across shards and its result is unknown to A. A's next reservation drops its own
   entry, a reader sees CT at 41 or above, and A's commit then goes through at 41.
2. Append B's transaction reads A's `abort` fence for 41 before that fence is majority-committed. The primary fails
   over, the fence is rolled back, and A's `commit` fence insert succeeds on the new primary. B's next reservation drops
   41 because B saw it fenced, CT passes 41, and A commits at 41.

It also found five major problems. A CT read inside a caller's transaction joins that transaction and sees the caller's
uncommitted counter increment, which has no entry. `reapStaleReservations()` can't be reached through
`PositionOrderedReader` or `DcbEventStore`, so behind an adapter nobody can call it. `PositionBackfill` reserves a block
and then writes positions with `updateOne` outside a transaction (`PositionBackfill.java:159`, `:165`), and the ADR
didn't cover it. A compatibility CT of 0 can't be told apart from an actual CT, so a DCB catch-up that only replays
stalls on such a store for its full 20 minutes before it throws. And an open native ambient transaction blocks every
default-mode reservation in every process, up to the transaction lifetime.

Every finding across the six reviews and the two pull request rounds falls into one of five kinds:

- **Evidence that can roll back.** An entry is resolved, dropped or settled because of something read at a level that a
  failover can undo, such as a fence written at w:1 or a fence seen inside a transaction that isn't majority-committed.
- **The caller's transaction.** A store read or write joins a transaction the caller owns, or waits for one, and sees or
  blocks on something it shouldn't.
- **A mechanism no caller can reach, or a write path the design left out.** The reap method behind an adapter, the
  composite read that didn't exist, and `PositionBackfill`.
- **The compatibility value.** What a store that knows nothing about CT reports, and whether a reader can tell that
  apart from an actual answer.
- **A watermark above what its read returned.** A CT that is higher than the events the caller actually read, such as a
  range read with a limit whose CT covers positions the limit cut off.

The sixth review's view was that the core of B' was sound. A unique fence per block, inserted first in the transaction,
means a block either commits with its fence or is fenced off for good, and no review broke that. What kept failing was
everything around it, and each round found a new path in a kind that was already known. Closing B' would take two rules
written down and applied to every path at once. Nothing resolves, drops or settles an entry except a majority read
outside any transaction in a session the store owns, and every CT handed to a caller is capped at the read that produced
it. The reviewer put the odds of a clean seventh round at low to moderate, and the design now had 12 moving parts for
one bug. I stopped there and asked for a search for something smaller (design 7). The closure itself still went to a
seventh review.

The closure was those two rules and three smaller changes. An append's own entry is excluded from what it drops, the
reap method is on the public reader surfaces, and a store that knows nothing about CT reports none instead of 0. The
seventh review found B' with the closure not sound, with 3 blockers, 3 major findings and 9 minor ones. The closure did
remove the sixth review's two interleavings. The blockers came from applying its rules to every path at once.

**The decider's check moves after commit.** The first rule says nothing resolves, drops or settles an entry except a
majority read outside any transaction. Applied to the decider, it forbids the read inside the append transaction that
the first review's fix rested on, so the decider has to take its CT from a read after commit. That brings the first
review's finding back. With the decider's snapshot at position 40:

1. The decider D reads the events after 40 and the token. Nothing after 40 matches yet.
2. Append X reserves 42, and D reserves 43.
3. X appends an event that matches D's criteria, with no condition. X's event markers overlap D's query markers, so the
   two transactions write-conflict. D wins, and X aborts and keeps block 42.
4. D commits at 43. X retries after its backoff and commits at 42 with its `commit` fence.
5. D's read after commit sees 42 and 43 fenced, so CT is at least 43. D saves at 43 with a state that doesn't hold X's
   event.
6. The next command reads after 43 and never applies 42.

Inside the transaction, D's snapshot has no fence for 42, so the check there reports CT below 43 and D skips the save.
The in-transaction check was right because of where it read, in the same snapshot as the token check. The rule confused
that with durability and gave it up to get durability.

**The token and the events from different points in time.** B' reads the events at majority. Nothing in the closure
moved the consistency token, which all three stores read before the events at the store's default read concern
(`SpringMongoEventStore.java:247`, `MongoEventStore.java:369`, `ReactorMongoEventStore.java:267`). Append M commits a
matching event at 50. The primary has applied it, so a read at local sees it, but it isn't majority-committed yet. Every
commit goes through that state, a w:majority one included. Decider D reads the token at local, which includes M's marker
increment, and then reads the events at majority from a snapshot without 50. D decides without 50. Inside D's append
transaction the store compares the token again (`SpringMongoEventStore.java:538`, `MongoEventStore.java:533`,
`ReactorMongoEventStore.java:394`), it equals the token D read, and the append goes through. That's a lost update on the
append condition, which the B' draft said was unaffected.

**The live start and the replay from different points in time.** A DCB catch-up that goes live takes its live start from
the operation time `hostInfo` returns (`SpringMongoSubscriptionModel.java:278`), which is read at local and is the time
of the latest oplog entry. It then replays through `DcbEventStore.read`, which B' made a majority read. Event E at
position 50 is applied on the primary but not majority-committed yet. The catch-up takes a live start after E. Its head
and window reads at majority come from a snapshot before E, so the replay doesn't return E. The change stream starts
after E, so it doesn't deliver E either. The checkpoint moves past 50, and E is lost.

The three major findings were about caller transactions and a write path. Inside a caller's transaction the two rules
can't both hold for a DCB read. If the events are read in the store's own majority session, the re-read
`GenericDcbApplicationService` does of its own uncommitted block (`GenericDcbApplicationService.java:161-162`) comes
back empty and the synchronous handlers get nothing. If the events are read in the caller's transaction and CT in the
store's session, CT can cover events the read didn't return. So the only consistent answer is no CT inside a caller's
transaction. With synchronous subscriptions, `GenericDcbApplicationService` always runs in the executor's transaction
(`:137`, `:172`), so the snapshot decider would never save a snapshot in that configuration. And `PositionBackfill` was
still not covered.

Two of the blockers are a kind no earlier review had named, which makes six kinds:

- **Values read at different read concerns, combined as if they described one point in time.** A token read at local
  beside events read at majority, or a live start read at local beside a replay read at majority.

I dropped B' on this evidence.

#### 7. The design search after B'

The search started from what the first six reviews had taught, and its first step was to rule out ideas that looked
cheaper than B':

- **CT from the server's view of open transactions** (`$currentOp`, the majority commit point, `replSetGetStatus`). A
  block is reserved before its transaction's first operation reaches the server, and until then the transaction doesn't
  exist server side. Any CT built from server transaction state passes such a block.
- **Writing `$$CLUSTER_TIME` or `$$NOW` on the events inside the transaction.** The value is taken when the statement
  runs, before commit, so two transactions can commit in the opposite order to their times. That's the same defect as a
  position taken before commit.
- **Scanning the oplog from the reservation time to see whether a block committed.** That's a change stream over the
  whole time since the reservation, for every open block, limited by how long the oplog keeps entries. Too expensive per
  read.
- **Reading with `readConcern: snapshot` at a given cluster time.** It gives a consistent read across collections, which
  says nothing about blocks that were reserved and not yet committed at that time.
- **Storing a cluster time with the decider's snapshot and filling the gap from the change stream on the next command.**
  Correct, but the change stream read starts from a time that may be hours old, on every command, and it fails once the
  oplog no longer has that time. The time can only be retired once every transaction open at that point has ended, which
  is the CT problem again. ADR 144 already uses this idea for catch-up resumes, where it fits.

The search also looked at a single sequencer, the way Axon Server works. One leader per replication group accepts every
append and assigns positions in arrival order, so there are no gaps to resolve. On MongoDB that means every Occurrent
append becomes a request to a leader process, which writes the event store. The leader can group appends, so 50 queued
appends become one transaction with positions in queue order, and the append conditions are checked against the leader's
own view of the markers, which is exact as long as it is the only writer. With a commit at majority taking roughly 5 to
20 ms, that is thousands of appends per second from one leader, at tens of milliseconds of latency per append. Those
figures are reasoned from commit latency, not measured. That's one to two orders of magnitude better than reserving
inside the transaction under contention, because the leader serializes without conflicts.

I didn't adopt it, because it turns a library into a service you run:

- A caller's transaction can't contain the leader's write. Spring's outer-transaction mode, native ambient sessions, and
  `GenericDcbApplicationService` dispatching inside the append's transaction all stop working.
- It needs leader election with a fenced lease, a client protocol, and a failover during which nobody can append.
- It only orders new events. The positions already in a store stay reservation order.

The search had two more store-wide candidates. One is a committed-block ledger, B' without the `inflight` array. The
other is a finalizer, a leader-elected process that follows the event collection's change stream, which delivers commits
in commit order, and writes a second, commit-ordered position on each event with one update per event. I rejected the
finalizer. Each of those updates is itself a write to the event collection, so every event shows up a second time in the
change stream that subscriptions read. Which store-wide design to use is still open, and it's searched separately.

The search's main finding was that the DCB consumers don't need a global CT at all. A snapshot decider or a `@Snapshot`
view covers one boundary, and MongoDB already orders the appends on one boundary by their marker versions. That is the
decision below.

### Prior art

I compared with other event stores, from research done for this release. I cite them, I didn't copy them, and the claims
I haven't checked myself are marked.

- Axon Framework's JPA and JDBC stores track gaps with `GapAwareTrackingToken` and give up on a gap once it is older
  than a timeout (I haven't checked the 60-second default, or that the timestamp is set by the client). That's the age
  rule in design 4, on the reader side only. AxonFramework/AxonFramework#4829, "A committed event is skipped for ever
  when a visible neighbour's timestamp ages past the gap timeout", was closed as not planned on 2026-08-07 (checked).
- Axon Server assigns positions with one sequencer per replication group (unchecked), which is design 7's single
  sequencer.
- Marten has a `UseTransactionEvidenceForGapSkipping` option that defaults to true (checked in its source,
  `ProjectionOptions.cs`). It resolves a gap by transaction evidence, the idea behind design 5. That nothing stops a
  late commit there is unchecked.
- A PostgreSQL store can get an exact CT from `pg_snapshot_xmin`. MongoDB has nothing like it.

## Decision

### DCB snapshots resume from marker versions

**A DCB snapshot's version becomes the versions of its boundary's marker documents, and the next read selects the events
whose marker versions are above them.** Positions are never compared.

A marker, from ADR 21, is a document per tag and per type, with key `tag:<tag>` or `type:<type>` and a `version`. Every
DCB append increments, inside its transaction, the markers for every tag and the type of each event it writes, plus the
markers of its condition's query when it has one (`MongoEventStore.java:497-501`, `:548-550`, `:561-577`). I write
`keys(C)` for the marker keys of criteria C, which is `DcbMarkerModel.queryMarkerKeys(C)` (`DcbMarkerModel.java:66-79`).

The write path, on the three MongoDB stores:

1. After incrementing the markers, the append reads the marker documents back inside the same transaction, with one
   `find` by `_id`.
2. It writes an `mv` array on each event, with one element `{k, e, v}` for each of the event's own marker keys (every
   tag and its type): the key, the marker's epoch, and the version this transaction gave it.
3. Each marker document gets an `epoch`, an `ObjectId` set with `$setOnInsert` when the marker is created. Pruning
   markers (ADR 21) recreates them with a new epoch. A marker that existed before the upgrade has no epoch, so each 0.35
   marker write adds a second update to the same unordered bulk write. Its filter is `_id` and `epoch` with `$exists`
   false, it sets `epoch` to a new `ObjectId`, and it doesn't upsert. It writes once per marker that existed before the
   upgrade, the first time a 0.35 append touches it. After that it matches nothing, so there's no write and no conflict.
   A 0.34 writer never touches the epoch. A read reports versions only for keys whose marker has an epoch. I didn't use
   an update pipeline with `$ifNull`, because a pipeline replaces `$inc` with `$add`, and a counter's stored type, int32
   or int64 as `UpdateEventDamage` describes for the position counter, would then need handling of its own. Each marker
   document also gets a counter `s`, incremented in the same update as `version`, which a section below explains.
4. A compound index `{"mv.k": 1, "mv.e": 1, "mv.v": 1}` is created with the other DCB indexes, and queried with
   `$elemMatch` so the three bounds apply to the same element.
5. `DcbAppendResult` gets an `Optional<DcbMarkerVersions>`: the epoch and the version after this append, for each key in
   `keys(C)` of the condition's criteria. It is present only for a conditional append with a consistency token and
   criteria other than `MatchAll`.
6. `DcbDocumentMapper.toCloudEvent` strips `mv` when it turns a stored document into a CloudEvent, the way it already
   strips the DCB tags and the position (`DcbDocumentMapper.java:65-73`). So `mv` never shows up as a string extension
   on an event a read returns.
7. `updateEvent` keeps `mv` and its epochs, the way it already keeps the position and the DCB tags
   (`SpringMongoEventStore.java:368-374`). It copies the original document's raw `mv` value, whatever its type, never
   through a typed cast, because the CloudEvent no longer has it and a 0.34 `updateEvent` may have left a string there.
   Today `updateEvent` replaces the whole document (`MongoEventStore.java:744-756`), so without this an updated event
   would have no `mv` and no read after versions would ever select it again. A TCK test covers it in all three stores,
   with a case for a string `mv`. Whether the `mv` index accepts a string is unverified, and that case checks it.

The read path:

1. A read reports versions only when it asks for them, with `DcbReadOptions.withVersions()`.
   `DcbReadOptions.afterVersions(DcbMarkerVersions)` sits next to `afterPosition` and implies `withVersions()`.
   Combining `afterVersions` with `afterPosition` is refused. Only the snapshot deciders and the `@Snapshot` view ask,
   and they ask on every read, including a cold start with no snapshot and the full read after `DcbSnapshotInvalid`, so
   every read they make gives them versions to save.
2. Every read that doesn't ask keeps today's path exactly. It reads the token first, then the events, both at the
   store's default read concern, joining a caller's transaction when there is one. That includes every plain
   `read(criteria)` a command or a query does, the DCB catch-up's head and window reads
   (`DcbCatchupSubscriptionModel.java:185`, `:190`, `ReactorDcbCatchupSubscriptionModel.java:324`, `:332`) and the
   re-read `GenericDcbApplicationService` does of its own block (`GenericDcbApplicationService.java:161-162`). So this
   design changes neither the catch-up's read concern nor what a synchronous handler sees, and the seventh review's
   third blocker and its caller-transaction findings can't be reached through it.
3. A read that asks for versions takes the marker documents, the consistency token, the position counter and the events
   from one point-in-time read. In default mode the store runs a short read-only transaction with read concern
   `snapshot`, in a session it opens itself, and commits it with write concern majority, so that the snapshot it read is
   majority-committed. That a read-only commit waits for majority is unverified, as the paragraphs after this list say.
   The token is the sum of the versions it read over `keys(C)`, which is the key set the token already sums today
   (`MongoEventStore.java:580-605`). The counter that bounds the events and gives `lastSequencePosition` is read inside
   the same snapshot. The commit has a time bound, which the paragraphs after this list explain. Any failure before the
   commit succeeds, a read error or a commit error, voids the versions and the token, and neither is returned. The read
   falls back to today's path, the decider or view skips the save, and it logs one warning per instance that names the
   cause.
4. Inside a caller's transaction the read uses that transaction's snapshot. On a sharded cluster a transaction with read
   concern `local` isn't one snapshot across shards, so there the read reports no versions unless the caller's
   transaction reads at `snapshot`. The store detects a sharded cluster with `hello`, whose reply from a `mongos` has
   `msg` set to `isdbgrid`, and reads the joined transaction's read concern. When either is unknown, the read reports no
   versions.
5. The read runs on the primary, because transactions do. The read preference the Spring stores apply through
   `queryOptions` doesn't apply to it.
6. For each item of the criteria, the store picks one selection key. For an item with tags it's the item's first tag key
   in canonical order. An item without tags has no single key, so it gets one branch per type key. The filter is the
   criteria plus, per item, `mv` `$elemMatch` on that key, the saved epoch, and a version above the saved one.
7. The read is refused with `DcbSnapshotInvalid`, and the caller reads the boundary in full, when the saved versions
   can't be compared with the read's. That's a saved epoch that differs from the marker's current epoch, which means the
   marker was pruned. It's a saved version above the version in the read's snapshot, which means the snapshot is ahead
   of the read, after a prune or when a caller's transaction has a snapshot older than the one the decider saved. And
   it's a marker written by 0.34 with no epoch, a key missing from the snapshot, or saved keys that differ from
   `keys(C)`. Last, it's a key whose count of increments without `mv`, `u`, differs from the saved one, which a section
   below explains.
8. `DcbEventStream` gets an `Optional<DcbMarkerVersions>`, the versions of `keys(C)` in the snapshot the events came
   from. It is present only when the read asked for versions and had no limit, skip, backward order or upper position. A
   read that may have stopped early doesn't report versions it didn't read up to.

The read-only transaction commits with `TransactionOptions` of its own, with read concern `snapshot`, write concern
`WriteConcern.MAJORITY.withWTimeout(bound)`, `maxCommitTime(bound)` and `timeout(bound)`. The last one stops a
client-wide `timeoutMS`, including 0, from replacing the bound. All three methods exist in the 5.8.0 driver, and the
driver version the project resolves is checked when it's built. It never reuses the options a user configured for
writes, which are `EventStoreConfig.transactionOptions` (`EventStoreConfig.java:113`), the Spring store's
`transactionTemplate` (`SpringMongoEventStore.java:117`) and the reactor store's `transactionalOperator`
(`ReactorMongoEventStore.java:361`). The bound is a store setting, 5 seconds by default.

The native store starts and commits the read-only transaction by hand. `ClientSession.withTransaction` would retry a
commit that timed out on its write concern for up to 120 seconds in the 5.8.0 driver (`ClientSessionImpl`), and
committing by hand removes that loop. The driver still retries a commit once on a retryable error, such as a step-down,
which matters for liveness only. After a failed commit the store closes the session and doesn't call `abortTransaction`.
The Spring transaction managers don't loop on a commit either, and that the reactor store doesn't is checked when it's
built. No versions or token reach the caller before the commit succeeds. The reactor store uses the `Mono` form of
`transactional` over the collected read, which emits after the commit, never the `Flux` form, which emits before it.

The bound decides liveness only. Take a set with a primary, a secondary and an arbiter, with the secondary down. Appends
still commit there, because the implicit default write concern on such a set is w:1. A commit at majority can't be
acknowledged, and without a bound it waits for ever, so every snapshot command would block where today it completes. The
review of this design checked that against the MongoDB
[write concern](https://www.mongodb.com/docs/manual/reference/write-concern/) documentation. With the bound, the commit
times out, the read falls back to a full read on today's path that discards the base state, and the command completes
without saving a snapshot. It takes up to the bound longer than it does today. Correctness doesn't depend on the bound,
because a read whose commit failed returns no versions.

Two claims in this section are unverified until a test with a `failCommand` fail point runs. One is that a read-only
transaction's commit waits for majority at all. The other is that the bound stops that wait.

A snapshot read outside a transaction isn't an alternative. It has no commit to wait on, but when the majority point
stalls, its token trails the primary's markers, so a conditional append fails its check, reads the same token again, and
loops.

Reporting versions on every full read was the first proposal, and it put that commit on every DCB command. Only the
deciders and the view ask now, so only they wait on the bound.

The consumers, on both stacks:

- `GenericDcbApplicationService` gets `DcbExecuteOptions.withVersions()`, which asks for versions with no base, and
  `DcbExecuteOptions.fromVersions(...)`, which implies it. It passes them on as `withVersions()` and `afterVersions`.
- The snapshot deciders store a `DcbMarkerVersions` as their version and read with `fromVersions(base)`, or with
  `withVersions()` when they have no snapshot. Before saving at the append result's versions, a decider checks two
  things. The read has to have reported every key of the append's versions, each with the same epoch as the append's,
  because a prune between the read and the append can leave an equal sum
  ([#1300](https://github.com/johanhaleby/occurrent/issues/1300)). A key the read reported without an epoch counts as a
  mismatch. And the append has to be majority-committed, which the next section covers. If either check fails, or the
  result has no versions, it skips the save. On `DcbSnapshotInvalid` it reads the boundary in full with
  `withVersions()`, so it can save a fresh map.
- On every delivery the `@Snapshot` view reads `afterVersions` from its saved versions, applies what it read, and saves
  at the read's versions. A redelivered event is at or below the saved versions, so the read doesn't return it, and the
  view doesn't look at the delivered event's `mv`. When the read's snapshot doesn't have the delivered event yet, the
  view saves below it and applies it on the next delivery for that boundary. That's a delay, never a loss.
  `everyNEvents` counts events since the snapshot instead of positions.
- A `@Snapshot` view over `MatchAll` logs a warning when it's registered and runs without a snapshot. I chose not to
  refuse it, because `@Snapshot` shipped in 0.34.0 and refusing it would stop an application that starts today. A
  decider or view that gets no versions from a store that can't give them logs one warning.
- `InMemoryEventStore` keeps a counter and an epoch per key under its existing lock and reports the same versions.

`PositionBackfill` only gives positions to events that have none, and every DCB event gets its position when it is
written, so it isn't part of this.

### The decider confirms its append before it saves

The snapshot decider saves after `applicationService.execute` returns (`SnapshotDcbDeciderApplicationService.java:144`,
`:158-162`). With synchronous subscriptions, `execute` runs the read, the decision and the append in the executor's
transaction (`GenericDcbApplicationService.java:137`, `:146-163`, `:172`). The read, the token check, the marker
increments and the read-back of the versions then all happen in one transaction's snapshot, so the state the decider
saves is complete in every mode. The seventh review's first blocker came from building the watermark after commit, and
this design never does that.

Completeness isn't durability. Versions from an append that rolls back after a failover are handed out again to other
events, and a snapshot saved at them skips those events. The same happens without a failover when the caller owns the
transaction. `NativeMongoTransactionExecutor` joins a transaction already open on the thread
(`NativeMongoTransactionExecutor.java:78-82`), and Spring's `@Transactional` does the same, so `execute` can return
before anything is committed:

1. The caller wraps `decider.execute(withdraw)` and a later call to `somethingElse()` in one
   `executor.inTransaction(...)`, with `tag:account:a1` at version 7.
2. Inside, the token check passes, `tag:account:a1` goes to 8, and `execute` returns with versions at 8.
3. The decider saves at 8 with the `Withdrawn` applied.
4. `somethingElse()` throws, the transaction aborts, and the marker is back at 7.
5. An unconditional deposit on `a1` commits with `mv` version 8.
6. The next withdraw reads after 8 and misses the deposit, and its token from the read is 8, so its append goes through.
   The deposit is lost to the decider, and the aborted `Withdrawn` stays in its state.

So the decider confirms before it saves. It reads its own append back at read concern majority, outside any session, by
the append's `AppendId` within its first and last position, which the unique position index finds. Found means the
append is majority-committed, so its versions can't be handed out again. Not found means the decider skips the save and
logs one warning per decider instance, with the reason and the consequence that the next command reads more of its
boundary. Every DCB append already writes its `AppendId` on every event in all three stores, conditional or not
(`MongoEventStore.java:475`, `:517`, `SpringMongoEventStore.java:430`, `:522`, `ReactorMongoEventStore.java:357`,
`:490`). The read can't use any store's existing read path, because each one joins a transaction the caller has open and
would see the append's own uncommitted write. The native store's `findEvents` joins the ambient session
(`MongoEventStore.java:227-229`), and both Spring stores set `SessionSynchronization.ALWAYS` on their template
(`SpringMongoEventStore.java:917`, `ReactorMongoEventStore.java:817`). A sessionless read can't see a caller's
uncommitted or aborted transaction, so step 3 above becomes a skipped save. It also means a decider that runs inside a
transaction the caller owns, such as Spring's `@Transactional`, never saves a snapshot, because its append isn't
committed when the decider looks for it.

So each store gets a read path of its own, used only by the confirmation, that opens no session and joins none. Its
filter is the `AppendId` and a position from the append's first to its last, and it reads on the primary.

- The native store reads `eventCollection.withReadConcern(MAJORITY).withReadPreference(primary()).find(filter)`, with no
  session argument.
- The Spring store reads a collection from `MongoDatabaseFactory.getMongoDatabase()`, the driver's database without the
  template's session binding.
- The reactor store reads a collection from `ReactiveMongoDatabaseFactory.getMongoDatabase()`, without the session in
  the subscriber context.

That a collection from either Spring factory stays out of a Spring transaction is unverified. A TCK test settles it. It
runs a snapshot decider inside a caller's transaction on each store, rolls the transaction back, and asserts that no
snapshot was saved. It also asserts on the `DcbAppendConfirmation` answer itself while the caller's transaction is still
open, so it fails if the confirmation joins the transaction. It saves to `InMemorySnapshotStore`, or to
`InMemoryReactiveSnapshotStore` for the reactor store, because a Spring snapshot store's save joins the caller's
transaction by default and would hide that defect. A second case runs the decider on a committed path and asserts that
the snapshot was saved. If a factory's collection does join the transaction, that store runs its confirmation under
`PROPAGATION_NOT_SUPPORTED` instead.

A store built on `MongoTemplate.withSession(...)` holds a factory bound to a session, so its confirmation would join
that session. Such a store is unsupported for snapshot deciders, and the store javadoc and the snapshot documentation
say so. 0.35.0 doesn't detect it.

The store answers the confirmation through a small SPI, `DcbAppendConfirmation`, which the decider reaches through the
application service. A third-party store or a decorator that doesn't implement it answers unconfirmed, so a decider on
it never saves a snapshot. `InMemoryEventStore` always confirms. An append result with no `AppendId` means the decider
skips the save. An error from the confirmation counts as not found, so the decider skips the save and the command
doesn't fail.

Saving after the caller's transaction commits isn't in 0.35.0, and I record it as a known limitation, tracked in
[#1307](https://github.com/johanhaleby/occurrent/issues/1307). The warning names an active caller transaction when the
store can detect one.

That is the split the seventh review itself suggested, completeness from the transaction and durability from a majority
read. It isn't the seventh review's first blocker again, because the versions still come from the read-back inside the
transaction, and the read after commit only confirms that the append is durable. It needs no privilege and no knowledge
of the server's default write concern. It costs one indexed read per saved snapshot, and with the default every-N policy
a decider doesn't save on every command.

A first version of this section inferred the commit write concern at store start instead, from the transaction, the
session, the client and `getDefaultRWConcern`. The review of this design showed that inference can be wrong. A default
lowered or an arbiter added after start isn't seen, a numeric or tag `w` isn't majority, `mongos` doesn't show a shard's
arbiters, and a joined transaction's options may not be readable. Each of those lets the decider save at versions that
later roll back. The confirmation replaces it.

The default commit write concern in `EventStoreConfig` (`EventStoreConfig.java:113`) and
`NativeMongoTransactionExecutor` (`NativeMongoTransactionExecutor.java:59`) is empty `TransactionOptions`, and
correctness no longer depends on it. A deployment that commits at w:1 gets a snapshot when replication has caught up by
the time the decider confirms, and a skipped save when it hasn't.

### A counter detects old writers, so resuming from versions is on by default

A 0.34 writer increments markers but writes no `mv`. The review of this design showed that's a lost update, not only a
stale snapshot:

1. Instance A runs 0.35, executes a withdraw with a full read, and saves at `tag:account:a1` version 8.
2. Instance B still runs 0.34 and deposits on `a1`. The marker goes to 9, and the deposit has no `mv`.
3. A's next command reads after version 8, and the deposit has no `mv` to select it by. The token from A's snapshot is 9
   and includes B's increment, so A's append passes its check, and A saves at 10.

An earlier version of this design put resuming from versions behind a setting that was off by default, because nothing
in the store could tell that a writer doesn't write `mv`. The only safe default was to trust no snapshot until an
operator said every writer ran 0.35. A counter on the marker now detects such a writer, so resuming from versions is on
by default in 0.35.0.

Every 0.35 writer adds `$inc` of a field `s` to the marker update that increments `version`, so `s` counts the
increments that also wrote `mv`. Every marker increment a 0.35 append makes also increments `s`, and every event a 0.35
append writes has `mv` for each of its marker keys. So only an increment that `s` doesn't count can come with an event
that lacks `mv`. A conditional append also increments the markers of its query's keys that none of its events has
(`MongoEventStore.java:548-550`). Those increments are counted in `s` and miss nothing, because no event of that append
has the key. Writing `mv`, `s` and the epoch happens on every 0.35 write, whatever the setting below says. A 0.34
writer, or any other writer that doesn't know about `s`, increments `version` and doesn't touch `s`. Each element of a
`DcbMarkerVersions` holds `{k, e, v, u}`, where u is v minus s, the number of increments that wrote no `mv`, with a
missing `s` read as 0. `afterVersions` refuses with `DcbSnapshotInvalid` when u has changed for any key, and the
built-in consumers then read in full and save a fresh map. A decider's own append raises v and s together, so u stays
the same in the versions it saves. The decider saves u from its append's read-back, not from its read.

The schedule above again, with instance A on 0.35, instance B on 0.34, and `tag:account:a1` at version 8 with `s` at 3:

1. A saves `{a1: v 8, u 5}`.
2. B deposits on `a1`. 0.34 raises the version to 9, writes no `mv` and doesn't touch `s`, which stays at 3, so u goes
   from 5 to 6.
3. A's next read after version 8 sees u at 6 and refuses with `DcbSnapshotInvalid`. A reads the boundary in full, which
   finds the deposit.

The check doesn't depend on the 0.34 increment being the latest one. Say a 0.35 append then takes the marker to version
10 and `s` to 4. A consumer that still holds `{v 8, u 5}` sees u at 6 and refuses too. And it catches any writer that
increments markers without writing `mv`, not only 0.34.

The cost is one field per marker document and one per element of a saved map. There's no new write and no new contended
document, because `s` is one more field in the same update on the same marker document, in the same unordered bulk
write, inside the same transaction (`MongoEventStore.java:569`, `SpringMongoEventStore.java:575`,
`ReactorMongoEventStore.java:423`). 0.34.0 writes markers with the same upsert and no `s`, and no other code in this
repository writes marker documents. Pruning a marker (ADR 21) resets v and s together, and the epoch already catches
that.

The setting stays, so an operator can turn resuming from versions off. With it off:

- No read reports versions. The deciders and the view read their boundary's full history and never save a DCB snapshot.
- A direct `afterVersions` or `fromVersions` call returns `DcbSnapshotInvalid`, the same result as after a prune. The
  javadoc of both says so.
- The store logs one warning at start that names the setting.

An upgrade from 0.34 needs no manual step, provided nothing replaces the document of an event a 0.35 node wrote. That
rules out a 0.34 `updateEvent`, a 0.34 tool and any other code that replaces event documents, during the rolling
overlap, after a rollback to 0.34 followed by a re-upgrade, and from a tool that shares the event collection. Nothing
has to be switched during the upgrade. The migration guide explains the detection and the setting, and the changelog
names the setting and its default. Events written before the upgrade have no `mv` and are reached only by a full read,
and every 0.35 snapshot starts from one, because 0.34 DCB snapshots are invalid.

If a 0.34 `updateEvent` does run on such an event, 0.34.0 reads `mv` as a string extension through
`DocumentCloudEventReader` (`DocumentCloudEventReader.java:68-83` at 0.34.0) and, when the update function changed the
event, writes it back as a string. A function that builds a fresh event drops `mv` instead, with the same consequence.
Any other code that replaces the document drops `mv` or turns it into a string the same way. No marker changes, so u
doesn't move, and nothing moves it afterwards. The miss is permanent until something forces a full read of that
boundary, such as a prune or another increment without `mv`. A snapshot decider or `@Snapshot` view whose saved versions
are below the event's never applies it. And the decider's token counts the event's marker increment, so its next append
passes its condition without the event. That's a lost update, not only an event missing from the state. Full reads,
subscriptions and catch-ups select by tags, type and position, never by `mv`, so they still return the event. I chose a
documented precondition over a repair step, and the migration guide states the precondition and what happens when it
isn't met.

### Why a late append can't be missed

Two properties make this work.

**Per key, marker versions follow commit order.** The transaction that sets marker k to version j+1 took its snapshot
after the transaction that set k to j committed. Two transactions can't both increment k at once. MongoDB aborts one
with a write conflict, and it reruns on a snapshot that includes the other's commit. So an event with `mv[k] <= V[k]`,
for versions V read in one majority-committed snapshot, is committed and visible to any read in that snapshot or a later
one.

**A matching event has the selection key of an item it matches.** An event matching a tagged item has every tag of that
item, and the store writes a version for every tag of the event. An event matching an untagged item has one of the
item's types, and the store writes a version for its type.

Now take a snapshot saved at versions V0, which has applied exactly the matching events committed in the snapshot V0
came from. Take any matching event e, and k the selection key of an item it matches. If e committed in that snapshot,
then `e.mv[k] <= V0[k]` and it is already in the state. If it didn't, then `e.mv[k] > V0[k]`, and the next
`afterVersions(V0)` read returns it once it is visible. Each event is on exactly one side, so none is applied twice. The
event's position never enters it.

The deposit and the withdraw from #1230, with marker `tag:account:a1` at version 7 when the withdraw reads:

1. The deposit reserves 41. Its first transaction aborts and it backs off. Nothing it did is committed.
2. The withdraw reads the boundary at version 7, decides, and appends at 42 with its token. Inside its transaction the
   token still matches, it increments `tag:account:a1` to 8, writes `mv` version 8 on its `Withdrawn`, and commits.
3. The decider confirms the append at majority and saves the snapshot at version 8. The only events with version 8 on
   `tag:account:a1` are the withdraw's own, and the decider applied them.
4. The deposit retries. Its increment of `tag:account:a1` can only succeed on a snapshot that includes the withdraw's
   commit, so it gets version 9, and it commits at position 41 with `mv` version 9.
5. The next command reads after version 8 and gets the deposit. The `@Snapshot` view, delivered the deposit after the
   withdraw, reads after its saved 8, gets the deposit at version 9 and applies it too.

Had the deposit committed between the withdraw's read and its append, the withdraw's token check would have failed and
the command would have read again, as it does today.

### Every pair of values the design combines

The seventh review's lesson is that two values read at different read concerns can't be used as if they described one
point in time. So here is every pair this design combines, and why each is safe. The argument rests on two facts. Marker
versions only grow, and a prune changes the epoch. A majority-committed write is present on every later primary.

- **The versions and the events after them** come from one snapshot, the store's read-only transaction or the caller's
  transaction.
- **The token and the events, in a read that asks for versions,** come from the same snapshot, because the token is
  computed from the versions.
- **The position counter and the snapshot.** The counter is read inside the snapshot. Read before it, as `read()` does
  today (`MongoEventStore.java:369-372`, `SpringMongoEventStore.java:247-250`), it loses an event. The counter reads
  50. Append Y then commits a deposit at 51 with `tag:account:a1` at 8. The snapshot sees `a1` at 8 but the events are
      bounded at 50, so Y is cut off while the token covers it. The decider appends on token 8 without Y and saves at 9,
      and Y is never selected again.
- **The token and the events in every other read** keep today's path, token first, both at the default read concern.
  This design doesn't change them. In the Spring stores the two may already come from different members, because the
  events read applies `queryOptions` (`SpringMongoEventStore.java:259`) and the token read doesn't (`:596`). The seventh
  review raised that as a hypothesis, and whether it routes to a secondary while a primary is up is unchecked.
- **The token a decider read and the token inside its append.** The token it read comes from snapshot P, which is
  majority-committed once the read transaction has committed, if that commit waits for majority, which is unverified.
  The append transaction's snapshot S is later, on the primary, so it contains everything in P. Equal sums then mean no
  key in `keys(C)` moved, since each key only grows, and an append after S write-conflicts with the decider's append.
  The unsafe case is a token newer than the events, which is the seventh review's second blocker, and reading both from
  one snapshot removes it.
- **The versions an append reports and the decider's state.** The versions are each read version plus one, and the state
  is the base, the events after it in P and the decider's own events. That's the argument above. The epoch check covers
  a prune in between, and the confirmation covers durability.
- **The saved versions and the next read.** They come from different reads. If the read's snapshot is older than the
  saved versions, the read refuses. If it's newer, the events above the saved versions cover the difference.
- **The view's read and the event that triggered it** come from different reads, and nothing ties them together. If the
  read misses the event, the view applies it on the next delivery for that boundary.

- **The append's commit and the confirmation.** The confirmation reads the append after it committed, at majority and
  outside any session. Found means the append is majority-committed, so no later primary loses it. Not found, or an
  error, means no save.
- **The epochs the decider read and the epochs in its append's versions.** They come from different transactions. The
  decider compares them before it saves, so a prune in between, which can leave an equal sum, leads to a skipped save.
- **The token over every writer and the events after versions over the writers that write `mv`.** The token sums every
  increment, including one from a writer that writes no `mv`, and the read after versions selects only events with `mv`.
  That's the gap the rolling upgrade showed, and `s` closes it. Any increment without `mv` moves u, and the read
  refuses.

The one read after commit is the decider's confirmation, and it builds nothing. The versions still come from inside the
transaction.

### What the design depends on

The design as first proposed needed eight corrections, and the reviews of this design found more. Each changed something
above:

- **One point-in-time read.** Today `read()` reads the token with no session and then the events, which join the
  caller's ambient session when there is one (`MongoEventStore.java:361-390`, `findEvents` at `:227-229`). Versions read
  that way can be from a different moment than the events, so a read that asks for versions becomes one read-only
  transaction.
- **The token from the versions' snapshot.** The first proposal kept the token's own read beside the new snapshot read.
  That is the seventh review's second blocker again, a token from one point in time beside events from an older one. A
  read that asks for versions computes the token from the versions it read.
- **One selection key per item.** The first proposal selected on every key of the query, so a marker read that saw the
  markers at different moments couldn't apply an event twice. But a type key changes with every append of that type in
  the whole store, so a branch on `type:Withdrawn` above version v scans every `Withdrawn` since the snapshot before the
  criteria filter applies. With one point-in-time read the extra keys aren't needed.
- **The epoch on every `mv` element.** After a prune the recreated marker starts again at 1. Events written before the
  prune with version 5 would match "above 1" and be applied twice unless the read also matches the epoch.
- **`afterVersions` on `DcbReadOptions`.** The store read is what `GenericDcbApplicationService` calls, so the read
  options have to take the versions, not only `DcbExecuteOptions`.
- **Versions from an append only once durable.** A commit at w:1 can be rolled back after a failover, and the versions
  it used are handed out again. The first answer was to report versions only at majority commit, and then to infer the
  commit write concern at store start. The confirmation read replaced both.
- **No `afterClusterTime` for the view.** The first proposal read after the delivered event's cluster time, so the view
  would always see its own trigger. The review of this design found that nothing passes that cluster time to the view,
  and that a replayed event isn't always majority-committed. I dropped it and accept the delay.
- **The rolling upgrade.** A first precondition, stop every 0.34 writer, had nothing to enforce it. A setting that was
  off by default came next, and still relied on an operator to say the last 0.34 writer was gone. The counter `s`
  detects such a writer, so neither is needed.
- **The counter inside the snapshot.** Found by the review of this design, as the pair above shows.
- **Versions only on request.** Found by the review of this design, because of the blocked commit on a set with an
  arbiter.
- **`updateEvent` keeps `mv`.** Found by the review of this design. Without it an updated event drops out of every read
  after versions.
- **The epoch check before saving, the void map after a failed commit, no versions in a sharded `local` transaction, and
  the warning for a view over `MatchAll`.** Smaller findings from the same review.
- **A bounded commit.** Found by a later review, because the read-only transaction's majority commit still blocked every
  snapshot command on a set with an arbiter and a secondary down.
- **A confirmation outside the caller's transaction.** Found by a later review, because every existing read path joins
  the caller's transaction, and a confirmation through one would find its own uncommitted append.
- **Versions on every decider and view read, `mv` stripped from the CloudEvent and copied from the original document,
  the confirmation SPI, and the sharded check.** Smaller findings from the same later review.

One precondition comes from MongoDB itself. A majority write is durable only with `writeConcernMajorityJournalDefault`
set to `true` in the replica set configuration and no `j: false` on the write. Otherwise a majority-committed write can
still roll back, according to the [write concern](https://www.mongodb.com/docs/manual/reference/write-concern/)
documentation as the review of this design read it.

### Store-wide positions in 0.35

For 0.35, a bare position is documented as unsafe to resume from on MongoDB:

- The javadoc, the documentation and the migration guide say that a position on its own is not a safe resume point on
  MongoDB. The thing to persist is the opaque `Checkpoint` a catch-up stores, which has its live start with it (ADR
  144).
- The entry points that take a bare position as a place to resume from are deprecated. Those I know of are
  `GlobalCheckpoint.of(long)` (`GlobalCheckpoint.java:88`), the `startAtGlobalPosition` annotation attribute, and
  `DcbStartAt.afterPosition` (`DcbStartAt.java:74-75`). `DcbExecuteOptions.fromPosition` and
  `DomainEventQueries.afterPosition` are deprecated in favour of the version forms above. The pull request that adds the
  deprecations lists every public method that takes a position as a lower bound for reading or subscribing, and every
  method that hands out a position meant to resume from.
- A catch-up that only replays and never goes live promises "everything committed before I started". Every event that
  committed before the run started has a position at or below the head the run reads, and the replay reads it. A run
  resumed after a crash reads a new head and replays after its stored position, which still covers everything committed
  before the first start. What it doesn't promise is "everything up to position H", which would need a CT.
- That promise holds only when the window reads run on a member that has every write the head read saw. Today the head
  (`PositionCatchupPipeline.java:84`) and the windows are both read at the default read concern, and on the primary that
  holds. Three cases void it, and the javadoc names them. Stream window reads routed to a secondary can miss an event
  the primary's head covers, and the Spring stream read applies `queryOptions` (`SpringMongoEventStore.java:664`). DCB
  window reads go through `queryOptions` too (`SpringMongoEventStore.java:259`). And a later change that moves the
  windows to majority without moving the head is the seventh review's third blocker again.
- A position-only checkpoint written by 0.34 can't be repaired by anything. The migration guide says to rebuild it or
  accept it.

A store-wide fix, one that gives a bare position a safe meaning, is still open and searched separately. It has to meet
two constraints. It must not change the DCB path above. And if it reads windows at majority, it reads the head and the
live start (ADR 144) in the same causal session, with `afterClusterTime` at or above the head read.

## Consequences

- #1230 is fixed for the snapshot deciders and the `@Snapshot` view on every store in this repository, without a new
  shared write, a background task or a clock. The one timeout is meant to bound how long a read waits, not what it
  returns, and that it does is unverified until a fail-point test runs.
- Per DCB append, the store does one more `find` by `_id` of a few marker documents inside the transaction. Each event
  grows by one `mv` element per tag and type, roughly 40 to 60 bytes each (an estimate), with one more index entry per
  element. The position counter write is unchanged, and each marker update sets one more field, `s`, in the same write.
- Per DCB read that asks for versions, a short read-only transaction on the primary replaces today's separate reads, and
  the Spring stores' read preference doesn't apply to it. Only the deciders and the view ask, on every read. Its commit
  is meant to wait at most the bound, 5 seconds by default, and a command whose commit times out falls back to today's
  read and saves no snapshot. Every other read is unchanged.
- Per saved DCB snapshot, the decider does one indexed read at majority. When the append isn't majority-committed by
  then, it skips the save with a warning, and the next command reads more of its boundary.
- Resuming from versions is on by default in 0.35.0. A writer that increments markers without writing `mv`, such as
  0.34, moves u, the count of increments without `mv`, and the snapshots it affects are rebuilt from a full read. With
  the setting off, every decision reads its boundary's full history, no DCB snapshot is saved, a direct `afterVersions`
  or `fromVersions` call returns `DcbSnapshotInvalid`, and the store names the setting in a warning at start.
- An upgrade from 0.34 needs no manual step, provided nothing replaces the document of an event a 0.35 node wrote, be it
  a 0.34 `updateEvent`, a 0.34 tool or any other document replacement, whether during the rolling overlap, after a
  rollback to 0.34 and a re-upgrade, or from a tool that shares the event collection. Such an event drops out of every
  read that resumes after saved versions, and nothing moves u, so the miss is permanent until something forces a full
  read of that boundary. The decider's token counts the event's marker increment, so its next append passes its
  condition without the event, which is a lost update. An update function that builds a fresh event drops `mv` with the
  same consequence. Full reads, subscriptions and catch-ups select by tags, type and position, never by `mv`, so they
  still return the event. The migration guide states the precondition and explains the detection and the setting, and
  the changelog names the setting and its default.
- The first command on a boundary that existed before the upgrade reads in full and saves nothing, because its markers
  have no epoch yet. Its append gives them one, and the next command can save.
- A decider inside a transaction the caller owns never saves a snapshot. Saving after that transaction commits isn't in
  0.35.0, and this is a known limitation, tracked in #1307. The warning names an active caller transaction when the
  store can detect one.
- The stores already require transactions. A durable snapshot also needs `writeConcernMajorityJournalDefault` set to
  `true` and no `j: false`.
- The public API breaks. `DcbEventStream` and `DcbAppendResult` get a new record component, so a record deconstruction
  pattern with the old number of components stops compiling. Their compatibility constructors leave the versions absent.
  `DcbReadOptions` gets `withVersions()` and `afterVersions(...)`, and `DcbExecuteOptions` gets `withVersions()` and
  `fromVersions(...)`. The stores get the `DcbAppendConfirmation` SPI, a setting for the commit bound and the setting
  that turns resuming from versions off. The DCB snapshot version changes type from `long` to a structured version. The
  stream snapshot keeps its version. The position entry points listed above are deprecated. The changelog, the migration
  guide and an OpenRewrite recipe for the record patterns cover it.
- Existing DCB snapshots are invalid. They are ignored or deleted and rebuilt from a full read on the next command.
- Criteria that are `MatchAll` get no DCB snapshot. A `@Snapshot` view over `MatchAll` logs a warning when it's
  registered and runs without one, so an application written against 0.34.0 still starts. Selecting by version would
  need a marker that every append increments, which ADR 21 already calls a whole-store lock.
- Pruning markers (ADR 21) now costs more than resetting counters. Every DCB snapshot on a pruned boundary is refused
  with `DcbSnapshotInvalid` and rebuilt from a full read on its next use.
- [#1300](https://github.com/johanhaleby/occurrent/issues/1300) is a related open problem. A consistency token read
  before a prune can let a conflicting append through after it. The marker epoch is one of the fixes it lists. The
  decider compares epochs before it saves, and whether the token uses the epoch too is decided in #1300.
- `updateEvent` keeps `mv`. An event whose tags or type change through `updateEvent`, or that `delete(Filter)` removes,
  can still move between boundaries without a marker increment. The consistency token has the same blind spot today, and
  this ADR doesn't change it.
- Inside a caller's transaction on a sharded cluster, a read reports no versions unless the transaction reads at
  `snapshot`, so a decider there reads its boundary in full.
- The `@Snapshot` view can lag behind the event that triggered it until the next delivery for that boundary. It never
  loses the event.
- Store-wide positions stay as ADR 84 describes them. A catch-up that resumes from a bare position can still miss an
  event until the store-wide fix, and a 0.34 position-only checkpoint stays unrepairable.
- Some things can't be settled until the code exists, and are checked when it's built:
  - The in-transaction read-back returns the transaction's own increments.
  - The Spring store can open its own session for the read-only transaction when no caller transaction is active.
  - MongoDB picks the `mv` index for the read after versions. A wrong plan there is slow, not wrong.
  - A collection from either Spring database factory stays out of a Spring transaction. The TCK test above settles it.
  - A read-only transaction with read concern `snapshot` on the primary reads at the primary's latest applied point, and
    its majority commit waits for that point to be majority-committed. Whether the commit of a read-only transaction
    waits for majority at all, and whether the bound stops that wait, is settled by a test with a `failCommand` fail
    point.
- Whether a failover can roll back a counter increment made at w:1 while the event's transaction survives is unchecked.
  If it can, a replay-only catch-up's head can sit below a committed event, and the head has to be the highest committed
  position instead of the counter.
