# 175. A conditional DCB append through a mongos queries the events in a snapshot of its own

Date: 2026-11-14

## Status

Accepted. Resolves [#1476](https://github.com/johanhaleby/occurrent/issues/1476). Amends
[ADR 149](0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md) on sharded
clusters, where its step 3 refused every append whose markers moved.

## Context

ADR 149's append check reads the markers of a condition inside the append's transaction and compares them with the
token. When some marker moved, step 3 rules out the criteria items that can't have a matching event committed after the
read, and the store queries the events of the rest. On anything but a replica set, step 3 refuses instead.

That refusal costs a retry in three cases where nothing matching was appended, the ones #1476 lists:

- An event of another type with the same tag moved the marker.
- An event with only some of the item's tags moved one of its markers.
- An event of an excluded type with the same tag moved the marker.

ADR 149 gives two reasons step 3 can't run through a mongos.

1. A transaction at read concern local or majority opens a snapshot on each shard when it first touches that shard.
   The query can then read the events from before the markers the check just read.
2. A plain read isn't one snapshot either. A token can hold one tag marker from after a matching event E and another from
   before it, while the read misses E. This ADR calls that a torn token. Step 3 rules out an item with tags as soon as one
   of its markers didn't move, so the marker from after E rules out the item and the append passes.

The append's transaction isn't always the store's own. A `TransactionExecutor` can open it, and a caller can append
inside a transaction of their own, at whatever read concern and write concern they chose.

## Decision

### The store queries the events in a read-only snapshot transaction of its own

Through a mongos, when the markers can't settle a criteria item and the store has to query the events, it doesn't query
them in the append's transaction. It starts a second session and a read-only transaction on it, at read concern
`snapshot` and write concern `majority`, with `markerVersionsCommitTimeout` as the bound on its commit. A read with
`withVersions()` uses the same options. In that transaction it reads the condition's markers again and compares them with what the append's
transaction read.

- When every marker key has the same epoch in both reads, and a version in the snapshot at least as high as in the
  append's transaction, the store queries the events in the same snapshot. A matching event refuses the append, as on
  a replica set, and no matching event lets the check pass.
- When some key has another epoch or a lower version in the snapshot, the append is refused.
- When the snapshot transaction can't start, read or commit, the append is refused.

The append's transaction then increments the condition's markers as before. The store doesn't change that
transaction's options, so it runs as the store, the `TransactionExecutor` or the caller started it.

All three stores do the same. The Spring and reactor stores take the second session from the template's database
factory and use a copy of the template bound to it, with session synchronization `NEVER`, so no Spring transaction
binds that session.

The scope of the check now depends only on the deployment that ADR 149's `hello` test finds.

| Scope | When | Step 3 |
| --- | --- | --- |
| `REPLICA_SET` | The store runs on a replica set. | As in ADR 149, with the query in the append's transaction. |
| `SNAPSHOT_THROUGH_MONGOS` | The store runs through a mongos, or a load balancer in front of one. | As described here. |
| `MARKERS_ONLY` | Anything else, such as a standalone server. | Every marker that moved refuses, as in ADR 149. |

### Through a mongos, step 3 rules out an item only when none of its tag markers moved

In `SNAPSHOT_THROUGH_MONGOS`, an item with tags is ruled out only when none of its tag markers moved. For every other
item the query selects on every tag key whose marker moved, with the version the token saw for that key.

A torn token is then refused. Its marker from before E has moved since the read, because E incremented it. E's `mv`
holds that key with a version above the token's, so the query on that key finds E.

An item without tags already worked this way on a replica set, and it does the same through a mongos.

### Why a matching event can't get through

Take a matching event E. E's append incremented, in its own transaction, every tag marker of an item with tags that E
matches, or one of the type markers of an item without tags ([ADR 21](0021-dcb-write-path-query-scoped-concurrency.md)).
Take k, one of those marker keys whose version in the token is from before E. When there is no such k, the token covers
E, and that's [#1479](https://github.com/johanhaleby/occurrent/issues/1479). There are two cases, depending on whether
the append's transaction read k after E's increment.

- **It did.** The snapshot shows k under the same epoch and at a version at least as high, or the append is refused.
  A version that high was written by E's commit or by a later one, so the snapshot's point in time is after E's commit.
  A snapshot transaction through a mongos reads every shard at one point in time, so it sees E's event document too.
  The marker of k moved since the token, so the query selects on k, and it finds E under the conditions of ADR 149.
- **It didn't.** The append's transaction increments k from a snapshot of k's shard without E's increment. MongoDB
  commits only one of two transactions that write the same document, so the two appends serialize as ADR 149 says. When
  E's append committed first, the append's transaction fails with a write conflict. The store's own transaction retries
  and then reads E's increment of k, which is the case above. In a transaction the store didn't start, the conflict
  aborts that transaction, as it does on a replica set.

Neither case depends on the read concern or the write concern of the append's transaction, so the check is the same in
the store's own transaction, in one a `TransactionExecutor` opened and in a caller's.

The comparison is what makes the first case work. The second session isn't causally consistent with the append's, so
the store doesn't rely on its snapshot being from after the append's read, and refuses one that shows an earlier
version.

## Consequences

- Through a mongos, the three cases of #1476 pass in the store's own transaction, in a `TransactionExecutor`'s and in a
  caller's, whatever read concern and write concern those run at.
- A test on two shards for each store checks the three cases of #1476, a torn token built by hand, a token read while
  a matching event had committed on one shard and not the other, an append in the store's own transaction, in a
  `TransactionExecutor`'s and in a caller's, a snapshot transaction that fails, and two conditional appends in one
  caller's transaction. A unit test checks the comparison.
- A conditional append through a mongos that has to query the events costs a second session and a read-only
  transaction, whose commit waits for `majority` for at most `markerVersionsCommitTimeout`. An append whose markers
  didn't move, or rule out every item, doesn't start it.
- The append's transaction keeps the options it was given on all three stores, a stronger `w` such as `3` or a custom
  write concern name included.
- The check refuses with no matching event when the snapshot shows a marker under another epoch or at a lower version
  than the append's transaction read, or when the snapshot transaction can't start, read or commit. That throws
  `DcbAppendConditionNotFulfilledException` for an append that could have passed, and lets nothing through. The store
  logs the first failure of each kind of the snapshot transaction as a warning, and later ones at debug level.
- Two conditional appends in one transaction that share a marker key behave this way when the second one's markers
  moved. The second reads that marker at the first one's increment, which isn't committed, so the snapshot shows a lower
  version and the second append is refused. When the first append matches the second one's criteria, that refusal is
  the one the condition asks for. The snapshot can't see the uncommitted event, so the comparison is what refuses the
  second append, and the test with two appends in one caller's transaction checks it. When the first append doesn't
  match them, the refusal is one with no matching event.
- The type marker stripes of [ADR 162](0162-a-dcb-type-marker-is-split-into-stripes.md) change only the keys of an item
  without tags, which already select on every moved key in every scope. This ADR changes step 3 only for items with
  tags, whose keys the stripes don't touch. The stripes change is
  [#1488](https://github.com/johanhaleby/occurrent/pull/1488), merged as `5dffc2757`. The unit tests of the check run
  an item without tags over striped type markers in both scopes, and no test on a sharded cluster turns the stripes on.
- This doesn't make a plain read through a mongos safe. A token can still hold E's markers while the read misses E, and
  then no marker moves and the append passes. That's [#1479](https://github.com/johanhaleby/occurrent/issues/1479),
  and the check alone can't see it.

## Open point: a condition without a token in a caller's transaction

A condition without a token has no versions to compare, so the store queries for a matching event inside the append's
transaction. In a caller's transaction at read concern local through a mongos, the snapshot on the events' shard can
open before a matching E commits, and the snapshot on the markers' shard after it. The query then misses E.

Two probes on two shards held E's commit on the events' shard and appended G, matching E, with no token:

- In a caller's transaction at read concern local, the commit failed with `WriteConflict` (112) from the events' shard,
  and only E was stored.
- In a transaction at read concern `snapshot` and write concern `majority`, the append failed with `WriteConflict`
  (112) from the markers' shard, and only E was stored.

E and G were in different partition streams, so the conflict didn't come from a shared stream. I haven't identified
which write conflicted, so I don't know whether this holds in general.

In a caller's transaction the store doesn't retry. The caller gets a `MongoCommandException` labelled
`TransientTransactionError` instead of `DcbAppendConditionNotFulfilledException`, and its own retry decides what
happens next.

## Alternatives considered

**Query the events in the append's transaction, as on a replica set.** A probe on two shards had that transaction read
the events' shard before E committed and the markers' shard after it. The query in the append's transaction missed E,
and a snapshot transaction on a second session found it, with E fully committed and with E committed on one shard only.

**Run the append's transaction at read concern `snapshot` and write concern `majority`.** The query in that transaction
then reads one point in time. Spring Data 5.1 keeps a transaction manager's options in a private field, so the Spring
and reactor stores could get the pair only by aborting the transaction they had just begun and starting it again, and
by reading the bound session through a protected method of the transaction manager. A stronger `w` would become
`majority`. An append in a caller's or a `TransactionExecutor`'s transaction at other options would still refuse every
moved marker.

**Move the second session's cluster time up to the append's session.** The Spring and reactor stores can't reach the
session Spring binds without internals of Spring Data 5.1, since `MongoResourceHolder` and
`ReactiveMongoResourceHolder` are package-private. In the probes on two shards it changed nothing the probes could see.
The comparison makes it unnecessary.

**Select an item with tags on its first tag key, as on a replica set.** With a torn token whose first tag marker is from
after E, the query finds nothing and the append passes.

**Keep refusing through a mongos.** That's correct and keeps the retries #1476 is about.

**Read the token and the events in one snapshot transaction.** That closes #1479 as well, but on its own it doesn't close
reason 1 for the append, and it costs a transaction on every read. #1479 decides it separately.
