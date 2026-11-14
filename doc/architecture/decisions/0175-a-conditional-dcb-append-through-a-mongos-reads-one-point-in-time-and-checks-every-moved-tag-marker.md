# 175. A conditional DCB append through a mongos reads one point in time and checks every moved tag marker

Date: 2026-11-13

## Status

Accepted. Resolves [#1476](https://github.com/johanhaleby/occurrent/issues/1476). Amends
[ADR 149](0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md) on sharded
clusters, where its step 3 refused every append whose markers moved. Takes up the first option ADR 149 left open.

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
   before it, while the read misses E. This ADR calls that a torn token. Step 3 rules out an item with tags as soon as one of its markers didn't move, so
   the marker from after E rules out the item and the append passes.

## Decision

### The store asks for one point in time, and the check reads what it got

Through a mongos, each store runs its own conditional append's transaction at read concern `snapshot` and write
concern `majority`. MongoDB promises one point in time across the shards only for that pair.

| Store | How it asks |
| --- | --- |
| Native `MongoEventStore` | Merges both into its `TransactionOptions`, keeping a configured `wtimeout`. |
| `SpringMongoEventStore` | Runs the append through a copy of its `TransactionTemplate` labelled `mongo:readConcern=snapshot` and `mongo:writeConcern=majority`, which Spring Data's default resolver turns into transaction options. |
| `ReactorMongoEventStore` | Builds a `TransactionalOperator` with those labels on the `ReactiveTransactionManager` its `EventStoreConfig` was given. |

The check doesn't trust what the store asked for. Inside the transaction it reads the options of the transaction the
bound session runs, and gives the check one of three scopes.

| Scope | When | Step 3 |
| --- | --- | --- |
| `REPLICA_SET` | The store runs on a replica set. | As in ADR 149. |
| `SNAPSHOT_THROUGH_MONGOS` | The store runs through a mongos, and the session's transaction has read concern `snapshot` and write concern `majority`. | As described below. |
| `MARKERS_ONLY` | Anything else, including any failure to read the options. | Every marker that moved refuses, as in ADR 149. |

The Spring stores reach the bound session through a private subclass of `MongoTransactionManager` or
`ReactiveMongoTransactionManager` that calls the protected `doGetTransaction`. That's an internal of Spring Data 5.1.
A test asserts what it returns, so an upgrade that changes it fails the build. At run time a cast that fails, a missing session, a
transaction object of another class and any exception all give `MARKERS_ONLY`, never `SNAPSHOT_THROUGH_MONGOS`.

### Through a mongos, step 3 rules out an item only when none of its tag markers moved

In `SNAPSHOT_THROUGH_MONGOS`, an item with tags is ruled out only when none of its tag markers moved. For every other
item the query selects on every tag key whose marker moved, with the version the token saw for that key.

A torn token is then refused. Its marker from before E has moved since the read, because E incremented it. E's `mv`
holds that key with a version above the token's, so the query on that key finds E.

An item without tags already worked this way on a replica set, and it does the same through a mongos.

## Consequences

- Through a mongos, the three cases of #1476 pass when the store runs its own transaction.
- A test on two shards for each store checks the three cases of #1476, a torn token built by hand, a token read while
  a matching event had committed on one shard and not the other, and an append in a caller's transaction.
- A conditional append through a mongos costs a transaction at read concern `snapshot` and write concern `majority`.
- An append that joins a caller's transaction gets the caller's options. Unless the caller started it at `snapshot` and
  `majority`, the check is `MARKERS_ONLY` and refuses as it did before.
- A reactor store built from a `TransactionalOperator` has no transaction manager to put labels on, so its appends
  through a mongos run without them and the check is `MARKERS_ONLY`. A store built with
  `transactionConfig(ReactiveTransactionManager)`, as the reactive Spring Boot starter does, gets the new scope.
- A Spring transaction manager with a `MongoTransactionOptionsResolver` that ignores the labels starts the transaction
  without them, and the check is `MARKERS_ONLY`.
- The type marker stripes of [ADR 162](0162-a-dcb-type-marker-is-split-into-stripes.md) change only the keys of an item
  without tags, which already select on every moved key in every scope. This ADR changes step 3 only for items with
  tags, whose keys the stripes don't touch. I checked this by reading the stripes change in
  [#1488](https://github.com/johanhaleby/occurrent/pull/1488) at `7bc6adfe0`. No test combines the two yet.
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
- At read concern `snapshot` and write concern `majority`, the append failed with `WriteConflict` (112) from the markers'
  shard, and only E was stored.

E and G were in different partition streams, so the conflict didn't come from a shared stream. I haven't identified
which write conflicted, so I don't know whether this holds in general.

In a caller's transaction the store doesn't retry. The caller gets a `MongoCommandException` labelled
`TransientTransactionError` instead of `DcbAppendConditionNotFulfilledException`, and its own retry decides what
happens next.

## Alternatives considered

**Select an item with tags on its first tag key, as on a replica set.** With a torn token whose first tag marker is from
after E, the query finds nothing and the append passes.

**Keep refusing through a mongos.** That's correct and keeps the retries #1476 is about.

**Read the token and the events in one snapshot transaction.** That closes #1479 as well, but on its own it doesn't close
reason 1 for the append, and it costs a transaction on every read. #1479 decides it separately.

**Trust the store's own request instead of reading the session.** A caller's transaction that the append joins, or a
resolver that drops the labels, would then run step 3 without one point in time, and that can let a matching append
through.
