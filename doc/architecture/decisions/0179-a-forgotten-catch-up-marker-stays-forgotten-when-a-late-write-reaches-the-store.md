# 179. A forgotten catch-up marker stays forgotten when a late write reaches the store

Date: 2026-11-15

## Status

Accepted. Fixes [#1150](https://github.com/johanhaleby/occurrent/issues/1150) and
[#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 23. Builds on the marker order in
[ADR 177](0177-a-lifecycle-call-takes-effect-at-one-instant-as-in-some-serial-order.md) and keeps the lease condition
from [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) on marker writes. Applies to both
`CatchupThenPushSubscriptionModel`s, both `CatchupProjectionFeed`s, both `DomainEventFeed`s, the Spring Boot
registrars that build them, and every `CheckpointStorage` that Occurrent ships.

## Context

A catch-up marker records that a catch-up has replayed the whole history for a subscription id, so the next catch-up
for that id skips the replay. The push models and the projection feeds keep it in the `CheckpointStorage` they are
given, under the subscription id, and follow the order from ADR 177: read the marker at step 1 and write it at step 4.

Some paths delete the marker so that whatever runs next for the id replays the history again:

* `ReactiveHandover` deletes it through `Source.forgetCaughtUp()` when a catch-up fails, so the event whose delivery
  failed is delivered again by the replay.
* The reactor `CatchupThenPushSubscriptionModel` deletes it on `cancelSubscription(id)`, and so does the blocking one.

Every one of those deletes loses to a marker write that the store applies after it. A client can give up on a save
after a timeout while the database still runs it:

1. The marker write times out on the client.
2. The delete runs and succeeds.
3. The database applies the marker write.

The marker is back, the next catch-up skips the replay, and an event only the replay would deliver again is never
delivered. Nothing is logged, since the delete succeeded. On the blocking push model the per-id lock around the
delete orders the client calls in one process, not the moment the store applies them, so the same three steps happen
through `cancelSubscription(id)`. #1293 item 23 is the restart variant on the reactor push model. Its record of
markers it has not deleted yet (`markersNotYetDeleted`) is kept in memory, so a restart forgets it while the stored
marker stays.

No `CheckpointWriteCondition` can refuse step 3. After a delete nothing is stored, and `any()`, `ifAbsent()` and
`notOlderThan(..)` all accept a write over nothing.

The write version cannot be the fence either. Under competing consumers the blocking push model writes the marker
with `notOlderThan(leaseToken)`. A `cancelSubscription(id)` followed by a new `subscribe(id, ..)` under the same lease
gives both attempts the same token, so a version left by the delete makes `notOlderThan(leaseToken)` accept both
attempts' writes or refuse both. Adding the delete count to the lease version fails the other way. After a few
deletes inside one lease the stored version passes the next lease holder's token, and the old holder's late write is
accepted, which is what ADR 116 exists to refuse.

A marker left behind at the subscription id itself does not work on MongoDB during a rolling upgrade.
`NativeMongoCheckpointStorage.exists(id)` in 0.34.0 matches on `_id` alone, so a 0.34.0 node would read such a
document as a marker and skip a replay.

## Decision

### A generation per marker

Every marker has a generation, a counter that starts at 0 when nothing has been stored for the id.

* Forgetting a marker turns its record into a tombstone and raises its generation by one, in one atomic write to
  that record. The tombstone holds only the generation.
* A generation never goes down. Nothing but a forget changes it.
* A marker write names the generation its attempt read at step 1, and the store writes it only while the stored
  generation still equals that number. A lease condition, where the attempt has one, applies too.
* A write refused by either check stores nothing and returns `false`. A store failure still throws.

A late write from an attempt that started before a forget names a generation the forget has already raised, so the
store refuses it. That holds with or without a lease, and whichever node issued the write.

### `CatchupMarkerStorage`

A new interface on each stack holds the marker operations. `CheckpointStorage` itself does not change.

```java
public interface CatchupMarkerStorage extends CheckpointStorage {
    CatchupMarker readCatchupMarker(String subscriptionId);
    boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long expectedGeneration, CheckpointWriteCondition condition);
    void forgetCatchupMarker(String subscriptionId);
}

// subscription/core, shared by both stacks
public record CatchupMarker(@Nullable Checkpoint checkpoint, long generation) {   // checkpoint is null when absent
    public boolean isPresent() { return checkpoint != null; }
}
```

The reactor interface mirrors it with `Mono<CatchupMarker>`, `Mono<Boolean>` and `Mono<Void>`. `readCatchupMarker`
returns the marker and its generation from one read, so the attempt never pairs a marker with a generation from a
different moment. The names are provisional until the interfaces are built.

Every `CheckpointStorage` that Occurrent ships implements it: `NativeMongoCheckpointStorage`,
`SpringMongoCheckpointStorage`, `ReactorCheckpointStorage`, `SpringRedisCheckpointStorage` and both
`InMemoryCheckpointStorage`s.

### Where the marker is stored

The marker's state (present or forgotten), its generation and its lease version are stored in a record of their own,
apart from the plain checkpoint at the subscription id. The marker's position stays in that plain checkpoint, as in
0.34.0:

| Store | Record | Same atomic unit as the plain checkpoint |
| --- | --- | --- |
| MongoDB (Native, Spring, Reactor) | a document with `_id: {catchupMarker: "<id>"}` in the same checkpoint collection | no, two documents |
| Redis | a key `occurrent:catchup-marker:{<hash tag>}<sha256 of id>`, built the way the version key is | yes, same Cluster slot, one Lua script |
| In-memory | a second map under the store's existing lock | yes |

A 0.34.0 node never reads the record. Its reads and deletes match the plain id exactly, and no shipped 0.34.0
checkpoint storage scans the collection or the key space.

A marker write writes the record and then the plain checkpoint at the subscription id, the way 0.34.0 writes the
marker. That plain copy is the marker a 0.34.0 node reads and deletes during a rolling upgrade. The record decides
first:

| Record | Plain copy at the id | `readCatchupMarker` answers |
| --- | --- | --- |
| tombstone | any | absent, with the tombstone's generation |
| present | present | present |
| present | absent | absent. A 0.34.0 node forgot it, or the plain write failed |
| none | present | present, generation 0. Written by 0.34.0 |
| none | absent | absent, generation 0 |

`readCatchupMarker` fetches both with one query on MongoDB (`_id` in the record's id and the plain id) and one script
on Redis.

`saveCatchupMarker` writes the record first, and the plain copy only when the record was written. When the lease
condition refuses the plain write, the call returns `false`, and when the plain write fails, it throws. Either way the
marker reads absent, and the next catch-up replays. `forgetCatchupMarker` writes the
tombstone first, then deletes the plain copy. On Redis and in memory both steps of either call are one atomic
operation. On MongoDB they are two, and that order keeps every interleaving on the side of an extra replay. A read
between the two steps of a forget sees the tombstone, and a read between the two steps of a save sees a record
without its plain copy.

Reading the plain copy is what keeps a mixed cluster safe in both directions. A 0.34.0 node that forgets a marker
deletes the plain copy, and a 0.35.0 node then reads the marker as absent and replays. A late 0.34.0 write after a
0.35.0 forget is stored in the plain copy, and the tombstone keeps it from counting on a 0.35.0 node. A 0.34.0 node has no
fence, so a 0.34.0 node reading its own late write behaves as it does today.

Plain `read`, `exists`, `save` and `delete` keep their meaning and never touch the record.

On a sharded checkpoint collection the record's `_id` is an embedded document. MongoDB accepts an `_id` of any type
other than an array, a regular expression or undefined, and a hashed index hashes an embedded document as one value.
On MongoDB 4.4.5 a two-shard cluster sharded on `{_id: "hashed"}` and one sharded on `{_id: 1}` both took the record
next to a plain document with the same id. Inserts, finds by exact `_id`, the conditional upsert and deletes by `_id`
all worked, and mongos sent each find by exact `_id` to one shard. The downgrade cleanup below filters on
`_id.catchupMarker`, which mongos cannot route by shard key, so it runs on every shard. The same check has not run
on MongoDB 8.0.

### What a catch-up does with it

1. At step 1 the attempt calls `readCatchupMarker` once. A present marker skips the replay as today. Otherwise the
   attempt keeps the generation it read.
2. At step 4 it calls `saveCatchupMarker` with that generation, plus `notOlderThan(leaseToken)` where a
   `CheckpointWriteVersionSource` gives a version and `any()` otherwise.
3. A `false` means the marker was forgotten or the lease moved on since step 1. The attempt writes no marker and
   records no failure, so the next catch-up for the id replays the history. The live delivery the attempt has already
   started goes on as it would after a written marker.
4. Every path that deletes a marker calls `forgetCatchupMarker` instead of `delete`: `ReactiveHandover`'s
   `forgetCaughtUp()` on both reactor sources, both push models' `cancelSubscription(id)`, and any later feed reset or
   unregister ([#1291](https://github.com/johanhaleby/occurrent/issues/1291),
   [#1032](https://github.com/johanhaleby/occurrent/issues/1032)).

The reactor push model's `markersNotYetDeleted` and the second delete it triggers at step 1 are removed. The tombstone
is stored, so a restart no longer loses the record that a marker was forgotten.

Point 3 does not end the attempt the way a stop does. On the blocking push model a stopped catch-up relaunches its
replay at once while the model runs, and otherwise waits for `start(true)` and drops live events until then (ADR 85).
After a refused marker write the first replays the whole history for nothing, and under a lease that moved on every
relaunch is refused again. Under the second the subscription delivers nothing until someone calls `start(true)`.
Point 3 relies on ADR 177's order, where the marker is written after the drain on both engines. `ReactiveHandover`
writes it before the drain today ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 31), so the reactor
callers change only after that move.

Point 3 also changes a released behaviour. In 0.34.0 a blocking push model whose marker write is refused because the lease
moved on throws `CheckpointWriteConditionNotFulfilledException` out of `markCaughtUp()`. `BlockingHandover` records
it as a failed catch-up, and the registration then refuses every live event until the subscription is cancelled.
Under this ADR the subscription keeps delivering after the same refusal, and no marker is written.

Other deletes follow one rule. A delete that has to refuse a write that is already on its way to the store uses a
generation. A delete that only races other calls in the same process can use a lock. The catch-up progress checkpoint
in [#1258](https://github.com/johanhaleby/occurrent/issues/1258) is the second kind, and is fixed with
`lockHandover(id)` across its check and its save.

### Required where markers are written

Marker-writing code takes a `CatchupMarkerStorage` and has no unfenced fallback.

* Where Occurrent owns the parameter type, the `@Nullable CheckpointStorage catchupMarker` parameter becomes
  `@Nullable CatchupMarkerStorage catchupMarker`:
  * blocking `CatchupThenPushSubscriptionModel`, its four public constructors
  * reactor `CatchupThenPushSubscriptionModel`, its two public constructors
  * blocking `CatchupProjectionFeed.create(..)`, its four overloads
  * reactor `CatchupProjectionFeed.create(..)`, its six overloads
  * blocking `DomainEventFeed`, its three public constructors
  * reactor `DomainEventFeed`, its three public constructors
* The Spring Boot registrars resolve the application's `CheckpointStorage` bean, which Occurrent does not type. The
  blocking `ProjectionAnnotationRegistrar` and `SagaAnnotationRegistrar` and the reactor
  `ProjectionAnnotationRegistrar` refuse to start, the way `CheckpointFencingConfigurationCheck` refuses a storage
  that cannot fence, when a catch-up subscription needs a marker and the bean is not a `CatchupMarkerStorage`. The
  exception names the bean's class.
* A `CatchupMarkerStorage` evaluates write conditions, and the registrars ask `evaluatesWriteConditionsFor(id)` for
  each catch-up subscription id at startup. On Redis in Cluster-safe mode that answers `false` for the ids whose
  record cannot share a slot with the plain checkpoint (`""`, `"{}orders"`, `"a}b{c"`), and the marker calls refuse
  those ids with an `IllegalArgumentException`, as a conditional `save` already does. Such an id keeps a working
  marker today, because a marker write with `any()` is a plain `SET`, so this is listed as breaking too.

A `null` marker storage keeps its meaning, and every catch-up replays the history.

## Consequences

* A third-party `CheckpointStorage` that was handed to a catch-up model or feed stops compiling there, or stops the
  application at startup under Spring Boot. Its author implements `CatchupMarkerStorage`, and a TCK suite on each
  stack, `CatchupMarkerStorageConformance`, checks the contract. The changelog lists this as breaking, and the
  0.35.0 migration guide shows what to implement. A variable typed `CheckpointStorage` that holds a shipped store
  needs its type changed, nothing else.
* Code that reads a marker through plain `read(id)` keeps seeing the plain copy, which 0.35.0 still writes. Only the
  fence lives in the record.
* A marker save is two writes and a forget is two writes. Both are lifecycle calls, made once per catch-up or cancel.
* A retried forget whose first attempt was applied raises the generation twice. Every attempt that read the
  generation in between is refused, and each of them costs one more replay.
* A rolling upgrade on MongoDB or Redis is safe in both directions. A 0.34.0 node ignores the record. A 0.35.0 node
  reads every marker a 0.34.0 node wrote or deleted, except a 0.34.0 write after a 0.35.0 forget, which it ignores.
* A downgrade keeps every marker that was present, since the plain copy is still there. The records stay behind
  unread. `db.<checkpointCollection>.deleteMany({"_id.catchupMarker": {$exists: true}})` removes them on MongoDB, and
  on Redis they are the keys under `occurrent:catchup-marker:`.
* The fence is only as strong as the oldest node writing the id. A 0.34.0 node's late write still brings the marker
  back for the 0.34.0 nodes, as it does today.
* The ADR 177 order is unchanged. Step 1 now reads the generation with the marker, and step 4 can be refused.
* The reactor push model still takes no `CheckpointWriteVersionSource`
  ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 29), so its marker writes pass `any()` as the
  lease condition. The generation fence does not depend on it.
