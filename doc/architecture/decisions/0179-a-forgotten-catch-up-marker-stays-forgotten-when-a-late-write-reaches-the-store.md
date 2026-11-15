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
given, under the subscription id. ADR 177 puts the marker read at step 1 of a catch-up and the marker write at step 4.

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

A tombstone left at the subscription id itself does not work on MongoDB during a rolling upgrade.
`NativeMongoCheckpointStorage.exists(id)` in 0.34.0 matches on `_id` alone, so a 0.34.0 node would read such a
document as a marker and skip a replay.

## Decision

### A generation per subscription id

Every subscription id has a generation, a counter that starts at 0 and that only a forget raises.

* Forgetting a marker raises the generation by one in one atomic write. The generation never goes down.
* A marker write stores the generation its attempt read at step 1 with the marker. That number is the marker's
  stamp.
* A marker counts as present only while its stamp equals the current generation. A missing stamp or a missing
  generation counts as 0.

A late write from an attempt that started before a forget writes a stamp the forget has already passed, so its
marker reads absent even when the store applies it after the forget. That holds with or without a lease, and whichever
node issued the write.

### `CatchupMarkerStorage`

A new interface on each stack holds the marker operations. `CheckpointStorage` itself does not change.

```java
public interface CatchupMarkerStorage extends CheckpointStorage {
    CatchupMarker readCatchupMarker(String subscriptionId);
    boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition);
    void forgetCatchupMarker(String subscriptionId);
}

// subscription/core, shared by both stacks
public record CatchupMarker(@Nullable Checkpoint checkpoint, long generation) {   // checkpoint is null when absent
    public boolean isPresent() { return checkpoint != null; }
}
```

The reactor interface mirrors it with `Mono<CatchupMarker>`, `Mono<Boolean>` and `Mono<Void>`. `readCatchupMarker`
returns the marker and the current generation as the store held them at one instant during the call, so the attempt
never pairs a marker with a generation from a different moment. On MongoDB that holds only with two reads in a fixed
order and the conditions under "The ordered reads on MongoDB". The names are provisional until the interfaces are built.

`saveCatchupMarker` returns `false` only when the lease condition refuses the write. A save whose stamp a forget has
passed returns `true`, and the marker it wrote reads absent. A store failure throws. The caller
passes the generation `readCatchupMarker` returned, and the javadoc says so. The MongoDB stores cannot check it, since
their save never reads the generation document.

Every `CheckpointStorage` that Occurrent ships implements it: `NativeMongoCheckpointStorage`,
`SpringMongoCheckpointStorage`, `ReactorCheckpointStorage`, `SpringRedisCheckpointStorage` and both
`InMemoryCheckpointStorage`s.

### Where the marker is stored

The marker stays where 0.34.0 keeps it, and the generation is stored beside it:

| Store | Marker and its stamp | Generation |
| --- | --- | --- |
| MongoDB (Native, Spring, Reactor) | the plain checkpoint document at the subscription id, with a new `generation` field | a document `{_id: {catchupMarker: "<id>"}, generation: <n>}` in the same checkpoint collection |
| Redis | the plain checkpoint key and its version key, with the stamp in the generation hash | a hash at `occurrent:catchup-marker:{<hash tag>}<sha256 of id>`, built the way the version key is, holding the generation and the stamp |
| In-memory | the plain checkpoint, with the stamp beside it | a second map under the store's existing lock |

On MongoDB:

* `readCatchupMarker` reads the plain document first and the generation document second, two finds by exact `_id`,
  both with `withReadPreference(primary())`. The marker is present only when the plain document exists and its
  `generation` equals the generation document's.
* `saveCatchupMarker` is one `findOneAndUpdate` on the plain document, the conditional `$replaceWith` a checkpoint save
  already is, with `generation` added to the new document. It never reads the generation document.
* `forgetCatchupMarker` raises the generation with an upserting `$inc`, then deletes the plain document. The delete
  is there for 0.34.0 nodes. When it fails after the `$inc`, a 0.35.0 node still reads the marker as absent. When the
  `$inc` fails, nothing has changed and the forget throws to its caller, as `delete` does today.

On Redis each call is one Lua script over the plain key, its version key and the generation hash, which share one
Cluster slot. The save writes the plain key and its version key the way a conditional save does today, and sets the
stamp only when that write happens. The forget raises the generation, removes the stamp, and deletes the plain key and
its version key, as `delete` does. In memory each call is one step under the lock.

In a mixed cluster:

* A 0.34.0 node reads and deletes the plain checkpoint as before. On MongoDB it reads a checkpoint by its position
  fields and ignores `generation`, and it never reads the generation document or hash.
* A 0.34.0 save on MongoDB replaces the whole document, and the only field it can keep from the old one is `version`, so
  a marker it writes has no stamp and counts as generation 0. Once a 0.35.0 node has forgotten the id, the generation is
  at least 1, and a late 0.34.0 write reads absent on 0.35.0 nodes.
* A 0.34.0 forget deletes the plain checkpoint, and 0.35.0 nodes then read the marker as absent. It does not raise the
  generation, so a late write after a 0.34.0 forget is not fenced, as in 0.34.0.
* On Redis a 0.34.0 save does not touch the hash, so the stamp stays. After a 0.34.0 delete and a 0.34.0 write, a
  0.35.0 node reads that marker as present when the stamp from the last 0.35.0 save equals the current generation. That
  is the unfenced 0.34.0 forget from the previous point.

Plain `read`, `exists`, `save` and `delete` keep their meaning and never touch the generation.

On a sharded checkpoint collection the generation document's `_id` is an embedded document. MongoDB accepts an `_id` of
any type other than an array, a regular expression or undefined, and a hashed index hashes an embedded document as one
value. On MongoDB 4.4.5 a two-shard cluster sharded on `{_id: "hashed"}` and one sharded on `{_id: 1}` both took such a
document next to a plain document with the same id. Inserts, finds by exact `_id`, a pipeline upsert, a `$set` upsert
and deletes by `_id` all worked, and mongos sent each find by exact `_id` to one shard. The forget's `$inc` upsert was
not part of that check, and the check has not run on MongoDB 8.0.

### The ordered reads on MongoDB

A `$in` query over both documents is not one snapshot, and on a sharded collection the two documents can sit on
different shards. Such a query can return the generation from before a forget's `$inc` and the stamp from a late save
the store applied after it. The marker then reads present although it was never present at any one instant, and the
next catch-up skips the replay the forget asked for.

Reading the plain document first and the generation document second gives an answer that held at one instant, under
two conditions:

1. A stamp is never above the current generation.
2. The generation the second read sees is never lower than the generation at the time of the first read.

Say the first read sees stamp `s` and the second sees generation `g`. When `s` equals `g`, condition 1 puts the
generation at the first read at `s` or above, and condition 2 puts it at `g` or below. It was `s`, so the marker was
present at the first read. When they differ, either the generation was already above `s` at the first read, or a
forget raised it between the reads. Right after that forget's `$inc`, every stored stamp is at most the old
generation, so the marker was absent at that instant. Either way the answer held at one instant between the two reads.

Three rules keep the conditions:

* Every save stores, as its stamp, the generation its caller read from `readCatchupMarker`, which the
  `saveCatchupMarker` javadoc requires. A 0.34.0 save and a plain `save` write no stamp, which counts as 0. This keeps
  condition 1.
* Generation documents and generation hashes are never removed, and the generation only grows through `$inc`. A removed
  generation document puts the generation back at 0, and a late 0.35.0 save applied after that leaves a stamp above
  it. A later forget can then raise the generation to that stamp, and the forgotten marker reads present until the
  forget's delete runs. This keeps both conditions.
* Both reads go to the primary. A secondary can lag and return an older generation at the second read, which breaks
  condition 2.

The fence assumes that a forget, once acknowledged, is never rolled back, which means the application writes to the
checkpoint collection with majority write concern. The marker delete in 0.34.0 rests on the same assumption. Under
`w: 1` a failover can roll back an acknowledged `$inc`, which breaks condition 2. On a sharded collection the plain
document and the generation document can sit on different shards, so a save whose stamp is the rolled-back generation
can survive on one shard while the `$inc` is rolled back on the other, which breaks condition 1 too.

Redis and in-memory need none of this, since a read there is one script or one step under the lock.

### What a catch-up does with it

1. At step 1 the attempt calls `readCatchupMarker` once. A present marker skips the replay as today. Otherwise the
   attempt keeps the generation it read.
2. At step 4 it calls `saveCatchupMarker` with that generation, plus `notOlderThan(leaseToken)` where a
   `CheckpointWriteVersionSource` gives a version and `any()` otherwise.
3. A `false` means a node with a newer lease has written a marker at the id. The attempt writes no marker, records no
   failure, and keeps delivering live events. The next catch-up for the id reads the other node's marker. Under
   competing consumers `CompetingConsumerSubscriptionModel` pauses the node that lost the lease through
   `pauseSubscription(id)`, and a pause asked for during the replay is applied when the catch-up completes. Until the
   pause is applied, both nodes can deliver the same events, which at-least-once delivery allows.
4. A store failure from `saveCatchupMarker` is handled like a `false`. The attempt logs it with the subscription id,
   writes no marker, records no failure, and keeps delivering live events. An interrupt flag on the thread is kept.
5. Every path that deletes a marker calls `forgetCatchupMarker` instead of `delete`: the `forgetCaughtUp()`
   of the reactor push model and of the reactor `CatchupProjectionFeed`, both push models' `cancelSubscription(id)`,
   and a later feed reset such as the one [#1291](https://github.com/johanhaleby/occurrent/issues/1291) asks for.

The reactor push model's `markersNotYetDeleted` and the second delete it triggers at step 1 are removed. The
generation is stored, so a restart no longer loses the record that a marker was forgotten.

Points 3 and 4 rely on ADR 177's order, where the marker is written after the replay and the drain on both engines. A
marker that is not written then costs the next catch-up one replay and loses no event. ADR 177 records
`ReactiveHandover` as writing it before the drain ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item
31), so the reactor callers change only after that write has moved.

Points 3 and 4 change a released behaviour. In 0.34.0 a blocking push model whose marker write is refused because the
lease moved on throws `CheckpointWriteConditionNotFulfilledException` out of `markCaughtUp()`, and a store failure
there throws too. `BlockingHandover` records either as a failed catch-up, and the registration then refuses every
live event until the subscription is cancelled. The shipped blocking stores retry a failed save without limit under
their default `RetryStrategy`, so a store failure reaches that state only under a bounded one, while
`ReactorCheckpointStorage` gives up after five attempts by default. Under this ADR the subscription keeps delivering
after either, and no marker is written. The changelog lists this under Changes.

Other deletes follow one rule. A delete that has to refuse a write that is already on its way to the store uses a
generation. A delete that only races other calls in the same process can use a lock. The catch-up progress checkpoint
in [#1258](https://github.com/johanhaleby/occurrent/issues/1258) is the second kind, and is fixed with
`lockHandover(id)` across its check and its save.

### Alternatives not chosen

* A record holding the marker's state apart from a copy at the plain id. The two can disagree. After a forget whose
  delete of the plain copy failed, a late write whose record write passed and whose plain write the lease condition
  refused left a present record next to a stale plain copy, and the read answered present.
* A causally consistent session with majority read and write concern around the two reads. It would keep the second read
  from seeing an older generation than the first, but a forget acknowledged at `w: 1` could still be rolled back, so the
  majority-write assumption would stay. On top of that `NativeMongoCheckpointStorage` takes a `MongoDatabase` or a
  `MongoCollection`, and neither can start a session, so every MongoDB store would need a new constructor contract.
* A read that raises the generation with `$max` above any stamp it finds. It sees a stamp only while that stamp is
  above the generation, not once a later forget has raised the generation to the same number, and it turns every read
  into a possible write.

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
  generation hash cannot share a slot with the plain checkpoint (`""`, `"{}orders"`, `"a}b{c"`), and the marker calls
  refuse those ids with an `IllegalArgumentException`, as a conditional `save` already does. Such an id keeps a
  working marker today, because a marker write with `any()` is a plain `SET`, so this is listed as breaking too.

A `null` marker storage keeps its meaning, and every catch-up replays the history.

## Consequences

* A third-party `CheckpointStorage` that was handed to a catch-up model or feed stops compiling there, or stops the
  application at startup under Spring Boot. Its author implements `CatchupMarkerStorage`, and a TCK suite on each
  stack, `CatchupMarkerStorageConformance`, checks the contract. The changelog lists this as breaking, and the
  0.35.0 migration guide shows what to implement. A variable typed `CheckpointStorage` that holds a shipped store
  needs its type changed, nothing else.
* Plain `read(id)` still returns the marker's position, including a marker the generation has fenced. Only
  `readCatchupMarker` applies the fence.
* A marker save is one write. A forget is two writes on MongoDB and one script on Redis. A read is two finds on MongoDB
  and one script on Redis.
* A late save whose stamp is older than the current generation can replace a marker written under the current one, when
  its lease condition lets it. The id then reads absent and the next catch-up replays once.
* A retried forget whose first attempt was applied raises the generation twice. Every attempt that read the
  generation in between has its marker fenced, and each of them costs one more replay.
* A rolling upgrade needs no migration. A 0.34.0 node ignores the generation, and a 0.35.0 node reads the markers a
  0.34.0 node writes and deletes, with the exceptions listed under "Where the marker is stored".
* A downgrade keeps every marker where 0.34.0 reads it. A marker the generation fenced stays too, and a 0.34.0 node
  reads it as present, the exposure 0.34.0 has to any late write. The generation documents and hashes stay behind
  unread, and the migration guide says to leave them in place. Removing one puts that id's generation back at 0, and a
  late 0.35.0 save applied afterwards leaves a stamp above it, which breaks condition 1 under "The ordered reads on
  MongoDB". On Redis the generation hashes are the keys under `occurrent:catchup-marker:`.
* The checkpoint collection, or the Redis key space, gains one generation document or hash for every subscription id
  that was ever forgotten, and a cancel does not remove it.
* The fence is only as strong as the oldest node writing the id. A 0.34.0 node's late write still brings the marker
  back for the 0.34.0 nodes, as it does today.
* The ADR 177 order is unchanged. Step 1 now reads the generation with the marker, and step 4 can be refused.
* The reactor push model still takes no `CheckpointWriteVersionSource`
  ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 29), so its marker writes pass `any()` as the
  lease condition. The generation fence does not depend on it.
