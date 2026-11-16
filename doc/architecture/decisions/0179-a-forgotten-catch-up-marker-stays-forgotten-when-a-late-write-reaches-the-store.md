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

Every subscription id has a generation, a number that starts at 0 and that only a forget changes.

* Forgetting a marker changes the generation in one atomic write. On MongoDB and in memory the generation is a
  counter that the forget raises by one, and it never goes down. On Redis the forget sets a fresh random token, for the
  reason under "Where the marker is stored".
* A marker write stores the generation its attempt read at step 1 with the marker. That number is the marker's
  stamp.
* A marker counts as present only while its stamp equals the current generation. A missing stamp or a missing
  generation counts as 0.

A late write from an attempt that started before a forget writes a stamp the forget has already replaced, so its
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
returns the marker and the generation as the store held them at one instant, so the attempt never pairs a marker with a
generation from a different moment. On MongoDB and in memory it also sees every forget acknowledged before the call
started. On MongoDB that holds only with two reads in a fixed order and the conditions under "The ordered reads on
MongoDB". The names are provisional until the interfaces are built.

`saveCatchupMarker` returns `false` only when the lease condition refuses the write. A save whose stamp a forget has
replaced returns `true`, and the marker it wrote reads absent. A store failure throws. The caller
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
  both with `withReadPreference(primary())`. The plain read uses `ReadConcern.MAJORITY`. The generation read uses
  `ReadConcern.LINEARIZABLE` with a `maxTimeMS`. The marker is present only when the plain document exists and its
  `generation` equals the generation document's.
* `saveCatchupMarker` is one `findOneAndUpdate` on the plain document, the conditional `$replaceWith` a checkpoint save
  already is, with `generation` added to the new document. It never reads the generation document, and it uses the
  write concern of the collection the application gave the store.
* `forgetCatchupMarker` raises the generation with an upserting `$inc`, then deletes the plain document. Both writes
  use `WriteConcern.MAJORITY` with a `wtimeout`, whatever write concern the application set on its collection. Without
  the `wtimeout`, a forget would wait forever while no majority is reachable. The delete is there for 0.34.0 nodes.
  When it fails after the `$inc`, a 0.35.0 node still reads the marker as absent. When the `$inc` fails, or its
  `wtimeout` passes, the store's `RetryStrategy` sends it again, and the delete waits until an `$inc` succeeds. The
  default `RetryStrategy` has no attempt limit, so the forget returns only once a majority has the raise. Under a
  `RetryStrategy` that gives up, or once the store shuts down, which ends every retry, the forget throws to its caller
  without sending the delete, as `delete` throws today.
  An `$inc` that passed its `wtimeout` may still have raised the generation, and the next attempt raises it once more.
* A client with a `timeoutMS` makes the driver send no `wtimeout` and no `maxTimeMS`. A `timeoutMS` above 0 then
  bounds each forget write and the generation read. A `timeoutMS` of 0 waits forever, so the store gives those
  commands an operation timeout of 10 seconds, the way ADR 149 bounds the position reservation of an append. Without
  it, a forget or a read on such a client blocked past 45 seconds while no majority answered (checked on 4.4.5 with
  driver 5.8.0).
* On a standalone server, MongoDB refuses `LINEARIZABLE` with error code 123 (checked on 4.4.5). The store then reads
  the generation with `MAJORITY` and keeps doing so for its lifetime, without trying `LINEARIZABLE` again. A standalone
  server has no failover and no rollback, so the two read concerns return the same generation there. The store learns
  the topology from that first refusal, because a store built from a `MongoCollection` has no client to ask.

On Redis each call is one Lua script over the plain key, its version key and the generation hash, which share one
Cluster slot. The save writes the plain key and its version key the way a conditional save does today, and sets the
stamp only when that write happens. The forget sets the generation to a token, removes the stamp, and deletes the plain
key and its version key, as `delete` does. The store draws the token in Java with `SecureRandom`, as a `long` other
than 0, and passes it to the script. In memory each call is one step under the lock.

A counter does not work on Redis. Redis replicates to its replicas asynchronously, so a failover can lose an increment
the old master acknowledged, and the counter then repeats a value:

1. A forget raises the generation from 4 to 5 on the master, and an attempt reads 5.
2. The master fails before the replica has the forget, and the replica becomes master with the generation at 4.
3. A second forget raises the generation from 4 to 5 again.
4. The attempt's late save is applied with stamp 5, and the marker reads present although the second forget came after
   the attempt started.

A random token is not repeated by a lost write, so the late save's stamp differs from the token the second forget set,
and the marker reads absent. The read is one script, so it needs no order between two reads. The
generation hash is never removed, since a removed hash puts the generation back at 0 and a late save from an attempt
that read 0 would read present.

A failover on Redis can still lose any acknowledged checkpoint write, a forget included, as in 0.34.0. A lost forget
brings its marker back, and the next catch-up for the id skips the replay that forget asked for. A master that a
failover has replaced can also keep answering for a moment, and a read there misses a forget made on the new master, as
a read in 0.34.0 does. The token only keeps a late write from bringing a marker back after a forget that Redis kept.
[#1541](https://github.com/johanhaleby/occurrent/issues/1541) tracks documenting the lost writes for every Redis
checkpoint save and delete, in the documentation and the `SpringRedisCheckpointStorage` javadoc.

Occurrent accepts this loss on Redis. The Redis documentation for [`WAIT`](https://redis.io/docs/latest/commands/wait/)
says it does not make Redis a strongly consistent store, and that a failover can still lose a write that several
replicas received. The other Redis alternatives under "Alternatives not chosen", a counter with the replication id, a
time-based token and a write-time check, only change how the generation is formed, so none of them brings a lost forget
back. An application that cannot accept a lost forget keeps its catch-up markers in MongoDB, where the forget is written
with `WriteConcern.MAJORITY`.

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
and deletes by `_id` all worked, and mongos sent each find by exact `_id` to one shard. On the hashed cluster the
forget's `$inc` upsert and the delete with `w: "majority"`, and finds by exact `_id` with `MAJORITY` and with
`LINEARIZABLE`, worked too. The check has not run on MongoDB 8.0.

### The ordered reads on MongoDB

A `$in` query over both documents is not one snapshot, and on a sharded collection the two documents can sit on
different shards. Such a query can return the generation from before a forget's `$inc` and the stamp from a late save
the store applied after it. The marker then reads present although it was never present at any one instant, and the
next catch-up skips the replay the forget asked for.

Reading the plain document first and the generation document second gives an answer that held at one instant no
later than the second read, under two conditions:

1. A stamp is never above the current generation.
2. The generation the second read sees is never lower than the generation at the instant the first read reflects.

Say the first read sees stamp `s` and the second sees generation `g`. When `s` equals `g`, condition 1 puts the
generation at the instant of the first read at `s` or above, and condition 2 puts it at `g` or below. It was `s`, so
the marker was present then. When they differ, either the generation was already above `s` at that instant, or a
forget raised it before the second read. Right after that forget's `$inc`, every stored stamp is at most the old
generation, so the marker was absent at that instant. Either way the answer held at one instant.

Here "the generation" means the value a majority of the replica set has committed. Five rules keep the conditions,
across a failover too:

* Every save stores, as its stamp, the generation its caller read from `readCatchupMarker`, which the
  `saveCatchupMarker` javadoc requires. A 0.34.0 save and a plain `save` write no stamp, which counts as 0.
* The generation read is `LINEARIZABLE`. It returns only what a majority has committed, so a stamp never comes from an
  `$inc` that a failover later rolls back. It also reflects every write a majority committed before the read started.
  A node that has lost an election and does not know it yet cannot get a majority to confirm the read, so it fails the
  read instead of answering from old data. `MAJORITY` alone does not give the second guarantee. A primary that a newer
  election has replaced can keep serving majority reads from its last commit point until it steps down.
* The forget is written with `WriteConcern.MAJORITY`. A forget the store acknowledged is committed and never rolls
  back.
* Generation documents are never removed, and the generation only grows through `$inc`. A removed
  generation document puts the generation back at 0, and a late 0.35.0 save applied after that writes a stamp above
  it. A later forget can then raise the generation to that stamp, and the forgotten marker reads present until the
  forget's delete runs.
* The plain read is `MAJORITY`, so it never sees a stamp from a save that a failover later rolls back. It may come
  from a primary that an election has replaced and reflect an earlier instant. The proof above allows that, since
  the second read is the one that has to be current.

The first, second and fourth rules keep condition 1. The second and fourth keep condition 2. The fifth makes the first
read reflect a committed instant, which the proof needs. The third makes an acknowledged forget a committed one, so the
generation read of a `readCatchupMarker` that starts after it sees it, and the marker reads absent.

Without these concerns the fence would break on a failover. With MongoDB's default `local` read concern a primary can
return an `$inc` that is not committed yet. The attempt reads generation 1 from it, the primary loses an election and
the `$inc` rolls back to 0, and the attempt's save is applied on the new primary with stamp 1. A later forget raises the
generation to 1 and the forgotten marker reads present.

0.34.0 has the same gap without a late write. Its `exists` reads the plain document at the default `local` read
concern, so a primary that an election has replaced can still return the document a forget on the new primary
deleted. The concerns close that gap for `readCatchupMarker`, and plain `exists` keeps it.

The marker save keeps the collection's write concern, because a rolled-back save only loses the marker. The next
catch-up for the id replays once, and no event is lost. Forcing `MAJORITY` on it would make every catch-up wait for a
majority at its end and protect nothing the fence needs.

In memory a read is one step under the lock, and on Redis it is one script, as the previous section says.

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
  from seeing an older generation than the first. `NativeMongoCheckpointStorage` takes a `MongoDatabase` or a
  `MongoCollection`, and neither can start a session, so every MongoDB store would need a new constructor contract.
* A write acknowledged by a majority in place of the `LINEARIZABLE` read, such as a no-op update of the generation
  document before reading it with `MAJORITY`. Every catch-up start would then write. An acknowledged write proves a
  majority took it, but a `MAJORITY` read after it can still go to a primary that an election has replaced, so the
  write alone does not make the read current.
* The marker and the generation in one document. One read would see both at one instant, but it still has to be a
  current read for the same reason, and a 0.34.0 save replaces the whole plain document, so during a rolling upgrade a
  0.34.0 node would reset the generation.
* A read that raises the generation with `$max` above any stamp it finds. It sees a stamp only while that stamp is
  above the generation, not once a later forget has raised the generation to the same number, and it turns every read
  into a possible write.
* On Redis, `WAIT` after the forget script, to hold the forget until replicas have it. The Redis documentation calls
  `WAIT` best effort. A failover can still promote a replica that lacks the forget. It would narrow the loss without
  closing it, and every forget would wait for replicas.
* On Redis, a counter combined with the master's replication id, which changes on a failover. The script would have to
  read the replication id on every forget, and a random token is unique without it.
* On Redis, a token based on the time. Two forgets in the same clock tick, or on a master whose clock is behind the old
  master's, can produce the same token.
* On Redis, a save that checks its stamp against the generation when it writes, and refuses a stale one. With a
  counter it accepts the late save in the example above, since the stamp equals the repeated value. With a token the
  read already reports such a marker as absent.

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
* On a replica set or a sharded cluster, a forget waits until a majority has its two writes, and a `readCatchupMarker`
  waits for the round trip a `LINEARIZABLE` read makes to a majority. A standalone server answers as before. When no
  majority is reachable, each forget attempt fails after its `wtimeout` and each read attempt after its `maxTimeMS`,
  or after the client's `timeoutMS` when that is above 0.
  The default `RetryStrategy` then keeps trying, so the call returns once a majority is back, and only a
  `RetryStrategy` that gives up, or the store shutting down, makes it throw. The changelog
  says the forget ignores a weaker write concern on the application's collection.
* A late save whose stamp is not the current generation can replace a marker written under the current one, when
  its lease condition lets it. The id then reads absent and the next catch-up replays once.
* A retried forget whose first attempt was applied changes the generation twice. Every attempt that read the
  generation in between has its marker fenced, and each of them costs one more replay.
* During a rolling upgrade a forgotten marker can come back. The fence covers every forget a 0.35.0 node makes,
  but a forget made on a 0.34.0 node, and a read on a 0.34.0 node, behave as in 0.34.0 until every
  node runs 0.35.0. A 0.34.0 forget does not change the generation, so a late write after it brings the marker back
  as it does today, and the 0.35.0 nodes read that marker as present too. It affects an application that cancels a
  catch-up subscription on a 0.34.0 node during the rollout, while a marker write for the same id is still on its way
  to the store. The next subscribe of that id then skips the replay it needed. No new case starts once the last
  0.34.0 node is gone, but a marker that came back stays present until a 0.35.0 node forgets the id. On MongoDB this
  happens only for an id no 0.35.0 node has forgotten, since a 0.34.0 save drops the stamp and any generation above
  0 then fences the marker. On Redis it happens unless the last marker call a 0.35.0 node made on the id was a
  forget, because the stamp sits in the generation hash, which a 0.34.0 save and a 0.34.0 delete leave alone. A
  0.34.0 node's late write after a 0.35.0 forget still brings the marker back for the 0.34.0 nodes. No mixed order makes a node read a marker as present where an
  all-0.34.0 deployment would read it absent. A 0.35.0 node saves and deletes the plain checkpoint under the same
  conditions as 0.34.0, and on read it only adds the stamp check, which can turn present into absent and never the other
  way. The one difference is a forget whose `$inc` has no majority yet. It sends no delete until a retried `$inc` gets
  one, so the 0.34.0 nodes keep reading the marker until then, and go on reading it when the forget throws instead. The list under "In a mixed cluster" gives each case.
* A downgrade keeps every marker where 0.34.0 reads it. A marker the generation fenced stays too, and a 0.34.0 node
  reads it as present, the exposure 0.34.0 has to any late write. The generation documents and hashes stay behind
  unread, and the migration guide says to leave them in place. Removing one puts that id's generation back at 0. On
  MongoDB a late 0.35.0 save applied afterwards writes a stamp above it, which breaks condition 1 under "The ordered
  reads on MongoDB", and on Redis a late save from an attempt that read 0 reads present. On Redis the generation hashes
  are the keys under `occurrent:catchup-marker:`.
* The checkpoint collection, or the Redis key space, gains one generation document or hash for every subscription id
  that was ever forgotten, and a cancel does not remove it.
* The ADR 177 order is unchanged. Step 1 now reads the generation with the marker, and step 4 can be refused.
* The reactor push model still takes no `CheckpointWriteVersionSource`
  ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 29), so its marker writes pass `any()` as the
  lease condition. The generation fence does not depend on it.
