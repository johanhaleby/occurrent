# 179. A catch-up marker is the position the next catch-up resumes after

Date: 2026-11-16

## Status

Accepted. Fixes [#1150](https://github.com/johanhaleby/occurrent/issues/1150) and
[#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 23. Amends
[ADR 62](0062-pluggable-projection-event-source.md), keeps the marker order of
[ADR 177](0177-a-lifecycle-call-takes-effect-at-one-instant-as-in-some-serial-order.md) and the replay callbacks of
[ADR 110](0110-a-replay-tells-the-view-where-it-begins-and-ends.md), and keeps the lease condition of
[ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) on the blocking push model's marker
writes. Applies to both `CatchupThenPushSubscriptionModel`s, both `CatchupProjectionFeed`s and both
`DomainEventFeed`s.

## Context

In 0.34.0 a catch-up marker records that a catch-up has replayed the whole history for a subscription id, and the next
catch-up for that id skips its replay. The push models and the projection feeds keep it in the `CheckpointStorage`
they are given, under the subscription id. ADR 177 puts the marker read at step 1 of a catch-up and the marker write
at step 4.

Two paths delete the marker so that whatever runs next for the id replays the history again:

* `ReactiveHandover` deletes it through `Source.forgetCaughtUp()` when a catch-up fails, so the event whose delivery
  failed is delivered again by the replay.
* Both `CatchupThenPushSubscriptionModel`s delete it on `cancelSubscription(id)`.

Each of those deletes loses to a marker write that the store applies after it. A client can give up on a save after a
timeout while the database still runs it:

1. The marker write times out on the client.
2. The delete runs and succeeds.
3. The database applies the marker write.

The marker is back, the next catch-up skips the replay, and an event only the replay would deliver again is never
delivered. Nothing is logged, since the delete succeeded. #1293 item 23 is the restart variant on the reactor push
model. Its record of markers it has not deleted yet (`markersNotYetDeleted`) is kept in memory, so a restart forgets
it while the stored marker stays.

A marker that means "skip the whole replay" turns every lost delete and every late write into lost events. So the
fix changes what the marker means, rather than protecting the delete.

## Decision

### The marker is a resume position

Every catch-up start does this:

1. Read the stored marker P. No marker means the start has none.
2. Read the head H with `reader.currentPosition()`, before the replay.
3. Replay `PositionRange.afterPosition(P)`, or `PositionRange.fromBeginning()` when there is no marker.
4. Write H as the marker, at the point where 0.34.0 writes it. The blocking stack writes it after the drain, and the
   reactor stack before the live drain, as ADR 110 orders them.

A catch-up that fails before its marker write writes nothing, so P stays and the next start replays from P again. On
the reactor stack the write comes before the live drain, so a catch-up whose drain fails has already stored H. The
events that drain did not deliver were handed in by calls that have not completed, so their listeners don't
acknowledge them and the broker delivers them again. The write is a plain `save`.
The blocking push model keeps passing `writeConditionFor(id)`, so a lease that has moved on is still refused as
ADR 116 says. There is no other condition on it, and no fence.

`goLive()` without a catch-up still writes no marker, since it replays nothing.

### A lost or late write costs a longer replay

Every value a start writes is the head its own replay read up to, which the next section shows is safe to resume
after. So a marker that is older than it should be, because a write was lost, or because an old H reached the store after a newer one, makes the next start replay more
events than it had to. It cannot make a start skip one. The handover's existing cache de-duplicates the overlap
between the replay and the live feed within `CatchupThenLiveOptions.dedupCacheSize()`, and beyond that the
at-least-once contract already applies. The blocking push model's class javadoc says that applying the same event
twice must leave the projection as applying it once would.

All of this holds for one filter and one projection per subscription id. The marker is keyed by the id alone, so a
subscribe that reuses an id with another filter or another projection resumes after a head that the old one read,
and skips the events at or below it that only the new filter matches. 0.34.0 skipped the whole replay in that case, so
this is no regression, and the migration guide says to use a new subscription id when the filter or the projection
changes.

The three steps in the context no longer lose events. A late write after a cancel's delete restores an H that the
cancelled subscription's own replay had read up to, so the next subscribe resumes after it instead of replaying from
the beginning. That is a resume, not a skipped event.

### Why H is safe to resume after

`PositionOrderedReader.currentPosition()` says the head isn't a safe place to resume from. On MongoDB an event can
reserve a position at or below H and commit after H was read, and a read after H never returns it.

The catch-up relies on the live path for that event, which is the same reliance 0.34.0 has on every start after the
first, when it replays nothing at all:

* The push models register the live feed before the replay starts, and the replay reads H. At `4d1cd8a916` the
  blocking model calls `liveFeed.subscribeCatchupThenPush` at `CatchupThenPushSubscriptionModel.java:388` and launches
  the replay at `:393`, and the reactor model registers at `:273` and launches at `:275`. An event that commits after
  H was read commits after that registration, and the broker delivers it.
* The feeds are handed live events by the application's own broker listener. Before the feed is live, and while a
  replay runs, `acceptOrDefer` answers `DEFERRED`, the listener doesn't acknowledge the event, and the broker delivers
  it again. `acceptAndWait` keeps the event in the feed's buffer until the drain after the replay applies it. A
  blocking listener acknowledges the event once the call returns and not when it throws. A reactor listener
  acknowledges it once the returned `Mono` completes and not when it errors, and the reactor `acceptOrDefer` completes
  with `DEFERRED`.

Both need a broker that keeps the backlog of a consumer that is offline, which the blocking push model's class javadoc
already states for a restart. Reading H after the live registration is part of this decision, so a push model that
read H earlier would break it.

H, and not the head read at mark time, is what gets stored, because H is the head the replay read up to. The
reactor stack writes the marker before the live drain, so a crash between the write and the drain would leave events
above H that only the buffer held. With H stored, the next start replays them. With the head at mark time stored, only
the broker would deliver them again.

### Markers that 0.34.0 stored

A 0.34.0 marker holds the head read at mark time, which is at or above that start's H, and every event after its live
registration reached the broker. So it is a valid resume position, and a 0.35.0 start resumes after it. That replays
more than 0.34.0 did, since 0.34.0 skipped the replay. Stored markers need no migration, and the rolling upgrade has no
precondition on either stack. A 0.34.0 node that reads a marker a 0.35.0 node wrote skips its replay, as 0.34.0 always
did.

### Cancel

`cancelSubscription(id)` keeps deleting the marker, the same clean-up a cancel does for a durable subscription's
checkpoint, so the next subscribe of the id replays from the beginning. It no longer promises that. A delete a failover
loses, or a marker write the store applies after the delete, makes the next subscribe resume from the old position instead.
An application that needs a guaranteed rebuild, or that changes the filter or the projection, subscribes under a new
subscription id. The blocking push model still
deletes first and cancels nothing when the delete throws.

### The reactor failure forget goes

`ReactiveHandover.forgetCaughtUp`, `Source.forgetCaughtUp()`, the reactor push model's `markersNotYetDeleted` and the
reactor feed's forget are removed. They are in `internal` packages or private, so no public API changes. A catch-up
that fails before its marker write stores nothing, so the next start replays the failed event from P. When the drain
fails after the write, the calls that handed in the undelivered events have not completed, and the broker delivers
those events again. A live event whose delivery fails is a
broker message nobody acknowledged, and the broker delivers it again.

### The replay callbacks

`ReplayAware` and `ReactiveReplayAware` keep their 0.34.0 meaning. A start is one of three kinds:

* No replay. `goLive()` replays nothing and sends none of these callbacks, as in 0.34.0.
* A replay from the beginning, when no marker is stored. The view gets `replayStarted()`, then `replayCompleted()` or
  `replayAbandoned()`, and `alreadyDeliveredByReplay()` for a live copy of an event the replay delivered.
* A replay after a stored position. Its events reach the view the way live events do, and none of these callbacks is
  sent.

The handovers keep calling the source's `replayStarted()`, `replayCompleted()`, `replayAbandoned()` and
`alreadyDeliveredByReplay()` on every replay, so the decision sits in each source. Both handovers call
`isAlreadyCaughtUp()` before `replayStarted()`, `BlockingHandover` at line 720 and `ReactiveHandover` at line 1248 at
`4d1cd8a916`. A marker source reads P and H there, answers `false`, and keeps both for the rest of that start. It passes
the callbacks on to the view only when P is absent.

A view that rebuilds itself between `replayStarted()` and `replayCompleted()`, such as `CoalescingMaterializedView`,
keeps seeing a rebuild only when it gets one. `RecordingMaterializedView` deletes every append it has recorded for the
projection after `replayStarted()`, so sending it for a replay after P would drop the records of appends the view still
holds. Without it the view records each replayed append as it applies it, and a live copy that the handover drops as a
duplicate needs no record of its own.

The `CatchupListener` that `listenForCatchup` gives a push model is unchanged. In 0.34.0 it already gets
`catchupStarted` from the thread that launches the replay and `historyRead` before every drain, including the drain of
a start that replays nothing (`BlockingHandover.java:1295` at `4d1cd8a916`), so it tells the listener that a catch-up
has begun, not that the view is rebuilt. A replay from the beginning that a stop interrupts sends `catchupStarted` and
no `historyRead`, and the recording records nothing until `historyRead` for a later catch-up. If the next start resumed
after a position another node stored and sent neither, the recording would never record again.

### What was tried and dropped

An earlier version of this ADR kept the marker as a completion flag and fenced it. A generation per subscription id
changed only on a forget, and a marker read as present only while the generation it was saved under was the current
one. It lived behind a new `CatchupMarkerStorage` interface, with a conformance suite, and every marker-writing constructor
took that type. MongoDB raised the generation with `$inc` and then switched to random generations, wrote the forget
with `w: "majority"` and `j: true`, and refused to forget on a server without journaling. Redis kept the generation in
a hash next to the checkpoint and wrote a random token.

That design needs a forget that stays done once acknowledged. `InMemoryCheckpointStorage` keeps one, and a restart
loses every marker it holds anyway. The MongoDB and Redis storages can't promise one, and the reactor forget can give
up:

* Redis acknowledges a write before its replicas have it, so a failover loses an acknowledged forget. `WAIT` would
  narrow the window, and the Redis documentation calls it best effort.
* MongoDB's in-memory storage engine acknowledges `j: true` without a journal, and a replica set with an in-memory
  voting member must set `writeConcernMajorityJournalDefault` to `false`, so a majority write can roll back. The store
  can't tell which engine the server runs.
* `ReactiveHandover`'s failure forget retries three times and then only logs, so the marker stays.

Each of those losses skips a replay and loses events, and the fence also broke the API of every third-party
`CheckpointStorage` and needed a rolling-upgrade precondition. With the marker as a resume position, the same losses
cost a longer replay, the API is unchanged, and the upgrade needs nothing.

### Alternatives not chosen

* The fence above.
* A tombstone at the subscription id after a forget. On MongoDB, `NativeMongoCheckpointStorage.exists(id)` in 0.34.0
  matches on `_id` alone, so a 0.34.0 node would read the tombstone as a marker during a rolling upgrade.
* The write version as the fence. Under competing consumers a cancel followed by a new subscribe under the same lease
  gives both the same token, so `notOlderThan(leaseToken)` accepts both writes or refuses both.
* No marker at all, so every start replays the whole history. It is the safest, and it makes every restart of a large
  projection replay everything, which is what the marker exists to avoid.

## Consequences

* Every start after the first replays the events since the previous start's H. That costs a read of those events, and
  after a restart it applies again the events the live feed delivered before the restart, since the de-duplication
  cache is in memory. The at-least-once contract already allows that, and a projection has to tolerate it.
* `cancelSubscription(id)` no longer guarantees a full replay on the next subscribe. The changelog lists this as
  breaking, and the 0.35.0 migration guide says to use a new subscription id for a rebuild.
* `CheckpointStorage` is unchanged. A third-party storage keeps working as a marker store, and a store that loses or
  reorders writes costs replays, not events.
* MongoDB's in-memory storage engine and a Redis failover are no longer a reason a marker can lose events. They can
  still lose a delete, which makes a subscribe after a cancel resume instead of rebuild.
* The ADR 177 order is unchanged. Step 1 reads the marker as a position, and step 4 writes H.
* The reactor push model still takes no `CheckpointWriteVersionSource`
  ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 29), so its marker writes pass `any()`. Nothing
  here depends on it.
