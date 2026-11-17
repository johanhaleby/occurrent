# 179. A catch-up marker means done on the blocking stack and a resume position on the reactor stack

Date: 2026-11-16

## Status

Accepted. Decides [#1150](https://github.com/johanhaleby/occurrent/issues/1150) and
[#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 23. Amends
[ADR 62](0062-pluggable-projection-event-source.md) and
[ADR 133](0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md) for the reactor stack,
amends the marker order of
[ADR 177](0177-a-lifecycle-call-takes-effect-at-one-instant-as-in-some-serial-order.md) for the reactor stack, keeps
the replay callbacks of
[ADR 110](0110-a-replay-tells-the-view-where-it-begins-and-ends.md), and keeps the lease condition of
[ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) on the blocking push model's marker
writes. Applies to both `CatchupThenPushSubscriptionModel`s, both `CatchupProjectionFeed`s and both
`DomainEventFeed`s.

## Context

In 0.34.0 a catch-up marker records that a catch-up has replayed the whole history for a subscription id, and the next
catch-up for that id skips its replay. The push models and the projection feeds keep it in the `CheckpointStorage`
they are given, under the subscription id. ADR 177 puts the marker read at step 1 of a catch-up and the marker write
at step 4. The blocking push model's class javadoc says it plainly: "The stored value marks completion, it is not a
live resume position." ADR 62 gives the same meaning.

Two paths delete the marker so that whatever runs next for the id replays the history again:

* `ReactiveHandover` deletes it through `Source.forgetCaughtUp()` when a catch-up or a delivery fails, so the event
  whose delivery failed is delivered again by the replay.
* Both `CatchupThenPushSubscriptionModel`s delete it on `cancelSubscription(id)`.

Each of those deletes loses to a marker write that the store applies after it. A client can give up on a save after a
timeout while the database still runs it:

1. The marker write times out on the client.
2. The delete runs and succeeds.
3. The database applies the marker write.

The marker is back, and the next catch-up skips the replay. On the reactor failure path that loses events. The event
whose delivery failed was only to be delivered again by that replay, and nothing is logged, since the delete
succeeded. #1293 item 23 is the restart variant on the reactor push model. Its record of markers it has not deleted
yet (`markersNotYetDeleted`) is kept in memory, so a restart forgets it while the stored marker stays.

The blocking stack has no failure delete. `BlockingHandover` writes the marker only after the replay and the drain
have delivered every event, and a catch-up or a delivery that fails does not touch the marker. Its only delete is
`cancelSubscription(id)`.

## Decision

### The blocking stack keeps the 0.34.0 marker

On the blocking `CatchupThenPushSubscriptionModel`, `CatchupProjectionFeed` and `DomainEventFeed`, a stored marker
means the id has caught up. A start that finds it skips its replay and goes live, and the broker delivers what the
subscription missed while it was down. A start that finds none replays the whole history, and stores the marker once
the replay and the drain are done. The stored value is not read as a position.

The blocking push model deletes the marker only in `cancelSubscription(id)`. The delete and the marker write take the
same per-id lock (`markerLocks`), and a write checks under that lock that its replay still owns the id and saves the
marker under it too. ADR 177 replaces `markerLocks` with the replay turn
([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 33), and the save has to stay inside it. So a write
that is running when a cancel arrives finishes before the delete, and a write that comes after the cancel finds its
replay gone and writes nothing. What the lock cannot order is the store applying a write late, after the delete has
returned. The next subscribe of the id then skips its replay, see "Cancel".

### The reactor stack resumes after its marker

On the reactor `CatchupThenPushSubscriptionModel`, `CatchupProjectionFeed` and `DomainEventFeed`, every catch-up
start does this:

1. Read the stored marker P. No marker means the start has none.
2. Read the head H with `reader.currentPosition()`, before the replay.
3. Replay `PositionRange.afterPosition(P)`, or `PositionRange.fromBeginning()` when there is no marker.
4. Write H as the marker, before the live drain, at the point where 0.34.0 writes it, as ADR 110 orders it.

A catch-up that one of these sources asks for from its own code, such as a view calling `catchUp()` from the code that applies an event,
reads P and H when it gets its turn to replay, not when it is asked. It then resumes after the marker the catch-up
that asked has stored.

The reactor stack deletes the marker when a catch-up or a delivery fails, as in 0.34.0, and "Fed-back events on the
reactor stack" below states that rule and what a delete that fails does. Its write comes before the live drain, so a
catch-up whose drain fails has already stored H, and that delete removes it. The events that drain did not deliver
were handed in by calls that have not completed, so their listeners don't acknowledge them and the broker delivers
them again. An event a handler fed back is the exception, and the same section says why the reactor stack does not
write H while one waits in the drain.

The write takes no new condition.

ADR 62 rejected a resume watermark that the live feed moves with each delivery. Occurrent reserves positions outside
the write transaction, so a missing position in the delivery stream is either a hole that fills in later or a
permanent gap, and the feed cannot tell which. The reactor marker is not that watermark. Only a catch-up reads or
stores it, it is read before the replay, and it holds a head that was read, never a position derived from
deliveries. Live resume stays the broker's job, and the overlap between the replay and the live feed is still
de-duplicated by event id.

A checkpoint stored under the id that is not a global position, such as a resume token a durable subscription stored
when it used the id before, fails a reactor catch-up with an `IllegalStateException` that names the id and says to delete the
checkpoint or subscribe under a new id. Reading it as no marker would replay from the beginning and then write H over
a checkpoint that something else owns, so the catch-up fails instead, and that checkpoint stays stored. On the
blocking stack any stored checkpoint still reads as a finished catch-up, as in 0.34.0.

The check looks at the stored value, not at who stored it. A durable subscription that stores a global position, such
as the reactor catch-up subscription while it catches up, leaves a checkpoint the reactor catch-up reads as its marker.
That catch-up resumes after it and then replaces it with H. So a subscription id must not be shared between a durable
subscription and a catch-up-then-push model or a projection feed, and the 0.35.0 migration guide has to say so. 0.34.0
read any checkpoint under the id as the marker as well.
[#1564](https://github.com/johanhaleby/occurrent/issues/1564) is about storing markers under a key of their own.

`goLive()` without a catch-up still writes no marker on either stack, since it replays nothing.

### Why the stacks differ

The reactor failure delete is what makes a done flag lose events there. Under a done flag, a marker write the store
loses or applies after that delete makes the next start skip its replay, and the event whose delivery failed is never
delivered again. That is #1150. Under a resume position the same late write stores an H that its own catch-up read
before its replay. The write comes before the drain, so the live buffer can still hold an event at or below H when the
store applies it. The next start resumes after H and still skips nothing, since the broker delivers again what that drain did not
deliver, and the fed-back rule below keeps H unwritten while an event no broker delivers again waits. The next
section shows why that holds.

The blocking stack has no failure delete, so a done flag loses nothing there that the broker does not deliver again.
A resume position would cost it a replay on every restart, of every event written since the previous start, with
nothing in return. A push `@Saga` runs on the blocking stack only, and such a replay recreates an instance that its
TTL already removed, from its start event, and dispatches that instance's commands again. The first 0.35.0 start
would replay everything written since the id first caught up, since that is when a 0.34.0 marker was written.

### Why H is safe to resume after

`PositionOrderedReader.currentPosition()` says the head isn't a safe place to resume from. On MongoDB an event can
reserve a position at or below H and commit after H was read, and a read after H never returns it.

The reactor catch-up relies on the live path for that event, which is the same reliance 0.34.0 has on every start
after the first, when it replays nothing at all:

* The reactor push model registers the live feed before the replay starts, and the replay reads H. At `4d1cd8a916` it
  registers at `CatchupThenPushSubscriptionModel.java:273` and launches the replay at `:275`. An event that commits
  after H was read commits after that registration, and the broker delivers it.
* The feeds are handed live events by the application's own broker listener. Before the feed is live, and while a
  replay runs, `acceptOrDefer` completes with `DEFERRED`, the listener doesn't acknowledge the event, and the broker
  delivers it again. `acceptAndWait` keeps the event in the feed's buffer until the drain after the replay applies it,
  and the listener acknowledges it once the returned `Mono` completes and not when it errors.

Both need a broker that keeps the backlog of a consumer that is offline, which the push model's class javadoc already
states for a restart. Reading H after the live registration is part of this decision, so a push model that read H
earlier would break it.

H, and not the head read at mark time, is what gets stored, because H is the head the replay read up to. The
reactor stack writes the marker before the live drain, so a crash between the write and the drain would leave events
above H that only the buffer held. With H stored, the next start replays them. With the head at mark time stored, only
the broker would deliver them again.

Every value a reactor start writes is the head its own replay read up to. So a marker that is older than it should be,
because a write was lost, or because an old H reached the store after a newer one, makes the next start replay more
events than it had to. It cannot make a start skip one. The handover's cache de-duplicates the overlap between the
replay and the live feed within `CatchupThenLiveOptions.dedupCacheSize()`, and beyond that the at-least-once contract
already applies.

### Markers that 0.34.0 stored

On the blocking stack a 0.34.0 marker keeps its meaning, so the upgrade changes nothing there.

On the reactor stack a 0.34.0 marker holds the head read at mark time, which is at or above that start's H. 0.34.0 read
it as a finished catch-up and skipped the replay, so a 0.35.0 start that resumes after it replays at least as much as
0.34.0 did, and the upgrade loses no event that 0.34.0 had not lost already. An event a handler fed back can sit at or
below that head without having been applied, when its delivery failed and the delete that follows was lost, or the
process stopped before it delivered the event. No broker delivers such an event again, see "Fed-back events on the
reactor stack", so 0.34.0 had lost it, and a 0.35.0 start that resumes after the marker does not bring it back. Only
a rebuild under a new subscription id applies such an event, since it replays the whole history.
Stored markers need no migration. A 0.34.0 node that reads a marker a 0.35.0 node wrote skips its replay, as 0.34.0
always did.

The marker was written by the first catch-up of the id, since a 0.34.0 start that found it skipped its replay and
never wrote it again. So the first 0.35.0 reactor start replays every event written since the id first caught up,
which for an id that has run for long is most of the history. That replay is after a stored position, so it sends no
`replayStarted()`, and a view that batches its writes during a replay, such as the one `Projections` builds, writes per
event through it. After that, each restart replays the events written since the previous start, the ones handled live
while it ran included, since the marker holds the head read at that start.

A reactor `@Projection` with `source = PUSH` waits for its catch-up at every start unless it sets
`startupMode = BACKGROUND`, and the marker is stored only once the replay is done. The first 0.35.0 start has the
longest replay, and every later start replays the events since the previous one. A startup probe that restarts the
application during such a replay keeps the marker from being written, so the next start replays the same events
again. The 0.35.0 migration guide has to recommend `startupMode = BACKGROUND` for every start, not only the first.
Storing the marker during the replay would shorten the replay after such a restart, and is not part of this decision.

### Cancel

`cancelSubscription(id)` keeps deleting the marker on both push models, the same clean-up a cancel does for a durable
subscription's checkpoint, so the next subscribe of the id replays from the beginning. It no longer promises that. A
delete the store loses, or a marker write the store applies after the delete, keeps a marker stored for the next subscribe.
On the blocking stack that subscribe skips its replay, and on the reactor stack it resumes after the old position. A
delete that throws on the reactor push model works as in 0.34.0. The next subscribe of the id in the same process
tries the delete again and replays from the beginning, see "Fed-back events on the reactor stack". The blocking push
model still deletes first and cancels nothing when the delete throws.

An application that needs a guaranteed rebuild, or that changes the filter or the projection, subscribes under a new
subscription id. The marker is keyed by the id alone, so a subscribe that reuses an id with another filter or another
projection skips the replay on the blocking stack, and resumes after a head the old one read on the reactor stack. On
either stack it misses the earlier events that only the new filter matches.
[#1553](https://github.com/johanhaleby/occurrent/issues/1553) is about storing a fingerprint of the filter and the
projection with the marker, so a start with a changed one replays from the beginning.

A push `@Saga` is where a new id costs most. A new id replays the whole history into the saga, so every instance that
its TTL removed is recreated from its start event and dispatches its commands again, as the `SagaStateStore` javadoc
warns for a redelivery. Rebuilding a saga under a new id needs completed instances kept for as long as the history it
replays.

### Fed-back events on the reactor stack

A handler or a view can write an event to the event store and feed it back, and `ReactiveHandover` answers such an
event as taken in once it is queued. Say the live delivery of an event W is running when a catch-up A reads H. A holds
live delivery back and waits for W's delivery to end before it replays, so that delivery is over before A writes H.
What W's handler fed back is not. Take an event X that W's handler wrote and fed back, at a position at or below H,
whose write committed after A's replay read past that position. A's replay does not read X, X waits in the live feed
behind A's hold, and it is delivered only after A's write of H has completed, since the hold lasts until then. If that
delivery fails, H is stored, the next start resumes after it, and nothing delivers X again.

So `ReactiveHandover` does not write H while an event its own code fed, and that A's replay did not deliver, waits in
the live feed. P stays stored and the next start replays after P. X lies after P, because an X written before the
catch-up that stored P read its head would have been replayed by that catch-up or kept it from storing P. An event A's
replay did read is delivered before H is written, and its live copy is skipped as a repeat. Only a catch-up that runs
after the source has gone live can meet such an event, which is a feed's `catchUp()` after `goLive()` or one a view
asks for. A push model runs its catch-up before any live event is handled.

`ReactiveHandover` also keeps deleting the marker through `Source.forgetCaughtUp()` when a delivery or a catch-up
fails, as in 0.34.0, as a backstop behind that rule. It tries the delete four times and then logs. The reactor push
model keeps `markersNotYetDeleted` from 0.34.0. An id whose last delete threw is in it, and the next catch-up of that
id in the same process tries the delete again and replays from the beginning instead of resuming after the marker that
is still stored. The set is kept in memory, so a restart empties it and the next start resumes after that marker. When
the checkpoint stored under the id is not a global position, such as a durable subscription's resume token, the
catch-up fails naming the id and the backstop deletes nothing, since that checkpoint is not the catch-up's to delete.
A durable subscription's checkpoint that is a global position passes that check and is deleted like a marker, which is
one more reason an id must not be shared.
[#1552](https://github.com/johanhaleby/occurrent/issues/1552) tracks delivering the fed-back events before the marker
is written, as `BlockingHandover` does, which would let the marker move on under steady fed-back traffic.

### The replay callbacks

`ReplayAware` and `ReactiveReplayAware` keep their 0.34.0 meaning. On the blocking stack every replay is from the
beginning, as in 0.34.0. On the reactor stack a start is one of three kinds:

* No replay. `goLive()` replays nothing and sends none of these callbacks, as in 0.34.0.
* A replay from the beginning, when no marker is stored. The view gets `replayStarted()`, then `replayCompleted()` or
  `replayAbandoned()`, and `alreadyDeliveredByReplay()` for a live copy of an event the replay delivered.
* A replay after a stored position. Its events reach the view the way live events do, and none of these callbacks is
  sent.

`ReactiveHandover` keeps calling the source's callbacks on every replay, so the decision sits in each reactor source.
`RecordingReactiveUpdate` deletes every append it has recorded for the projection after `replayStarted()`, so sending
it for a replay after P would drop the records of appends the view still holds. The price is the per-event writes
"Markers that 0.34.0 stored" describes.

A replay after P is plain delivery for the applied-append recorder too. Outside a catch-up the recorder records every
append the projection applies, so it records each append such a replay applies. ADR 132 decision 6 still holds. It
keeps a full replay, the history a catch-up reads from the beginning, from recording anything, and a replay after P is
not one. Its events are the ones written since the previous start, which is the volume the same projection records
when it handles them live. Both MongoDB stores upsert one document per projection id and append id, so an append the
projection recorded before the restart costs one more write and adds no document. A new document is written for each
replayed append that has no record yet, which on the first 0.35.0 start can be most of the appends since the id first
caught up, and the TTL index on `recordedAt` removes them as it removes every other record.

The `CatchupListener` that `listenForCatchup` gives a push model follows the same rule on the reactor stack. The
reactor push model sends `catchupStarted`, `historyRead` and `alreadyDeliveredByReplay` only for a replay from the
beginning. A recorder that listens there, as the reactive Spring Boot starter's does, then keeps its records over a
restart that replays after P, and a wait for an append the projection applied before the restart can still finish.
In 0.34.0 every reactor start sent `catchupStarted`, and the recorder deleted those records. The blocking push model
keeps sending both on every start, as in 0.34.0, including a start that finds the marker and replays nothing, so
there they tell the listener that a catch-up has begun, not that the view is rebuilt.

### What was tried and dropped

An earlier version of this ADR kept the marker as a completion flag on both stacks and fenced it. A generation per
subscription id changed only on a forget, and a marker read as present only while the generation it was saved under
was the current one. It lived behind a new `CatchupMarkerStorage` interface, with a conformance suite, and every
marker-writing constructor took that type. MongoDB raised the generation with `$inc` and then switched to random
generations, wrote the forget with `w: "majority"` and `j: true`, and refused to forget on a server without
journaling. Redis kept the generation in a hash next to the checkpoint and wrote a random token.

That design needs a forget that stays done once acknowledged. `InMemoryCheckpointStorage` keeps one, and a restart
loses every marker it holds anyway. The MongoDB and Redis storages can't promise one, and the reactor forget can give
up:

* Redis acknowledges a write before its replicas have it, so a failover loses an acknowledged forget. `WAIT` would
  narrow the window, and the Redis documentation calls it best effort.
* MongoDB's in-memory storage engine acknowledges `j: true` without a journal, and a replica set with an in-memory
  voting member must set `writeConcernMajorityJournalDefault` to `false`, so a majority write can roll back. The store
  can't tell which engine the server runs.
* `ReactiveHandover`'s failure forget retries three times and then only logs, so the marker stays.

Each of those losses skips a replay and loses events on the reactor failure path, and the fence also broke the API of
every third-party `CheckpointStorage` and needed a rolling-upgrade precondition.

### Alternatives not chosen

* The fence above.
* A done flag on both stacks, as in 0.34.0. On the reactor stack a marker write that the store applies after the failure delete
  makes the next start skip its replay and loses the event whose delivery failed, which is #1150.
* A resume position on both stacks. The blocking stack has no failure delete for it to fix, and every restart would
  replay the events since the previous start, push sagas included.
* A watermark that moves with each applied delivery. ADR 62 rejects it, since a missing position can be a hole or a
  gap.
* A tombstone at the subscription id after a forget. On MongoDB, `NativeMongoCheckpointStorage.exists(id)` in 0.34.0
  matches on `_id` alone, so a 0.34.0 node would read the tombstone as a marker during a rolling upgrade.
* The write version as the fence. Under competing consumers a cancel followed by a new subscribe under the same lease
  gives both the same token, so `notOlderThan(leaseToken)` accepts both writes or refuses both.
* No marker at all, so every start replays the whole history. It is the safest, and it makes every restart of a large
  projection replay everything, which is what the marker exists to avoid.

## Consequences

* The two stacks give the same `CheckpointStorage` entry two meanings. Code that reads a catch-up marker has to know
  which stack wrote it.
* On the blocking stack the marker and the code that reads and writes it stay as in 0.34.0. What changes is the cancel
  contract in the next bullet. A cancel delete that the store loses, after a Redis failover or on a MongoDB member with
  the in-memory engine, or a marker write the store applies after that delete, makes a later subscribe of the same id
  skip its replay. 0.34.0 had the same gap and promised a rebuild anyway. Once a rebuild means a new subscription id,
  the same id is only ever subscribed again to resume, and the gap costs nothing.
* On the reactor stack every start after the first replays the events since the previous start's H, per event, without
  the replay callbacks. After a restart it applies again the events the live feed delivered before the restart, since
  the de-duplication cache is in memory. The at-least-once contract already allows that, and a projection has to
  tolerate it. The first 0.35.0 start replays everything since the id first caught up.
* `cancelSubscription(id)` no longer guarantees a full replay on the next subscribe, on either stack. The cancel still
  deletes the marker as clean-up, and a delete that is lost makes the next subscribe skip its replay on the blocking
  stack and resume after the old position on the reactor stack. That is a breaking change for the changelog on both
  stacks, and the 0.35.0 migration guide has to tell users to take a new subscription id for a rebuild, with the push
  `@Saga` caveat from "Cancel". The javadoc that promises the rebuild changes with it: `CancellableSubscriptions` on
  both stacks and the class javadoc of both `CatchupThenPushSubscriptionModel`s, and on the documentation site the
  push subscription pages.
* `CheckpointStorage` is unchanged. The reactor stack reads the stored value as a global position, either the
  `GlobalCheckpoint` it saved or the `StringBasedCheckpoint` form a store reads back, as `ReactorCheckpointStorage`
  does for MongoDB. So a third-party storage keeps working as a reactor marker store as long as `read` returns a value
  `GlobalCheckpoint` can read as the position `save` stored. A store that loses or reorders writes costs replays
  there, not events. A checkpoint under the id that is not a global position now fails a reactor catch-up, which is a
  breaking change for the changelog.
* A subscription id must not be shared between a durable subscription and a catch-up-then-push model or a projection
  feed, on either stack, and the migration guide has to say so.
  [#1564](https://github.com/johanhaleby/occurrent/issues/1564) is about a key space for markers that would lift that.
* The reactor push model's `CatchupListener` hears a catch-up only when it replays from the beginning. The blocking
  one still hears every start.
* The reactor stack writes no marker while an event a handler fed back waits in the live feed and was not replayed. A
  feed whose `catchUp()` runs again after `goLive()` under steady fed-back traffic keeps an older marker, so the next
  restart replays more, until [#1552](https://github.com/johanhaleby/occurrent/issues/1552) delivers those events
  before the marker as the blocking stack does.
* The reactor stack still deletes the marker when a catch-up or a delivery fails, as in 0.34.0, as a backstop. When
  that delete throws, the reactor push model's next catch-up of the id in the same process replays from the beginning,
  as in 0.34.0.
* ADR 177's marker order is amended for the reactor stack. Step 1 reads the marker on both stacks, the blocking stack
  writes it after the drain, and the reactor stack writes H before the drain on purpose, so
  [#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 31 no longer applies to the reactor stack.
* The reactor push model still takes no `CheckpointWriteVersionSource`
  ([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 29), so its marker writes pass `any()`. Nothing
  here depends on it.
