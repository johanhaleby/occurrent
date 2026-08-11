# 111. A projection records the position it has applied, and a reader can wait for it

Date: 2026-08-08

## Status

Accepted. Resolves [#361](https://github.com/johanhaleby/occurrent/issues/361). Additive across the projection DSL, the
view DSL and the Spring Boot starters.

> **Withdrawn on 2026-08-11 by [ADR 122](0122-an-applied-position-is-not-a-completed-prefix.md), before this
> feature ever shipped.** `waitUntilApplied` read a monotonic maximum as a completed prefix, which needs
> position-ordered delivery, and no delivery source Occurrent ships provides that. `AppliedProjectionPositionStore`,
> its recording wrappers, both Mongo-backed stores, and `@Projection`'s `recordAppliedPosition` attribute are all
> removed from 0.33.0. [#361](https://github.com/johanhaleby/occurrent/issues/361) reopens.

## Context

An asynchronous projection updates its read model behind the write side. A client that ran a command and got back the
global position 4711 cannot ask whether that projection has reached 4711, and cannot wait until it has. Occurrent keeps
no record of how far a projection has progressed, so there is nothing to compare 4711 against.

`@Projection(mode = SYNCHRONOUS)` already covers the case where the read model is updated inside the writer's
transaction, before the command returns. This ADR is about the asynchronous case, which is a different mechanism with a
different cost, and the two stay separate.

[ADR 68](0068-first-grade-event-metadata-in-the-dsls.md) built the one piece this needs and then deliberately left the
rest. `EventMetadata.getPosition()` now reaches every projection handler, and ADR 68 says persisting that position
alongside the view state, plus a read API that returns it or waits for it, "is a separate and larger capability". That
is this ADR. (Issue #361 cites "ADR 0065" for the metadata work. ADR 65 renames the checkpoint-storage module
coordinates and has nothing to do with it.)

### What the code does today

`MaterializedView.updateFromRepository` (`MaterializedView.java:63-69`) reads the state for one key, applies the event
to it, and saves the result. There are two places that happens. The live path runs it once per delivered event. The
catch-up path introduced by [ADR 110](0110-a-replay-tells-the-view-where-it-begins-and-ends.md) buffers replayed events
per key and writes each key once per batch, in `CoalescingMaterializedView` (blocking) and
`CoalescingMaterializedUpdate` (reactor). Whatever this ADR decides has to hold on both, including when a batch write
fails halfway through.

Every write replaces the whole stored document. Spring Data `save`, the `CrudRepository` adapter and
`MongoBulkViewStateOperations` all do, and there is no partial-update helper anywhere. So a position stored "with the
state" means either that the application's own state type carries it, or that it is written somewhere else.

`ViewStateRepository` is two abstract methods plus ADR 110's defaulted bulk ones, on purpose. Implementations outside
this repository cannot be seen from here, so ADR 110 refused to widen it and added a small capability interface and
defaulted methods instead. That is the shape to measure this change against.

### What a position guarantees

[ADR 84](0084-what-a-position-guarantees.md) fixes the semantics. A position is positive, unique and strictly
increasing. It is not dense, so gaps are permanent, and it is not commit order. `currentPosition()` is a high-watermark
rather than a fence.

Two consequences follow directly. The wait has to be "applied at least P" and never "applied exactly P", and it has to
survive a P that no event ever carries. `EventMetadata.getPosition()` is also nullable by design, because a store can
have position writing turned off, so the design needs an answer for an event with no position at all.

### Naming

[ADR 46](0046-rename-subscription-position-to-checkpoint.md) reserves "checkpoint" for a subscription's resume marker
and keeps "position" for the ordering value. What this ADR adds is neither. It is the position a projection has already
applied, so it is called the **applied position**, and nothing in it is named checkpoint.

## Decision

### 1. The requirement is that the recorded position never runs ahead of the state, and one atomic write is not the only way to get there

Issue #361 asks for the position to be stored "in the same atomic write" as the view state. The reason it gives is the
one that matters, so that the stored position never runs ahead of the state it describes. Atomicity is one way to
guarantee that. Writing the position strictly after the state write is another, and it is the one this ADR takes.

If the process dies between the two writes, the state is durable and the position is not, so the recorded position
lags. A reader then waits longer than it had to, or times out, and the redelivery contract applies the event again and
advances the position on the next attempt. Lagging is the safe direction. Running ahead is the unsafe one, and ordering
the writes rules it out.

This buys the freedom to record one value per projection instead of one per key, which the next decision needs, and it
costs one small extra write per delivered event on the live path.

### 2. The applied position is recorded once per projection, not once per view instance

A keyed projection stores one document per key, so a per-key position is the storage-natural choice and it would be
genuinely atomic, since it would ride inside the document the projection already writes. It also answers the wrong
question.

A command can append several events, and the position the client gets back is the last one. If that last event updates
the view instance for customer B while the client reads the view instance for customer A, then A's own position stops
at the earlier event and never reaches the position the client holds. The client waits for a value that will not
arrive, on a read model that is in fact fully up to date. That failure is invisible from the call site, and it is the
normal shape of "I wrote something, now let me read it".

One value per projection has no such case. It answers "has this projection applied everything up to 4711", which is
exactly what the client is asking. A single-instance projection has one key anyway, so for those two the two designs
coincide.

Per-key applied positions are not ruled out forever. They are a strictly narrower answer that can be added later
without changing anything decided here, and the place to add them is the state type rather than this storage.

### 3. `AppliedProjectionPositionStore` is one small interface with three methods

Add to `dsl/projection-dsl/common`, in `org.occurrent.dsl.projection`:

```java
public interface AppliedProjectionPositionStore {
    OptionalLong appliedPosition(String projectionId);

    void advance(String projectionId, long position);

    default boolean waitUntilApplied(String projectionId, long position, Duration timeout) { /* polls */ }

    default boolean waitUntilApplied(String projectionId, long position, Duration timeout, Backoff backoff) { /* polls */ }

    static AppliedProjectionPositionStore inMemory() { /* a map, for tests and single-process applications */ }
}
```

`advance` never moves the stored value backwards, which an ordinary restart needs. A projection that restarts and
replays from the beginning applies position 1 again while the stored state is already at 4711, and a reader watching at
that moment would otherwise see the projection go backwards and then forwards again. `advance` takes the higher of the two, which
MongoDB does in one upsert with `$max`.

`waitUntilApplied` returns `false` on timeout rather than throwing, and reads the value once before it starts sleeping.
That is the shape `Subscription.waitUntilStarted(Duration)` already established for a blocking wait in this codebase.
The difference is that `waitUntilStarted` waits on something inside the process while this one waits on a stored value,
so the default implementation polls. Polling is what makes the answer correct for a reader in a different process from
the projection, which is the common deployment. An implementation backed by a store that can push a change is free to
override the method.

**No reactive `Mono`-returning `waitUntilApplied` exists on the reactor stack, deliberately.**
`AppliedProjectionPositionStore` is blocking-shaped on both stacks, and `ReactiveMongoAppliedProjectionPositionStore` implements that
same interface directly. A reactor caller that waits blocks the calling thread, the same bridge the rest of that
stack already makes in the other direction.

**The polls back off rather than running at a fixed rate, and the pace is `org.occurrent.retry.Backoff`.** A fixed
25 ms interval is wrong in the direction that matters, because the polls that keep coming are exactly the ones a
lagging projection cannot afford. The load a waiting caller puts on the store is anti-correlated with the health of
the thing it is waiting for. The default is `Backoff.exponential(25 ms, 250 ms, 2.0)`, so the first poll still answers
an already-caught-up projection immediately and a projection that is behind is asked at most four times a second per
waiter. The cap is the low end of the range considered, since this sits on a read path where a caller is blocked and
tail latency is the cost of a longer cap.

Reusing `Backoff` rather than a pair of `Duration` parameters keeps one vocabulary for delay shapes across the
library, and the caller-facing overload takes the sealed type directly. `Backoff.none()` is rejected with an
`IllegalArgumentException`, since a wait with no delay between polls is a busy loop against the store.

Full `RetryStrategy` delegation was considered and rejected. `RetryStrategy` is exception-driven, counts
`maxAttempts`, and has no wall-clock deadline, while a poll that finds the projection still behind is the normal
answer rather than an error, and a wait is defined by a timeout rather than by an attempt count. `Backoff` is the part
of that family that carries only the delay shape, which is the part this needs.

An application tunes the pace through `occurrent.projection.applied-position.initial`, `.max` and `.multiplier`, which
the Mongo starters bind into `Backoff.exponential` and hand to the store they build. The store applies it by
overriding the three-argument `waitUntilApplied`, so a caller that never names a `Backoff` still gets the configured
one.

The wait method lives on the storage interface rather than in a second type because the storage is the only thing that
knows how to observe the value, and `ViewStateRepository` already carries defaulted convenience methods next to its
abstract ones.

There is deliberately no method that returns the view state once the projection has caught up. It would have to
distinguish "no such view instance" from "timed out" in its return type, and the caller can read through the repository
it already holds once `waitUntilApplied` answers `true`.

**The shipped Mongo stores retry a failing read or write, and the interface does not mention retry at all.** Both take
a retry policy, defaulting to exponential backoff from 100 ms to 2 seconds, the same default
`NativeMongoCheckpointStorage` uses. It guards both directions, because both have a silent failure mode. A transient
blip inside `advance` while the projection is applying an event would otherwise fail that delivery and cost a
redelivery for something the store would have accepted a moment later. A transient blip inside `appliedPosition` during a wait would
otherwise abort a caller's wait with an exception rather than being absorbed. The blocking store uses
`RetryStrategy`, the reactor one uses `reactor.util.retry.Retry`, matching how each stack retries elsewhere.

Retry stays out of `AppliedProjectionPositionStore` itself. Error policy belongs to an implementation, and the in-memory store
has nothing to retry. Keep the two mechanisms apart when reading this. The retry policy decides what happens when a
store operation **fails**, and `Backoff` decides how long to wait between polls that **succeeded** and found the
projection still behind. They never apply to the same event.

### 4. A delegating materialized view records the position, and it is built by an explicit factory

```java
MaterializedView<E> view = Projections.materializedView(projection, repository);
MaterializedView<E> recording = Projections.recordingAppliedPosition(view, storage, "orders");
```

The reactor stack gets the same factory over the update function it builds, since that is what
`ReactiveProjectionRunner.project(..)` accepts:

```java
BiFunction<EventMetadata, E, Mono<Void>> update = Projections.reactiveUpdateWithMetadata(projection, repository);
BiFunction<EventMetadata, E, Mono<Void>> recording = Projections.recordingAppliedPosition(update, storage, "orders");
```

A factory over an existing view beats an extra parameter on `materializedView(..)` for two reasons. The blocking
factory already has six overloads and adding a projection id plus a storage to the ones that lack them multiplies them
again. More importantly, wrapping works for a view Occurrent did not build, which is the view DSL's Kotlin
`materialized(..)`, the `CrudRepository` adapter, and anything an application wrote itself. Recording only for
framework-built views would leave most real projections out.

The recorder implements `ReplayAware` and forwards every lifecycle call to whatever it wraps. It also
uses those calls itself, and it must. During a replay the view it wraps is buffering, so a position written per event
would describe state that is still in memory. The recorder therefore keeps the highest position it has seen while a
replay is running and writes it in `replayCompleted()`, after the delegate has flushed. `replayAbandoned()` discards
it, since the next replay recomputes everything anyway.

This makes ADR 110's statement that "there is no delegating materialized view" out of date. It does not bring back the
`static Optional<X> of(Object)` unwrapping helper that ADR 110 declined, because the recorder is the outermost view and
answers the `instanceof` probe itself. A caller that wraps in the other order gets an unbatched replay, and the javadoc
says which order to use.

### 5. Recording is opt-in, per projection

An extra write per event is not something to hand every projection whether it asked or not. `@Projection` gets a
boolean attribute:

```java
@Projection(id = "orders", recordAppliedPosition = true)
```

The registrar then wraps the view it built with the recorder, keyed by `annotation.id()`, which is already the
subscription id, the single-instance view key, and the id handed to `DefaultProjectionStoreProvider`. It resolves an
`AppliedProjectionPositionStore` bean and fails at startup with a message naming the attribute when there is none, following
[ADR 11](0011-introduce-optional-capability-interface-for-filtered-stream-reads.md) on refusing a requested capability
that is not configured.

`recordAppliedPosition = true` together with `mode = SYNCHRONOUS` is refused at startup. A synchronous projection has
already updated the read model by the time the command returns, so recording a position for it buys a write and
nothing else, and asking for both means one of the two was misunderstood.

The Mongo starters contribute the implementation, the same way they contribute `CheckpointStorage` and
`DefaultProjectionStoreProvider`. `occurrentAppliedProjectionPositionStore` is `@ConditionalOnMissingBean`, writes to the
collection named by `occurrent.projection.applied-position-collection` (default `appliedPositions`), and stores one
document per projection id.

> **Amended on 2026-08-11 by [ADR 122](0122-an-applied-position-is-not-a-completed-prefix.md).** The
> `recordAppliedPosition` attribute, the registrar wiring described here, and the Mongo starter beans are all
> removed. No configuration this decision permits was ever sound, so there was nothing safe left to keep opt-in.

### 6. An event with no position is refused rather than ignored

When a recording projection is handed an event whose metadata has no position, it throws an `IllegalStateException`
naming both causes, either the event store has position writing turned off, or the event arrived on a path that carries
no metadata.

Silently skipping is the worse option and ADR 68's own amendment says why. A null view-instance id used to skip an
event without a word, so a projection keyed on the position quietly updated nothing, and the fix was to reject it where
the key needed it. The same reasoning applies here. A recording projection that silently records nothing produces a
reader that waits until it times out and a stored position that is wrong rather than absent.

### 7. Ordered delivery is what makes the recorded position mean anything

The recorded value means "this projection has applied every event it was given, up to and including this position".
That holds when the projection is given its events in position order, which is what a subscription does. It does not
hold for a push feed driven by several threads at once, where the event at 4711 can be applied before the event at
4700, and the recorded position would then claim more than the read model has. `advance` never going backwards keeps
the value monotonic, and it cannot invent the ordering guarantee. The javadoc states the precondition, and a projection
fed by a subscription meets it.

There is a second, milder case worth stating in the same place. A projection subscribes with a filter derived from the
event types it handles, so an event it does not handle never reaches it and never moves its position. Waiting for the
position of such an event times out. That is the honest answer rather than a defect, because a projection that never
sees the event has no effect for the client to read, but the javadoc has to say it or the timeout looks like a bug.

> **Amended on 2026-08-11 by [ADR 122](0122-an-applied-position-is-not-a-completed-prefix.md).** "Which is what a
> subscription does" was wrong. Occurrent reserves Mongo positions before commit while change streams deliver in
> commit order, so position order is not delivery order there either, contradicting
> [ADR 84](0084-what-a-position-guarantees.md). `advance` never going backwards keeps the recorded value monotonic,
> but a monotonic value is not a completed prefix unless delivery is actually position-ordered, and on every
> delivery source Occurrent ships it was not.

### 8. What this deliberately does not cover

The on-demand query path (`Projections.project(..)`) applies events with `EventMetadata.empty()` and has no delivery
behind it, so it has no position to record and never will. That is ADR 68's distinction between cannot and was not
given. A domain-event feed is the "was not given" side, since it has a position only when the application passes
metadata in, and one that opts into recording without doing so hits the refusal in decision 6.

No conformance suite in the TCK for `AppliedProjectionPositionStore`. There is exactly one implementation plus an in-memory one
when this lands, and `CheckpointStorageConformance` exists because five implementations across four backends had to
agree. If a second backend implements this, that is the moment to add one.

## Consequences

- A client that holds a position can wait for a projection to reach it, with a timeout, on both the blocking and the
  reactor stack, and can read the current applied position without waiting.
- A recording projection pays one extra store write per delivered event on the live path, and one per replay rather
  than one per batch during a catch-up. A projection that does not opt in pays nothing, and no existing
  `ViewStateRepository` or `MaterializedView` implementation changes.
- The recorded position can lag the read model after a crash between the two writes. It cannot lead it. Everything a
  reader concludes from it stays true, and the next delivery corrects the lag.
- A projection keyed by something other than the events' subject still gets one projection-wide answer, so a client
  waiting for 4711 waits for the projection as a whole rather than for the instance it is about to read. That is
  stricter than necessary and never wrong.
- Waiting costs one small read every 25 ms per waiting caller. A read-your-writes wait is normally a few tens of
  milliseconds, so this is cheap, and it is the price of an answer that is correct across processes.
- `MaterializedView` now has a delegating implementation, which ADR 110 said did not exist. Anything that probes a view
  with `instanceof` has to be handed the outermost one.
- `EventMetadata.getPosition()` gets its first framework consumer. Until now it was only read by application code, so a
  store with position writing turned off failed nothing inside Occurrent. It now fails a recording projection loudly at
  the first event.
