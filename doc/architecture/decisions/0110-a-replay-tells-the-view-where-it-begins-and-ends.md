# 110. A replay tells the view where it begins and ends

Date: 2026-08-08

## Status

Accepted. Unit B1 of the post-0.31.0 architecture review remediation arc, which trails the 0.32.0 tag.

This ADR decides a contract and nothing else. The implementation follows in its own unit, as
[#622](https://github.com/johanhaleby/occurrent/issues/622) asks for. Nothing described under "Decision" exists in the
code yet.

## Context

A projection catch-up costs one store read and one store write per replayed event. The fold is
`MaterializedView.updateFromRepository` (`MaterializedView.java:63-69`):

```java
retryStrategy.execute(() -> {
    S currentState = repository.findById(id).orElse(view.initialState());
    S updatedState = view.evolve(currentState, metadata, event);
    repository.save(id, updatedState);
});
```

and the replay drives it one event at a time. On the blocking stack that is the loop in `BlockingHandover.catchUp`
(`BlockingHandover.java:192-208`), calling `deliver.accept(replayed)` once per event (`:204`). On the reactor stack it is
`ReactiveHandover`'s replay `concatMap` (`ReactiveHandover.java:220`), where each call is additionally wrapped in
`Mono.fromRunnable(..).subscribeOn(Schedulers.boundedElastic())` by the reactor projection DSL
(`dsl/projection-dsl/reactor/.../Projections.java:92`, `:120`, `:139`) even though the whole handover pipeline already
runs on `boundedElastic` (`ReactiveHandover.java:231`), so a replayed event also pays a scheduler hop it does not need.

For a history of N events over K distinct projection keys, that is 2N round trips where the same result is reachable in
roughly 2K. Event-sourced histories have high key locality, because one order accumulates dozens of events and one
account hundreds. A rebuild of a million events over ten thousand orders pays two million round trips to compute ten
thousand final states.

This is a design problem before it is an implementation. Three things stand in the way, and only one of them is the
obstacle it looks like.

**The repository interface is deliberately tiny.** `ViewStateRepository` declares `findById` (`:36`) and `save` (`:38`)
and nothing else, plus a `create(Function, BiConsumer)` factory (`:48-60`) so a repository can be two lambdas. Every
in-repo implementation is a plain adapter over those two operations, the Spring Data Mongo ones
(`SpringMongoViewExtensions.kt:160-169`, `SpringMongoProjectionExtensions.kt:39-48`,
`MongoProjectionStoreProvider.java:45-53`), the `CrudRepository` adapter
(`ProjectionAnnotationRegistrar.java:528-532`), and an in-memory example. Adding abstract batch methods would break
every implementation outside this repository, and Occurrent cannot see those.

**No phase signal reaches the fold.** The blocking feed's payload carrier is a private record with three factories
(`CatchupProjectionFeed.java:316-332`). `replayed(EventMetadata, E)` and `live(EventMetadata, E)` build the same value,
and `live(E)` differs from both only in having no metadata, so what the payload records is whether metadata arrived with
the event rather than which phase it belongs to. `deliver` then reads it exactly that way, as "has metadata or not",
because that is what picks the `MaterializedView` overload (`:193-204`). The replay/live distinction does exist
elsewhere. `ReplayAwareSubscriptionModel.isCatchingUp`
(`ReplayAwareSubscriptionModel.java:48`) answers it, reached through
`static Optional<ReplayAwareSubscriptionModel> of(Object)` (`:58-65`). But its only consumers are the Spring registrars
and the saga timer check (`SagaAnnotationRegistrar.java:226-227`). Nothing in the projection DSL, in `MaterializedView`,
or in `ViewStateRepository` consults it, and the DSL does not hold the subscription model to ask.

**The completion marker is ordered last on purpose.** `BlockingHandover.catchUp` drains the live buffer and only then
calls `source.markCaughtUp()` (`:218-219`). The class javadoc (`:52-57`) says why. A blocking `accept` returns before its
payload has been applied to the view during the catch-up window, so a caller that acknowledges on return may acknowledge
ahead of the update, and re-replaying the whole history is what covers that. The reactor engine orders the marker before
the live drain instead, and documents that this is deliberate and internally consistent, because there a live payload's
`accept` Mono completes only once its update has actually run (`ReactiveHandover.java:52-61`). Either way the marker
means "everything the replay was responsible for is durable", and any batching has to keep that true.

The obstacle that actually blocks batching is the second one, and not in the shape the review proposed. A `replayed`
flag on the payload tells the fold which phase one event belongs to. It does not tell it when the phase ends. A buffer
that is never told the replay finished is written either too late, after `markCaughtUp` has already recorded a
completion the buffer has not reached the store, or never at all. **What batching needs is a boundary, not a label.** No
boundary is expressible anywhere in the current types. `deliver` is a `Consumer<T>`, `MaterializedView` is two `update`
overloads, and neither has a completion channel.

One existing property matters for everything below, because it is what makes the failure semantics tractable. The
delivery contract is already at-least-once, and a failed catch-up already applies events a second time to state that
already includes them. `markCaughtUp` runs only on success, so a catch-up that throws leaves the marker unwritten while
every save it already performed is durable, and the next start replays from the beginning onto that partially advanced
state. This is documented, not accidental. `CatchupThenLiveOptions.java:23-25` says that beyond the de-dup window the
same event can arrive twice and the view has to end up in the same state when it does, and
`CatchupProjectionFeed.goLive` (`:253-256`) says the view has to tolerate the same event arriving twice.

## Decision

**1. The win is coalescing, and it needs no change to `ViewStateRepository` at all.** During a replay, buffer the
replayed events per projection key. At the end of a batch, read each key's state once, apply that key's buffered events
to it in arrival order, and write it once. That is K reads and K writes for a batch touching K keys, against the 2N the
current per-event fold pays, and it is reachable through `findById` and `save` exactly as they are today. The defaulted
bulk operations decided below reduce the round trips further, but they are an optimisation on top of this, not the
mechanism. Framing the issue as "batching means widening a published interface" inverted the dependency. The interface
widening is optional, the coalescing is not.

**2. A view learns the replay boundaries from a small capability interface, not from the payload.** Add to the view DSL:

```java
public interface ReplayAwareMaterializedView {
    void replayStarted();
    void replayCompleted();
    void replayAbandoned();
}
```

A `MaterializedView` that wants replay batching implements it. The feed probes with `instanceof` at the point of need,
the idiom `SagaInstances.java:79` uses for `SagaStateStoreQueries`. A view that does not implement it is never told
anything and keeps writing through per event, exactly as today.

Three alternatives were rejected. A `replayed` flag on the delivery payload does not carry a boundary, which is the
whole requirement. A third `update` overload taking a phase argument puts a subscription-layer concept into the one
method every user of `MaterializedView` must implement, for the benefit of the few that batch. Asking
`ReplayAwareSubscriptionModel.isCatchingUp` is both unreachable from the DSL and racy, since the answer can change
between the question and the update, and it makes the view ask where the runner already knows the answer and can simply
say so.

No `static Optional<ReplayAwareMaterializedView> of(Object)` helper, unlike `ReplayAwareSubscriptionModel` and
`IntrospectableSubscriptionModel`. Those exist to unwrap `DelegatingSubscriptionModel`. There is no delegating
materialized view, so the helper would be ceremony around a bare `instanceof`.

**3. The boundary is driven by whoever owns the replay, and every path that does not own one degrades to today's
behaviour.** Internally, `BlockingHandover.Source` gains defaulted `replayStarted()`, `replayCompleted()` and
`replayAbandoned()` methods, and `ReactiveHandover.Source` gains the same with `Mono<Void> replayCompleted()` so the
write can be asynchronous. Both engines live under `.internal`, so this costs no public surface.
`CatchupProjectionFeed` implements them by forwarding to the view when it implements the capability.

The subscription-fed runners (`ProjectionRunner.java:149-161` and its reactor twin) are deliberately out of scope. They
hand a `Consumer<CloudEvent>` to a subscription model and never see a replay boundary. The model knows it, but
`ReplayAwareSubscriptionModel` only answers when asked and has no completion callback. Giving those paths a boundary
means adding a tell-shaped signal to seven subscription model implementations, which is disproportionate to the win and
is its own decision. Until then those paths simply never call the lifecycle methods and write through per event. The
degradation is silent by construction rather than by oversight, because a view that is never told a replay started
never buffers, so there is no correctness cliff, only a missed optimisation.

The reactive capability interface cannot live beside the blocking one in `dsl/view-dsl`, because that module carries no
reactor dependency (its pom lists `occurrent-subscription-dsl-blocking`, `occurrent-retry`, Kotlin and Spring Data
Mongo). It belongs in the reactor projection DSL, where the reactive fold bridge already is.

**4. `ViewStateRepository` gains defaulted bulk operations, in the shape [ADR 76](0076-batch-command-dispatch-seam.md)
settled for command dispatch.**

```java
default Map<ID, S> findAllById(Collection<ID> ids) { /* loops findById, an absent key means not found */ }
default void saveAll(Map<ID, S> states) { /* loops save */ }
```

The defaults do exactly what the coalescing view would do by hand, so behaviour is unchanged for every existing
implementation, including one built from two lambdas through `create(..)`. This is the same argument ADR 76 made for
`dispatchAll`, that the information exists and is discarded at the point where the call is made. The coalescing view
holds a map of key to state when it writes, and would throw that grouping away by looping over `save`. A Mongo-backed
repository can answer the read with one `_id in (..)` query and the write with one unordered bulk operation, which the
event store internals already do (`SpringMongoEventStore.java:537-543`).

As in ADR 76, **this is something an implementation may take advantage of, not a guarantee the framework provides**, and
the javadoc must say so, so that nobody reads the methods' existence as a promise of atomicity. `saveAll` takes a `Map`
rather than the `Iterable` Spring Data's `saveAll` takes, because this repository is keyed externally and the
association would otherwise be lost. The javadoc must name that difference too.

**5. Partial failure is defined by five rules.**

1. **Nothing may be buffered when `markCaughtUp` runs.** The write is ordered after the last replayed event and before
   the marker on both engines, which on the blocking stack also places it before the live buffer drain, so a drained
   live payload updates state the replay has already written.
2. **A write that fails fails the catch-up**, propagating exactly as a failure inside the per-event fold does today.
   The handover records it, the marker is not written, and the next start replays the whole history.
3. **A write is not atomic across keys, and this ADR does not promise that it is.** When one fails partway, some keys
   are durable and some are not. That is safe only because of the two rules above. With no marker, the whole replay
   reruns.
4. **A stop discards rather than writes.** When `keepReplaying()` returns false, `replayAbandoned()` is called and the
   buffer is dropped. A stop already drains nothing, goes live for nothing and writes no marker
   (`BlockingHandover.java:210-217`), so writing a partial batch would only store state the next replay recomputes.
   `replayAbandoned()` must not throw, and the engine guards it so that it cannot mask the failure that triggered it.
5. **Retry stays scoped to one key at a time.** `RetryStrategy` recovers a lost update when the store reports the
   conflict, by re-reading the winner's state and applying the event again. That works per key, because a failed `save`
   leaves the state unchanged, so the re-read is correct. It does not work across an overridden `saveAll`, which
   reports no per-key outcome, so a retry would re-read keys it already wrote and apply the same events to them a
   second time. A repository that overrides `saveAll` therefore trades per-key retry for fewer round trips, and should
   make the write atomic if it can. The javadoc must say this.

Rule 3 sounds worse than it is. It is the at-least-once contract the code already documents, and coalescing narrows the
window rather than widening it. At the moment of failure, fewer events are durable than under the per-event fold, never
more.

**6. Coalescing is on by default during replay, for framework-built views only, sized by a `batchSize` setting.** The
views `Projections.materializedView(..)` builds are the ones where Occurrent owns both the `View` fold and the
`ViewStateRepository`, and where it knows live events are buffered for the duration of the replay so no concurrent
writer exists within the process. A view a user supplies is never wrapped, because Occurrent cannot know its semantics.
Such a user opts in by implementing the capability.

Default on rather than opt-in, because a performance feature reachable only by reading release notes is close to
unreachable, and because the behaviour it changes is behaviour a replay does not promise anyway. `batchSize` limits the
buffered events and therefore the memory, and a size of 1 means write through, which is the escape hatch for anyone the
change surprises. The setting belongs to the view builder rather than to `CatchupThenLiveOptions`, because coalescing is
a property of the view, `CatchupThenLiveOptions` is a property of the handover, and the same view can be driven by a
feed that has no handover at all. The implementation unit chooses the exact overload shape, and should prefer a small
options value over a fourth positional parameter, since `materializedView` already has four overloads and the
combinations multiply. The starting default is 1000 events, to be confirmed against the benchmark harness in
[#624](https://github.com/johanhaleby/occurrent/issues/624) rather than guessed at twice.

Reads are deferred to the moment of writing rather than taken when a key is first seen. That keeps the read and the
write inside one short window per batch, so a `@Version` optimistic locking check still sees a fresh read, and it is
what makes `findAllById` useful at all.

## Consequences

- A replay of N events over K keys costs about 2K round trips instead of 2N with the defaulted repository, and about two
  per batch for a repository that implements the bulk operations. The improvement grows with key locality, which is the
  normal shape of an event-sourced history.
- Nothing changes for any existing `ViewStateRepository` or `MaterializedView` implementation, in this repository or
  outside it. Both additions are defaults or an interface nobody is required to implement.
- The replay/live distinction becomes expressible for the first time, but only as a boundary and only to the write path.
  An application that wants to suppress a side effect for one replayed event still has no per-event answer inside the
  fold. That is a separate need, and this ADR deliberately does not serve it, because serving it means widening
  `MaterializedView.update`, which every user implements.
- The subscription-fed projection runners keep paying 2N until a subscription model can announce that its catch-up
  finished. That gap is now named rather than latent, and the shape of the fix is known.
- A view is staler during a replay than it was, per-key writes are collapsed, and cross-key write order changes.
  Anything watching the view collection for changes (a Mongo change stream, an audit trail keyed off saves) sees fewer
  writes during a rebuild. Within one key, order is unchanged.
- The failure window narrows, but the at-least-once contract does not change. A failed or stopped catch-up still leaves
  the view partially advanced and still applies those events again on the next replay. This ADR states that property
  explicitly, where before it had to be inferred from the ordering of `markCaughtUp`.
- `saveAll` will look like the natural place to hang a transaction, and an implementation that makes it atomic gets a
  genuinely better failure story. One that only appears atomic gets a worse one, silently, because the retry rule
  assumes it is not. The javadoc names the limit and the type system cannot enforce it, the same trade ADR 76 accepted.
- Occurrent ships no repository that overrides the bulk operations when these methods land, so they are exercised only
  by its own tests until the Mongo-backed repositories adopt them.
- The redundant per-event `subscribeOn(boundedElastic)` on the reactor fold bridge is worth removing while the replay
  path is open, since the handover pipeline already runs there. It is a separate change from batching and should be
  measured, not assumed.
