# 62. Pluggable projection event source (push subscription model)

Date: 2026-07-18

## Status

Accepted. The push subscription model and the v1 replay-then-push catch-up (broker owns live-resume) are
implemented. Broker-independent resume (v2) is a deliberate non-goal, see the section below.

## Context

A projection built with the Projection DSL (ADR 58) or declared with `@Projection` (ADR 59) is fed only by a
subscription, and every shipped subscription model reads a MongoDB change stream (directly, through a catch-up replay,
or through the in-memory background dispatcher). In production a common deployment forwards Occurrent cloud events to a
broker such as RabbitMQ and consumes them with a listener (for example a Spring `@RabbitListener`) instead of reading
change streams. Those deployments cannot use the Projection DSL, because there is no way to feed a projection from the
broker.

The projection feed is already source-agnostic. Every runner collapses to
`subscribable.subscribe(id, filter, startAt, action)` where `action = cloudEvent -> materializedView.update(converter.toDomainEvent(cloudEvent))`,
and the fold-and-persist primitive (`MaterializedView.update`, `Projections.materializedView(...)`) has no subscription
coupling. There is also a proven in-tree precedent for a non-change-stream `Subscribable`: `SynchronousSubscriptionModel`
(ADR 57) is register-only, matcher-based (`SubscriptionFilterMatcher`), ignores `StartAt`, and is driven by an external
`dispatch(List)` call. So what is missing is a first-class generic push source, not a new framework.

## Decision

**Add a generic push subscription model, `PushSubscriptionModel` (blocking and reactor), a register-only `Subscribable`
driven by an external `accept(CloudEvent)` call rather than a change stream.** A broker listener registers projections
on it through the same `ProjectionRunner`/`project(...)` used for change-stream subscriptions and hands each received
event to `accept(...)`, which routes it to every handler whose `SubscriptionFilter` matches, on the calling thread
(sequentially, through the returned `Mono`, on the reactor stack). A handler exception propagates to the caller, so the
listener decides whether to acknowledge or redeliver.

**Occurrent stays transport-neutral.** The push model has no dependency on any broker. The application wires its own
listener and calls `accept(...)`; the same mechanism serves RabbitMQ, Kafka, Spring application events, or HTTP. The
pushed cloud events must carry the Occurrent extensions the handlers rely on (at minimum `streamid` and `streamversion`),
so the deployment forwards the stored cloud event as CloudEvents JSON and reconstructs it on the listener side.

**Share the register-and-route machinery.** `PushSubscriptionModel` and `SynchronousSubscriptionModel` are near
identical: both register `(id, matcher, action)` and route each event to the matching handlers. That machinery, id
uniqueness, the filter-to-`Predicate` translation, ordered dispatch, and the already-started `Subscription` handle, is
extracted into `RegisteringSubscribable` in the blocking and reactor subscription api modules. `SynchronousSubscriptionModel`
adds the application-service `dispatch(List)` entry point; `PushSubscriptionModel` adds the externally driven `accept`.
They differ in role (one is wired into the write path by the application service, the other is driven by an external
listener) but not in mechanism, so a shared base removes the duplication without conflating the two.

**No new DSL surface for the feed function.** The user's "higher-order function that feeds projections" is already
present two ways, so no third path is added: `PushSubscriptionModel::accept` is itself a filtered `Consumer<CloudEvent>`
that fans out to every registered projection, and `Projections.materializedView(projection, repository)::update` is the
domain-event-level `Consumer<E>`. A separate per-projection feed factory would duplicate this with a narrower fan-out.

## Consequences

- A listener can drive the Projection DSL in production without MongoDB change streams, and without Occurrent taking a
  broker dependency.
- `PushSubscriptionModel` carries only the live tail. A broker is not a log, so a new or rebuilt projection cannot be
  backfilled from the queue. For a new or rebuilt projection, replay history from the event store first (the existing
  catch-up or on-demand projection reads) and then attach the push feed.
- Steady-state delivery is at-least-once with the same contract as the change-stream path: the fold must be idempotent
  under redelivery. The push model itself keeps no checkpoint.
- Ordering follows the transport. A single-queue broker preserves publish order; a multi-queue or partitioned broker
  does not guarantee global order, so a projection that depends on strict global order across streams needs a transport
  that preserves it.

## Decision: replay-then-push catch-up (v1, broker owns live-resume)

A push feed cannot backfill a new or rebuilt projection, because a broker is not a log. `CatchupThenPushSubscriptionModel`
(blocking and reactor) fills that gap with a one-time catch-up in front of a `PushSubscriptionModel`. It splits
responsibility deliberately:

- **Catch-up is Occurrent's job**, run once per subscription id. On subscribe it registers on the live feed first and
  buffers, replays the projection's history from the event store in position order (`PositionOrderedReader`), then drains
  the buffer and goes live, de-duplicating the replay-to-live overlap by event id. Because buffering starts before the
  head is read, an event committing during the replay is delivered either by the replay or by the buffered feed, and no
  reconcile pass is needed. The buffer is bounded and fails loud on overflow, so a full from-scratch rebuild over a live
  feed (unbounded buffering) is a fail-loud error rather than silent truncation: rebuild offline instead.
- **Live-resume is the broker's job, not Occurrent's.** After catch-up the listener acknowledges each message only once
  `accept(...)` returns, so an unprocessed event is redelivered. The model persists no live position watermark, which is
  what sidesteps the watermark problem below entirely. Delivery is at-least-once, so the fold must be idempotent, the
  same contract as the change-stream path.
- **A one-shot catch-up-complete marker** (an optional `CheckpointStorage`) records that the replay finished, so a
  restart skips it and lets the broker resume. The stored value marks completion, it is not a moving resume watermark.

**Why no feed-derived resume watermark.** Occurrent reserves global positions from a shared counter outside the write
transaction (`MongoEventStore.java:282-284`), so an abandoned or rolled-back write leaves a permanent gap, and a
concurrent lower reservation can commit after a higher one (a temporary hole that fills in later). Positions are only
globally-unique and strictly monotonic, never dense or in commit order (ADR 0007, ADR 0021, `MongoEventStorePositionTest`).
A missing position is therefore ambiguous from the delivery stream alone: a temporary hole to wait for, or a permanent gap
that never arrives, indistinguishably. So a feed-derived contiguity watermark (advance a checkpoint to the highest
gap-free position seen over broker-order delivery) is unworkable: it stalls forever at the first permanent gap, and any
timeout that advances it past a still-uncommitted position reintroduces the event loss it was meant to prevent. This is
also why the overlap is de-duplicated by event id, not by position. Putting live-resume on the broker avoids the whole
problem: the broker delivers in commit order and redelivers what was not acknowledged, exactly like a change stream, so
no position frontier is ever reconstructed.

Only stream and capability-agnostic subscription filters can be catch-up-replayed (their plain `Filter` drives the
position-ordered read). A DCB subscription filter is rejected, since a DCB boundary needs a different replay read.

**Consequence and limit.** Correctness across a restart depends on the broker retaining the backlog for an offline
consumer (a durable queue with a preserved offset). If the consumer is offline longer than the broker retains, or the
offset or catch-up marker is lost, the projection must be rebuilt. For RabbitMQ specifically a durable queue already
retains messages for an offline consumer, so this v1 covers the common production case.

## Domain-event feeds (no double encode/decode)

When the external source already delivers domain events (a listener with its own message converter), routing them
through the CloudEvent push feed means `domainEvent -> toCloudEvent -> toDomainEvent -> fold`, a full serialize and
deserialize per live event. That is avoided by feeding the projection in domain space directly.

The layering is preserved: the CloudEvent components stay the base, and the converter is composed only where events are
genuinely CloudEvents. The live path has a domain source and a domain sink (the `View` fold, itself a domain-typed base
in the view DSL), so it folds directly with no CloudEvent hop. The only decode is the catch-up replay, which reads the
store (CloudEvents) and decodes each event once, never a double round trip.

- `Projections.domainEventFeed(projection, repository)` returns the live-only domain feed (a `Consumer<E>` blocking, a
  `Function<E, Mono<Void>>` reactor), a named form of the existing `materializedView(...)::update` / `reactiveUpdate(...)`.
- `CatchupProjectionFeed` adds the one-time catch-up to a domain feed: buffer live, replay the store
  (decode once), drain, go live, de-duplicating the replay-to-live overlap by a caller-supplied `Function<E,String>`
  event-id applied in domain space (so it does not depend on the CloudEvent id). Same v1 contract as the CloudEvent
  handover: broker owns live-resume, one-shot catch-up marker, at-least-once idempotent folds, bounded fail-loud buffer.
- `DomainEventFeed<E>` is the application-owned fan-out sink (the domain twin of `PushSubscriptionModel`) that drives
  several projections from one source. It carries the domain-specific event-id function as a constructor argument, which
  is why `@Projection(source = PUSH, subscriptionModelName = "...")` needs no event-id annotation attribute and no
  feed-handle registry: the listener feeds the bean it owns, and the registrar just registers projections on it and
  catches them up. Both stacks accept a `ViewStateRepository` or a `MaterializedView` store (on the reactor stack the
  fold runs on `boundedElastic`).

## The `Pushable` capability

The `accept(CloudEvent)` capability is a small `Pushable` interface (blocking and reactor, in the subscription api
modules) that `PushSubscriptionModel` implements, so a listener or wiring can depend on "a target I push cloud events
into" rather than a concrete model, and a model may be pushable without every subscription model being one. Change-stream
(pull-based, checkpoint-tracking) models are deliberately not made pushable: feeding external events into their cursor
would double-deliver and corrupt the durable checkpoint. To offer both change-stream and push subscriptions from a single
bean, compose a routing model that sends each subscription to exactly one source rather than unioning both into one.

## Non-goal: broker-independent resume (v2)

Making live-resume independent of broker retention is a deliberate non-goal, and may never be built. The decision is a
boundary of responsibility: delivery guarantees for the live feed, ordering and no-loss redelivery, belong to the
transport that already provides them (RabbitMQ, Kafka), not to Occurrent reconstructing them. The push source is meant to
be driven by a transport that guarantees ordering. Occurrent's job is the one-time catch-up from the event store, not to
compensate for a transport that drops or reorders messages.

Should broker-independent resume ever be needed, it belongs with the push transport integration (the push module or a
transport-specific adapter), resting on that transport's own ordering and durability guarantee, rather than inside the
core as a store-side checkpoint. A within-Occurrent version was considered (replay from a checkpoint that lags real time
by MongoDB's `transactionLifetimeLimitSeconds`, since a reservation not committed within that window is guaranteed
aborted, so such a checkpoint cannot skip a still-pending low position). It is sound, but it is intricate and
MongoDB-coupled (reserved-head sampling, transaction-lifetime configuration) and would put the core in the business of
reconstructing guarantees the transport should own, so it is explicitly out of scope. A deployment that needs resume the
broker cannot provide should rebuild the projection offline from the event store instead.

## `@Projection` push-source routing

`@Projection` binds to a push source through a new explicit `source` attribute: `source = Source.PUSH` routes the
projection to a `PushSubscriptionModel` bean (selected by `subscriptionModel` type or `subscriptionModelName`), which the
bean-post-processor wraps in `CatchupThenPushSubscriptionModel` with the event store as the replay reader and the
framework's `CheckpointStorage` as the catch-up marker. The default `Source.EVENT_STORE` keeps the existing behavior.
An explicit attribute was chosen over auto-detecting a push bean, since a durable public annotation should not change its
event source implicitly based on which beans happen to be present. Push source is rejected together with
`mode = SYNCHRONOUS` and the catch-up start knobs (the catch-up always replays from the beginning and live-resume is the
broker's job), and with a `DcbProjection` (a DCB boundary cannot be catch-up-replayed in position order). Implemented on
both the blocking and reactor stacks.

## Amendment (2026-07-25): the catch-up handover is one shared engine per stack

When this ADR was implemented, extracting a shared core between the CloudEvent handover
(`CatchupThenPushSubscriptionModel`) and the domain feed (`CatchupProjectionFeed`) was assessed and deliberately
deferred, on the grounds that it was cross-module and would touch already-tested code. The parallel implementations were
kept and unification was recorded as a candidate follow-up. That deferral is now reversed.

**Why.** The coordination is concurrency-sensitive (a lock and a bounded buffer on one stack, a unicast sink and
`concatMap` on the other) and it was written out four times. That is the kind of duplication where a divergence is
hardest to see, because reading any one copy tells you nothing about the other three.

The evidence that the copies do drift is concrete: the reactor `CatchupProjectionFeed` folded replayed events through a
metadata-less `Function<E, Mono<Void>>`, so a projection keyed by metadata (`Projection.idWithMetadata()`) mis-keyed
every replayed event on the reactor stack while catching up correctly on blocking, and the divergence was invisible
because the blocking test class had a metadata test and the reactor one simply had no equivalent.

Be precise about what that proves, though. That bug lived in the caller's fold wiring, which this extraction
deliberately leaves with each caller, so a shared engine would not have prevented it and a fifth caller could
reproduce the same class of mistake tomorrow. It is evidence that four hand-maintained twins drift unnoticed, not
evidence that this particular extraction was the fix for that particular bug. The extraction stands on the duplicated
coordination alone.

**What is shared.** The duplicated part is the *coordination*, not the payload handling: the bounded live buffer and its
fail-loud overflow, the de-duplication window, the live/replaying state, the poison latch that fails live events after a
failed catch-up, the drain, and the ordering rules in the replay-then-push section above. That is extracted into
`BlockingHandover<L, R>` and `ReactiveHandover<L, R>`, which live in `internal` packages of the existing
`occurrent-subscription-api-blocking` and `occurrent-subscription-api-reactor` modules, with the shared
`HandoverOptions` and `HandoverMessages` in `org.occurrent.subscription.internal` alongside `BoundedIdCache` and
`ReplayFilters`. This follows the precedent this ADR already set for `RegisteringSubscribable`, which was likewise
added inside the pre-existing subscription api modules: the two callers differ in role, one driven by a broker over
CloudEvents and one by a listener over domain events, but not in mechanism.

**No new published artifacts.** An earlier attempt minted `occurrent-subscription-handover-{common,blocking,reactor}`
for this and was reversed. Three published artifacts, three BOM entries and an aggregator pom are too much permanent
user-facing surface for reuse across four call sites that are all internal to Occurrent, and both consumers already
depend on the subscription api and core modules. The `internal` package name carries the "not user API" signal instead,
the same way `BoundedIdCache`, `RetryImpl` and `SnapshotSupport` already do.

**Two engines, not one.** `Stream` versus `Flux` and `long` versus `Mono<Long>` run through the whole SPI, so a single
engine is not reachable without wrapping one stack in the other. Only data and messages are genuinely shared across
both: the two default tunables, the positioned-reader guard, the tunable validation, the overflow message, and the
de-duplication cache.

**Two type parameters, not one.** `L` is the live payload and `R` the replayed one. They are kept distinct because a
replayed event carries `EventMetadata` decoded from its CloudEvent and a live domain event has none. Collapsing them
would also not be behaviour-preserving, since `MaterializedView.update(E)` and `update(EventMetadata, E)` are separate
interface methods that a caller's view may implement differently.

**What stays with each caller.** The replay read, the decode, the delivery functions, the de-duplication key function,
and the filter derivation. `Projections.domainEventFeed(...)`, `DomainEventFeed<E>` and the `@Projection` routing
described above are unchanged, and every public signature of the four classes is unchanged, so the existing four
catch-up test classes pass without modification. That is what makes this a behaviour-preserving extraction rather than a
rewrite.

**Named `handover`, not `catchup`.** This ADR already calls the CloudEvent side "the CloudEvent handover" and the
blocking push model already had a private `Handover` class, so `handover` was the existing vocabulary for exactly this
replay-to-live transition. `catchup` would also have read as the change-stream catch-up subscription models under
`subscription/util`, which are a different mechanism: they page a bulk window and reconcile against a moving stream
head, machinery these engines neither have nor need.

**One difference is preserved on purpose.** The reactor stack completes its catch-up signal and persists the
catch-up-complete marker before the buffered live events are folded, whereas the blocking stack returns only after the
drain. Each is self-consistent with its own acknowledgement model: a blocking `accept` returns before the fold, so the
marker must wait for the drain to keep the store the backstop, while a reactor ack completes only after its fold. The
engines state this explicitly rather than leaving it to be inferred from the pipeline shape.

## Amendment (2026-07-26): a domain-event feed carries metadata on the live path too

The "Domain-event feeds" section above says `Projections.domainEventFeed(...)` returns a `Consumer<E>` (blocking) or a
`Function<E, Mono<Void>>` (reactor). It now returns a `MaterializedView<E>` and a
`BiFunction<EventMetadata, E, Mono<Void>>` respectively, and `CatchupProjectionFeed` and `DomainEventFeed` gained
`accept(EventMetadata, E)` beside `accept(E)` on both stacks. The reactor `DomainEventFeed` also gained a
`register(id, BiFunction<EventMetadata, E, Mono<Void>>, Filter)` overload, and the reactor `CatchupProjectionFeed`
factory taking a metadata-aware fold is now public.

**Why.** A projection keyed through `Projection.idWithMetadata()` caught up correctly and then broke on every live
event. The replay reads the event store, so it has CloudEvents and real metadata, but a live domain event has none. The
loud half of that was already visible (`EventMetadata.getStreamId()` throws on empty metadata). The quiet half was
worse: `getPosition()` and `get(key)` return `null` on empty metadata, a `null` id is a documented instruction to skip
the event, so a position-keyed projection dropped every live event with no error anywhere.

Occurrent cannot derive the metadata. Stream id, version and position are properties of the stored CloudEvent, and a
domain-event feed exists precisely so a listener that already converted the message avoids the round trip back through
one. So the application supplies it, since it is the only party that has it, and the one-argument forms stay for the
common case where the broker gives nothing to supply.

**Two live delivery routes, not one.** `accept(E)` still routes to `MaterializedView.update(E)` and
`accept(EventMetadata, E)` to `update(EventMetadata, E)`. Routing everything through the metadata overload with
`EventMetadata.empty()` would be shorter but not behaviour-preserving, because those are separate interface methods a
caller's view may implement differently. The reactor stack needs no such split: its fold is a single `BiFunction`.

**A guard at delivery, not at registration.** `Projection` gained `metadataKeyed()`, set only by
`Builder.id(BiFunction)`
and carried across `adapt(...)`. Where a `null` id used to mean "skip", the materializers now fail when the projection
is metadata-keyed *and* the metadata it was handed is empty.

Registration-time rejection was considered and is wrong twice over. It would forbid the very capability being added,
since whether a listener supplies metadata is a runtime property of that listener, unknowable when the projection is
registered. And the flag is an unsound signal for "is metadata-keyed": a caller writing
`id((metadata, event) -> event.orderId())` uses the metadata overload while ignoring it, which is a legitimate and
tested pattern, so rejecting on the flag alone would break working code. The conjunction avoids that, because such a
projection still returns a real id and never reaches the branch.

**What this supersedes in the 2026-07-25 amendment.** That amendment argued for keeping the handover engines' `L` and
`R` type parameters distinct partly because "a live domain event has none". That premise is now gone. The
parameters are still distinct, and the second reason given there still holds: the blocking stack keeps two live delivery
routes, so its live payload carries a nullable metadata while a replayed payload always has one. Collapsing them is
tracked as separate follow-up work rather than folded in here, so a behaviour change to what a split-overload view
observes cannot hide inside a feature change. The claim there that `Projections.domainEventFeed(...)` and
`DomainEventFeed` are "unchanged" was true of that extraction and is not true of this one.

## Amendment (2026-07-27): one type parameter per engine, and the tunables become a public record

**Two type parameters reverses to one.** `BlockingHandover<L, R>` and `ReactiveHandover<L, R>` are now
`BlockingHandover<T>` and `ReactiveHandover<T>`, with one `deliver` and one `dedupId` instead of two of each.

Both reasons the 2026-07-25 amendment gave are gone, and neither was abandoned as a matter of taste. The first premise,
that a live domain event carries no `EventMetadata`, stopped being true when the feeds gained `accept(EventMetadata, E)`
(recorded in the 2026-07-26 amendment above). The second, that `MaterializedView.update(E)` and
`update(EventMetadata, E)` are separately implementable so collapsing would not preserve behaviour, was resolved by
merging the blocking feed's two carriers into a single record with a nullable metadata. The routing decision still
happens, but it happens on a null check inside the caller's own delivery function, where the engine's type parameters
were never involved. Leaving the ADR as it stood would have documented a type-level guarantee the code no longer had.

The reactive engine never had a call site where `L` and `R` differed, so its half is dead-code removal. Three of the four
callers already bound `L == R`, and two passed literally the same reference twice.

**What this gives up.** A caller can no longer count replayed versus live deliveries by which lambda fired, and cannot
differentiate error handling per phase on the reactive side. No caller did either. The engine tests lose their
which-lambda-fired assertions and keep ordering and delivered-once, which is stronger for the interleaving test, since
one list now pins the order across both phases rather than two lists pinning each phase separately.

**One widened contract.** The same caller-supplied function is now invoked on both sides of the blocking engine's
monitor, outside it for the replay fold and while holding it for the drain and a live `accept`. Both lambdas already
called into the same view, so this is practically unchanged, but the engine documents it rather than describing the lock
asymmetry as a per-path property.

**`Named handover, not catchup` also reverses, for the options record only.** `HandoverOptions` moves from
`org.occurrent.subscription.internal` to the public `org.occurrent.subscription` as `CatchupThenLiveOptions`, replacing
the loose `(int dedupCacheSize, int maxBufferedEvents)` pair at all seven public occurrences and retiring the eight
per-caller `DEFAULT_*` constants.

The rename is forced, not preferred. `CatchupSubscriptionModelConfig.DEFAULT_HANDOVER_CACHE_SIZE` is public API in
0.30.0 and the changelog already describes that mechanism as the "handover cache", for the change-stream catch-up models
under `subscription/util` with a different default. A second public "handover" de-dup cache would collide with released
vocabulary. So `handover` stays the engines' internal word, which is what the 2026-07-25 amendment actually established,
and the public type is named after what a caller configures. `CatchupThenLiveOptions` maps onto
`CatchupThenPushSubscriptionModel` and `catchUp()`, and its suffix keeps it distinguishable from
`CatchupSubscriptionModelConfig`.

`org.occurrent.subscription` adds no coupling: both projection-dsl poms already declare a compile-scope
`occurrent-subscription-core`, every public `CatchupProjectionFeed.create(...)` already takes `CheckpointStorage` from
`org.occurrent.subscription.api.*`, and that package already holds public types such as `StartAt` and
`SubscriptionFilter`.

The reactor feed also gains the defaults-taking metadata-aware factory it was missing. Before this, the preferred
metadata-aware path existed only in the int-pair form, so `reactor/DomainEventFeed` had to hand-write both default
constants while the plain-`Function` path three methods above it called a clean defaults-taking form. The API penalised
the path callers should use.

## Amendment (2026-07-28): a failed catch-up releases its registration, and cancellation becomes its own capability

Both stacks register the handover on the live feed before running the replay, which is deliberate and stays: it is what
captures an event committing during the replay instead of losing it in the gap between the replay head and going live.
A catch-up failure never released that registration, and `RegisteringSubscribable` had no way to release one, since
`subscriptionIds` and `registrations` were append-only. Two things followed. The subscription id could never be reused,
so retrying the same projection hit `Subscription <id> is already registered`. And because `route` is an unguarded loop
over the registrations in order, the dead handler rethrew its stored `catchUpFailure` on every later event and starved
every handler behind it, including a healthy subscription with a different id.

`route` keeps its unguarded loop. A push source needs a handler error to reach it so it can decide whether to nack, and
`a_throwing_handler_propagates_to_the_caller` pins that.

**Cancellation moved into `CancellableSubscriptions`,** a one-method capability that `SubscriptionModelLifeCycle` now
extends, so `RegisteringSubscribable` implements the narrow one. Two alternatives were rejected. A bare
`cancelSubscription` method on `RegisteringSubscribable` would have been a lookalike, carrying the same name and
signature as the interface method under a weaker contract with no interface behind it. Implementing the whole of
`SubscriptionModelLifeCycle` would have been worse than untidy: `DcbSubscriptionModelAdapter` uses
`instanceof SubscriptionModelLifeCycle` as a runtime capability gate, and a push model failing that gate is the
behaviour this ADR already decided on, so a push model must keep failing it. Beyond that, `stop` and `pauseSubscription`
have no sound meaning here. Every other implementation has an upstream buffer, so a pause defers events, while these
models take their events from the caller and have no replay, so a pause would drop them. On
`SynchronousSubscriptionModel` a pause would silently stop updating a read model whose whole purpose is read-after-write.

Moving a method onto a new superinterface is compatible in both directions: every current implementor still satisfies
`SubscriptionModelLifeCycle`, and a call against it still resolves.

**A `DomainEventFeed` fails terminally instead, and does not drop the projection.** `catchUpAll()` runs each
`CatchupProjectionFeed.catchUp()`, and each feed owns its own handover, so a failure poisons that feed the same way.
Dropping it would be the wrong recovery: the application declared that projection, so running without it is worse than
not running at all. The two differ because of who owns the decision. A subscription model hands the failure straight
back from `subscribe`, so the caller already knows and the released id is a courtesy. A feed is assembled by the
application and fanned out to in registration order, so a silently missing projection would surface much later as a
read model that is simply wrong. The contract is stated on `catchUpAll` on both stacks: fix the cause and build a new
feed.

## Amendment (2026-08-02): a push sink feeds one consumer, so there is no fan-out left to reason about

Superseded by [ADR 88](0088-a-push-sink-feeds-one-consumer.md). The description above of `PushSubscriptionModel` and
`DomainEventFeed` as fan-out sinks, and the rejection of "a separate per-projection feed factory" as "a narrower
fan-out", no longer describe the code: both sinks now take exactly one consumer and refuse a second registration.

The original rejection was argued on API surface. It never weighed error isolation, and that is what decides it. One
received message carries one acknowledgement decision, so several consumers on one sink share it, and a consumer that
keeps failing holds up every consumer behind it on every redelivery. ADR 88 has the full argument.

What survives from this ADR: the unguarded ordered `route` loop, kept so a push source's error reaches the listener and
it can nack (the 2026-07-28 amendment), is unchanged and still right. The fix was never to guard the loop; it was for
there to be at most one consumer to loop over. `Source.PUSH` staying a single enum value whose flavour is inferred from
the feed bean's type is unchanged. And the terminal-failure contract in the last section above still holds for the one
projection a feed carries; what it no longer does is strand siblings, because there are none.
