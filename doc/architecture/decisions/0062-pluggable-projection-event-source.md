# 62. Pluggable projection event source (push subscription model)

Date: 2026-07-18

## Status

Accepted. The push subscription model and the v1 replay-then-push bootstrap catch-up (broker owns live-resume) are
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

## Decision: replay-then-push bootstrap catch-up (v1, broker owns live-resume)

A push feed cannot backfill a new or rebuilt projection, because a broker is not a log. `ReplayThenPushSubscriptionModel`
(blocking and reactor) fills that gap with a one-time bootstrap in front of a `PushSubscriptionModel`. It splits
responsibility deliberately:

- **Bootstrap is Occurrent's job**, run once per subscription id. On subscribe it registers on the live feed first and
  buffers, replays the projection's history from the event store in position order (`PositionOrderedReader`), then drains
  the buffer and goes live, de-duplicating the replay-to-live overlap by event id. Because buffering starts before the
  head is read, an event committing during the replay is delivered either by the replay or by the buffered feed, and no
  reconcile pass is needed. The buffer is bounded and fails loud on overflow, so a full from-scratch rebuild over a live
  feed (unbounded buffering) is a fail-loud error rather than silent truncation: rebuild offline instead.
- **Live-resume is the broker's job, not Occurrent's.** After bootstrap the listener acknowledges each message only once
  `accept(...)` returns, so an unprocessed event is redelivered. The model persists no live position watermark, which is
  what sidesteps the watermark problem below entirely. Delivery is at-least-once, so the fold must be idempotent, the
  same contract as the change-stream path.
- **A one-shot bootstrap-complete marker** (an optional `CheckpointStorage`) records that the replay finished, so a
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

Only stream and capability-agnostic subscription filters can be bootstrap-replayed (their plain `Filter` drives the
position-ordered read). A DCB subscription filter is rejected, since a DCB boundary needs a different replay read.

**Consequence and limit.** Correctness across a restart depends on the broker retaining the backlog for an offline
consumer (a durable queue with a preserved offset). If the consumer is offline longer than the broker retains, or the
offset or bootstrap marker is lost, the projection must be rebuilt. For RabbitMQ specifically a durable queue already
retains messages for an offline consumer, so this v1 covers the common production case.

## Non-goal: broker-independent resume (v2)

Making live-resume independent of broker retention is a deliberate non-goal, and may never be built. The decision is a
boundary of responsibility: delivery guarantees for the live feed, ordering and no-loss redelivery, belong to the
transport that already provides them (RabbitMQ, Kafka), not to Occurrent reconstructing them. The push source is meant to
be driven by a transport that guarantees ordering. Occurrent's job is the one-time bootstrap from the event store, not to
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
bean-post-processor wraps in `ReplayThenPushSubscriptionModel` with the event store as the replay reader and the
framework's `CheckpointStorage` as the bootstrap marker. The default `Source.EVENT_STORE` keeps the existing behavior.
An explicit attribute was chosen over auto-detecting a push bean, since a durable public annotation should not change its
event source implicitly based on which beans happen to be present. Push source is rejected together with
`mode = SYNCHRONOUS` and the catch-up start knobs (the bootstrap always replays from the beginning and live-resume is the
broker's job), and with a `DcbProjection` (a DCB boundary cannot be bootstrap-replayed in position order). Implemented on
both the blocking and reactor stacks.
