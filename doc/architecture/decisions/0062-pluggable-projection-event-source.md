# 62. Pluggable projection event source (push subscription model)

Date: 2026-07-18

## Status

Accepted (push subscription model). The replay-then-push catch-up handover in "Future direction" is proposed, not
implemented.

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

## Future direction: replay-then-push catch-up handover (proposed)

To let a push-fed projection resume after downtime without relying on the broker retaining the backlog, a catch-up model
would, on subscribe, register on the push feed first and buffer, replay history from the resume point to the store head
in position order (reusing `PositionOrderedReader`), then drain the buffer and switch to live, deduplicating the overlap
by event id (not by a position watermark, since Occurrent positions can commit late and a fixed watermark would drop a
late-committing low-position event). Because buffering starts before the head snapshot, no separate reconcile pass is
needed, unlike the seek-then-live catch-up for change streams.

This is deferred because it has genuine correctness questions that a naive implementation gets wrong, and it should be
settled before it ships:

- **Resume checkpoint safety over broker order.** The existing `DurableSubscriptionModel` checkpoints the last delivered
  position after each action and depends on the underlying subscription delivering in position order. A push feed
  delivers in broker order, so checkpointing "last position seen" from the live feed can skip a late-committing hole on
  restart, which is silent event loss rather than mere redelivery. A sound checkpoint tracks the contiguous
  gap-free frontier, or the model persists a checkpoint only from the position-ordered replay phase and relies on the
  broker plus idempotent folds for the live tail. This depends on the store's position-density semantics.
- **Bounded buffering.** Buffering the live feed across a full from-scratch replay is unbounded on a large history, so a
  full rebuild must replay offline from the event store and then attach the feed. The handover is bounded and sound only
  for resume with a small gap. The buffer needs a cap with fail-loud on overflow, not silent drop.
- **Missing position on live events.** A live event used to advance a checkpoint must carry `position`; if it is absent,
  fail loud rather than mis-checkpoint.
- **`@Projection` push-source selection.** Binding `@Projection` to a push source (routing to a configured push
  `SubscriptionModel` bean versus a new annotation attribute) is the most annotation-invasive piece and should be
  settled here before coding, together with whether the catch-up start knobs (`startAt`, `startAtGlobalPosition`,
  `resumeBehavior`) drive the replay start.

For RabbitMQ specifically, a durable queue already retains messages for an offline consumer, so the live push feed plus
an offline event-store replay for new projections covers the common production case without this handover.
