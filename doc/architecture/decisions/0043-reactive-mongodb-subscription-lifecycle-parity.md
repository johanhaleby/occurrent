# 43. Reactive MongoDB subscription lifecycle parity

Date: 2026-07-01

## Status

Accepted

## Context

The reactive `SubscriptionModel` (`org.occurrent.subscription.api.reactor`) is intentionally a bare primitive: `subscribe(filter, startAt) -> Flux<CloudEvent>`. The caller subscribes to the `Flux` and disposes it themselves. There is no way to name a subscription, look it up later, pause it, resume it, or cancel it by id. The blocking side has had this since the beginning: a blocking `SubscriptionModel extends Subscribable, SubscriptionModelLifeCycle`, letting `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` expose named subscriptions with `pauseSubscription`, `resumeSubscription`, `cancelSubscription`, `isRunning`, `isPaused`, `start`, and `stop`. [ADR 42](0042-reactive-mongodb-subscription-model-resilience.md) brought the reactive model to error-resilience parity with the blocking one. This closes the remaining lifecycle gap.

## Decision

Two new interfaces in `subscription-api-reactor`, mirroring their blocking counterparts:

- `Subscribable`: `Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action)` plus the same three default overloads the blocking `Subscribable` has. The action returns `Mono<Void>`, matching the convention `ReactorDurableSubscriptionModel` already established, not `Consumer<CloudEvent>`.
- `SubscriptionModelLifeCycle`: `stop()`, `start()`/`start(boolean)`, `isRunning()`, `isRunning(id)`, `isPaused(id)`, `pauseSubscription(id)`, `resumeSubscription(id)`, `cancelSubscription(id)`, `shutdown()`. Identical shape to the blocking one. These stay synchronous rather than returning `Mono<Void>`, because they only touch in-memory bookkeeping (which named subscriptions are tracked where), not I/O.
- A new reactive `Subscription` interface: `id()` and `Mono<Void> waitUntilStarted()` (plus a default `Mono<Boolean> waitUntilStarted(Duration)` timeout variant), the non-blocking counterpart to the blocking `Subscription`'s blocking `waitUntilStarted()`.

Both are additive. The existing reactive `SubscriptionModel` (the `Flux` primitive) is untouched, so every existing caller of `subscribe(filter, startAt)` is unaffected. `ReactorMongoSubscriptionModel` now implements `PositionAwareSubscriptionModel, Subscribable, SubscriptionModelLifeCycle` directly, side by side, rather than through a new combined marker interface. Nothing else needs to program against "a reactive subscription model with lifecycle" as a type yet, so introducing one now would be speculative; if a second implementation needs the same combination, extracting a shared interface then is straightforward.

`ReactorMongoSubscriptionModel` tracks named subscriptions in `runningSubscriptions`/`pausedSubscriptions` (`ConcurrentHashMap<String, InternalSubscription>`), mirroring the blocking dual-map invariant. `InternalSubscription` holds the `Disposable` for the live subscription, the shared `AtomicReference<StartAt>` position tracker from ADR 42, the filter, and the action, so `resumeSubscription` can restart from exactly the same tracked position `pauseSubscription` left off at, the same "no replay, no gap" guarantee the error-restart path already has.

Named `subscribe(...)` reuses the resilient change stream from ADR 42 (factored out into `resilientChangeStream(...)`) rather than duplicating the retry wiring, and drives it with `.concatMap(action)`, so the next event is not processed until the current action's `Mono<Void>` completes, matching the blocking model's synchronous per-event semantics.

`waitUntilStarted()` is backed by a `Sinks.Empty<Void>` that completes via `doOnSubscribe` on the underlying `mongo.changeStream(...)` Flux. This only signals that the change stream has been subscribed to, not that the server has acknowledged the command and the cursor is positioned. That is a weaker guarantee than the blocking and native models have, where the equivalent signal fires only after a blocking round trip to the server has already completed. A stronger reactive signal would need to hook into MongoDB reactive driver internals for "command acknowledged" specifically, which Spring Data's `ReactiveMongoOperations` does not expose. Closing this gap fully was judged disproportionate to the value, since the only consumer of the distinction is a caller that writes immediately after `waitUntilStarted()` resolves and expects to see that specific write, an edge case rather than the common case.

## Consequences

- The reactive stack has full lifecycle parity with the blocking one: named subscriptions, pause, resume, cancel, start, stop, shutdown.
- Existing consumers of the `Flux` primitive (`ReactorDurableSubscriptionModel`, the DCB subscription adapter, anyone else building on `PositionAwareSubscriptionModel`) are unaffected, verified by `test-compile` across every module depending on `subscription-api-reactor`.
- `waitUntilStarted()`'s weaker readiness guarantee (subscribed, not confirmed-healthy) is a real, documented limitation. A caller that writes immediately after it resolves and expects to see that exact write can occasionally miss it under load, the same class of timing gap the native model's `waitUntilStarted()` javadoc already calls out for a different reason. This surfaced as measurable flakiness in this PR's own test suite when run in a resource-constrained environment, not as a logic defect, resolved by leaning on the standard `Awaitility`-based polling pattern already used elsewhere in this codebase's own Mongo tests rather than tightening the readiness signal itself.
