# 42. Reactive MongoDB subscription model resilience

Date: 2026-07-01

## Status

Accepted

## Context

`ReactorMongoSubscriptionModel` had no error handling at all. `subscribe(...)` built a `Flux<CloudEvent>` directly from `ReactiveMongoOperations.changeStream(...)` with no retry, no restart, and no backoff. A replica-set failover, a transient network error, or `ChangeStreamHistoryLost` (error code 286) terminated the `Flux` with an `onError` signal and the subscription was silently dead. `SpringMongoSubscriptionModel` survives this class of MongoDB operational disruption in production, and `NativeMongoSubscriptionModel` was brought to the same level in a companion change (its own ADR, not yet merged as of this writing). This PR brings the reactive model to the same level, while keeping its existing `subscribe(filter, startAt) -> Flux<CloudEvent>` contract unchanged so every current consumer (`ReactorDurableSubscriptionModel`, the DCB subscription adapter, and anyone else building on `PositionAwareSubscriptionModel`) gets the resilience without any code change on their side.

## Decision

`subscribe(...)` now tracks the position of the last change-stream document read in an `AtomicReference<StartAt>`, and wraps the change stream in `retryWhen(...)`:

```java
return Flux.defer(() -> {
    AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
    return changeStream(filter, currentStartAt)
            .retryWhen(Retry.backoff(Long.MAX_VALUE, config.minBackoff)
                    .maxBackoff(config.maxBackoff)
                    .filter(throwable -> shouldRestart(throwable, currentStartAt)));
});
```

`changeStream(filter, currentStartAt)` is itself a `Flux.defer(...)`, so `retryWhen` resubscribing to it re-reads `currentStartAt` on every attempt, including the retried ones. The outer `Flux.defer` gives each subscriber of the returned `Flux` its own tracked position. Inside `changeStream(...)`, the `flatMap` that processes each raw change-stream event advances `currentStartAt` to the resume token of every document received, even if it doesn't deserialize into a delivered CloudEvent, mirroring `NativeMongoSubscriptionModel`'s tracking, so a retry resumes gap-free instead of replaying or skipping events.

The `filter` predicate (`shouldRestart`) classifies the error, the same way `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` do:

- `MongoCommandException` with code 286 (`ChangeStreamHistoryLost`, unwrapped from Spring's `UncategorizedMongoDbException` if wrapped): restarts from `StartAt.now()` only when `ReactorMongoSubscriptionModelConfig.restartSubscriptionsOnChangeStreamHistoryLost` is `true` (default `false`). Otherwise it logs at ERROR and the filter returns `false`, so Reactor propagates the original error and the subscription stops, same as the other two models.
- Anything else (a failover, a transient network error, or anything else the driver itself could not resume): logged at WARN, the filter returns `true`, and the retry resumes from the tracked position.

`Retry.backoff(Long.MAX_VALUE, minBackoff).maxBackoff(maxBackoff)` uses Reactor's default 2.0 multiplier, matching the exponential backoff shape `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` use via `RetryStrategy.exponentialBackoff`. `ReactorMongoSubscriptionModelConfig` carries `minBackoff`/`maxBackoff` (default 100 ms / 2 s) and `restartSubscriptionsOnChangeStreamHistoryLost` (default `false`), added via a new constructor overload, the existing 3-arg constructor is unchanged.

The config carries backoff bounds rather than a full pluggable `reactor.util.retry.Retry`, unlike `GenericApplicationService`/`GenericDcbApplicationService`/`ReactorMongoEventStore` elsewhere in the reactive stack, which do expose a `Retry` constructor parameter. Those call sites don't layer any model-owned classification on top of the caller's retry spec. This one does (the history-lost branching and the position-bump side effect), and a `RetryBackoffSpec`'s `.filter(...)` can't be composed with an externally supplied `Retry` without either constraining the config to that concrete type or reimplementing `Retry` as a wrapper. Two `Duration` fields avoid that composability problem entirely and are the two knobs that actually vary in practice.

Disposal (a subscriber calling `Disposable.dispose()`) sends a `cancel()` signal upstream, not an `onError`, so `retryWhen` is never invoked on cancellation. Unlike `NativeMongoSubscriptionModel`, no "intentionally closed" flag is needed here.

## Consequences

- `ReactorMongoSubscriptionModel` survives the same class of MongoDB operational disruption `SpringMongoSubscriptionModel` and `NativeMongoSubscriptionModel` do, recovering gap-free from the position of the last change-stream document it read.
- The `subscribe(filter, startAt) -> Flux<CloudEvent>` contract is unchanged. This is purely additive, existing callers get the resilience automatically.
- Verified against real MongoDB (a replica-set Testcontainer) by mocking `ReactiveMongoOperations.changeStream(...)` to error once with the exact exception types a failover or a 286 response would surface, then delegate to the real operations, the same technique `SpringMongoSubscriptionModelTest` and the native model's resilience test use. A live multi-node failover test was rejected for the same reason given in ADR 41, the deterministic injection tests exercise the identical code path with far less infrastructure and flakiness risk.
- This PR intentionally does not add subscription lifecycle (named subscriptions, pause, resume, cancel) to the reactive model. That is tracked separately as reactive lifecycle parity, stacked on this change.
