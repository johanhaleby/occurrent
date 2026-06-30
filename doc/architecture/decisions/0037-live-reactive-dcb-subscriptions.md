# 37. Live reactive DCB subscriptions

Date: 2026-06-30

## Status

Accepted

## Context

The reactive stack gained a DCB event store (ADR 34), application service (ADR 35), and query DSL (ADR 36). It still had no way to subscribe to DCB events. The blocking stack has a `DcbSubscriptionModel` facade and a `DcbSubscriptions` DSL. This adds the live reactive equivalent.

Most of the machinery is already shared and reactive-agnostic: `DcbSubscriptionFilter` and `DcbStartAt` live in `subscription-core`, `DcbSubscriptionFilterConverter` builds the change-stream `$match`, and `ReactorMongoSubscriptionModel` already applies a `DcbSubscriptionFilter` server-side. So the missing pieces are a reactive facade and a reactive DSL.

## Decision

Add live reactive DCB subscriptions. Live only. History replay by `dcbposition` (catch-up) is a separate, larger piece and is deferred.

### Reactive DcbSubscriptionModel facade

A new `org.occurrent.subscription.api.reactor.DcbSubscriptionModel` shaped like the reactive `SubscriptionModel`, not the blocking one. It is `Flux` based with no subscription id and no callback: `Flux<CloudEvent> subscribe(DcbQuery query, DcbStartAt startAt)` plus default overloads, and `from(SubscriptionModel delegate)`. The adapter subscribes the delegate with `DcbSubscriptionFilter.filter(query)` and `startAt.toStartAt()`, then keeps an in-process `dcbposition > 0 and matches(query)` floor so the subscription stays scoped to its own query even on a backend that does not honor the server-side filter. This mirrors the blocking adapter.

### Reactive DcbSubscriptions DSL

Added to the reactor DCB DSL module: `Flux<E> subscribe(DcbQuery, DcbStartAt)` and `Flux<DcbEvent<E>> subscribeWithMetadata(...)`, where `DcbEvent<E>` carries the domain event and a `DcbEventMetadata`. The `Flux` is the subscription, so it is cancelled by cancelling the downstream subscription, for example disposing the `Disposable` returned by `subscribe()`. `DcbEventMetadata` is built directly from the `CloudEvent` (`dcbPosition`, `dcbTags`), because the reactive subscription stack has no generic `EventMetadata` wrapper, so reading the CloudEvent is simpler than mirroring the blocking wrapper.

### Live only, for now

A `DcbStartAt` is passed through to the underlying `SubscriptionModel`, so whether a replay-oriented start such as `DcbStartAt.beginning()` or `afterPosition(...)` replays history depends on that model. The current reactive subscription models have no DCB catch-up, so such a start behaves like a live start today. This is documented on the facade and the DSL. Live subscriptions cover activity feeds and triggers. Rebuilding a projection from history needs the deferred catch-up phase.

## Consequences

- Reactive callers can subscribe to live DCB events filtered by `DcbQuery`, server-side, and convert them to domain events with metadata, all as a `Flux`.
- The reactive stack still cannot replay DCB history reactively. Reactive `dcbposition` catch-up is the remaining phase in `.context/reactive-dcb-plan.md`. There is no reactive catch-up for streams either, so that phase pioneers reactive catch-up rather than mirroring existing reactive code.
- `DcbEventMetadata` now exists in a blocking and a reactive form, reading the same CloudEvent extensions.
