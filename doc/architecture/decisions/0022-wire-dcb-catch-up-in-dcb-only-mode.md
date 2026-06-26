# 22. Wire DCB catch-up in DCB-only mode

Date: 2026-06-26

## Status

Accepted

## Context

ADR 0020 added a DCB mode to `CatchupSubscriptionModel` so a pure-DCB application can replay history by `dcbposition` before switching to live delivery. It also recorded the next step as future direction: the Spring Boot starter can wire catch-up in DCB-only mode.

Until now the starter did not. `OccurrentMongoAutoConfiguration` wrapped the Mongo subscription model in a `CatchupSubscriptionModel` only when the event store had the STREAM capability:

```java
if (eventStoreProperties.getCapabilities().contains(STREAM)) {
    subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, eventStoreQueries, config);
}
```

In a DCB-only application the subscription model was therefore `CompetingConsumer -> Durable -> SpringMongo`, with no catch-up. A read model fed by a `DcbSubscriptions` subscription could only see events that arrived after it subscribed. It could not rebuild from history, so after a restart it stayed empty until new events were written. The `occurrentDcbSubscriptions` bean Javadoc said as much and called the wiring a follow-up.

The catch-up model needs one `DcbQuery` at construction (ADR 0020). The auto-configured subscription model is a single shared bean used by every `DcbSubscriptions` subscription, and each subscription carries its own `DcbQuery`. So the wiring has to decide what query the one shared model is built with.

## Decision

Select the catch-up mode by event-store capability in `occurrentCompetingDurableSubscriptionModel`:

- STREAM present (STREAM-only or STREAM and DCB together): keep the existing stream `CatchupSubscriptionModel(durable, eventStoreQueries, config)`. Unchanged.
- DCB-only (`capabilities.contains(DCB)` and not STREAM): wrap with the DCB-mode `CatchupSubscriptionModel(durable, dcbEventStore, DcbQuery.all(), config)`, reusing the same `CatchupSubscriptionModelConfig`.
- Neither applies: leave the durable model unwrapped, as before.

The `DcbEventStore` is taken through an `ObjectProvider` so the bean method stays valid when the event store is disabled, and the DCB catch-up model is built only when a store is present.

Build the shared model with `DcbQuery.all()`. ADR 0020 framed the per-instance query as one projection one query, where the stored position belongs to that query. The shared auto-configured model is used differently. It replays every DCB event by `dcbposition`, and each subscription narrows to its own `DcbQuery` in the `DcbSubscriptions` consumer, which already post-filters by `DcbCloudEvents.getPosition(event) > 0` and `DcbCloudEvents.matches(event, query)`. This is the same shape the live path already has. The stored position is the global `dcbposition`, not a per-query offset, so it stays unambiguous across subscriptions even though they share one model. Each subscription has its own id and therefore its own stored position.

Do not build a dual-mode catch-up model for the STREAM-and-DCB case now. A single `CatchupSubscriptionModel` instance is stream or DCB, never both. Serving stream subscriptions by time and DCB subscriptions by `dcbposition` from one instance would need a dual-mode model and a way to pick the mode per subscription. The example that motivated this work is DCB-only, so the STREAM-and-DCB case keeps the stream catch-up it has today, and the dual-mode model is left as a follow-up. This keeps the change small and leaves a clear path to it.

## Consequences

Positive:

- A DCB-only Spring Boot application can rebuild a read model from history. A subscription started at a `DcbSubscriptionPosition` replays by `dcbposition` and then goes live, with no change to the `DcbSubscriptions` API.
- One shared subscription model serves all DCB subscriptions, so the durable and competing-consumer wrappers and the position storage stay shared, as in stream mode.
- The change is confined to one bean method plus imports. The stream path is untouched.

Negative:

- The DCB-mode model replays all DCB events for any catching-up subscription and relies on the `DcbSubscriptions` consumer to narrow. A subscription over a small slice of a large store still pages the whole DCB sequence during catch-up. This matches how the live DCB path already reads all CloudEvents and post-filters.
- A STREAM-and-DCB application gets stream catch-up but not DCB catch-up. DCB subscriptions there remain live only until the dual-mode model is built.
- Triggering catch-up is still manual. A caller asks for replay from the start with `StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0))`. A declarative `@DcbSubscription` with catch-up and resume options, the DCB analog of `@Subscription`, is the natural next step and is left as a follow-up.
