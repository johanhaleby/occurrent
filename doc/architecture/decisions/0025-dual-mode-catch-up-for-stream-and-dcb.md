# 25. Dual-mode catch-up for STREAM-and-DCB applications

Date: 2026-06-27

## Status

Accepted

## Context

ADR 0020 gave `CatchupSubscriptionModel` a DCB mode that replays history by `dcbposition`, and ADR 0022 wired it in the Spring Boot starter for DCB-only applications. Both left one case open: an application that has both the STREAM and the DCB capability.

A `CatchupSubscriptionModel` instance was stream or DCB, never both. The mode was fixed at construction: one set of constructors took an `EventStoreQueries` for stream catch-up, another took a `DcbEventStore` and a `DcbQuery` for DCB catch-up, and `subscribe` routed every subscription to that one mode through an `isDcbMode()` flag. So in a STREAM-and-DCB application the starter wired the stream catch-up model, and DCB subscriptions on it got no catch-up: they could only see events written after they subscribed. ADR 0022 named this as the remaining follow-up.

What makes a clean fix possible now: a DCB subscription is identifiable. After ADR 0023 a DCB subscription carries a `DcbSubscriptionFilter`, and a DCB resume carries a `DcbSubscriptionPosition`. So the model can tell a DCB subscription from a stream one per call, rather than per instance.

## Decision

Make `CatchupSubscriptionModel` able to hold both catch-up backends and route each subscription to the right one.

- Add a dual-mode constructor `CatchupSubscriptionModel(subscriptionModel, eventStoreQueries, dcbEventStore, dcbQuery, config)` that keeps both the stream query API and the DCB store.
- Route per subscription in `subscribe`. A model with only one backend routes there, preserving the original single-mode behavior exactly. A dual-mode model routes by the subscription's nature: a `DcbSubscriptionFilter` or a resume from a `DcbSubscriptionPosition` goes to DCB catch-up, everything else goes to stream catch-up. The old per-instance `isDcbMode()` flag is gone.
- In the Spring Boot starter, build the dual-mode model when the event store has both STREAM and DCB capabilities. STREAM-only and DCB-only keep their existing single-mode wiring. The DCB side is still constructed with `DcbQuery.all()`, the shared query every `DcbSubscriptions` subscription narrows in its own consumer, exactly as in DCB-only mode (ADR 0022).

The DCB replay still uses the model's `DcbQuery` and the stream replay still uses the time-based query, so the two modes are the ones already proven by ADR 0020 and the existing stream catch-up. This change only lets one instance offer both and pick per subscription. The durable, competing-consumer, and change-stream machinery underneath is untouched.

## Consequences

- A STREAM-and-DCB application now gets catch-up for both its stream subscriptions and its DCB subscriptions from the single wired subscription model. This closes the follow-up ADR 0022 deferred.
- The routing depends on a DCB subscription carrying a `DcbSubscriptionFilter` or a `DcbSubscriptionPosition`. That holds for the `DcbSubscriptions` DSL and the typed `DcbSubscriptionModel`, which always pass the DCB filter. A caller that hand-rolls a DCB subscription with a stream filter and no DCB position would be routed to stream catch-up, which is the same ambiguity the type split in ADR 0024 removes at the typed entry points.
- Single-mode behavior is unchanged, so DCB-only and STREAM-only applications are unaffected.
