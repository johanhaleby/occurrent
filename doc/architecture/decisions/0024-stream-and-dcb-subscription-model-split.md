# 24. Split the subscription model and start position into stream and DCB types

Date: 2026-06-27

## Status

Accepted

## Context

A subscription has two flavors now: stream subscriptions filtered by an Occurrent `Filter` and started at a time, and DCB subscriptions filtered by a `DcbQuery` and started at a `dcbposition`. Both go through one `SubscriptionModel` whose `subscribe` takes a `SubscriptionFilter` and a `StartAt`, and `StartAt` is a shared union over every position type.

So nothing stops a caller mixing the two. A DCB subscription can be handed a `StartAt.subscriptionPosition(TimeBasedSubscriptionPosition)`, and a stream subscription a `DcbSubscriptionPosition`. The mismatch is only reconciled at runtime, and the failure is quiet: a time-based start on a DCB subscription does not replay, it silently goes live. The `DcbSubscriptions` DSL took a generic `StartAt`, so it carried the same hole.

The goal is to make those illegal states unrepresentable, with a full split into separate model and start-position types rather than only a position marker.

The constraint is that the machinery underneath is genuinely shared. DCB events flow through the same MongoDB change stream as stream events (ADR 0017), and the durable, competing-consumer, and catch-up decorators wrap one subscription model for both. So the split has to live at the API and the filter/position/query type level, over a shared core, not as a fork of the decorator lifecycle.

The `dcbposition` start type, `DcbSubscriptionPosition`, lived in the catch-up module, which sits above the subscription API. A DCB start type that produces it cannot then live in the API or core where the typed model interfaces belong.

## Decision

Split at the type and facade level, leaving the shared lifecycle untouched.

- Move `DcbSubscriptionPosition` down to `subscription-core` (package `org.occurrent.subscription`), next to the other `SubscriptionPosition` value types. It depends only on `SubscriptionPosition` and `StringBasedSubscriptionPosition`, both already in core, so the move is clean. DCB is unreleased, so the package change costs nothing. This is the prefactor that lets the start type live low enough.
- Add `DcbStartAt` in `subscription-core`, the DCB counterpart to `StartAt`. It is a distinct sealed type that can only express DCB starts: `now()`, `subscriptionModelDefault()`, `beginning()` (replay from the start of the DCB sequence), and `afterPosition(lastProcessedPosition)` (resume after a known position). `toStartAt()` bridges to the generic `StartAt` the shared model consumes. Because it is a separate type, a DCB start cannot carry a time-based position and vice versa.
- Add `DcbSubscriptionModel` and `StreamSubscriptionModel` in `subscription-api-blocking`, typed facades over the shared `SubscriptionModel`. `DcbSubscriptionModel.subscribe` takes a `DcbQuery` and a `DcbStartAt`, `StreamSubscriptionModel.subscribe` takes a `Filter` and a `StartAt`. Neither exposes the low-level `subscribe(SubscriptionFilter, StartAt, ...)`, so neither can be handed the other's filter or position. Each is obtained with a static `from(SubscriptionModel)` that returns an adapter translating the typed call into the low-level one (building a `DcbSubscriptionFilter` or wrapping the `Filter` in an `OccurrentSubscriptionFilter`, and converting the start position) and forwarding every life-cycle call to the delegate. The decorator stack is not changed or forked.
- Rebase the `DcbSubscriptions` DSL on `DcbSubscriptionModel` and `DcbStartAt`. Its start parameter is now a `DcbStartAt`, and it builds a `DcbSubscriptionModel` from the injected `SubscriptionModel`.

Do not add auto-configured beans for the two facade interfaces yet. The `from(SubscriptionModel)` factories are enough, and `DcbSubscriptions` already gives the DCB-typed entry point. Decide bean wiring in the `@DcbSubscription` work, where a wired typed model is actually consumed, to avoid churning the characterized bean set now.

## Consequences

- A DCB subscription cannot be given a stream filter or a time-based start, and a stream subscription cannot be given a DCB query or a `dcbposition`, at the typed entry points. The mismatch that used to fail quietly at runtime is now a compile error.
- The shared durable, competing-consumer, catch-up, and change-stream machinery is untouched. The split is types and thin adapters, so there is one subscription lifecycle, not two.
- `DcbSubscriptionPosition` moved package. This is a source-incompatible change for any code importing it, which is acceptable while DCB is unreleased.
- The raw `SubscriptionModel` still exists and still takes any `SubscriptionFilter` and `StartAt`, for advanced or genuinely mixed use. The facades are the safe default, not a cage. A caller who wants the union can still reach it.
- The facade interfaces are obtained by wrapping, not by the concrete models implementing them. This keeps the generic decorator stack free of DCB and stream specifics, at the cost of the facade not being the bean type injected today. That tradeoff is revisited when the annotation work wires a typed model.
