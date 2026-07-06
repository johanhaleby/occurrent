# 50. Stream catch-up subscriptions exclude DCB events

Date: 2026-07-06

## Status

Accepted

## Context

[ADR 49](0049-dcb-reads-exclude-non-dcb-stream-events.md) fixed issue #279: on a store with both `STREAM` and `DCB`
capabilities enabled, DCB reads were matching stream-written events that never carried DCB tags. That fix added a
`dcbTags`-exists guard at the DCB query-building layer of every store, plus the live DCB subscription match stage.

While reviewing that fix we found the reverse gap, filed as issue #281. On the same dual-capability store, plain
`Filter`-based reads and subscriptions had no DCB-tag awareness at all, so they could return DCB-tagged events mixed in
with stream-only ones. `StreamCatchupSubscriptionModel` and its reactor twin `ReactorStreamCatchupSubscriptionModel`
both promise in their own Javadoc that they "replay historic stream events", but with no guard they replayed and
live-delivered DCB-tagged events too.

The first instinct was to make the guard live where ADR 49's guard lives, at the generic query layer
(`EventStoreQueries`, `PositionOrderedReader`), mirroring #279 exactly. On review that is wrong. `EventStoreQueries` and
the plain native, Spring, reactor, and in-memory subscription models carry no "Stream" branding and never promised
capability-scoped results, unlike `DcbCriteria`, which [ADR 17](0017-introduce-dcb-as-shared-cloudevent-capability.md)
and [ADR 18](0018-spring-mongo-event-store-capabilities.md) explicitly scoped to DCB-tagged events. A plain query
caller asks for everything matching its `Filter` and should keep getting exactly that. Only
`StreamCatchupSubscriptionModel` (and its reactor twin) made the "stream events" promise, so only it should enforce it.
The corrected design keeps the generic layer neutral by default and gives any caller an explicit, reusable way to opt
into capability scoping, then has `StreamCatchupSubscriptionModel` use exactly that to keep its own promise honest.

The `@StreamSubscription` and `@DcbSubscription` annotations were investigated and need no change. They already build a
plain `Filter` or `DcbCriteria` from `eventTypes()` or `tags()` and route through `StreamCatchupSubscriptionModel` or
`DcbCatchupSubscriptionModel` respectively, so fixing the model classes covers annotation-driven subscriptions for free,
with no annotation or bean-post-processor change. A unified `@Subscription` annotation was considered and rejected.
[ADR 24](0024-stream-and-dcb-subscription-model-split.md) already documents why the stream and DCB split exists for a
load-bearing reason: stream and DCB subscriptions have incompatible start-position types and DCB's `tags()` has no
stream equivalent, so the split turns a whole class of silent runtime mismatches into compile errors. The annotation
split inherits that same reasoning, and reviving a unified annotation would undo it.

## Decision

**A dedicated `Filter.capability(EventStoreCapability)` primitive.** `Filter` gains a `CapabilityFilter` case and a
`Filter.capability(EventStoreCapability)` factory. A DCB append always stamps the DCB tags extension, so `DCB` matches
events that carry it and `STREAM` matches events that lack it. This is a dedicated case, not a generic
`exists`/`notExists` field primitive, because nothing else in the codebase needs a general field-existence filter today.
A general primitive would commit every `Filter` converter to implementing it forever for a speculative caller, and it
would force the awkward problem of mapping an arbitrary CloudEvent extension name to a per-store field name. The
capability case only ever needs the single known mapping from the DCB capability to each store's `dcbTags` artifact,
and the concrete field or extension name each store checks lives in that store's `Filter`-conversion code, not in
`Filter` itself.

**`EventStoreCapability` relocated to a new `common/eventstore-capability` module.** The enum previously lived in
`eventstore/api/common`. `common/filter` cannot depend on `eventstore-api-common` without creating a cycle, because
`eventstore-api-common` already depends on `common/filter`. `EventStoreCapability` is a plain dependency-free enum, so
it moved into a new minimal module that both `common/filter` and `eventstore-api-common` depend on, keeping the same
`org.occurrent.eventstore.api` package so no import anywhere changed. The new module is also the single home for the
DCB tag extension name constant that #279's guard sites already needed to share.

**`StreamCatchupSubscriptionModel` composes the capability filter for both replay and live handover.** It is
store-agnostic, so it gets no store-specific guard. Instead it ANDs `Filter.capability(EventStoreCapability.STREAM)`
into the `Filter` it uses for its own catch-up reads (the time-based `query`/`count` path, the position-based
`readInPositionOrder` path, and the during-catch-up reconciliation re-read), and into the `Filter` it hands to the
delegated live subscription at handover, including the paths that resume straight to live without a replay phase. The
caller's own filter is still honored. The capability guard is composed on top of it. The reactor twin does the same,
including the in-process live predicate it matches events against. Because the live models already translate an
`OccurrentSubscriptionFilter` generically, they need no capability-awareness of their own.

**No index or schema change.** This mirrors ADR 49's reasoning in reverse. The `STREAM` guard resolves to
`$exists: false` on the `dcbTags` field, and a sparse index cannot accelerate `$exists: false` (a sparse index only
contains entries for documents that have the field). A marker-field migration on the already-shipped `STREAM` write
path would be disproportionate for a capability that has zero production deployments today.

## Consequences

A plain `EventStoreQueries` caller sees no behavior change at all. `EventStoreQueries.query(Filter.all())` on a
dual-capability store still returns both stream and DCB events. A plain native, Spring, reactor, or in-memory
subscription model that is not wrapped in `StreamCatchupSubscriptionModel` also still delivers DCB events with no
filter. The generic layer stays neutral.

Only `StreamCatchupSubscriptionModel` subscribers get capability-scoped delivery, including `@StreamSubscription`-annotated
methods, which inherit the fix by routing through it. Such a subscriber only ever sees stream-capability events, in both
the replay and the live phases, even when it supplies its own `OccurrentSubscriptionFilter` that would otherwise match a
DCB event.

There is zero production impact, because DCB has no live deployments. A future reader should not add a startup guard,
migration, or index for this change. None is needed.
