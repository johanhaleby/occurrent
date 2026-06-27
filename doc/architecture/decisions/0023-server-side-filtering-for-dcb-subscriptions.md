# 23. Server-side filtering for DCB subscriptions

Date: 2026-06-27

## Status

Accepted

## Context

ADR 0019 introduced the DCB DSL and recorded a v1 simplification: `DcbSubscriptions` subscribed with `OccurrentSubscriptionFilter.filter(Filter.all())` and post-filtered every delivered CloudEvent in process by a `DcbQuery` (`DcbCloudEvents.getPosition(event) > 0` and `DcbCloudEvents.matches(event, query)`). A stream subscription instead accepts a `Filter` that becomes a server-side MongoDB change stream `$match`, so the `@Subscription` annotation can push `Filter.type(...)` down to the database.

That gap matters under load. A DCB read model that cares about a few event types still received every DCB event in the store and discarded most of them in the consumer thread. The course-enrollment dashboard subscriber is the concrete example: it subscribed with `DcbQuery.all()` and did an is-type check per event.

The subscription filter layer already has the seam needed to fix this. `org.occurrent.subscription.SubscriptionFilter` is an empty marker interface. `OccurrentSubscriptionFilter` is its stream implementation, wrapping an Occurrent `Filter`. The Mongo subscription models already dispatch on the concrete `SubscriptionFilter` type: `ApplyFilterToChangeStreamOptionsBuilder` (Spring) and `NativeMongoSubscriptionModel` (native driver) translate `OccurrentSubscriptionFilter` to a change stream `$match` and throw on filter types they do not recognize.

The user asked whether the filter abstraction itself should be split into a common part plus a stream-specific and a DCB-specific part, rather than bending one filter type to serve both, and asked for the choice that holds up long term and under full production load.

The DCB query already translates to MongoDB. `SpringMongoEventStore.toCriteria(DcbQueryItem)` turns a query item into criteria for DCB reads: types to `$in` on `type`, excluded types to `$nin`, tags to `$all` on the stored `dcbTags` indexed array. The event store writes that `dcbTags` array next to the newline-joined `dcbtags` extension precisely so tag containment is queryable.

## Decision

Treat `SubscriptionFilter` as the common abstraction and give DCB its own implementation rather than forking the stream `Filter`.

- Add `org.occurrent.subscription.DcbSubscriptionFilter`, a record wrapping a `DcbQuery`, in `subscription/core`. It is the DCB counterpart to `OccurrentSubscriptionFilter`: the stream filter wraps a `Filter`, the DCB filter wraps a `DcbQuery`. The stream `Filter` class is not changed or forked.
- Add `DcbSubscriptionFilterConverter` in `subscription/mongodb/common/base`. It converts a `DcbQuery` into one change stream `$match` aggregation stage that reproduces the `DcbCloudEvents.matches` semantics server-side: within an item, types are any-of (`$in`), tags are all-of (`$all` on the `dcbTags` array), excluded types are none-of (`$nin`), and the items are OR-ed. A `dcbposition > 0` condition is always applied so only DCB-tagged events are delivered, matching the in-process guard. All fields are matched under the `fullDocument` prefix the change stream wraps the stored document in.
- One converter serves both Mongo backends. It returns an `org.bson.Document` `$match` stage, which the Spring model passes to `ChangeStreamOptionsBuilder.filter(Document...)` and the native model adds straight to its pipeline. Both gain a `DcbSubscriptionFilter` dispatch branch.
- The in-memory subscription model honors `DcbSubscriptionFilter` too, matching in process via `DcbCloudEvents`, so non-Mongo subscribables filter correctly rather than throwing on the new filter type. `InMemorySubscription` now holds a `Predicate<CloudEvent>` instead of a stream `Filter`, which is the general shape both the stream and DCB cases reduce to.
- `DcbSubscriptions` passes a `DcbSubscriptionFilter(query)` to the backend instead of `Filter.all()`. It keeps its in-process `getPosition > 0 && matches(query)` check as a correctness floor, because the DSL wraps an arbitrary `Subscribable` and cannot assume the backend honors the filter. A capable backend reduces volume server-side, and the floor guarantees correctness everywhere.
- The `dcbTags` array field name was a private constant duplicated in the event store. Promote it to the public `OccurrentCloudEventMongoDocumentMapper.DCB_TAGS_INDEX_FIELD` so the event store that writes it and the subscription that matches against it share one definition. This is the storage contract between writer and reader.

On the question of splitting the `Filter` class itself: do not. `SubscriptionFilter` is already the common seam, so the clean split is stream `Filter` for stream subscriptions and `DcbQuery` for DCB subscriptions, each an implementation of `SubscriptionFilter` over shared Mongo plumbing. The two query models share no structure today that a common filter base would capture without inventing it. If a genuine shared core emerges later, extract it then rather than speculatively now.

## Consequences

- A DCB subscription against MongoDB now filters server-side. A read model that cares about a few types or a tag boundary no longer pulls every DCB event into the consumer.
- The change stream `fullDocument` carries the `dcbTags` array, since the event store stores it on the document, so tag all-of works server-side as `$all` on that array rather than as a brittle match on the newline-joined `dcbtags` string.
- All three blocking subscription backends (Spring Mongo, native Mongo, in-memory) accept `DcbSubscriptionFilter`. Routing `DcbSubscriptions` through it does not break any shipped backend. The reactor model is out of scope, since DCB support is blocking only. The native-driver model gains the filter for parity, but there is no native-driver DCB event store today (DCB append lives in the Spring Mongo and in-memory stores), so the native path is covered by the shared converter's unit tests and the Spring end-to-end test rather than by a synthetic native end-to-end test over a workflow that does not exist yet.
- `CatchupSubscriptionModel`, the decorator the DCB stack wires above the Mongo model, had a single upfront guard that rejected every filter except `OccurrentSubscriptionFilter`. That guard belongs to the stream catch-up path, which converts the filter into an Occurrent `Filter` for the historical query. It now lives only there, so the DCB path passes a `DcbSubscriptionFilter` straight through to the inner model.
- This supersedes the ADR 0019 note that DCB subscriptions only post-filter in process. The in-process check remains as a correctness floor, not as the only filter.
- The DCB query to Mongo translation now exists in two places: `SpringMongoEventStore.toCriteria` for DCB reads (spring-data `Criteria`, bounded position window, no prefix) and `DcbSubscriptionFilterConverter` for change stream matches (`Document`, `fullDocument` prefix, `dcbposition > 0`). They differ in field prefix and position handling, so they are kept separate rather than unified speculatively. Unify them behind one parameterized converter only if a third caller appears or they start drifting.
