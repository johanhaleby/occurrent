# 16. DCB DSL Module

Date: 2026-05-23

## Status

Accepted

## Context

Occurrent already has DSL modules for stream-based queries, CloudEvent subscriptions, modules, views, and deciders. DCB adds a different read model: queries are based on DCB metadata, reads return a DCB sequence position, and append conditions use that position.

Putting DCB methods into the existing query and subscription DSLs would make those DSLs harder to explain and would add DCB dependencies to users that only use stream-based event sourcing. At the same time, DCB-written events are still normal CloudEvents, so existing subscription models can deliver them.

## Decision

Add a separate opt-in blocking module:

- Maven module: `dsl/dcb-dsl/blocking`
- Artifact: `dcb-dsl-blocking`
- Package: `org.occurrent.dsl.dcb.blocking`

The module provides DCB domain query helpers over `DcbEventStore` and `CloudEventConverter`.

The query API is deliberately smaller than `DomainEventQueries`:

- Java callers use static helpers on `DcbDomainEventQueries`.
- Kotlin callers use extension functions on `DcbEventStore`.
- `query(...)` returns a `Stream<E>`, `Sequence<E>`, or `List<E>` depending on the helper.
- `queryWithPosition(...)` returns `DcbDomainEventStream<E>` when callers need the observed DCB sequence position.

The module also provides live subscription helpers:

- Kotlin callers use `Subscribable.subscribeDcb(...)`.

These helpers subscribe to DCB-tagged CloudEvents and then apply exact `DcbQuery` matching in process. They are subscription conveniences, not DCB reads. They do not provide a DCB high-watermark, append-condition semantics, or replay consistency beyond the configured subscription model.

The DCB subscription helpers reuse the existing subscription DSL `EventMetadata` instead of introducing a separate DCB metadata type. The DCB module adds opt-in Kotlin extension properties for `dcbPosition` and `dcbTags`. This keeps one metadata object for live CloudEvent delivery while avoiding a DCB dependency in the regular subscription DSL.

The existing DSLs remain unchanged:

- `DomainEventQueries` remains the stream/general CloudEvent query DSL.
- `Subscriptions` remains the general CloudEvent subscription DSL.
- `EventStoreOperations` remains a general CloudEvent administration API.
- `Module` does not grow DCB entry points in this iteration.

DCB query matching is shared through the DCB API so stores and DSL subscriptions use the same type, tag, OR-item, and excluded-type semantics.

DCB-written events keep Occurrent `streamid` and `streamversion` metadata, even when the store is configured with only the DCB capability. DCB-only disables stream APIs and stream indexes, but it does not change the CloudEvent shape written to storage.

## Consequences

Positive:

- Stream-based users do not get new DCB dependencies or methods.
- DCB users get small helpers that preserve DCB concepts instead of hiding the sequence position.
- Live DCB subscriptions are convenient while staying honest about their consistency model.
- DCB and regular subscription callbacks can use the same metadata type.
- Shared query matching reduces the risk of drift between stores and subscription helpers.

Negative:

- Users must add another module when they want DCB DSL helpers.
- There are now separate helper entry points for stream/general CloudEvent queries and DCB queries.
- DCB subscription helpers may receive a broader set of CloudEvents from the underlying subscription model and filter them in process.
- `dcb-dsl-blocking` depends on `subscription-dsl-blocking` to reuse `EventMetadata`.
