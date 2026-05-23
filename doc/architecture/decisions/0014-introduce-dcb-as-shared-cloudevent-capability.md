# 14. Introduce DCB as shared CloudEvent capability

Date: 2026-05-22

## Status

Accepted

## Context

Occurrent is stream-first and stores CloudEvents. Existing APIs, subscriptions, queries, and application services expect CloudEvents to be the integration boundary.

Dynamic Consistency Boundary, DCB, requires:

- globally ordered events,
- explicit tags,
- reads by type/tag query after a sequence position,
- append conditions of the form "fail if matching events exist after this sequence position".

ADR 7 explored replacing stream semantics with global positions and checkpoints and was rejected. ADR 8 proposed a stream-first approach with global position, tags, checkpoint-based consistency, and a DCB ApplicationService. ADR 9 accepted that `StreamReadFilter` is only a read optimization and must not be treated as a correctness mechanism.

A separate DCB store was considered, but it creates a parallel ecosystem. Normal subscriptions, projections, and queries would need DCB-specific counterparts because they would not observe the same CloudEvents.

## Decision

Introduce DCB as an explicit optional capability over normal persisted CloudEvents.

DCB-capable event stores keep existing stream APIs and semantics, and additionally implement a small DCB API. DCB appends still write to a stream, but they also assign global DCB metadata.

DCB metadata is represented in two forms:

- CloudEvent extensions:
  - `dcbtags`: canonical encoded tags, visible to normal CloudEvent consumers.
  - `dcbposition`: store-assigned global sequence position, visible on read.
- Storage-native metadata:
  - indexed `dcbTags` array in MongoDB for efficient DCB matching.
  - numeric `dcbposition` field for ordering and high-watermark reads.

DCB matching uses indexed metadata and CloudEvent extensions, never payload inspection.

DCB query items are OR-combined. Within one query item, included CloudEvent types
match as any-of, DCB tags match as all-of, and excluded CloudEvent types match as
none-of. Excluded types refine a positive selector; an item with only exclusions is
not a valid DCB query item.

The first implementation provides:

- blocking DCB API,
- in-memory DCB support in the existing `InMemoryEventStore`,
- Spring Mongo blocking DCB support in the existing `SpringMongoEventStore`,
- blocking DCB application service using `CloudEventConverter`, `TagGenerator`, and partitioned stream id generation.

Existing stream APIs remain backward compatible:

- `EventStore`, `WriteCondition`, and `WriteResult` keep stream-version semantics.
- Existing stream writes do not need DCB tags.
- Existing events without DCB metadata remain valid stream events but do not participate in DCB tag queries unless backfilled or rewritten with tags.

DCB append conditions are enforced by checking for matching events after the supplied sequence position and by advancing deterministic checkpoint keys in sorted order. The checkpoint strategy may be conservative and false-conflict, but it must not allow write skew.

## Consequences

Positive:

- DCB-written events remain normal CloudEvents.
- Existing subscriptions, queries, and projections can observe DCB-written events.
- DCB is opt-in and does not expand the core stream `EventStore` API.
- Stream and DCB usage can run side by side over the same event collection.

Negative:

- DCB-capable stores need additional metadata, indexes, and checkpoint collections.
- DCB support is initially limited to blocking in-memory and Spring Mongo.
- Existing events need explicit tag metadata before they can participate in DCB queries.
- Checkpoint writes add write amplification and operational complexity.
