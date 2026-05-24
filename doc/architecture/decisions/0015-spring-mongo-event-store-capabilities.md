# 15. Spring Mongo Event Store Capabilities

Date: 2026-05-23

## Status

Accepted

## Context

`SpringMongoEventStore` supports the existing stream-based EventStore API and the new DCB API. The two APIs share the same CloudEvent collection, but they need different MongoDB support structures:

- stream reads and writes need the `streamid + streamversion` index,
- DCB reads and appends need `dcbposition`, `dcbTags`, a DCB position collection, and a DCB checkpoint collection.

Creating every index for every application is unnecessary for stream-only users. It also makes it harder to explain what happens when an application starts stream-only, adds DCB later, or moves back to a narrower capability set.

Index creation can be operationally expensive on large MongoDB collections. Production users should be able to plan capability changes, create indexes out-of-band when needed, and know which indexes can later be removed manually.

## Decision

Configure the Spring Mongo event store with a non-empty set of capabilities:

- `STREAM`
- `DCB`

The default is `{STREAM}` for backward compatibility.

`EventStoreConfig.Builder` exposes:

- `eventStoreCapabilities(Set<SpringMongoEventStoreCapability> capabilities)`
- `eventStoreCapabilities(SpringMongoEventStoreCapability capability, SpringMongoEventStoreCapability... additionalCapabilities)`

Initialization is capability-driven:

- Always create the event collection and CloudEvent `id + source` unique index.
- If `STREAM` is enabled, create the `streamid + streamversion` unique index.
- If `DCB` is enabled, create the `dcbposition` unique sparse index, the `dcbTags` index, the DCB position collection, and the DCB checkpoint collection.

The Spring Boot starter exposes this as:

- `occurrent.event-store.capabilities=stream`
- `occurrent.event-store.capabilities=dcb`
- `occurrent.event-store.capabilities=stream,dcb`

Spring Boot application-service auto-configuration follows the same capability set:

- If `STREAM` is enabled, the starter may create the regular stream-based `ApplicationService`.
- If `DCB` is enabled, the starter may create `DcbApplicationService`.
- If both capabilities are enabled, both application services may be created.

DCB application-service auto-configuration requires a user-provided `TagGenerator` bean, because tags are domain-specific. Occurrent does not infer DCB tags from CloudEvent payloads. The existing `occurrent.application-service.enabled` and `occurrent.application-service.enable-default-retry-strategy` properties apply to both stream and DCB application services.

Occurrent only creates missing indexes and collections. It never drops indexes or collections automatically.

Compatibility when changing capabilities:

| From | To | Compatible? | What Happens | Safe To Remove Indexes? |
|---|---|---:|---|---|
| `{STREAM}` | `{DCB}` | Yes, with caveat | DCB infrastructure is created and stream APIs become unsupported. Existing stream-written events remain stored as CloudEvents, but DCB reads will not return them unless they are backfilled with `dcbtags` and `dcbposition`. | After all apps run DCB-only and rollback to stream is not needed, the stream id/version index can be removed. Keep CloudEvent id/source. |
| `{STREAM}` | `{STREAM, DCB}` | Yes | DCB infrastructure is added. Existing stream reads still work. DCB reads return only DCB-tagged or backfilled events. Stream-written events without DCB metadata remain visible through stream APIs and normal CloudEvent queries and subscriptions. | No. Both stream and DCB indexes are used. |
| `{DCB}` | `{STREAM}` | Yes, with caveat | Stream index is added and DCB APIs become unsupported. DCB-written events are still normal CloudEvents with storage stream metadata, so they can be loaded through stream APIs by their storage stream ids, typically DCB partition streams. Their DCB metadata remains present but unused. | After all apps run stream-only and rollback to DCB is not needed, DCB position/tag indexes can be removed. Keep CloudEvent id/source and stream index. |
| `{DCB}` | `{STREAM, DCB}` | Yes | Stream index is added. DCB-written events remain readable through DCB APIs and also become readable through stream APIs by their storage stream ids. | No. Both stream and DCB indexes are used. |
| `{STREAM, DCB}` | `{STREAM}` | Yes, with caveat | DCB APIs become unsupported. DCB-written events remain readable through stream APIs by storage stream id, and stream-written events remain readable as before. DCB metadata and support collections remain but are not used. | After all apps run stream-only and rollback to DCB is not needed, DCB position/tag indexes can be removed. Keep CloudEvent id/source and stream index. |
| `{STREAM, DCB}` | `{DCB}` | Yes, with caveat | Stream APIs become unsupported. DCB reads still return DCB-written or backfilled events. Stream-only events remain stored as CloudEvents but are ignored by DCB reads unless backfilled with DCB metadata. | After all apps run DCB-only and rollback to stream is not needed, the stream id/version index can be removed. Keep CloudEvent id/source and DCB indexes. |

Operational rules:

- Index creation can block or degrade writes on large collections. For production collections with many events, create the required indexes out-of-band before enabling a new capability in application config. In MongoDB Atlas, prefer the platform's rolling index-management workflow where available.
- Index removal is an operator decision. It is safe only after all application instances have stopped using the removed capability and rollback is not required.
- DCB-written events remain normal CloudEvents with Occurrent stream metadata.
- Stream-written events are not automatically DCB-readable because DCB requires explicit tags and a DCB sequence position.

## Consequences

Positive:

- Stream-only users keep the smallest MongoDB setup by default.
- DCB remains opt-in and can be composed with stream support.
- Spring Boot applications get application-service beans that match their enabled event-store capabilities.
- Capability changes are explicit and documented.
- DCB-written events can later be read through stream APIs because they are still stored as normal CloudEvents with stream metadata.

Negative:

- Users must understand that enabling a capability may create indexes on startup.
- Historical stream events need an explicit backfill before they participate in DCB reads.
- DCB Spring Boot applications must provide a `TagGenerator` bean before the starter can create a `DcbApplicationService`.
- Index cleanup is manual and requires operational care.
- Runtime guards are needed because the class still implements both Java interfaces.
