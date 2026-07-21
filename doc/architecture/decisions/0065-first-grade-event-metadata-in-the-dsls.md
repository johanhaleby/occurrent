# 65. First-grade event metadata in the saga and projection/view DSLs

Date: 2026-07-21

## Status

Accepted. Additive across the saga DSL (ADR 63), the view/projection DSLs, and the Spring Boot starters.

## Context

An event carries more than its domain payload. Occurrent stores each event as a CloudEvent with extensions for the
stream id, the stream version, the global sequence position, and, on the DCB path, the event's tags. This metadata is
available at the subscription boundary and is already surfaced to plain subscribers: the subscription DSL delivers an
`EventMetadata` alongside the event (`(EventMetadata, E)`), and the DCB DSL wraps it as `DcbEventMetadata` for a
Java-friendly view of the position and tags.

The saga and projection/view DSLs did not surface any of it. Both convert the CloudEvent to the domain event `E` with
`CloudEventConverter.toDomainEvent` and hand only `E` to user code, so a saga reaction and a projection fold could not
see the stream id, the version, or the position of the event they were processing. This blocks real needs: keying a
projection's view instance by stream id (one view per aggregate), a saga correlating or making a decision by stream id
or position, and, the motivating case, an asynchronous projection recording the global position it has applied so a
client can later read the projection only once it has caught up to a given position.

The metadata type to expose already exists. `EventMetadata` (in `occurrent-subscription-dsl-common`) is a bag built
once from a CloudEvent's extensions, with typed accessors for stream id, version, and position and a generic reader for
any other extension. `DcbEventMetadata` layers the DCB position and tags on top of it without the generic type
depending on the DCB API. The question was therefore not what type to introduce, but how to route the existing type
into two fold surfaces that were designed event-only, without complicating the common case.

## Decision

**Event metadata is exposed through additive, metadata-first overloads that reuse `EventMetadata`.** Every existing
event-only method keeps its signature and behavior, and gains a metadata-carrying sibling. The event-only form
delegates to the metadata-aware one (or the reverse), so existing user code compiles and behaves exactly as before, and
a caller opts into metadata only where it needs it. This mirrors the convention the subscription DSL and the
`View.updateView` extension already use, rather than inventing a second style.

**`EventMetadata` is the single fold currency, for both the stream and the DCB paths.** A DCB projection or saga that
wants the position or tags wraps the delivered `EventMetadata` with `DcbEventMetadata.from(...)` (or reads the Kotlin
`EventMetadata.dcbTags` extension), the same way the DCB DSL already layers on the generic type. There is no separate
DCB fold surface. This keeps one metadata channel through the DSLs and avoids a parallel set of DCB-typed overloads.

**Metadata is present only where an event arrives as a CloudEvent.** The live and catch-up subscription paths carry the
CloudEvent, so the runner builds `EventMetadata.from(cloudEvent)` and threads it into the fold. The on-demand
query-replay path (folding a view from a query result, or a live domain-event feed that never saw a CloudEvent) has no
metadata to give, so it folds with `EventMetadata.empty()`: `position` is null and the stream accessors have nothing to
return. This is a documented property of those paths, not a gap to paper over, because the metadata genuinely does not
exist when replaying bare domain events.

**Sagas receive metadata at reaction time only; they do not persist it per event.** A saga's `evolve`, `react`, and
`onStart`, and a flow saga's triggering-event reaction, can read the current event's metadata. A plain `Saga<E, S, C>`
that needs to remember a slice of it (a position, a stream id) stores that slice in its own state `S`. The flow saga's
received-event log stays a log of domain events, so its persisted `FlowState` schema is unchanged and a later flow step
cannot read an earlier event's metadata. Persisting metadata for every received event was rejected as speculative: it
would enlarge the persisted schema and the `ReceivedEvents` surface for a need that a plain saga already covers through
its own state. Because flow sagas are new this release, this can be revisited before a later release turns the persisted
format into a compatibility constraint.

**Version-gated reads are out of scope and deferred.** Persisting each projection's applied global position atomically
with its view state, and a read API that returns the current applied position or blocks until the projection has caught
up to a given position, is a separate and larger capability. It builds on this metadata primitive (the fold must see
the position to record it) but adds a storage and read-path design of its own. It is tracked as a follow-up rather than
bundled here.

## Consequences

- Saga reactions and projection/view folds can read the stream id, version, position, and any CloudEvent extension of
  the event they process, and a projection can key its view instance by metadata such as the stream id.
- No existing signature changes and no user code has to be touched, so the change is fully additive and needs no
  migration or OpenRewrite recipe.
- Folding on the query-replay path yields empty metadata. Code that reads `position` or the stream accessors there gets
  null or an error by construction, which is correct: those events were never delivered as CloudEvents.
- A plain saga persists only the metadata slice it puts in its own state. Flow sagas remain event-only in their
  persisted log, so cross-step metadata access is not available and would be a future, format-affecting change.
- Because the saga DSL and the projection DSL are new this release, their metadata support is described as part of those
  features rather than as a change to them; the view DSL predates this release, so its new metadata overloads are a
  genuine addition to an existing API.
- Version-gated projection reads remain unbuilt. The metadata primitive is the prerequisite that makes them
  implementable next.
