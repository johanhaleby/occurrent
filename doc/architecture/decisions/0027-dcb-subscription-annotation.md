# 27. Add the @DcbSubscription annotation

Date: 2026-06-27

## Status

Accepted

## Context

ADR 0026 renamed `@Subscription` to `@StreamSubscription` in anticipation of a declarative DCB annotation. This is that annotation.

Until now a DCB subscription was programmatic: inject the `DcbSubscriptions` DSL and call `subscribe(id, query, startAt, handler)` by hand, as the course-enrollment dashboard did with `DcbQuery.all()`, `DcbStartAt.beginning()`, and an in-handler type check. A stream subscription, by contrast, is a single annotated method. The catch-up ergonomics make the declarative form clearly worth having for DCB too: the annotation can express the query and the start position, and the processor can resolve the first-run-then-resume and always-replay behavior that is fiddly to write by hand.

The stream processor already resolves all of that for the stream path. Its start-position logic builds a dynamic `StartAt` that replays from the beginning on the first run and resumes from the stored position afterwards, and, for the always-replay case, returns null for the competing-consumer and durable models so an in-memory read model is not split across instances and does not persist a position. The same shape is needed for DCB, only over `dcbposition` instead of time.

## Decision

Add `@DcbSubscription`, the DCB analog of `@StreamSubscription`.

- Attributes: `id`, `eventTypes` (the domain event classes, resolved to CloudEvent types through the converter, exactly as `@StreamSubscription` does, defaulting to the method's event parameter), `tagsAllOf` (the DCB tag boundary), `startAt` (a `DcbStartPosition` of BEGINNING, NOW, or DEFAULT), `startAtDcbPosition` (an explicit position to start after, the DCB counterpart to the stream `startAtTimeEpochMillis`, mutually exclusive with a non-DEFAULT `startAt`), `resumeBehavior`, and `startupMode`. The processor builds the `DcbQuery` from the event types and tags: neither gives `all()`, types only gives `types(...)`, tags only gives `tagsAllOf(...)`, both give `typeAndTagsAllOf(...)`.
- The processor resolves the start position by mirroring the stream path over DCB positions. BEGINNING with the default resume replays from `dcbposition` 0 on the first run and resumes from the stored position afterwards. BEGINNING with `SAME_AS_START_AT` always replays, and disables the competing consumer and durable position storage for that subscription so an always-rebuilt in-memory read model sees every event and keeps no checkpoint. NOW is live only, DEFAULT resumes or goes live.
- To express the first-run-then-resume and always-replay starts, `DcbStartAt` gains a `dynamic` factory mirroring `StartAt.dynamic`, including the null-to-delegate contract. This is the typed DCB counterpart the processor needs.
- The DCB path routes through the `DcbSubscriptions` DSL, so it inherits the server-side filter and the in-process correctness floor. The method receives the domain event and, optionally, the generic `EventMetadata` or the DCB-specific `DcbEventMetadata`.
- The backend-agnostic processor helpers (the event and metadata parameter binding, the concrete-event-type resolution) are now extracted and shared between the stream and DCB paths. ADR 0026 deferred this until a second consumer existed, and now it does, so the seams are drawn against a real caller rather than guessed.
- The course-enrollment `CourseDashboardSubscriber` is migrated to `@DcbSubscription`, replacing the hand-rolled subscription and the in-handler type check.

## Consequences

- The annotation pair is complete and symmetric: `@StreamSubscription` for stream subscriptions, `@DcbSubscription` for DCB ones, sharing one processor.
- A DCB read model can be declared with a single annotated method and get history replay, resume, and the always-replay in-memory mode without hand-rolling the start position.
- `DcbStartAt` gains a `dynamic` factory, which also makes the typed DCB start positions as expressive as the stream ones for advanced callers.
- The example demonstrates the declarative DCB subscription end to end.
