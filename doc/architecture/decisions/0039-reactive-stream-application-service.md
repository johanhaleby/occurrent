# 39. Reactive stream application service

Date: 2026-06-30

## Status

Accepted

## Context

Occurrent had a blocking stream-based `ApplicationService` (`application-service-blocking`) from the start, but the only reactive application service was the DCB one added in ADR 35. A caller using the reactive Spring MongoDB event store for ordinary stream-per-aggregate work had no application service and had to hand-write the read, decide, write, and retry loop. The reactive event store API (`eventstore-api-reactor`) already exposes everything needed (`read` returning a `Mono<EventStream>`, conditional `write` returning a `Mono<WriteResult>`, and `ReadEventStreamWithFilter`), so the gap was only the service layer.

## Decision

Add a reactive stream `ApplicationService` in `application-service-reactor`, package `org.occurrent.application.service.reactor`, mirroring the blocking one with reactive return types.

- `ApplicationService<E>` returns `Mono<WriteResult>` from `execute(streamId, ExecuteOptions<E>, Function<Stream<E>, Stream<E>>)`, plus the same `String`/`UUID`/`ExecuteFilter` convenience overloads as the blocking interface. The already-deprecated `Consumer`-side-effect overloads are not carried over.
- `generic.GenericApplicationService<E>` runs read, decide, and write as one unit with `Mono.defer(...).retryWhen(...)`, retrying from a fresh read on a `WriteConditionNotFulfilledException` (default five attempts with exponential backoff, rethrowing the original failure when exhausted), exactly like the blocking service and the reactive DCB service. The domain function stays a synchronous `Function<Stream<E>, Stream<E>>`, and the post-write side-effect is reactive (`Function<Stream<E>, Mono<Void>>`), composed after the retry so it runs once on success.
- `ExecuteOptions<E>` carries a read filter (a raw `StreamReadFilter` or an `ExecuteFilter`) and the reactive side-effect. A reactive `PolicySideEffect` and Kotlin extension functions mirror the blocking ergonomics.

### ExecuteFilter moved to a shared module

`ExecuteFilter` resolves domain event classes to a `StreamReadFilter` and has nothing blocking about it, but it lived in `application-service-blocking`. It moved to the shared `application-service-common` module (package `org.occurrent.application.service`) so the blocking and reactive services use one copy. This is a breaking change for callers that imported `org.occurrent.application.service.blocking.ExecuteFilter`.

## Consequences

- Reactive callers get the same application-service ergonomics as blocking callers: read filtering, optimistic-concurrency retry, and a post-write side-effect, all as a `Mono`. This closes the application-service parity gap between the blocking and reactive stacks.
- `ExecuteFilter` now lives in `org.occurrent.application.service`. The old `org.occurrent.application.service.blocking.ExecuteFilter` import must be updated.
- `ExecuteOptions` and the `ApplicationService` interface stay separate per stack because their side-effect and return types differ (synchronous `Consumer`/`WriteResult` versus reactive `Function<…, Mono<Void>>`/`Mono<WriteResult>`), which Java cannot unify without an abstraction that costs more than the duplication.
