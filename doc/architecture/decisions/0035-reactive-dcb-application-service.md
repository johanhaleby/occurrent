# 35. Reactive DCB application service

Date: 2026-06-30

## Status

Accepted

## Context

The reactive Spring store gained DCB support (ADR 34), but the reactive side had no application service for any use case. The blocking `DcbApplicationService` runs the read, decide, and append cycle and retries from a fresh read on a DCB conflict. Reactive callers (for example a WebFlux endpoint) had to drive that cycle by hand against the reactive event store. This is the first reactive application service in the repo.

## Decision

Add a reactive `DcbApplicationService` in a new module `application/service/reactor` (artifact `application-service-reactor`, package `org.occurrent.application.service.reactor.dcb`), mirroring the blocking service's behavior on top of the reactive event store.

### The domain function stays synchronous

`execute` returns `Mono<Optional<DcbAppendResult>>`, but the domain function is `Function<Stream<E>, Stream<E>>`, the same shape as the blocking service. Only the read and append I/O are reactive. A domain decision is a pure function, and the reactive read already materializes its matched events into a list, so a `Flux<E>` domain function would add reactive ceremony to pure logic with no streaming benefit and would not compose with the existing synchronous deciders. Keeping the function synchronous also makes migration from the blocking service a one-line return-type change.

### Read, decide, and append as one retried unit

The read, convert, decide, tag, and append run inside a single `Mono`, wrapped in `Retry.backoff` filtered on `DcbAppendConditionNotFulfilledException`. A retry re-subscribes the `Mono`, so each attempt reads fresh and decides against the current events, matching the blocking service. The default retry mirrors the blocking policy (five attempts, 100ms to 2s backoff).

### Reactive side-effect

`DcbExecuteOptions` carries a `Function<Stream<E>, Mono<Void>>` side-effect rather than the blocking `Consumer<Stream<E>>`, so a post-append side-effect can do non-blocking work. It is composed after the retry, so it runs once on a successful append with the newly produced events, and not on the no-new-events path. A side-effect with no I/O wraps in `Mono.fromRunnable`, which is the right tradeoff for allowing non-blocking side-effects.

### Reuse and duplication

The service reuses the existing `CloudEventConverter`, which is synchronous and shared. `TagGenerator` is a one-method functional interface duplicated into the reactive package rather than depending on `application-service-blocking`, which would point the reactive module at the blocking one.

## Consequences

- Reactive callers get the same read-decide-append-with-conflict-retry ergonomics the blocking side has, returning a `Mono` that composes into a reactive pipeline.
- The reactive DCB DSL and subscriptions are still missing. They are the remaining phases in `.context/reactive-dcb-plan.md` and are demand-gated.
- `TagGenerator` now exists in two packages. If a third consumer appears, hoisting it to a shared module is the time to do it, not now.
