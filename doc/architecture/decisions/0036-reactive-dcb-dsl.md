# 36. Reactive DCB DSL

Date: 2026-06-30

## Status

Accepted

## Context

The blocking DCB DSL (`dcb-dsl-blocking`) gives application code a query helper (`DcbDomainEventQueries`) and Kotlin decider extensions over the blocking DCB application service. The reactive side had neither. This adds the DCB-specific reactive DSL on top of the reactive event store (ADR 34) and the reactive application service (ADR 35).

## Decision

Add a `dsl/dcb-dsl/reactor` module (artifact `dcb-dsl-reactor`, package `org.occurrent.dsl.dcb.reactor`) with the reactive query helper and the reactive decider extensions.

### Query helper built directly on the reactive event store

The blocking `DcbDomainEventQueries` wraps a `DomainEventQueries` (the general blocking query DSL) and delegates ordinary stream queries to it. There is no reactive `DomainEventQueries`, and building one is a non-DCB concern. So the reactive `DcbDomainEventQueries` is built directly on the reactive `DcbEventStore` plus a `CloudEventConverter`, and provides only the DCB query methods: `Flux<E> query(DcbQuery)` and `Mono<DcbDomainEventStream<E>> queryWithPosition(DcbQuery)`. It does not offer the general stream-query delegation the blocking version has, because that part is exactly what would need a reactive `DomainEventQueries`. `DcbDomainEventStream` is a small result record duplicated into the reactive package rather than depending on the blocking DSL.

### Decider extensions return Mono, with a non-null state constraint

The Kotlin decider extensions mirror the blocking ones. The reactive application service runs the domain decision synchronously inside a `Function<Stream<E>, Stream<E>>`, so the decider runs synchronously the same way, and only the result is reactive. `execute` returns `Mono<DcbAppendResult>` (empty on a no-op command), `executeAndReturnDecision` returns `Mono<Decider.Decision<S, E>>`, and `executeAndReturnEvents` returns `Mono<List<E>>`.

`executeAndReturnState` returns `Mono<S>` and therefore binds `S` to a non-null type, because a `Mono` cannot carry a null value. A decider whose folded state can be null (the common "does not exist yet" initial state) cannot use `executeAndReturnState` reactively. That is inherent to `Mono`, not a shortcut. Such a decider uses `executeAndReturnDecision` and reads its state, or `executeAndReturnEvents`, both of which allow a null state. A null-friendly `Mono<Optional<S>>` variant can be added later if it is wanted.

### Out of scope

The blocking DSL also has a live `subscribeDcb` helper and DCB subscription metadata. Those belong with reactive DCB subscriptions and are deferred to the next phase, so this module has no subscription surface.

## Consequences

- Reactive callers can run DCB queries that return domain events as a `Flux`, capture the consistency token for a conditional append, and run deciders through the reactive application service, all returning composable `Mono`/`Flux`.
- `DcbDomainEventStream` now exists in two packages. As with the duplicated `TagGenerator`, a shared home is the right move only once a third consumer appears.
- The reactive DCB subscriptions and the live `subscribeDcb` helper remain, tracked in `.context/reactive-dcb-plan.md` as the last phase.
