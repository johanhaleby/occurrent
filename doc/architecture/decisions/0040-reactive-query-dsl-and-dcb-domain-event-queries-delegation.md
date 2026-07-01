# 40. Reactive query DSL and DCB domain event queries delegation

Date: 2026-07-01

## Status

Accepted

## Context

`dsl/query-dsl` had only a `blocking` module. Its `DomainEventQueries<T>` wraps a blocking `EventStoreQueries` and a `CloudEventConverter<T>` to give callers typed, converted queries (`query(Filter)`, `query(Class)`, `queryOne(...)`, `count()`, `exists()`, `all()`) instead of a raw `Stream<CloudEvent>`. The reactive event store API (`eventstore-api-reactor`) already exposes the same `EventStoreQueries` capability, returning `Flux`/`Mono` instead of `Stream`/plain values, and `ReactorMongoEventStore` already implements it. There was no reactive counterpart to the blocking `DomainEventQueries`, so a reactive caller doing ordinary filtered or typed queries had to hand-write the CloudEvent-to-domain-event mapping.

The gap showed up concretely in `dcb-dsl-reactor`. The blocking `DcbDomainEventQueries` wraps a blocking `DomainEventQueries<E>`: it owns the DCB query family (`query(DcbQuery)`, `queryWithPosition(...)`) and delegates every plain stream-oriented query method to the wrapped `DomainEventQueries`, so one object serves both query styles. The reactive `DcbDomainEventQueries` could not do the same, because there was no reactive `DomainEventQueries` to wrap. Its Javadoc said so directly: "The reactive side has no general `DomainEventQueries` to wrap, so the stream-oriented query delegation of the blocking version is intentionally absent." It only exposed the DCB query family, built directly on the reactive `DcbEventStore` and a `CloudEventConverter`.

## Decision

Add `query-dsl-reactor`, a reactive `DomainEventQueries<T>` in package `org.occurrent.dsl.query.reactor`, mirroring the blocking one method-for-method:

- Wraps a reactive `EventStoreQueries` and a `CloudEventConverter<T>`.
- Query methods return `Flux<E>` (the idiomatic reactive shape for a lazily-consumed collection).
- `queryOne(...)` returns `Mono<E>`, empty when nothing matches, instead of a nullable value.
- `count()` and `exists(Filter)` return `Mono<Long>`/`Mono<Boolean>`, matching the reactive `EventStoreQueries` signatures.
- Exposes `eventStoreQueries()` so capabilities layered on top of the event store, such as DCB, can check whether the underlying store also implements the reactive `DcbEventStore`.
- Exposes `toDomainEvents(Flux<CloudEvent>)` so callers that already have a `Flux<CloudEvent>` from elsewhere can convert it without reaching into the `CloudEventConverter` directly.
- A Kotlin extensions file mirrors the blocking one's `KClass`-based overloads for `query`/`queryOne`. It replaces the blocking file's `queryForSequence` family with nothing, since `Flux` is already the idiomatic reactive return type in Kotlin, and keeps a `queryForList` convenience that collects a `Flux` into `Mono<List<T>>` for callers that want the whole result as one value.

`DcbDomainEventQueries` in `dcb-dsl-reactor` now wraps a `DomainEventQueries<E>` instead of a raw `DcbEventStore` and `CloudEventConverter<E>`, exactly like the blocking version:

```java
public DcbDomainEventQueries(DomainEventQueries<E> domainEventQueries) {
    this.domainEventQueries = requireNonNull(domainEventQueries, ...);
    this.dcbEventStore = requireDcbEventStore(domainEventQueries);
}
```

The constructor validates that the wrapped `DomainEventQueries`'s `eventStoreQueries()` also implements the reactive `DcbEventStore`, the same check the blocking version performs, and throws `IllegalArgumentException` otherwise. Every plain stream-query method delegates to the wrapped `DomainEventQueries`; the DCB query family converts CloudEvents by wrapping the DCB read's materialized event list in a `Flux` and passing it through `domainEventQueries.toDomainEvents(...)`, instead of calling the `CloudEventConverter` directly.

This is a breaking change to `DcbDomainEventQueries`'s constructor. Nothing outside `dcb-dsl-reactor` constructed it directly (no example app uses the reactive DCB DSL's query type yet), so the blast radius is the class itself, its Kotlin extensions, and its own test.

## Consequences

- Reactive callers doing ordinary filtered or typed queries get the same ergonomics blocking callers already had: typed results, `Flux`/`Mono` instead of raw CloudEvents, and Kotlin `KClass` overloads.
- `DcbDomainEventQueries` in `dcb-dsl-reactor` reaches full structural parity with the blocking version: both wrap a `DomainEventQueries<E>`, both derive DCB capability via an `instanceof` check on the wrapped store, both delegate the same method set.
- `new DcbDomainEventQueries(DcbEventStore, CloudEventConverter)` no longer compiles. Callers must build a `DomainEventQueries<E>` first and pass that instead.
- The reactive `DomainEventQueries` and the blocking one stay separate classes rather than a shared generic abstraction, because their return types differ throughout (`Flux`/`Mono` versus `Stream`/plain values) and the method-for-method mirroring costs less than a unifying abstraction would.
