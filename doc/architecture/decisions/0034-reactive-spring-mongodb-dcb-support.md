# 34. Reactive Spring MongoDB DCB support

Date: 2026-06-30

## Status

Accepted

## Context

DCB shipped for the in-memory, Spring blocking, and native driver event stores. The reactive Spring store (`ReactorMongoEventStore`) was the last event store without it. The blocking DCB API is synchronous, so a reactive store needs a `Mono`/`Flux` variant of the `DcbEventStore` interface. The reactive store already does multi-document transactions with Spring's `TransactionalOperator` and can reuse the shared marker model, so the work is mostly translating the blocking append cycle to Reactor.

## Decision

Add reactive DCB to `ReactorMongoEventStore`, reaching event-store parity with the blocking and native stores.

### Reactive DcbEventStore interface in its own module

A new module `eventstore-api-dcb-reactor` holds `org.occurrent.eventstore.api.dcb.reactor.DcbEventStore`, a `Mono`/`Flux` mirror of the blocking interface. It depends on `eventstore-api-dcb` for the shared value types and on `reactor-core`. Keeping it in its own module keeps project-reactor off the classpath of blocking-only consumers of `eventstore-api-dcb`, mirroring the existing `api/blocking` and `api/reactor` split.

### Reuse the shared marker model and storage contract

`ReactorMongoEventStore` reuses `DcbMarkerModel` and `DcbDocumentMapper` from `eventstore-mongodb-dcb-common` unchanged, so the on-disk contract and the per-attribute marker scheme are identical to the blocking and native stores. `EventStoreConfig` gains the same capability set and `DcbStreamIdGenerator`, defaulting to stream-only.

### Reactive append mirrors the blocking semantics

Positions are reserved outside the transaction. Marker increments, condition enforcement, and inserts run inside `transactionalOperator.transactional(...)`. The conditional check recomputes the consistency token within the transaction, and the in-transaction marker read participates in the reactive transaction snapshot through Reactor context propagation, confirmed by the token-conflict tests. The one reactive difference is retry. The reactive transaction does not auto-retry a transient conflict, so the transactional `Mono` is wrapped in `Retry.backoff` filtered on transient-transaction and cold-start duplicate-key errors. A `DcbAppendConditionNotFulfilledException` and a `DuplicateCloudEventException` are not retried.

## Consequences

- All four event stores now support DCB with the same token semantics and the same storage contract.
- The reactive side still has no DCB application service, DSL, or subscriptions. Those are tracked as later phases in `.context/reactive-dcb-plan.md` and are demand-gated.
- The `streamId` plus `streamVersion` unique index stays stream-mode only here too, for the same reason as the other stores. DCB correctness rests on the markers and the unique `dcbposition`.
