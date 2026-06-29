# 33. Native MongoDB driver DCB parity via a shared marker model

Date: 2026-06-29

## Status

Accepted

## Context

DCB shipped for the in-memory store and the Spring blocking MongoDB store. The native MongoDB driver store (`MongoEventStore`) was still stream-only, so an application that uses the plain synchronous driver without Spring could not use DCB at all.

The DCB write path is subtle. Its correctness rests on a per-attribute marker model and a storage contract that ADR 21 and ADR 31 describe: positions reserved outside the transaction, one version marker per tag and per type (never combined), a consistency token that sums those versions, and a single-read snapshot of the markers. Two MongoDB stores that write the same collections must agree on every part of that contract, or optimistic concurrency breaks silently.

## Decision

Add DCB to the native driver `MongoEventStore` so it reaches behavioral parity with the Spring blocking store, and share the parts that must not differ between the two drivers rather than copying them.

### Share the marker model and storage contract

Extract the driver-agnostic DCB code into a new `eventstore-mongodb-dcb-common` module:

- `DcbMarkerModel` owns the marker-key derivation, the checkpoint and position field constants, the support-collection naming, and DCB event validation.
- `DcbDocumentMapper` owns the DCB storage contract, the `dcbTags` index array and the `dcbposition` field, and the DCB-aware document-to-CloudEvent conversion. The stream-only common mapper no longer knows about DCB.

Both Mongo stores depend on this module, so the on-disk contract has a single source of truth. The same module backs the DCB subscription code, which also reads the `dcbTags` field and deserializes DCB events.

### Reimplement the orchestration per driver

The read and append orchestration stays per store, because it is tied to each driver's API. The native store uses `ClientSession.withTransaction`, the synchronous driver's `Bson` filters, and `findOneAndUpdate` for the position counter, where the Spring store uses `MongoTemplate`, `Criteria`, and `TransactionTemplate`. The native store mirrors the Spring semantics: positions reserved outside the transaction, marker versions bumped inside it, the token-sum conflict check for a token-carrying condition and a live-existence check otherwise, and the same partition placement by boundary tags. A shared abstraction over these two driver APIs would be more complex than the two concrete versions, so the orchestration is not shared.

The one driver difference worth noting is retry. The synchronous driver's `withTransaction` already retries a `TransientTransactionError` and re-runs the body, which re-evaluates the condition. The Spring `TransactionTemplate` does not, which is why the Spring store wraps the transaction in an explicit retry. The native store keeps a thin retry too, mainly for the cold-start duplicate-key race on a marker or the position counter, which is not labeled transient and so is not retried by `withTransaction`.

### Share the capability vocabulary

The event-store capability set (`STREAM`, `DCB`) moved into a shared `EventStoreCapability` enum in `eventstore-api-common`, replacing the Spring-specific enum, because a capability is neither Spring nor Mongo specific. The native `EventStoreConfig` gains the same capability set and `DcbStreamIdGenerator` the Spring config already had, defaulting to stream-only so existing native applications are untouched.

## Consequences

- The native driver store supports DCB with the same guarantees and the same token semantics as the Spring store. Tokens stay opaque and are still not comparable across stores.
- The marker model and storage contract cannot drift between the two Mongo stores, because they share one copy.
- The native store now creates the DCB support collections and indexes when the DCB capability is enabled, and gates each method by capability.
- The high-cardinality marker growth noted in ADR 21 applies to the native store as well, since it shares the same marker model. Nothing reclaims markers automatically.
