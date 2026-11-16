# 180. Occurrent owns the stored form of a saga instance

Date: 2026-11-16

## Status

Accepted, for [#1259](https://github.com/johanhaleby/occurrent/issues/1259). Supersedes two clauses of
[ADR 70](0070-saga-instance-observation.md) by reference, since ADR 70 shipped in 0.34.0. One says `SagaEnvelope`
"remains the `SagaStateStore` type", and the other says `SagaEnvelope` "cannot move" because the SPI both returns and
accepts it. ADR 70 gets a pointer in its Status section.

Amends [ADR 154](0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md) in place, since it hasn't
shipped, and adds an amendment note to [ADR 63](0063-saga-dsl.md).

## Context

Before this decision, each `SagaStateStore` turned a `SagaEnvelope` into its own stored form and back, one field at a
time. `SpringMongoSagaStateStore` wrote every envelope component and every `SagaFailure` component as a field of the
instance's document. It wrote a flow saga's `FlowStateImpl` in an `instanceof FlowStateImpl` branch of its own.

So every bookkeeping field the executor gains has to be added to every store. 0.35.0 adds four of them,
`SagaEnvelope.streamMarks`, `SagaFailure.releasedAt`, `SagaFailure.quarantineId` and
`SagaEnvelope.quarantineSkipCount`.

A store of your own that doesn't save one of them still compiles and still runs. An instance it quarantines without a
quarantine id or a skip count records none of the inputs it skips, so a later release has nothing to replay. The
defaulting constructors let a store do that without noticing. A store that built its envelope with the 13-argument
`SagaEnvelope` constructor got empty stream marks and no skip count without a compile error.

[#1538](https://github.com/johanhaleby/occurrent/pull/1538) answered this by checking after the write. The executor
read the instance back after the write that quarantines it and after the write that starts a release:

- When the quarantine id or the skip count was missing after a quarantine, it wrote the instance back as it was before
  the quarantine, failed the input and logged an `ERROR`.
- When something else wrote the instance before each attempt of that undo, the instance stayed quarantined without
  them, and the executor logged a second `ERROR` that said so.
- When `releasedAt`, the quarantine id or the skip count was missing after a release started, the release threw.

So whether quarantine and release worked depended on every store saving three fields, and `SagaExecution` needed a
read and an undo to check that it had.

## Decision

Occurrent owns the stored form of a saga instance. `SagaCodec`, in `saga-dsl-common`, encodes a `SagaEnvelope` into a
`StoredSaga` and decodes a `StoredSaga` into a `SagaEnvelope`. A store persists `StoredSaga` values and never sees an
envelope.

### What a store persists

A `StoredSaga` has four components, and a store persists all four and nothing else:

| Component | What it is |
| --- | --- |
| `sagaId` | The instance id. |
| `version` | The version the compare-and-set compares. |
| `encoded` | An `EncodedSaga`, the fields by name in the order a store should write them. |
| `index` | A `SagaIndex` with what a store queries on. |

`SagaIndex` has `storedStatus`, `nextTimerDueAtEpochMilli`, `updatedAtEpochMilli`, `releasing` and `quarantineId`.
`SagaCodec.indexOf(EncodedSaga)` derives each of them from the encoded fields, so a store never computes one itself.

A value in `EncodedSaga` is a `String`, an `Integer`, a `Long`, a `Boolean`, an `Instant`, a `CloudEvent`, or a `List`
or `Map` of those. The `state` field has whatever the store's `SagaStateCodec` returned. `EncodedSaga` copies every
`List` and `Map` in the other fields, at any depth, into copies that cannot be changed, so a caller holding a stored
`EncodedSaga` cannot change the instance outside a compare-and-set. A `SagaStateCodec` must not encode a state as
`null`, because a stored `null` reads back as an instance with no state, so `SagaCodec` throws before the save instead.

### The user state

A store supplies a `SagaStateCodec<S>` from `stateCodec()`, because how a user state is represented depends on the
store. `SpringMongoSagaStateStore` converts it with the application's `MongoConverter`, as before.
`SagaStateStore.inMemory()` keeps the object as it is, with `SagaStateCodec.identity()`.

The codec encodes a flow saga's `FlowStateImpl` itself, field by field, and never hands it to the `SagaStateCodec`. The
received events become `CloudEvent` values, made by the `CloudEventConverter` the runner was given.
`SpringMongoSagaStateStore` writes each of them as a CloudEvent JSON string with `cloudevents-json-jackson` and reads
it back, which is what it stored before. A flow saga whose state is a `FlowState` other than `FlowStateImpl` gets an
`IllegalArgumentException` on its first save.

### The SPI

`SagaStateStore` and `SagaStateStoreQueries` change as a break before 1.0:

| Member | 0.34.0 | 0.35.0 |
| --- | --- | --- |
| `stateCodec()` | absent | `SagaStateCodec<S> stateCodec()` |
| `find` | `Optional<SagaEnvelope<S>> find(String sagaId)` | `Optional<StoredSaga> find(String sagaId)` |
| `findWithoutState` | a default that calls `find` | `Optional<StoredSaga> findWithoutState(String sagaId)`, no default |
| `compareAndSave` | `boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion)` | `boolean compareAndSave(StoredSaga saga, long expectedVersion)` |
| `compareAndSaveWithoutState` | a default that calls `compareAndSave` | `boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion)`, no default |
| `findWithDueTimers` | `List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit)` | `List<StoredSaga> findWithDueTimers(Instant now, int limit)` |
| `SagaStateStoreQueries.findByStatus` | `List<SagaEnvelope<S>> findByStatus(...)` | `List<StoredSaga> findByStatus(...)` |

`delete(String sagaId)` doesn't change.

`SagaCodec` also names the fields each method needs:

- `findWithoutState` returns at least the fields in `fieldsWithoutState()`.
- `findWithDueTimers` and `findByStatus` return at least the fields in `instanceFields()`.
- `compareAndSaveWithoutState` sets the fields in `fieldsExceptState()`, and removes each one the instance has no
  value for.

`storedStatuses(SagaStatus)` gives the stored strings that a query for a status matches.

The stored statuses `QUARANTINED_RECORDING` and `ACTIVE_MARKED` are constants on `SagaCodec`.
`SpringMongoSagaStateStore` keeps its two constants of the same names, set to the codec's.

### The MongoDB document stays the same

The field names, their order and their value types are the ones `SpringMongoSagaStateStore` wrote before this
decision, so stored data needs no migration. `SagaDocumentLayoutTest` checks this against 8 committed documents. 7 of
them were written by the store before the codec took over the mapping, and 1 was written by hand the way 0.34.0 writes
it. Each document read through the store and the codec gives the expected envelope. That envelope written through the
codec and the store gives the same document again, compared as canonical extended JSON.

The codec reads data 0.34.0 wrote. A field missing from it reads as the value that was true for that data. A document
without `started` reads as started, and one without `streamMarks` reads as an instance with no stream marks.

### What goes

- The read-back after the write that quarantines an instance, the undo of a quarantine whose id didn't persist, and its
  two `ERROR` logs.
- The read-back after the write that starts a release, and the `IllegalStateException` it threw.
- The `SagaEnvelope` constructors with 14, 13 and 11 arguments, and the `SagaFailure` constructors with 6 and 5
  arguments. Each filled the components it didn't take with a default. `SagaEnvelope` and `SagaFailure` keep only
  their canonical constructors.

### Precedents

In each of these three frameworks, the framework decides what an instance is stored as:

- Axon Framework hands its `SagaStore` the saga and its association values. `JpaSagaStore` serializes the saga with
  the `Serializer` it is configured with, and keeps each association value as an `AssociationValueEntry` row of its
  own, which is what a lookup by association value reads.
- NServiceBus SQL Persistence serializes saga data to JSON with Newtonsoft.Json into one column, and copies the
  correlation property into an indexed column of its own. The persister finds a saga by that column and never queries
  the JSON.
- Spring Statemachine's `StateMachinePersist` writes and reads a `StateMachineContext`, a type the framework defines.
  Its repository persistence for JPA, MongoDB and Redis serializes that context with Kryo.

Axon and NServiceBus also keep the values a lookup needs next to the serialized form, the way `SagaIndex` does here.

## Consequences

- A `SagaStateStore` of your own stops compiling. The upgrade guide to 0.35.0 shows how to move it to `StoredSaga`.
- A store that writes the `EncodedSaga` fields as it gets them, as `SpringMongoSagaStateStore` does, needs no change
  when Occurrent adds a field. A store that maps each field to a column of its own still has to add one.
- The codec can't see what a store does with the `EncodedSaga`. A store that doesn't write a field still loses it, but it
  has to skip it on purpose, since the store no longer names any field to write.
- A call to one of the removed constructors stops compiling. The 13-argument `SagaEnvelope` constructor and the
  5-argument `SagaFailure` constructor were the canonical ones in 0.34.0, so a call written against 0.34.0 breaks too.
  The upgrade guide to 0.35.0 shows the call to the canonical constructor.
- `SagaStateStore.inMemory()` goes through the codec on every save and every read. A flow saga on it turns every
  received event into a `CloudEvent` on every save, which it didn't do before. I haven't measured the cost. Every test
  that runs a saga on the in-memory store now goes through the codec too, so a field the codec drops fails those tests.
- The warning once a flow saga instance retains 1000 events moved from `SpringMongoSagaStateStore` into the codec, so
  every store gets it.
- `SagaEnvelope` is no longer the `SagaStateStore` type, so the reason ADR 70 gave for why it can't move no longer
  applies. `SagaCodec`'s public methods still take and return it, and this decision doesn't move it.
- No store reads or builds `FlowStateImpl` any more, so ADR 63's reason for making it `public` no longer applies.
  It stays `public`, since `SagaCodec` and `FlowSagaImpl`, each in another package of `saga-dsl-common`, still use
  it. Making it non-public is left for a later change.
- `SagaExecution` no longer reads an instance back after a quarantine or after the start of a release, so each of
  them takes one store read fewer.

## Rejected alternatives

- **Each store maps the fields, and the executor reads back what it wrote**, as #1538 did. Every store implements the
  mapping again, the executor finds a dropped field only after the write, and the undo can itself lose to another
  writer on every attempt.
- **One opaque value per instance**, a `byte[]` or a blob. The MongoDB document would change, so stored data would need
  a migration. A store could no longer query the status, the next timer or `updatedAt`, which `findWithDueTimers`,
  `findByStatus` and `findReleasingSagaIds` need, and neither could the hand-written queries in the runbook.
- **Keep `SagaEnvelope` as the SPI type and offer a helper that a store may call.** A store of your own doesn't have to
  call it, so it can still drop a field without a compile error.
