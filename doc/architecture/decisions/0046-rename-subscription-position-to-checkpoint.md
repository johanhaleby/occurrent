# 46. Rename SubscriptionPosition to Checkpoint

Date: 2026-07-04

## Status

Accepted

## Context

A subscription needs to remember where it left off so it can resume after a restart without replaying or skipping
events. Occurrent modelled that resume marker with the `SubscriptionPosition` type family: the `SubscriptionPosition`
interface, its implementations (`GlobalSubscriptionPosition`, `StringBasedSubscriptionPosition`,
`TimeBasedSubscriptionPosition`, `MongoResumeTokenSubscriptionPosition`, `MongoOperationTimeSubscriptionPosition`),
the `SubscriptionPositionStorage` capability that reads and writes it, the catch-up `SubscriptionPositionStorageConfig`,
the `PositionAwareSubscriptionModel.globalSubscriptionPosition()` accessor, and the Spring Boot
`occurrentSubscriptionPositionStorage` beans.

Two forces made the name worth revisiting.

First, the wider event-sourcing vocabulary draws a line that Occurrent's naming blurred. A *position* (or offset) is
the value that locates an event on an ordering axis. A *checkpoint* is the durable record a consumer writes to
remember the last position it processed, so it can resume. EventStoreDB, SQLStreamStore, and common projection
practice all use "checkpoint" for the persisted resume marker and "position"/"offset" for the value it holds. What
`SubscriptionPositionStorage` stores is, in that vocabulary, a checkpoint; the value inside it is a position.

Second, [ADR 45](0045-unified-global-position.md) had just unified the event ordering axis on the single word
`position`: the CloudEvent extension, the stored field, the read bounds (`PositionRange`, `PositionOrderedReader`,
`currentPosition`), and the reservation calls all say `position`, so a reader never has to learn that "DCB position"
and "stream position" are the same value. That left `GlobalSubscriptionPosition` (itself renamed from
`DcbSubscriptionPosition` in ADR 45) and `position` both using the word "position" for two different things: the
subscription's resume marker and the ordering value the marker carries. The overlap is exactly the position/checkpoint
distinction above, left unnamed.

Occurrent is pre-release (`0.x`) and DCB is unreleased, so the public API can change without a deprecation shim. The
one hard constraint is data already written by existing deployments: the MongoDB position-storage adapters persist a
generic document field named `subscriptionPosition`, and an in-place upgrade must not lose a stored resume marker.

## Decision

Rename the whole `SubscriptionPosition` family to `Checkpoint`, and keep `position` for the ordering axis. The
subscription's persisted resume marker is a **checkpoint**; the value it carries is a **position**. Concretely:
`SubscriptionPosition` -> `Checkpoint`, `SubscriptionPositionStorage` -> `CheckpointStorage`, `GlobalSubscriptionPosition`
-> `GlobalCheckpoint`, `TimeBasedSubscriptionPosition` -> `TimeBasedCheckpoint`, `StringBasedSubscriptionPosition` ->
`StringBasedCheckpoint`, the two Mongo marker types and the storage adapters likewise, `SubscriptionPositionStorageConfig`
-> `CheckpointStorageConfig` with its records and factory methods, `PositionAwareSubscriptionModel` ->
`CheckpointAwareSubscriptionModel` with `globalCheckpoint()`, `PositionAwareCloudEvent` -> `CheckpointAwareCloudEvent`,
`StartAt.subscriptionPosition(...)` -> `StartAt.checkpoint(...)`, and the Spring beans to `occurrentCheckpointStorage`.
The full symbol-by-symbol mapping is recorded in `changelog.md`.

This sharpens ADR 45 rather than competing with it: the ordering value stays `position` everywhere ADR 45 put it, and
the resume marker that wraps such a value becomes a `checkpoint`. `GlobalCheckpoint` is a checkpoint backed by a global
`position`; the two words no longer collide on one concept.

**Hard break, no deprecated aliases.** The rename is a source- and binary-incompatible public API change. Given the
`0.x` status and that the affected consumers inside this repository (DSLs, starters, examples) are updated in the same
change, a clean break is preferred over carrying deprecated shims that would themselves have to be removed later.

**"Position" identifiers outside this family are untouched.** Everything ADR 45 named `position` (the CloudEvent
`position` extension, the stored `position` field, `PositionRange`, `PositionOrderedReader`, `currentPosition`,
`reservePositions`, `writesPosition`, `streamPosition`, the `position-backfill` module) is the ordering axis, not the
subscription marker, and keeps its name.

**Backward-compatible database migration.** The MongoDB generic checkpoint field is renamed from `subscriptionPosition`
to `checkpoint`. On read, a storage adapter checks the new `checkpoint` key and falls back to the legacy
`subscriptionPosition` key, so a marker written by an older version is still understood once. On the next save the new
`checkpoint` key is written and the legacy field is removed. All three MongoDB adapters persist by replacing the whole
document (the native adapter via `replaceOne`, and the Spring blocking and reactor adapters because an operator-less
`Update.fromDocument(...)` is applied by Spring Data as a full-document replacement), so any field absent from the new
document, including the legacy one, does not survive. The `resumeToken` and `operationTime` fields, the `_id`, the serialized `asString()` form of any
marker, and user-supplied collection names are unchanged. Redis stores the raw marker string under the caller's own
subscription id, so it has nothing to migrate. This backward-compatible round trip is covered by explicit tests for all
three MongoDB adapters, as a required part of this change.

## Consequences

The vocabulary now matches the wider event-sourcing world and internally separates the resume marker (`checkpoint`)
from the ordering value it carries (`position`), removing the ambiguity ADR 45 left. `GlobalSubscriptionPosition` is
renamed a second time within days of ADR 45; doing it before release, in one deliberate pass, is the point.

The word "checkpoint" is now used for three unrelated mechanisms: the subscription resume marker introduced here, the
DCB "consistency checkpoint" append-condition keys ([ADR 7](0007-unify-stream-and-dcb-concurrency-using-global-per-event-position-and-head-document-gating.md)),
and the `PositionBackfillCheckpoint` progress marker in the backfill migration tool. They live in different modules and
contexts; where they could be read together, the surrounding javadoc names which one is meant. This overload is the
accepted cost of aligning the subscription marker with the canonical term.

Every downstream consumer that referenced the old types must update at the next upgrade. Existing stored MongoDB
checkpoints are read transparently and rewritten under the new field name on the first save after upgrade, with no
operator action; the legacy field is not left behind.
