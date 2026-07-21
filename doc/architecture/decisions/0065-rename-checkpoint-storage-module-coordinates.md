# 65. Rename the checkpoint-storage module coordinates

Date: 2026-07-21

## Status

Accepted

## Context

[ADR 46](0046-rename-subscription-position-to-checkpoint.md) renamed the subscription resume-marker type family from `SubscriptionPosition` to `Checkpoint`. That rename reached the `SubscriptionPositionStorage` capability and every adapter class, so the classes are now `NativeMongoCheckpointStorage`, `SpringMongoCheckpointStorage`, `ReactorCheckpointStorage`, and `SpringRedisCheckpointStorage`. It did not reach the four Maven modules that ship those adapters, whose coordinates stayed `occurrent-subscription-*-position-storage`. A module named `position-storage` whose only public class is a `CheckpointStorage` now contradicts itself.

ADR 46's "position stays" carve-out is scoped to the event ordering axis from [ADR 45](0045-unified-global-position.md): `PositionRange`, `currentPosition`, the `position-backfill` module, and the CloudEvent `position` extension. These four modules persist a checkpoint, not a position, so leaving them named `position-storage` is an incomplete application of ADR 46, not an intended exception to it.

[ADR 55](0055-uniform-occurrent-artifact-coordinate-naming.md) ran a full artifact-coordinate pass for 0.30.0 (the `occurrent-` prefix). It prefixed these four modules but kept the `position-storage` tail, so the coordinate rename and the type rename shipped in the same release without being reconciled. That was the natural point to catch this, and it was missed.

## Decision

Rename the four module coordinates from `-position-storage` to `-checkpoint-storage`, matching the `CheckpointStorage` type each ships and finishing the rename ADR 46 started:

| Old artifactId | New artifactId |
|---|---|
| `occurrent-subscription-mongodb-native-blocking-position-storage` | `occurrent-subscription-mongodb-native-blocking-checkpoint-storage` |
| `occurrent-subscription-mongodb-spring-blocking-position-storage` | `occurrent-subscription-mongodb-spring-blocking-checkpoint-storage` |
| `occurrent-subscription-mongodb-spring-reactor-position-storage` | `occurrent-subscription-mongodb-spring-reactor-checkpoint-storage` |
| `occurrent-subscription-redis-spring-blocking-position-storage` | `occurrent-subscription-redis-spring-blocking-checkpoint-storage` |

The module directories are renamed to match. The `org.occurrent` groupId is unchanged. This is coordinates and directories only: the packages inside the modules never contained "position", and the classes are already `*CheckpointStorage`, so no type, package, or source changes are needed. The event ordering-axis modules (`position-backfill`, `global-position-catchup`) keep their names, consistent with ADR 45.

This is a source- and binary-incompatible coordinate change for anyone depending on the four modules by name. Given the `0.x` status and that 0.30.0 already made hard, shim-free coordinate breaks under ADR 55, a clean rename is preferred over carrying an alias. The `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe rewrites the coordinates for Maven and Gradle, and the [upgrade guide](../migration/upgrading-to-0.31.0.md) records the mapping.

## Consequences

The module coordinate now says what the module ships. A consumer looking for the checkpoint-storage adapter finds it under a checkpoint-storage coordinate, and the coordinate no longer reuses the ordering-axis word "position" that ADR 45 and ADR 46 worked to disambiguate.

Every consumer depending on one of the four modules updates its coordinate at the 0.31.0 upgrade, either by running the recipe or by hand from the mapping above. This is the only remaining reference to the old checkpoint naming, so after 0.31.0 nothing in a published coordinate still calls a checkpoint a position.
