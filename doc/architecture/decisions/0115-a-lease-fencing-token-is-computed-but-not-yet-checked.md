# 115. A lease fencing token is computed but not yet checked

Date: 2026-08-09

## Status

Accepted. Fixes #660. Wiring the token into a real check is filed separately as #665.

## Context

`MongoListenerLockService.acquireOrRefreshFor`'s javadoc promised that actions requiring the lock use a fencing token, an increasing number a downstream write can check so that a write from a node that already lost the lease is rejected. The service computes that number. `version` increments whenever the lease changes hands and stays the same on a refresh, and it comes back as `ListenerLock.version()`.

Nothing reads it. `MongoLeaseCompetingConsumerStrategySupport.acquireLease` collapses the returned `Optional<ListenerLock>` to `.isPresent()` and throws the lock itself away. No write anywhere in Occurrent compares against the version. A subscriber whose lease has moved to another node, the case the javadoc warned about, is not rejected by anything.

[ADR 113](0113-a-competing-consumers-status-and-its-lease-call-are-one-step.md) already named this gap and filed it separately rather than fixing it. This ADR is that follow-up.

## Decision

**The javadoc is corrected to describe what a lease handover can actually redo today, instead of promising a fence that doesn't exist.** A subscriber's lease can expire while it still believes it holds the lock and is still acting on events. A checkpoint written after that point can move the checkpoint backward, so the new holder redelivers events already handled once. Delivery stays at least once, and the redelivered events are processed again.

**`ListenerLock`, `version()`, and the version-increment logic in `MongoListenerLockService`'s update pipeline stay exactly as they are.** They cost nothing to keep, since the field is already computed as part of the same atomic update that decides who holds the lease, and a real fence needs precisely this value. Removing them now would mean rebuilding the same pipeline logic from scratch when the fence is wired.

**Wiring the token into an actual check is deferred to #665, not built here.** Making a checkpoint write reject a stale token touches two public interfaces. `CompetingConsumerStrategy` is boolean-shaped throughout (`registerCompetingConsumer` and `hasLock` both return `boolean`) and its own javadoc invites third-party implementations, so widening it is a breaking change for anyone who implemented it. `CheckpointStorage` is the thing that would need to compare the token before accepting a write, and it ships four implementations, `NativeMongoCheckpointStorage`, `SpringMongoCheckpointStorage`, `ReactorCheckpointStorage`, and `SpringRedisCheckpointStorage`, all of which would need the same check added. `CompetingConsumerSubscriptionModel` sits between the two and never sees individual event deliveries today, so getting the token from the strategy to the checkpoint write needs a path that doesn't exist yet. That is a design task of its own, not a documentation-sized change.

## Consequences

Until #665 lands, a node can still write a checkpoint after losing its lease. The worst it can do is move the checkpoint backward, so some events get redelivered and reprocessed. Nothing is lost and no checkpoint is left permanently wrong, which is why documenting the gap rather than leaving it silently wrong is enough for now.

A reader of `MongoListenerLockService` finds a `version` field and a `ListenerLock.version()` accessor with no caller. That is intentional, not an oversight, and this ADR is the record for it.
