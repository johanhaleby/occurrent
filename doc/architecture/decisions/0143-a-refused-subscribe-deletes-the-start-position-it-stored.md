# 143. A refused subscribe deletes the start position it stored

Date: 2026-10-06

## Status

Proposed. Part of [#1197](https://github.com/johanhaleby/occurrent/issues/1197). Applies to `DurableSubscriptionModel`
and the blocking `CheckpointStorage`. The first position it deletes is the one
[ADR 130](0130-a-subscriptions-first-position-race-resolves-by-order-not-by-write-order.md) writes.

## Context

`DurableSubscriptionModel` records a subscription's start position inside `subscribe(..)`, before the wrapped model
has seen the call, when the subscription asks for the model default and nothing is stored for its id. If the wrapped
model already holds that id, it refuses the subscribe with `DuplicateSubscriptionIdException`, and the recorded
position stays in checkpoint storage.

That position is harmful when the subscription the wrapped model already holds is paused and has stored nothing yet,
for example one started from `StartAt.now()` that was paused before it handled an event. `resumeSubscription(..)`
reads the stored checkpoint and resumes from it. The position the refused subscribe recorded is later than the pause,
so every event written while the subscription was paused is skipped. With `NativeMongoSubscriptionModel`, a
subscription paused before `e1` was written and resumed after a refused subscribe received only `e2`, where it should
have received `e1` and `e2`.

`DurableSubscriptionModel` refuses a subscribe of an id the wrapped model holds before it stores anything. That check
asks the wrapped model, and its answer can miss the id. `NativeMongoSubscriptionModel` moves an id between its running
and paused subscriptions in two steps, so `isRunning(id)` and `isPaused(id)`, asked one after the other while a pause
or a resume runs, can both answer `false`. A subscription model written outside Occurrent can answer late or loosely
too. So the check alone does not close the loss.

## Decision

The check up front stays, and asks a wrapped model that implements `IntrospectableSubscriptions` for its
`subscriptionIds()`. The native and Spring MongoDB models answer that under the same lock their pause, resume, stop and
start take, so the answer never misses an id that is being moved. Any other wrapped model is asked `isRunning(id)` and
`isPaused(id)`.

When a subscribe gets past the check, stores a first position, and the wrapped model then refuses it with
`DuplicateSubscriptionIdException`, `DurableSubscriptionModel` deletes that position again. It deletes it only if it
is still the one the subscribe stored, because the running subscription of the id may have written its own checkpoint
in between, and deleting that one would move the subscription back.

The blocking `CheckpointStorage` gets two default methods for this:

```java
default void deleteIfUnchanged(String subscriptionId, Checkpoint checkpoint, OptionalLong writeVersion)
default boolean deletesIfUnchanged()
```

`deleteIfUnchanged` deletes the checkpoint and its version only if the stored checkpoint has the same `asString()` and
the stored version is `writeVersion`, with an empty `writeVersion` meaning no version is stored. The comparison and the
delete are one atomic step. The default refuses with `UnsupportedOperationException`, and `deletesIfUnchanged()`
answers `false`, the same way an optional capability of `CheckpointStorage` such as `evaluatesWriteConditions()` is
declared. The name follows the reactor `delete(subscriptionId, CheckpointWriteCondition)`.

The four blocking storages Occurrent ships implement it:

| Storage | How the comparison and the delete are one step |
|---|---|
| `InMemoryCheckpointStorage` | Under the lock every write takes |
| `NativeMongoCheckpointStorage` | One `deleteOne` whose filter holds the id, the checkpoint field and the version |
| `SpringMongoCheckpointStorage` | The same `deleteOne` |
| `SpringRedisCheckpointStorage` | A Lua script that compares both keys and deletes them |

`DurableSubscriptionModel` passes an empty version, because a first position is written with `ifAbsent()` or `any()`,
and neither stores a version of its own. The method returns nothing. A MongoDB or Redis call that is retried after a
timeout can't tell whether the first attempt deleted, so a returned `boolean` would not be reliable.

Only `DuplicateSubscriptionIdException` deletes the position. A subscribe refused for another reason, such as one on a
model that was shut down, has no live subscription of the id that could resume from the position, and a later
subscribe starts from it, so it receives more events and not fewer.

A failure to delete is added to the refusal as a suppressed exception, and the caller gets the refusal unchanged.

## Consequences

A storage that keeps the default can't delete the position, and a wrapped model that does not implement
`IntrospectableSubscriptions` can miss an id in the check up front. With both, the loss described above can still
happen. `DurableSubscriptionModel` logs one warning when it's created with that combination, naming the storage, the
wrapped model, and the methods to implement. That warning does not close the loss, and implementing either side does.

A custom wrapped model has to answer `isRunning(id)` and `isPaused(id)` for each id, since the check up front relies on
those answers when the model does not list its subscriptions.

The undo can't tell a later write of the same checkpoint with the same version from the one it stored, and deletes it.
For the MongoDB storages the first position is an operation time and the checkpoints a subscription writes while it
handles events are resume tokens, so the two don't collide. A MongoDB `ifAbsent()` write of a value already stored by
another node is reported as written, so the undo can delete a first position another node stored with the same
operation time.

`SpringRedisCheckpointStorage` in its Cluster-safe mode refuses `deleteIfUnchanged` for the subscription ids its class
javadoc names, with `IllegalArgumentException`. For those ids that exception is added to the refusal as a suppressed exception, and the
position stays.

The conformance suite in `occurrent-tck-subscription-blocking` holds a storage that answers `true` from
`deletesIfUnchanged()` to the contract, and one that answers `false` to refusing every call and deleting nothing.
