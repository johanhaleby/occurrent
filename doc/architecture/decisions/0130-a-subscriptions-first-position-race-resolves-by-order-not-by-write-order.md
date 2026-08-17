# 130. A subscription's first-position race resolves by order, not by write order

Date: 2026-08-17

## Status

Accepted. Closes #771, together with #738 before it. Amends [ADR 116](0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md)'s
closing paragraph and [ADR 86](0086-a-manual-subscription-is-registered-not-started.md)'s fourth amendment, both of
which named this as the open question. Builds on the `ifAbsent()` pin both amendments record.

## Context

`ManualStartSubscriptionModel.pinStartPosition` and `ReactorDurableSubscriptionModel`'s own first-position pin write
a subscription's very first checkpoint with `CheckpointWriteCondition.ifAbsent()`, so only the first of two racing
writes lands. ADR 116's amendments narrowed the window this leaves behind twice, and named what was left of it as a
question neither wrapper could answer on its own: `Checkpoint` promises only `asString()`, so neither model can tell
which of two positions is earlier, and a stored position that turns out to be the later one costs the events between
the two.

Two shapes of the same gap reached #771. The wrapper reads whether a checkpoint already exists before it captures
its own position, and a checkpoint that was there at that read is taken without comparison, on the reasoning that it
was written before this node's own capture. That reasoning has a premise. A position source hands out positions
in the order it is asked for them. It also has a hole its own author found by adversarial review of `ManualStartSubscriptionModel.pinStartPosition`.
`cancelSubscription` deletes a stored checkpoint, so a delete followed by another node's registration is reachable
between the existence read and this node's own write, and what the existence read found is not what the write
actually raced against. Comparing values instead was considered and rejected in ADR 116's amendment. A subscription
running elsewhere rewrites its checkpoint on every event by default, so a value comparison would warn on every
ordinary registration against a busy subscription, burying the case the warning exists for in noise from the case
it must never fire on. Telling the two apart needs the same thing the residual race needs, which is an ordering on
the positions rather than on when a write happened to land.

Closing it, per ADR 116's own framing, needs either an ordering on the positions or agreement between the racing
nodes before either captures one. Agreement is not reachable here. Registration runs before a competing consumer is
registered, so there is no lease yet to decide with, and building one for this alone would be a distributed
coordination mechanism invented for a single caller. An ordering is reachable, but not as a promise `Checkpoint`
itself can make. Its single `asString()` method, and implementations living outside this repository, rule that out
as ADR 116 already says. What is reachable is an ordering some storages can make good on for positions they minted
themselves, which is exactly what both Mongo storages already do for the fencing token in `notOlderThan`.

## Decision

**A new, additive, opt-in method on `CheckpointStorage`, both stacks, not a fourth `CheckpointWriteCondition` case.**

```java
Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate);
```

(`Mono<Checkpoint>` on the reactor twin, empty meaning the same thing.) Called once a plain
`save(id, checkpoint, ifAbsent())` is not enough to settle a subscription's first position on its own, whether
because that write already lost to whatever is stored now, or because the candidate was captured earlier, at
registration on a stopped model, and needs reconciling against whatever governs by the time it is asked about
again. A storage that can compare the two, atomically with any write that comparison calls for, answers with
whichever checkpoint now governs, either the candidate, once durably written in place of a stored position proved
later, or the stored one, confirmed earlier than or equal to the candidate and left untouched. Either way the caller raises
no `StartPositionAlreadyPinnedException`, because a position settled this way cannot skip anything the candidate
would have covered. The default answers empty, unconditionally, which is the honest answer for a storage with
nothing to compare positions by, and leaves the caller to the narrower, write-order-based rule 0.33.0 shipped,
unchanged.

`CheckpointWriteCondition` was considered for the fourth case instead, since it already carries the vocabulary this
needs. Rejected. It shipped in 0.33.0, so a new sealed case is a real break. Every implementation, including the
third-party ones the TCK exists for, has to answer for a case only one caller in this repository will ever ask for,
which earns none of the migration-recipe-and-guide weight ADR 116 paid for the three cases every checkpoint store
actually needs. A dedicated method costs nothing to storages that do not override it, and says in its own signature
what it is for rather than folding a special case into a general condition type.

**Mongo answers it by comparing `operationTime`, the same field `MongoOperationTimeCheckpoint` already carries and
`MongoCommons` already builds documents from.** `NativeMongoCheckpointStorage`, `SpringMongoCheckpointStorage` and
`ReactorCheckpointStorage` all reach a new `MongoCommons.buildFirstCheckpointRaceResolution` pipeline stage, the same
shape as `buildConditionalCheckpointWrite`: one `findOneAndUpdate` round trip, upserting, that replaces the stored
document with the candidate's when nothing is stored yet or the stored `operationTime` is later, and leaves the
document untouched otherwise. `MongoCommons.interpretFirstCheckpointRaceResolution` reads the returned document back
into the answer. It is present when the document carries `operationTime`, whether that is the newly written
candidate or a stored position that proved earlier or equal, and empty when it carries `resumeToken`, the generic
`checkpoint` field or the legacy one instead, since only real delivery produces those and this method must never
touch a checkpoint from real delivery. A `candidate` that is not a `MongoOperationTimeCheckpoint` is answered empty in Java, no round trip
needed, since a caller-supplied position source can hand back anything.

This is why the capability is scoped to Mongo's own `operationTime` rather than built as a general
`Comparable<Checkpoint>` on the type itself. `ManualStartSubscriptionModel`'s own javadoc already scopes its safety
claim to "a position source that hands out positions in the order it is asked for them, as the MongoDB subscription
models do", and both Mongo subscription models mint `MongoOperationTimeCheckpoint` from the server's own clock for
exactly the capture this pin makes. The comparison can therefore run entirely on data the storage already persists,
with no new contract on `Checkpoint` and no opt-in marker interface a caller would have to know to implement.
Redis and the in-memory storages answer empty, unchanged, since neither has an ordered position to compare by.

**`ManualStartSubscriptionModel.pinStartPosition` tries this first on a lost `ifAbsent()` write, before its existing
`checkpointAlreadyExisted` fallback.**

```java
} catch (CheckpointWriteConditionNotFulfilledException e) {
    if (checkpointStorage.resolveFirstCheckpointRace(subscriptionId, positionToPin).isPresent()) {
        return;
    }
    if (checkpointAlreadyExisted) {
        return;
    }
    refuseUnlessTheStoredPositionIsTheOneRead(checkpointStorage, subscriptionId, positionToPin);
}
```

A storage that resolves it settles both #771 holes at once, since the resolution is made against whatever is
actually stored at that moment rather than against what `exists()` found earlier. That closes the residual race,
because the earlier of two racing positions now wins regardless of which write reached storage first, and it closes
the delete-recreate hole, because a checkpoint that was deleted and rewritten between the existence read and this
write is compared on its own merits rather than accepted on the strength of a presence check that ran against
something else entirely.
A storage that cannot resolve it falls through to the existing rule exactly as it stood, so nothing regresses for a
caller on Redis, the in-memory storage, or a storage of their own.

**`ReactorDurableSubscriptionModel` gets the same call at both of the places it records a subscription's first
position.** `recordFirstPosition`'s lost `ifAbsent()` write tries it before
`refuseUnlessTheStoredPositionIsTheOneRead`, the same shape as the blocking model, closing the residual race there
and turning what was always a refusal on this stack into a resolution when the storage can make one.
`resolveStartAt`'s handling of a subscription registered while the model was stopped gets a second call the blocking
model has no equivalent of, because that is where this stack's own delete-recreate hole lives, per ADR 89's own
amendment. A registration-time candidate is reconciled against whatever `storage.read()` finds at start, rather than
that read being trusted on presence alone.

```java
if (positionAtRegistration != null) {
    return storage.read(subscriptionId)
            .flatMap(stored -> positionAtRegistration
                    .flatMap(checkpoint -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint))
                    .onErrorResume(__ -> Mono.empty())
                    .defaultIfEmpty(stored))
            .switchIfEmpty(Mono.defer(() -> positionAtRegistration.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint))))
            .map(StartAt::checkpoint);
}
```

`storage.read(subscriptionId)` runs first and on its own, not folded into the same chain as
`positionAtRegistration`, because a stored checkpoint must keep governing exactly as it always has even when
`positionAtRegistration` itself cannot be read or the storage cannot compare, which is a guarantee ADR 89's own
amendment already makes and this must not narrow: "a checkpoint already stored for it, or a start position of its
own, still lets it start despite this failure". `onErrorResume` and `defaultIfEmpty` are what keep that guarantee
while still attempting the improvement when both sides can be read.

## Consequences

Checked against the hard rule first. Both #771 holes are event-loss gaps under a design that promises none, and
this closes both of them for the storages this library ships and defaults to. Neither previously-refused
registration reaches a different outcome from starting. A resolution that adopts the candidate is exactly what a
correct comparison would have chosen, and one that keeps the stored position is exactly the outcome the fallback
already reached, only now confirmed by comparison rather than by a presence check that could be looking at the
wrong checkpoint.

What stays open, by design and stated so nobody has to rediscover it. A caller on Redis, the in-memory storage, or a
custom `CheckpointStorage` still runs against the narrower, write-order-based rule 0.33.0 shipped, with its own
`checkpointAlreadyExisted`-style residual on the blocking stack and its own registered-while-stopped residual on the
reactor stack. Closing those needs an ordering capability those storages do not have, which is exactly the
constraint that ruled out putting this on `Checkpoint` itself. A caller who needs the guarantee on such a storage has
the same two options ADR 116 already named. Mint positions this storage can order, or accept the narrower rule.

Nothing here is a breaking change. `resolveFirstCheckpointRace` is an additive default method on both
`CheckpointStorage` interfaces, so every existing implementation, in this repository and outside it, compiles and
behaves exactly as it did without touching it. No `CheckpointWriteCondition` case changes, so no OpenRewrite recipe
and no migration-guide section are owed. The changelog entry sits under `#### Changes`, not `#### Breaking changes`.

`StartPositionAlreadyPinnedException` is thrown in strictly fewer cases than it was, never in more. Every case a
storage now resolves is one that previously reached the exception's narrower comparison path or its
`checkpointAlreadyExisted` silent-accept path, and both of those outcomes are subsumed by the ordered resolution
wherever it can run. A caller catching the exception for retry logic sees it less often on the storages this closes
the gap for, never more.
