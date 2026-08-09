# Upgrading to Occurrent 0.33.0

`CheckpointStorage` and its reactor twin gain a conditional write. This is a real break, and every implementation of
either interface, in this repository and outside it, now has two more members to answer. No calling code changes,
because the two-argument `save` you already call stays exactly as it was, as a default that delegates to the new
one. There is no recipe, because filling in a method body is not a rename a recipe could apply.
[ADR 116](../architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) has the
reasoning.

## 1. What changed

`CheckpointStorage.save` gained a third parameter, `CheckpointWriteCondition condition`, stating what must be true
of the stored version before the write is allowed:

```java
Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

default Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
    return save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
}

OptionalLong writeVersion(String subscriptionId);
```

The reactor twin gets the same two members, `Mono<Checkpoint> save(String, Checkpoint, CheckpointWriteCondition)`
and `Mono<Long> writeVersion(String)`, with an empty `Mono` meaning no version is stored. A refusal on that stack
signals `Mono.error`, it never throws from assembly.

`CheckpointWriteCondition` is sealed with three cases. `any()` is what the two-argument `save` has always meant. The
write always succeeds and the stored version, if there is one, is carried forward untouched. `notOlderThan(long)`
succeeds when nothing is stored yet, or when the stored version is not greater than the one offered, and otherwise
throws `CheckpointWriteConditionNotFulfilledException`. `ifAbsent()` succeeds only when no checkpoint is stored yet
for that subscription id, whatever version it would carry, and refuses the same way otherwise.

## 2. If you implement `CheckpointStorage` yourself, this one does not compile

Add the `save` overload and `writeVersion`. A store that only ever wrote unconditionally can keep doing exactly
that and refuse the rest:

```java
@Override
public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
    if (!(condition instanceof CheckpointWriteCondition.Any)) {
        throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
    }
    return save(subscriptionId, checkpoint); // your existing unconditional write
}

@Override
public OptionalLong writeVersion(String subscriptionId) {
    return OptionalLong.empty();
}
```

That is not a stopgap you are expected to replace before compiling. It is the correct, permanent answer for a store
that genuinely cannot evaluate a condition. `UnsupportedOperationException` is the same refusal an event store gives
for a capability it was not built with, and Occurrent's own Mongo and Redis checkpoint storages answer this way
today, until a later release teaches them the real comparison.

If your store can evaluate a condition for real, the two rules that matter are the same two the TCK asserts on every
storage that declares it supports them. `any()` must leave whatever version is stored untouched, carrying it
forward, rather than clearing it or overwriting it with something inferred from the write. And `notOlderThan(v)`
must accept when nothing is stored, since that is a checkpoint written before this condition existed, and every
checkpoint saved by an earlier release has to stay readable.

## 3. One release between the lease change and the fence

Competing consumers ships the fencing token and the checkpoint write condition together in 0.33.0. During a rolling
upgrade, a node still running 0.32.0 releases its lease the old way. It deletes the lock document rather than
unsetting it, so the token restarts at zero instead of continuing to climb. A 0.33.0 node that then acquires the
lease reads that fresh zero and offers it as `notOlderThan(0)`, which a checkpoint already stamped with a higher
version refuses. The node's write is refused, its event is not acknowledged, its lease is judged idle at the next
refresh and released, and the next node to acquire it lands in the same position. That repeats, once per unit of the
version the checkpoint remembers, and each cycle costs one lease period and one re-run of whatever your handler did
before the refusal.

It ends on its own once every node in the deployment runs 0.33.0, because from then on every release keeps
incrementing the token rather than resetting it. If you need it to end sooner, on a subscription that is stuck
cycling, `CheckpointStorage.delete(subscriptionId)` clears the checkpoint and its stored version together, and the
subscription resumes as a fresh one. That costs a replay. Everything since the subscription's global checkpoint is
redelivered, which is within the at-least-once contract this library has always kept, not a new kind of loss.
