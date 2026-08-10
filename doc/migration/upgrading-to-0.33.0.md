# Upgrading to Occurrent 0.33.0

`CheckpointStorage` and its reactor twin gain a conditional write. This is a real break, and every implementation of
either interface, in this repository and outside it, now has two more members to answer. No calling code changes,
because the two-argument `save` you already call stays exactly as it was, as a default that delegates to the new
one. `UpgradeToOccurrent_0_33` stubs the two new members for you on a class it finds missing them, a throwing
placeholder plus a review comment each, so the module compiles again. Filling in real behaviour is still yours, see
section 2. [ADR 116](../architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md)
has the reasoning.

Five subscription-capability interfaces are also renamed. None of them extended `SubscriptionModel`, and the old
names claimed a relationship they never had. `UpgradeToOccurrent_0_33` renames those too, see
[section 5](#5-five-subscription-capability-interfaces-are-renamed).

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

A test double that overrides the two-argument `save` to observe writes stops seeing them, because the subscription
models now call the three-argument `save` directly, so override that one instead.

`CheckpointWriteCondition` is sealed with three cases. `any()` is what the two-argument `save` has always meant. The
write always succeeds and the stored version, if there is one, is carried forward untouched. `notOlderThan(long)`
succeeds when nothing is stored yet, or when the stored version is not greater than the one offered, and otherwise
throws `CheckpointWriteConditionNotFulfilledException`. `ifAbsent()` succeeds only when no checkpoint is stored yet
for that subscription id, whatever version it would carry, and refuses the same way otherwise.

## 2. If you implement `CheckpointStorage` yourself, this one does not compile

Run `UpgradeToOccurrent_0_33` first. On every implementation it finds missing them, it adds the `save` overload and
`writeVersion` as a throwing stub, each marked with a `TODO [Occurrent 0.33 upgrade]` comment, so the module
compiles again without a manual pass. Filling in the stub is still yours. A store that only ever wrote
unconditionally can keep doing exactly that and refuse the rest:

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
for a capability it was not built with.

Occurrent's own Mongo and Redis checkpoint storages do not need this recipe. They already evaluate `notOlderThan`
and `ifAbsent` for real. Redis Cluster is the exception. It still refuses a conditional write outright, and
section 4 covers why.

If your store can evaluate a condition for real, the two rules that matter are the same two the TCK asserts on every
storage that declares it supports them. `any()` must leave whatever version is stored untouched, carrying it
forward, rather than clearing it or overwriting it with something inferred from the write. And `notOlderThan(v)`
must accept when nothing is stored, since that is a checkpoint written before this condition existed, and every
checkpoint saved by an earlier release has to stay readable.

## 3. A 0.32.0 node during a rolling upgrade

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

## 4. Redis Cluster

`SpringRedisCheckpointStorage` refuses `notOlderThan` and `ifAbsent` on Redis Cluster, on the first conditional
write, because the checkpoint and its stored version live in two differently named keys that Cluster will not
guarantee land in the same slot.

## 5. Five subscription-capability interfaces are renamed

Only relevant if you implement or call one of these directly.

| Old | New |
|---|---|
| `org.occurrent.subscription.api.blocking.ReplayAwareSubscriptionModel` | `ReplayAwareSubscriptions` |
| `org.occurrent.subscription.api.reactor.ReplayAwareSubscriptionModel` | `ReplayAwareSubscriptions` |
| `org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel` | `IntrospectableSubscriptions` |
| `org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel` | `IntrospectableSubscriptions` |
| `org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel` | `SubscriptionModelWrapper` |

None of the five ever extended `SubscriptionModel`. `SubscriptionModelWrapper`'s two methods are renamed with it,
so the type and its methods share one vocabulary:

| Old method | New method |
|---|---|
| `getDelegatedSubscriptionModel()` | `getWrappedSubscriptionModel()` |
| `getDelegatedSubscriptionModelRecursively()` | `getWrappedSubscriptionModelRecursively()` |

### Run the recipe

`UpgradeToOccurrent_0_33` renames all five interfaces and both methods for you, in Java and Kotlin alike, the same
way [section 5 of the 0.32.0 guide](upgrading-to-0.32.0.md#5-the-reactor-subscriptionmodel-is-now-fluxsubscriptionmodel)
renamed the reactor `SubscriptionModel`. Run it once, as part of the upgrade.

### By hand

Change the import and the type name at every use listed in the first table, and the two method names on
`SubscriptionModelWrapper` listed in the second.
