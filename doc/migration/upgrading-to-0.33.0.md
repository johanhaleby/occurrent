# Upgrading to Occurrent 0.33.0

`CheckpointStorage` and its reactor twin gain a conditional write. This is a real break, and every implementation of
either interface, in this repository and outside it, now has two more members to answer. No calling code changes,
because the two-argument `save` you already call stays exactly as it was, as a default that delegates to the new
one. `UpgradeToOccurrent_0_33` stubs the two new members for you on a Java class it finds missing them, delegating
`any()` to your existing write and marking the rest with a review comment, so the module compiles again. That stub
is Java only, so a Kotlin implementer adds the same two members by hand, see section 2. Evaluating a condition for
real is still yours either way.
[ADR 116](../architecture/decisions/0116-a-checkpoint-write-from-a-lease-that-has-moved-on-is-refused.md) has the
reasoning.

Five subscription-capability interfaces are also renamed, and two of their static lookup methods go from `of` to
`findIn`. None of the interfaces extended `SubscriptionModel`, and the old names claimed a relationship they never
had. `UpgradeToOccurrent_0_33` renames those too, see
[section 5](#5-five-subscription-capability-interfaces-are-renamed).

A saga timer's name is a `TimerName` rather than a `String`. Most saga code compiles unchanged, because every method
that took a timer name as a string still takes one. What breaks is building a `SagaTimeout` from two strings,
constructing `StartTimeout`, `StartTimeoutAt` or `CancelTimeout` directly with a string name, reading `timerName()`
into a `String`, and matching a timer effect against a `String` component. `UpgradeToOccurrent_0_33` rewrites every
construction it can prove and every `String`-declared read, and marks the rest, see
[section 7](#7-a-saga-timers-name-is-a-timername).

## 1. What changed

`CheckpointStorage.save` gained a third parameter, `CheckpointWriteCondition condition`, stating what must be true
of the stored version before the write is allowed:

```java
Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

default Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
    return save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
}

OptionalLong writeVersion(String subscriptionId);

default boolean evaluatesWriteConditions() {
    return false;
}
```

The reactor twin gets the same three members, `Mono<Checkpoint> save(String, Checkpoint, CheckpointWriteCondition)`,
`Mono<Long> writeVersion(String)` and the same `evaluatesWriteConditions()`, with an empty `Mono` meaning no version
is stored. A refusal on that stack signals `Mono.error`, it never throws from assembly.

`evaluatesWriteConditions()` is the only one of the three with a default, and the default is `false`, so a storage that
writes unconditionally compiles and keeps working without answering it. Say `true` when your storage accepts and
refuses `notOlderThan` and `ifAbsent` as documented and leaves a stored version untouched under `any()`. A caller that
depends on a conditional write asks first, which is how the Spring Boot starter refuses a wiring that would otherwise
throw on the first checkpoint write. Section 8 covers that failure.

A test double that overrides the two-argument `save` to observe writes stops seeing them, because the subscription
models now call the three-argument `save` directly, so override that one instead.

`CheckpointWriteCondition` is sealed with three cases. `any()` is what the two-argument `save` has always meant. The
write always succeeds and the stored version, if there is one, is carried forward untouched. `notOlderThan(long)`
succeeds when nothing is stored yet, or when the stored version is not greater than the one offered, and otherwise
throws `CheckpointWriteConditionNotFulfilledException`. `ifAbsent()` succeeds only when no checkpoint is stored yet
for that subscription id, whatever version it would carry, and refuses the same way otherwise.

## 2. If you implement `CheckpointStorage` yourself, this one does not compile

If you implement in Java, run `UpgradeToOccurrent_0_33` first. On every Java implementation it finds missing them, it
adds the `save` overload and `writeVersion`, each marked with a `TODO [Occurrent 0.33 upgrade]` comment, so the
module compiles again without a manual pass. This checkpoint-storage stub is Java only, the same limitation the saga
timer rewrite in section 7 runs into, so a Kotlin implementer does the equivalent by hand, below. What it generates
for a Java class is exactly the snippet below. `save` delegates `any()` to your existing two-argument override and refuses
anything stronger, and `writeVersion` answers empty. A store that only ever wrote unconditionally can leave that
exactly as generated:

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

Such a store leaves `evaluatesWriteConditions()` alone, since the default already answers `false` for it. That answer
is what lets a caller find out before it wires anything up, rather than on the first write, so leaving it at the
default is part of the recipe rather than an omission. One place acts on it today. The Spring Boot Mongo starter
refuses to start when it would pair your store with a competing-consumer lease, and section 8 says what to do about
that.

Occurrent's own Mongo and Redis checkpoint storages do not need this recipe. They already evaluate `notOlderThan`
and `ifAbsent` for real, on Redis Cluster too, see section 4. `SpringRedisCheckpointStorage`'s original two
constructors refuse one subscription id shape outright for a conditional write, whether or not the deployment they
run against is actually Cluster. `SpringRedisCheckpointStorage.forStandalone(..)`, new in this release, is the other
mode. A conditional write against a standalone or replicated deployment built that way accepts that shape too, the
same as it always accepted every subscription id outside the version key's own reserved namespace, see section 4.

A store whose answer to a conditional write depends on the subscription id, the way the two Redis modes above do, can
say so precisely with `evaluatesWriteConditionsFor(String subscriptionId)`, a second new default method that answers
`evaluatesWriteConditions()` for every id unless overridden. The blocking Spring Boot starter's own fencing check
reads it too, see section 8. No such check exists on the reactor stack yet, so a reactor `CheckpointStorage`
implementer overrides it only for callers that ask directly.

If your store can evaluate a condition for real, the two rules that matter are the same two the TCK asserts on every
storage that declares it supports them. `any()` must leave whatever version is stored untouched, carrying it
forward, rather than clearing it or overwriting it with something inferred from the write. And `notOlderThan(v)`
must accept when nothing is stored, since that is a checkpoint written before this condition existed, and every
checkpoint saved by an earlier release has to stay readable.

### By hand in Kotlin

This checkpoint-storage stub does not touch a Kotlin file at all, the same limitation the saga timer rewrite in
section 7 runs into. A Kotlin implementer of the blocking `CheckpointStorage` adds the same two members the Java
stub above adds, against the interface's real signatures:

```kotlin
override fun save(subscriptionId: String, checkpoint: Checkpoint, condition: CheckpointWriteCondition): Checkpoint {
    if (condition !is CheckpointWriteCondition.Any) {
        throw UnsupportedOperationException("This storage cannot evaluate $condition, only any() is supported.")
    }
    return save(subscriptionId, checkpoint) // your existing unconditional write
}

override fun writeVersion(subscriptionId: String): OptionalLong = OptionalLong.empty()
```

The reactor twin signals rather than throws, the same way the Java reactor stub does:

```kotlin
override fun save(subscriptionId: String, checkpoint: Checkpoint, condition: CheckpointWriteCondition): Mono<Checkpoint> {
    if (condition !is CheckpointWriteCondition.Any) {
        return Mono.error(UnsupportedOperationException("This storage cannot evaluate $condition, only any() is supported."))
    }
    return save(subscriptionId, checkpoint) // your existing unconditional write
}

override fun writeVersion(subscriptionId: String): Mono<Long> = Mono.empty()
```

Both are the same permanent answer as the Java stub, not a placeholder to replace before compiling: `any()`
delegates to your existing two-argument `save`, anything stronger is refused, and `writeVersion` answers empty.
Leave `evaluatesWriteConditions()` at its default `false` unless your store evaluates `notOlderThan` and `ifAbsent`
for real, in which case write the same real evaluation the paragraph above asks of a Java store.

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

`SpringRedisCheckpointStorage` keeps the checkpoint and its stored version in two differently named keys, and
Cluster refuses a script that touches keys in different slots. The version key's name carries a hash tag built from
whatever the checkpoint key itself hashes on, so Cluster places both in the same slot and `notOlderThan` and
`ifAbsent` work there exactly as they do on a standalone or replicated server. The version key also carries a
SHA-256 digest of the subscription id after that tag, so two ids that happen to share a hash tag, two tenant-scoped
ids under the same `{tenant}` for instance, still get their own version key instead of silently sharing one
fencing version. A digest rather than a raw or delimited copy of the id, because the tag can itself equal the whole
subscription id, and two non-cryptographic constructions tried here both let one id's own text be misread as a
different id's tag plus copy.

One shape this cannot help is a subscription id where Cluster itself falls back to hashing the whole id (no brace
pair, an unmatched brace, or an empty pair like `{}`) and that whole id is either empty or contains a closing brace
somewhere in it, for example `""`, `"{}orders"` or `"a}b{c"`.

Built with either of `SpringRedisCheckpointStorage`'s original two constructors, `save` refuses an id of that shape
outright with an `IllegalArgumentException` for `notOlderThan` and `ifAbsent`, whether or not the deployment behind
it is actually Cluster. That is what keeps `evaluatesWriteConditions()` true without exception in that mode, rather
than true for every id except one Cluster would otherwise refuse two calls downstream, and
`evaluatesWriteConditionsFor(subscriptionId)` answers `false` for that shape there (and for the version key's own
reserved namespace below, in both modes) and `true` for every other id. `SpringRedisCheckpointStorage.forStandalone(..)`,
new in this release, is for a deployment that is standalone or replicated, where slot alignment is not a concept a
server has. Built that way, `save` accepts that shape too for `notOlderThan` and `ifAbsent`, and
`evaluatesWriteConditionsFor` agrees, answering `true` for it. Do not build a Cluster deployment's storage with
`forStandalone`. A conditional write for an id the standalone mode accepts but Cluster cannot align a slot for then
fails with Redis's own `CROSSSLOT` error instead of the refusal above. `any()` never refuses one for slot alignment,
in either mode, since it writes only the checkpoint key. `delete` never refuses one either. An id of this shape can
only ever have had a checkpoint written for it through `any()` in Cluster-safe mode, since a conditional write
already refuses one before touching Redis, so its version key can never exist to strand. On a
`CROSSSLOT` failure `delete` falls back to two single-key deletes instead, which is provably safe for that reason and
not merely convenient.

This also assumes the `RedisOperations` passed in serializes a key to its own literal bytes, the same assumption
the checkpoint's plain `GET` already makes.

`read`, `save`, `delete`, and `exists` also refuse a subscription id that starts with the version key's own
reserved prefix (`occurrent:checkpoint-version:`), with an `IllegalArgumentException`. This is a Cluster-independent
guard, since a caller-chosen id equal to another subscription's version key would let a write against it corrupt
that other subscription's stored version on a standalone or replicated server too. Nothing this library or a
realistic caller produces starts with that prefix by accident, but 0.32.0 had no version key at all, so an id of
that exact shape worked there like any other, and this is a behaviour change for one that already exists.

If you have such an id, migrate it before upgrading, while still on the previous version. Read the checkpoint
under the old id, save it under a new one that does not start with the reserved prefix, delete the old id, then
point wherever the application passes that subscription id (a `subscribe(..)` call, typically) at the new one.
Do this through the storage's own API and before the upgrade, because afterward `read` refuses the old id the
same as `save` and `delete` do, so the API can no longer see the checkpoint to move it. If the upgrade has already
happened, migrate directly in Redis instead, with the application stopped, `GET` the old key, `SET` the new one to
that value, then `DEL` the old key, three single-key commands rather than `RENAME`. `RENAME` itself needs both keys
in the same Cluster slot and fails with `CROSSSLOT` otherwise, which a new id chosen only to avoid the reserved
prefix has no reason to land in, so it is not a safe substitute here. Update the application's own subscription id
the same way once the data has moved.

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

One published TCK base class moves with the interface it is named after:

| Old | New |
|---|---|
| `org.occurrent.tck.subscription.blocking.IntrospectableSubscriptionModelConformance` | `IntrospectableSubscriptionsConformance` |

Only relevant if you extend it yourself to check your own subscription model against the introspection contract.

The static lookup on `ReplayAwareSubscriptions` and `IntrospectableSubscriptions` is also renamed, `of` becomes
`findIn`, and narrows its parameter from `Object` to the new `SubscriptionModelCapability` marker:

| Old | New |
|---|---|
| `ReplayAwareSubscriptions.of(Object)` | `ReplayAwareSubscriptions.findIn(SubscriptionModelCapability)` |
| `IntrospectableSubscriptions.of(Object)` | `IntrospectableSubscriptions.findIn(SubscriptionModelCapability)` |

`of` is the convention Java uses for constructing a value, and `Optional.of` in particular never returns empty, but
this method searches a `SubscriptionModelWrapper` chain and can come back empty, so `findIn` says what it actually
does. Every capability facet, `Subscribable`, `SubscriptionModelLifeCycle`, `SubscriptionModel`,
`SubscriptionModelWrapper` and the rest ADR 118 below lists, now extends `SubscriptionModelCapability`, so a caller
whose argument is statically typed as one of them keeps compiling without change. A caller holding the same value
through a broader static type, an `Object` variable being the case that comes up, does not compile against the
narrowed parameter even though the value underneath is a genuine subscription model, and has to narrow the
variable's declared type instead, to the marker itself or to whichever facet it actually is. That is the trade-off
this rename makes, not a gap in it. `findIn` keeps taking the marker rather than gaining a second `Object`-typed
overload.
[ADR 118](../architecture/decisions/0118-a-subscription-model-capability-marker-replaces-object-in-the-of-lookups.md)
has the reasoning. `RepositionableSubscriptions.findIn` never shipped under the `of` name, so it is not in this table.

### Run the recipe

`UpgradeToOccurrent_0_33` renames all five interfaces, both methods, the `of` to `findIn` rename above, and the TCK
base class for you, in Java and Kotlin alike, the same way
[section 5 of the 0.32.0 guide](upgrading-to-0.32.0.md#5-the-reactor-subscriptionmodel-is-now-fluxsubscriptionmodel)
renamed the reactor `SubscriptionModel`. Run it once, as part of the upgrade.

### By hand

Change the import and the type name at every use listed in the tables above, the two method names on
`SubscriptionModelWrapper`, and `of` to `findIn` wherever you call `ReplayAwareSubscriptions` or
`IntrospectableSubscriptions`.

## 6. A flow saga's `join` is deprecated in favor of step conditions

`join` keeps working, and this section is only useful if you want to move off it, or if you need something `join`
cannot express in the first place. What changes regardless is `join`'s own reaction window, narrower in this release
whether or not you migrate off it, in the first step as much as any later one. See
[section 11](#11-a-lowered-joins-reaction-now-reads-its-own-window-not-the-whole-retained-history) before relying on
a `join`'s reaction to read more than the events its own expectations fulfilled.

A flow step can now wait on a `StepCondition` tree instead of only a single-branch choice or a `join`. `join`'s
per-type counting is one case a tree expresses, `allOf(event(Type, count), ...)`, so every existing `join` call has a
direct equivalent:

Java, before and after:

```java
// Before
step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end());

// After
step.on(StepCondition.allOf(StepCondition.event(PlayerReady.class, 2)), Continuation.end());
```

Kotlin, before and after:

```kotlin
// Before
join(expect<PlayerReady>(2), then = end)

// After
on(allOf(event<PlayerReady>(2)), then = end)
```

`whenFulfilled`/the trailing reaction lambda carries over unchanged. It still reads `ReceivedEvents`, not a single
triggering event. A tree also expresses what `join` never could, an alternative (`anyOf(...)`) or a predicate over an
event (`event(Type, predicate)`), so a step currently hand-rolling either with an `onlyIf` guard and manual counting
against `ReceivedEvents` can drop that guard for a tree instead. [ADR 120](../architecture/decisions/0120-a-step-condition-is-a-monotone-matcher-tree.md)
has the full design, including the normalization laws `allOf`/`anyOf` apply and the window-reset rule a mixed step
makes visible. No `UpgradeToOccurrent_0_33` recipe rewrites `join` calls. The API still works, and a recipe belongs
with whichever future release removes it.

## 7. A saga timer's name is a `TimerName`

Most saga code compiles unchanged, and that is worth saying first, because it covers the majority of callers.
`SagaEffect.startTimeout`, `startTimeoutAt` and `cancelTimeout` keep the forms that take a string, and so do
`evolveOnTimeout` and `reactOnTimeout` on the builder. Each of them reads its argument with `TimerName.parse`, so
`startTimeout("payment", ofMinutes(30))` arms the same timer it always did and `reactOnTimeout("payment", ..)` still
matches it. Nothing on disk changes either. A timer is stored under the string it has been stored under since
0.32.0, so a saga instance with a pending timer keeps firing across the upgrade.

A timer's name is now a value with two shapes. `Simple` is a name on its own, `Qualified` is a name inside a
namespace, and a qualified name writes itself out with a colon between the two:

```java
TimerName.parse("payment");                // Simple("payment")
TimerName.parse("step:awaiting-players");  // Qualified("step", "awaiting-players")
TimerName.of("step", "awaiting-players");  // the same qualified name, built from its parts
```

`parse` splits at the first colon and accepts every string, so it gives back the name a stored string already meant.
`encode()` writes a name back out as that string. `toString()` returns the same thing as `encode()` on both shapes,
so anything that puts a timer name into a log prints exactly what it printed before.

Four shapes stop compiling.

1. **Building a `SagaTimeout` from two strings.** It is `record SagaTimeout(String sagaId, TimerName timerName)`, and
   there is no two-string constructor beside it.
2. **Constructing `StartTimeout`, `StartTimeoutAt` or `CancelTimeout` directly with a string timer name.** These are
   public records, so `new StartTimeout<>("payment", duration)` is as much a 0.32.0 call as the string-taking
   `SagaEffect.startTimeout` factory, and it breaks the same way `SagaTimeout`'s constructor does.
3. **Reading `timerName()` into a `String`.** The accessor keeps its name and changes its type.
4. **Matching a timer effect against a `String` component.** A record pattern has to bind a `TimerName` instead.

Two more reads keep compiling and quietly answer differently. `timeout.timerName().equals("payment")` is now always
false, and an assertion written as `assertThat(timeout.timerName()).isEqualTo("payment")` fails for the same reason.
Compare `timeout.timerName().encode()` against the string, or compare the name against
`TimerName.parse("payment")`.
[ADR 121](../architecture/decisions/0121-a-saga-timers-name-carries-its-namespace.md) has the reasoning.

### Run the recipe

`UpgradeToOccurrent_0_33` rewrites the shapes it can prove.

`new SagaTimeout(sagaId, name)` with a string second argument becomes `new SagaTimeout(sagaId,
TimerName.parse(name))`, and the same wrapping happens to the string timer-name argument of a direct
`new StartTimeout<>(name, after)`, `new StartTimeoutAt<>(name, at)` or `new CancelTimeout<>(name)`. Every one of
these is exact rather than best effort, because `parse` gives back the value the old string already named,
`"step:awaiting-players"` from a flow saga test included.

A `timerName()` read into a declared `String` gains `encode()`, so `String name = timeout.timerName()` becomes
`String name = timeout.timerName().encode()`. The recipe does this in the three places where the wanted type is
written down in the source, a variable declared `String`, an assignment to a `String`, and a return from a method
declared to return `String`.

Every other read of `timerName()` gets a `TODO [Occurrent 0.33 upgrade]` comment instead, because the recipe cannot
see what the surrounding code wants the name for and the two answers are far apart. A read handed to something that
takes an `Object`, a logging call above all, prints the same text as before and needs no change, so the comment can
just go. A read that wanted a string needs `encode()`. The `equals` and `assertThat` cases above are in this group,
which is why they are marked rather than left silent.

The recipe leaves a record pattern alone. What a pattern binds is a judgement about what the code inside the case
then does with the name, and every one of them is a compile error anyway, so the compiler points at the ones you
have.

The recipe is Java only. A Kotlin caller does all of it by hand, the same limitation the `StartAt.subscriptionPosition`
rename ran into in [0.30.0](upgrading-to-0.30.0.md).

### By hand

Wrap the string handed to the `SagaTimeout` constructor in `TimerName.parse`, and call `encode()` on a `timerName()`
read that wanted a string. In a record pattern, bind a `TimerName` and call `encode()` on it where the code below
needs the string:

```java
// Before
case SagaEffect.CancelTimeout<C>(String timerName) -> cancel(timerName);

// After
case SagaEffect.CancelTimeout<C>(TimerName timerName) -> cancel(timerName.encode());
```

A test that fires a flow step's timer has a shorter answer than `parse`. `FlowSaga.stepTimer("awaiting-players")`
gives the name that step arms, so the test never writes the `step:` namespace itself, and
`SagaInput.timeout(sagaId, timerName)` fires it without building a `SagaTimeout` first:

```java
// Before
lobby.step(state, SagaInput.timeout(new SagaTimeout("game-1", "step:awaiting-players")));

// After
lobby.step(state, SagaInput.timeout("game-1", stepTimer("awaiting-players")));
```

Kotlin has both of those. There is a top-level `stepTimer` next to `saga { }`, and `startTimeout`, `startTimeoutAt`,
`cancelTimeout`, `evolveOnTimeout` and `reactOnTimeout` each take a `TimerName` as well as a string.

## 8. Three ways the Spring Boot starter now refuses to start

All three used to start and go wrong later, so they are startup failures on purpose. Each message names the beans
involved and what to do.

**Several `CompetingConsumerStrategy` beans with no `@Primary`.** Adding a strategy of your own used to leave two beans
of that type, and Occurrent read the ambiguity as no strategy at all, which wrote every checkpoint unconditionally and
ran a `@Saga`'s timer poller on every instance. Both of those are the protections the strategy exists to provide.
Occurrent now throws `AmbiguousCompetingConsumerStrategyException` during startup. Mark the bean you want with
`@Primary`, or leave only that one in the context.

You probably do not have two. The Mongo starter's default strategy backs off for yours now, whatever type yours is, so
one strategy bean of your own replaces it rather than joining it. That also fixes a case that was quietly broken. A
custom strategy of a type other than `SpringMongoLeaseCompetingConsumerStrategy` never reached the subscription model
at all, which kept delivering under the starter's own lease.

**A `CheckpointStorage` that only writes unconditionally, wired next to a strategy.** The starter stamps each
checkpoint write with the lease version, and a storage that refuses `notOlderThan` throws
`UnsupportedOperationException` the first time a node writes a checkpoint while holding its lease. That is the ordinary
case of one consumer running, not a rare one, so the pair is refused up front with
`CheckpointStorageCannotFenceException`. Two ways out:

1. Answer `true` from `evaluatesWriteConditions()` on a storage that does evaluate conditions, see section 2.
2. Set `occurrent.subscription.competing-consumer.fence-checkpoints=false` to keep a storage that cannot. Every
   checkpoint is then written unconditionally, which is what 0.32.0 did. A node that has lost its lease can move a
   checkpoint backwards, and the events between the two positions are delivered again, which stays inside the
   at-least-once contract.

The second way out does not apply under `occurrent.subscription.mode=manual`. There the starter builds a
`ManualStartSubscriptionModel` that records a subscription's first start position when it is registered, whenever the
position it was registered with resolves to the subscription model default. That write uses `ifAbsent()`, and it is
what keeps two nodes registering the same subscription from overwriting each other. The storage has to evaluate
`ifAbsent` whatever the fencing setting says, and the model throws
`IllegalArgumentException` naming the storage class during startup when it does not. Answer `true` from
`evaluatesWriteConditions()` on a storage that does evaluate it, or declare the subscription model bean yourself and
build it with the one-argument `ManualStartSubscriptionModel.stoppedByDefault(SubscriptionModel)`, which records no
position at all and lets a subscription's first run start from the moment you start it.

**A `CheckpointStorage` that evaluates write conditions overall but refuses one or more declared subscription ids,
wired next to a strategy.** `evaluatesWriteConditions()` answering `true` is not the last word once
`evaluatesWriteConditionsFor(String)` exists. `SpringRedisCheckpointStorage`'s Cluster-safe mode is exactly this
case, answering `true` overall while refusing the one subscription id shape in section 4 for a conditional write. Once
every singleton exists, the starter asks `evaluatesWriteConditionsFor` for the subscription ids
`CheckpointStorageCannotFenceSubscriptionException`'s own javadoc names precisely, and throws that exception naming
the storage and every refused id when the answer is `false` for at least one. That javadoc also says which ids are
left out even though the storage might refuse them, a `@SynchronousSubscription` among them since it never writes a
checkpoint at all, and which are asked about even though the storage refusing them would never matter.
Rename the affected id to a shape the storage accepts, use a storage that evaluates write
conditions for it (`forStandalone(..)` on `SpringRedisCheckpointStorage`, if the deployment allows it), or fall back
to `occurrent.subscription.competing-consumer.fence-checkpoints=false`. That third way out has the same limit under
`occurrent.subscription.mode=manual` as the second way out above. A first-run `ifAbsent()` write for the affected id
runs whatever this setting is, so disabling the fence only trades this exception's own message for a less specific
one from the storage itself, once that write is attempted.

None of the three above covers what manual mode can also do at startup, because this one is about timing rather than
wiring. Two nodes registering a brand new subscription at the same moment both read a start position and both try to
record it, only the first write is kept, and the node whose position was not kept now fails with
`StartPositionAlreadyPinnedException` instead of starting the subscription from a position it never read. The two
positions were read on different machines and nothing can order them, so accepting the stored one risks starting past
the events written between them.

Start that node again. It finds the recorded position already there, takes it, and starts, which is what a node
registering a subscription somebody else already registered has done since this write was added. The events between
the two positions are a separate question, and replaying that interval is the only way to get them, which is safe
while the subscription is not running anywhere. A subscription that has run before, or that one node registered
earlier, does not reach that race, because the position is already recorded before the second node asks about it.

One node on its own can hit the same exception with nobody else registering, and that one does not clear by starting
again. It happens when the checkpoint storage answers the question of whether a checkpoint exists, or reads one back,
from a replica that has not caught up with the write, so a `MongoTemplate` reading from a secondary can refuse a
registration for a subscription with real history. Restarting into the same lagging reader refuses it again. Point the
storage at a reader that has seen the write, or wait for the replica to catch up, and then start the node.

Telling the two apart is a matter of what the storage holds. A position recorded for this subscription that a primary
read confirms means the first case, and starting again is the whole of it. No such position, or one only some readers
can see, means the second.

**On the reactive stack the same refusal is not limited to the subscriptions under `manual`.**
`ReactorDurableSubscriptionModel` records a first start position the same conditional way now, and refuses a
registration that loses that write with the same `StartPositionAlreadyPinnedException`. The reactive stack has no
`ManualStartSubscriptionModel`, because its layer order never needed one, so this sits in the only durable model it
has. Any reactive subscription whose start position resolves to the subscription model default can be refused on its
very first pass, whatever `occurrent.subscription.mode` says. A registration that names a position of its own records
nothing and is never refused, so a `@Projection(startAt = StartPosition.BEGINNING_OF_TIME)` and anything else asking
for a replay is untouched. Everything above about the two causes and how to tell them apart applies unchanged.

Where it reaches you depends on what your durable model wraps. A wrapped model that manages named subscriptions of
its own means the exception comes straight out of `subscribe(..)`, and under Spring that fails the context refresh
the way the blocking one does. That is what the reactive Mongo starter builds, since `ReactorCatchupSubscriptionModel`
is such a model. Nothing is logged on that path, so the exception itself is all you get, which is enough because it
reaches whoever called `subscribe(..)`. A wrapped model offering only the plain subscription primitive means
`subscribe(..)` returns and the exception is signalled on the returned `Subscription.waitUntilStarted()` instead,
with an `ERROR` logged next to it, because there is no caller holding it at that point.
Call `waitUntilStarted()` if you want a refused subscription to stop your application from starting on that path,
because a refused one is dropped from the model rather than left registered, so it will not be running and
`resumeSubscription(..)` will not find it. Register it again to retry.

One more thing to watch out for if you wrote your own reactive `CheckpointStorage`. That first write uses `ifAbsent()`,
and a storage answering `false` from `evaluatesWriteConditionsFor(String)` gets the unconditional write it got in
0.32.0 instead, plus a `WARN` naming the class. Nothing fails, and the race stays open for that storage, because the
storage is what would have to evaluate the condition. Answer `true` from it once your storage really does evaluate
`ifAbsent`, see section 2. `ReactorCheckpointStorage` and the reactive in-memory storage both already do.

## 9. A flow saga can cap the events of the step it is parked in

Nothing here changes unless you ask for it, so you can skip this section if no flow saga of yours idles in one step.

`historyWindow` never limited the whole of a flow saga's retained state. It limits the carry-over behind the current
step's entry, and it is applied when a step is left, and it puts no limit at all on the events of the step being left. An
instance parked in one step while a large number of correlated events arrive therefore kept all of them, whatever
`historyWindow` was set to, `historyWindow(0)` included. The 0.31.0 entry that introduced `historyWindow` claimed the
retained state did not grow without bound, and that was only true for a flow whose steps turn over.

`stepWindow(int)` is the cap for the other half. It limits how many of the current step's own events are kept and is
applied on every delivery.

```java
FlowSaga.<OrderEvent, OrderCommand>builder()
        .historyWindow(20)
        .stepWindow(50)
        .startsOn(OrderPlaced.class)
```

Kotlin has `stepWindow(50)` in the `saga { }` block beside `historyWindow`.

An instance holds at most `historyWindow + 2 * stepWindow + 1` events at any one moment. The doubled `stepWindow` is
real rather than slack. A transition keeps the events of the step being left, so that step's reaction can read them,
and the step being entered then fills its own cap before anything is dropped.

**What a callback can read shrinks, and that is the whole cost.** A step condition is unaffected, because its counts
are kept in the instance's state rather than counted from the events, so a step completes on the same event it would
have without the cap. What reads less is everything that reads the received events directly:

- a guard, `on(Type.class, onlyIf, ...)`, so a retry guard counting `PaymentFailed` needs its threshold to fit inside
  the cap, the same requirement `historyWindow` already has
- a `timeout`'s `onExpiry`
- a window-condition reaction, `on(condition, then, whenFulfilled)`, which reads what is left of its step's window

Two things stay guaranteed at any cap of 1 or more. `received.initiating()` still reaches the start event, which is
kept as the first retained event and never counts against the cap, and the event that fired a branch is still the last
element of `received.asList()`.

### Name the predicate of any leaf in a capped step

A step's counts have to be matched back to the leaves that produced them after a restart or a redeploy, and a lambda is
a different object every time the class loads. So a window-condition leaf that carries a predicate needs a name for it, and
`build()` refuses a capped flow with a leaf whose predicate has no name, naming the step. A guard's `onlyIf` needs no name,
since a guard is checked against the arriving event rather than counted over a window.

```java
.step("review", step -> step
        .on(event(Payment.class, 2, "isBig", p -> p.amount() > 1000), next()))
```

Two leaves may share a name only when they hold the same predicate value. Two leaves over one event type sharing a name
while holding different predicates are refused when the saga is built, because nothing would then tell their counts
apart.

**Change the name whenever the predicate's meaning changes.** Keeping `"isBig"` while changing the test from
`amount() > 1000` to `amount() > 5000` is the one thing this cannot detect, and an instance parked in that step then
keeps counting the events it matched under the old test. Changing the name is what says the old counts no longer apply.
Changing the count a leaf asks for is always safe and needs no new name.

A flow without a `stepWindow` needs none of this. A predicate with no name costs nothing there, and such a step counts
its window on every delivery exactly as it always did.

### One failure mode comes with the cap

Once the cap has dropped a step's older events, nothing can rebuild that step's counts from scratch. So changing what a
capped step waits on, whether that is a leaf's event type or a predicate's name, makes every instance parked in that step
past its cap refuse its next delivery with an `IllegalStateException` naming the step. Retrying does not help. Put the
previous condition declaration back until the parked instances have moved on, or delete the instance. An instance still
inside the cap counts its window again and carries on, since its events are all still there.

[ADR 123](../architecture/decisions/0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md)
has the reasoning, including why this is a second setting rather than a new meaning for `historyWindow`.

## 10. A saga or subscription declaring a supertype event is refused

A saga declares the event types it handles, and so does an annotation-based subscription (`@Subscription`,
`@StreamSubscription`, `@SynchronousSubscription`, `@DcbSubscription`), but 0.32.0 turned those declarations into a type
filter differently on each side. A saga took its declared types verbatim, with no expansion at all, so declaring a
sealed supertype asked for that supertype's own CloudEvent type alone and missed every concrete type it permits. A
subscription expanded a sealed type into the concrete types it permits instead, but dropped the declared type itself,
the mirror problem, and treated a reopened level below it as a complete answer rather than an incomplete one.

0.33.0 expands a declared sealed type into the concrete types it permits, which fixes that case on both sides. Where the
concrete types cannot be found, a saga is refused when it is built and a subscription is refused when it is registered,
which for a Spring Boot application is startup. A saga sees this message:

```
java.lang.IllegalArgumentException: the concrete event types dispatch would accept for com.example.OrderEvent cannot all
be enumerated, so a filter derived from it would miss some of them. Declare the concrete event types instead, make
OrderEvent and every level below it final or sealed, or set a replacementFilter(...), which is used instead of deriving
one and is the way out when a CloudEventTypeMapper of your own maps the whole hierarchy onto a single CloudEvent type
string.
```

A subscription reports the same shape with a message of its own, naming the subscription id alongside the type:

```
java.lang.IllegalArgumentException: the concrete event types dispatch would accept for com.example.OrderEvent cannot all
be enumerated, so a filter derived from it for subscription 'order-subscription' would miss some of them. Declare the
concrete event types with the annotation's eventTypes attribute instead (for example eventTypes = {MyEvent1.class,
MyEvent2.class}), or make OrderEvent and every level below it final or sealed.
```

**For a saga, read this as a report about a saga that never worked, not as a regression.** Under every type mapper
Occurrent ships, the saga you are being told about was already receiving nothing, or already missing part of its
hierarchy, and it looked like a process still waiting for events rather than a defect. The exception is the first time
anything said so.

**For a subscription, only one of the four shapes below is new.** `SubscriptionAnnotations` already refused a non-sealed
interface, a non-sealed abstract class and an array with an `IllegalArgumentException` in 0.32.0, so a subscription
declaring any of those three shapes was already refused before this release, under an older message this release also
rewords. The third shape, a sealed hierarchy reopened below the declared type, is what changes in substance. 0.32.0
accepted it and built a filter
naming only the reopened level, so the concrete events stored beneath that level were silently missed rather than
delivered. 0.33.0 refuses it instead, the same new refusal a saga gets in this shape, so a subscription in this one shape
that started fine under 0.32.0 now refuses to start. That is the breaking part of this change for a subscription.

You are affected when a declared type is one of these:

| Shape | Java | Kotlin |
|---|---|---|
| An interface that is not sealed | `interface OrderEvent` | `interface OrderEvent` |
| An abstract class that is not sealed | `abstract class OrderEvent` | `abstract class OrderEvent` |
| A sealed hierarchy reopened below the declared type | `non-sealed class Base implements OrderEvent` | `open class Base : OrderEvent` or `abstract class Base : OrderEvent` |
| An array type | `OrderEvent[]` | `Array<OrderEvent>` |

The third row applies even when the declared type can be instantiated. A `sealed class` that is not abstract still claims
its subtypes are knowable, so a reopened level below it is refused just the same, and the events under that level are
exactly the ones that were going missing. For a subscription, the third row is the only one of the four that is new in
0.33.0, the first, second and fourth were already refused in 0.32.0.

The fourth row is refused for a different reason than the first three. This expansion does not support an array as a
declared event type at all, not because its concrete subtypes cannot be enumerated. Sealing or finalizing an array is
not a real option, so its message skips that remedy. A saga's message points at declaring the concrete event types. A
subscription's message cannot tell where the array came from, so it names both fixes, changing the handler method's own
event parameter to a concrete event type, or listing concrete event types in the `eventTypes` attribute if that is
where the array was declared instead.

A saga or a subscription that declares concrete types is unaffected, and so is one that declares a sealed type whose
every level is sealed or final. Java records and Kotlin data classes are final already, so an ordinary sealed hierarchy
of records needs nothing.

### Seal the hierarchy

The better remedy when you own the events, because the saga then keeps working when you add an event type. In Java, mark
the reopened level `sealed` and list what it permits:

```java
// Before, refused: Base reopens the hierarchy, so nothing below it can be found
public sealed interface OrderEvent permits Base { }
public non-sealed class Base implements OrderEvent { }

// After
public sealed interface OrderEvent permits Base { }
public sealed class Base implements OrderEvent permits OrderPlaced, PaymentReserved { }
```

In Kotlin, an `open class` or an `abstract class` in the middle becomes `sealed`:

```kotlin
sealed interface OrderEvent
sealed class Base : OrderEvent            // was open class or abstract class
data class OrderPlaced(val orderId: String) : Base()
```

The same fix applies to a subscription's declared event type. Seal the reopened level and its permitted subtypes stay
findable, whether the declared type is inferred from the annotated handler method's event parameter or set explicitly
through the annotation's `eventTypes` element.

### Or declare the concrete event types

Use this when the hierarchy is not yours to seal, or when it is deliberately open. Replace the supertype registration
with one per concrete type:

```java
// Before, refused
Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
        .correlateAll(OrderEvent::orderId)
        .startsOn(OrderEvent.class)
        .react(OrderEvent.class, (state, event) -> ...)
        .build();

// After
Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
        .correlateAll(OrderEvent::orderId)
        .startsOn(OrderPlaced.class)
        .react(OrderPlaced.class, (state, event) -> ...)
        .react(PaymentReserved.class, (state, event) -> ...)
        .build();
```

A flow step written as `step.on(OrderEvent.class, then)` or a step condition written as `event(OrderEvent.class, 2)`
changes the same way, to one `on(..)` per concrete type, or to `anyOf(event(OrderPlaced.class), event(PaymentReserved.class))`
when the step should fire on either.

One handler per type is more code than one on the supertype. If that matters, note that handler lookup still falls back
through superclasses and interfaces, so you can register the shared handler under a concrete type and delegate, or keep
one method and reference it from each registration.

For an annotation-based subscription, list the concrete types with the annotation's `eventTypes` attribute instead of
the declared supertype, for example `@Subscription(id = "order-subscription", eventTypes = {OrderPlaced.class,
PaymentReserved.class})`.

### Or, for a saga, set a replacement filter

New in 0.33.0, and the remedy to reach for when the hierarchy is genuinely open and you know what your events are stored
as. A saga can now say what it subscribes on instead of having it derived from its event types, so nothing is derived and
nothing is refused:

```java
Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
        .correlateAll(OrderEvent::orderId)
        .startsOn(OrderEvent.class)
        .react(OrderEvent.class, (state, event) -> ...)
        .replacementFilter(Filter.type("order-event"))
        .build();
```

`FlowSaga.Builder` has the same method, both Kotlin `saga { }` blocks expose it as `replacementFilter(...)`, and
`Saga.create(...)` takes one as a trailing argument. A subscription has no equivalent, so for `@Subscription` and its
siblings the `eventTypes` attribute above is still the answer.

Reach for this only when the hierarchy is the problem. If all you want is to select on subject, source, data or time
while keeping your declared event types, use `narrowingFilter(...)` instead, which is combined with the derived filter
rather than used in place of it, and which leaves the build-time check on. Both builders and both Kotlin blocks have it.
`Saga.create(...)` does not, and a saga it returns cannot be given one afterwards, since the factory hands back an
anonymous implementation. Implement `Saga` yourself instead of calling the factory, which is what its own javadoc
already tells you to do for `onStart` and `isTerminal`. One thing to know if you go that way. The build-time check that
refuses a type whose concrete types cannot be enumerated belongs to the two builders and the factory, so a saga you
implement yourself never runs it, and under the type mappers Occurrent ships a supertype in its `eventTypes()` leaves it
subscribing on a filter that misses events its own handlers would have taken. Declare the concrete types there, which is what a builder would have made you do.

Four things become yours to get right with a replacement, and the first two apply to a narrowing too. The filter has to
match the saga's start events, because one that excludes them means no instance is ever created. It also has to admit the
events that move an instance on, because an instance whose later events are excluded never reaches `isTerminal` and stays
alive with its timers running. Beyond those, a replacement has to stay inside what your `CloudEventConverter` can turn
into a domain event, since every CloudEvent it admits is converted before the saga sees it, and one that fails to convert
fails that delivery rather than being skipped. And the build-time hierarchy check is switched off for every event type
the saga declares, not only for the one you could not enumerate, so a replacement you set for an unrelated reason also
stops you being told about a sealed hierarchy that was reopened somewhere else in the same saga.

A saga that declares no event types and sets no replacement is the exception to the split above. Its derived filter
matches everything, so a narrowing on it is the whole selector, and the conversion point applies to that narrowing
exactly as it does to a replacement. Set a replacement as well and that replacement is the base instead, so this does
not arise.

A flow saga pays two more. The first belongs to a replacement, which makes it append every correlated event it receives
to the instance's retained history before it checks which branch handles the type, so a filter broader than the types
the flow names grows that history, and under a `stepWindow` cap those events take slots the step's own events would
otherwise hold. The second belongs to both and is the one that surprises people. A guard reads what has arrived, so a
selector that excludes an event type changes the answer `received.none(Rejected.class)` gives, and a branch can fire
that would not have fired otherwise. A narrowing can only remove matches, so that is the only
direction it can move in. A replacement can be broader or narrower than the flow's types, and does this when it is narrower.

One operational note for a saga that is already running. Adding a narrowing does not replay, so the events it would have
excluded before you added it are already in the instance's history. Removing one later resumes from the stored
checkpoint, and that checkpoint only moved for the events the saga actually received. So an excluded event that sits at
or before it stays skipped for good, while one that arrived after the last event the saga did receive is delivered when
it resumes.

### If you wrote a type mapper that collapses the hierarchy

One case genuinely worked in 0.32.0 and now throws, for a saga and for a subscription's reopened-hierarchy shape alike. A
`CloudEventTypeMapper` that maps every type in a hierarchy onto one CloudEvent type string makes a declared supertype
match, because the string the filter asks for is the string every event has. Occurrent cannot tell your mapper apart from
the default one at build time, which is why the refusal does not make an exception for it.

For a saga, `replacementFilter(Filter.type("order-event"))` is the direct answer, since it says the thing reflection could not work
out. Declaring the concrete types also keeps working, because under such a mapper they all map to the same string, and it
is the better choice when you can enumerate them. Reach for the filter when you cannot, which is the case a hierarchy
other people extend was always going to end up in.

A subscription has only the second of those. Its `eventTypes` attribute is the explicit list, the same one the concrete
types remedy above uses.

An annotation-based subscription's filter changes too, but not from the same mapper, and it needs nothing from you. It
used to derive its filter from the concrete types a sealed type permits and leave the declared type out. The collapsing
mapper above never had a gap from that, since its concrete types already mapped to the one string every event has. The
gap is real whenever the declared type is itself concrete, an event is stored as an instance of it directly, and the
mapper gives that instance a CloudEvent type none of the permitted concrete types share, true automatically under the
class-keyed mapper Occurrent ships, since the old filter never named that type's own CloudEvent type. The filter now
names the declared type too, and a subscription that only hit this gap keeps working, because another type in a filter
can only widen what matches.

### Why there is no recipe for this one

`UpgradeToOccurrent_0_33` does not touch this, and it cannot even flag it for review, which is worth explaining because
every other breaking change in this release gets recipe help.

Rewriting was never possible, since the concrete subtypes of an open hierarchy cannot be read off the declaration. A
review marker looked possible and is not. Deciding whether `startsOn(OrderEvent.class)` or a subscription's declared
event type is refused means knowing whether the type is sealed, and the type behind a class literal in another file does
not carry that. OpenRewrite has the flag in its model but does not populate it there, so a marker would have flagged
every sealed hierarchy as well, which is exactly the code this release fixes. Pointing you at correct code is worse than
pointing at nothing.

So this section is the migration path. `build()` throws with the type named the first time the saga is built, and a
subscription throws it the first time the subscription registers, which for a Spring Boot application is startup either
way, so a test that starts your context or builds your sagas finds all of them.

## 11. A lowered `join`'s reaction now reads its own window, not the whole retained history

`join`'s callback used to read every event the instance still keeps, the same as a guard or a `timeout`'s `onExpiry`
does. It now reads the retained events since the step it fired from was entered instead, the same window
`on(StepCondition, ...)` reads. That is every one of them by default, but a `stepWindow` cap can have evicted the
step's own oldest events by the time the reaction runs, so the callback then sees only what survived the cap. The
condition itself still fires on the same event either way, since [section 9](#9-a-flow-saga-can-cap-the-events-of-the-step-it-is-parked-in)
covers `stepWindow` carrying its counts forward rather than re-deriving them from what is still kept.
`received.initiating()` still reaches the start event no matter how many steps ago it arrived, or how tight the cap.

You are affected in two shapes, and the first-step one is easy to miss.

A `join` past a saga's first step no longer sees an earlier step's events at all, whatever their type. That
includes a repeat of one of its own expectation types. If an earlier step left behind an event of the exact type
this `join` is waiting for, the old callback counted it alongside the one that fired the `join`, and the new one
does not.

Java, before and after:

```java
FlowSaga.<GameEvent, GameCommand>builder()
        .startsOn(MatchStarted.class)
        .step("lobby", step -> step.on(PlayerJoined.class, Continuation.next()))
        .step("ready-check", step -> step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end(),
                received -> {
                    // Before: counted every PlayerJoined "lobby" left behind, plus this step's own, whatever the type.
                    // After: counts only events received since "ready-check" was entered, so this is 0 unless a
                    // PlayerJoined also arrives inside this step. The same drop applies to a repeated PlayerReady.
                    int lateJoiners = received.count(PlayerJoined.class);
                    return List.of(new StartMatch(lateJoiners));
                }));
```

A `join` in a saga's first step keeps whatever a `stepWindow` cap has left of its own events, since there is no
earlier step to lose anything to, but the window a reaction reads always starts after the initiating event, in
every step including the first. A first-step `join` whose callback reaches for the start type through `count`,
`all`, `first`, `any`, `none` or `asList` now sees nothing where it used to see one:

```java
FlowSaga.<GameEvent, GameCommand>builder()
        .startsOn(MatchStarted.class)
        .step("ready-check", step -> step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end(),
                received -> {
                    // Before: 1, the MatchStarted event was in the whole retained history the callback read.
                    // After: 0, the window starts after index 0 even in the first step. initiating() still
                    // returns it either way.
                    int starts = received.count(MatchStarted.class);
                    return List.of(new StartMatch(starts));
                }));
```

`received.initiating()` is the one accessor built to reach past the window, and it keeps returning the start event
in both shapes above. Condition evaluation is unaffected by either shape. A first-step `join` already counted only
post-start arrivals before this release, since a `join`'s condition has always counted since the step's own entry.

`join`'s deprecation javadoc said none of this could happen, that lowering it to `on(allOf(...))` changed nothing
about what the callback sees. That was false, and the javadoc is corrected in this release.

### Why there is no recipe for this one

`UpgradeToOccurrent_0_33` does not touch `StepBuilder`, `saga.flow` or `Expectation` at all, and it cannot flag this
either. A behavioural change to what a callback reads is not something a rewrite can see. `step.join(...)` and the
code inside `whenFulfilled` look identical before and after this release, so there is no syntax to match against.
Whether a given `join` is affected depends on what its callback actually reads and on what an earlier step in the
same saga left behind, both runtime facts a static rewrite has no way to evaluate.

So this section is the migration path. Search your codebase for every `StepBuilder.join` call (`.join(` in Java)
and every Kotlin `StepScope.join` call too, written as a bare `join(...)` with no receiver inside a `step { }`
block, and check whether `whenFulfilled` reads a type through a generic accessor rather than only through the
expectation that fired it. In a step past the first, that includes a repeated occurrence of the `join`'s own
expectation type. In the first step, it includes the saga's own start type. A test that drives the saga across a
step transition, and a test that fires a first-step `join` and asserts on its effects, both catch a real regression
the same way `FlowSagaTest`'s own
`join` tests now do.
