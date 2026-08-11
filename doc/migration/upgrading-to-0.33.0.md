# Upgrading to Occurrent 0.33.0

`CheckpointStorage` and its reactor twin gain a conditional write. This is a real break, and every implementation of
either interface, in this repository and outside it, now has two more members to answer. No calling code changes,
because the two-argument `save` you already call stays exactly as it was, as a default that delegates to the new
one. `UpgradeToOccurrent_0_33` stubs the two new members for you on a class it finds missing them, delegating `any()`
to your existing write and marking the rest with a review comment, so the module compiles again. Evaluating a
condition for real is still yours, see section 2.
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

Run `UpgradeToOccurrent_0_33` first. On every implementation it finds missing them, it adds the `save` overload and
`writeVersion`, each marked with a `TODO [Occurrent 0.33 upgrade]` comment, so the module compiles again without a
manual pass. What it generates is exactly the snippet below. `save` delegates `any()` to your existing two-argument
override and refuses anything stronger, and `writeVersion` answers empty. A store that only ever wrote
unconditionally can leave that exactly as generated:

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

Nothing breaks. `join` keeps working exactly as it did, and this section is only useful if you want to move off it,
or if you need something `join` cannot express in the first place.

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

## 8. Two ways the Spring Boot starter now refuses to start

Both of these used to start and go wrong later, so they are startup failures on purpose. Each message names the beans
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

## 9. A flow saga can cap the events of the step it is parked in

Nothing here changes unless you ask for it, so you can skip this section if no flow saga of yours idles in one step.

`historyWindow` never limited the whole of a flow saga's retained state. It limits the carry-over behind the current
step's entry, and it is applied when a step is left, so the step being left keeps every one of its own events. An
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

Kotlin has `stepWindow(50)` in the `saga { }` block beside `historyWindow`. Set both and the instance keeps at most
`historyWindow + stepWindow + 1` events, the last one being the initiating event, which is always kept.

**What a callback can read shrinks, and that is the whole cost.** A step condition is unaffected, because its counts are
carried in the instance's state rather than recounted from the events, so a step completes on the same event it would
have without the cap. What reads less is everything that reads the received events directly:

- a guard, `on(Type.class, onlyIf, ...)`, so a retry guard counting `PaymentFailed` needs its threshold to fit inside
  the cap, the same requirement `historyWindow` already has
- a `timeout`'s `onExpiry`
- a window-condition reaction, `on(condition, then, whenFulfilled)`, which reads what is left of its step's window

Two things stay guaranteed at any cap of 1 or more. `received.initiating()` still reaches the start event, and the event
that fired a branch is still the last element of `received.asList()`.

**One failure mode comes with the cap.** Once the cap has dropped a step's older events, nothing can rebuild that step's
counts from scratch, so changing which events a capped step waits on while instances are parked in that step makes those
instances refuse their next delivery with an `IllegalStateException` naming the step. Retrying does not help. Put the
previous condition declaration back until the parked instances have moved on, or delete the instance. Changing the count
a leaf asks for is safe, and so is changing a step no instance is currently parked in.

**A saga that cannot tell two of its leaves apart is refused at startup.** Two leaves over one event type that both
have a predicate, in the same step, look identical to the bookkeeping that keeps their counts, so `build()` throws
`IllegalStateException` naming the step rather than risk moving one leaf's count onto the other's predicate after a
redeploy reorders them. Ask for different counts on the two leaves, restructure the step, or leave that flow without a
`stepWindow`. Such a step works exactly as before when you do not set one.

[ADR 123](../architecture/decisions/0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md)
has the reasoning, including why this is a second setting rather than a new meaning for `historyWindow`.
