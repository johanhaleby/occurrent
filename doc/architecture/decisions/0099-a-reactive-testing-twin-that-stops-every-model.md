# 99. A reactive testing twin, and deny-by-default that stops every model

Date: 2026-08-05

## Status

Accepted. Resolves #530.

## Context

`occurrent-testing-junit-jupiter-blocking` gives a blocking application deny-by-default subscriptions in tests. A
reactive application could not use it at all, so the `occurrent-testing-*` family would have debuted in 0.32.0 serving
one stack. #530 filed the twin, blocked on a reactor `IntrospectableSubscriptionModel` that did not exist. #395 landed
one (#537), amending ADR 89 to say so, which unblocked this.

Two things surfaced while building it that were not decided when the blocking extension shipped.

**A Spring context can hold two life-cycle bearing models.** The reactive starter registers a durable model as
`@Primary` and a `SynchronousSubscriptionModel` alongside it, so the existing single-model
`stoppedByDefault(SubscriptionModelLifeCycle)` stops only the primary, leaving synchronous subscriptions running
through every test. That gap exists on the blocking stack too, and both artifacts are unreleased, so it is free to fix
now rather than carried forward.

**Two neutral leaves means two ways to wire Spring.** `occurrent-testing-spring-boot`'s
`OccurrentTestingConfiguration` hard-required a blocking `SubscriptionModelLifeCycle` bean by type, so a purely
reactive context failed to start on `@EnableOccurrentTesting`.

## Decision

**Both extensions accept more than one model.** `stoppedByDefault(SubscriptionModelLifeCycle, SubscriptionModelLifeCycle...)`
on both stacks, plus a `List` overload for a caller already holding them, such as every life-cycle bean in a Spring
context. `beforeEach`/`afterEach` stop every model given. `start(id)` tries each model in turn, since none of the DSL
wrappers forward introspection to say up front which model owns an id, and reports every model's available ids if none
of them has it. `startAll()` unions the ids across every model, and refuses (naming that at least one model cannot
list) rather than silently under-reporting if any model in the list is not introspectable, a decision reused from
`deleteCheckpoints`. Signature widening rather than a breaking change: the artifact is unreleased, and every existing
single-model call site still compiles because a varargs tail can be empty.

**The reactive twin mirrors the blocking extension, with exactly two differences, both forced by the types.**
`Subscription.waitUntilStarted()` and `CheckpointStorage.delete(String)` return a `Mono` on the reactor stack rather
than blocking, and a JUnit `beforeEach` is synchronous, so the extension blocks on them itself rather than asking every
test to. And there is no reactive `DelegatingSubscriptionModel` to unwrap, so introspection is a plain `instanceof` on
the model handed in, through the reactor `IntrospectableSubscriptionModel`, rather than the blocking side's recursive
`of(..)`. Everything else, the ordering (stop, clear state, clear checkpoints, resume `alwaysStart`), `startAll`, and
composing with `OccurrentMongoFlush` through `clearingStateWith(Runnable)`, carries over unchanged.

**One Spring annotation serves both stacks, and mixed applications get both extensions.** `@EnableOccurrentTesting`
now imports an `ImportSelector` (`OccurrentTestingImportSelector`) that checks whether the blocking extension class,
the reactor one, or both, are on the classpath, and registers the matching configuration or configurations. Both
neutral leaves, and their respective subscription API modules, became optional dependencies of
`occurrent-testing-spring-boot`, the same classpath-probing pattern ADR 87 and ADR 95 use for an optional artifact:
adding the leaf is the opt-in. A blocking-only application never pulls in `reactor-core`, a reactive-only one never
pulls in the blocking subscription API, and an application using both stacks gets two extension beans, under distinct
names (`occurrentSubscriptionsExtension`, `occurrentReactorSubscriptionsExtension`) and distinct types, each stopping
only its own stack's models. Both configuration beans now collect every `SubscriptionModelLifeCycle` bean through
`ObjectProvider<..>.orderedStream()`, rather than injecting one by type, for the same every-model reason as above.

## Consequences

**This module's own tests stay container-free.** There is no reactive in-memory event store, so the reactive tests use
`SynchronousSubscriptionModel` as the delivery vehicle: it needs no store, dispatch is a direct method call, and it is
both life-cycle bearing and introspectable. Two instances stand in for the two-model case. The leaf lodges in the
container-free `misc` CI shard alongside the blocking one.

**A test that constructed the blocking extension by hand is unaffected.** The single-argument call remains a valid
call to the widened varargs factory, and `OccurrentTestingConfiguration`'s behaviour is unchanged for an application
with exactly one blocking `SubscriptionModelLifeCycle` bean, which is every application before this.

**#530 is closed.** The docs site gets a branch, stacked on the existing testing-chapter chain, held for release like
the others.

**Amended for 0.33.0: `IntrospectableSubscriptionModel` is renamed to `IntrospectableSubscriptions`, and
`DelegatingSubscriptionModel` to `SubscriptionModelWrapper`.** Neither capability changed. Both names moved
because neither interface ever extended `SubscriptionModel`.
