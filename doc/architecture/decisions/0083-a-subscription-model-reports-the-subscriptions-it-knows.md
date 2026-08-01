# 83. A subscription model reports the subscriptions it knows

Date: 2026-07-31

## Status

Accepted. Resolves #482.

## Context

`SubscriptionModelLifeCycle` answers questions about one subscription at a time. `isRunning(String)` and
`isPaused(String)` both take an id, and nothing anywhere returns the set of ids a model knows about.

That gap has two visible costs, both introduced by ADR 82's testing modules.

`OccurrentSubscriptionsExtension.start("odrers")` on a typo cannot say what the real ids are. It catches
the `IllegalArgumentException` from `resumeSubscription` and appends the ids the extension has itself been
told about, which on a fresh extension is nothing, and on a typo is the same wrong id. The message is
worthless in exactly the case that produces it.

ADR 82 also removed `startAll()` before shipping. It could only have resumed ids already named through
`alwaysStart` or `start`, so on a fresh extension it would have done nothing and said nothing about it.
That is worse than not having the method, so it was cut, and the removal recorded as a known hole.

## Decision

**A subscription model reports its own ids, rather than the Spring annotation machinery reporting them.**
`OccurrentBlockingAnnotationBeanPostProcessor` and its reactive twin already collect every annotated id
into a private `registeredIds` set for duplicate detection, and exposing that was the obvious move. It is
the wrong one. It only covers annotation-registered subscriptions, misses anything registered by hand, and
is unavailable to `occurrent-testing-junit-jupiter`, which is deliberately framework-neutral. Every
concrete model already holds the ids in its own maps, so the model is both the more complete answer and
the more portable one.

**A separate capability interface, not a method on `SubscriptionModelLifeCycle`.**

```java
public interface IntrospectableSubscriptionModel {
    Set<String> subscriptionIds();
}
```

Adding an abstract method to `SubscriptionModelLifeCycle` breaks every implementation outside this
repository, and a `default` has no honest body: returning an empty set is indistinguishable from a model
with no subscriptions, which is the silent-wrongness this ADR exists to remove. A capability composed in
alongside the others matches AGENTS.md's "small capability interfaces composed together" and the existing
`CheckpointAwareSubscriptionModel` / `CancellableSubscriptions` / `DelegatingSubscriptionModel` shape.

It does **not** extend `SubscriptionModel`, unlike `CheckpointAwareSubscriptionModel`. The testing
extension holds a `SubscriptionModelLifeCycle`, not a `SubscriptionModel`, and a standalone capability
composes with either. `DelegatingSubscriptionModel` is standalone for the same reason.

**A wrapper implements it only when it knows ids its delegate does not.** The interface ships a static
`of(Object)` that unwraps a `DelegatingSubscriptionModel` chain until it finds an introspectable model,
which is what `getDelegatedSubscriptionModelRecursively()` already exists for ("mainly for testing
purposes, since it may support more features than the `DelegatingSubscriptionModel` instance"). That
covers `DurableSubscriptionModel`, `CatchupSubscriptionModel` and the delegating adapters, none of which
hold ids of their own, so none of them are touched.

`CompetingConsumerSubscriptionModel` is the exception and it is not optional. A competing consumer that
has not won the lock sits in its `Waiting` state, and `delegate.subscribe(...)` is only called once
`startWaitingConsumer` runs. Until then the id exists only in the wrapper. Unwrapping past it would
under-report in precisely the deployment where it matters, since the Spring Boot starter's
`SubscriptionModel` bean is a competing-consumer model wrapping a durable one wrapping the Mongo model. It
therefore reports the union of its own consumers and its delegate's.

**`of(...)` returns an `Optional`, and the testing extension keeps the distinction.** "This model cannot
list its subscriptions" and "this model has no subscriptions" are different answers and are reported
differently. `startAll()` throws `IllegalStateException` naming the model class and pointing at
`start(String)` rather than silently starting nothing.

**`startAll()` ships.** It resumes every id the model reports that is currently paused, so an id another
part of the test already started is left alone. This is the "keep one test with everything running" advice
in the documentation, made convenient. The testing modules are unreleased, so it lands in the first
release rather than being added afterwards.

## Consequences

`IntrospectableSubscriptionModel` is published API from its first release, so the name is fixed once
`occurrent-subscription-api-blocking` ships it. Renaming later needs an `UpgradeToOccurrent_*` recipe.

The extension's `knownIds` set stays, demoted to a fallback for a model that cannot be introspected. Both
paths are covered by tests, including one built on a `SubscriptionModelLifeCycle` with no introspectable
model anywhere in it, so the fallback message cannot rot unnoticed.

There is no reactive twin. The testing modules are blocking-only, and adding
`org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel` with no caller would be
speculative. The reactor models hold the same maps, so the addition stays cheap when something needs it.

The subscription conformance suite planned in #395 should pin this alongside the paused-after-stop
guarantee ADR 82 added to `SubscriptionModelLifeCycle.stop()`. Both are contracts that currently hold
because every implementation happens to agree, which is what a TCK exists to convert into a promise.
