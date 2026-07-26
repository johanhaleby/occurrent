# 72. Store-neutral Spring Boot annotation modules

Date: 2026-07-26

## Status

Accepted. Breaking for 0.30.0 callers of `OccurrentProperties` and of the autoconfigure artifact coordinate, automated by the `org.occurrent.UpgradeToOccurrent_0_31` OpenRewrite recipe.

Amends [ADR 55](0055-uniform-occurrent-artifact-coordinate-naming.md), which assumed every Spring Boot module is qualified by a store.

## Context

All Spring Boot annotation registration lived inside the two MongoDB starters. `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@SynchronousSubscription`, `@Projection`, `@Snapshot` and `@Saga` were processed by bean-post-processors and per-annotation registrars in `org.occurrent.springboot.mongo.blocking` and `...reactor`, with the shared pieces in a module called `occurrent-mongodb-spring-boot-autoconfigure`.

None of that is MongoDB-specific in substance. The annotations describe subscriptions, projections, snapshots and sagas over contracts any store implements. A JDBC starter ([#410](https://github.com/johanhaleby/occurrent/issues/410)) therefore had two options, and only one was acceptable: duplicate the machinery, or move it somewhere that does not name a database.

Measuring the actual coupling before moving anything changed the shape of the work. Across the six blocking machinery files there are 31 distinct Occurrent package roots imported, of which exactly 2 name MongoDB. The blocking post-processor and its `SubscriptionAnnotationRegistrar` import nothing Mongo at all, and neither do three of the four reactive files. The `occurrent-mongodb-spring-boot-autoconfigure` module was misnamed rather than misplaced: its registration helpers and capability conditions are entirely store-neutral.

The release status matters too. Only 4 files existed in the blocking package at tag `occurrent-0.30.0`. Every registrar and both `StartPositionSupport` classes were created by [#383](https://github.com/johanhaleby/occurrent/issues/383) and are unreleased, and the two bean-post-processors, while released, were package-private. So the entire released surface affected by the move is the 8 public types in the old `mongo.common` package plus one artifact coordinate.

## Decision

Three modules under `framework/spring-boot-autoconfigure/`, split `common`, `blocking` and `reactor` the way `eventstore/api` and `dsl/snapshot-dsl` already are, because [ADR 62's companion decision in PR 386](0062-pluggable-projection-event-source.md) settled that a `common` module holds only stack-neutral types. Folding both stacks into one module would put reactor interfaces on a blocking-only user's classpath.

The `common` module is the renamed existing artifact, so the change adds two published artifacts rather than three.

| artifactId | Package |
|---|---|
| `occurrent-spring-boot-autoconfigure` | `org.occurrent.springboot.common` |
| `occurrent-blocking-spring-boot-autoconfigure` | `org.occurrent.springboot.blocking` |
| `occurrent-reactor-spring-boot-autoconfigure` | `org.occurrent.springboot.reactor` |

The store starters keep `OccurrentMongoAutoConfiguration`, `EnableOccurrent` and their reactive twins. That is the genuinely MongoDB-specific half.

### Store-neutral is not framework-neutral

These modules keep `spring-boot-autoconfigure` in their names on purpose. The registration machinery implements `BeanPostProcessor`, `ApplicationContextAware`, `SmartInitializingSingleton` and `DisposableBean`, and every registrar resolves collaborators through `ApplicationContext`. That is the mechanism, not incidental flavouring. What is being generalised here is which store, not which container.

The annotation types themselves are already framework-neutral and were left alone. They live in `occurrent-annotations`, whose only dependency is an optional jspecify, and `application/command-dispatch-annotation` and `application/service/dcb-annotation` already consume them without Spring. A Quarkus or Micronaut integration reuses the annotations and the DSLs and writes its own equivalent of the registrars.

### Store defaults arrive through capability seams

The zero-config defaults (a `@Projection` with no store bean falling back to a MongoDB collection, and the `@Snapshot` and `@Saga` equivalents) are the only places the machinery reached a store. Each becomes a small optional capability interface that the store starter implements and contributes as a bean, resolved with `getBeanProvider(...).getIfAvailable()`:

- `DefaultProjectionStoreProvider`, `DefaultSnapshotStoreProvider`, `DefaultSagaStateStoreProvider` in the blocking module
- `DefaultReactiveSnapshotStoreProvider` in the reactor module
- `StartupWorkaround` in `common`, for the eager-bean-creation workaround around [spring-framework#32904](https://github.com/spring-projects/spring-framework/issues/32904)

Separate interfaces rather than one multi-method type, following the small-capability rule in `AGENTS.md`. The asymmetry is real rather than an oversight: the reactor stack has no `@Saga`, and its projection registrar deliberately fails loud instead of defaulting, so it needs one seam where blocking needs three.

Storage conventions travel with the implementation. The `occurrent-snapshot-<id>` and `saga-<id>` collection names now live in the Mongo starter, not in the neutral module. When no provider is present the registrar fails loud naming the annotation id, rather than silently skipping the default.

### The capability question is asked by interface, not by concrete store

The reactive `StartPositionSupport` decided whether history replay was possible by resolving `ReactorMongoEventStore` and calling `writesPosition()`. The store-neutral `PositionOrderedReader` already declares that method, so the capability is reachable without the concrete type.

The rule is: ask the event store, not any reader that happens to be in the context. It lives in one place, `PositionOrderedEventStores.find(ApplicationContext)` in the reactor module, which is public precisely because it is a contract rather than a helper.

**Both the probe and the catch-up wiring must use it, and that is the load-bearing part.** A first attempt had the neutral probe answer "can history be replayed" through the capability while the store starter still composed its catch-up layer from the concrete `ReactorMongoEventStore`. Those two can disagree: a user-supplied reactive `EventStore` implementing `PositionOrderedReader` made the probe say yes while no `ReactorCatchupSubscriptionModel` was wired, so `startAt = BEGINNING` silently did not replay where it used to fail at startup. Fail-loud became fail-silent. `ReactiveCatchupLayerWiringTest` pins it, and reverting the wiring to the concrete lookup fails that test.

So the seam obligation on a store starter is not just "contribute a `PositionOrderedReader` event store". It is: if your store answers the capability, your catch-up layer must be wired from the same answer.

Resolution deliberately goes through bean *names* for `EventStore` and only then checks the `PositionOrderedReader` instance, so no unrelated reader bean is instantiated. Two candidates are handed to the container first, so `@Primary` and `@Fallback` still decide, and only a genuinely unresolvable pair throws, naming the beans. An earlier `getBeanProvider(...).stream()` version was wrong on all three counts: it force-created every matching bean while running mid-refresh, ignored `@Primary`, and silently took the first in registration order.

One behavior difference remains and is intended: a user-supplied reactive `EventStore` implementing `PositionOrderedReader` now reports replay as supported, where before only the Mongo store did. Such a store does write positions, and it now also gets the catch-up layer, so the capability and the wiring agree.

### The store-default seams use `@Fallback`, not `@ConditionalOnMissingBean`

`@EnableOccurrent` activates the autoconfiguration through a plain `@Import`, and this repository already documents that `@ConditionalOnMissingBean` can then evaluate before a user's bean is registered and let both through. That is why `TagGenerator` and `CloudEventTypeMapper` use `@Fallback`. The four `Default*Provider` beans do the same, for the same reason.

That makes two provider beans a legitimate state rather than an impossible one, so each resolution site catches `NoUniqueBeanDefinitionException` and reports the annotation, its id, the provider type and the candidate bean names. Without that, the carefully worded provider-absent message was unreachable on ambiguity and users got a bare Spring exception.

### Everything stays package-private except the bean names

The store starter needs one thing from the post-processor: the bean name of the synchronous subscription DSL. That is a constant, not a type, so it lives in `OccurrentBlockingBeanNames` / `OccurrentReactorBeanNames` and both post-processors, all four registrars and both `StartPositionSupport` classes stay package-private. The encapsulation #383 established survives intact.

The complete public surface of the two per-stack modules is therefore: the four `Default*Provider` seams, the two bean-name holders, the two `Occurrent*AnnotationConfiguration` classes a starter imports, and `PositionOrderedEventStores`. Every one of those is something a store starter has to name. Nothing else is reachable.

An earlier attempt made the post-processors public to carry that constant, reasoning that they were already part of the contract. That was more surface than the problem needed. A related worry, that a package-private post-processor declared by a `static @Bean` would lose its `DisposableBean` destroy callback and leak the saga timer poller, was tested and is unfounded: Spring resolves `DisposableBean` from the created instance, not from the declared return type. `AnnotationBeanPostProcessorDestroyCallbackTest` observes the callback directly rather than resting on that inference.

## Consequences

`OccurrentProperties` and the 7 other public types move package, and the autoconfigure artifact changes coordinates. Both are handled by `UpgradeToOccurrent_0_31` plus a section in `doc/migration/upgrading-to-0.31.0.md`.

This is the second coordinate rename for that artifact, after the 0.30 one. ADR 55 warned against exactly that, and the warning stands. What makes it acceptable is that 0.31.0 already forces a migration, so users perform one upgrade rather than two. A third rename would not be defensible, which is the point of moving to a name that no longer has to change when a second store arrives.

Property keys are unaffected. `OccurrentProperties` is `@ConfigurationProperties(prefix = "occurrent")`, a hard-coded prefix, so IDE completion and every `occurrent.*` key survive the move. Only the `sourceType` entries in the generated `spring-configuration-metadata.json` change.

**`OccurrentProperties` is store-neutral in code but not in configuration surface, and that is a known gap.** An earlier reading of this called it neutral apart from the `TimeRepresentation` import. That was derived from an import grep and was wrong. At least four of its keys describe MongoDB:

- `occurrent.event-store.collection`, defaulting to `events`, a collection rather than a table
- `occurrent.event-store.time-representation`, which is how Mongo stores a timestamp
- `occurrent.subscription.collection`, defaulting to `subscriptions`
- `occurrent.subscription.restart-on-change-stream-history-lost`, which is meaningless without a change stream

Splitting the class was still rejected, and for a stronger reason than tidiness: `occurrent.event-store.time-representation` and `occurrent.event-store.capabilities` share one prefix, so a split means either two beans binding one prefix or renaming released keys. Doing that on top of a package move and a coordinate rename in the same release is more churn than the problem justifies.

So a SQL starter inherits a properties bean with keys it must ignore. That is acceptable for #410, which needs the registration machinery rather than the property surface, but it is real and it is the next thing to fix in this area. It should be its own issue, sized against whatever a second store actually needs, rather than guessed at now.

The invariant worth keeping mechanical is therefore narrower than it first looked: `grep -rniE "mongo"` over the blocking and reactor modules' main sources and POMs must return nothing. The `common` module cannot pass that check today, and pretending otherwise would just teach the next reader to ignore a failing grep.

A store starter now owes four things: import the stack's annotation configuration, contribute whichever `Default*Provider` beans it can, contribute a `StartupWorkaround` if its template needs one, and wire its own event store. It owes no copy of the registration machinery, which was the point.
