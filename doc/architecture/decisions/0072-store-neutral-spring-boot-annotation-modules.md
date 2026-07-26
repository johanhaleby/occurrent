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

Resolve `PositionOrderedReader` and narrow to the candidate that is also an `EventStore`. Two details make that the right lookup rather than the obvious one. Resolving `EventStore` directly would be worse, because it is declared `@ConditionalOnMissingBean`, so a user-supplied store gives two candidates and `getIfAvailable()` throws. And resolving `PositionOrderedReader` without narrowing is ambiguous, because a `@Projection(source = PUSH)` application declares its own reader bean. Narrowing to the candidate that is also an `EventStore` reproduces the original "ask the store, not a feed" semantics store-neutrally.

This is the one part of the move that is not behavior-preserving: a user-supplied reactive `EventStore` implementing `PositionOrderedReader` now reports position replay as supported, where before only the Mongo store did. Such a store does write positions, so this is a fix.

### The post-processor becomes public, the registrars stay package-private

Each stack exposes exactly one public type, the bean-post-processor, which also carries the `SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME` constant the store starter needs. All four registrars and `StartPositionSupport` remain package-private, keeping the encapsulation #383 established.

The alternative was leaving the post-processor package-private and narrowing the `static @Bean` return type to `BeanPostProcessor`. That was tried and it works: Spring resolves `DisposableBean` from the created instance, not from the declared return type, so the destroy callback still fires. The concern that motivated the public type turned out to be unfounded.

Public is still the better choice, for a plainer reason. The store starter needs `SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME` from this class anyway, so the type is already part of the contract between the neutral module and a store starter. Making that explicit beats reaching a constant through a type the declaration pretends is something else.

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
