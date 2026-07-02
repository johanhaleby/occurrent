# 44. Reactive Spring Boot starter

Date: 2026-07-02

## Status

Accepted

## Context

Occurrent had one Spring Boot starter, `spring-boot-starter-mongodb`, which wires the blocking stack: a `SpringMongoEventStore`, a competing-consumer durable catch-up subscription model, the blocking DSLs, the blocking application service, and annotation-driven subscriptions through `@StreamSubscription` and `@DcbSubscription`. The reactive (Project Reactor) stack was complete at the library level (event store, application service, query DSLs, subscription model with resilience and lifecycle, durable and DCB catch-up wrappers, position storage) but had no auto-configuration, so a reactive application had to hand-wire every bean.

We want reactive applications to get the same one-dependency experience, including annotation-driven subscriptions, without regressing the blocking side or forcing the reactive stack onto blocking users.

Two shapes were considered for packaging. One starter with a runtime `occurrent.stack=blocking|reactive` flag, or two starters selected by which dependency and enable-annotation the application uses. A single flagged starter would force every user to pull in both stacks and both MongoDB drivers, and a runtime flag can disagree with what is actually on the classpath. Spring Boot itself uses two separate starters for MongoDB, `spring-boot-starter-data-mongodb` and `spring-boot-starter-data-mongodb-reactive`, gated by which types are present.

The reactive stack also does not mirror the blocking one one-to-one below the DSL line. There is no reactive competing-consumer model and no reactive stream (non-DCB) catch-up model. The reactive `PositionAwareSubscriptionModel` does not extend `Subscribable` or the lifecycle interface the way the blocking hierarchy does.

## Decision

Add a second starter, `spring-boot-starter-mongodb-reactive`, alongside the blocking one. Each starter is opt-in through its own enable annotation (`@EnableOccurrent` for blocking, `@EnableOccurrentReactive` for reactive) and gated with `@ConditionalOnClass` on its own stack's types, so the choice follows the dependency the application adds rather than a runtime property. Both can sit on one classpath because the bean types are disjoint.

Extract the stack-neutral autoconfiguration pieces (`OccurrentProperties`, the Jackson3 `CloudEventConverter` configuration, the `CloudEventTypeMapper` fallback, and the capability `Condition` classes) into a shared `spring-boot-autoconfigure-mongodb-common` module that both starters depend on. A single shared `@ConfigurationProperties(prefix = "occurrent")` class also means both starters can coexist without a duplicate-bean clash.

Wire the reactive subscription model as `Durable(Catchup(mongo))`, with the durable model on the outside as the `Subscribable` and lifecycle authority, and a DCB catch-up layer added only when the DCB capability is enabled and a reactive `DcbEventStore` is present. There is no competing-consumer layer.

The reactive annotation processor mirrors the blocking one, with handlers returning `Mono<Void>`. A `@StreamSubscription` that asks to replay history (a start time or `BEGINNING_OF_TIME`) fails loud, because there is no reactive stream catch-up model. `NOW` and `DEFAULT` are supported for stream subscriptions, giving live delivery plus durable resume. `@DcbSubscription` replays history by dcbposition through the reactive DCB catch-up model, matching the blocking behavior.

## Consequences

Reactive applications get auto-configuration and annotation-driven subscriptions with the same properties and ergonomics as blocking ones. Blocking users are unaffected and never pull in reactive modules.

The reactive starter is not a byte-for-byte parity of the blocking one. It has no competing-consumer support, and stream subscriptions cannot replay history. These follow from missing reactive infrastructure rather than a wiring choice, and are surfaced as a loud failure rather than a silent behavior difference. Adding a reactive competing-consumer model or a reactive stream catch-up model would lift these limitations later.

`OccurrentProperties` moved package from `org.occurrent.springboot.mongo.blocking` to `org.occurrent.springboot.mongo.common`. It is an internal configuration holder rather than a documented API, and the `occurrent.*` property keys are unchanged.
