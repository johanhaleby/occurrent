# 29. Rename the Subscriptions DSL to StreamSubscriptions

Date: 2026-06-28

## Status

Accepted

## Context

The stream and DCB sides of the subscription API are named explicitly everywhere now: `@StreamSubscription` and `@DcbSubscription` (ADR 0026, ADR 0027), `StreamSubscriptionModel` and `DcbSubscriptionModel` (ADR 0024), and the DCB DSL `DcbSubscriptions`. The one name left behind is the stream subscription DSL itself, the Kotlin class `Subscriptions` and its builder `subscriptions(...)`. Next to `DcbSubscriptions` it reads as if it covers every subscription rather than the stream ones, the same lopsidedness the annotation rename fixed.

`Subscriptions` is released (occurrent 0.20.4) and is a Spring bean: the Mongo auto-config exposes it with `@Bean`, the annotation processor looks it up with `getBean`, and callers inject it. So the rename has to preserve Java, source, and binary compatibility.

A Kotlin `typealias` is not enough. It is a Kotlin-source-only construct: it keeps Kotlin callers compiling but disappears at the bytecode level, so the class is gone for Java callers and for already-compiled binaries. The released `Subscriptions` type has to keep existing as a real class.

## Decision

Make `StreamSubscriptions` the canonical class and keep `Subscriptions` as a real deprecated subclass.

- The canonical class is `open class StreamSubscriptions<E>` and holds all of the DSL logic unchanged. It is `open` so the deprecated type can extend it.
- `Subscriptions<E>` becomes a `@Deprecated` subclass of `StreamSubscriptions<E>` with an empty body whose constructor delegates to `super`. A subclass keeps the released type present in the bytecode, so Java references, compiled callers, and source callers all keep working, which a typealias cannot do.
- The canonical builder is `streamSubscriptions(...)`. The released `subscriptions(...)` builder keeps its exact signature (a `Subscriptions<E>` receiver), deprecated and delegating, for source and binary compatibility.
- The auto-config `@Bean` keeps returning a `Subscriptions` instance with its released type, so `getBean(Subscriptions.class)` and existing `@Autowired Subscriptions` keep resolving. Because that instance is-a `StreamSubscriptions`, new code that injects `StreamSubscriptions` resolves to the same bean, and the processor looks the bean up by the canonical type.
- The view DSL `updateView` extensions move their receiver to `StreamSubscriptions<E>`. They are `inline`, so there is no compiled method symbol for old callers to bind to, and an extension on the parent applies to the `Subscriptions` subtype, so this is source compatible with no binary gap.

`EventMetadata` is deliberately left unchanged. It is the common metadata carrier that `DcbEventMetadata` already builds on, and `streamId`/`streamVersion` are present on DCB events too, so a `StreamEventMetadata` view would re-expose what every event already carries and add no fields.

## Consequences

- The stream DSL name matches `@StreamSubscription`, `StreamSubscriptionModel`, and the DCB counterpart `DcbSubscriptions`. The pair is symmetric.
- Existing Java, Kotlin, and compiled callers keep working, because `Subscriptions` is still a real class, now deprecated, that extends the canonical one.
- The deprecated subclass and builder stay until the old name is removed, the same shape as the `@Subscription` deprecation.
