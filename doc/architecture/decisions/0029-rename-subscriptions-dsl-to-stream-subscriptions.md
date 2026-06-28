# 29. Rename the Subscriptions DSL to StreamSubscriptions

Date: 2026-06-28

## Status

Accepted

## Context

The stream and DCB sides of the subscription API are named explicitly everywhere now: `@StreamSubscription` and `@DcbSubscription` (ADR 0026, ADR 0027), `StreamSubscriptionModel` and `DcbSubscriptionModel` (ADR 0024), and the DCB DSL `DcbSubscriptions`. The one name left behind is the stream subscription DSL itself, the Kotlin class `Subscriptions` and its builder `subscriptions(...)`. Next to `DcbSubscriptions` it reads as if it covers all subscriptions rather than the stream ones, the same lopsidedness the annotation rename fixed.

The class is released, so it cannot be hard-renamed without breaking callers. It is also a Spring bean: the Mongo auto-config exposes it with `@Bean`, and the annotation processor looks it up with `getBean(Subscriptions.class)` from Java. A Kotlin `typealias` is a Kotlin-only compile-time construct, so it keeps Kotlin callers compiling but does nothing for Java references to the type.

## Decision

Make `StreamSubscriptions` the canonical name and keep `Subscriptions` as a deprecated alias.

- Rename the class `Subscriptions` to `StreamSubscriptions` and the builder `subscriptions(...)` to `streamSubscriptions(...)`. These are the names new code should use.
- Keep a deprecated `typealias Subscriptions<E> = StreamSubscriptions<E>` and a deprecated `subscriptions(...)` shim that delegates to the new builder, so existing Kotlin callers keep compiling with a deprecation warning that points at the new name.
- Migrate the two Java framework references (the auto-config bean and the processor's `getBean` lookup) directly to `StreamSubscriptions`, since the typealias does not reach Java. The bean is now of type `StreamSubscriptions`, and Kotlin callers that inject `Subscriptions<E>` resolve to the same type through the alias.
- Migrate the shipped library extensions in the view DSL to the new name. Example, test, and other module callers keep compiling through the alias and can migrate at leisure.

`EventMetadata` is deliberately left unchanged. It is the common metadata carrier that `DcbEventMetadata` already builds on, and `streamId`/`streamVersion` are present on DCB events too (a DCB event is stored in a partition stream), so a `StreamEventMetadata` view would re-expose what every event already carries and add no fields.

## Consequences

- The stream DSL name matches `@StreamSubscription`, `StreamSubscriptionModel`, and the DCB counterpart `DcbSubscriptions`. The pair is symmetric.
- Existing Kotlin code keeps working through the deprecated alias and shim. Java code that referenced the bean type is migrated, because the alias cannot help it.
- The deprecated alias and shim stay until the old name is removed, the same shape as the `@Subscription` deprecation.
