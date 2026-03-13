# 13. Namespace Kotlin typed execute filters under ExecuteFilters

Date: 2026-03-13

## Status

Accepted

## Context

Occurrent now supports application-service-level typed stream read filters through `ExecuteFilter`.

Java can express these filters cleanly through static factory methods on `ExecuteFilter`:

```java
ExecuteFilter.type(NameDefined.class)
ExecuteFilter.includeTypes(NameDefined.class, NameWasChanged.class)
ExecuteFilter.excludeTypes(NameDefined.class, NameWasChanged.class)
```

Kotlin needed an equivalent API. Several alternatives were considered:

- put the helpers on `ApplicationService`, for example `applicationService.excludeTypes(...)`
- keep top-level Kotlin helpers such as `excludeTypes<A, B>()`
- introduce a small Kotlin namespace dedicated to typed execute filters

The first option was rejected because it pollutes `ApplicationService` with filter-construction concerns. `ApplicationService` should execute commands, not act as a namespace for building filters.

The second option worked for single-type helpers such as `type<MyEvent>()`, but multi-type reified helpers such as `excludeTypes<A, B>()` were too weakly typed. In practice they returned `ExecuteFilter<Any>`, because Kotlin could not express a clean top-level API that both:

- infers a common event supertype safely
- keeps the compact `excludeTypes<A, B>()` call shape

That weakened the type contract and undercut the stricter Java API, which now uses `ExecuteFilter<? extends E>`.

`StreamReadFilter` itself must remain unchanged because it is part of the event store API and has no knowledge of `CloudEventConverter` or `CloudEventTypeGetter`.

## Decision

1. Keep Java typed filter construction on `ExecuteFilter`.
   - Java continues to use `ExecuteFilter.type(...)`, `includeTypes(...)`, and `excludeTypes(...)`.

2. Introduce a Kotlin namespace object named `ExecuteFilters`.
   - Kotlin typed filter helpers live under this namespace.
   - Recommended Kotlin usage is:

```kotlin
options().filter(ExecuteFilters.type<NameDefined>())
applicationService.executeSequence(
    streamId,
    ExecuteFilters.excludeTypes(NameWasChanged::class, NameDefined::class),
    fn
)
```

3. Use `KClass` varargs for multi-type Kotlin helpers.
   - `ExecuteFilters.includeTypes(vararg eventTypes: KClass<out E>)`
   - `ExecuteFilters.excludeTypes(vararg eventTypes: KClass<out E>)`
   - This keeps the API strongly typed without requiring `ApplicationService` to carry the helper methods.

4. Deprecate the earlier top-level Kotlin typed filter helpers.
   - Deprecate top-level `type(...)`, `includeTypes(...)`, and `excludeTypes(...)` in favor of `ExecuteFilters`.
   - Deprecate the weakly typed reified multi-type helpers that returned `ExecuteFilter<Any>`.
   - Keep them temporarily as compatibility shims.

## Consequences

Positive:

- `ApplicationService` remains focused and does not become a namespace for filter construction.
- Kotlin gets a clear and discoverable namespace for typed execute filters.
- Multi-type Kotlin helpers become strongly typed by using `KClass` varargs.
- The Kotlin API better matches the stronger Java `ExecuteFilter<? extends E>` contract.
- `StreamReadFilter` stays independent from application-service-specific type resolution.

Negative:

- Kotlin users must write `ExecuteFilters.type<...>()` or import from `ExecuteFilters` instead of relying on the shortest possible top-level form.
- Existing Kotlin users of top-level typed filter helpers will see deprecations and need to migrate.
- Multi-type Kotlin helpers become slightly more explicit because they use `KClass` arguments instead of only reified type arguments.
