# 66. Kotlin-first DCB criteria construction with reified type selection

Date: 2026-07-19

## Status

Accepted

## Context

`DcbCriteriaBuilder` builds a `DcbCriteria` from domain event classes instead of raw CloudEvent type strings, resolving each class to its type string through the configured `CloudEventTypeMapper`/`CloudEventConverter` (ADR 47). It shipped in 0.30.0 as a Java class, with one Kotlin convenience, the extension `typeOf<T, E>()`.

From Kotlin the surface was clumsy. Every class needed `::class.java`. The one reified helper forced the caller to also spell the base event type, `typeOf<NameDefined, DomainEvent>()`, because an extension on a Java class turns the base type into a function type parameter, and Kotlin requires all-or-none explicit type arguments. Selecting several types fell back to the Java `Class` varargs. And there was no way to reuse a shared tag boundary (a `DcbCriterion`) and give it query-specific event types without re-spelling the tags inline, since only the builder holds the converter that maps a class to its type string.

The subscription DSL already solved the reified problem: `Subscriptions<E>.subscribe<reified E1 : E, reified E2 : E>(...)` infers the base type because `subscribe` is a member of a Kotlin generic class, so the base `E` is the class type parameter and the function's own type parameters are just the event types.

## Decision

Convert `DcbCriteriaBuilder` from Java to Kotlin and make `type`/`types` reified members, so the base event type is inferred from the builder the same way `Subscriptions.subscribe` infers it.

- Reified members, base inferred: `type<A>()`, `types<A, B>()`, `types<A, B, C>()`. Beyond three types, use the `KClass` form.
- `KClass` forms with a required first argument: `type(A::class)` and `types(first: KClass<out E>, vararg rest: KClass<out E>)`, mirroring the Java `types(first, rest...)` shape and handling any number of types.
- The `typeOf<T, E>()` extension is renamed to the member `type<A>()`. A `@Deprecated(ReplaceWith("type<A>()"))` shim stays one release.
- Seed the builder with a boundary criterion through `criteria(boundary: DcbCriterion)` on `DcbDomainEventQueries` and `DcbSubscriptions`. A seeded builder's `type`/`types`/`tags` refine the boundary (setting their own dimension, keeping the boundary's other dimensions), which reuses a shared tag boundary and adds the events a given query or subscription cares about. `all`, `anyOf`, and `tagsAnyOf` contradict a single boundary and throw when the builder is seeded.
- The Java-facing surface is unchanged in behavior and JVM signatures: the constructors and the `Class`-based `type`/`types`/`tags`/`tagsAnyOf`/`all`/`anyOf` members keep identical erasures, so existing Java callers compile and run unchanged.

The boundary parameter is typed `DcbCriterion`, not `DcbCriteria`. A `DcbCriterion` is a single alternative (types any-of, tags all-of), so refining it with more types stays within one criterion. A general `DcbCriteria` can be an `anyOf` tree or match-all, and distributing types across alternatives would be an AND across alternatives, which the OR-of-items model rejects (ADR 32). Restricting the parameter to `DcbCriterion` makes that impossible at compile time, so ADR 32 needs no amendment.

The two reified `types()` overloads (arity two and three) erase to the same JVM signature, so each carries a `@JvmName` (`types2`, `types3`) to disambiguate the bytecode. These names are never seen by callers, who write `types<A, B>()`, and they are the same technique `Subscriptions.subscribe` uses with `@JvmName("subscribeAnyOf")`.

ADR 12 (Kotlin extension names must not collide with Java members) does not apply here, because `type`/`types` are now overloaded members on one Kotlin class rather than extensions competing with Java members, and the reified and `KClass` forms have distinct signatures from the `Class`-based members.

## Consequences

- Kotlin criteria construction reads `criteria().type<NameDefined>()`, `criteria().types<A, B>()`, or `criteria().types(A::class, B::class)`, with the base event type inferred. The course-enrollment example moves to `criteria().types<StudentEnrolledInCourse, StudentUnenrolledFromCourse>()`.
- `DcbCriteriaBuilder` is now a Kotlin class. Java callers are unaffected and are covered by the existing Java tests. `typeOf` is deprecated and will be removed a release later. There is no OpenRewrite recipe for the rename, because a `ChangeMethodName` cannot drop a type-argument, and the deprecation shim carries the IDE replacement.
- The semantic model, matcher, and server-side conversion are untouched. ADR 32 (no boolean tree) and ADR 47 (tag and typed-class construction) stand as they are.
