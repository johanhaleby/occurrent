# 12. Avoid Kotlin extension name collisions with Java ApplicationService members

Date: 2026-03-13

## Status

Accepted

## Context

`ApplicationService` is a Java API whose primary execution contract is stream-based:

- `execute(String, Function<Stream<E>, Stream<E>>)`
- `execute(String, ExecuteOptions<E>, Function<Stream<E>, Stream<E>>)`

Occurrent also exposes Kotlin extensions that adapt the Java API to Kotlin collections:

- `Sequence<E>`
- `List<E>`

When Kotlin extension functions use the same name as Java members, Kotlin prefers the Java members during overload resolution. In practice this led to Kotlin code binding to the Java `Stream`-based overloads instead of the intended Kotlin `Sequence` or `List` extensions.

The failure mode was confusing:

- Kotlin users sometimes had to add explicit lambda parameter types such as `events: Sequence<GameEvent>`
- `MakeGuess.kt` and similar examples became harder to read
- attempts to hide or reshape the Java API for Kotlin introduced Java-facing complexity in `ExecuteOptions`

This project is still pre-1.0.0, but it is an open source library used by third parties. Backward incompatibility should still be minimized where possible.

## Decision

1. Keep `ExecuteOptions` as the only options type.
   - Do not introduce a Kotlin-only parallel options abstraction.

2. Keep the Java `ApplicationService.execute(...)` API unchanged.
   - Do not rename Java `execute` to `executeStream`.
   - Do not change Java `execute` semantics from `Stream` to `List`.
   - Do not add Java `executeList` in this change.

3. Introduce explicit Kotlin names for collection-based `ApplicationService` execution.
   - Add `executeSequence(...)`.
   - Add `executeList(...)`.
   - Deprecate the old Kotlin `execute(...)` extensions first instead of removing them immediately.

4. Keep `options()` and add top-level Kotlin convenience helpers.
   - Add top-level `options()`.
   - Add top-level `filter(...)`.
   - Add top-level `sideEffect(...)`.
   - These helpers return ordinary `ExecuteOptions<E>` and do not introduce new state or types.

5. Keep `sideEffect` terminology consistently.
   - Keep typed side effects named `sideEffect(...)`.
   - Rename collection-based one-argument Kotlin helpers to `sideEffectOnSequence(...)` and `sideEffectOnList(...)`.
   - Deprecate the old collection-based `sideEffect(...)` overloads first instead of breaking them immediately.

## Consequences

Positive:
- The Java API stays stable and unsurprising for third-party users.
- Kotlin users get explicit APIs that avoid accidental binding to Java `Stream`-based members.
- Common Kotlin examples become readable again without explicit event type arguments or explicit `Sequence<E>` lambda parameters.
- `options()`, `filter(...)`, and `sideEffect(...)` can be used directly in Kotlin without introducing a second options abstraction.
- Migration risk is reduced by using additive APIs plus deprecation before removal.

Negative:
- Kotlin users must learn the explicit names `executeSequence(...)` and `executeList(...)`.
- The Kotlin API becomes intentionally asymmetric with the Java API.
- There will be a transitional period where both old and new Kotlin names coexist.
- Collection-based side-effect helpers gain longer names to stay unambiguous next to the Java `Stream` API.
