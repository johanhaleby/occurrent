# 54. List instead of Stream for event store writes

Date: 2026-07-08

## Status

Accepted

## Context

The blocking event store write API and the application service domain function were typed with
`java.util.stream.Stream`, and the Kotlin sugar mirrored that with `Sequence`. The original reason was a
possibility that a store might consume events lazily and insert them without materializing the whole batch.

That never happened, and it cannot happen for an append. An append is one bounded, atomic unit of work. Every
store implementation collected the incoming `Stream` on the first line of `write`, because the batch is
iterated more than once (build the documents, then insert) and because an optimistic-concurrency retry
re-reads the same events, and a `Stream` cannot be consumed twice. The DCB append API, added later, had
already settled on `List<CloudEvent>`. So the write surface was internally inconsistent: DCB took a `List`,
the classic stream API took a `Stream` that was immediately turned into a `List` anyway.

The cost of the `Stream` shape was paid everywhere and bought nothing. Callers hold a `List` (a decider
returns `List<E>`, a converter produces a list), so they wrote `.stream()` to call `write`, and the store
wrote `.toList()` to use it. A set of adapter types existed only to reconcile the shapes: `CommandConversion`
and `StreamCommandComposition` in Java, `ConversionExtensions` and an `executeSequence` family in Kotlin, plus
several `@JvmName` annotations whose only purpose was to keep a `List` overload from clashing on the JVM with
its `Stream` or `Sequence` twin.

## Decision

**The write side uses `List`, the read side stays lazy.** `EventStore.write(...)` takes `List<CloudEvent>`,
the application service domain function is `Function<List<E>, List<E>>`, its side effect is
`Consumer<List<E>>` (blocking) or `Function<List<E>, Mono<Void>>` (reactor), and
`CloudEventConverter.toCloudEvents` takes and returns a `List`. Query and read methods keep returning
`Stream<CloudEvent>` (blocking) and `Flux<CloudEvent>` (reactor), and `CloudEventConverter.toDomainEvents`
stays `Stream`, because a query result can be large or unbounded and laziness there is real.

`List` rather than `Collection` or `Iterable`, because the order of events within an append is meaningful and
`List` is the type that says so. It also gives `size()`, `isEmpty()`, and re-iteration for free, which the
stores and validation need and a single-use `Stream` cannot provide.

**The reactor synchronous domain function flips too.** The decision function that a decider expresses is
synchronous in both stacks; only the surrounding I/O is `Mono` and `Flux`. That function moves to
`Function<List<E>, List<E>>` on the reactor stack as well, so one `List`-based decider feeds both stacks
identically. The reactive I/O and the `write(Flux<CloudEvent>)` signatures are unchanged.

**The shape-bridging machinery is removed.** `CommandConversion`, `StreamCommandComposition`, the Kotlin
`ConversionExtensions`, the `executeSequence` and `sideEffectOnSequence` families, the `Sequence` command
overloads in the module DSL, and the `@JvmName` annotations that existed only for a `Stream`-or-`Sequence`
versus `List` clash are all gone. `ListCommandComposition` and `PartialFunctionApplication` remain and do the
real work. The Kotlin write extensions that took a `Sequence<CloudEvent>` are removed, since the Java
`write(List<CloudEvent>)` is directly callable from Kotlin with `listOf(...)`.

## Consequences

This is a breaking change to the stable, released stream API, not only to the unreleased DCB surface, so it
lands in the next major version. A caller that passed a `Stream` to `write`, or wrote a
`Function<Stream<E>, Stream<E>>` domain function, or used `CommandConversion` or `StreamCommandComposition`,
changes to the `List` form. In practice this deletes ceremony: the `.stream()` at the call site and the
`.toList()` in the store both disappear, and the adapter types are no longer needed because a decider's
`List<E>` shape now matches the API directly.

The read side is untouched, so queries, catch-up replay, and position-ordered reads keep their lazy `Stream`
and `Flux` semantics and their memory behavior. The in-memory store's post-write notification hook, and the
in-memory subscription model that consumes it, move from `Consumer<Stream<CloudEvent>>` to
`Consumer<List<CloudEvent>>` for the same reason as the write API, since they are always handed an
already-materialized batch.

One related cleanup is deliberately left out of this change. The `Sequence` and `Stream` overloads on the
view DSL's `evolve` helpers are a read-model fold concern rather than part of the write path, and collapsing
them to `List` is a separate, self-contained follow-up.
