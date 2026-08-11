# 124. A saga expands a declared sealed event type

Date: 2026-08-11

## Status

Accepted. Fixes #743. Ships in 0.33.0, which is held for it.

123 was the highest number claimed anywhere at write time, on `cdx33/u11-stepcondition-counters`, which is unmerged.
The check covered every remote branch (47 of them) rather than `main` alone, because this repository has already had one
collision from a number claimed on a branch that never merged. The audit ran again at the moment this file was written,
and it had to, since 122 was the highest number when the work started and 123 was taken while it was in progress.

## Context

A saga declares the event types it handles, and `SagaFilters.filterFor` turns each one into a single
`Condition.eq(cloudEventConverter.getCloudEventType(type))`, combined with `or`
(`dsl/saga-dsl/blocking/src/main/java/org/occurrent/dsl/saga/blocking/SagaFilters.java:36-43`). Declare a sealed
interface and the filter asks for the CloudEvent type of the interface, which no stored event has, so the
subscription matches nothing and the saga receives no events at all. Nothing refuses it and nothing warns.

Issue #743 reported this from reading the code and marked it as not reproduced. It reproduces on all three routes into
`eventTypes()`. A core builder saga with `startsOn(OrderEvent.class)` never creates an instance, because its filter is
one equality on `OrderEvent`. A flow step written as `step.on(OrderEvent.class, ..)` gets its start event through and
then never reaches the step. A window condition written as `event(OrderEvent.class, 2)` behaves the same way. The same
sagas written against concrete types issue their commands, so the failure is the supertype and not the harness.

**Every layer below the filter already accepts a supertype, on purpose.** `TypeDispatch` resolves a handler through
superclasses and then interfaces and its javadoc says so (`internal/TypeDispatch.java:26-29`). Whether an event creates
an instance is decided by `startType.isInstance(event)` (`internal/SagaExecutionSupport.java:180`). A flow step's
branches, its window condition matchers and `ReceivedEvents` all match with `isInstance`
(`flow/FlowSagaImpl.java:152,184,417`, `flow/ReceivedEvents.java:130,141,152`). The filter derivation is the only place
in the saga DSL that compares type strings for equality.

That makes this an inconsistency inside the library rather than a mistake by the caller. A caller who declares a
supertype is using a capability the dispatch documents, and the subscription then withholds the events. So the fix is to
make the filter agree with the dispatch, not to refuse the declaration.

The constraint was already known one module over. `Projection.Builder.on` documents it and tells the caller to set an
explicit `filter(Filter)` when a handler is registered for a supertype
(`dsl/projection-dsl/common/src/main/java/org/occurrent/dsl/projection/Projection.java:259-262`), and `Projection` has
that override. `SagaFilters` says in its own javadoc that it mirrors the projection DSL's derivation, and it copied the
derivation without either the note or the override. The constraint travelled and the mitigation did not.

## Decision

### A sealed declared type is joined by the concrete types it permits

`EventTypeExpansion.expand(Set)` (`dsl/saga-dsl/common/src/main/java/org/occurrent/dsl/saga/internal/EventTypeExpansion.java`)
walks `getPermittedSubclasses()` transitively and returns the declared types plus every concrete type they permit. A
saga declaring a sealed `OrderEvent` reports `OrderEvent`, `OrderPlaced` and `PaymentReserved` from `eventTypes()`, and
its subscription asks for all three.

**Expansion only adds disjuncts to the filter, so it can never remove a match. That is why this is a fix and not a
heuristic.** The library cannot prove that a declared supertype is broken, because a custom `CloudEventTypeMapper` may
map a whole hierarchy onto one type string, in which case declaring the supertype already worked. Adding the concrete
types alongside it leaves that caller working and fixes everyone else.

The declared type stays in the set for the same reason. Dropping it in favour of the concrete types would break exactly
the caller whose mapper collapses the hierarchy.

An intermediate sealed interface is left out, because no event is ever stored under its name and the caller never
declared it. A sealed class that can be instantiated stays, because events do carry its name.

The filter shape does not change. A saga declaring several types already produced `Filter.type(Condition.or(eq, ..))`,
which both `SubscriptionFilterMatcher` and the MongoDB change stream already handle, so an expanded set asks nothing new
of any store.

### Iteration order follows the declared types

`expand` returns an insertion-ordered set rather than `Set.copyOf`, so the filter and any exception naming one of the
types come out the same on every run. This is not cosmetic. Correlation coverage is checked over the expanded set, and
with `Set.copyOf` the type named in that exception changed between JVM runs.

### A type that cannot be expanded is refused when the saga is built

A declared type that is never stored under its own name and whose concrete subtypes cannot all be found throws
`IllegalStateException` from `build()`. That covers a plain interface, an abstract class that is not sealed, and a
sealed hierarchy with a plain abstract class somewhere inside it, which Java allows through `non-sealed` and Kotlin
allows for any `abstract class`. The walk reports that branch as incomplete rather than stopping there quietly, since a
partial expansion would match some events and miss others, which is the same silence in a smaller place.

The message names the type and tells the caller to declare the concrete event types instead, or to make every level of
the hierarchy below it sealed. It names declaring the concrete types first because that always works, including under a
mapper that collapses the hierarchy.

### One check, both builders

`Saga.Builder.build()`, `Saga.create(..)` and `FlowSaga.Builder.build()` all call `expand`. All three routes in #743
reach the same `eventTypes()`, so one call at the point each builder produces that set covers the core builder,
`on(Class, ..)` and a window condition together. `SagaFilters` is unchanged, `startEventTypes()` is unchanged because it
already matches with `isInstance`, and `FlowSagaImpl` is unchanged because `eventTypes` there is only an accessor.

Expansion runs before correlation coverage is checked, so a concrete type a saga only receives through a declared sealed
supertype needs a correlation too. A flow saga that correlates two of three permitted types and declares their sealed
supertype now fails at build time, which is correct, because it will receive the third.

### The refusal is taught by the exception, not by five javadoc copies

Only `Saga#eventTypes()` gains a paragraph. The registration methods that can trip the refusal (`startsOn`, `evolve`,
`react`, `StepBuilder.on`, `StepCondition.event`) are left alone rather than each carrying a copy of the same fact,
which is the accretion this repository's docs have been bitten by before. The exception fires at build time with the
offending type named, which is where a caller meets it.

## Rejected alternatives

**Refuse every non-concrete declared type.** Loud, and it contradicts the dispatch layer, which documents supertype
handler lookup as a feature. It would also refuse the sealed hierarchy that `correlateAll`'s own javadoc calls "the
common case" (`Saga.java:408`), forcing every such saga to list its concrete types by hand for no benefit.

**Widen to `Filter.all()` when a declared type is not concrete.** Every event in the store then reaches the saga, and
`getDomainEventType` throws on events outside the saga's own hierarchy.

**Add `Saga#filter()` and document the constraint, matching the projection DSL.** This is what the projection DSL did,
and it is the docs-and-discipline answer this repository's own convention rejects for a library whose callers are
unknown. Recorded as #751 on its own merits, because an explicit filter is useful for more than this.

**Put the expansion in `SagaFilters`.** It lives in `dsl/saga-dsl/blocking`, so only the blocking runner would benefit,
and `eventTypes()` would keep reporting a set that matches nothing.

## Consequences

A saga that declares a sealed supertype now receives the events it always looked like it was waiting for. This is a fix
to behaviour the saga DSL shipped in 0.32.0, so it gets its own changelog entry saying so.

A saga that declares a supertype whose subtypes cannot be found stops building. That caller receives nothing today, so
this moves them from silently broken to loudly broken, with the remedy in the message. It is a behaviour change against
0.32.0 and it is the point of the change.

A flow saga that declares a sealed supertype and correlates only some of its permitted types now fails at build time
where it built before. The events it did not correlate for are the ones it will now receive.

**The projection DSL still behaves the other way, and that is recorded rather than left as an accident.** It has the
same derivation and the same supertype handler lookup, and it documents the constraint instead of fixing it. Two sibling
APIs that share a derivation now differ, so #750 records the divergence, the list of the other places with the same
derivation (`ProjectionFilters`, `SubscriptionFilters`, `DomainEventQueries`, and the subscription and snapshot
annotation registrars), and the reasoning for not widening a change the release is held for into four more modules on
the day of the tag. Anyone who reads the difference should find #750 before concluding one side is wrong.

`EventTypeExpansion` uses nothing but reflection, so whichever module adopts this next can call it or copy it.
`ProjectionFilters` already re-implements the subscription DSL's derivation on purpose rather than depend on that
module, so copying is the established choice.
