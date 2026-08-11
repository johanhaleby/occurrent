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
interface and the filter asks for the CloudEvent type of the interface, which no stored event has, so the subscription
silently misses every concrete type that interface covers. When the supertype is the only thing declared, that is every
event the saga wanted, and it receives nothing at all. Nothing refuses it and nothing warns.

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

An intermediate sealed interface is left out, because it cannot be instantiated and the caller never
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
`IllegalArgumentException` from `build()`. That covers a plain interface and an abstract class that is not sealed, and it
covers a sealed hierarchy that is reopened part way down.

**A level reopens the hierarchy when it is neither sealed nor final, whether or not it is abstract.** The abstract case
is the obvious one, a `non-sealed abstract class` in Java or any `abstract class` in Kotlin. The concrete case is easy to
miss and was missed in the first version of this change: `non-sealed class Base implements SealedEvent` in Java, or
`open class` in Kotlin, is stored under its own name, so the walk collected it and called the branch complete while
`class Special extends Base` stayed invisible. The filter then asked for `Base` and not `Special`, and a saga handling
`SealedEvent` silently missed every `Special`. That is the defect this ADR exists to remove, surviving one level down, so
the walk requires a level to be sealed or final before it calls a branch complete.

The refused failure is best described as silently missing stored event types rather than as matching nothing. Matching
nothing is only the degenerate case where the declared type is the sole entry. A reopened sealed hierarchy expands to the
types it can find, so a filter derived from it would have matched some events and missed others, which is harder to
notice than receiving nothing at all.

The message names the type and tells the caller to declare the concrete event types instead, or to make every level of
the hierarchy below it final or sealed. It names declaring the concrete types first because that is never refused, and it
is also the remedy under a mapper that collapses the hierarchy, since the concrete types map to the same string the
declared one did.

### One expansion for the whole repository, not a second copy

This expansion already existed. `SubscriptionAnnotations.getConcreteEventTypes` in
`framework/spring-boot-autoconfigure/common` recursed permitted subclasses and refused a non-sealed interface or abstract
type for `@Subscription`, `@Saga` and `@Projection`, and the first version of this change wrote the same algorithm a
second time in the saga DSL without finding it. `git grep getPermittedSubclasses` finds it in one command, and the
convention that says to run that grep is now in AGENTS.md.

**Comparing the two copies is what found the next defect, which is why they are diffed rather than one being deleted.**
The subscription copy expands a sealed type and drops the declared type, keeping only what it permits. So a subscription
never asks for the declared type's own CloudEvent type, and any event stored under that string is missed without a word.
With the mappers Occurrent ships nothing is stored under it, so nothing is missed today. Write a `CloudEventTypeMapper`
that maps a hierarchy onto the type string of the type it was declared with and `@Subscription(OrderEvent.class)` receives
nothing, which is this ADR's defect one module over. The saga version kept the declared type and did not have it.

So the two converge on one helper carrying the better choice from each side, rather than two behaviours kept because
touching the other caller was inconvenient:

| Behaviour | From | Why |
|---|---|---|
| Keep the declared type in the set | The saga version | Makes a collapsing custom mapper work, and fixes the subscription gap above. Another disjunct can only widen what matches. |
| Refuse an array type | The subscription version | An array is final and not an interface, so a check for interface and abstract alone lets it through, and no event is stored under it. |
| Throw `IllegalArgumentException` | The subscription version | A caller fixes this by passing different types, which AGENTS.md says is an argument exception. The first version of this change threw `IllegalStateException`. |
| The caller writes the message | Both | A saga and a subscription have different things to say, so the helper reports which type it could not expand and each caller formats and throws. |

`EventTypeExpansion` lives in `org.occurrent.filter.internal` in `occurrent-filter`, which both callers already depend on,
so nothing gains a dependency and no cycle appears. It is not a new module on purpose, because the repository's module
count makes Sonatype's 1000-file publishing limit the binding constraint and one class does not justify a new
publishable artifact.
Deriving which event types a filter has to name is what `occurrent-filter` is for.

`concreteTypesOf` exists beside `expand` because the subscription path needs the concrete types on their own. It checks
each one against the handler method's single parameter, and a declared supertype is not assignable to a narrower parameter
that its own concrete types are, so widening that check would have refused configurations that work. The filter is built
from `expand`, the check runs over `concreteTypesOf`, and no existing subscription starts failing.

### One check, both builders

`Saga.Builder.build()`, `Saga.create(..)` and `FlowSaga.Builder.build()` all call `expand`. All three routes in #743
reach the same `eventTypes()`, so one call at the point each builder produces that set covers the core builder,
`on(Class, ..)` and a window condition together. `SagaFilters` is unchanged, `startEventTypes()` is unchanged because it
already matches with `isInstance`, and `FlowSagaImpl` is unchanged because `eventTypes` there is only an accessor.

Correlation coverage is checked over the expanded set, which changes nothing and is worth writing down so nobody claims
otherwise. An expanded type is always a subtype of a declared type, `TypeDispatch` resolves a correlator through
superclasses and interfaces, and every declared type is checked before what it expanded into. So a correlator that covers
the declared type covers its expansions, and a declared type that has none already threw. A saga that correlates two of
three permitted types and declares their sealed supertype failed to build before this change and still does, naming the
supertype both times.

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
and `eventTypes()` would keep reporting the declared supertype on its own.

## Consequences

A saga that declares a sealed supertype now receives the events it always looked like it was waiting for. This is a fix
to behaviour the saga DSL shipped in 0.32.0, so it gets its own changelog entry saying so.

A saga that declares a supertype whose subtypes cannot be found stops building. Under the type mappers Occurrent ships
that caller was receiving nothing, or missing part of the hierarchy, so this moves them from silently broken to loudly
broken with the remedy in the message. It is a breaking runtime change against 0.32.0, so it gets a
`#### Breaking changes` entry and a section in `doc/migration/upgrading-to-0.33.0.md`.

**One caller is worse off, and the honest thing is to name them rather than claim the refusal only catches broken sagas.**
A custom `CloudEventTypeMapper` that maps a whole hierarchy onto one type string makes a declared open supertype work,
because the string the filter asks for is the string every event has. That saga built and ran in 0.32.0 and now throws.
Expansion was designed to keep it working, by keeping the declared type in the set, and the refusal fires before that
helps. There is no override yet, since `Saga` has no `filter()` (#751), so the remedies are to declare the concrete types,
which works under a collapsing mapper too because they all map to the same string, or to seal the hierarchy. How many
callers run such a mapper is unknowable from here, which is exactly why it is written down instead of dismissed.

**The projection DSL still behaves the other way, and that is recorded rather than left as an accident.** It has the
same derivation and the same supertype handler lookup, and it documents the constraint instead of fixing it. Two sibling
APIs that share a derivation now differ, so #750 records the divergence, the list of the other places with the same
derivation (`ProjectionFilters`, `SubscriptionFilters`, `DomainEventQueries`, and the subscription and snapshot
annotation registrars), and the reasoning for not widening a change the release is held for into four more modules on
the day of the tag. Anyone who reads the difference should find #750 before concluding one side is wrong.

A subscription's derived filter now also names the declared sealed type. That matters only for a custom
`CloudEventTypeMapper` that maps a hierarchy onto the type string of the type it was declared with, where such a
subscription received nothing before, so it gets its own changelog entry as a change to released behaviour.

**No OpenRewrite recipe helps with the refusal, and a review marker was tried and abandoned rather than skipped on
principle.** Rewriting was never possible, since the concrete subtypes of an open hierarchy cannot be read off the
declaration. Marking looked possible, by flagging a class literal handed to the saga DSL whose type is an interface or an
abstract class that is not sealed. OpenRewrite has `Flag.Sealed` in its type model but does not populate it for the type
behind a class literal, which a test proved by flagging a correctly sealed hierarchy, so the marker would have pointed at
exactly the code this release fixes. Telling a reader to change correct code is worse than telling them nothing, so the
recipe was removed and section 10 of the upgrade guide is the migration path instead. Deciding this needs the sealed
modifier from the class declaration, which means a scanning recipe holding state across files, and that is a bigger piece
of machinery than a marker is worth.

`EventTypeExpansion` uses nothing but reflection, so whichever module adopts this next can call it directly.
`ProjectionFilters` re-implements the subscription DSL's derivation on purpose rather than depend on that module, so the
projection side in #750 can copy it or call it, whichever fits when that work happens.
