# 124. A saga expands a declared sealed event type

Date: 2026-08-11

## Status

Accepted. Fixes #743. Ships in 0.33.0, which is held for it.

Superseded on the exemption only by [ADR 126](0126-every-derived-event-type-filter-expands-a-declared-sealed-type.md),
which is where the reversal is recorded, together with why. The paragraph below titled "The one
deliberate exemption" describes 0.33.0 and no later release. A non-sealed concrete class declared directly is
refused from 0.34.0, resolving [#753](https://github.com/johanhaleby/occurrent/issues/753), so the rule this ADR
states now holds with nothing exempt from it. Two counts below went with the exemption. The property test runs
fifteen declared-type shapes rather than thirteen, and its outcome for the exempted case is gone, so it has two
outcomes rather than three. The decision itself is otherwise unchanged. See the changelog's breaking
changes and section 3 of
[the 0.34.0 upgrade guide](../../migration/upgrading-to-0.34.0.md#3-declaring-an-event-type-whose-concrete-subtypes-cannot-be-found-is-refused).

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

### The property, stated once

**The derived filter must name every event type that dispatch would accept.**

There is exactly one deliberate exemption, named at the end of this section, and one case nobody has checked, also named
there. Everything below follows from that sentence, and it is written here because it was not written down soon enough. Three
variants of the same defect were found one at a time, each by review rather than by design, and each fix revealed the
next. First the declared supertype itself. Then a non-sealed concrete class inside a sealed hierarchy, whose branch was
called complete. Then an instantiable sealed root, whose incomplete hierarchy was excused because the root itself could be
stored. Approximating the property case by case is what produced that sequence, so it is now stated positively and tested
as a property rather than as a list of shapes.

`EventTypeExpansionTest.the_filter_names_every_type_dispatch_would_accept` is that test. It runs 13 declared-type shapes,
a record, a sealed interface, a nested sealed interface, an instantiable sealed class, a diamond, a plain interface, an
abstract class, three kinds of reopened hierarchy, an array, and a non-sealed concrete class, and for each asserts that
expansion either names every concrete type in the fixture that an `isInstance` dispatch would accept, or refuses. Adding a
hierarchy shape to the fixture adds a row, which is what should catch the fourth variant before a release rather than
after one.

**The one deliberate exemption.** A non-sealed concrete class declared directly is accepted, and its subclasses cannot be
found, so dispatch accepts events the filter does not name. This is pre-existing behaviour and the exemption preserves it
on purpose, because refusing it would refuse every saga and subscription that declares an event class which is not final,
a much larger break than the defect being fixed. The repository's own convention of records for domain events makes it
rare, since a record is implicitly final, and Kotlin data classes are final too. The property test asserts this case as
its own outcome rather than letting it pass as though nothing were missing, so it is visible in the test output instead of
hiding in it. #753 tracks closing it.

**The case nobody has checked.** `Class.getPermittedSubclasses()` is documented to omit a permitted subclass it cannot
resolve, and no test here covers a sealed hierarchy with a permitted subclass missing from the runtime classpath. If it
returns a short list rather than failing, the walk would call that hierarchy complete and derive a filter missing those
types. A class absent at runtime cannot be dispatched to either, so the two probably stay consistent, but that is
reasoning rather than evidence and it is recorded as unverified rather than claimed.

### A sealed declared type is joined by the concrete types it permits

`EventTypeExpansion.expand(Set)` (`common/filter/src/main/java/org/occurrent/filter/internal/EventTypeExpansion.java`, it did not stay in the saga module, see below)
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
declared it. A sealed class that can be instantiated stays in the set, because events do carry its name, and its
hierarchy still has to be complete for it to be accepted at all.

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

**Whether the declared root can be stored says nothing about the hierarchy below it, which was the third variant.** A
sealed class that can be instantiated, `sealed class Base permits Done, Reopened` where `Base` is concrete, used to have
its incomplete hierarchy excused because `Base` itself names real events. It does, and the concrete types under
`Reopened` still went missing while dispatch accepted them. A sealed declaration is a claim that the subtypes are
knowable, so an incomplete hierarchy under a sealed root is refused whether or not the root can be stored. The exemption
is narrowed to a non-sealed concrete class declared directly, which is the compatibility case and nothing more.

The refused failure is best described as silently missing stored event types rather than as matching nothing. Matching
nothing is only the degenerate case where the declared type is the sole entry. A reopened sealed hierarchy expands to the
types it can find, so a filter derived from it would have matched some events and missed others, which is harder to
notice than receiving nothing at all.

The message names the type and tells the caller to declare the concrete event types instead, or to make every level of
the hierarchy below it final or sealed. It names declaring the concrete types first because that is never refused, and it
is also the remedy under a mapper that collapses the hierarchy, since the concrete types map to the same string the
declared one did.

> **Amended for 0.33.0, under [#751](https://github.com/johanhaleby/occurrent/issues/751).** The message now names a
> third remedy, an explicit `filter(...)` on the builder, which replaces the derived filter and so removes the reason to
> refuse anything. Declaring the concrete types is still named first, because it is the answer whenever the types can be
> enumerated. The array message is unchanged, since sealing an array is impossible and keeping an array as a declared
> event type is not worth pointing anyone at.
>
> **Amended again on 2026-08-14.** That method is now called `replacementFilter(...)`, and the message names it by that
> name. The split is in the 2026-08-14 section at the end of this document.

### One expansion for the whole repository, not a second copy

This expansion already existed. `SubscriptionAnnotations.getConcreteEventTypes` in
`framework/spring-boot-autoconfigure/common` recursed permitted subclasses and refused a non-sealed interface or abstract
type for `@Subscription`, `@StreamSubscription`, `@SynchronousSubscription` and `@DcbSubscription`, and the first version
of this change wrote the same algorithm a second time in the saga DSL without finding it. `git grep getPermittedSubclasses` finds it in one command, and the
convention that says to run that grep is now in AGENTS.md.

**Comparing the two copies is what found the next defect, which is why they are diffed rather than one being deleted.**
The subscription copy expands a sealed type and drops the declared type, keeping only what it permits. So a subscription
never asks for the declared type's own CloudEvent type, and any event stored under that string is missed without a word.
That is a real gap whenever the declared type is itself concrete, an event is stored as an instance of it directly, and
the mapper gives that instance a CloudEvent type none of the permitted concrete types share, which the class-keyed
mapper Occurrent ships does automatically, no custom mapper needed.
`@Subscription(OrderEvent.class)` on a concrete sealed `OrderEvent` receives every permitted concrete type but never an
`OrderEvent` stored on its own, which is this ADR's defect one module over. The saga version kept the declared type and
did not have it.

So the two converge on one helper carrying the better choice from each side, rather than two behaviours kept because
touching the other caller was inconvenient:

| Behaviour | From | Why |
|---|---|---|
| Keep the declared type in the set | The saga version | Fixes the subscription gap above, a concrete declared type whose own instances are stored had no CloudEvent type of its own in the filter. Another disjunct can only widen what matches. |
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

> **Amended for 0.33.0, under [#751](https://github.com/johanhaleby/occurrent/issues/751).** All three still walk the
> hierarchy, but there are now two entry points and only one of them refuses. A saga given an explicit filter derives no
> filter, so it calls `EventTypeExpansion.expandWhatCanBeFound` instead of `expand` and reports the concrete types that
> could be found rather than being refused. An array or a primitive is refused on either path, since neither is a
> hierarchy leniency could help with. A primitive can match nothing at all, and an array is refused for consistency with
> the strict path rather than because an object cannot be one.
>
> This paragraph's closing claim that `SagaFilters` and `FlowSagaImpl` are unchanged no longer holds, since #751 changed
> both. `SagaFilters` reads an explicit filter ahead of deriving one, and `FlowSagaImpl` carries the filter and answers
> `filter()`. `startEventTypes()` is still unchanged, and still for the reason given.
>
> The property this ADR is built on is untouched, because it is about the derived filter and an explicit filter is not
> one. Correlation coverage still runs over the expanded set on both paths, so the paragraph that says so needs no
> amendment.
>
> **Amended again on 2026-08-14.** The method that switches the strict walk off is now `replacementFilter(...)`, and a
> `narrowingFilter(...)` does not switch it off. See the 2026-08-14 section at the end of this document.

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

> **Amended for 0.33.0, under [#751](https://github.com/johanhaleby/occurrent/issues/751).** `Saga#filter()` shipped, and
> this rejection stands as written, because the two are different proposals. What was rejected here is an explicit filter
> *instead of* expansion, leaving a caller who declares a sealed type to find out from the documentation that they need
> one. What shipped is an explicit filter *alongside* expansion, for the caller whose hierarchy cannot be enumerated at
> all.
>
> **Amended again on 2026-08-14.** What shipped is now called `Saga#replacementFilter()`, and it gained a sibling,
> `Saga#narrowingFilter()`, which combines with expansion rather than replacing it. The rejection above still stands
> against both.

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

> **Amended for 0.33.0, under [#751](https://github.com/johanhaleby/occurrent/issues/751).** That caller now has a
> remedy that says the thing reflection could not work out. `replacementFilter(Filter.type("order-event"))` on either
> builder is used instead of the derived filter, so the saga is built without deriving one and the refusal never runs.
> Declaring the concrete types and sealing the hierarchy are still the better answers when either is available. The cost
> of the new one is that it switches the hierarchy check off for every event type that saga declares rather than only
> the one that could not be enumerated, which is stated on `Saga#replacementFilter()` itself and in section 10 of the
> upgrade guide. (The method was called `filter(...)` when this amendment was first written, and was renamed on
> 2026-08-14.)

**The projection DSL still behaves the other way, and that is recorded rather than left as an accident.** It has the
same derivation and the same supertype handler lookup, and it documents the constraint instead of fixing it. Two sibling
APIs that share a derivation now differ, so #750 records the divergence, the list of the other places with the same
derivation (`ProjectionFilters`, `SubscriptionFilters`, `DomainEventQueries`, and the subscription and snapshot
annotation registrars), and the reasoning for not widening a change the release is held for into four more modules on
the day of the tag. Anyone who reads the difference should find #750 before concluding one side is wrong.

A subscription's derived filter now also names the declared sealed type. That matters whenever the declared type is
itself concrete, an event is stored as an instance of it directly, and the mapper gives that instance a CloudEvent type
none of the permitted concrete types share, true automatically under the class-keyed mapper Occurrent ships, since such
a subscription missed exactly those events before, even while every concrete type it named kept arriving, so it gets its
own changelog entry as a change to released behaviour.

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

## Amended on 2026-08-14: the explicit filter splits in two

`filter(Filter)` answered two questions through one name, and only ever gave the answer to the rarer one.

A caller who wants to select on subject, source, data or time *as well as* their declared event types writes
`filter(Filter.subject("order-1"))` and gets a saga subscribing on every event type with that subject. The declared
types stop narrowing anything, every admitted CloudEvent goes to the converter, and the build-time hierarchy check is
switched off for every declared type on the saga. None of that is what the caller asked for, and nothing tells them.
This repository's own test fixture is the evidence, since it used a subject filter as the example of the replacement.

So the two questions get two methods, on both builders and both Kotlin `saga { }` blocks:

- `narrowingFilter(Filter)` is combined with the selector derived from the event types, so a saga still asks for its own
  types and also requires the condition.
- `replacementFilter(Filter)` is used instead of deriving one, which is what `filter(Filter)` did.

`filter` keeps neither meaning. It is deleted rather than repointed, because a name that silently changes meaning is the
one break a compiler cannot catch, and `AGENTS.md` makes an unreleased member free to remove. ADR 121 settled this shape
already, when it split `TimerName.parse` from `TimerName.of` rather than overloading one name, on the grounds that
hiding the difference behind one name puts the API's least obvious rule on the argument count. Both of these take a
`Filter`, so the difference is invisible in the signature.

### The composition, and why the narrowing keeps the check on

`SagaFilters.filterFor` starts from the replacement when there is one and the derived type filter otherwise, then ANDs
the narrowing onto it. Both can be set at once, and the result is defined and useful under a mapper that collapses a
hierarchy, so there is no illegal state and no precedence rule to remember.

**For a saga produced by `Saga.Builder`, `FlowSaga.Builder` or `Saga.create`, with a non-empty `eventTypes()`, the
strict hierarchy walk runs if and only if `replacementFilter()` is null.** A narrowing does not key it, because the
selector is still derived and so still has to name every type dispatch would accept, which is the property at the top
of this document.

Both halves of that scope do real work. An empty `eventTypes()` never ran the walk, with or without either method,
and this change does not touch that branch. It still derives a filter, `Filter.all()`, so "derives nothing" would be
wrong. What it never derives is a *type* filter.

And the walk lives in those three producers alone. A saga written by implementing this interface directly never runs it,
whatever its `eventTypes()` says, because there is no build step to run it in. `SagaFilters` still derives a type
filter for such a saga, so it can subscribe on a filter that misses types dispatch would accept, which is the very
defect this document exists to remove. That is not new, it is the pre-existing shape of a hand-written implementation,
but the sections below send a caller who wants a narrowing down exactly that route, so it is stated rather than left to
be discovered.

The verb matters. The walk *runs*, it does not necessarily refuse, and even a replacement does not switch off
everything: `expandWhatCanBeFound` still refuses an array or a primitive. What a replacement switches off is the
hierarchy refusal.

An AND can only remove matches, so a narrowing can never admit an event the base did not. That is what makes it safe to
leave the check on, and it is the argument for the ruling rather than a precedent, because this is the first ADR here to
let a configured value combine with a derived default instead of replacing it. The shape does exist in code already, in
the catch-up subscription models, which AND a capability scope onto the caller's filter.

The narrowing goes on the right of the AND. An AND is walked left to right and stops at the first mismatch, so keeping
the cheap type conditions first means a `Filter.data(..)` narrowing is not read for an event whose type already ruled it
out.

### What each method makes the caller responsible for

Both oblige the caller to admit the start event types, and not to starve the saga's own handlers. A saga is worse off
than a projection here, because an instance whose later events are excluded never reaches `isTerminal` and keeps its
timers running.

A flow saga adds one that is sharper than either of those, and it belongs to both members rather than to narrowing
alone. A guard reads what has arrived, so a selector that excludes an event type changes the answer
`received.none(Rejected.class)` gives, and a branch can fire that would not have fired otherwise. Either member does this when it excludes an event of a type a
guard asks about. A narrowing can only remove matches, so that is the only direction it can move in. A replacement can be broader or
narrower than the declared types, and does this when it is narrower, which the `Filter.subject("order-1")` example
earlier in this section already is. That is the saga taking the wrong action rather than no action, and no monotonicity argument
removes it, because removing matches is exactly what flips a negative predicate.

A replacement adds two more. Every CloudEvent it admits is converted before the saga sees it, and a flow saga appends
every correlated event to the instance's retained history before it looks at which branch handles it. A narrowing adds
neither, relative to the same saga without one, since every event it admits was already admitted by the derived
selector. That is a relative claim. A saga whose `eventTypes()` is empty and which sets no replacement derives
`Filter.all()`, so its narrowing is then the whole selector and the conversion obligation applies to it in full. With a
replacement set, that replacement is the base and this exception does not arise.

### Rejected: one accessor returning a sealed selector type

A `sealed interface` with a derived case and a replacing case would make the choice explicit in the type. ADR 91 is the
precedent that would support it, where sealed states made "a cause exists exactly when it failed" true by construction.
It does not apply, because under the composition above no combination is illegal, so there is nothing to make true by
construction and the type would only add a name to look up. ADR 70 sets the bar a new public type has to clear, and
ADR 119 rejected a helper class for the capability walk on the same grounds.

Two nullable accessors also copy across unchanged when #750 fixes the projection DSL, where a sealed selector on the
saga side alone would make that convergence harder. #750 and #758 should take this vocabulary with them.

### `Saga.create` gets the replacement only

The seven-argument factory keeps one trailing `Filter` and it means the replacement. Two adjacent nullable `Filter`
parameters would let a caller swap them and get the opposite semantics with no compile error, which is the footgun
ADR 121 refuses to put on argument count.

That leaves a gap, and `AGENTS.md` is what decides whether the gap is allowed. It says an overload earns its place when
a user driving the type directly cannot work around its absence, and names `SagaRunner.run` waiting unconditionally as
the shape of that mistake. This is not that shape, because a caller who wants a narrowing can implement `Saga` instead
of calling the factory, which is the route `create`'s own javadoc already sends them down for `onStart` and
`isTerminal`. What they cannot do is add one to a saga the factory already returned, since that is an anonymous
implementation.
