# 126. Every derived event-type filter expands a declared sealed type

Date: 2026-08-16

## Status

Accepted. Decides #750, #753 and #758. The code change follows in a separate unit. This
records the decision, not the diff.

Amended in 0.34.0 on the exemption only, by [#753](https://github.com/johanhaleby/occurrent/issues/753), and this
amendment is what supersedes the exemption
[ADR 124](0124-a-saga-expands-a-declared-sealed-event-type.md) shipped in 0.33.0, which points here. Amended in
place rather than through a successor because this ADR has not shipped in any release. What
this ADR decided about #753 was to leave the exemption in place and record it, on the reading that a uniform
exemption needed no code change. That reading held while the loss stayed invisible, and it is what changed. A
caller declaring a concrete class that is neither final nor sealed and publishing a subclass of it never
receives that subclass and gets no warning, under every `CloudEventTypeMapper` Occurrent ships, and no later
release makes that visible without the same break, so the refusal arrives in 0.34.0 instead. A mapper of the
caller's own that collapses the hierarchy onto one CloudEvent type string is the case that was working, and
the refusal costs it an explicit filter, which is the escape this decision already gives the other shapes. Every passage below saying the exemption is uniform across the sites
this ADR routes through is still true of the code, since they all share the one helper. The exemption itself
is gone, and with it the `Outcome.EXEMPT_AND_MISSES_SUBCLASSES` outcome named below, since the property test
now has only the two outcomes left. Everything else here is unchanged. See the changelog's breaking changes and section 3 of
[the 0.34.0 upgrade guide](../../migration/upgrading-to-0.34.0.md#3-declaring-an-event-type-whose-concrete-subtypes-cannot-be-found-is-refused).

125 was the highest ADR number claimed anywhere at write time, across all 75 remote branches
that exist, not main alone, matching the convention ADR 124 adopted after this repository's
prior numbering collisions. The audit ran again immediately before this file was written.

## Context

ADR 124 fixed a saga that declares a sealed supertype and silently receives nothing, by making
`SagaFilters` expand the declared type into the concrete types it permits before deriving a
filter. It also found, by diffing two independent copies of the same walk, that the projection
DSL has the identical derivation and the identical gap, documented instead of fixed
(`Projection.Builder#on`'s javadoc tells a caller to set an explicit `filter` when a handler is
keyed on a supertype). It deferred widening the fix, named the deferral #750, and listed the other places
carrying the same derivation: `ProjectionFilters`, `SubscriptionFilters`, `DomainEventQueries`,
and the subscription and snapshot annotation registrars. The stated reason was timing, not
disagreement: 0.33.0 was tagged the day #750 was filed, and widening a held release into four
more modules was the wrong trade that day.

That release shipped today. The reasoning that justified deferring #750 was about not touching
four more modules on the day of a held tag, and there is no tag being held now, so the reasoning
that motivated the deferral no longer applies. #758 asks the same question from a different
angle, whether the projection DSL's documentation is the right level for this gap or whether it
should expand like the saga now does, and the deferral it recorded was likewise about not having
a design conversation hours before a tag rather than about the answer. #753 names the one thing
the fix itself does not close everywhere, that a directly declared non-sealed concrete class
still has subclasses no filter can name, and asks whether that residual is worth removing.

**Both issues, read against the code as it now stands, contain claims that were accurate when
written and are not accurate today, and this ADR corrects them rather than repeating them.**
#750 names the implementation as `org.occurrent.dsl.saga.internal.EventTypeExpansion` in
`dsl/saga-dsl/common`. That was true when #750 was filed at 18:33 on 2026-08-11. Commit
`7aa5d76c1`, landed the same day at 21:09 as part of the work ADR 124 records, moved it to
`common/filter/src/main/java/org/occurrent/filter/internal/EventTypeExpansion.java`, package
`org.occurrent.filter.internal`, artifact `occurrent-filter`, specifically so both the saga DSL
and the annotation registrars could call one implementation instead of keeping two. `SagaFilters`
has since moved again, from `dsl/saga-dsl/blocking` to `dsl/saga-dsl/common`
(`38274d24e`, closing #786), so ADR 124's own path references to it are stale in the same way.

**#750's list of remaining call sites is also stale, in the direction of overstating the gap.**
`SubscriptionAnnotations.typesToSubscribeOn` and `getConcreteEventTypes`
(`framework/spring-boot-autoconfigure/common`) already call `EventTypeExpansion.expand` and
`concreteTypesOf`, and both `SubscriptionAnnotationRegistrar` classes (blocking and reactor)
already reach that expansion by delegation, for `@Subscription`, `@StreamSubscription`,
`@SynchronousSubscription` and `@DcbSubscription` alike. That has been true since `7aa5d76c1`,
which fixed the subscription side's own defect (dropping the declared type) at the same time it
introduced the shared helper, before #750 was ever filed. Naming these classes as part of the
remaining work in #750's list, and repeating that list unchecked, would have sent whoever picked
up #750 to re-verify code that was never broken.

What remains, verified against the current tree rather than against either issue's text, is
four call sites that still build a filter from `Class.map(cloudEventConverter::getCloudEventType)`
with no expansion, one per DSL this decision needs to reach:

| Call site | Module | Escape hatch today |
|---|---|---|
| `ProjectionFilters.filterFor` | `dsl/projection-dsl/common` | `Projection.filter()`, set-once, short-circuits |
| `SubscriptionFilters.filterFromEventTypes` (Kotlin) | `dsl/subscription-dsl/common` | none found |
| `DomainEventQueries.createFilterFrom` and five single-type methods | `dsl/query-dsl/blocking`, `dsl/query-dsl/reactor` | none, ad hoc query calls |
| `SnapshotAnnotationRegistrar.snapshotFilterFor` | `framework/spring-boot-autoconfigure/blocking`, `.../reactor` | `SnapshotView.filter()`, set-once, short-circuits |

`occurrent-filter` is already a direct dependency of `dsl/projection-dsl/common` and
`dsl/subscription-dsl/common`, so calling `EventTypeExpansion` from `ProjectionFilters` or
`SubscriptionFilters` adds no new dependency. Neither `dsl/query-dsl/blocking` nor
`dsl/query-dsl/reactor` declares it today, only picking up `Filter` transitively through the
event store API, so those two need the dependency added.

`ProjectionFilters`'s own javadoc says it "deliberately re-implements the subscription DSL's
`filterFromEventTypes` rather than depend on `subscription-dsl-common`, keeping this module
independent of the subscription stack." That reasoning is about not depending on
`subscription-dsl-common`, and `occurrent-filter` is not that module and is already a dependency
here. It gives no reason to keep re-implementing the walk that `EventTypeExpansion` already
does once, correctly, with a property test behind it.

## Decision

### The property restated without "dispatch"

ADR 124 stated its property as "the derived filter must name every event type that dispatch
would accept." That wording fit the saga DSL, where an `isInstance` dispatch is literally what
decides whether a handler runs. A projection's handler is chosen by the same `isInstance`
matching, a subscription's registered handler is invoked the same way, and a snapshot view applies
an event the same way. `DomainEventQueries` is the one surface with no dispatch at all: it sends the
derived CloudEvent-type filter to the store and converts every event the store returns, so nothing
downstream re-checks assignability. Its claim to the property comes from the API contract instead,
since a caller passing `OrderEvent.class` to a method typed `Class<E>` is asking for the events
assignable to it, and a filter naming only the supertype's own CloudEvent type answers a narrower
question than the signature poses. The mechanism differs and the property does
not, so it is restated once, at the level all four share:

**A filter derived from a caller's declared event types must match every stored event that the
caller's own declared types would accept.**

Every module in the table above derives such a filter. None of them is exempt from the property
for a reason particular to that module. Each was simply not part of the work that first noticed
the gap.

### Every remaining site adopts `EventTypeExpansion`

`ProjectionFilters` and `SnapshotAnnotationRegistrar` keep their existing shape unchanged. An
explicit `filter()` on `Projection` or `SnapshotView`, when set, still replaces the derived
filter outright. When it is not set, the site calls `EventTypeExpansion.expand` instead of
mapping declared types straight across, the same choice `SagaFilters` already made for the
strict path. Nothing about the override contract changes, only what the code does when the
caller has not used it.

`SubscriptionFilters.filterFromEventTypes` and `DomainEventQueries` gain no override, because
neither has one today. Both call `EventTypeExpansion.expand` too, for the same reason
`SagaFilters` uses the strict path rather than `expandWhatCanBeFound`. A subscription is a
standing registration and a query is a one-shot correctness question, and in neither case does
silently matching a narrower set than the caller declared become acceptable just because there
is nowhere to opt out. Whether the subscription DSL or the query DSL should eventually gain an explicit
override of its own is a separate, smaller question, left open for whoever implements this or
for a later issue. It does not block adopting `expand` unconditionally now, because adding an
override later only widens what a caller can do, it never has to change what `expand` did in
its absence.

### One step from expanded types to `Filter`, not a fifth copy of the same switch

`ProjectionFilters`, `SagaFilters` and both `SnapshotAnnotationRegistrar` classes each carry
their own three-way branch on the expanded set (empty, one type, several), producing
`Filter.all()`, a single `Filter.type(..)`, or an `or`-combined one. `DomainEventQueries` gets
there with `.reduce(Filter::or)` instead, and `SubscriptionFilters.kt` with a Kotlin `when`.
One semantic difference survives the consolidation and the shared step must not erase it:
`DomainEventQueries.createFilterFrom` maps a null or empty type set to a null filter, which its
collection overloads turn into an empty result, where the registrar branches map empty to
`Filter.all()`. Routing the query DSL through a shared step that answers empty with
`Filter.all()` would silently turn "match nothing" into "query the whole store", so its call
sites short-circuit on a null or empty set before deriving, and the shared step is only ever
handed a non-empty expanded set there.
Six call sites, once this decision lands, would carry six near-identical copies of a branch that
has never varied between them. That is exactly the shape #750's own history warns about. The
saga DSL's expansion walk was itself a second copy of the annotation registrars' walk, written
because nobody thought to grep for it first, and `AGENTS.md` now carries the convention that
exists to stop that from happening again.

So `EventTypeExpansion` gains one more static method, alongside `expand` and
`expandWhatCanBeFound`, that goes the rest of the way from an expanded set to a `Filter`:

```java
static <E> Filter deriveFilter(Set<Class<? extends E>> declaredTypes,
                                Function<Class<?>, String> cloudEventTypeOf,
                                Function<Class<?>, RuntimeException> cannotExpand)
```

Each of the six call sites becomes a call to `deriveFilter` (or, on the two builders that keep
an override, a call guarded by that override) instead of hand-rolling the branch. This is not a
new abstraction over a problem that does not exist yet. The branch already exists in three
copies today, and this decision is what stops it from becoming five or six. `occurrent-filter`
is the module that already owns `Filter` and `Condition`, so this is where the step belongs, not
a new shared module and not a helper class in one DSL that the others import.

### #753 needs no code change of its own

`EventTypeExpansion`'s one deliberate exemption, that a directly declared non-sealed concrete
class is accepted even though its subclasses cannot be found, lives in `expand` and
`expandWhatCanBeFound` themselves, not in any caller. Every site this decision touches reaches
the exemption by calling those methods, the same way `SagaFilters` and `SubscriptionAnnotations`
already do. There is nothing left for #753 to fix once every derivation goes through the one
helper. The exemption was already uniform by construction from the moment `EventTypeExpansion`
became the single implementation, and this decision does not change that shape, only which
callers use it. `EventTypeExpansionTest.the_filter_names_every_type_dispatch_would_accept`
already asserts the exempted case as `Outcome.EXEMPT_AND_MISSES_SUBCLASSES` for the property
in general, not for the saga DSL specifically, so it needs no new row and no new test to cover
whichever caller adopts `expand` next.

**This is a decision to leave #753 open with no further action, and it is written down here
rather than left for #753 to record on its own**, because a reader who finds #753 open after
this ADR lands should not have to guess whether it was missed.

### #758's question is answered: expand, do not only document

#758 asked whether documenting the constraint, the way `Projection.Builder#on`'s javadoc does
today, is the right level for it. It is not. The javadoc tells a caller what the code does
instead of making the code do what the javadoc's own reader would expect, which is the same
shape ADR 124 already rejected for the saga DSL under "add `Saga#filter()` and document the
constraint, matching the projection DSL," on the grounds that documentation alone is the wrong
answer for a library whose callers cannot be observed from this repository. Nothing about
`Projection` makes that reasoning not apply to `Projection`. That `Projection` already has
`filter()` where `Saga` did not is not a reason to keep the narrower default. It is an override
for a caller who wants something other than the expanded set, such as a `CloudEventTypeMapper`
that collapses a hierarchy onto one type string in a way expansion would not improve on, and an
override serves that purpose equally well whether the default it overrides is narrow or expanded.

## Rejected alternatives

**Leave the divergence documented, closing #750 and #758 as "working as designed."** This was
a live option while the release stayed held, and it stopped being one the moment the release
shipped. The `filter()` override that made it defensible for `Projection` gives a caller a way
out only if they read the javadoc before hitting the gap, which is exactly the shape ADR 124
rejected when it was proposed for sagas.

**Fix each remaining site with its own local expansion, matching what it already has instead of
calling `EventTypeExpansion`.** This is the path that produced the defect ADR 124 spent most of
its length removing, two independently written copies of the same walk disagreeing on a detail
neither author meant to leave inconsistent. Four more independent copies would multiply, not
remove, that risk.

**Leave the switch-to-`Filter` step duplicated at each call site rather than adding
`deriveFilter`.** Cheaper today, and it recreates the exact shape that made the saga DSL's
expansion walk a silent second copy of the registrars' walk in the first place, this time for
the smaller piece of code sitting right next to the one this decision already unifies.

**Give `Projection` and `SnapshotView` a `narrowingFilter`/`replacementFilter` split now,
matching what `Saga` gained under #751.** ADR 124 names this vocabulary and says "#750 and #758
should take this with them" when rejecting a sealed selector type for the saga side. That
sentence is about which shape to reach for if either module ever needs a narrowing filter, not
a mandate to add one now. Neither issue asked for narrowing, `filter()`'s existing replace-only
semantics keep working unchanged under expansion, and adding a capability nobody has asked for
yet is exactly what "not beyond what the ADR needs to demonstrate" excludes. Left for whichever
issue motivates it.

## Consequences

Once implemented, a projection, a subscription built through `dsl/subscription-dsl`, a query
through `DomainEventQueries`, or a `@Snapshot` view, each keyed on a supertype, receives or
matches every concrete type that declaring the supertype already looked like it should. This is
the same shape of fix ADR 124 shipped for sagas and the annotation-based subscriptions, reaching
the four places it named as the rest of the work.

It is a breaking runtime change against 0.33.0 for the same reason ADR 124's fix was breaking
against 0.32.0. A caller whose declared type cannot be expanded, an open interface, a non-sealed
abstract class, or a sealed hierarchy reopened below the declared level, moves from a filter
that silently matched too little to a refusal naming the type and the remedy, on whichever of
`Projection.Builder.build()`, the subscription DSL's registration, `DomainEventQueries`' query
call, or `@Snapshot` registration derives that filter. The implementing unit records this as a
`#### Breaking changes` entry in `changelog.md` and a section in
`doc/migration/upgrading-to-0.34.0.md`, following the pattern ADR 124's own consequences and
`doc/migration/upgrading-to-0.33.0.md` section 10 already set. No OpenRewrite recipe can migrate
the refusal itself, for the same reason ADR 124 gives. Telling a reopened hierarchy from a
correctly sealed one needs the sealed modifier from the class declaration, which OpenRewrite
does not expose on the type behind a class literal.

**One caller per newly expanding site is worse off, the same way ADR 124 named for the saga
DSL, and naming them here is the same honesty that ADR made a point of.** A custom
`CloudEventTypeMapper` collapsing a hierarchy onto one type string makes an undifferentiated
supertype filter work today in `ProjectionFilters`, `SubscriptionFilters`, `DomainEventQueries`
and `SnapshotAnnotationRegistrar`, the same way it did for the pre-ADR-124 saga DSL. That caller
keeps working wherever an override already exists (`Projection.filter()`, `SnapshotView.filter()`)
and needs one added wherever it does not yet (the subscription DSL, `DomainEventQueries`) to stay
working after this lands, exactly the gap ADR 124 recorded and left unresolved for `Saga` until
#751 shipped `replacementFilter`.

#753 stays open, with this ADR's Decision section as the record that no code change follows
from it. The exemption it describes is already uniform across every site this decision reaches,
because all of them share the one helper the exemption lives in.

#750 and #758 close through this ADR together with the implementing unit's pull request.
Whoever reads ADR 124's Consequences section and follows it to #750 should find this ADR, not
an issue still asking the question ADR 124 deferred answering.

## Amendment (2026-08-22): two more call sites found by #912, neither reached through `deriveFilter`

The table above and the "six call sites" language that follows it describe the four DSLs #750
and #758 named plus the saga DSL and the annotation-based subscription, verified against the
tree at the time this decision was written. `ExecuteFilter.type`/`includeTypes`
(`application/service/common`) and `DcbCriteriaBuilder.type`/`types` (`dsl/dcb-dsl/common`) were
not in that count. [#912](https://github.com/johanhaleby/occurrent/issues/912), filed while
closing #753, found both still mapped a declared class straight to its own CloudEvent type with
no expansion at all, the same defect this decision's table describes for the other four.

Neither goes through `deriveFilter`. `ExecuteFilter` resolves to a `StreamReadFilter`, not a
`Filter`, and `DcbCriteriaBuilder` resolves to a `DcbCriterion`, not a `Filter` either, so
`deriveFilter`'s `Function<Class<?>, String>` to `Filter` pipeline does not fit either shape.
Both call `EventTypeExpansion.expand` directly instead and build their own result type from the
expanded set. `deriveFilter` stays the right shared step for every site that does resolve to a
plain `Filter`. Neither of these two ever sees an empty declared-type set either, since `type`
and `includeTypes`, and `type` and `types` on the DCB side, all require at least one `Class`
argument, so the `Filter.all()` branch `deriveFilter` exists to produce has nothing to reach on
either path.
The property this decision states, that a filter derived from a caller's declared event types
must match every stored event the caller's own declared types would accept, holds for both:
`ExecuteFilter.type`/`includeTypes` and `DcbCriteriaBuilder.type`/`types` refuse a declared type
whose concrete types cannot all be found, the same refusal every site in the table above applies.

`ExecuteFilter.excludeTypes` is a distinct derivation this decision's property does not describe
and does not need to. The property above is about an inclusive filter matching too little. An
exclusive filter's defect runs the other way, matching too much by excluding too little, so
`excludeTypes` widens to every concrete type `EventTypeExpansion.expandWhatCanBeFound` can find
rather than refusing. [#912](https://github.com/johanhaleby/occurrent/issues/912)'s own pull
request records that direction-dependent choice and its reasoning. `DcbCriteriaBuilder` has no
exclusive derivation to reach.
