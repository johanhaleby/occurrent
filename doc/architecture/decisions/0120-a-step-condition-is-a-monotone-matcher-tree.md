# 120. A step condition is a monotone matcher tree

Date: 2026-08-10

## Status

Accepted. Resolves #707. 118 and 119 landed elsewhere (the rv33 epic) while this was in flight, so 120 is the next
free number as of this ADR, re-verified at write time per the max-plus-one rule.

## Context

A flow saga step is today either a choice, one or more `on(...)` branches where the first matching arriving event
wins, or a single `join(...)`, a conjunction of per-type counts since the step was entered. The two cannot mix in one
step, and `join` itself cannot express disjunction. There is no way to say "wait for either of two alternatives" or
"two events of one type and one of another, or a single event of a third". The workaround is an `onlyIf` guard that
hand-counts matches against `ReceivedEvents`, rebuilt on every step that needs anything `join` cannot express, which
is exactly the bookkeeping `join` exists to remove. Issue #707 records the gap. `join` shipped in 0.31.0, not 0.32.0
as the issue body claims, and the routing comment on #707 carries the correction.

## Decision

**A leaf is an event-level match lifted to a window-level count.** `StepCondition.EventMatcher<E>` pairs a type with
an optional predicate over one event, and `StepCondition.AtLeast<E>` lifts a matcher to "the window since this step
was entered contains at least `n` events matching it". The two levels are kept apart deliberately, an event-level
predicate decides one event, a window-level count decides how many. Merging them into one shape would blur what a
leaf is actually asking. The unified public factory is `event(type)`, `event(type, count)`, `event(type, predicate)`,
`event(type, count, predicate)`, all four converging on `AtLeast`.

**`AllOf` and `AnyOf` combine leaves and other composites into a tree**, built by `allOf(...)`/`anyOf(...)` rather
than constructed directly, mirroring `DcbCriteria.anyOf` (the in-repo criteria-tree precedent, and the reason `and`/
`or` were rejected as the composite names below). Singleton collapse, same-kind flatten one level, empty rejected,
declaration order preserved, no dedupe. A class-literal shortcut, `allOf(Class, Class...)`/`anyOf(Class, Class...)`,
expands each argument to a predicate-less, count-one leaf.

**A tree is data, so it is buildable once and reused.** `var cancelled = anyOf(event(Cancelled.class),
event(TimedOut.class))` is a plain value, usable across several `on(...)` calls or across steps. `StepBuilder.on`
therefore takes `StepCondition<? extends E>`, not the invariant `StepCondition<E>`. A tree built from leaves of
different concrete event types infers `E` as their least upper bound, which an invariant parameter would refuse. The
internal narrowing to `StepCondition<E>` is an unchecked cast, safe for the same reason the existing guard-predicate
narrowing is. A leaf's predicate only ever runs after its own `eventType.isInstance` has already accepted the event,
so nothing is read at a type wider than what was checked.

**`join` is deprecated in favor of `on(allOf(...))`, reimplemented as sugar over it.** An expectation of `n` events
of a type becomes `event(type, n)`, and the whole expectation list becomes one `allOf(...)` tree. Deprecated,
`StepBuilder.join` (both overloads), Kotlin `StepScope.join`, `StepScope.expect<T>`, and `Expectation`. Plain
`@Deprecated` (`@deprecated` javadoc naming the replacement), no `forRemoval`, no OpenRewrite recipe. The API still
works unchanged, and a recipe belongs with the removal release, not this one. `rewrite-kotlin` cannot fire on the
Kotlin sugar either way, since `join`/`expect` are ordinary Kotlin functions, not annotations a recipe can target.

**A step's branch list is unified.** A classic type match and a window condition are two kinds of trigger on one
ordered list, not two mechanisms layered on top of each other. `Trigger<E>` is sealed over `ArrivingEvent`
(a type plus an optional guard, exactly today's classic branch) and `WindowCondition` (a `StepCondition<E>`).
`on(Class, ...)` compiles to `ArrivingEvent`, `on(StepCondition, ...)` and the lowered `join(...)` compile to
`WindowCondition`. The evaluator scans a step's branches in declaration order on every arriving event, and the first
satisfied one wins, exactly as a classic-only step behaves today, generalized rather than replaced.

**Guards are not window leaves, and this is deliberate and is the divergence a caller needs to know.** A guarded
classic branch, `on(PaymentFailed.class, (f, received) -> received.count(Retry.class) >= 2, then)`, only tests its
guard when a `PaymentFailed` itself arrives. Two `Retry` events with no `PaymentFailed` ever arriving never reach the
guard at all, because the branch's trigger gate is `PaymentFailed.class.isInstance(event)`, checked before the guard
runs. A window condition, `on(event(Retry.class, 2), then)`, is re-evaluated on every arriving event regardless of
its type, because a tree can span several leaf types and any of them can be the one that tips it over. Merging a
guard into the window machinery would make it re-evaluate the same way, and the two Retry events alone would then
satisfy it. Kept apart, a guard fires only on its own type's arrival, same as today. `StepBuilder`'s javadoc states
this next to the overload it applies to, and `FlowSagaTest.StepConditions` asserts it, with the Retry/PaymentFailed
shape above.

**A condition tree is monotone.** A leaf only ever asks whether enough matching events have arrived, never whether
one is absent or whether a count stays under a limit. There is deliberately no `not()` and no way to match on
absence. This is what makes checking a tree incrementally, one arriving event at a time, correct. A leaf's truth
value, once true, is never revisited to false by a later event, so the evaluator never needs to re-scan history
looking for a negative that a later event could undo. A step's `timeout` is what expresses "this did not happen in
time", and a condition tree is not asked to duplicate it. Runtime-varying counts (a threshold decided per-instance
rather than fixed at build time) remain out of scope too, the same non-goal ADR 63 already recorded for `join`.

**This partially reverses ADR 63's non-goal.** ADR 63 named "no dynamic N-of-M joins" as a flow-layer non-goal.
Static alternative and conjunction trees, built once at saga-definition time, are now expressible directly in the
flow layer, so a process that needs them no longer has to drop to the machine-core `Saga` builder. Runtime-varying
counts, decided per-instance rather than fixed at build time, remain out of scope, unchanged from ADR 63.

**No new `ActionKind`.** Every branch firing, whatever its trigger kind, writes `ActionKind.BRANCH` with its real
index, and a lowered `join` step writes index `0`, the only branch such a step has. The `JOIN` constant stays
declared on the enum (a store's `ActionKind.valueOf` must not meet an unknown constant reading a document a
pre-upgrade process wrote), but nothing writes it any more. `react` keeps a defensive `JOIN` arm that applies the
reaction of the step's one branch with `ReceivedEvents` only, never through the event-casting `BRANCH` path (which
would throw `ClassCastException` on a timeout input if it were ever reached that way). The arm is unreachable in
practice. `evolve` always overwrites `lastAction` from the fresh input before `react` runs in the same delivery, so a
stale persisted `JOIN` value is read back once and immediately superseded, never acted on. An in-flight instance
persisted mid-join before this release carries over transparently, since the new `evolve` reads only fields whose
meaning is unchanged.

**Naming.** The window-level lift is `AtLeast`, not `Expected`. The sealed interface is public, so callers
pattern-match over its variants, and `Expected` beside the deprecated `Expectation` in the same package would collide
in completion and imports. `AtLeast` also states the actual semantics, at least `n` in-window, directly.

## Rejected alternatives

**A third `StepBody` variant, added beside the existing choice and join bodies, keeping all three separate.** This
was the first shape considered. It fails on the one thing this ADR exists for, mixing. A step that wants a classic
branch and a window condition side by side needs one ordered list either way, so a third, separate body still has to
grow into exactly the unified branch list this ADR chose, just one release later and with an extra variant to carry
along the way. Choosing the unified shape now avoids that detour.

**A trio of differently-named leaf constructors**, one for a bare type match, one for a count, one for a predicate,
instead of one overloaded `event(...)` family. Rejected for asking a caller to remember which name carries which
combination of type, count, and predicate, when a single name with progressively more arguments says the same thing
and reads the same way at every call site.

**`and`/`or` as the composite names**, instead of `allOf`/`anyOf`. Rejected in favor of the vocabulary
`DcbCriteria.anyOf` already established in this codebase for exactly this shape, a tree of alternatives and
conjunctions over matchers, so a reader who already knows one reads the other for free.

## Consequences

- A process that needs a disjunction, a mixed count-and-alternative tree, or a predicate over an event no longer has
  to hand-roll it with an `onlyIf` guard and manual counting. It declares the tree once, and the tree can be reused.
- `join` keeps working, unchanged in behavior (the sugar-equivalence tests are the guard for that), but is
  deprecated. A caller on `join` today has no forced migration timer, there is no `forRemoval` and no rewrite recipe
  in this release.
- A step mixes classic and window-condition branches freely, evaluated in one declaration order. The mutual exclusion
  between `on(...)` and `join(...)` stays exactly as strict as it is today, since `join` still refuses to coexist
  with any branch, classic or window-condition.
- The window-reset rule a mixed step now makes visible, stated rather than left implicit. Every transition, including
  a `transitionTo` back into the same step, resets the step's entry point, so re-entry restarts every branch's window
  in that step, not only a join's. A classic branch self-looping wipes a sibling window condition's partial count,
  exactly the same way it already wipes a join's. `FlowSagaImpl.applyTransition` and `StepBuilder.on(StepCondition,
  Continuation)`'s javadoc both say so, and `FlowSagaTest.StatedRules` asserts it.
- No wire format changed. `ActionKind` keeps all four constants, `FlowStateImpl`'s bookkeeping fields keep their
  existing meanings (still not a stable API by ADR 63's compatibility note), and `SpringMongoSagaStateStore` needed
  no change.
- Occurrent is a published library with callers outside this repository. The reuse pattern this design leans on, a
  tree assigned to a `var` and passed to more than one `on(...)` call, is checked by compile-shape tests against two
  event hierarchies, a clean sealed one, where a tree's inferred type is the domain type directly, and one whose
  leaves also share a second, unrelated interface, the shape most likely to push a compiler's inferred least-upper-
  bound somewhere unexpected. Both compile and both bind to `on` without a cast.

## Amendment (2026-08-11): a reaction reads the window that fired, `allOf` refuses a duplicated requirement, and a predicate must be deterministic

A design review of everything since 0.32.0 read this record against the code and found four statements that do not hold.
All four are corrected here, and the code changed to match where the code was the thing that was wrong. Everything above
stays decided, meaning monotone conditions only, no negation and no absence, one unified `event` leaf, `allOf`/`anyOf`
composites, `join` deprecated and lowered to sugar, no new `ActionKind`, and guarded classic branches deliberately not
lowered.

**A window-condition reaction now reads the window its condition was evaluated over, not the whole retained history.**
The `Consequences` section above, and `StepCondition`'s javadoc, promised that `whenFulfilled` sees "the window it fired
on". It received `ReceivedEvents.of(state.received())`, the whole retained log, which is the same set only in a first
step. From the second step onwards the two diverged, and the divergence was not academic, because the documented
`review()` example inferred which `anyOf` alternative had fired by asking `received.none(Rejected.class)`, which answers
about an earlier step's leftovers as readily as about its own window. A saga could take the correct transition and then
emit the wrong effects.

The reaction is handed a view over the retained list that answers `count`, `all`, `first`, `any`, `none` and `asList`
over the fired window alone, while `initiating()` still reaches element 0. Concatenating `[initiating] + window` instead
was considered and rejected, because it reintroduces the same defect for a step whose condition counts the initiating
event's own type, since the evaluator deliberately never counts the start delivery. A guard and a `timeout`'s `onExpiry` keep
reading the whole retained history, which is what makes a count spanning several steps possible and is their documented
contract.

**A lowered `join`'s reaction is exempt, and dropping that exemption breaks a shipped API silently.** `join` shipped in 0.31.0
with a callback that reads the whole retained history, and lowering it to a condition tree is sugar, which means the
semantics are preserved exactly. Narrowing every `WindowCondition` reaction would therefore have silently changed what a
second-step `join` callback can count, with no exception and no changelog entry, for an API a caller can still be using.
`WindowCondition` has a `loweredFromJoin` flag, set only by `StepBuilder.join`, and `reactionWindow` narrows only when
it is false. `FlowSagaTest.LoweredJoin.a_join_reaction_still_reads_the_whole_retained_history_and_not_just_its_own_window`
is what would fail if that flag were dropped. This is the same line the duplicate rule draws below, released behaviour
preserved and the unreleased surface made strict, and it is why the `Consequences` claim that "join keeps working,
unchanged in behavior" is still true and why `doc/migration/upgrading-to-0.33.0.md` needs no correction.

> **Amended on 2026-08-12 by [ADR 125](0125-a-lowered-joins-reaction-reads-its-own-window-not-the-whole-retained-history.md).**
> The exemption above is reversed. A lowered `join`'s reaction now narrows exactly like every other `WindowCondition`
> trigger, and `loweredFromJoin` is gone from the record. The `Consequences` claim that "join keeps working, unchanged
> in behavior" no longer holds for the reaction window, and `doc/migration/upgrading-to-0.33.0.md` does need the
> correction this paragraph said it would not, added as its section 11.

**The documented examples stop inferring the matched alternative, and no API is added for it.** A step that reacts
differently per outcome writes one `on(...)` branch per outcome, ordered, first satisfied one winning. That is strictly
more explicit than a single `anyOf` branch that works out afterwards what fired, because the ordering that breaks a tie
is written in the declaration instead of buried inside the tree. Exposing the matched child would need either identity
comparison against the instance the caller built, which is fragile (records have value equality, predicated leaves do
not), or names on conditions, which is a materially larger public API. `anyOf` is for alternatives that share a reaction.

**"No dedupe, duplicates preserved" is reversed for `allOf`, and kept for `anyOf`.** Within one `allOf`, after
flattening, two children are refused with `IllegalArgumentException` when some `EventMatcher` is reachable from both,
comparing matchers by equality, which for a saga declared in a `@Saga` factory or a `@Bean` means at startup rather than
on a delivery.

The rule is stated on the matcher rather than on exact leaf equality, because exact-duplicate rejection alone leaves
`allOf(event(A, 2), event(A, 3))` legal, and that expression reads as five and behaves as three, which is the more
misleading of the two. It searches through composite children rather than comparing only leaf siblings, because an
`allOf` child can be a whole `anyOf` subtree and one event reaching a leaf inside it satisfies that child too, so
`allOf(event(A), anyOf(event(A), event(B)))` reads as two events and is fulfilled by one `A`. Searching through
composites also subsumes the equal-children case, since two equal children reach the same matchers.

**The stated guarantee is exactly what the check does, and no wider.** A guarantee written wider than the check is worse
than the gap it papers over, so both the javadoc and this record name what gets through. Two matchers that are unequal
but can still be satisfied by one event are allowed through. A supertype and a subtype of it stay legal, because refusing
`allOf(event(BaseEvent.class), event(A.class))` would refuse a legitimate "one event of any kind plus one `A`
specifically". Two leaves over one type whose predicates are separately written lambdas stay legal too, because distinct
lambdas never compare equal and nothing can tell a duplicate from two genuinely different tests. Both are accepted
deliberately and both have a test saying so.

Refusing on a shared matcher does refuse some trees that are only sometimes satisfiable by one event, such as
`allOf(anyOf(event(A), event(B)), anyOf(event(C), event(A)))`, where `A` alone satisfies both children but `B` and `C`
together also do. That cost is accepted. This DSL has no way to say "two distinct events" (nothing consumes an event, by
design), so a declaration whose meaning depends on which of two readings the caller had in mind cannot be expressed
correctly here anyway, and a message naming the shared type at startup is better than a step that completes early in
production.

Summing the counts of two same-matcher children was considered and rejected. Silently changing what a declaration means
is worse than refusing it, and the reading is not even unambiguous, since `allOf(event(A, 2), event(A, 3))` could
defensibly mean three or five.

`anyOf` stays permissive **and this asymmetry is deliberate**. A repeated `allOf` child reads as an extra required
occurrence and is satisfied by an existing one, so the declaration is stronger than the behaviour. A repeated `anyOf`
alternative asks for exactly what it says and is satisfied by exactly that, so nothing reads as stronger than it is, and
a tree assembled from data can carry a harmless duplicate. Do not make the two symmetric for symmetry's sake.

**The deprecated `join` lowering is exempt from the rejection too.** `join(List.of(expect(A, 2), expect(A, 2)))` is a
working declaration in a shipped API, and inheriting the rejection would turn it into a context that fails to refresh, on
a patch upgrade. The lowering therefore collapses same-type expectations to the highest count asked for before building
the tree, which is what such a join has always meant, since each expectation is checked against the same window
independently. Together with the reaction-window exemption above, that is why the `Consequences` claim that "join keeps
working, unchanged in behavior" is still true, deliberately. Released behaviour is preserved, and the unreleased API is
made strict. The programmatic `allOf(Collection)` overload is not exempt, because
a caller assembling required types from data who ends up with one matcher twice has exactly the bug the rule exists to
catch, and is the least likely to see it.

**"The evaluator never needs to re-scan history" was never true, and monotonicity is not what makes it unnecessary.**
The `Decision` section argued that monotonicity means "the evaluator never needs to re-scan history looking for a
negative that a later event could undo". It re-derives every leaf's count from the step window on every delivery, and
always has. What monotonicity actually buys is soundness rather than incrementality. No leaf's truth can be undone by a
later event, so re-deriving counts from the step window each delivery gives the same answer an incremental evaluator would.

That equivalence needs one thing the record never asked for, so `StepCondition` now requires it. **A leaf's predicate
must be a deterministic function of the event it is given.** Because the predicate is re-run against the window on every
delivery, one that consults the clock, a random source, mutable state, or a remote service can answer differently for
the same event later, which breaks "once true, always true" and makes a replay diverge from the original run. The public
type cannot enforce purity, so this is a contract statement on the javadoc.

An incremental evaluator with persisted per-step counters is deliberately **not** built here. Given the determinism
contract it buys performance rather than correctness, its payoff needs a bound on retention that this change does not
introduce, and the persisted shape has to survive an ordinary redeploy that renames or reorders leaves, which two
rejected designs did not. It is tracked separately, together with the retention bound, because the two are one change seen
twice. Counters are what make it possible to stop retaining the active step's events, and the retention bound is what makes
counters pay for themselves.

> **Superseded by [ADR 123](0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md)
> (2026-08-11).** Both halves are now built. A step's counts are carried in `FlowStateImpl.stepConditionProgress` and
> `stepWindow(int)` caps how many of the current step's own events are kept. Two corrections to the paragraph above. The
> shape that survives a redeploy is not the one #741 sketched, since a fingerprint of event type, whether a leaf has a
> predicate, and the count it asks for is identical for two leaves that agree on all three, so swapping them moves each
> count onto the other's predicate silently. ADR 123 keeps raw uncapped counts, leaves the count a leaf asks for out of the
> fingerprint, and proves at build time that two leaves sharing a fingerprint entry are the same `EventMatcher`. And
> counters do not make the cap possible so much as make it safe, because a step with only classic branches could always
> have dropped its oldest events, while a window condition under a cap needs the counts to stay correct.

**Retention is documented rather than changed.** `historyWindow` bounds the carry-over behind the current step's entry
and only that. Trimming runs on a transition, and the step being left keeps all of its own events, so `historyWindow(0)`
still retains every event of the active step and an instance parked in one noisy step grows. `FlowSaga.Builder.historyWindow`,
`FlowStateImpl`'s "Bounded retention" section and `ReceivedEvents` now say so plainly instead of implying a limit that only
applies to a flow whose steps turn over.

> **Superseded by [ADR 123](0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md)
> (2026-08-11).** Retention is now changed as well as documented. `historyWindow` still limits only the carry-over, and that
> part stands, but the current step is no longer uncapped, because `stepWindow(int)` limits how many of its own events are
> kept. Under that cap, a guard, a `timeout`'s `onExpiry` and a window-condition reaction all read only the events still
> kept, so this record's statement that `whenFulfilled` reads every event received since the step was entered is narrowed
> for a capped step, which reads what is left of that window with the tipping event among it. `FlowStateImpl`'s section is
> renamed "Retained events" and covers both settings.

**The wire format did change, and the self-loop reset rule now really is where this record said it was.** The
`Consequences` claim that "no wire format changed ... `SpringMongoSagaStateStore` needed no change" is superseded.
Handing a reaction the fired window needs the entry position of the step being left, and `applyTransition` overwrites
`stepEntryIndex` with the entered step's entry before `react` runs, so that position is destroyed and is not derivable
afterwards. `FlowStateImpl` has a `previousStepEntryIndex` component and `SpringMongoSagaStateStore` persists it.
The field stays what ADR 63's compatibility note already says the bookkeeping is, an implementation detail rather than a
stable format. Compatibility is kept in both directions. A document written by 0.31.0 or 0.32.0 has no such field, reads back as
`-1`, and a reaction then falls back to the whole retained history, which is exactly what it saw when that document was
written. An out-of-repo `SagaStateStore` compiled against the previous eight-component record keeps compiling through a
secondary constructor that supplies `-1`, and degrades the same way. Two tests cover the two directions, and the
old-document one seeds the document by hand, since a document this store wrote a moment ago only proves the store agrees
with itself. Separately, this record claimed the self-loop window-reset rule appears in
`StepBuilder.on(StepCondition, Continuation)`'s javadoc when it did not. It does now.
