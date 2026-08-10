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
