# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

## 1. A flow saga's `join`, Kotlin's `expect<T>` and `Expectation` are removed

`StepBuilder.join`, Kotlin's `expect<T>`/`join`, and the `Expectation` type are gone. `join` was already deprecated
in 0.33.0 in favor of `on(StepCondition, ...)` with `allOf(...)`, and that replacement is what every caller now
needs. An expectation of `n` events of a type becomes `event(type, n)`, and the whole list becomes one `allOf(...)`
tree:

Java, before and after:

```java
// Before
step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end());

// After
step.on(StepCondition.allOf(StepCondition.event(PlayerReady.class, 2)), Continuation.end());
```

Kotlin, before and after:

```kotlin
// Before
join(expect<PlayerReady>(2), then = end)

// After
on(allOf(event<PlayerReady>(2)), then = end)
```

`whenFulfilled`, or the trailing reaction lambda in Kotlin, carries over unchanged. It still reads
`ReceivedEvents`, not a single triggering event.

[ADR 125](../architecture/decisions/0125-a-lowered-joins-reaction-reads-its-own-window-not-the-whole-retained-history.md)
had rejected removing `join` outright. No recipe covered it, so removal would have broken every caller with no
automated fix. That recipe now exists, so this release acts on the decision ADR 125 already reasoned through
rather than relitigating it. See [#707](https://github.com/johanhaleby/occurrent/issues/707) and
[#806](https://github.com/johanhaleby/occurrent/issues/806).

### Run the recipe

`UpgradeToOccurrent_0_34` rewrites the shapes it can prove, both `join` overloads.

A `join` call rewrites when its expecting argument is a literal `List.of(...)` or `Arrays.asList(...)`, and every
element of that list is itself a literal `Expectation.of(Class)` or `Expectation.of(Class, int)` call. The recipe
cannot see what a variable or a method call contains, so a call built either way is left alone.

Two expectations naming the same type collapse to the higher of their counts, the same way `join` itself always
did, but only when both counts are integer literals. A count that is a variable or an expression cannot be
compared at rewrite time, so a duplicate-typed pair with a non-literal count is also left alone.

Every call the recipe leaves alone stops compiling once `join` is removed, so the compiler finds it for you. Fix
it by hand using the Java example above, generalized to your own list contents.

The recipe is Java only. `expect<T>` is an inline reified Kotlin function with no call site left in the compiled
class to match, and Kotlin's `join` takes named arguments and a trailing lambda, syntax the Java-template
machinery behind this recipe cannot rewrite. Every Kotlin call site needs the by-hand translation below.

### By hand

Translate a Java call the recipe left alone, or any Kotlin call, using the shapes above. Two more cases are worth
naming directly.

A `join` built from a variable or a method call, rather than a literal list, translates the same way once you
have the list of expectations in front of you. Build the `StepCondition` tree from that same list by hand, and
`on(allOf(...), ...)` replaces `join(...)` exactly as it does above.

A duplicate-typed pair whose count is not a literal needs you to work out which count wins before you write the
`event(...)` leaf. `join(List.of(Expectation.of(Type.class, a), Expectation.of(Type.class, b)), ...)` always meant
whichever of `a` and `b` is larger, so `event(Type.class, Math.max(a, b))` is the direct translation in Java, and
the Kotlin equivalent reads the same way.

## 2. A flow saga's `stepWindow` now caps only its own declared events

No recipe, and most callers need to do nothing. This only matters if your flow sets a
`narrowingFilter`, a `replacementFilter` wider than the flow's own declared types, or uses a
`CloudEventTypeMapper` that collapses several domain types onto one CloudEvent type string.

The 0.33.0 upgrade guide's [section 9](upgrading-to-0.33.0.md#9-a-flow-saga-can-cap-the-events-of-the-step-it-is-parked-in)
and [section 10's replacement-filter caveat](upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused)
describe `stepWindow` as it shipped in 0.33.0, where every correlated event counted toward the cap
regardless of whether any step declared its type. That let an event outside a flow's own declared
types evict one of the step's own events, and the absolute bound section 9 states,
`historyWindow + 2 * stepWindow + 1`, held because of that same defect.

`stepWindow` now counts and evicts only events of a type some step's `on(...)` branch or
window-condition leaf actually names. An event of any other type is still retained, never
discarded, but it no longer takes one of the cap's slots or evicts a declared event to make room
for itself. The bound in section 9 still holds for a flow's own declared-type events. It no longer
bounds a step fed only events of a type no step declares, which is not a new gap. It was always the
kind of growth `stepWindow` and `historyWindow` alone did not close, only masked. Watch the
0.33.0 store-boundary warning if your flow admits such events and you care about total document
size. See [ADR 129](../architecture/decisions/0129-a-flow-sagas-stepwindow-caps-only-its-own-declared-events.md)
for the full decision.
