# 121. A saga timer's name carries its namespace

Date: 2026-08-10

## Status

Accepted. Designs #716, which stays open for the implementation. Ships in 0.33.0, which is held for it.

120 was the highest number claimed anywhere at write time. The check covered every remote branch (51 of them), every
local branch, and the open pull requests, not `main` alone, because this repository has already had one collision from
a number claimed on a branch that never merged.

Amends [ADR 63](0063-saga-dsl.md) on one of the three questions it deferred `compose` over.

## Context

A saga timer is identified by a string. The core DSL takes one from the user, `startTimeout("payment",
ofMinutes(30))`, and the flow DSL builds one per step by putting `step:` in front of the step name. Both end up in the
same flat map of pending timers, and nothing ever reads the prefix back off.

`FlowSagaImpl` writes that string in four places and reads it in none. `TIMER_PREFIX` is declared at
`FlowSagaImpl.java:46`, joined to a step name when a step arms its timeout (`:351`), joined again when a transition
cancels the previous step's timeout (`:336`), and joined a third time so the result can be compared with `equals`
against the name of the timer that just fired (`:191`). The prefix is a formatting rule that exists so two strings
built the same way match.

That would be a private detail if it stayed inside the file, and it does not. A test that fires a step timeout writes
the prefix itself, because `SagaInput.timeout(new SagaTimeout("game-1", "step:awaiting-players"))` is the only way to
name the timer. Issue #716 is that call. `SagaInstance` takes the opposite position on purpose and says so in its
javadoc at `SagaInstance.java:62`, where a timer's name is not exposed because which timers a saga arms is part of how
the process is implemented. The API a test writes against is the one place that contradicts that, and what it hands
the reader is a formatting rule the flow DSL never promised to keep.

The collision this invites is already written down. ADR 63 defers `compose` partly because two child sagas "may use
timer names from the same namespace and collide" (`0063-saga-dsl.md:117`), and leaves composition undesigned until
correlation, timer namespacing, and terminal semantics across children have an answer (`:173`). A prefix concatenated
into a shared map is what a namespace looks like when nobody has decided it is one.

## Decision

### A timer's name is a value with two shapes

```java
public sealed interface TimerName permits TimerName.Simple, TimerName.Qualified {
    record Simple(String name) implements TimerName { }
    record Qualified(String namespace, String name) implements TimerName { }

    static TimerName of(String name) { ... }
    static TimerName of(String namespace, String name) { ... }

    String encode();
}
```

`Simple` is a name on its own, `Qualified` is a name inside a namespace. Cases are nested in the sealed type's own
file, the style `SagaEffect` already uses (`SagaEffect.java:35,56`). The flow DSL's step timers become
`Qualified("step", stepName)`, and `FlowSagaImpl.TIMER_PREFIX` becomes the namespace constant `"step"` with the colon
gone from it. Core never mentions `step` anywhere.

### There is one way to read a stored name, and every string goes through it

`TimerName.of(String)` is the only way to get a `TimerName` out of a string. It splits at the first `:`, so `"payment"`
gives `Simple("payment")` and `"step:awaiting-players"` gives `Qualified("step", "awaiting-players")`. The string
overloads that already exist call it too, so a name means the same thing wherever it enters the API.

That is what keeps a 0.32.0 caller working. Someone who armed `startTimeout("a:b", ofMinutes(5))` and registered
`reactOnTimeout("a:b", ..)` gets `Qualified("a", "b")` on both sides, so the two still match and the timer still fires.
If only one side read the colon, their timer would stop firing with no error anywhere, which is the failure this change
exists to remove rather than to reintroduce in a new place.

**Every string is a name, including the awkward ones.** `":x"` gives `Qualified("", "x")`, an empty namespace. `"x:"`
gives `Qualified("x", "")`, an empty name. `""` gives `Simple("")`. `"a:b:c"` splits once, at the first colon, giving
`Qualified("a", "b:c")`, so a name may contain a colon and a namespace never can. `null` throws
`NullPointerException`, the same as everywhere else in this module.

`of` never throws for any other reason. It runs on a name read back out of a database, so a string it refused would
turn stored data into an exception on the path that fires timers, for a saga that was working before the upgrade.

### The two round trips need two different rules

`Simple` refuses a colon in its name and `Qualified` refuses one in its namespace, throwing `IllegalArgumentException`
from the compact constructor. These are public records, so `new Simple("a:b")` compiles, and without the refusal it
would build a value that writes itself out as `"a:b"` and reads back as `Qualified("a", "b")`.

Refusing the separator was first written down as an alternative to letting `of` read every string the same way, and
the two are not alternatives, because they hold up different halves of the same property.

- `of(x.encode()).equals(x)`, a value surviving its trip to storage, holds because `Simple` and `Qualified` refuse the
  values that would come back as something else.
- `of(s).encode().equals(s)`, a stored string surviving its trip through the API, holds for every string because `of`
  splits at the first colon and `encode` puts the same colon back.

`of` on its own is what the second needs, and it leaves `new Simple("a:b")` constructible, so the first breaks. Both
rules ship. The refusal costs the 0.32.0 caller nothing, because they never write `new Simple(..)`, they write
`startTimeout("a:b", ..)`, which goes through `of`.

One rule generates all of it. A value can be built directly exactly when `of` can produce it.

### The string exists below `TimerEntry` and nowhere above it

Two lines of code cross that boundary. `SagaExecutionSupport.applyEffects` turns a timer effect's name into the key of
the pending-timer map when a timer is armed or cancelled (`SagaExecutionSupport.java:204,206,207`), and
`SagaExecution` turns a due `TimerEntry`'s name back into a `SagaTimeout` when the timer fires
(`SagaExecution.java:118`).

The store does neither. `SpringMongoSagaStateStore` reads and writes `TimerEntry.name` as the `String` it already is,
so no store implementation changes and no stored document changes. `step:awaiting-players` stays exactly as persisted
since 0.32.0.

That is also the whole migration argument. A saga instance sitting in Mongo right now with `step:awaiting-players`
pending is read back the way it always was, and the name becomes `Qualified("step", "awaiting-players")` at
`SagaExecution`, which is the value the flow DSL matches on. One with `payment` pending becomes `Simple("payment")`,
which is what `reactOnTimeout("payment", ..)` registered under, because that registration went through `of` as well.
No in-flight instance is stranded, because both sides of every comparison read the stored string the same way. A test
that loads a 0.32.0-shaped envelope and fires its timer ships with the implementation, and it is the check that this
paragraph is true.

### `SagaTimeout.timerName()` returns the value

`SagaTimeout` becomes `record SagaTimeout(String sagaId, TimerName timerName)`. The accessor keeps its name and
changes its type.

Adding a `timerId()` beside a `timerName()` that still hands back a string was rejected, because it leaves two names
for one thing and the string one is exactly what a caller should stop reaching for. `timeout.timerName().startsWith(
"step:")` is the defect, and keeping the accessor that makes it compile keeps the defect available.

This does not compile against a caller who reads `timerName()` as a `String`, or who writes `new SagaTimeout(id,
"payment")`. The saga DSL shipped one release ago in 0.32.0, and the accessor's type is the whole point of the change,
so the break is taken now rather than carried. A handler registered for one timer already knows which timer fired, so
the accessor is mostly read for logging, and `TimerName.encode()` gives the old string back for that.

`sagaId` stays. The core DSL hands the whole `SagaTimeout` to the handlers registered by `evolveOnTimeout` and
`reactOnTimeout` (`Saga.java:474,489`), so removing it would take away something those handlers can read. It is unused
only on the flow path, where `evolve` reads just the timer name (`FlowSagaImpl.java:128`).

### The flow DSL hands out a step's timer name, and tests use it

`FlowSaga.stepTimer(String)` returns `Qualified("step", stepName)` and is the one new symbol in the flow DSL. Core adds
`SagaInput.timeout(String sagaId, TimerName timerName)`, so a test reads:

```kotlin
lobby.step(started.state, SagaInput.timeout("game-1", stepTimer("awaiting-players")))
assertThat(step.timerEffects()).containsExactly(SagaEffect.cancelTimeout(stepTimer("awaiting-players")))
```

The same symbol appears on both sides, and the only string in it is the step's own name, which the user wrote. For a
core DSL saga the same overload reads `SagaInput.timeout("order-1", TimerName.of("payment"))`, and the existing
`SagaInput.timeout(SagaTimeout)` stays for anyone already building one.

`timeoutEvolvers` and `timeoutReactors` (`Saga.java:379,380`) become `Map<TimerName, ..>`. The duplicate-registration
check then compares values, so `evolveOnTimeout("a:b", ..)` and `evolveOnTimeout(TimerName.of("a", "b"), ..)` on one
builder are recognised as the same timer and the second throws.

### Naming

**`TimerName`, not `TimerId`.** This repository uses `id` for the identity of one live thing, `sagaId`,
`subscriptionId`, `subscriberId`. A timer's identity is not that. It is chosen when the saga is defined, and every
instance of that saga arms a timer under the same one, so `TimerId` would read as the identity of one armed timer,
which no type in this design has. The vocabulary is already `timerName`, on the accessor, on the parameters of
`startTimeout`, `evolveOnTimeout` and `reactOnTimeout`, and in ADR 63's "timer namespacing". `TimerName` keeps it, and
`SagaTimeout.timerName()` changes type without changing its name.

**`Simple` and `Qualified`, not `Named` and `Qualified`.** Both shapes have a name, so `Named` does not say which one
it is. Simple against qualified is the pairing Java already uses for a class's own name against the one with its
package in front (`Class.getSimpleName`), so a Java reader arrives with the distinction.

**`of`, not `parse`.** `parse` suggests there is a second way in that does not read the colon, and there deliberately
is not. One name with one or two arguments also follows `event(...)` in [ADR 120](0120-a-step-condition-is-a-monotone-matcher-tree.md)
and `startTimeout(...)` here, rather than growing a family of differently named constructors. `of`'s javadoc states
that a colon is read, with `"step:awaiting-players"` as the example.

### The string overloads are not deprecated in this release

`startTimeout(String, Duration)`, `startTimeoutAt(String, Instant)`, `cancelTimeout(String)`, `evolveOnTimeout(String,
..)` and `reactOnTimeout(String, ..)` all stay, and none of them gets `@Deprecated`.

Deprecation says an API is wrong to use, and this one is not. `startTimeout("payment", ofMinutes(30))` in a core DSL
saga names a timer with no namespace, and it reads the same as it always did. The defect was never that a name is a
string, it was that the flow DSL's formatting rule reached a caller who should not have had to know it, and a typed
name is what removes that.

Deprecating them would also put a warning on most correct saga code and on the documentation's own examples, for a
spelling that will keep being the shortest right one. The overloads are additive, so nothing forces the decision now,
and a later release can still deprecate without rework.

### This unblocks `compose`'s namespacing, it does not answer it

ADR 63 defers `compose` on three things, correlation keys, timer namespacing, and what a terminal transition in one
child means for a sibling that is not finished. This gives the second one a mechanism and leaves the other two where
they were. Two children can now arm timers that do not collide, because a name is a namespace and a name rather than a
string that happens to have a prefix on it.

What it does not decide is who assigns a child's namespace, whether that comes from the child's position, its name, or
the caller, and what happens when a composed saga is itself composed. Splitting at the first colon means a nested name
is representable, `Qualified("a", "b:c")` writes itself out as `"a:b:c"`, but nothing here designs it.

One constraint goes forward from this. `step` is now claimed by the flow DSL, so whatever assigns namespaces to
composed children has to keep away from it, or the two collide in the way this change removes.

## Rejected alternatives

**`SagaInput.timeout(String sagaId, String timerName)`, two plain strings.** This is the sharpest illustration of the
defect, which is why it is written down rather than left out. `SagaInput` is core and cannot tell whether the saga it
feeds is a flow saga, so the only thing it can do with that second string is read it the one way, giving
`Simple("awaiting-players")`. The flow DSL matches `Qualified("step", "awaiting-players")`. The two are not equal, the
input is consumed without doing anything, and nothing throws. A call that looks right and quietly does nothing is
worse than the prefix it was meant to remove. The same argument rules out keeping a `SagaTimeout(String, String)`
constructor. Requiring a `TimerName` makes it impossible to get wrong, because the only way to name a step's timer is
to ask the flow DSL for it.

**A helper that builds the string, `stepTimerName("awaiting-players")` returning `"step:awaiting-players"`.**
Considered first and rejected by the maintainer. It hands the caller the formatted string back, so the prefix is still
in the caller's hands and the encoding is still part of the API. It would ship only to be removed by this design.

**A sealed `TimerName` with a `Step` case contributed by the flow DSL.** There is no `module-info.java` anywhere in
this repository, so everything is in the unnamed module and a sealed type's permitted subtypes have to be in the same
package. The flow DSL is in `org.occurrent.dsl.saga.flow` and core is in `org.occurrent.dsl.saga`, so it cannot add a
case. The restriction points the right way anyway, because `compose` wants a namespace per child, decided when the
composition is built, and a fixed set of classes cannot give it one.

**A flow DSL factory for the whole input, `stepTimedOut(String)` returning a `SagaInput<E>`.** Compared against
`stepTimer` with both call sites written out. It shortens the line that fires a timer and does nothing for the line
that asserts a timer effect, which still needs the name, so it adds a second flow symbol rather than replacing the
first. It also hides `sagaId`, which would make `SagaTimeout` either invent one or make it optional, and that is a
change to the core model with reasons of its own. Recorded here so it can be added on top of `stepTimer` later without
rework, if the firing line reads badly in practice.

**A helper that drives the saga, `fireStepTimeout(lobby, state, "awaiting-players")` returning a `Saga.Step`.** A
second way to drive a saga, competing with `Saga.step`, still needing the name for assertions, and it would stop the
testing chapter showing that a fired timer is an ordinary input like any event.

**Splitting a stored name at the last colon.** `"a:b:c"` would give `Qualified("a:b", "c")`, whose namespace contains a
colon, and writing that out and reading it again gives `Qualified("a", "b:c")`. The value would not survive its own
trip to storage.

**Dropping `sagaId` from `SagaTimeout` while its shape is open anyway.** Core DSL handlers receive it
(`Saga.java:474,489`). Only the flow path ignores it.

## Consequences

- A test names a step's timer with `stepTimer("awaiting-players")` and never writes `step:`. The prefix stops being
  part of anything a user can see, which is what `SagaInstance`'s javadoc already said should be true of timer names.
- Nothing on disk changes. The stored name is the same string it has been since 0.32.0, every `SagaStateStore`
  implementation is untouched, and an instance with a pending timer keeps firing across the upgrade.
- `SagaTimeout.timerName()` changes type, so a caller reading it as a `String` or building a `SagaTimeout` from two
  strings has to change. `TimerName.encode()` is the string they were reading.
- The core DSL's string API keeps working and keeps compiling without warnings. A saga that only ever uses plain timer
  names needs no change at all.
- The flow DSL's four string-joining sites become one namespace constant and a comparison between two values, and
  `FlowSagaImpl` stops building a string it never reads.
- `compose` keeps its deferral, with one of its three open questions now holding a mechanism instead of a gap, and
  `step` recorded as a namespace the flow DSL has taken.
- One test keeps asserting the stored string on purpose
  (`SpringMongoSagaStateStoreMongoTest.java:418,486,494`), because it is what says the stored form did not change.
