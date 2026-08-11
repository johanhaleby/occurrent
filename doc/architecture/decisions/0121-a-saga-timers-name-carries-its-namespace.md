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

`FlowSagaImpl` builds that string in four places and never takes it apart again. `TIMER_PREFIX` is declared at
`FlowSagaImpl.java:46`, joined to a step name when a step arms its timeout (`:351`), joined again when a transition
cancels the previous step's timeout (`:336`), and joined a third time so the result can be compared with `equals`
against the name of the timer that just fired (`:191`). That last one reads the string, but only to match it against
another string built the same way. The prefix is a formatting rule that exists so those two agree.

That would be a private detail if it stayed inside the file, and it does not. A test that fires a step timeout writes
the prefix itself, because `SagaInput.timeout(new SagaTimeout("game-1", "step:awaiting-players"))` is the only way to
name the timer. Issue #716 is that call. `SagaInstance` takes the opposite position on purpose and says so in its
javadoc at `SagaInstance.java:62-63`, where a timer's name is not exposed because which timers a saga arms is part of
how the process is written rather than of its observable lifecycle. The API a test writes against is the one place
that contradicts that, and what it hands the reader is a formatting rule the flow DSL never promised to keep.

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

    static TimerName parse(String name) { ... }              // total, splits at the first colon
    static TimerName of(String namespace, String name) { ... }

    String encode();
}
```

`Simple` is a name on its own, `Qualified` is a name inside a namespace. Cases are nested in the sealed type's own
file, the style `SagaEffect` already uses (`SagaEffect.java:35,56`). The flow DSL's step timers become
`Qualified("step", stepName)`, and `FlowSagaImpl.TIMER_PREFIX` becomes the namespace constant `"step"` with the colon
gone from it. Core never mentions `step` anywhere.

**`toString()` on both records returns `encode()`.** A record's generated `toString` would print
`Qualified[namespace=step, name=awaiting-players]`, and every place that puts a timer name into a log keeps compiling
while changing what it prints, because a logger takes `Object`. This repository has one already.
`SagaExecutionSupport.java:125-126` warns when a fired timer changes nothing, a message written to help a user find a
timer name that does not match its registration, and it passes the name straight in. It prints
`step:awaiting-players` today and has to keep doing that. `TimerName` is an interface, so `toString` cannot be
inherited from it and each record overrides it.

### There is one way to read a name out of a string, and every string goes through it

`TimerName.parse(String)` is the only way to turn a string into a `TimerName`. It splits at the first `:`, so
`"payment"` gives `Simple("payment")` and `"step:awaiting-players"` gives `Qualified("step", "awaiting-players")`.
Every string-taking method in the API calls it, so a name means the same thing wherever it enters.

That is what keeps a 0.32.0 caller working. Someone who armed `startTimeout("a:b", ofMinutes(5))` and registered
`reactOnTimeout("a:b", ..)` gets `Qualified("a", "b")` on both sides, so the two still match and the timer still fires.
If only one side read the colon, their timer would stop firing with no error anywhere, which is the failure this change
exists to remove rather than to reintroduce in a new place.

**Every string is a name, including the awkward ones.** `":x"` gives `Qualified("", "x")`, an empty namespace. `"x:"`
gives `Qualified("x", "")`, an empty name. `""` gives `Simple("")`. `"a:b:c"` splits once, at the first colon, giving
`Qualified("a", "b:c")`, so a name may contain a colon and a namespace never can. `null` throws
`NullPointerException`, the same as everywhere else in this module.

`parse` never throws for any other reason. It runs on a name read back out of a database, so a string it refused would
turn stored data into an exception on the path that fires timers, for a saga that was working before the upgrade.

### The two round trips need two different rules

`Simple` refuses a colon in its name, and `Qualified` and `of(namespace, name)` refuse one in the namespace, throwing
`IllegalArgumentException`. These are public records, so `new Simple("a:b")` compiles, and without the refusal it would
build a value that writes itself out as `"a:b"` and reads back as `Qualified("a", "b")`.

Refusing the separator was first written down as an alternative to letting `parse` read every string the same way, and
the two are not alternatives, because they hold up different halves of the same property.

- `parse(x.encode()).equals(x)`, a value surviving its trip to storage, holds because `Simple` and `Qualified` refuse
  the values that would come back as something else.
- `parse(s).encode().equals(s)`, a stored string surviving its trip through the API, holds for every string because
  `parse` splits at the first colon and `encode` puts the same colon back.

`parse` on its own is what the second needs, and it leaves `new Simple("a:b")` constructible, so the first breaks. Both
rules ship. The refusal costs the 0.32.0 caller nothing, because they never write `new Simple(..)`, they write
`startTimeout("a:b", ..)`, which goes through `parse`.

One rule generates all of it. A value can be built directly exactly when `parse` can produce it.

### `SagaEffect`'s three timer records carry the value

`StartTimeout`, `StartTimeoutAt` and `CancelTimeout` (`SagaEffect.java:65,77,85`) take a `TimerName` instead of a
`String`. The alternative is that they keep the string and the flow DSL calls `encode()` every time it arms or cancels
a step timeout, which puts the concatenation straight back where this change took it out, and which would stop
`SagaEffect.cancelTimeout(stepTimer("awaiting-players"))` compiling. That assertion is the call site this design was
chosen for, so the records change.

The three static factories keep their string overloads and gain `TimerName` ones, so `startTimeout("payment",
ofMinutes(30))` still compiles and now builds `StartTimeout(Simple("payment"), ..)`. What does not survive is
deconstructing an effect against a `String` component. A reaction test that pattern-matches
`case SagaEffect.CancelTimeout<C>(String name)` over `Saga.Step.timerEffects()` has to bind a `TimerName` instead.

### Where the string is, and where the value is

The pending-timer map and `SagaEnvelope.TimerEntry` stay keyed by the encoded string, and so does the store. Above
them, `SagaEffect`, `SagaTimeout` and the registration maps hold `TimerName`. Five places in the executor sit on that
line, and they are worth naming individually, because the design is only correct if they all read a name the same way.

- `SagaExecutionSupport.applyEffects` turns an armed or cancelled timer's name into the map key
  (`SagaExecutionSupport.java:204,206,207`).
- `SagaExecutionSupport:139` removes the fired timer from that map by the same key. This is the one with the worst
  failure mode. A key that disagrees with the one `applyEffects` wrote leaves the timer in the map, and a one-shot
  timer that is never consumed fires again on every poll for as long as the instance lives.
- `SagaExecutionSupport:126` puts the fired timer's name into the warning about a name that matches no registration,
  which is why `toString` has to return `encode()`.
- `SagaExecution:118` turns a due `TimerEntry`'s name into a `SagaTimeout` when the timer fires. This is the only
  place a stored string becomes a `TimerName`.
- `SagaExecution:114-118` and `:175-180` keep comparing encoded strings when they work out which timers are due and
  whether one still is. They are unchanged, deliberately. Due-ness is a question about a stored entry, not about a
  name's shape, and `hasDueTimer` compares against the same `TimerEntry.name` it read.

No store is on that list. `SpringMongoSagaStateStore` reads and writes `TimerEntry.name` as the `String` it already
is, so no store implementation changes and no stored document changes. `step:awaiting-players` stays exactly as
persisted since 0.32.0.

That is also the whole data migration argument. A saga instance sitting in Mongo right now with `step:awaiting-players`
pending is read back the way it always was, and the name becomes `Qualified("step", "awaiting-players")` at
`SagaExecution`, which is the value the flow DSL matches on. One with `payment` pending becomes `Simple("payment")`,
which is what `reactOnTimeout("payment", ..)` registered under, because that registration went through `parse` as
well. No in-flight instance is stranded, because both sides of every comparison read the stored string the same way. A
test that loads a 0.32.0-shaped envelope and fires its timer ships with the implementation, and it is the check that
this paragraph is true.

### The break gets a recipe and a migration guide section

Occurrent's rule for an API that has already shipped is in `AGENTS.md:75`, an `org.occurrent.UpgradeToOccurrent_*`
OpenRewrite recipe plus an entry under `doc/migration/upgrading-to-*.md`. The saga DSL shipped in 0.32.0, so this
change owes both, and 0.33.0's three other breaks already have theirs in `doc/migration/upgrading-to-0.33.0.md`.

A recipe can rewrite some of it and not all of it, so it follows `MigrateStreamToList_0_30`, which rewrites what it
can prove and leaves a `TODO` comment on the rest for a human to finish.

- `new SagaTimeout(sagaId, name)` becomes `new SagaTimeout(sagaId, TimerName.parse(name))` wherever the second
  argument is a `String`. This one is provable and total, because `parse` gives the value the old string already
  meant.
- Reading `timerName()` into a `String` becomes `timerName().encode()` where the recipe can see the target type. Where
  it cannot, it leaves a `TODO` comment, and the compiler points at the rest.
- Deconstructing a `SagaEffect` timer record against a `String` component is left for a human. A record pattern's
  binding type is a judgment about what the surrounding code then does with it.

The guide section covers all three by hand as well, since a Kotlin caller gets no help from the recipe. The
`StartAt.subscriptionPosition` renames in 0.30.0 already ran into that limitation of `rewrite-kotlin` and documented
it as a manual step.

### `SagaTimeout.timerName()` returns the value

`SagaTimeout` becomes `record SagaTimeout(String sagaId, TimerName timerName)`. The accessor keeps its name and
changes its type.

Adding a `timerId()` beside a `timerName()` that still hands back a string was rejected, because it leaves two names
for one thing and the string one is exactly what a caller should stop reaching for. `timeout.timerName().startsWith(
"step:")` is the defect, and keeping the accessor that makes it compile keeps the defect available.

The compile error lands on an assignment to a `String`, on passing the result where a `String` parameter is declared,
and on `new SagaTimeout(id, name)` itself. It does not land on a logging call, because a logger takes `Object`, which is why
`toString` returning `encode()` is part of this decision rather than a detail left to the implementation. A handler
registered for one timer already knows which timer fired, so the accessor is mostly read for logging, and
`encode()` is the string for anywhere that genuinely needs one.

The saga DSL shipped one release ago in 0.32.0, and the accessor's type is the whole point of the change, so the break
is taken now rather than carried, with the recipe and guide above as the migration path.

**`SagaTimeout` has one constructor, and it takes a `TimerName`.** There is no two-string constructor calling `parse`
alongside it. This went beyond what the approved plan scoped, which was the accessor's type alone, and the maintainer
ruled on it with the alternative written out, on the grounds that it is the better shape to live with.

The reason is the recipe rather than the trap. The constructor is the one part of this migration a recipe can rewrite
completely and provably, since `parse` gives back the value the old string already meant, so every existing call site
is fixed mechanically. That is what the migration convention exists for, and a compatibility constructor kept for a
population the recipe already reaches would earn its place only by being permanent.

The silent-no-op argument that rules out `SagaInput.timeout(String, String)` below does not carry here on its own, and
it is worth saying so, because the two look alike. That overload would be new API whose only use is the trap. This
constructor had correct existing callers, including a flow-saga test passing `"step:awaiting-players"`, which `parse`
reads correctly.

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
core DSL saga the same overload reads `SagaInput.timeout("order-1", TimerName.parse("payment"))`, and the existing
`SagaInput.timeout(SagaTimeout)` stays for anyone already building one.

`timeoutEvolvers` and `timeoutReactors` (`Saga.java:379,380`) become `Map<TimerName, ..>`. The duplicate-registration
check then compares values, so `evolveOnTimeout("a:b", ..)` and `evolveOnTimeout(TimerName.of("a", "b"), ..)` on one
builder are recognised as the same timer and the second throws.

### Kotlin gets the same overloads

`SagaExtensions.kt` has string-taking `evolveOnTimeout` and `reactOnTimeout` (`:98,103`) and `startTimeout`,
`startTimeoutAt` and `cancelTimeout` on `SagaEffects` (`:140,143,146`). Each gains a `TimerName` overload beside the
string one. Without them a Kotlin core DSL saga could only reach a namespaced timer by writing `"a:b"`, which is the
concatenation this change exists to take away, and it would be the only place in the API where that is still the
answer. This repository mirrors its Java statics in Kotlin, which is what ADR 120 did for step conditions.

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

**`parse(String)` and `of(String, String)`, two names for two different operations.** One overloaded `of` was tried
first, on the reasoning that a single door is easier to trust than two. It is not one door. `of(String)` and
`of(String, String)` differ in the thing that matters most about them, since the first never throws for any string and
the second refuses a colon in the namespace. Hiding that behind one name would put the API's least obvious rule on the
argument count.

So they get their own names. `parse` reads a name out of a string that may already carry a namespace, and it always
succeeds. `of` builds one from parts that are already separated, and it refuses a namespace it could not write back
out. `TimerName.parse("payment")` reads slightly oddly for a literal with no colon in it, and that is accepted,
because reading the string is exactly what happens.

This is not the trio [ADR 120](0120-a-step-condition-is-a-monotone-matcher-tree.md) rejected in favour of one
overloaded `event(...)`. Those three did the same thing with more arguments, so one name fit. These two do different
things.

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

### `compose` gets a way to write a namespace down, and nothing more

ADR 63 defers `compose` on three things, correlation keys, timer namespacing, and what a terminal transition in one
child means for a sibling that is not finished. This gives the second one a way to represent a namespace and leaves
all three deferred.

It is worth being exact about how little that is, because a namespace that exists as a value is easy to mistake for a
namespace that has been assigned. Two composed flow sagas both arm `Qualified("step", "awaiting-approval")` for a step
of that name, because the namespace is fixed at `"step"` by the flow lowering and nothing varies it per child. They
collide exactly as ADR 63 warned. What has changed is that the colliding thing is now a value with a namespace in it,
so an operation that rewrites a child's timer names before combining them has somewhere to put the child's identity.
That operation does not exist, and designing it is designing `compose`.

Nothing here decides who assigns a child's namespace, whether it comes from the child's position, its name, or the
caller, or what happens when a composed saga is itself composed. Splitting at the first colon means a nested name is
representable, `Qualified("a", "b:c")` writes itself out as `"a:b:c"`, but nothing here designs that either.

One constraint goes forward from this. `step` is now claimed by the flow DSL, so whatever assigns namespaces to
composed children has to keep away from it, or the two collide in the way this change is meant to make expressible.

## Rejected alternatives

**`SagaInput.timeout(String sagaId, String timerName)`, two plain strings.** This is the sharpest illustration of the
defect, which is why it is written down rather than left out. `SagaInput` is core and cannot tell whether the saga it
feeds is a flow saga, so the only thing it can do with that second string is read it the one way, giving
`Simple("awaiting-players")`. The flow DSL matches `Qualified("step", "awaiting-players")`. The two are not equal, the
input is consumed without doing anything, and nothing throws. A call that looks right and quietly does nothing is
worse than the prefix it was meant to remove. Requiring a `TimerName` makes it impossible to get wrong, because the
only way to name a step's timer is to ask the flow DSL for it.

**A `SagaTimeout(String sagaId, String timerName)` constructor kept beside the `TimerName` one, calling `parse`.** It
would have kept every 0.32.0 caller compiling with its meaning exactly preserved, so unlike the overload above it had
a real population of correct callers and could not be dismissed by the same argument. Rejected because the recipe
rewrites those call sites completely and provably, which leaves the constructor with nothing to do except stay
forever, and because it would leave `new SagaTimeout("game-1", "awaiting-players")` reading like a way to name a flow
step timer when it gives `Simple("awaiting-players")` and matches nothing.

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
- This is a source-breaking release for saga code, and it owes an `UpgradeToOccurrent_0_33` recipe and a section in
  `doc/migration/upgrading-to-0.33.0.md` like 0.33.0's three other breaks. The recipe rewrites the `SagaTimeout`
  constructor and the three timer-effect record constructors completely, does what it can prove for the accessor,
  and leaves a `TODO` comment on the rest.
- Five kinds of call site have to change. Reading `timerName()` into a `String`, building a `SagaTimeout` from two
  strings, constructing `StartTimeout`, `StartTimeoutAt` or `CancelTimeout` directly with a string timer name,
  deconstructing a `SagaEffect` timer record against a `String` component, and implementing a `Saga` by hand against
  the registration maps. A saga that only builds timer effects and registrations by calling the string-taking
  methods compiles unchanged.
- Anything that logs a timer name keeps compiling and keeps printing the same text, because `toString` returns
  `encode()`. This is the reason `toString` is a decision here rather than an implementation detail.
- The flow DSL's four string-joining sites become one namespace constant and a comparison between two values, and
  `FlowSagaImpl` stops building a string it only ever compares.
- `compose` stays deferred on all three of its open questions. Timer namespacing gained a representation, not an
  assignment, and two composed flow children with a step of the same name still collide. `step` is recorded as a
  namespace the flow DSL has taken.
- One test keeps asserting the stored string on purpose
  (`framework/spring-boot-starter-mongodb/.../SpringMongoSagaStateStoreMongoTest.java:418,486,494`), because it is what
  says the stored form did not change.
