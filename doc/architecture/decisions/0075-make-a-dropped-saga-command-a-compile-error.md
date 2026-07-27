# 75. Make a dropped saga command a compile error in the Kotlin DSL

Date: 2026-07-26

## Status

Accepted. Changes the unreleased Kotlin surface of the saga DSL (ADR 63). Java is extended additively, with no-commands overloads on `StepBuilder` and a metadata-carrying `startsOn`, but the compile-error behaviour itself is Kotlin-only.

## Context

A Kotlin saga reaction collects its effects into a receiver and returns `Unit`. A core reaction is
`SagaEffects<C>.(S, T) -> Unit` and a flow reaction is `FlowReactions<C>.(T) -> Unit`, so the body calls `issue(command)`
for each command and the receiver accumulates them.

A Kotlin lambda whose expected return type is `Unit` silently discards the value of its last expression. So this
compiles, does nothing at all, and produces no warning:

```kotlin
react<PaymentReserved> { _, e -> ShipOrder(e.orderId) }
```

The reaction produces a command and throws it away. Nothing in the type system objects, because any expression is
acceptable where `Unit` is expected. A reader cannot tell the difference between this and a correct reaction by looking.

This is the second instance of one class of bug on this surface. `FlowSaga.startsOn` previously took a correlator and a
start reaction as two defaultable lambdas, so moving a brace turned a correlation rule into a command dispatch and both
spellings compiled. That was fixed by removing the parameter. Both are cases where a Kotlin-only convenience had types
that could not distinguish the intended use from the mistake.

The Java side never had the problem. `Saga.react` returns `List<SagaEffect<C>>` and a flow step's `on` takes
`Function<T, List<C>>`, so a Java reaction has always been an expression whose value is used.

## Decision

**The lambda returns the receiver instead of `Unit`.** `issue` accumulates into the receiver's list, as it does today,
and returns the receiver, so the lambda's return type is something only the receiver can produce. The imperative style
that reads well for several commands is unchanged:

```kotlin
on<PaymentReserved>(then = end) {
    issue(ReserveRemainder(it.orderId))
    issue(ShipOrder(it.orderId))
}
```

The bug was never the receiver or the imperative style. It was `Unit` swallowing any expression.

Verified against the shipped API rather than a sketch. The whole build compiles, including the `order-fulfillment`
example, which was not edited at all. Reintroducing the bug into that example is now a compile error:

```
OrderFulfillmentFlowSaga.kt:45:58 Return type mismatch: expected 'FlowReactions<OrderCommand>', actual 'ShipOrder'.
```

**A body whose last statement is not an `issue(...)` ends with `nothing`.** That is the receiver itself under a name that
reads better than `this`, and it is needed because a trailing `if` without an `else` has type `Unit`. A reaction with no
commands at all takes no lambda instead, through the defaulted `on<T>(then = ...)` described below.

`nothing` is a normal part of the vocabulary rather than an escape hatch. A reaction that issues a command in some cases
and none in the others is an ordinary thing to write. An earlier draft called that case rare on the grounds that no
reaction in this repository needs one, which was true and beside the point: this is a published library, so its own
examples are not the population of what callers write. The four spellings that compile are an `if` with an `else` branch
of `nothing`, a trailing `nothing` on its own line, a conditional followed by an unconditional command, and a `when` whose
branches all end on a command or `nothing`.

The name was a close call between `nothing`, `noMore` and `nothingElse`, and it went back and forth before settling.
`nothing` wins the call site that should be the house style, the `if` with an `else` branch, where "otherwise nothing"
is simply how you would say it. `noMore` and `nothingElse` both scope themselves more tightly to the effects, and both
read better than `nothing` in the trailing position, after a line that already issued a command.

The trailing position decided it, in the sense that it is the rarer of the two and the documentation leads with the
`else` form. Names from the continuation vocabulary (`done`, `stop`, `complete`) were excluded outright, because a step
already has `end`, `next` and `transitionTo` one line away and any of those would read as flow control. `noCommands` and
`noEffects` were excluded because the core DSL deals in effects and the flow DSL in commands, so neither word is neutral
across the two.

The known weakness of `nothing` is that it can be read as "nothing happens here", when the branch does still fire and
still follows its continuation, and only the command list is empty. The name does not carry that, so the documentation
states it in the sentence that introduces the word. Anyone revisiting this should know it was decided on how the common
call site reads, not on the word being unambiguous.

**A flow branch with no commands takes no lambda.** The event-only `on`, `startsOn`, `join` and `timeout` default their
reaction to `{ nothing }`, so a step that only advances reads `on<PlayerJoined>(then = end)`. The metadata-carrying
siblings keep their lambda required, because defaulting both would make the no-lambda call match two candidates with
neither more specific.

That default lives on the existing parameter rather than on a separate no-reaction overload, which was tried first and
does not work. A separate `on(then, onlyIf)` overload puts `onlyIf` last, so a two-parameter trailing lambda becomes
ambiguous between that overload's guard and the metadata-carrying reaction, both of which take two parameters. Defaulting
the parameter keeps the reaction last in every overload, so a trailing lambda always binds to it.

The Java `StepBuilder` gains the matching no-commands overloads, since `FlowSaga.Builder.startsOn(Class)` already existed
as exactly this convenience and its absence on `on` was the inconsistency. Java has no default arguments, so there the
overloads are the only way to express it, and no ambiguity arises because Java resolves on arity and declared types
rather than on a trailing lambda.

**A start reaction can read the starting event's metadata.** `evolve`, `react`, `onStart` and the flow `on` all had a
metadata-carrying form and `startsOn` did not, so a start reaction could not read the initiating event's stream id or
position, which is where a correlation-adjacent value is most likely wanted. `FlowSagaImpl.onStart` already received the
metadata and discarded it, so this is a `BiFunction` overload on the builder and one changed call site.

Two related asymmetries were considered and deliberately left. A flow guard still cannot read metadata, because closing
that would give the two `on` overloads different guard shapes and trade one asymmetry for another. The blocking DCB
projection runner still has no caller-supplied update overload, because it never had the primitive form at all, so
adding one is new capability rather than a gap.

**Rejected: making reactions return their effects as a list.** This was the plan of record until late, either as
`{ listOf(ShipOrder(it.orderId)) }` or with a vararg `issue` returning the list. It fixes the bare-command bug and
introduces a different silent one in its place. With a list-returning `issue`, a body of `issue(a)` then `issue(b)`
compiles and issues only `b`, because the first value is discarded. That is worse than the bug being fixed, because the
same text is correct today: the reshape would have silently reinterpreted working code. Under the accepted design `issue`
mutates the collector, so both commands are recorded whether or not the value is used.

The list form also would have rewritten every reaction in the tests, the documentation and the `order-fulfillment`
example for less safety, where the accepted design leaves every correct reaction compiling and meaning the same thing.

**Rejected: a single-command overload.** A `(T) -> C` overload beside a list-returning one was wanted and was tried.
Kotlin reports `Overload resolution ambiguity` for both candidates, and for the `{ listOf(a, b) }` form as well as the
single-command one, each followed by `Unresolved reference 'it'`. `@JvmName` resolved the JVM erasure clash, so that was
not the obstacle: Kotlin does not narrow candidates on a postponed lambda's return type. This is moot under the accepted
design but is recorded so it is not retried.

**Rejected: distinct names such as `on` for one command and `onMany` for several.** It doubles the surface across `on`,
`join`, `timeout`, `react`, `onStart` and `reactOnTimeout`, each with a metadata sibling, for a distinction that is not
domain-meaningful, since both declare the identical branch in the step graph. No honest name exists either: `onMany`
reads as "on many events", and `onAll` collides with `correlateAll`, which already means the fallback for all event
types. Java also has a single `on`, and the documentation shows Java and Kotlin side by side, so two Kotlin names would
make the two tabs disagree about how many ways there are to write a reaction.

## Consequences

- A Kotlin reaction that produces a command and discards it is a compile error rather than a saga that silently does
  nothing at runtime.
- Correct reactions are unaffected, so this is close to a no-op for the tests, the documentation and the example. Only a
  body ending in a conditional needs the new `nothing`.
- One new word enters the vocabulary, `nothing`. That is the price of the compile error, and it is a loud cost: a body
  that needs it and omits it does not compile, rather than misbehaving later.
- `SagaEffects` and `FlowReactions` both stay, with their mutable lists, and every method returns the receiver. Chaining
  becomes available as a side effect, so `issue(cmd).cancelTimeout("payment")` reads as well as two statements.
- The surface is unreleased, so there is no migration path and no OpenRewrite recipe.
- The Java builders are untouched except for the additive no-commands overloads, because a Java reaction was already an
  expression whose value is used.
- The receiver is still mutable, so a reaction that captured it and mutated it after returning would misbehave. Nothing
  in the DSL hands it out, and the type is constructed per invocation, so this is a theoretical rather than a reachable
  concern.
- No Kotlin linter was added. There is no detekt or ktlint in this build, and the mistake is now a compile error, so a
  lint rule would have nothing left to catch.
