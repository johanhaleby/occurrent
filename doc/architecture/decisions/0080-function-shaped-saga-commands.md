# 80. Function-shaped commands, not a lambda-carrying saga effect

Date: 2026-07-30

## Status

Accepted. Extends ADR 63 (saga DSL) and ADR 76 (batch command dispatch seam) additively. Nothing in `Saga`,
`SagaEffect`, `Step`, or the executor changes.

## Context

Occurrent's documented command philosophy says a command does not have to be a data structure. The domain model is
plain functions, and the "command" is the function itself, invoked through an `ApplicationService`. Explicit command
records are described as the right choice when you need to serialize a command or when you are using deciders, and as
avoidable otherwise.

The saga DSL does not offer that option. A reaction issues a `C`, and the executor hands each `C` to a
`CommandDispatcher<C>`. A team that follows the philosophy and then reaches for a saga has to abandon it: invent a
`sealed interface OrderCommand`, one record per intent, and a `handle(events, command)` switch inside the dispatcher.
That is precisely the command-handler boilerplate the philosophy says they do not need, reintroduced by the one
component that never gave them a choice.

The literal fix is a fifth `SagaEffect` variant carrying a lambda that the runner executes. This ADR rejects that and
records what it does instead, because the rejected shape is the obvious one and will be proposed again.

## Decision

**A function-shaped command is an ordinary `C`, not a new effect.** `Saga<E, S, C>` never bounded `C`, so the DSL
already permits it. What was missing sat one module away: `CommandDispatchers` had a single factory, `decider(...)`,
and its own javadoc said the non-decider path was a lambda the caller writes. So `occurrent-command-dispatch` gains

```java
public record Invocation<E>(String streamId, Function<List<E>, List<E>> decision) { }

public static <E> CommandDispatcher<Invocation<E>> invocation(ApplicationService<E> applicationService)
```

and `occurrent-command-dispatch-dcb` gains the twin, where a `DcbCriteria` read boundary takes the place of the stream
id and an optional `TagGenerator` takes the place of the one a `DcbDecider` carries. A saga typed
`Saga<OrderEvent, S, Invocation<OrderEvent>>` issues the domain function directly, and there are no command types and
no switch anywhere.

### Why not a lambda-carrying effect

**The argument that decides it is convergence, not a tally of costs.** Every attempt to make a lambda effect routable,
batchable, or wireable ends by bolting a stream id onto it. At that point `Run(streamId, fn)` *is*
`IssueCommand(Invocation(streamId, fn))`, except with a bespoke interpreter that bypasses `dispatchAll`, `@Saga`
wiring, and every dispatcher decorator anyone writes later. The variant is strictly dominated by the command it
converges on.

Three supporting reasons discriminate between the two designs rather than merely sounding bad:

1. **It puts a second dispatch path in the executor, permanently.** Running such an effect means `SagaExecution` needs
   an `ApplicationService`, so `occurrent-saga-dsl-blocking` takes a hard dependency on the application-service module
   and both `SagaRunner.run(...)` and `@Saga` grow a second collaborator beside `CommandDispatcher`. That cost is not
   recoverable later. `Invocation` adds no collaborators at all.
2. **It forecloses ADR 63's outbox v2 at the wrong granularity.** Not because a lambda cannot be persisted, since
   `Invocation` cannot be persisted either. Because the opt-out would be value-level rather than type-level. With
   `C = Invocation<E>` a whole saga is statically and visibly excluded, and a wiring-time check can say so. With a
   fifth variant, a saga whose `C` is a perfectly serializable record can smuggle one unserializable effect into one
   rare branch, and the outbox can only discover it at runtime, on the branch nobody exercised.
3. **The type would permit too much.** An `Invocation`'s decision is a `Function<List<E>, List<E>>` run by
   `ApplicationService.execute`, so the only thing the framework lets it express is "re-fold this stream and return
   events to append". That re-fold is exactly what makes at-least-once dispatch safe, and here it is a constraint from
   the type rather than a warning in prose. `SagaEffect.run(Runnable)` is typed to permit anything.

The positive case is the mirror of reason 1. Because an invocation travels through `CommandDispatcher`, `dispatchAll`
can fold a run of consecutive invocations sharing a `streamId` into a single `execute`, composing their functions with
`ListCommandComposition.composeCommands` so each sees what the ones before it decided. One reaction issuing two
invocations against one stream is then one atomic append rather than two. That is what ADR 76 built the seam for, and
it is structurally unavailable to a raw effect variant, which has no dispatcher to override. Order is never rearranged
to make a group larger, since dispatch is contractually in order, so two invocations separated by one to a different
stream stay three appends.

The DCB twin deliberately does not group. Two invocations with equal `DcbCriteria` may carry different tag generators,
and one append can be tagged only one way, so folding would have to either drop a generator or invent a rule for
combining them.

### Arguments deliberately not used

Three tempting objections do not discriminate between the two designs, and leaning on them would make the rejection
look motivated rather than reasoned:

- **"It breaks equality assertions."** `IssueCommand(Invocation("order-1", λ))` has the identical reference-equality
  problem. Whatever this proves, it proves about the accepted design too.
- **"`Step.timerEffects()`'s negated `instanceof` would misclassify it."** It would, but that accessor is unreleased
  and the fix costs three lines. A fragility that can be fixed for free is not an argument against a feature, so it
  was simply fixed: `timerEffects()` now matches exhaustively, and a fifth variant would stop it compiling exactly
  where `SagaExecutionSupport.applyEffects` already does.
- **"It invites non-idempotent receivers."** False symmetry. Nothing stops a hand-written `CommandDispatcher<C>` from
  charging a card today. The surviving form of this concern is reason 3 above, which is about what the type permits,
  not about what a user might do.

### What it costs

A saga typed on `Invocation` gives up equality assertions on the commands it issued, because a lambda has no value
equality. This is a real loss and is not papered over: `assertThat(step.issuedCommands()).containsExactly(...)` is the
fastest test the DSL offers, and it does not survive the choice.

Two things replace it, and the documentation leads with them. A unit test applies the decision to the events it cares
about, `step.issuedCommands().single().decision().apply(events)`, which asserts what the command *does* rather than
what it was named, including that a second run returns nothing, which is the property at-least-once dispatch rests
on. An end-to-end test runs the saga against an in-memory event store and asserts the events that land. Timers are
unaffected, since `timerEffects()` still compares by value.

Data commands therefore remain the default in the documentation. The choice is a real trade, not an upgrade: command
records buy assertable intent and a future outbox, function-shaped commands buy the absence of a type hierarchy and a
switch. `Saga.adapt` also cannot widen an `Invocation<Narrow>` to an `Invocation<Wide>`, since it requires
`SubC extends C` and Java generics are invariant, so a feature saga should be typed on the module-wide event type from
the start.

## Consequences

- A saga can issue the domain function itself, in the core and flow DSLs, in Java and Kotlin, on the stream and DCB
  write paths, with no command types and no handler switch.
- `Saga`, `SagaEffect`, `Step`, `SagaExecutionSupport`, `SagaRunner`, and `@Saga` are unchanged. The whole feature is a
  record and a factory in the dispatch modules, plus two Kotlin extensions.
- `occurrent-command-dispatch` gains `occurrent-application-service-blocking` as an optional dependency, matching how
  `occurrent-decider` is already declared, and `occurrent-command-composition` as a required one. The latter is not
  optional because `dispatchAll` reaches for it only on the branch that folds a same-stream run, so a missing
  dependency would surface as a runtime failure on a rarely exercised path. That module is a dependency-free leaf, so
  requiring it costs nothing.
- The Kotlin `issue(streamId) { events -> ... }` extensions live in `occurrent-saga-dsl-blocking`, not in
  `occurrent-saga-dsl-common`. Common is the pure descriptor and knows nothing about dispatch, so putting a dispatch
  type into its public Kotlin API would undo that. Blocking already depends on both sides.
- A saga using `Invocation` is permanently at-least-once, and visibly so from its type. The designed outbox v2 stays
  available to every saga that uses data commands.
- `Step.timerEffects()` no longer classifies by negation, so a future `SagaEffect` variant has to be classified
  deliberately in both places that interpret the union.
