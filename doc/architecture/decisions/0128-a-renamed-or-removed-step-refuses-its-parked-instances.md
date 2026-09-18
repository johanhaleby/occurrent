# 128. A renamed or removed step refuses its parked instances

Date: 2026-08-16

## Status

Accepted. Resolves #748, deferred by [ADR 123](0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md)'s
Consequences section. 0125 is the current maximum, re-audited across every remote branch at write time per the
max-plus-one rule.

## Context

A flow saga instance's position is a step name, persisted as `FlowStateImpl.currentStep()`. Nothing checks that name
against the currently compiled flow before using it. Rename or remove a step while an instance is still parked in
it, and the next delivery that instance receives throws a bare `NullPointerException` from inside `evolve`:

- On the event path, `evolveOnEvent` looks up `stepsByName.get(state.currentStep())` and uses the result
  unconditionally.
- On the timer path, `evolveOnTimeout` does the same lookup for a timer armed while the step still existed.

The issue also named a second shape, `reactToBranch` reading `from.branches().get(state.matchedBranchIndex())`,
which it worried could throw `IndexOutOfBoundsException` for a step that kept its name but lost the branch an
instance's carried bookkeeping points at.

ADR 123 added the first loud refusal in this file, for a related but distinct case, a step's window-condition
declaration changing while `stepWindow` had already evicted the events its counts would need to be rebuilt from.
That case is genuinely unrecoverable, the data is gone, so refusing is the only honest answer, and ADR 123's own
Consequences section deferred this issue rather than reuse that shape without thinking it through, saying
"deciding what an instance parked in a step that no longer exists should do is a lifecycle decision rather than a
message." A routing comment on #748 asked whoever picked it up to follow that refusal's shape rather than invent another, and
separately to decide deliberately whether a missing step is a hard refusal or something recoverable, since, unlike
the declaration-change case, nothing about a renamed or removed step's data is lost. The step's own events are all
still there. Only the definition's name for them is gone.

Two things settle the shape of the fix, both checked against the execution machinery rather than assumed from the
issue text.

**The two crash sites are also the only two that matter, and fixing them closes the `IndexOutOfBoundsException`
shape too, with no separate change.** `SagaExecutionSupport.process` computes `nextState = saga.evolve(...)` and
hands that same value to `saga.react(nextState, ...)` in the same call, against the same compiled, immutable
`FlowSagaImpl`. Every bookkeeping field `react` reads back, `previousStep`, `matchedBranchIndex`, `lastAction`, is
derived inside `evolveOnEvent`/`evolveOnTimeout` from the very `CompiledStep` those methods just looked up and used.
Guard the lookup, and `reactToBranch`, `reactToJoin` and `armTimeoutIfAny` can no longer be reached with a step
name or a branch index the current build does not have. There is nothing left for a second fix to do there.

**The two guarded call sites do not carry the same cost when they refuse.** `SagaRunner`'s own javadoc documents
that an exception on the event path propagates to the subscription model, that the whole step is retried wherever
that model offers the event again, and that the
subscription is a single ordered channel shared by every instance the saga handles, so while one event keeps being
redelivered and failing the events behind it wait. One instance parked on a gone step is what holds that channel. A
consume-side broker bridge need not hold it, since `DeliveryFailurePolicy` is where its choice of what to do with a
failed delivery is configured. That is not a new failure mode this decision introduces, it is the same accepted
architecture ADR 123's own refusal already lives with. The timer path is different. `SagaExecution.pollTimers`
catches a failing timeout per instance, logs it, and leaves it due for the next poll.

**Amended for [#998](https://github.com/johanhaleby/occurrent/issues/998).** This paragraph used to end "A missing
step firing a timer costs one stuck instance", which was wrong in the same way ADR 134's Context was. The timer
survives, and nothing in `findWithDueTimers` requires a store to give a different instance a turn, so once
`timerBatchLimit` instances are in that state the saga can stop firing timers altogether.
See [#1003](https://github.com/johanhaleby/occurrent/issues/1003).

**Amended for [#1028](https://github.com/johanhaleby/occurrent/issues/1028).** Two more of the paragraph's claims
have gone. It used to attribute to `SagaRunner`'s javadoc that the subscription model "retries the same event
forever", and that attribution was wrong on the day it was written. On 2026-08-16 the javadoc said the events
behind a failing one wait "until it succeeds or someone intervenes", and the file has never contained the word
"forever" or the phrase "retries the same event". The paragraph also used to end "A missing step reached by an
event can cost the whole subscription until fixed", which 0.34.0 superseded. An instance that keeps failing is
quarantined at `quarantineAfter` wherever four conditions hold, and `SagaRunner`'s Event path section and
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md) list them. Whether they
hold for an instance parked on a renamed step is a separate question neither record answers, and one configuration
where the budget elapses and the instance is quarantined instead is enough to falsify an "until fixed". What this
record no longer asserts is that the wait always runs until somebody intervenes.

**Amended for [#1071](https://github.com/johanhaleby/occurrent/issues/1071).** The paragraph attributed an
unconditional redelivery to `SagaRunner`'s javadoc. A consume-side broker bridge sends a failed delivery through its
`DeliveryFailurePolicy`, so a redelivery is not something this record can assume. The held channel is the cost this
record compares the two call sites on, and a bridge need not pay it.

**The decision below still stands, on a reason neither amendment touches.** It never rested on how long an
event-path refusal blocks, only on that refusal costing what any other event-path exception in this architecture
already costs. An `IllegalStateException` out of `evolve` travels the same route as a throwing `react` or a throwing
dispatcher, because nothing on the event path treats it differently from any other failure of a delivery, so refusing
here adds no failure mode ADR 123's refusal did not already accept. Both amendments change how large that shared
cost is. Neither makes it this decision's cost rather than the architecture's.

## Decision

**A renamed or removed step refuses the delivery, the same `IllegalStateException` shape ADR 123 already
established, and nothing else changes.** No builder-level recovery hook, no step-alias or migration API, no new
exception type. The message names the step, says it no longer exists in the current definition because it was
renamed or removed while an instance was parked in it, and gives two remedies, both already public and traced
against real code rather than asserted:

- Put the step back, or add a temporary step under the old name whose only job is to `transitionTo(...)` the real
  destination (or `Continuation.end()`), until every parked instance has moved past it. This is the existing DSL,
  not new API. Keeping the old name alive for one release, as a bridge to the new one, is how a rename gets to
  look graceful with no code change here.
- Delete the instance with `SagaStateStore.delete(sagaId)`. Traced through `SagaExecutionSupport.process`, once the
  envelope is gone, `current == null`, a non-start event fails `startEventOrNull`, and `process` returns
  `Outcome.skip()`, no crash, no state written, the subscription's checkpoint advances. On the timer path a deleted
  envelope fails `SagaExecution.hasDueTimer`'s null check the same way, and the timer is simply dropped.

The issue also named softer answers, completing the instance, moving it to a step the saga names, or reaching a
callback the saga declares. Nobody has asked for graceful step migration, the issue itself calls it a
recommendation rather than a requirement, and a public interface should wait for someone who actually needs it
rather than a hypothetical future caller. All three are already reachable today through the bridging-step
technique above, using API that already ships, so building a second way to say the same thing is not a gap, it is
a duplicate.

No migration guide, no `UpgradeToOccurrent_*` recipe, no held docs branch. The old behavior was an unintentional
crash, not documented behavior a caller could have been relying on. Turning a bare `NullPointerException` into a
named, actionable refusal is a bug fix, not a change to a shipped contract.

## Rejected alternatives

**A builder-level recovery hook** (`onMissingStep`, a step-alias map, or similar), letting the saga author declare
what happens to an orphaned instance. Rejected for now because it is unrequested public API for a problem the
existing DSL already has a workaround for, and building ahead of an actual need is exactly what this codebase's
conventions warn against. Worth building the day the bridging-step workaround itself becomes the thing people are
tired of writing by hand.

**Silently no-op the delivery instead of refusing.** Considered and rejected because it trades a loud, actionable
failure for an instance that is stuck with no signal at all. A refusal that names the step at least tells an
operator where to look. A swallowed delivery does not, and the instance never moves again until someone happens to
notice on their own.

## Consequences

- `evolveOnEvent` and `evolveOnTimeout` both refuse a delivery to a step absent from the current build, naming the
  step and both remedies. `reactToBranch`, `reactToJoin` and `armTimeoutIfAny` are unchanged, carrying a comment
  each explaining why they are now provably unreachable with an inconsistent step or branch index.
- No public API changes. `FlowSaga.Builder` gains nothing.
- No migration guide entry, no OpenRewrite recipe, since this corrects a crash rather than a documented behavior.
- The changelog entry is filed as a bug fix against 0.33.0, where flow sagas and `stepWindow` shipped.
- A rename that wants to look seamless to a parked instance keeps the old step name alive for one release as a
  bridge to the new one, using `transitionTo(...)`, rather than removing it outright. This is now the documented
  answer for the three softer outcomes #748 asked about.
- Amends [ADR 123](0123-a-step-conditions-counts-are-carried-so-the-steps-events-can-be-dropped.md), whose
  Consequences section named #748 as deliberately unfixed. That entry is resolved by this record.
