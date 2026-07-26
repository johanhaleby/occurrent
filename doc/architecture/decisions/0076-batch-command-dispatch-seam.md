# 76. Give a command dispatcher one saga reaction's commands as a unit

Date: 2026-07-26

## Status

Accepted. Additive on top of the saga DSL (ADR 63) and the command dispatch port, both unreleased.

## Context

A saga reaction returns a list. `Saga.react` hands back `List<SagaEffect<C>>` and a flow step's `on` hands back
`List<C>`, so one delivered event can issue several commands. The list is not a convenience: reactions are keyed by
event type, one per type per step, so without it a process that must issue two commands on one event would have to be
faked with a synthetic intermediate event and an extra step.

`SagaExecution` dispatches that list before it saves the instance state, and that ordering is deliberate. A command
dispatched before the save can be dispatched twice, whereas a command dispatched after a save that then fails is lost
outright, and for a saga a duplicate is recoverable while a loss is not. ADR 63 recorded the resulting contract as
at-least-once command dispatch with exactly-once timer bookkeeping, and recorded a document-local outbox as the designed
second version, deferred because it is additive.

What ADR 63 documented was the compare-and-set amplification: a lost save retries the whole transition, which
re-dispatches the entire command list, up to `maxCasAttempts` times. A mid-list dispatch failure behaves differently and
is sharper. If the second of three commands throws, that exception is not the internal `CasConflict` signal, so the
retry predicate leaves it alone and it propagates on the first attempt. The save never runs, the instance stays at its
previous state, and on redelivery the reaction dispatches the first command again from the top. There is no per-command
progress marker, so a command that keeps failing re-fires the ones before it on every redelivery, without bound.

For the receiver ADR 63 assumes, an `ApplicationService`-backed dispatcher, this costs nothing: it re-folds the
authoritative stream and the target's invariants reject a command that was already applied. For a genuinely external
effect, sending an email or charging through a gateway that does not deduplicate, it is a real hazard.

The dispatcher is often in a position to remove the hazard and has no way to know it should. `CommandDispatcher`
exposes a single `dispatch(C)`, and `SagaExecution` loops over the list itself, so a dispatcher whose commands all land
on one stream or one decider cannot tell that a group of commands came from one reaction and could be written in one
transaction. The information exists and is discarded at the port.

## Decision

**`CommandDispatcher` gains a `default void dispatchAll(List<C> commands)` that loops over `dispatch`, and
`SagaExecution` calls it instead of looping.** Behaviour is unchanged for every existing dispatcher, including a plain
lambda, because the default does exactly what the executor did before. The interface keeps one abstract method, so it
remains a valid `@FunctionalInterface` and `cmd -> applicationService.execute(...)` still compiles.

**It is a seam a dispatcher may exploit, not a guarantee the framework provides.** A dispatcher that can make one
reaction's commands atomic overrides `dispatchAll` and writes them together, which closes the re-dispatch window for
that dispatcher. A dispatcher that cannot inherits the loop and the existing behaviour. Occurrent promises nothing
extra either way, and the documented contract stays at-least-once. The javadoc says so explicitly, so that a later
reader does not mistake the method's existence for exactly-once dispatch having arrived.

**This is not the outbox and not a per-command checkpoint.** A per-command progress marker would need a store write per
command, and the marker write can fail exactly where the command write can, which is the outbox problem rather than a
shortcut around it. The outbox stays where ADR 63 put it, as the second-version fix if exactly-once dispatch becomes
necessary. Reversing the order to save before dispatching was rejected outright, because it trades a recoverable
duplicate for an unrecoverable lost command.

**The mid-list failure is documented where the amplification already is.** The `SagaExecution` class javadoc described
the compare-and-set amplification but not this case, which left the sharper of the two properties implicit. It now
describes both.

## Consequences

- A dispatcher targeting a single stream or decider can make one reaction's commands a single write, so the prefix
  re-dispatch on a mid-list failure disappears for that dispatcher without any change to the saga or the executor.
- Nothing changes for an existing dispatcher, and no migration is needed. The change is additive on an unreleased
  interface, so there is no OpenRewrite recipe either.
- The delivery contract is unchanged. Command dispatch is still at-least-once, receivers must still be idempotent, and
  they must still tolerate the compare-and-set multiplicity ADR 63 describes, which is stronger than plain
  at-least-once.
- A reader can now find the mid-list failure documented rather than discovering it from a duplicated side effect in
  production.
- `dispatchAll` will look like the natural place to hang a transaction, which invites a future dispatcher to promise
  atomicity it cannot deliver across two different stores. The javadoc names the limit, but the type system does not
  enforce it, and it cannot.
- Occurrent ships no overriding implementation of its own, so the seam is unexercised in the library beyond its tests
  until an application or a future outbox uses it.
