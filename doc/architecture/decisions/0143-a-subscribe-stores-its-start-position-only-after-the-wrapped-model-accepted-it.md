# 143. A subscribe stores its start position only after the wrapped model accepted it

Date: 2026-10-06

## Status

Proposed. Part of [#1197](https://github.com/johanhaleby/occurrent/issues/1197). Applies to the blocking
`DurableSubscriptionModel`. The first position it stores is the one
[ADR 130](0130-a-subscriptions-first-position-race-resolves-by-order-not-by-write-order.md) writes.

## Context

A subscription that asks for the model default, with nothing stored for its id, gets its start position recorded from
the wrapped model's `globalCheckpoint()` inside `subscribe(..)`, on the caller's thread. That's what lets
`subscribe(..)` refuse with `IllegalStateException` when the wrapped model answers `null`. Until this decision the
position was recorded before the wrapped model saw the call. When the wrapped model already held the id, it refused
with `DuplicateSubscriptionIdException`, and the position stayed in checkpoint storage.

That position is harmful when the subscription the wrapped model already holds is paused and has stored nothing yet,
for example one started from `StartAt.now()` and paused before it handled an event. `resumeSubscription(..)` resumes
from the stored checkpoint, which the refused subscribe recorded after the pause, so every event written while the
subscription was paused is skipped. With `NativeMongoSubscriptionModel`, a subscription paused before `e1` was written
and resumed after a refused subscribe received only `e2`.

The first fix deleted the position again when the wrapped model refused the subscribe, but only if the stored value
was still the one the subscribe had written. That needed a new `CheckpointStorage` method, and it was still wrong.
On an idle MongoDB replica set two subscribes read the same operation time, and an `ifAbsent()` write of a value
equal to the stored one is reported as written. So the refused subscribe took the running subscription's start
position for its own and deleted it, and a crash of the running subscription then skipped events.

## Decision

`subscribe(..)` calls the wrapped model's `subscribe(..)`, or `subscribePaused(..)` when the subscription is held
paused, exactly as before. The first position is recorded on the caller's thread once that call has returned. A
subscribe the wrapped model refuses before it evaluates the start position never gets that far, so there is nothing to
delete.

The wrapped model gets a dynamic `StartAt`, and the first evaluation that finds the position recorded returns it:

| When the wrapped model evaluates it | What happens |
|---|---|
| Before `subscribe(..)` has recorded the position, for example before the wrapped model's `subscribe(..)` has returned, or while `subscribe(..)` records it | The evaluation records the position itself and returns it, without waiting for `subscribe(..)`. When both record, only the first to write a position to checkpoint storage writes one, and the evaluation returns that position. When recording fails, the evaluation throws and records nothing. `subscribe(..)` still records the position, in the recording it already started or once the wrapped model's `subscribe(..)` has returned, and so can a later evaluation |
| After `subscribe(..)` recorded the position | The evaluation returns that position |
| After `subscribe(..)` failed to record it | `subscribe(..)` cancels the wrapped subscription and the evaluation throws `IllegalStateException`, so the wrapped model gets no start position |
| Before the wrapped model's `subscribe(..)` throws | `subscribe(..)` cancels nothing on the wrapped model, as in 0.33.0. The exception the wrapped model threw gets a suppressed exception saying the wrapped model may still hold a subscription for the id, unless it is `DuplicateSubscriptionIdException`. An evaluation still recording the position when `subscribe(..)` rethrows that exception gets no start position. It throws `IllegalStateException` once recording returns, or what recording threw |
| After `subscribe(..)` rethrew what the wrapped model's `subscribe(..)` threw | The evaluation can throw `IllegalStateException`, and a subscription the wrapped model still holds may then get no start position. In 0.33.0 that evaluation returned a start position |

The first row exists because a wrapped model may wait for its own evaluation inside `subscribe(..)`, and that model
would wait forever for a position the caller records only after `subscribe(..)` returns. A wrapped model of your own
has three requirements, which `DurableSubscriptionModel` doesn't check:

- It refuses an id it already holds before it evaluates the start position. A model that evaluates first can store a
  position for a subscribe it then refuses, as in 0.33.0.
- When its evaluation inside `subscribe(..)` throws, its `subscribe(..)` throws as well and holds no subscription for
  the id. A model that waits and evaluates again doesn't return from `subscribe(..)` until the position can be
  recorded.
- Its `globalCheckpoint()` doesn't need a lock that its `pauseSubscription(..)`, or another of its lifecycle calls,
  holds while it waits for the thread evaluating the start position. An evaluation that finds no position recorded or
  stored calls `globalCheckpoint()` itself, so neither the lifecycle call nor the evaluation would return. In 0.33.0
  every evaluation that found no checkpoint stored called `globalCheckpoint()`, so 0.33.0 had the same requirement.

The native and Spring MongoDB models meet all three. They refuse a known id before they evaluate anything, and they
evaluate the start position on their executor when the change stream opens, without waiting for it in `subscribe(..)`.
Their `globalCheckpoint()` isn't synchronized.

With that order, two things are true for those two models:

- A subscribe that the wrapped model refuses writes no checkpoint.
- Without `startWhenNoStartPositionCanBeRecorded(true)`, no subscription that asks for the model default delivers
  anything before its first position is stored. The model can't open it without the first evaluation, and that
  evaluation returns a position only once it is stored.

For a wrapped model of your own that evaluates the start position inside `subscribe(..)` and then throws, the position
that evaluation stored stays stored, as described under consequences.

`DurableSubscriptionModel` doesn't change the run state between the wrapped `subscribe(..)` and the recording. A
pause, stop, start or shutdown in that window, or a call made straight to the wrapped model, acts on the wrapped
subscription the same way it does on any other, and once the subscription runs, its evaluation returns the stored
position. `resumeSubscription(..)` on `DurableSubscriptionModel` waits for the per-id lock, so it comes after the
recording.

The per-id lock in `DurableSubscriptionModel` orders `subscribe(..)`, `resumeSubscription(..)` and
`cancelSubscription(..)` of one id on one durable model. `pauseSubscription(..)`, `stop()`, `start(..)` and
`shutdown()` don't take it. Two durable models over one wrapped model are ordered by the wrapped model, which accepts
the id once and refuses the other subscribe, and the refused one records nothing. Two nodes each have a wrapped model
that accepts, so both record. On a storage that evaluates write conditions, the `ifAbsent()` write and ADR 130 decide
which position is kept. On one that doesn't, both positions are written unconditionally and the later write is kept,
as before this decision, and `DurableSubscriptionModel` logs a warning.

`CheckpointStorage` is unchanged, and so is the conformance suite in `occurrent-tck-subscription-blocking`.

## Alternatives considered

- **Delete the position again on a refusal.** This is the first fix described above. It needed a new
  `CheckpointStorage` method that every storage of your own would have to implement, and it deleted a position the
  running subscription relied on.
- **Ask the wrapped model whether it holds the id before recording.** `NativeMongoSubscriptionModel` moves an id
  between its running and paused subscriptions in two steps, so `isRunning(id)` and `isPaused(id)` asked one after the
  other can both answer `false` while a pause or resume runs. With the position recorded after the wrapped model
  accepted the id, the wrapped model's own refusal comes first, so the check adds nothing.
- **Subscribe paused, record, then resume.** `resumeSubscription(..)` in the native and Spring MongoDB models starts
  the model again after a `stop()`, so a stop that came in while the position was recorded would be undone by the
  resume. The default `subscribePaused(..)` throws `UnsupportedOperationException` as well, so a wrapped model of your
  own would need a second path.

## Consequences

A wrapped model of your own has the three requirements listed under the decision, and nothing enforces them.

No evaluation waits while the caller's thread reads the stored checkpoint or calls `globalCheckpoint()`. So a wrapped
model that evaluates the start position while it holds a reentrant lock, which its `globalCheckpoint()` takes as well,
works as it did in 0.33.0. An evaluation can wait while the caller's thread or another evaluation writes the first
position to checkpoint storage, and the caller's thread can wait while an evaluation writes it. That write also settles
which position is kept when another node wrote one first, as ADR 130 describes. Without that wait, a storage that
doesn't evaluate write conditions could get two first positions from the caller's thread and an evaluation of one
subscribe, and a crash before the first checkpoint could then restart from the later one and skip the events between
them.

The wait covers only the first position the caller's thread and the evaluations record together. A later evaluation
that finds nothing stored, for example after `startWhenNoStartPositionCanBeRecorded(true)` let the subscription start
without one, writes its position the way another node would. On a storage that evaluates write conditions, one first
position stays stored, and that evaluation starts from it, or from its own position when that is earlier and the
storage replaced the stored one with it, as ADR 130 describes. The stored position is then never later than the
position an evaluation started from, so a restart can deliver events again but skips none. On a storage that doesn't
evaluate write conditions, the later write is kept, as before this decision.

When the position can't be recorded, the wrapped model held the subscription for a moment before `subscribe(..)`
cancelled it. The MongoDB models delivered nothing in that moment, since their evaluation throws.

When that cancel throws as well, the wrapped model may still hold the subscription. `subscribe(..)` throws the
recording failure with a suppressed exception that says so, and keeps the subscription registered, so the subscription
stores no checkpoint once a later `cancelSubscription(..)` has cancelled it, or a later `subscribe(..)` of the id has
replaced it. That `cancelSubscription(..)` tries the cancel again and keeps the checkpoint stored for the id, since the
held subscription itself, an earlier run or another node may have written it.

A wrapped model of your own can evaluate the start position inside its `subscribe(..)`, hold the subscription, and
then throw. `subscribe(..)` cancels nothing on the wrapped model then, as in 0.33.0, so the wrapped model still holds
that subscription and any position the evaluation stored stays stored. Deleting the position would repeat the first fix
described under context, which deleted a position the running subscription relied on.

Cancelling the subscription isn't safe either, because the durable model can't tell whether the subscription the
wrapped model holds for the id is this subscribe's. It may belong to an earlier `subscribe(..)` on the same durable
model, to a second durable model over the same wrapped model, or to a subscription made with the same id straight on
the wrapped model. A cancel by id would stop that subscription, which then receives no more events, and nothing
reports why.

For a subscribe with the model default, an evaluation that starts after `subscribe(..)` rethrew what the wrapped
`subscribe(..)` threw can fail with `IllegalStateException`, and the subscription the wrapped model still holds may
then get no start position. In 0.33.0 that evaluation returned a start position.

For a subscribe with the model default, when the wrapped model evaluated the start position before it threw, the
exception gets a suppressed exception saying the wrapped model may still hold a subscription for the id. An evaluation
still recording the position when the wrapped `subscribe(..)` threw counts too. A `DuplicateSubscriptionIdException`
gets none, since its id always belongs to another subscribe.

When nothing else subscribed the id, `getWrappedSubscriptionModel().cancelSubscription(id)` frees the subscription and
keeps the checkpoint stored for the id. The durable model's `cancelSubscription(..)` frees it as well, but it can also
delete that checkpoint, so when an earlier run had stored one, the next subscribe starts from the current position
and skips the events written since that checkpoint. It keeps the checkpoint when an earlier subscribe of the id, whose
cancel of the wrapped subscription failed, left its registration behind.

The durable model's `cancelSubscription(..)` also stops the checkpoint writes of every subscription the wrapped model
may still hold after a `subscribe(..)` of the id threw, before it deletes the checkpoint. An action of such a
subscription that returns after the cancel then writes nothing. In 0.33.0 that action wrote its checkpoint after the
cancel had deleted it, so the next subscribe of the id resumed from that checkpoint. The durable model refers to those
subscriptions only weakly, so it keeps each one only as long as the wrapped model, or an action of that subscription
still running, refers to it. A later subscribe of the id doesn't stop their writes, so until a cancel of the id they
write their checkpoints as in 0.33.0.