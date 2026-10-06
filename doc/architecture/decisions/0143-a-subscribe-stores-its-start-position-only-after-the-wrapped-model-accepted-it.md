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
| Before `subscribe(..)` starts recording the position, for example before the wrapped model's `subscribe(..)` has returned | The evaluation records the position itself and returns it. When recording fails, the evaluation throws, and `subscribe(..)` records again once the wrapped model's `subscribe(..)` has returned |
| While `subscribe(..)` records the position | The evaluation waits until the position is stored, then returns it |
| After `subscribe(..)` failed to record it | `subscribe(..)` cancels the wrapped subscription and the evaluation throws `IllegalStateException`, so the wrapped model gets no start position |
| Before the wrapped model's `subscribe(..)` throws | When the evaluation recorded the position or threw, `subscribe(..)` cancels the wrapped subscription, unless the exception is `DuplicateSubscriptionIdException` or an earlier `subscribe(..)` on the same durable model left the id registered, including one that opted out of checkpoint management. An evaluation still recording the position when the wrapped `subscribe(..)` threw throws `IllegalStateException`, its subscription isn't cancelled, and `cancelSubscription(..)` frees the id |

The first row exists because a wrapped model may wait for its own evaluation inside `subscribe(..)`, and that model
would wait forever for a position the caller records only after `subscribe(..)` returns. That gives a wrapped model
of your own three requirements, which `DurableSubscriptionModel` doesn't check:

- It refuses an id it already holds before it evaluates the start position. A model that evaluates first can store a
  position for a subscribe it then refuses, as in 0.33.0.
- When its evaluation inside `subscribe(..)` throws, its `subscribe(..)` throws as well. A model that waits and
  evaluates again doesn't return from `subscribe(..)` until the position can be recorded.
- Its `globalCheckpoint()` doesn't need a lock that its `pauseSubscription(..)`, or another of its lifecycle calls,
  holds while it waits for the thread evaluating the start position. That evaluation can wait while `subscribe(..)`
  calls `globalCheckpoint()` to record the position, so neither the pause nor `subscribe(..)` would return.

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

The wrapped model's first evaluation can wait while the caller's thread records the position. That reads the stored
checkpoint, calls `globalCheckpoint()` and writes the position. When another node wrote a position first, it also
settles which one is kept, as ADR 130 describes.

When the position can't be recorded, the wrapped model held the subscription for a moment before `subscribe(..)`
cancelled it. The MongoDB models delivered nothing in that moment, since their evaluation throws.

When that cancel throws as well, the wrapped model may still hold the subscription. `subscribe(..)` throws the
recording failure with a suppressed exception that says so, and keeps the subscription registered, so the subscription
stores no checkpoint once a later `cancelSubscription(..)` has cancelled it, or a later `subscribe(..)` of the id has
replaced it. That `cancelSubscription(..)` tries the cancel again and keeps the checkpoint stored for the id, since the
held subscription itself, an earlier run or another node may have written it.

A wrapped model of your own can evaluate the start position inside its `subscribe(..)`, hold the subscription, and
then throw. Any position the evaluation stored stays stored, as in 0.33.0. Deleting it again would repeat the first
fix described under context, which deleted a position the running subscription relied on.

`subscribe(..)` cancels the subscription the wrapped model may hold, the same way as after a failed recording, when the
evaluation recorded the position or threw before the wrapped `subscribe(..)` threw. It doesn't when the exception is
`DuplicateSubscriptionIdException`, or when an earlier `subscribe(..)` on the same durable model left the id registered,
including one that opted out of checkpoint management, since the subscription the wrapped model holds may then belong
to that earlier subscribe. A subscription made with the
same id straight on the wrapped model isn't registered in the durable model, so it is cancelled as well.

A subscription whose evaluation hadn't finished recording the position when the wrapped `subscribe(..)` threw isn't
cancelled, and `cancelSubscription(..)` frees the id. Cancelling it too would mean waiting inside `subscribe(..)` for
the evaluation to finish, and that wait wouldn't keep any event from being skipped.
