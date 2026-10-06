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
subscribe the wrapped model refuses never gets that far, so it stores nothing, and there is nothing to delete.

The wrapped model gets a dynamic `StartAt`, and its first evaluation returns the recorded position:

| When the wrapped model evaluates it | What happens |
|---|---|
| While `subscribe(..)` records the position | The evaluation waits until the position is stored, then returns it |
| After `subscribe(..)` failed to record it | `subscribe(..)` cancels the wrapped subscription and the evaluation throws `IllegalStateException`, so nothing starts |
| Before the wrapped model's `subscribe(..)` returned | The evaluation records the position itself, which is where 0.33.0 recorded it |

The last row exists because a wrapped model may wait for its own evaluation inside `subscribe(..)`, and that model
would wait forever for a position the caller records only after `subscribe(..)` returns. Such a wrapped model has to
refuse an id it already holds before it evaluates the start position. The native and Spring MongoDB models refuse a
known id before they evaluate anything, and they evaluate the start position on their executor when the change
stream opens.

With that order, two things are true:

- A subscribe that the wrapped model refuses writes no checkpoint.
- No subscription delivers anything before its first position is stored, because the wrapped model can't open it
  without the first evaluation, and that evaluation returns only once the position is stored.

`DurableSubscriptionModel` doesn't change the run state between the wrapped `subscribe(..)` and the recording. A
pause, resume, stop, start or shutdown in that window acts on the wrapped subscription the same way it does on any
other, and once the subscription runs, its evaluation returns the stored position.

The per-id lock in `DurableSubscriptionModel` orders `subscribe(..)`, `resumeSubscription(..)` and
`cancelSubscription(..)` of one id on one durable model. `pauseSubscription(..)`, `stop()`, `start(..)` and
`shutdown()` don't take it. Two durable models over one wrapped model are ordered by the wrapped model, which accepts
the id once and refuses the other subscribe, and the refused one records nothing. Two nodes each have a wrapped model
that accepts, so both record, and the `ifAbsent()` write and ADR 130 decide which position is kept.

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

A wrapped model of your own has to refuse an id it already holds before it evaluates the start position. A model that
evaluates first can store a position for a subscribe it then refuses, as in 0.33.0.

The wrapped model's first evaluation can wait for one `globalCheckpoint()` call and one checkpoint write on the
caller's thread.

When the position can't be recorded, the wrapped model held the subscription for a moment before `subscribe(..)`
cancelled it. It delivered nothing in that moment.
