# 168. A reactor catch-up registers a takeover replay when it answers the loss

Date: 2026-11-06

## Status

Accepted. Amends [ADR 146](0146-a-catch-up-takes-over-a-live-start-the-wrapped-model-loses-as-it-opens.md), whose
paragraph on what a catch-up answers for a subscription between the wrapped model's pause and `afterPause` now holds
for the blocking stack only. Applies to the reactor catch-ups that ADR 146 covers.

## Context

ADR 146 has a catch-up record that it takes the subscription over when it answers the wrapped model's report of the
lost live start, before the wrapped model pauses the subscription. Between that pause and `afterPause`, the
subscription reads as paused, and a `resumeSubscription(id)` of the catch-up runs the takeover's replay itself instead
of resuming the wrapped model at the lost live start.

In occurrent 0.34.0 the reactor catch-up recorded the takeover the same way, and registered the takeover's replay as
the one in flight for the id only in `afterPause`. A `pauseSubscription(id)` of the catch-up in between found no replay
in flight and went to the wrapped model, which already held the subscription paused. Once `afterPause` ran, the
takeover's replay resumed the subscription at its new live start without knowing about that pause.

## Decision

On the blocking stack ADR 146 holds as written. Between the wrapped model's pause and `afterPause`, the subscription
reads as paused. A `resumeSubscription(id)` of the blocking catch-up in that time runs the takeover's replay itself,
and the `afterPause` that follows finds nothing left to do. A blocking `start(true)` that finds the takeover recorded
runs the replay the same way.

On the reactor stack, when the wrapped model reports the loss of the live start the last replay of the subscription
handed over, the catch-up registers the takeover's replay as the one in flight for the id before it answers, and
`afterPause` only launches that replay. It registers nothing in two cases:

- Another replay of the id is in flight already, such as a takeover registered when the wrapped model reported the
  same loss before. The catch-up takes the loss over for that replay, and `afterPause` launches it if it has not
  launched yet.
- The wrapped model holds the subscription paused already. The catch-up takes the loss over with nothing to run, and a
  later `resumeSubscription(id)` of the catch-up registers the takeover and runs its replay.

A takeover the catch-up registered answers for the subscription as any replay in flight does, until it hands over.
While the catch-up is not stopped, between the wrapped model's pause and `afterPause`, `isRunning(id)` answers `true`
and `isPaused(id)` answers `false`, and a `resumeSubscription(id)` of the catch-up throws
`SubscriptionAlreadyRunningException` instead of resuming the wrapped model at the lost live start. A takeover
registered while the catch-up is stopped is registered parked, reads as paused, and first launches when a
`start(true)` or a resume unparks it.

A `pauseSubscription(id)` of the catch-up in that time marks the takeover paused, and the subscription reads as
paused. What a resume does then depends on whether it comes before `afterPause`:

- A resume before `afterPause` runs the takeover's replay, and `afterPause` finds nothing left to launch.
- When `afterPause` comes first, it launches the replay even though the takeover is paused. The replay hands over by
  resuming the wrapped model at the new live start and pausing the subscription there at once, and the subscription
  stays paused until it is resumed. A resume while the replay still runs only clears the pause, so the replay hands
  over and the subscription runs.

A `stop()` of the catch-up in that time parks the takeover, which then reads as paused, and `afterPause` launches
nothing. A `start(true)` or a resume of the subscription runs the takeover's replay.

ADR 146's next paragraph holds on both stacks. A blocking `start(true)` that read the subscriptions before the takeover
was recorded, and a reactor `start(true)`, can still resume the wrapped model at the lost live start, and the catch-up
takes the new loss over as well.

## Alternatives considered

**Register the replay in `afterPause`, as 0.34.0 did.** A pause between the wrapped model's pause and `afterPause` then
reaches only the wrapped model, and the takeover's replay resumes the subscription at its new live start regardless.
Registering the replay when the catch-up answers lets the pause, resume, `isRunning(id)` and `isPaused(id)` of the
reactor catch-up find the takeover the same way they find any replay in flight.

## Consequences

The two stacks answer differently in the same window. On the reactor stack a resume between the wrapped model's pause
and `afterPause` throws `SubscriptionAlreadyRunningException`, unless the subscription was paused or stopped through
the catch-up first. On the blocking stack the same resume runs the takeover's replay.
