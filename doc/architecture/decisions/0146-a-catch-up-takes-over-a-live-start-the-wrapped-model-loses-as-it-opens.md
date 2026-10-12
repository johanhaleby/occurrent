# 146. A catch-up takes over a live start the wrapped model loses as it opens

Date: 2026-10-12

## Status

Accepted. Resolves [#1232](https://github.com/johanhaleby/occurrent/issues/1232). Applies to the catch-ups that
[ADR 144](0144-a-catch-up-checkpoint-keeps-the-live-start-it-read-before-the-replay.md) covers, on both stacks, and to
the MongoDB subscription models they run on: `SpringMongoSubscriptionModel` and `NativeMongoSubscriptionModel` on the
blocking stack, `ReactorMongoSubscriptionModel` on the reactor stack. It closes the gap ADR 144 left between the check
of the live start and the handover.

## Context

Once its replay is done, a catch-up asks the wrapped model whether it can still resume from the live start, through
`canResumeFrom(..)`, and then hands that live start over by subscribing the wrapped model at it, or by resuming it there.
Those are two calls. The MongoDB models open the change stream after the second one returns, on their own thread, and
retry the open after a failure, so the time between the check and the open is not only the few milliseconds between
the two calls.

When the oplog drops the live start in that time, the MongoDB model gets error 286, `ChangeStreamHistoryLost`, as it
opens. With `restartSubscriptionsOnChangeStreamHistoryLost` on, which the Spring Boot starters turn on by default, it
restarts the subscription from the present, and the events written between the live start and the restart are never
delivered. A `DurableSubscriptionModel` in between also stores that present, so a restart of the process doesn't bring
them back either. With it off, the model logs the failure and forgets the subscription.

#1232 suggested one call that checks and opens in the same step, which would mean a new method on
`CheckpointAwareSubscriptionModel` that every wrapper forwards.

## Decision

The MongoDB models ask before they do anything about lost history. A `HistoryLossListener` gets
`handleHistoryLoss(subscriptionId, lostFrom)` and answers with the new sealed `HistoryLossHandling`:

- `HistoryLossHandling.leaveToSubscriptionModel()` has the model restart the subscription or forget it, as it is
  configured to. It is the default, so a listener written before this change behaves as before.
- `HistoryLossHandling.takeOver(afterPause)` has the model pause the subscription, keep it, ask no other listener, and
  run `afterPause` on the thread that found the history lost.

The model asks the listeners in the order they were added, in both restart modes, and the first that takes the
subscription over gets it. A pause, a resume or a cancel of the subscription, or a shutdown, that came first drops the
takeover. A listener that throws fails that attempt, and the model asks again when it tries again. A task that throws
is logged at `ERROR`, and the subscription stays paused.

`lostFrom` is the position the subscription opened at, while it has not read past it. The models don't resolve a
dynamic `StartAt` again for it, since the catch-up's supplier reads and saves the checkpoint storage.

The blocking `HistoryLossReportingSubscriptions` gets the method as a default. The reactor API gets its own
`HistoryLossReportingSubscriptions`, which also has `subscribeFailingIfStartLost(..)`, a `subscribe(..)` whose `Flux`
fails with the new `HistoryLostException` instead of restarting when the history no longer reaches back to its start.
It also gets `RepositionableSubscriptions`, which the blocking API already had. `ReactorMongoSubscriptionModel`
implements both.

A catch-up records the live start it handed over for each subscription id, and takes the subscription over when the
wrapped model loses exactly that position. A loss once the subscription has read past it is ordinary lost history and
stays with the wrapped model. After the takeover the catch-up:

1. Replays again from the position its replay first started from, with a live start read then, as it does when the
   check after the replay fails. The takeover counts toward the 3 times a replay runs again, so a catch-up whose live
   start is lost each time still gives up after 4 replays.
2. On the blocking stack with `UseCheckpointInStorage`, stores its checkpoint again before it replays, holding the
   lost live start and the replay origin, so a process that ends during that replay replays again after a restart. On the reactor stack the
   `ReactorDurableSubscriptionModel` above the catch-up already holds that checkpoint from the last event it replayed.
3. Resumes the subscription the wrapped model holds paused, at the new live start, through
   `RepositionableSubscriptions.resumeSubscription(id, startAt)`.
4. Answers `isRunning(id)` with `true` and `isPaused(id)` with `false` meanwhile, as for any replay in flight, so a
   competing consumer that reads them doesn't see the wrapped model's pause.

On the blocking stack the handle the first handover returned follows the takeover, so its `waitUntilStarted()` keeps
waiting until the new handover has opened. On the reactor stack a named subscription's start already completes once
the change stream is subscribed, before MongoDB has answered, so there is nothing left to wait for. A reactor takeover
whose replay fails logs it at `ERROR`, and the subscription stays paused until it is resumed, which replays again.

The reactor catch-up without a subscription id reads its live half through `subscribeFailingIfStartLost(..)`, and
replays again on a `HistoryLostException` whose `lostFrom()` is the live start it handed over.

After a cancel or a shutdown the catch-up takes nothing over, and a new handover replaces the live start it recorded
for the id.

## Alternatives considered

**One call that checks and opens.** It needs a new method on `CheckpointAwareSubscriptionModel` in both stacks, and
every wrapper between the catch-up and the MongoDB model has to forward it. The MongoDB models open the change stream
on their own thread after `subscribe(..)` returns, so the method would have to wait for the open, or report the loss
later anyway. The listener already reports lost history from that thread, and a listener's answer is all the takeover
needs.

**Have the wrapped model replay.** A MongoDB model doesn't know the event store, and the catch-up is the one that knows
where its replay started.

**Forward the reactor capabilities through `ReactorDurableSubscriptionModel`.** The reactor starters put the durable
model above the catch-up, so the catch-up wraps `ReactorMongoSubscriptionModel` directly and needs nothing forwarded.
Forwarding `RepositionableSubscriptions` through the durable model would resume a subscription past its own
bookkeeping.

## Consequences

- An event written between the check and the open of the change stream is delivered, in both restart modes, on both
  stacks, for position, time and DCB catch-ups. The catch-up delivers what it replays again a second time, as any
  replay that runs again does.
- A takeover's replay that gives up has no caller to throw at. The blocking catch-up logs the failure at `ERROR` and
  runs the replay again after a backoff of up to 2 seconds, as it does for any replay a resume started. The reactor one
  keeps the subscription paused.
- A wrapped model of your own that implements neither capability handles a lost live start as it would without the
  catch-up.
- `waitUntilStarted()` on the reactor stack doesn't wait for a takeover. Changing that needs `ReactorMongoSubscriptionModel`
  to report a start only once MongoDB has answered, which is a change of its own.
