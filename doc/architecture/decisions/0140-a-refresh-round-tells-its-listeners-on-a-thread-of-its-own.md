# 140. A refresh round tells its listeners on a thread of its own

Date: 2026-09-29

## Status

Accepted. Resolves part of [#1166](https://github.com/johanhaleby/occurrent/issues/1166). Applies to both MongoDB
lease strategies, `SpringMongoLeaseCompetingConsumerStrategy` and `NativeMongoLeaseCompetingConsumerStrategy`,
which share `MongoLeaseCompetingConsumerStrategySupport`.

## Context

Each strategy refreshes every lease on the node from one scheduled thread, every half lease time. Until now that
thread also called `onConsumeGranted` and `onConsumeProhibited` on every `CompetingConsumerListener` as soon as it
had refreshed a lease, and `CompetingConsumerSubscriptionModel` is such a listener.

A prohibition pauses the subscription in the wrapped model. `SpringMongoSubscriptionModel` pauses a subscription whose
change stream is still opening by waiting for the open to finish, which a slow or failing server stretches past the
lease time. While it waited, the refresh thread refreshed nothing, so every other lease on the node expired and moved
to another node, although nothing was wrong with any of them.

## Decision

**The refresh thread refreshes and never calls a listener. It hands what a round changed to a single notifier
thread, which calls the listeners in the order the round decided the changes.**

A change can be out of date by the time the notifier gets to it, since registering, releasing and the next round keep
changing the lease meanwhile. The notifier therefore calls `onConsumeGranted` only if the consumer still holds the
lease, and `onConsumeProhibited` only if it still does not. Delivering a prohibition for a lease the node has won back
would pause a subscription that keeps its lease, and no later round would resume it, since a grant is only reported
when the lease changes hands.

A listener that throws is logged, and the other listeners are called anyway. After `shutdown()` the notifier stops
without waiting for a call in progress, since that call may be waiting for the monitor of the subscription model that is
shutting the strategy down.

Registering, unregistering and releasing still call the listeners on the caller's thread before they return, and a
listener that throws still fails that call. Those callers rely on the listener having acted by the time the call
returns.

## Consequences

A listener that blocks now holds up the calls queued behind it, but no longer the refresh of any lease. A subscription
whose pause is still waiting keeps delivering events until the pause completes, as it did before. Its checkpoint
writes use the fencing token of the lease it lost, so they are refused once the next holder has written (ADR 139).

A `CompetingConsumerListener` of your own is now called from the notifier thread for a change a refresh round made,
and is not called at all for a change the lease has already moved past.
