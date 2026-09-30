# 140. A refresh round tells its listeners on a thread of its own

Date: 2026-09-29

## Status

Accepted. Resolves part of [#1166](https://github.com/johanhaleby/occurrent/issues/1166). Applies to both MongoDB
lease strategies, `SpringMongoLeaseCompetingConsumerStrategy` and `NativeMongoLeaseCompetingConsumerStrategy`,
which share `MongoLeaseCompetingConsumerStrategySupport`.

## Context

Each strategy refreshes every lease on the node from one scheduled thread. The strategies schedule it with
`ScheduledRefresh.auto()`, which runs a round every half lease time, and `ScheduledRefresh.every(..)` runs one at a
period its caller chooses. Until now that thread also called `onConsumeGranted` and `onConsumeProhibited` on every `CompetingConsumerListener` as soon as it
had refreshed a lease, and `CompetingConsumerSubscriptionModel` is such a listener.

A prohibition pauses the subscription in the wrapped model. `SpringMongoSubscriptionModel` pauses a subscription whose
change stream is still opening by waiting for the open to finish, which a slow or failing server stretches past the
lease time. While it waited, the refresh thread refreshed nothing, so every other lease on the node expired and moved
to another node, although nothing was wrong with any of them.

## Decision

**The refresh thread refreshes and never calls a listener. It hands each change a round made to a notifier for the
subscription the change is about, which calls the listeners for that subscription one change at a time, in the order
the round decided the changes.** The notifiers for different subscriptions call the listeners at the same time, on
threads from a pool that grows to one thread for each subscription with a change waiting.

A change can be out of date by the time the notifier gets to it, since registering, releasing and the next round keep
changing the lease meanwhile. The notifier therefore calls `onConsumeGranted` only if the consumer still holds the
lease, since starting a subscription on a node without its lease would deliver events twice. The notifier checks
before `CompetingConsumerSubscriptionModel` takes the lock it keeps for the subscription, so the lease can still move on
in between, and that model asks the strategy again once it holds the lock and ignores a grant for a lease the node no
longer holds. It always calls
`onConsumeProhibited`, also for a lease the node has won back since. `CompetingConsumerSubscriptionModel` then pauses
the subscription and releases the lease, and a later round grants it again and resumes the subscription. Dropping
that prohibition would keep a subscription whose delivery ended while the prohibition waited on a lease this node
goes on refreshing. The grant for the lease won back finds the subscription recorded as running and does nothing, and
no other node can take the lease over.

A listener that throws is logged, and the other listeners are called anyway. After `shutdown()` the notifiers stop
without waiting for a call in progress, since that call may be waiting for the monitor of the subscription model that is
shutting the strategy down.

Registering, unregistering and releasing still call the listeners on the caller's thread before they return, since
those callers rely on the listener having acted by the time the call returns. A listener that throws fails that call,
once every other listener has been called, with any later failure attached as suppressed.

## Consequences

A listener that blocks holds up the later calls for the same subscription. It holds up no call for another
subscription and no refresh of any lease. A subscription whose pause is still waiting keeps delivering events until the pause completes, as it did before. Its checkpoint
writes use the fencing token of the lease it lost, so they are refused once the next holder has written (ADR 139).

A `CompetingConsumerListener` of your own is called from a notifier thread for a change a refresh round made, and can
be called for two subscriptions at the same time. It is not called for a grant the lease had already moved past when the notifier checked, but the lease can move on
before the listener acts, so a listener that starts something on a grant asks the strategy again first. It can be told
about a prohibition for a lease the node holds again by then.
