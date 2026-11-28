# 187. A blocking catch-up goes live only once it holds nothing

Date: 2026-11-28

## Status

Accepted. Amends [ADR 108](0108-a-live-push-handler-runs-outside-the-handover-lock.md) on when the blocking drain goes
live. It also records what a write to the in-memory event store waits for during the drain, and which waiting calls a
handover refuses. #1601 made the changes, for 0.35.0.

## Context

`BlockingHandover` holds the live events that arrive while a catch-up replays, and delivers them once the replay ends.
It is the engine behind the blocking `CatchupThenPushSubscriptionModel`, `CatchupProjectionFeed` and
`DomainEventFeed`. ADR 108 moved each delivery outside the handover's lock. Its drain reserved every held event under
the lock, went live in the same lock section, and then delivered the held events after releasing the lock.

Going live before delivering let a live event that arrived during that delivery reach the handler ahead of older held
ones. A push `@Saga` skips a version that arrives 256 or more versions behind a later version of the same stream and
event type, so a held version that came in that late was lost. That is a loss under the isolation rule in
`AGENTS.md`, and 0.34.0 shipped it.

## Decision

**The handover goes live only once it holds nothing.** The drain delivers what it holds in rounds, and goes live only
when a round finds nothing left under the lock. One drain runs at a time. Until it goes live,
`isReadyForLiveDelivery(..)` answers `false`, `acceptOrDefer(..)` answers `DEFERRED`, and a live event fed without
waiting from inside any handover's own code is held behind the ones already held. The reservation under the lock and
the delivery outside it stay as ADR 108 decided.

**A live event fed from outside every handover's own code while the drain runs waits on the calling thread, without a
bound, until the drain has gone live, stopped or failed.** That is the write path of the in-memory event store. Without the wait,
writes that keep coming leave the drain something to deliver in every round, and the catch-up never goes live. The
wait has no bound for this reason. The in-memory push model delivers each write on the writing thread, one registration after the
other, in `RegisteringSubscribable.routeIsolated(..)`, and 0.34.0 did the same. So a live handler that hangs already
kept the writer, and every registration after it, waiting. The drain's wait extends that to the events the catch-up
held, and holds the writers to the pace of the drain, so the catch-up always goes live. Two other designs were
considered and rejected.

- A bounded wait that falls back to buffering. Writes that keep coming fill the buffer once the bound runs out, and a
  full buffer fails the catch-up for good.
- A delivery thread for each subscription. It keeps a slow handler from holding up the writer and the other
  registrations, but it makes the in-memory push model asynchronous, which is out of scope for 0.35.0.

A call from inside any handover's own code doesn't wait for the drain, since that drain can be waiting for the same
code, and its event is held behind the drain instead.

**A waiting `accept(..)` from inside another blocking handover's code, into a handover that isn't live, is refused
with a `PreDispatchRefusalException`.** Before, only a call from the handover's own code was refused. When handover
A's drain fed B and waited while B's drain fed A and waited, the two drains waited for each other forever, and every
later writer waited behind them. In 0.34.0 two drains never waited for each other, because the drain went live before
it delivered, but two replays that fed each other this way did. So a feed's `acceptAndWait(..)` from the projection,
view or handler of another blocking feed or catch-up subscription throws while the feed isn't live. Once the feed is
live, the call applies the event on the calling thread, as before.

The refusal stays, and the code that has the event writes it to the event store instead, so the fed feed gets it the
way it gets its other events. A handler that lets the refusal escape during its own catch-up fails that catch-up for
good, since `BlockingHandover` records the failure and refuses every later event. Feeding the event from a thread
the handler doesn't wait for was rejected as advice. Nothing feeds the event again when that call throws, because the
fed catch-up was stopped or failed, its buffer was full or the thread was interrupted, and the event is not in the
store, so no replay brings it back either. The refusal of a call from the handover's own code has
said "Feed live events from another thread" since 0.34.0, and the same loss applies there, so it gives the same advice
instead.

## Consequences

**A handler that waits for a lock the writing thread holds, or for another thread's write to the in-memory event
store, waits forever with that write while a drain runs.** The changelog, the 0.35.0 upgrade guide and the
`CatchupThenPushSubscriptionModel` javadoc say so.

**A slow or hanging live handler still holds up the in-memory event store's writer and every registration after it**,
as in 0.34.0, and now also while the catch-up delivers what it held. That is a gap in isolation between subscriptions
on the in-memory event store, and a delivery thread for each subscription is the way to close it.

**A handler that writes an event to the event store during its catch-up writes it again each time a replay delivers
the event it reacts to**, unless the write is conditional, on an expected stream version for example. Feeding a
projection feed directly had the opposite problem, since the event never reached the store and a rebuilt read model
lacked it.

**The reactor `ReactiveHandover` is unchanged.** It refuses only the event that finds its buffer full, and the
catch-up goes on.
