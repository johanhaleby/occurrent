# 113. A competing consumer's status and its lease call are one step

Date: 2026-08-08

## Status

Accepted. Fixes #651. Leaves the hand-back [ADR 112](0112-a-competing-consumer-can-be-paused-while-still-waiting-for-the-lock.md)
added on the subscription model's side alone, and says why the strategy needs none of its own.

## Context

`MongoLeaseCompetingConsumerStrategySupport` keeps a map from competing consumer to status for one
process. Every operation on it has the same three parts. Read the consumer's current status, make the
MongoDB call that status decides, write the result back. What the listeners are told is worked out by
comparing that result against the status that was read at the start.

Nothing serialized those three parts. The scheduled refresh runs them for every consumer the node holds,
and runs the registering path for every consumer that does not have the lease, while an application thread
starting, pausing or stopping a subscription runs them as well. Two threads could be inside them for one
consumer at the same time, and the one that finished last wrote a result it had worked out against a
status that was no longer there.

The expensive outcome is a consumer unregistered while the refresh was acquiring the lease for it.
`CompetingConsumerSubscriptionModel` removes a consumer from its own map before it calls the strategy, so
once the refresh writes that consumer back, nothing is left that will ever unregister it. The refresh
keeps committing its lease every half lease for as long as the process lives, the model has no record of
the consumer, and no other node can take that subscription over. One subscription is out of service for
the whole cluster until that process restarts.

The cheaper outcome is two threads reading the same status before either of them writes, so one change
from not having the lease to having it reaches `CompetingConsumerListener` twice, or a real change does
not reach it at all.

Both `NativeMongoLeaseCompetingConsumerStrategy` and `SpringMongoLeaseCompetingConsumerStrategy` are built
on this class, so both had all of it. Both already declare `registerCompetingConsumer` and
`unregisterCompetingConsumer` `synchronized`, which serializes two application threads against each other
and does nothing about the refresh thread, which never passes through either wrapper, and nothing about
`releaseCompetingConsumer`, which is not declared that way.

One thing is worth being explicit about, because a lock sitting next to a lease invites the wrong reading.
Nothing in this class decides which node consumes a subscription. The lock document decides that, through
a conditional `findOneAndUpdate` that matches only an expired lease or one the asking subscriber already
holds, written with majority write concern. This class keeps one node's view of its own consumers honest
about the calls that node made, and that is all it does.

## Decision

**Read the status, call MongoDB, write the result and work out what changed, all under one lock per
consumer.** That makes the three parts one step for a given consumer, which is what both outcomes above
need. Registering, unregistering, releasing and each consumer's turn in the refresh all take it.

**Tell the listeners after that lock is released, and never while holding it.** This is the constraint
that decides the shape of everything else, so it is worth stating plainly. A listener runs into
`CompetingConsumerSubscriptionModel`, whose callbacks are synchronized on the model, and which calls back
into the strategy from inside them, the hand-back in ADR 112 being one example. An application thread
pausing a subscription holds that same monitor before it ever reaches the strategy. Telling a listener
while holding the consumer means the refresh thread waits for the model while the application thread waits
for the consumer, and neither of them comes back. So the locked part works out which callback is due and
returns it, and the caller makes the call afterwards.

**The lock is a fixed array of 16 `ReentrantLock`s, picked by the consumer's hash.** A map from consumer
to lock has no safe moment at which an entry can be removed, so it grows for as long as the process runs.
A single lock for the whole instance would be correct too, but then registering one subscription would
wait for another subscription's round trip, which under the default retry strategy runs into seconds. Two
consumers landing on the same lock is that single lock again, for that pair only, and one support instance
holds one consumer per subscription, so a handful of them.

**Each consumer's turn in the refresh reads the status again under the lock rather than using what the
iteration handed over.** `ConcurrentHashMap.forEach` reads a consumer without holding its lock, so another
thread can unregister or release that consumer between the read and the refresh taking the lock. A
consumer that has gone is skipped for that round rather than written back.

**A refresh that finds its consumer gone does not hand the lease back, because it never took one.** The
check that the consumer is still registered happens inside the same locked step as the MongoDB call, so
there is no ordering in which an acquire completes for a consumer that was unregistered first. ADR 112's
hand-back stays exactly as it is, because it answers a different question, what the model does about a
grant that has already been delivered to it. That delivery is outside every lock here by the rule above,
which is precisely why the model has to be able to refuse it.

## Consequences

An application thread unregistering a consumer now waits for whatever call this node already had in flight
for that same consumer, which is one MongoDB round trip, or its retry backoff when MongoDB is unwell.
Handing a subscription over to another node is that much slower in a window where it used to be wrong.

The `synchronized` on `registerCompetingConsumer` and `unregisterCompetingConsumer` in both strategy
classes is no longer needed for correctness, now that the support serializes every caller including the
refresh thread. It is not free either. It is one monitor for the whole strategy, so two subscriptions
registering at the same time still wait for each other, which is the cost the locks here were shaped to
avoid, and release and the refresh thread never go through it anyway. Taking it off is a change to two
public classes in other modules, so it stays for now and comes out on its own.

> **Amended. The `synchronized` came out.** Neither `NativeMongoLeaseCompetingConsumerStrategy` nor
> `SpringMongoLeaseCompetingConsumerStrategy` declares `registerCompetingConsumer` or
> `unregisterCompetingConsumer` `synchronized` any longer. Registering or unregistering a consumer no
> longer waits behind every other consumer's call on the same strategy instance.

Two threads can still deliver their callbacks in the other order, in the window between one of them
releasing the lock and making its call. The model tolerates it, since `onConsumeGranted` looks the
consumer up, does not find it and returns, and a paused consumer hands the lease back per ADR 112.
Ordering the deliveries would need a sequence number per consumer, which brings back the map with no safe
moment to remove an entry from, for a window the model already copes with.

`MongoLeaseRaceTest` interleaves the threads on purpose rather than hoping they collide, by handing the
code under test a `MongoCollection` that stands still inside a chosen call until the test lets it go.
Four properties are asserted that way. A lease is not left with a consumer that was unregistered while the
refresh was taking it, one change of status is reported once, a failing MongoDB call leaves the consumer
as it was and gives it back to the next caller, and no consumer is held while a listener runs. That last
one deadlocks and fails on the test's timeout if the notification is ever moved back inside the lock,
which is the intended way to find out.

The re-read of the status under the lock has no test of its own. The window it closes is between
`forEach` reading a consumer and the refresh taking that consumer's lock, and there is no MongoDB call in
between to stand still inside, so forcing it would mean putting a hook into production code for a window
of a few microseconds. It stays as reasoning rather than as an assertion.

Two weaknesses in the lease itself came up while working on this and are not addressed here, because
neither is about one node's view of its consumers. `MongoListenerLockService.acquireOrRefreshFor`
computes a version for each lease and its own javadoc says a caller needs it as a fencing token, but
`ListenerLock.version()` has no callers, so a node frozen past its lease believes it still holds the lease
until its next refresh. And whether a lease has expired is judged against the asking node's own clock
while `expiresAt` was written from the holder's, so a node whose clock runs fast can take a healthy lease.
Both are filed separately.
