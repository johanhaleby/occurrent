# 90. A push sink feeds one consumer

Date: 2026-08-02

## Status

Accepted. Amends ADR 62, both its original fan-out rationale and its 2026-07-28 amendment.

## Context

`PushSubscriptionModel` and `DomainEventFeed` were both fan-out sinks: one instance, N registered consumers, driven by
one `accept(...)` call from a broker listener. That gives one received message **one acknowledgement decision and N
consumers**, and both sinks fanned out through an unguarded ordered loop (`RegisteringSubscribable.route`, and
`DomainEventFeed.accept`, a `for` on blocking and a `concatMap` on reactor).

So a consumer that keeps failing blocks every consumer registered behind it, for that message, forever. The loop aborts
at the failing consumer on every redelivery and the queue head never advances. If the broker eventually dead-letters
the message, the consumers behind it never see it at all. That is both halves of what this project now holds as a hard
rule, written down under "Design intentions" in `AGENTS.md`: **no design may lose events, and no saga, projection or
subscription may be blocked by another one being faulty.**

Three things are worth being precise about, because each of them was offered at some point as a reason this was fine.

**Redelivery does not fix it.** Live redelivery genuinely is de-duplicated: both handovers remember the last 10,000
event ids per consumer, on the replay path and the live path alike, so a consumer that already folded an event skips it
when the broker sends it again. That makes redelivery safe after a *transient* failure. It does nothing for a
persistent one, where every redelivery aborts at the same consumer.

**Fan-out on `Subscribable` in general is not the problem.** An event-store subscription model gives each subscription
its own cursor and its own checkpoint, so one subscription failing leaves the others advancing. The problem is
specific to push, where one external delivery drives N consumers under one acknowledgement.

**The two sinks were already inconsistent about it.** `CatchupThenPushSubscriptionModel` drops a subscription whose
catch-up failed so its siblings live, while `DomainEventFeed.catchUpAll` keeps the failed projection and rethrows for
every later event, permanently, documented as intended. Two answers to one question is the shape of a question that
was never really decided.

ADR 62 rejected a per-projection feed factory as "a narrower fan-out", on API-surface grounds. It never weighed error
isolation or queue topology, so this is not overturning a decision that considered them and went the other way. Its
2026-07-28 amendment keeps `route` unguarded deliberately, so the error reaches the listener and it can nack. That
reasoning is right and survives: the fix is not to guard the loop, it is for there to be nothing to loop over.

Nothing in this repository uses the fan-out. Every Spring test, every integration test and the one example is one
consumer per sink, and the several-consumer case existed only in a handful of unit tests written to check the fan-out
itself.
That is not why the topology changed, since Occurrent's callers are outside this repository, but it does mean the
change costs the tree nothing.

## Decision

**A push sink feeds exactly one consumer, and a second registration is refused.** This is structural rather than
advisory: the shared-queue configuration stops being expressible, so no application can reach the coupled state by
accident. Both types survive, `Subscribable` is untouched, and the topology an application should have had all along,
one queue per projection or saga, is now the only one it can have.

**Isolation is the default, and fan-out is what a subclass opts into.** `RegisteringSubscribable` has four subclasses:
the two push models and the two synchronous ones. Only the synchronous ones may fan out, because they are the
write-path dispatcher: no broker, no acknowledgement, no redelivery, so a handler failure fails the write rather than
stranding a sibling, and under a transaction nothing is folded by anyone. So the base takes one consumer by default and
the synchronous models declare `Consumers.MANY` with the reason written at the declaration. Not the other way round.
Under the isolation rule the dangerous configuration is the one that has to announce itself, and a fifth subclass added
later then gets the safe behaviour without anyone remembering to ask for it.

**Expressed as a protected constructor argument rather than an overridable method.** It is a fixed property of the
instance, it cannot be overridden inconsistently, and it appears in the subclass where a reader will meet it.

**`subscribe` stays `final`.** The first attempt at this reached for an overridable method purely to route around that
`final`, which was the wrong instinct: the constraint should be questioned, then kept if it earns its place. It earns
its place. It owns id uniqueness, filter translation and ordered dispatch, and this design needs nothing from removing
it. `AGENTS.md` records the general form of that lesson.

**The check counts current registrations, not whether one was ever made.** `CatchupThenPushSubscriptionModel` cancels a
subscription whose catch-up failed, and the same id must then be subscribable again.
`the_same_subscription_id_can_be_used_again_after_a_catch_up_failure` is the test that would fail if it stopped being
true. A one-way latch would break it, so the sole registration is held in an `AtomicReference` that
`cancelSubscription` clears.

**The rejection message is the migration path.** There is no OpenRewrite recipe for a bean topology: the fix is to
declare a second sink bean and point the second projection at it, which no rewrite can infer. A startup failure naming
both the consumer already registered and the one refused, and saying to declare one sink per projection or saga, is
more useful than a rewrite would be. It is shared verbatim across all four sinks through `SingleConsumerMessages`.

## Consequences

**This breaks an application that shares one push sink between several projections**, and it breaks it at startup with
a named failure rather than at runtime with a wrong answer. The feature shipped in 0.31.0, so the blast radius is a
single release wide. Pre-1.0, correcting this is preferred over carrying it into 1.0.

**Three problems dissolve rather than getting solved.** `startupMode` on a domain-feed projection is now unambiguously
per-projection, because a feed *is* one projection, so two projections on one feed can no longer disagree about it.
`catchUpAll`'s "one that failed early blocks the ones behind it" clause has no siblings left to apply to. Its terminal
contract still holds for the one projection, which is correct and unchanged. And `catchUp(String id)`, which #497
added for a projection registered late, keeps working and stays useful as the form that fails on an id mismatch, but
isolation no longer depends on it.

**`DomainEventFeed.catchUpAll` keeps a name that describes a topology it no longer has.** Renaming it means deprecating
a method that shipped one release ago to save one word, so the name stays and the javadoc says why.

**The synchronous models are left as they are, and that is not fully settled.** A faulty synchronous projection still
blocks the ones registered after it. Under a transaction that is arguably correct, since the write rolls back and
nothing is lost. But `TransactionExecutor` defaults to `noTransaction()`, and then the event is written while the
projections behind the faulty one never fold it, which is loss by the definition above. Checking that against the rule
on its own terms is ADR 57 territory and is filed separately rather than bundled here.

**Live redelivery de-dup is now documented.** It was real, it is what makes a transient failure recoverable, and it
was mentioned in no ADR and covered by no test for the case where an event is sent twice live and never replayed.
Whatever else changed, that gap was worth closing.
