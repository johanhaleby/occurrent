# 102. A subscription id is unique per subscription model instance

Date: 2026-08-06

## Status

Accepted. Closes #553, the contract question TCK phase 8 parked rather than settled in passing, recorded as an
amendment on [ADR 94](0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md).

## Context

The subscription TCK asserts that a model refuses a subscription id that is already in use, because a subscription id
identifies one subscription and reusing a live one would otherwise silently replace the handler already behind it.
The blocking stack honours that today: `InMemorySubscriptionModel`, `NativeMongoSubscriptionModel` and
`SpringMongoSubscriptionModel` throw `IllegalArgumentException("Subscription <id> is already defined.")`, and
`RegisteringSubscribable` throws its "is already registered" equivalent. So does the reactor stack, with the same
`IllegalArgumentException("Subscription <id> is already defined.")` message:
`ReactorMongoSubscriptionModel`, `ReactorDurableSubscriptionModel`, `ReactorCatchupSubscriptionModel`, and the two
models sharing `NamedCatchupSupport`, `ReactorStreamCatchupSubscriptionModel` and `ReactorDcbCatchupSubscriptionModel`.

`CompetingConsumerSubscriptionModel` did not, and that is not obviously a bug, which is why it went to an issue. The
whole point of the competing consumer pattern is that several subscribers use *one* subscription id and a
`CompetingConsumerStrategy` decides which of them consumes. A refusal written without care would break the pattern it
is refusing on behalf of. So the contract has to say what the uniqueness is scoped to before anything can be fixed.

Two readings were available. Either one subscription id means one subscription everywhere, in which case the model is
right to accept and the suite is asserting something four models happen to do rather than a contract; or it means one
subscription id means one subscription within a model instance, in which case the model has a bug and the suite is
right. Weakening the assertion was never on the table: it would have turned the property off for the four models that
honour it in order to accommodate the fifth.

## Decision

**Uniqueness is per model instance. Sharing a subscription id across instances is untouched, because that is the
pattern itself.**

Three things decide it, and none of them is a preference.

**A node is an instance.** `OccurrentMongoAutoConfiguration.occurrentCompetingDurableSubscriptionModel` is a plain
singleton `@Bean`, so an application running on three nodes has three application contexts and three
`CompetingConsumerSubscriptionModel` objects, each with its own delegate. Every existing test says the same thing in
its own way: `CompetingConsumerSubscriptionModelTest` and `CompetingConsumerSubscriptionModelChaosTest` build
`competingConsumerSubscriptionModel1` and `competingConsumerSubscriptionModel2` over separate delegates and subscribe
both to one id, and the one test named for multiple consumers of a subscription is two instances rather than two
subscribers on one. Nothing in the repository registers two subscribers for one id on a single instance, including all
twenty call sites of the overload that takes an explicit subscriber id, every one of them in that one test class. So
the refusal costs the pattern nothing it uses.

**Two consumers on one instance could not have worked anyway.** `cancelSubscription`, `pauseSubscription` and
`resumeSubscription` all resolve their consumer by subscription id alone, through
`findFirstCompetingConsumerMatching`, so the second of two would have been unreachable through every one of them, and
whichever the map happened to yield first would have answered for both. `subscriptionIds()` reports one id for the
two. They also share the single delegate, which refuses a duplicate id in its own right, so at most one of them could
ever have been subscribed. What the model accepted was not a working second consumer but a registration nothing could
address afterwards.

**The competition is external, and the strategy already owns it.** Coordination between consumers happens through
storage the model does not see, and `CompetingConsumerStrategy` is the interface for it. Two consumers inside one
instance would be competing through that external storage against a rival in the same JVM, sharing the delegate they
are competing to drive. There is nothing the pattern gains from it that a second instance does not give properly.

**The pause refusal is separate and was never a contract question.** `SubscriptionModelLifeCycle.pauseSubscription`
documents `@throws IllegalArgumentException If subscription is not running`, and the delegate throws for an id it does
not have. The wrapper looked for a competing consumer, found none, logged, and returned. That is a forwarding
omission, fixed by throwing, and this ADR records it only because it shipped in the same released model.

## Consequences

`CompetingConsumerSubscriptionModel` now throws `IllegalArgumentException` from `subscribe` for a subscription id it
already has, and from `pauseSubscription` for one it does not have. Both are changes to behaviour that shipped, so
both are in the changelog and the 0.32.0 migration guide. Neither is a source break, so there is no OpenRewrite
recipe to write: the call shape is unchanged and only what it does at runtime differs.

Occupying a subscription id and being a competing consumer for it are two different things in this model, and the
refusal covers both. A subscription whose start position opts out of competing consumption
(`StartAt` resolving to `null` for this model) is delegated straight through and remembered only in
`nonCompetingConsumersSubscriptions`, so it holds the id without ever being a competing consumer. Making the refusal
read only the competing consumers would have let a second subscription take an id the first one already had.

That in turn made a latent bug reachable. `cancelSubscription` removed the competing consumer but never removed from
`nonCompetingConsumersSubscriptions`, so a cancelled opted-out subscription was remembered forever. Before this change
that leak was only visible in `start()`, which resumed a subscription the delegate no longer had; with the refusal in
place it would also have refused a legitimate re-subscribe after a cancel. Cancelling now forgets the id from both
collections, which is what "cancelling releases the subscription id for reuse" means everywhere else.

The refusal is not atomic against a concurrent `subscribe` of the same id on one instance, matching the four models
that already refuse: all of them read their registry and then write it. Closing that race here would mean holding the
model's monitor across a call into the strategy, which is the lock ordering its own callbacks take in the opposite
direction.

**Still open, and deliberately not fixed here.** Pausing a consumer that is in the `Waiting` state (registered with the
strategy, not currently holding the lock) is still silently ignored, so a caller who pauses a subscription on every
node has it honoured on the leader and dropped on the rest, and a waiting consumer that was asked to pause still
starts when the lock is granted. That is not the case #553 named and not one the suite reaches, and it cannot be fixed
by throwing: the honest fix is for the wrapper to report `Waiting` through `isPaused`/`isRunning` and pause it as a
state of its own, which is a state reporting change rather than a forwarding one. Filed as #565 under #396.

The general conformance wiring #555 held back now ships as
`CompetingConsumerSubscriptionModelConformanceTest`, so the declarations in `CompetingConsumerSubscriptionModelFixture`
are asserted facts rather than reasoned claims, and this model is held to the same contract as the other four.
