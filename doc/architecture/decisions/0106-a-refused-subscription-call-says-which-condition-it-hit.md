# 106. A refused subscription call says which condition it hit

Date: 2026-08-07

## Status

Accepted. Closes #580. Answers the question [ADR 104](0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) and
[ADR 105](0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md)
both deferred here, and settles the type half of the coherence question
[ADR 93](0093-a-missing-capability-is-refused-and-a-reactive-publisher-is-cold.md) opened.

## Context

Every lifecycle refusal a subscription model can produce threw a bare `IllegalArgumentException`. Subscribing with an
id already in use, pausing something that is not running, resuming something that is, passing a filter the model
cannot apply, passing a start position it does not accept, all of them, on both stacks, across roughly 40 places.

A caller could not tell them apart. The only distinguishing information was the message, and
`SubscriptionModelConformance` says the message is not part of the contract, because Occurrent's own models word one
refusal as both "is not paused" and "isn't paused.". Counting properly, the same set of conditions was spelled six
different ways. `Subscribable.subscribe` declared no `@throws` at all, so nothing told a caller these refusals existed
in the first place.

Worse than the wording, one condition had no refusal of its own anywhere. An id a model has never seen fell through
into "is not paused" or "is not running", so "there is no such subscription here" and "there is one and you asked at
the wrong moment" arrived as the same exception with a message that claimed the second.

Two shipped callers paid for that. `OccurrentSubscriptionsExtension` holds several models and does not know which one
owns a given id, so it tries each in turn and catches `IllegalArgumentException` to decide whether to keep looking. It
cannot separate "not mine, try the next one" from "mine, and it is already running", so it searches past the model
that had the answer and then rebuilds a list of which ids exist by hand, from `IntrospectableSubscriptionModel`, to
make the eventual failure readable. The reactor twin does the same thing again. The Spring Boot registrars check for a
duplicate subscription id themselves rather than letting a model refuse it.

## Decision

**Each condition gets its own exception type, and the types form a sealed family under `IllegalArgumentException`.**

```java
public sealed abstract class SubscriptionRefusedException extends IllegalArgumentException
    permits DuplicateSubscriptionIdException, UnknownSubscriptionException, SubscriptionNotRunningException,
            SubscriptionAlreadyRunningException, UnsupportedSubscriptionFilterException, UnsupportedStartAtException
```

The four id-scoped types answer `subscriptionId()`, `UnsupportedSubscriptionFilterException` answers `filterType()`,
and `UnsupportedStartAtException` answers `startAt()`. They live in `subscription/core`, in `org.occurrent.subscription`,
which both API modules already depend on and which no other module puts a class in, so `permits` compiles and every
model can reach them.

**The root is `IllegalArgumentException` because that is what these calls already threw.** Catching it still catches
all six, no call site changes shape, and nothing needs an OpenRewrite recipe. It is also the honest root. Java's rule
is that an argument exception means the value passed was wrong and a different value would have worked, and that is
true of all six. Pausing subscription A while B is running fails because of A, not because the model is in a state
where pausing is impossible.

**Each type builds its own message in its constructor**, the way `WriteConditionNotFulfilledException` has always
done, so the six spellings collapse into one and an implementation outside this repository gets the same wording for
free. A second constructor takes a message of your own, which is how a model adds something the id alone does not say,
for example that a subscription is registered but has not been started yet.

**Sealed, so a caller can match on the whole set exhaustively.** The conformance suite enumerates the conditions a
model owes, so the set really is closed, and saying so in the type is more useful than leaving room for a seventh that
no suite would check. A model with a refusal of its own throws whatever it likes, it just does not join this family.

### A model now says when it has never heard of an id

`UnknownSubscriptionException` is new behaviour, not only a new type. Every model already keeps a registry of the ids
it knows, alongside the running or paused ones, so deciding this costs one lookup before the existing state check. A
wrapper answers for what it holds and what the model underneath holds together, because otherwise it would call an id
unknown that its delegate owns.

`CompetingConsumerSubscriptionModel` is the one place where "known" means something specific. Uniqueness there is
scoped to one model instance ([ADR 102](0102-a-subscription-id-is-unique-per-subscription-model-instance.md)), so an
id held in neither `nonCompetingConsumersSubscriptions` nor by a competing consumer is unknown to that instance
whatever a delegate may separately know. That also fixes a misdirected refusal. Resuming an id the model had never
seen used to report that another consumer currently subscribes to it, which was the last branch it happened to fall
into.

### Where the line with ADR 93 falls

ADR 93 chose `UnsupportedOperationException` for a capability a store was not built with, and
`DataFieldReader.refusing()` threw `IllegalArgumentException` for what looks like the same question on the
subscription side. The two are now told apart by one question. **Can the caller fix it by passing something else, or
do they have to build the object differently?**

Passing something else fixes all six family members, so they are argument exceptions. Nothing a caller passes gives a
store a payload reader it was never constructed with, so `DataFieldReader.refusing()` now throws
`UnsupportedOperationException`. That matches ADR 93 and it matches `StreamPositionDisabledConformance`, which already
holds a store built with its position turned off to refusing the position API the same way, and `supportsDataFilter()`
is declared by the fixture as a capability exactly like that one. The reader is unreleased, so this costs nothing but
two assertions in the event store suites.

### What stays an `IllegalStateException`

[ADR 104](0104-an-undeliverable-push-event-is-refused-not-acknowledged.md) offered its refusals to this family if one arrived. They stay where they are, and the same question decides
it. A catch-up that failed, a projection feed with nothing registered on it, a replay cancelled before it went live,
and a competing consumer whose lock another node holds all reject a perfectly good argument. No different argument
helps, and none of them is a mistake in the calling code. They are either a failure that already happened or the state
of another machine.

So `resumeSubscription` documents both roots. `SubscriptionAlreadyRunningException` when the caller got it wrong, and
`IllegalStateException` when another node holds the subscription.

The one-consumer refusal from [ADR 90](0090-a-push-sink-feeds-one-consumer.md) also stays a plain
`IllegalArgumentException`. It refuses a second subscription with a *different* id, so it is not the duplicate-id
condition wearing another name, and it belongs to the sink topology rather than to a subscription's lifecycle.

## Consequences

`Subscribable.subscribe` and the two `SubscriptionModelLifeCycle` methods now carry `@throws` clauses naming these
types, on both stacks. That is the first time subscribing documented that it can refuse at all.

The conformance suite asserts the types rather than `IllegalArgumentException`, and the test that used to pause an id
the model never had is now two tests, one for an id that does not exist and one for a subscription that is already
paused. That split is the contract change made visible. Only the blocking suite changed, since the reactor models run
it through `BlockingSubscriptionOverReactive` and the reactor suite deliberately covers only what blocking on a result
destroys.

`OccurrentSubscriptionsExtension` searches past `UnknownSubscriptionException` and lets every other refusal through,
so a subscription that is already running now reports that instead of being buried in a rebuilt message about which
ids exist. The Spring Boot registrars and `ManualStartPushSources` throw `DuplicateSubscriptionIdException` with the
context they already had.

These are changes to behaviour that shipped in 0.31.0, so they are in the changelog and in the 0.32.0 migration guide.
Neither the exception class nor the new unknown-id answer breaks a call site, so there is no recipe to write, which is
the same conclusion ADR 102 reached for the same reason.

This is Occurrent's first exception hierarchy. Every other named exception here extends `RuntimeException` directly.
That is recorded so the next one does not copy this without first asking whether its callers need to tell the cases
apart. These ones demonstrably did, and two shipped workarounds are the evidence.
