# 92. A subscription can filter on a payload field

Date: 2026-08-03

## Status

Accepted. Reverses the part of ADR 87 that left three places refusing. Resolves #499.

## Context

ADR 87 gave the in-memory event store and the in-memory subscription model a `DataFieldReader`, so both can answer
`Filter.data("amount", eq(42))`, and recorded that three places keep refusing because they "wrap any store and have no
configuration surface to hang a reader on". That was the right call for the size of that change. It left two bugs.

**A reactive stream subscription throws on its first event.** MongoDB translates a `data.` condition into the change
stream's own `$match`, so the event arrives already matched. `ReactorStreamCatchupSubscriptionModel` then re-checks it
in memory with a refusing reader, and the refusal is a thrown `IllegalArgumentException` rather than a non-match. This
is reachable through the auto-configured reactive starter: it builds `ReactorCatchupSubscriptionModel`, which
unconditionally builds two of these. The bug was described as store-agnostic, which is true, and as hand-wired only,
which is not.

**A synchronous or push subscription throws on its first event too.** Those models match entirely in memory through
`RegisteringSubscribable`, which called the reader-less matcher overload, and both starters register a synchronous
model bean.

Worth naming because it misled the issue and one planning pass: neither failure happens at `subscribe` time. Building
a matcher reads nothing. `DataFieldReader.refusing()` throws when a payload is actually read, which is on delivery.

## Decision

**Nothing is reshaped, because the reader-taking overloads already existed.** `SubscriptionFilterMatcher.matcherFor`
and `FilterMatcher.matchesFilter` have taken a `DataFieldReader` since ADR 87. The reader-less overloads simply
default to refusing. So the fix is threading, and every change is a new overload or an optional parameter. The option
of making the matchers instance-shaped, which #499 called the cleanest, would have been a breaking change to two
published static utilities to buy what already existed.

**The reader does not travel with the filter.** `Filter.data(name, condition)` is `filter("data." + name, condition)`,
a storage-agnostic value that is also handed to MongoDB to resolve server-side. Attaching an in-memory reading
strategy to it would couple a value object to one backend's evaluation.

**A catch-up wrapper trusts the store for a payload condition, and only for a payload condition.** This is the
decision most likely to be misread later, so it is the one to state carefully.

The in-process re-check on the live tail exists so that a backend which does not honour the filter server-side still
only delivers matching events. That reason holds for attributes and extensions, which are free to re-check. It does
not extend to a payload, because reading one needs a JSON dependency this model has no way to obtain: it wraps an
arbitrary `CheckpointAwareSubscriptionModel`, not a store it could ask.

So the live predicates replace every `data.` condition with one that matches anything and check the rest as before.
Replaced, not removed: dropping a payload condition out of an `OR` would change what the filter means and silently
discard an event that matched only on the payload. `PayloadConditions.assumingPayloadConditionsMatch` is that rewrite,
and it lives in `common/inmemory/filter-matching` because that module already owns what can be matched in memory.

**The consequence, chosen deliberately:** a third-party subscription model that ignores a payload filter now
over-delivers rather than throwing. Occurrent ships no such model. The only non-wrapper reactor
`CheckpointAwareSubscriptionModel` in the tree is `ReactorMongoSubscriptionModel`, which pushes the filter to the
server, so for every configuration Occurrent ships the re-check was verifying something already verified. Over-delivery
is the better failure: a throw on the first event fails a subscription that is otherwise correct.

**A synchronous or push model takes a reader, because for it the in-process match is the only match.** Nothing has
filtered the event before it arrives. `RegisteringSubscribable` gains a constructor taking a reader on both stacks,
defaulting to refusing, and each of the four concrete models gains the matching overload.

**Each starter contributes a reader only when the application asks.** A `DataFieldReader` bean, conditional on
`occurrent-common-inmemory-filter-matching-jackson` being present and on the application not defining its own. Adding
that artifact is the opt-in, which is the pattern ADR 87 already chose for the starters rather than a classpath probe.
The refusal message names the artifact, so an application that hits the refusal is told what to add.

## Consequences

A subscription can filter on a payload field on every model Occurrent ships. What that costs differs by model, and the
difference is not arbitrary: a model that does its own matching needs the reader, a model that re-checks what a store
already matched does not.

`InMemorySubscriptionModel` gains a single-argument constructor taking a reader. It has accepted one since ADR 87 in
its four-argument form only, which meant supplying an executor, a retry strategy and a queue to configure one thing.
That path also had no test at all: nothing in the repository constructed the model with a real reader, so the
threading every other model now copies was verified only by reading it.

Nothing that worked before changes. Every new constructor and overload is additive, the reader-less paths keep
refusing exactly as they did, and no coordinate or signature moved, so this needs no migration recipe.

The gap this leaves is coverage rather than behaviour. These tests live per module, because there is no subscription
conformance suite to hold an out-of-tree subscription model to the same contract. That belongs with #395, which is
where the note goes rather than inventing a suite here.
