# 92. A subscription model owns its filter

Date: 2026-08-03

## Status

Accepted. Resolves #499, and partially supersedes ADR 87.

## Context

#58 (ADR 87) gave `InMemoryEventStore` and `InMemorySubscriptionModel` a `DataFieldReader`, so `Filter.data(..)`
works in process. ADR 87 lines 87-93 recorded that three further places keep refusing because they "wrap any
store and have no configuration surface to hang a reader on": `SubscriptionFilterMatcher`,
`RegisteringSubscribable`, and `ReactorStreamCatchupSubscriptionModel`. That framing treated all three as one
problem. They are two.

#499 reported the concrete symptom: a subscription filtering on a CloudEvent `data` payload field refused, for
every store including MongoDB, even though the same filter works when querying the same store.

## Decision

The two halves ship separately, because only the first needs no new API. The reactor catch-up change lands with this
record; the reader on the synchronous and push models follows in its own change. Both are needed before the next
release, since the symptom #499 describes has two independent causes and fixing one leaves the other.

### The reactor catch-up model stops re-applying the filter

`ReactorStreamCatchupSubscriptionModel` refused at two sites (before this change, lines 209 and 215) which both
sat downstream of `subscriptionModel.subscribe(StreamSubscriptionFilter.filter(filter), ..)` — the same filter,
capability scope already ANDed on. So the in-process match was a second evaluation of a filter the wrapped model
had already applied. Three reasons it goes rather than being taught to read a payload:

- It could not keep its own promise. Its comment said it existed "so a backend that does not honor the filter
  server-side still only delivers matching events", but it refused an entire filter category outright, turning a
  filter the backend answered correctly into a per-event exception. It broke what it was defending.
- Nothing shipped needed it. The only non-wrapper reactor `CheckpointAwareSubscriptionModel` is
  `ReactorMongoSubscriptionModel`, which applies the whole filter server-side (`ReactorMongoSubscriptionModel.java:202`
  via `ApplyFilterToChangeStreamOptionsBuilder.java:53-56`). The blocking twin `StreamCatchupSubscriptionModel`
  has never had the check and always worked.
- It contradicted the SPI it depends on. `Subscribable.java:47` documents the filter as what limits "which events
  that are of interest from the EventStore", making that the model's job. A wrapper double-checking is distrust
  of a contract the blocking stack already relies on.

Also: the translation it duplicated is total over the filter type. `Filter` is a `public sealed interface`
(`Filter.java:39`) and `FilterConverter.convertFilterToCriteria` (`FilterConverter.java:58-76`) switches
exhaustively over `All`, `SingleConditionFilter`, `CapabilityFilter` and `CompositionFilter` with no default
branch, so nothing is silently dropped and a new variant breaks the build. Keeping both meant one filter
evaluated by two independent implementations, and ADR 87 lines 73-85 had to measure the in-process rules against
MongoDB to make them agree, so drift between them was a live hazard.

What is kept: the `OccurrentCloudEventExtension.getPosition(cloudEvent) > 0` guard, which is not a filter
concern — it drops an event a position-ordered handover cannot place.

Rejected alternative: keep the in-process match but strip data clauses out of the filter first. Safe as an
over-approximation under `AND`, but under `OR` it drops events that matched only the data clause, trading a loud
failure for silent under-delivery.

### The models that own their matching take a reader

`SynchronousSubscriptionModel` and `PushSubscriptionModel` (both stacks, four classes, sharing
`RegisteringSubscribable`) dispatch in process with no backend to delegate to, so a reader is the only way they
can answer a data filter. They get one by constructor, reader-less staying the default. The Spring starters
expose `JacksonDataFieldReader` as a `@ConditionalOnMissingBean DataFieldReader` so a Spring user's data filter
works without configuration; this adds no library, because `com.fasterxml.jackson.core:jackson-databind:2.21.4`
already resolves on both starters via `io.cloudevents:cloudevents-json-jackson` (verified with
`mvn dependency:tree`). ADR 87's no-JSON-dependency rule was about the matching module, not the starter.

### The refusal is eager, and the rule stays in one module

A data filter with no reader used to register successfully and throw per event from inside the predicate. The
matcher is now compiled up front and refuses there, which makes true an intent already written above both
matcher-build sites ("Build the matcher before reserving the id, so an unsupported filter does not leave the id
permanently taken"). To avoid exposing "does this filter read a data field" as public API, the validation happens
inside `common/inmemory/filter-matching`: `FilterMatcher` compiles a `Filter` plus a `DataFieldReader` into a
`Predicate<CloudEvent>`, and `DataFieldReader.refusing()` returns a package-private singleton the module
recognises rather than a lambda, so no `canReadData()`-style method appears on the public interface. The
`data.`-prefix rule (`ConditionMatcher.java:157`) never leaves the module. `InMemoryEventStore` deliberately
keeps lazy per-event evaluation: a query throws to the caller on the same call either way, so only a
subscription, which registers now and fails later on a background thread, gains from failing early.

## Consequences

A data filter now behaves the same whether a store is queried or subscribed to, on both stacks. ADR 87's
"Wired only where a caller already configures something" section is superseded in part: the reactor catch-up
model needed no configuration surface, because it should never have been matching. The two stacks' catch-up
models are structurally aligned, and `subscription/util/reactor/stream-catchup-subscription/pom.xml` no longer
depends on `occurrent-common-inmemory-filter-matching`, which is the structural confirmation that the code did
not belong there.

Nothing is removed from the public API: both `FilterMatcher.matchesFilter` overloads and both
`SubscriptionFilterMatcher.matcherFor` overloads stay, since they ship in 0.31.0. After this change neither
reader-less overload has an in-repo caller, which is when to decide separately whether to deprecate them
(removal would need an OpenRewrite recipe plus a migration doc).

A reactor subscription model that ignores the filter it was handed now over-delivers on the catch-up path
instead of being silently compensated for; that model is violating `Subscribable`'s documented contract, and the
blocking stack has always relied on it.

What this does not settle: whether the blocking catch-up model should gain any in-process guard at all, and DCB
subscription filters, which have no payload concept, so the `DcbSubscriptionFilter` branch of `matcherFor` is
untouched.
