# 67. Configurable change stream batchSize and maxAwaitTime

Date: 2026-07-21

## Status

Accepted

## Context

Tuning the MongoDB change stream cursor matters for high-load subscription consumers such as an outbox. Two knobs
dominate: `batchSize`, the number of documents the server returns per batch (a larger value cuts round-trips and
improves throughput), and `maxAwaitTime`, how long the server holds an idle `getMore` before returning a possibly
empty batch (a smaller value lowers latency, a larger one reduces chatter). Occurrent's subscription models opened
their change streams with driver/server defaults and offered no way to set either. This was raised in
[#173](https://github.com/johanhaleby/occurrent/issues/173).

Occurrent has three MongoDB subscription models, each on a different driver stack, and the stacks do not expose the
same surface:

- **`NativeMongoSubscriptionModel`** opens the stream directly on the sync driver's `MongoCollection.watch(...)`,
  returning a `ChangeStreamIterable` that has both `batchSize(int)` and `maxAwaitTime(long, TimeUnit)`.
- **`SpringMongoSubscriptionModel`** reads through Spring Data's `MessageListenerContainer`. The stream is described
  by a `ChangeStreamRequest`/`ChangeStreamRequestOptions`, whose surface carries a `maxAwaitTime` (applied by Spring's
  `ChangeStreamTask`) but no batch size &mdash; `ChangeStreamTask` never calls `ChangeStreamIterable.batchSize`. There is
  no supported way to set a batch size on this path short of abandoning the container.
- **`ReactorMongoSubscriptionModel`** reads through `ReactiveMongoTemplate.changeStream(...)`, whose `ChangeStreamOptions`
  carries neither knob. The raw reactive driver's `ChangeStreamPublisher` supports both, but reaching it means bypassing
  `ReactiveMongoTemplate.changeStream` and driving `MongoCollection.watch(...)` via `execute(...)`.

## Decision

**Expose `batchSize` and `maxAwaitTime` on each config only where the underlying stack supports them, rather than a
uniform surface with silent no-ops.**

- `NativeMongoSubscriptionModelConfig` gains `batchSize(int)` and `maxAwaitTime(Duration)`, applied to the
  `ChangeStreamIterable` before the cursor is opened.
- `SpringMongoSubscriptionModelConfig` gains `maxAwaitTime(Duration)` only, applied via the four-argument
  `ChangeStreamRequestOptions(database, collection, maxAwaitTime, changeStreamOptions)` constructor. `batchSize` is
  intentionally not offered here; its javadoc states why.
- `ReactorMongoSubscriptionModelConfig` gains neither in this change.

A uniform API where an unsupported option compiles but does nothing would be worse than an honest asymmetry: it would
imply a capability that is not there. The asymmetry is unavoidable regardless &mdash; `batchSize` cannot reach the Spring
blocking path without replacing `MessageListenerContainer` &mdash; so skipping a model does not buy a clean matrix, it
only moves the empty cell.

**Both options default to unset, so existing behavior is unchanged.** Fields are `@Nullable` and left `null` by default,
meaning the driver/server default still applies and no existing subscription changes its latency, throughput, or resource
profile on upgrade. Adopting the issue's suggested values (`batchSize` around 500, `maxAwaitTime` 200&ndash;1000 ms) as
defaults would have been a silent behavior change for every deployment; those values are documented as javadoc guidance
instead. This keeps the change additive, matching the repository convention. Values are validated eagerly:
`batchSize > 0` and `maxAwaitTime` at least 1 millisecond (`toMillis() > 0`, since the driver call is millisecond-based
and a sub-millisecond value would otherwise silently truncate to 0), throwing `IllegalArgumentException` otherwise.

**The Reactor path is deferred rather than bypassed now.** Supporting both knobs on Reactor requires leaving
`ReactiveMongoTemplate.changeStream` and driving the raw reactive driver, which concentrates real regression risk on the
most delicate model for the lowest-value cell. In particular, `shouldRestart` filters on the returned exception, and
Spring's `changeStream` runs its persistence-exception translator over driver errors; driving the raw publisher via
`execute(...)` yields the untranslated `MongoException`, so the change-stream-history-lost (error code 286) restart
detection would have to be re-verified. That is not worth bundling into this change. When it is pulled in, the native
model's private `createPipeline(TimeRepresentation, SubscriptionFilter)` should move into the shared
`occurrent-subscription-mongodb-common-base` module so native and reactor share one implementation, and the existing
generic `MongoCommons.applyStartPosition` (already used by the native path) applies the start position to the reactive
publisher unchanged.

## Consequences

- High-throughput native subscriptions can tune both knobs; Spring blocking subscriptions can bound delivery latency
  with `maxAwaitTime`. The change is additive and backward compatible on every model.
- The subscription API is deliberately asymmetric across models, documented in the configs' javadoc and here. A caller
  who needs `batchSize` must use the native model.
- Reactor gains no tuning yet; the follow-up and its main hazard are recorded above so the work can resume with the
  context intact.
