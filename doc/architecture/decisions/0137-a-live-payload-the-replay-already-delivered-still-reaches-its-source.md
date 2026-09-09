# 137. A live payload the replay already delivered still reaches its source

Date: 2026-09-09

## Status

Accepted. Extends
[ADR 132](0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md) decision 6 to the
catch-up-then-push handover, and amends
[ADR 135](0135-the-reactive-handover-dedup-is-fed-only-by-the-reconciliation-read.md), whose survey of the blocking
stacks did not reach that handover. Resolves [#963](https://github.com/johanhaleby/occurrent/issues/963).

## Context

A catch-up-then-push composition registers its live feed first, replays the store's history, then drains what
buffered while it replayed and goes live. An event committed during the replay can arrive twice, once from the
history read and once from the broker, and both engines de-duplicate that overlap by an id extracted from the
payload.

ADR 132 decision 6 splits a catch-up in two. The history it set out to read records nothing, and everything after
that records, because for some of what follows the catch-up is the only delivery there will ever be. The push
handover sends the boundary at the buffer drain, since a composition that was already caught up replays nothing and
never reaches `replayCompleted()`.

The overlapping event falls on the wrong side of that split. The replay applies it and records nothing, correctly.
The broker's copy is then dropped as a duplicate, so the delivery that would have recorded it never happens. The
projection has applied the event and `AppliedAppendStore.waitUntilApplied` answers `false` for that append until the
caller's wait times out.

Three read sites drop the copy on the blocking engine, `acceptReportingDelivery`, `acceptIfLive` and the drain's own
reservation, and one on the reactor engine, since every live payload there runs through one `concatMap`. Six shipped
compositions ride this engine, both `CatchupProjectionFeed`s, both `DomainEventFeed`s, and both
`CatchupThenPushSubscriptionModel`s.

### Why the two obvious repairs do not work here

**Stopping the replay from filling the cache**, which is what ADR 135 did for the reactor position catch-up and what
all three blocking position stacks already do, would apply the event twice. Those stacks can afford it because they
read history twice, a bulk read against no cache and a reconciliation read against one, so the second read bounds
the overlap. This handover reads once, from the beginning, with no reconciliation phase, and its live side is a
broker buffer with no positional relationship to that read. The overlap has no upper bound, so nothing would
suppress the second application.

**Bounding the replay with a head snapshot** fails for the same reason. There is no position at which the broker's
buffer and the history read meet, and the reader this handover replays from has no `currentHead()` to snapshot.

**Recording on replay delivery** is what ADR 132 decision 6 rejected, on three grounds that all still hold. A full
rebuild would insert one row per historical event. A coalescing view buffers during a replay, so a recorded id would
claim a read model that has not been written. And an abandoned replay discards that buffer, leaving every recorded
id untrue.

## Decision

### 1. The de-dup cache is two caches

`replayedIds` is written only by the replay loop. `deliveredIds` is written only by a successful live delivery.
Every read site checks both, so which payloads are delivered does not change at all, on any path.

What the two caches decide is what happens to the copy that is not delivered. That is one fact per cache, and one
cache cannot say which of the two a suppressed key is.

### 2. A suppression by `replayedIds` calls the source, a suppression by `deliveredIds` does not

The engines gain `Source.alreadyDeliveredByReplay(payload)`, called outside the lock, with the live payload as its
argument.
`CatchupListener` and the two replay-aware view interfaces gain the same call under the same name, so the fact
travels from the engine to the recorder in one vocabulary.

A suppression by `deliveredIds` stays what it was, a no-op. The earlier live delivery ran everything a delivery
runs, the recording included, so there is nothing left owing.

The hook is not a delivery and must not apply the payload again. It exists for the work a source does per delivery
rather than per application, which today is exactly one thing, writing down the append the payload came from.

### 3. The hook fires once per suppressed copy, not once per event

A broker that offers the same event three times gets three calls. Recording an append is writing an id into a set,
so a repeat costs a store round trip and changes nothing. Counting calls, or holding state to make the second one
quiet, would buy nothing and would need its own eviction rule.

### 4. The payload type is whatever that layer already has

`ReplayAware` and `ReactiveReplayAware` take an `EventMetadata`, since a pull feed's live payload has metadata and
nothing else. `CatchupListener` takes a `CloudEvent`, since `subscription/core` does not depend on the
cloudevents extension where `EventMetadata` lives. `RecordingMaterializedView` and `RecordingReactiveUpdate`
implement both and record either way.

## Consequences

- A projection declared `@Projection(recordAppliedAppends = true)` on any of the six compositions now records an
  append whose only delivery was the replay's, so `waitUntilApplied` answers for it.
- Delivery is unchanged. Four tests assert an event overlapping the handover is applied exactly once, two at the
  engines and two at the projection DSL, and none of them changed.
- A second problem closes with the split. The replay used to flood one bounded cache that evicts the eldest, so
  after a history longer than the cache the live-redelivery de-dup held nothing but replayed ids at the moment the
  handover went live. The two caches are filled by one writer each.
- `CatchupThenLiveOptions.dedupCacheSize` sizes each of the two caches, so a handover holds up to twice the ids it
  did. At the default of 10000 that is 10000 more short strings per registration.
- A hook that throws reaches the payload's own acknowledgement rather than the delivery pipeline, so the source
  offers the payload again and the recording is retried. On the drain path a throw releases the reservations the
  drain took, the same recovery a failed drain delivery gets.
- `replayedIds` is never cleared, which was already true of the single cache. An abandoned replay leaves keys behind
  that suppress a later `goLive` drain, and after this change that suppression records an append for state the
  abandoned replay discarded. A clear in `replayAbandoned` would trade that for a second application on a view
  that does not coalesce, so neither branch is right on its own and this ADR decides neither. Filed as
  [#974](https://github.com/johanhaleby/occurrent/issues/974).
