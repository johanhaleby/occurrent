# 137. A live payload the replay already delivered still reaches its source

Date: 2026-09-09

## Status

Accepted. Extends
[ADR 132](0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md) decision 6 to the
catch-up-then-push handover, and amends
[ADR 135](0135-the-reactive-handover-dedup-is-fed-only-by-the-reconciliation-read.md), whose survey of the blocking
stacks did not reach that handover. Resolves [#963](https://github.com/johanhaleby/occurrent/issues/963).

Updated before release for [#1041](https://github.com/johanhaleby/occurrent/issues/1041), which adds decisions 5
and 6 and settles the question [#974](https://github.com/johanhaleby/occurrent/issues/974) left open.

## Context

A catch-up-then-push composition registers its live feed first, replays the store's history, then drains what
buffered while it replayed and goes live. An event committed during the replay can arrive twice, once from the
history read and once from the broker, and both engines de-duplicate that overlap by a key extracted from the
payload. Both push models key a CloudEvent by its id and source together, since CloudEvents only promises that pair
to be unique, and two producers can each send an event with id `1`. A projection feed keys a domain event by the id
its caller extracts.

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
Every read site checks both, so splitting the cache does not change which payloads are delivered, on any path.
Decision 6 is the one place delivery does change.

What the two caches decide is what happens to the copy that is not delivered. That is one fact per cache, and one
cache cannot say which of the two a suppressed key is.

### 2. A suppression by `replayedIds` calls the source, a suppression by `deliveredIds` does not

The engines gain `Source.alreadyDeliveredByReplay(payload)`, called outside the lock, with the live payload as its
argument.
`CatchupListener` and the two replay-aware view interfaces gain the same call under the same name, so the fact
travels from the engine to the recorder in one vocabulary.

A suppression by `deliveredIds` stays what it was, a no-op. The earlier live delivery ran everything a delivery
runs, the recording included, so there is nothing left owing.

The hook is not a delivery and must not apply the payload again. It exists so a recording projection can write down
the append the payload came from, which neither the replay nor the suppressed copy would otherwise do.

The hook goes to the source whose replay filled `replayedIds`. That source is set when a replay starts, not by every
catch-up, so a catch-up that replays nothing, a feed's `goLive()` after its `catchUp()`, keeps reporting to the
source that can record. A replay also clears `replayedIds` when it starts, so every key in it came from the source
the hook reports to.

### 3. The hook fires once per suppressed copy, not once per event

A broker that offers the same event three times gets three calls. Recording an append is writing an id into a set,
so a repeat costs a store round trip and changes nothing. Counting calls, or holding state to make the second one
quiet, would buy nothing and would need its own eviction rule.

### 4. The payload type is whatever that layer already has

`ReplayAware` and `ReactiveReplayAware` take an `EventMetadata`, since a pull feed's live payload has metadata and
nothing else. `CatchupListener` takes a `CloudEvent`, since `subscription/core` does not depend on the
cloudevents extension where `EventMetadata` lives. `RecordingMaterializedView` and `RecordingReactiveUpdate`
implement both, and both overloads make the check in decision 5 before they record.

### 5. The hook proves a delivery, not an application

A call to `alreadyDeliveredByReplay` says the replay delivered a payload with the same key. It does not say the
projection applied it. `Projection.id` returning `null` skips an event, and the replay delivers that event all the
same.

So each recorder remembers the appends its replay applied an event of, and records a suppressed copy only when its
append is among them. The set belongs to one catch-up and starts empty with the next. It holds up to 10000 appends,
the handover's default replay cache size, which counts events rather than appends, so at that default no append is
forgotten while a copy of one of its events can still be suppressed. An append past the bound stays unrecorded and a
wait for it times out, which is the one wrong answer this allows. A wait never answers `true` for an append nothing
applied.

### 6. An abandoned replay's keys are cleared

When a replay is stopped or fails, the engine clears `replayedIds` and forgets the source that filled it, before it
calls `replayAbandoned()`. A pull feed's recorder forgets the appends that replay applied at the same time.

A view that coalesces has just discarded what it buffered from that replay. A key left behind would suppress the only
copy of an event the read model never received, and since `replayedIds` has no live writer that loss would last as
long as the handover. A view that writes through applied every event the replay delivered, so a later live copy
reaches it a second time. `goLive()` already promises at-least-once delivery, so the duplicate is allowed, and a lost
event is not.

Clearing the keys is not enough when the replay runs on a handover that is already live, a feed's `catchUp()` after
its `goLive()`. A live payload delivered while that replay runs goes into the same buffer the stop throws away, whether
or not its key was suppressed. So a replay holds live payloads back until it ends, the same as before a first
catch-up, after waiting for any live delivery or replay callback still running. They are delivered when it ends,
completed or stopped, and a handover that was live before the replay stays live after a stop rather than dropping
later payloads. While the replay runs, `acceptIfLive` refuses on both engines, so a caller that can redeliver is told
to try again.

A replay that fails ends differently on the two engines, because they acknowledge at different moments. The blocking
engine has already reported each buffered payload handled, so it delivers them before it records the failure. The
reactive engine has not acknowledged the payloads it holds back, and the failure fails their acknowledgements, so it
does not deliver them. Their callers offer them again, and the handover refuses everything from then on.

This also settles the reactive engine's second catch-up. Its live sink accepts one subscriber ever, so a catch-up on
a handover that is already live does not subscribe it again and keeps the pipeline that is already running.
Subscribing again was refused by the sink, recorded as a failed catch-up, and made the handover refuse every later
payload.

## Consequences

- A projection declared `@Projection(recordAppliedAppends = true)` on any of the six compositions now records an
  append whose only delivery was the replay's, when the replay applied an event of it, so `waitUntilApplied` answers
  for it.
- Delivery is unchanged outside decision 6. Four tests assert an event overlapping the handover is applied exactly
  once, two at the engines and two at the projection DSL, and none of them changed.
- A recording projection holds up to 10000 append ids per catch-up, one for each append its replay applied an event
  of.
- A second problem closes with the split. The replay used to flood one bounded cache that evicts the eldest, so
  after a history longer than the cache the live-redelivery de-dup held nothing but replayed ids at the moment the
  handover went live. The two caches are filled by one writer each.
- `CatchupThenLiveOptions.dedupCacheSize` sizes each of the two caches, so a handover holds up to twice the ids it
  did. At the default of 10000 that is 10000 more short strings per registration.
- A hook that throws reaches the payload's own acknowledgement rather than the delivery pipeline, so the source
  offers the payload again and the recording is retried. On the drain path a throw releases the reservations the
  drain took, the same recovery a failed drain delivery gets.
- `replayedIds` is cleared when a replay is abandoned (decision 6), which answers
  [#974](https://github.com/johanhaleby/occurrent/issues/974). After `stopCatchUp()` and then `goLive()`, a view that
  coalesces receives the events the stopped replay discarded, and a view that writes through receives them a second
  time. Neither suppression records an append, since nothing is suppressed.
- A replay that finishes keeps its keys until another replay starts or the cache evicts them. It applied and saved
  everything it delivered, so suppressing a later copy of one of those events is what the de-dup is for.
