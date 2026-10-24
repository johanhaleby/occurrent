# 154. A saga tells a late stream event from a redelivery per event type

Date: 2026-10-24

## Status

Accepted. Resolves [#1305](https://github.com/johanhaleby/occurrent/issues/1305). Amends
[ADR 109](0109-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of.md), which shipped in 0.32.0 and is
therefore corrected by reference rather than edited. Builds on
[ADR 150](0150-a-global-position-is-not-a-saga-redelivery-key.md), which left the stream key as the only redelivery key.

## Context

A saga recognises a redelivered event by its `streamid` with its `streamversion`. Up to this decision it kept one
number per stream, the highest stream version it had applied, and skipped every event at or below it.

That is only correct if the saga receives a stream's events in version order. The Occurrent event stores write them in
that order, but the feed between the store and the saga does not have to keep it. `KafkaTopicPerTypeDestinationResolver`
puts every event type on its own topic, so Kafka orders one stream's events of one type against each other and nothing
across types. A saga that listens to a stream with two event types can receive version 1000 of type B before version 2
of type A. It then skips the type A event, never applies it, and logs nothing. A custom partitioner, or RabbitMQ with a
prefetch above 1, can reorder events of a single type the same way.

This is the loss [ADR 150](0150-a-global-position-is-not-a-saga-redelivery-key.md) removed for global positions, found
again one level down in the stream version. `AGENTS.md` rules it out for every design.

## Decision

**The saga keeps the highest applied stream version per stream and event type.** Within one type it applies an event in
these cases and skips it in the others:

| Stream version of the event | What the saga does |
|---|---|
| Above the highest applied version of its type | Applies it |
| Equal to it, or one of the lower versions the saga recorded as applied | Skips it as a redelivery |
| Below it by less than the reorder window and not recorded as applied | Applies it, since it arrived late |
| Below it by the reorder window or more | Skips it and logs a warning once per stream |

The shipped Kafka topology keeps one stream's events of one type in order, so with a mark per type it loses nothing. A
mark per stream with a window would not be enough, because two types of one stream can be more than any window apart.

**The reorder window is a `SagaRunnerConfig` field, 256 by default.** An instance records at most that many applied
versions below the highest one, per stream and event type. The stored state therefore never holds more than streams
times types times 256 numbers, whatever the length of the stream. `SagaRunnerConfig.withReorderWindow(int)` changes the
window for a feed that reorders further.

The saga does not remember every applied version. A saga that listens to some of a stream's types sees permanent gaps
in the versions, so a record of what is missing below the mark would never close, and the state would grow with the
stream.

**An event further behind than the window is skipped, not applied.** The common redelivery is a restart that replays
from an old checkpoint, and applying every replayed event would run its reaction again. That is the behaviour before
this decision, and what changes is that the skip now logs a warning naming the stream and the window, so a late event
lost this way is visible. A feed that reorders by more than the window loses those events, and the fix is a larger
window.

The exception is the event the instance's failure record names. That record is cleared by the save that applies the
event, so while it names the event the event was never applied, and the saga treats it as new however far behind it is.
Skipping it would acknowledge an event whose reaction never ran, and a failing event that stays skipped could never reach
quarantine.

**The horizon keeps a raised window from undoing a skip.** Trimming `applied` forgets the versions at or below
`mark - window`, and a version the saga forgot looks the same as one it never applied. If the window were only read from
the configuration, raising it from 2 to 256 would turn every version just forgotten into a late arrival and run its
reaction again. Each type mark therefore stores the horizon, the highest version it has stopped remembering, and a
version at or below the larger of the horizon and `mark - window` is skipped. Lowering the window never lowers the horizon.

**The stored instance gains one field, and the old one keeps its meaning.** `streamMarks` holds, per stream, a floor
and per type the highest applied version, the applied versions below it and a horizon. The floor is the stream's watermark at the moment the stream first got marks, and
every version at or below it stays skipped whatever its type, which is what the instance did before. An instance stored
by 0.34.0 or by the 0.35.0 build of ADR 150 has no `streamMarks`, and it reads as a floor equal to each stream's
watermark and no marks. `streamWatermarks` is still written with the highest version of the stream across types, so a
0.34.0 node reads a document it understands.

## Consequences

A node that goes back to 0.34.0 replaces the document without `streamMarks`. The next 0.35.0 node reads it as a
pre-0.35.0 instance, so it skips what is at or below the watermark and nothing is applied twice. A late event is lost on
that stretch, as it was before this decision.

An instance stores up to 256 extra numbers per stream and event type, and it does so even when the feed delivers in
order, because the saga cannot know the next event will not be a late one. Each number is stored as a 64-bit integer, so
an instance that listens to a handful of streams with a few event types each adds some kilobytes to its document, and
every save rewrites them.

`@Saga` sagas use the default window. There is no property or annotation attribute for it, and one can be added when
someone needs a different value.

`EventMeta`, `SagaEnvelope` and `SagaRunnerConfig` each gain a component. The previous constructors stay, so a call
site compiles unchanged, but a record pattern over `SagaEnvelope` or `SagaRunnerConfig` has to name the new component and
stops compiling. That is a break of the build for shipped types, and the upgrade guide lists it under the changes that
can break the build with the before and after patterns. There is no rewrite recipe for it. The 0.34.0 change to the same
two records got none either, and one could be added by extending `MigrateAppendResultRecordPattern`.

ADR 109 gets a pointer in its Status section.
