# 154. A saga tells a late stream event from a redelivery per event type

Date: 2026-10-24

## Status

Accepted. Resolves [#1305](https://github.com/johanhaleby/occurrent/issues/1305). Amends
[ADR 109](0109-a-saga-refuses-an-event-it-cannot-recognise-a-redelivery-of.md), which shipped in 0.32.0 and is
therefore corrected by reference rather than edited. Builds on
[ADR 150](0150-a-global-position-is-not-a-saga-redelivery-key.md), which left the stream key as the only redelivery key.

Amended in place on 2026-11-04, before 0.35.0 shipped. A pending version is no longer dropped, for
[#1330](https://github.com/johanhaleby/occurrent/issues/1330). Each pending version keeps
the instant it first failed, for [#1318](https://github.com/johanhaleby/occurrent/issues/1318), as
[ADR 157](0157-a-sagas-quarantine-budget-runs-per-input.md) decides.

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
| Below it by the reorder window or more, and not pending | Skips it and logs a warning once per stream |
| Pending, however far below it | Applies it and removes it from pending |

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
lost this way is visible.

That skip is wrong for exactly one kind of event, a version the saga never applied. There are three ways that happens.
The version failed in the saga, it arrived further behind than the window, or it never reached the part of the saga that
applies events (it could not be routed to an instance, it came before the event that starts the instance, or the instance
was already completed or quarantined). The first can be recorded, because the saga knows it happened. The second and
third cannot be told from a redelivery, so they are the cases this decision leaves open, listed under Consequences.

**A version that failed in the saga is recorded as pending, and a pending version is applied however far behind it is.**
A broker can park a failing event and move on, and an operator can replay it hours later, after the instance applied
more than a window of later versions. Without a record the replay would be skipped as behind the window and its
reaction would never run. The instance therefore keeps, per stream and event type, the versions that failed, ascending.
The record is written on the delivering thread before the exception reaches the subscription, so a broker that parks
the event does so after the saga has written it. Applying the version removes it from pending in the same save that
records it as applied, so a second replay is an ordinary redelivery.

Pending is written whether or not quarantine is on. With quarantine on it goes in the same write as the failure record.
With quarantine off, which is the default, it is a write of its own that carries no failure record, because a failure
record starts the clock quarantine measures from and nothing would clear that clock for an event the broker parked.
A failing event that is offered again and again writes once. Each further attempt reads the instance and finds the
version already pending, which is what the quarantine path costs too.

Every pending version stays pending until a save applies it. Dropping one would acknowledge an event whose reaction
never ran, once the instance moved the window past it. When a failure takes the pending versions of one type past the
window, the saga logs a warning naming the stream, the type and the window, because the instance then grows with every
further failure. An instance that was already past a window that has been lowered since gets no warning.

The failure record keeps its older role as a fallback. A record naming an event still says that the event was never
applied, so the saga treats that event as new however far behind it is, and at or below the floor too. This covers an
instance whose record was written before pending existed. When quarantine is on and that event is offered again and
fails, the saga lists it as pending.

An instance holds one failure record, so a different input that fails replaces it. The saga then moves the replaced
event to `legacyFailed`, a list of stream versions kept per stream. It holds versions without an event type, because a
record written before pending does not say which type failed, and a stream version identifies exactly one event. A
version in that list is new however far behind it is, like a pending one. Applying it removes it from the list, and if
it fails again with its type known it moves to pending. A record written since pending existed is already pending when
it is replaced, so the list only ever holds the record an instance already had when it was upgraded. Nothing is dropped
from it either.

**The horizon keeps a raised window from undoing a skip.** Trimming `applied` forgets the versions at or below
`mark - window`, and a version the saga forgot looks the same as one it never applied. If the window were only read from
the configuration, raising it from 2 to 256 would turn every version just forgotten into a late arrival and run its
reaction again. Each type mark therefore stores the horizon, the highest version it has stopped remembering, and a
version at or below the larger of the horizon and `mark - window` is skipped. Lowering the window never lowers the horizon.

**The stored instance gains one field, and the old one keeps its meaning.** `streamMarks` holds, per stream, a floor
and per type the highest applied version, the applied versions below it and a horizon, and it holds the pending
versions per type, and the legacy failed versions of the stream. A document stored without the pending field or
without `legacyFailed` reads as nothing pending and nothing legacy. The floor is the stream's watermark at the moment
the stream first got marks, and every version at or below it stays skipped whatever its type, which is what the instance did before. An instance stored
by 0.34.0 or by the 0.35.0 build of ADR 150 has no `streamMarks`, and it reads as a floor equal to each stream's
watermark and no marks. `streamWatermarks` is still written with the highest version of the stream across types.

**An instance with marks is stored with a status 0.34.0 can't read.** 0.34.0 knows
nothing of `streamMarks`, and a 0.34.0 save that applies an event replaces the whole document, so such a save during a
rolling upgrade would drop the marks, and the next
0.35.0 node would skip a pending or late version as applied already. `SpringMongoSagaStateStore` therefore stores an
`ACTIVE` instance whose `streamMarks` are not empty with the status `ACTIVE_MARKED`, and reads it back as `ACTIVE`. Its
queries for `ACTIVE`, `findByStatus` and the timer poll, match both strings. 0.34.0 decodes the status with
`SagaStatus.valueOf`, so it throws on such an instance instead of saving it. The subscription, or
the source's delivery failure policy, decides what then happens to the event. A broker source that parks it doesn't
deliver it again, and the upgrade guide says how to publish it again. Its timer poll asks for
`ACTIVE` only, so the instance's timers wait for a 0.35.0 node. It is the mechanism ADR 178 uses for
`QUARANTINED_RECORDING`, and a quarantined instance with a quarantine id stays `QUARANTINED_RECORDING` whatever its
marks hold. The status depends on any marks rather than on the ones a drop would lose. Every event with a stream
version adds marks, so the narrower rule flags nearly the same instances with a more fragile test, and flagging only
the instances with a pending version would leave a late version exposed to a 0.34.0 save. `SagaStatus` gains no
constant, so no `switch` over it breaks.

## Consequences

**Exactly-once holds when the feed keeps a stream's order to within the reorder window per event type, or when the event
failed in the saga.** That is the precondition, and these are the cases that fall outside it:

| Case | What happens | Visible |
|---|---|---|
| The write that records a failed version itself fails (store down, compare-and-set lost every attempt) | The version is not pending. A replay is applied only while the instance is less than a window ahead of it | Error in the log, naming the event |
| An event the saga cannot route to an instance is parked | There is no instance to write on | Error, already logged |
| An event arrives before the event that starts its instance | The saga skips it and the source acknowledges it. This is how it behaved before 0.35.0 | No |
| The instance is completed or quarantined | Inert by design | Quarantine logs |
| A version that did not fail arrives more than the window behind, without a restart replaying it | Skipped as a redelivery | One warning per stream |

The fifth row is the one this design cannot close. A restart from an old checkpoint delivers a long run of behind-window
versions that are all correct skips, and a reordered version looks the same. Throwing on each would block the replay, and
applying each would run every reaction twice. Telling them apart takes a flag from the feed that says the delivery is a
replay, which only the MongoDB subscription models know today. The feeds that reorder deeper than 256 versions are the
catch-up handover (a live event can be delivered ahead of the events buffered during catch-up, which is a separate
defect), a listener that delivers from several threads, a Kafka topic whose partition count was raised after events were
produced, and a RabbitMQ `prefetchCount` or a number of concurrent publishers of one stream that is larger than the
window.

During a rolling upgrade a 0.34.0 node throws on every event for an `ACTIVE` instance that holds stream marks, which a
0.35.0 node adds for every event with a stream version it handles, a failed one included. What then happens to the
event depends on what feeds the saga. An Occurrent subscription, or a Kafka or RabbitMQ source with `REDELIVER`,
delivers it again, so the subscription, partition or queue stalls until a 0.35.0 node gets the event. A source with
`PARK` parks and acknowledges it, and once a Kafka rebalance hands a partition to a 0.34.0 consumer, that consumer
parks every event of such an instance. The parked messages have to be published again after the rollout, and one
`reorderWindow` or more behind the instance is skipped with a warning, so the upgrade guide advises `REDELIVER` on the
0.34.0 nodes for the rollout. A `PushSubscriptionModel` throws the exception back to the code that pushed the event,
and the instance never gets the event unless that code pushes it again. The timers of such an instance wait while a
0.34.0 node holds the timer lease. Saga nodes have to be rolled in one rollout, without a long canary. A query written by hand for `status: "ACTIVE"` misses the instances stored as `ACTIVE_MARKED`.

Going back to 0.34.0 is an explicit step. Once every 0.35.0 node has stopped, a query lists the `ACTIVE_MARKED`
instances whose marks hold a version in `pending` or `legacyFailed`, which 0.34.0 never applies once a later version of
its stream was applied. Then an `updateMany` sets `ACTIVE_MARKED` back to `ACTIVE`, and the 0.34.0 nodes start after it.
0.34.0 ignores the marks, and a 0.34.0 save that applies an event replaces the whole document and drops them. A pending
version below a later applied version is then skipped, and so is a late version below the watermark, as before this
decision. The next 0.35.0 node reads such an instance as a pre-0.35.0 one, so nothing is applied twice. The upgrade
guide gives both commands.

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
