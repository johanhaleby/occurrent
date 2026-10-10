# 144. A catch-up checkpoint keeps the live start it read before the replay

Date: 2026-10-09

## Status

Accepted. Part of [#1217](https://github.com/johanhaleby/occurrent/issues/1217). Applies to the position catch-ups,
`StreamCatchupSubscriptionModel` and `DcbCatchupSubscriptionModel` on the blocking stack and
`ReactorStreamCatchupSubscriptionModel` and `ReactorDcbCatchupSubscriptionModel` on the reactor stack, and to the
MongoDB checkpoint storages and subscription models they run on. Also part of
[#1218](https://github.com/johanhaleby/occurrent/issues/1218), which applies it to the time-based catch-up in the
blocking `StreamCatchupSubscriptionModel`. For that catch-up it reverses the choice in
[ADR 14](0014-reconcile-catchup-events-by-insertion-order-to-avoid-loss-under-clock-skew.md) to read the live start after
the replay.

## Context

A position catch-up replays the event store in global position order and then hands over to the wrapped
subscription model for live delivery. Before the replay it reads the live start, the position in the wrapped model's
change stream that live delivery picks up from, so an event committed during the replay is delivered live. While it
replays, it stores `position:N`, a `GlobalCheckpoint`, every so many events.

The MongoDB event stores reserve an event's position before the transaction that writes it commits
([ADR 84](0084-what-a-position-guarantees.md)). So event A can take position 1, B position
2, and B can commit first. Within one run that does no harm, because A commits after the live start and arrives live.

A restart turned it into a lost event. The replay delivered B and stored `position:2` while A was still uncommitted.
The process stopped and A committed. The next run replayed from position 3 and read a new live start before that
replay, after A's commit. A was below the replay and before the live start, so neither delivered it. A test that holds
A's commit open reproduced this on all four catch-ups.

From reading the code, the same happens wherever a catch-up starts from what it stored rather than from the position
it was given. That is a new process, a `CompetingConsumerSubscriptionModel` node taking over the lease, a blocking
`stop()` and `start()` during the replay, and a reactor pause and resume under `ReactorDurableSubscriptionModel`. I
tested only the new process.

A stored position cannot say which events below it are still uncommitted. MongoDB exposes no boundary for
transactions in flight, which is why [ADR 62](0062-pluggable-projection-event-source.md)
and [ADR 122](0122-an-applied-position-is-not-a-completed-prefix.md) rejected a contiguous
watermark. The live start read before the first replay attempt is the only point that is known to be before A's
commit.

The time-based catch-up in `StreamCatchupSubscriptionModel` replays in event time order and stores a time,
`TimeBasedCheckpoint`, while it replays. The application that writes an event sets its time, so A can have an earlier
time than B and commit after it. That catch-up read its live start after its replay. A test that holds A's commit open
showed A lost after a restart that resumed from B's time, and also within one run, with no restart, when A committed
while the replay ran. The replay had read past A's time, and the live start came after A's commit. The reconciliation
read after the replay, the newest events in insertion order, missed it too.

## Decision

The checkpoint a position catch-up stores during its replay holds the live start and the replay end next to the
position.

`GlobalCheckpoint` gains `of(position, liveFrom, replayOrigin, replayTo)`, `liveFrom()`, `replayOrigin()`,
`replayTo()` and `parse(Checkpoint)`. `liveFrom` is the live start the catch-up read before its replay, `replayOrigin`
is the position the first attempt at the replay started from, and `replayTo` is the head of the global sequence that
attempt read right after the live start. Its string form is `position:N;origin:S;replayTo:H;liveFrom:<live start>`,
with the live start's own string form last and unescaped. A `GlobalCheckpoint` without a live start is still
`position:N`.

The replay end splits the events by when they committed. Every event that committed before the live start had its
position reserved before the head was read, so its position is at or below the replay end. Every event above the
replay end committed after the live start, so live delivery from that live start delivers it.

A resume from a stored checkpoint with a live start replays from the stored position up to the stored replay end and
goes live from the stored live start. It does not reconcile past the replay end, because live delivery already
delivers everything above it. When the stored checkpoint has no live start, the catch-up reads one before the replay,
as before.

The MongoDB storages write the position as before, `checkpoint: "position:N"`, and the live start, the origin and the
replay end in three fields of their own, `catchupLiveFrom`, `catchupReplayOrigin` and `catchupReplayTo`.
`catchupLiveFrom` is the document the live start would be stored as on its own, with its `resumeToken` or
`operationTime` field, minus the `_id`. The fields must not be at the top level, because 0.33.0 reads `resumeToken`
and `operationTime` there before `checkpoint`, and would take the live start for the subscription's own position. A
later write replaces the whole document, so a plain position or a live subscription's checkpoint removes the three
fields. Storages that keep strings, such as `SpringRedisCheckpointStorage`, store the string form, and
`GlobalCheckpoint.parse` reads it back.

A live start can age out of the change stream history while the subscription is down. `CheckpointAwareSubscriptionModel`
gains `canResumeFrom(Checkpoint)`, `boolean` on the blocking stack and `Mono<Boolean>` on the reactor stack. It
answers `true` by default. The three MongoDB subscription models open a change stream at the checkpoint with a batch
size of 1, close it, and answer `false` when MongoDB refuses with error 286, `ChangeStreamHistoryLost`. A test against
a MongoDB container whose oplog had rolled over confirmed that code. Any other error fails the call, and the catch-up
fails with it, without a retry, whether it asked at a resume or once a replay was done. When the answer is `false`,
the catch-up logs a warning, replays from `replayOrigin` and reads a new live start before that replay.

A live start can also leave the history while the replay runs, on a first run as much as on a resume. So every
position catch-up asks `canResumeFrom` again once its replay is done, right before the handover. When the answer is
`false` then, it logs a warning, reads a new live start, replays again from `replayOrigin` the way a first run
does, and asks again. It replays again at most 3 times. When the wrapped model still answers `false` after the
fourth replay, the catch-up fails with an `IllegalStateException` that names the subscription and the number of
replays, and hands nothing over. On the blocking stack `waitUntilStarted()` throws it, as it throws a failed read of
the live start, and the model logs it at `ERROR`, as it now logs every replay that fails. That log is there for a
caller that never waits, such as the Spring Boot starter by default for a subscription that replays history. On the
reactor stack the subscription fails with it, and a named catch-up logs it at `ERROR` too. So a catch-up goes live
only from a live start the wrapped model answered `true` for after its last replay, or fails after 4 replays. It
never hands over a live start the wrapped model answered `false` for, and never replays without end. A blocking
replay that is launched again after a failed start runs the same check, since it runs the same replay. A replay
launched again after a stop runs it too, on either stack, and counts its replays from 0.
[ADR 28](0028-dcb-catch-up-captures-resume-token-before-replay.md) and [ADR 38](0038-reactive-dcb-catch-up.md)
expected such a handover to fail loudly. With `restartSubscriptionsOnChangeStreamHistoryLost` true,
`SpringMongoSubscriptionModel` and `ReactorMongoSubscriptionModel` went live from the present instead and skipped
the events in between.

A stored `position:N` without a live start, written by 0.33.0 or by a catch-up interrupted before this change,
resumes as in 0.33.0, with a live start read after the restart, and the catch-up logs a warning that names the
subscription and the stored value. A live start stored without a replay end is read back as a plain `position:N` and
resumes the same way. No released version writes one.

The time-based catch-up reads its live start before its replay too, on every run that goes live afterwards, and
stores it with the time.
`CatchupTimeCheckpoint` in `occurrent-subscription-core` holds the time, the live start, and the time the first
attempt at the replay started from, `replayOrigin`. It keeps both times as RFC 3339 strings, since core doesn't
depend on the module that has `TimeBasedCheckpoint`. Its string form is `<time>;origin:<time>;liveFrom:<live start>`.
The MongoDB storages write the time in `checkpoint` as before, the live start in `catchupLiveFrom`, and the origin as
a string in `catchupReplayOrigin`. A position checkpoint has a number there, a time checkpoint a string.

For the time catch-up this reverses the choice in
[ADR 14](0014-reconcile-catchup-events-by-insertion-order-to-avoid-loss-under-clock-skew.md) to read the live start after
the replay. ADR 14 kept it there, so that a long replay could not outlast it in the oplog, and counted on the read of
the newest events in insertion order after the replay to deliver what committed during it. The test without a restart
lost A anyway. The check below handles the oplog risk ADR 14 rejected this for.
[ADR 28](0028-dcb-catch-up-captures-resume-token-before-replay.md) made the same change for the DCB catch-up.

A time checkpoint has no replay end. A position splits the events by when they committed because a position is
reserved before the commit. A time is set by the writer, so an event that committed before the live start can have any
time, and no time read after the live start bounds them. A resume replays from the stored time to the present and goes
live from the stored live start.

The check that the live start is still in the history, the replay again from the origin, and the limit of 3 more
replays are the same code for both, `replayUntilLiveStartHolds` in `AbstractCatchupSubscriptionModel`. A stored time
without a live start, written by 0.33.0, resumes with a live start read at the resume, and the catch-up logs a warning
that names the subscription and the stored value.

At the handover, a catch-up replaces the stored checkpoint with the live start when that checkpoint is a position or
a time, with or without a live start, whichever kind of catch-up stored it. In 0.33.0 each handover replaced only its
own kind and passed the other to the wrapped model as where to start, and the MongoDB models opened at the present.

## Alternatives considered

- **A contiguous or committed watermark.** Rejected in ADR 62 and ADR 122, since MongoDB cannot say which reserved
  positions are still uncommitted.
- **Store nothing during the replay.** Every restart would replay from the original start. It removes the loss but
  makes `PersistCheckpointDuringCatchupPhase` meaningless, and a long rebuild restarted near its end does all of it
  again.
- **The string form on MongoDB too.** One format everywhere, but a 0.33.0 node or a rollback would read
  `position:N;origin:...` and fail with `NumberFormatException` in `GlobalCheckpoint.positionOf`. With the nested
  fields a 0.33.0 node reads `position:N` and behaves exactly as 0.33.0.
- **A separate checkpoint type.** `isGlobalCheckpoint` and `positionOf` route every catch-up and the reactor
  dispatcher, so a new type would have had to be taught to each of them. Extending `GlobalCheckpoint` is additive.
- **Replay a legacy `position:N` from 0.** It is loss-free only when the original start was 0, ignores an explicit
  `GlobalCheckpoint.of(k)` start, and can deliver the whole store again.
- **Fail on a legacy `position:N`.** It would block a rolling upgrade on a race that needs a crash in the middle of a
  replay and a late commit below the stored position.
- **Rely on the change stream's own handling of lost history for an aged-out live start.** With
  `restartSubscriptionsOnChangeStreamHistoryLost` true, the Spring Boot starter's default, the subscription restarts
  from the present and skips everything committed during the resumed replay, which is worse than 0.33.0.
- **Store the live start alone and replay to the current head on a resume.** Every event written while the
  subscription was down commits after the stored live start, so the replay and live delivery would both deliver it.
- **Replay again without a limit.** A catch-up whose oplog is shorter than every replay would replay forever, at
  full CPU and with no error, so nothing tells you to size the oplog.
- **Fail on the first live start lost during a replay.** One burst of writes during a replay can push the live start
  out of the oplog, and the next replay doesn't necessarily meet the same burst. 3 more replays give that a chance,
  and the limit is a private constant rather than configuration, since a larger oplog is the fix.
- **Read the time catch-up's live start before the replay only when it stores checkpoints during the replay.** A
  time replay that stores nothing would then not depend on the oplog. But a test with no restart lost A too, so the live
  start has to come before every time replay.
- **One checkpoint type for position and time.** Extending `GlobalCheckpoint` with a time would have taught every
  caller of `isGlobalCheckpoint` and `positionOf` about times. A separate `CatchupTimeCheckpoint` that reuses the
  MongoDB field names keeps the position code as it is.
- **A replay end for the time catch-up.** A time read after the live start doesn't bound the events that committed
  before it, so stopping the resumed replay there would lose them.
- **Fall back to a fresh live start at the stored position.** It delivers fewer events a second time than a replay
  from the origin, but an event committed late below the stored position is lost, as in 0.33.0.

## Consequences

A resume delivers again what the earlier attempt delivered after the last checkpoint it stored, as any resume from a
checkpoint does. Apart from those, live delivery after a resume starts from the stored live start, so any event
committed after the live start and delivered before the stored checkpoint can come again. That includes an event the
earlier attempt's reconciliation read delivered, whose position is above the replay end. An event that committed
after the live start with a position at or below the replay end can come again too, since the resumed replay delivers
it and so does live delivery. Such an event cannot be told apart from the late commit this decision is about, since
both committed after the live start. A catch-up that never restarts also delivers that last kind twice, see
[ADR 135](0135-the-reactive-handover-dedup-is-fed-only-by-the-reconciliation-read.md). Durable subscriptions deliver
at least once already, and a handler that tolerates a repeat needs no change.

A live start the oplog no longer has, at a resume or once the replay is done, makes the catch-up replay from the
origin again, which delivers everything between the origin and where the earlier replay had got to a second time. A
catch-up fails instead of replaying a fifth time once 4 replays in a row lost the live start, and the last 3 of those
replays each delivered the history from the origin. So size the oplog for the longest rebuild.

The check and the handover are two calls. A live start that leaves the history between them is still handed over,
and what happens then is up to the wrapped model's handling of lost history.

When a `canResumeFrom` of your own answers `true` for a live start it can no longer resume from, the catch-up goes
live from it anyway, and what happens then is up to the wrapped model's handling of lost history. A
`globalCheckpoint()` whose answer is meaningless in another process makes the stored live start meaningless too, and
the default `canResumeFrom` does not detect that.

A blocking catch-up whose `StartAt` answers `null` for the wrapped model reads no live start before its replay, so
it still stores a plain `position:N`, a resume logs the warning, and it behaves as in 0.33.0. A time catch-up in
that case reads no live start either and stores a plain time.

`applyResolvedStartPosition` in `MongoCommons` treats every `GlobalCheckpoint` as a position it does not recognize and
opens at the present, as 0.33.0 did for `position:N`. Without that, the `operationTime` inside a stored live start would
match and the whole string would fail to parse. [#1219](https://github.com/johanhaleby/occurrent/issues/1219) is about
the case where that fallback skips events.

A rollback to 0.33.0 reads the MongoDB document as `position:N` and resumes as 0.33.0 does. A string storage holding
the composite form makes 0.33.0 throw `NumberFormatException` when the subscription starts, so let every catch-up
reach live delivery first, or rewrite the value to `position:N`.

Without a replay end, a time catch-up delivers more events twice than a position catch-up. A resume can deliver every
matching event written after the stored live start twice, once from the resumed replay and once live, which includes
everything written while the subscription was stopped. Within one run, an event committed during the replay that the
replay also reads arrives live as well, since the bulk replay doesn't fill the bounded cache that live delivery
checks.

A time replay depends on the oplog now, as a position replay does. In 0.33.0 a time replay of any length went live,
because it read its live start only once it was done. Now a time replay that outlasts the oplog 4 times in a row fails
with the `IllegalStateException`.

A rollback to 0.33.0 reads the MongoDB document of a time checkpoint as the plain time. A string storage holding the
time form makes 0.33.0 hand the value to the MongoDB subscription model as a change-stream position, because it is not
a time. The model finds `operationTime` or `resumeToken` in it, parses the whole string as JSON, and `subscribe(..)`
throws a `JsonParseException`. Let every catch-up reach live delivery first, or rewrite the value to the time before
the first `;`.
