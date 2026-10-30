# 155. A catch-up reads its live start from the event store and refuses to resume where it could skip an event

Date: 2026-10-24

## Status

Accepted. Fixes [#1297](https://github.com/johanhaleby/occurrent/issues/1297). Part of
[#1230](https://github.com/johanhaleby/occurrent/issues/1230), since the DCB catch-ups resume from a stored position the
same way the stream catch-ups do. Applies to the three MongoDB event stores (`MongoEventStore`, `SpringMongoEventStore`
and `ReactorMongoEventStore`), `InMemoryEventStore`, the catch-ups on both stacks (`StreamCatchupSubscriptionModel`,
`DcbCatchupSubscriptionModel` and `CatchupSubscriptionModel` on the blocking stack,
`ReactorStreamCatchupSubscriptionModel`, `ReactorDcbCatchupSubscriptionModel` and `ReactorCatchupSubscriptionModel` on
the reactor stack), the MongoDB subscription models they run on (`NativeMongoSubscriptionModel`,
`SpringMongoSubscriptionModel` and `ReactorMongoSubscriptionModel`), `ReactorDurableSubscriptionModel` and both Spring
Boot starters.

Amends [ADR 144](0144-a-catch-up-checkpoint-keeps-the-live-start-it-read-before-the-replay.md), which resumes from a
position stored without a live start, replays from the origin when the live start is lost, and reads no live start in
no-live mode. Amends [ADR 146](0146-a-catch-up-takes-over-a-live-start-the-wrapped-model-loses-as-it-opens.md), under
which the wrapped model handles a history loss after its first open. Extends
[ADR 148](0148-dcb-snapshots-resume-from-marker-versions-not-positions.md) with the store-wide catch-up its last section
left open.

## Context

A MongoDB event store reserves an event's position before the append transaction commits, so positions are not commit
order ([ADR 84](0084-what-a-position-guarantees.md)). An append can reserve position 5, stall, and commit after an
append that reserved position 6 has committed and been read. A catch-up that resumes from position 6 replays everything
above 6 and never sees the event at 5.

The live start is the change stream position a catch-up goes live from when its replay ends. ADR 144 reads it before the
replay and stores it with the replay position, so a resume goes live from there and the change stream delivers the late
event.

[#1297](https://github.com/johanhaleby/occurrent/issues/1297) lists three ways a catch-up still resumes from a position
and nothing else:

- a bare position the application stored and passes back, for example through `startAtGlobalPosition`
- a checkpoint written by 0.34 as a plain `position:N`, with no live start
- a plain `position:N` that a no-live catch-up, one that replays and never goes live, writes in the middle of its replay

ADR 144 resumes from a stored `position:N` with a warning.

ADR 144's live start also comes from the subscription model, not from the store the replay reads, and nothing ties the
two together. A replay read sent to a secondary can miss an event that the live start is already past. ADR 148 left the
store-wide fix open, under two constraints. The DCB marker-version path doesn't change. And a fix that reads the replay
at majority reads the head, the highest position the replay reads to, and the live start in the same causal session,
with `afterClusterTime` at or above the head read.

## Decision

A catch-up delivers every matching event that commits after its start point. On a resume, that's the start point of the
run that stored the checkpoint. When a catch-up can't show this for a start, it refuses the start. It reads its live
start from the event store, in the same session as the replay reads, and the proof rests on the assumption A15 stated
below. Every refusal throws `CatchupCannotResumeException`, which names the subscription and the fix, and it writes
nothing before it throws.

### The event store reads the live start and every replay read in one session

The catch-up reads through a new internal SPI, `CatchupReads` in `org.occurrent.eventstore.api.blocking.internal`. It
has one method, `openCatchupReadScope(@Nullable String notBefore)`, which returns a `CatchupReadScope`. A scope has
`liveStart()`, `currentPosition()`, `readInPositionOrder(Filter, PositionRange)`, `query(Filter, int, int, SortBy)` and
`close()`. `DcbCatchupReadScope` adds a DCB `read`, which takes `between()` ranges only and never marker versions. The
reactor stack has the same types with `Mono` and `Flux`.

A MongoDB store opens a scope in four steps:

1. It opens its own `ClientSession` with `causallyConsistent(true)`, never the caller's session, since a caller's
   transaction may abort.
2. On a resume from a stored live start, passed as `notBefore`, it calls `advanceOperationTime` with that start first.
3. It reads the document that holds the store's position counter with a majority `find` on the primary. T0, the cluster
   time the scope starts from, is `session.getOperationTime()` after that read. The live start is T0 plus one increment.
4. Every replay read runs in that session at read concern majority, applied after the user's `queryOptions`, with the
   user's read preference kept. The driver sends `afterClusterTime` at or above T0 on each one.

An event that committed at or before T0 is in every majority snapshot from T0 on, and every replay read waits for one.
Its position counter update committed no later than the event, so the head read covers its position and the replay
returns it. An event that committed after T0 comes from the change stream, which starts at T0 plus one increment. A T0
older than it has to be only costs duplicates, which `BoundedIdCache` removes. This rests on A15, which isn't verified.

> A15. MongoDB's majority commit point never passes an oplog hole, single-node replica set included.

A majority `find` needs only the `find` action, so the catch-up runs on Atlas M0 and Flex and on roles that refuse
`hostInfo`. Since every read is at majority in one causal session, a read on a secondary or after a failover returns the
same events.

`InMemoryEventStore` returns `CatchupReads.unscoped(EventStoreQueries, PositionOrderedReader)`. It assigns a position
and makes the event visible in one critical section, so positions are commit order there. `unscoped` is internal, and
using it over a MongoDB store brings back the defect in [#1230](https://github.com/johanhaleby/occurrent/issues/1230).

The catch-up refuses at construction any `EventStoreQueries` that isn't one of these stores, a tracing, metrics or
tenancy decorator included. The message names `CatchupReads` and says to hand the catch-up the store itself and decorate
around the subscription instead. In Spring Boot, a user's own `EventStoreQueries` bean that isn't a MongoDB store fails
startup on both stacks.

### A checkpoint names its origin, its head and its live start

A checkpoint a catch-up writes during a replay in no-live mode, or during a replay whose origin is above the beginning,
is one of two marked forms, a `StringBasedCheckpoint` built by `CatchupCheckpoints.format(...)` in the internal package
`org.occurrent.subscription.internal`:

```
position:<N>;v:2;origin:<O>;replayTo:<H>;liveFrom:<L>      position replay
position:time;v:2;at:<t>;origin:<o>;liveFrom:<L>            time replay
```

`N` is the last position delivered, `O` or `o` where the run started, `H` the head the run replays to, `t` the time
watermark, and `L` the live start. A live-mode replay whose origin is the beginning keeps ADR 144's form, which 0.34
reads. 0.34 resumes that form without a skip, since it keeps a live start the change stream still has and replays from
the beginning when it doesn't.

During a rolling upgrade, 0.34 nodes and 0.35 nodes share the checkpoint storage. Each form a 0.35 node writes is either
one 0.34 resumes from safely or one every 0.34 reader throws on. 0.34 throws on both marked forms in
`GlobalCheckpoint.parse`. The time form starts with `position:` on purpose. A time marker outside that prefix wouldn't
throw on 0.34, because the time-based catch-up's parse swallows the `IllegalArgumentException`.
`CatchupCheckpoints.isMarkedTime` is asked before any prefix test, and the DCB and reactor catch-ups refuse a marked
time form.

The stored time `t` promises that every matching event committed at or before `L` with a time below `t` was delivered:

| Replay | The stored time |
| --- | --- |
| Bulk read with a sort that leads with time ascending, the default | the time of the last event delivered |
| Bulk read with any other sort | the time the attempt replays from, until the bulk read completes |
| The reconcile windows of [ADR 14](0014-reconcile-catchup-events-by-insertion-order-to-avoid-loss-under-clock-skew.md) | the time of the last event delivered |
| An event with no time | unchanged |

Under a non-default sort, a crash during the bulk read replays the attempt from its start.

### No-live mode reads a live start and hands over at it

A no-live catch-up opens a scope like a live one and writes the marked forms. When the replay ends, it hands the
subscription to the live model with `StartAt.dynamic`. A `SubscriptionModelWrapper` gets the caller's own answer, and
null still means persist nothing. The innermost model gets the live start. T0 is never saved.

A no-live resume from a stored live start that the change stream still has goes live from that live start, not from the
new scope's. The scope opens with `notBefore` set to the stored start.

A takeover is the replay a catch-up runs again when the wrapped model loses the live start as it opens (ADR 146). In
no-live mode a takeover keeps its checkpoint in memory instead of storing it.

### The starts a catch-up refuses

| Start | What happens |
| --- | --- |
| A bare position above 0 from any source: `startAtGlobalPosition`, `startAtDcbPosition`, `DcbStartAt.afterPosition(N)`, `StartAt.checkpoint(GlobalCheckpoint.of(N))`, a stored `position:N`, or a 0.34 checkpoint with no `replayTo` | refused |
| Position 0 | replays from the beginning |
| A time the application passes | accepted, and the live start is read first |
| A stored bare time | refused |
| A lost live start, origin at the beginning | replays from the beginning with a new live start |
| A lost live start, origin above the beginning | refused |
| A 0.34 checkpoint with a live start and origin above 0 | resumes when the live start is still in the change stream, refused when it isn't |
| A checkpoint the catch-up classifies as live | asks `canResumeFrom` first, refused when the answer is false |
| A position checkpoint with no filter, on a catch-up that runs both a stream and a DCB model | `IllegalArgumentException` naming `StreamSubscriptionFilter`, `DcbSubscriptionFilter` and `AgnosticSubscriptionFilter` |

There's one exception to writing nothing before a refusal. A takeover whose live start is lost, with origin above the
beginning, first stores a marked checkpoint with that origin, so that a restart refuses as well instead of resuming from
the lost live start.

A specific-time catch-up whose replay outlasts the history now fails at once. It no longer replays again up to three
times.

`startAtGlobalPosition`, `startAtDcbPosition` and `DcbStartAt.afterPosition(long)` are deprecated with `forRemoval =
true`. `GlobalCheckpoint.of(long)` gets javadoc only and is not deprecated.

How a refusal ends a replay depends on how the replay started. A replay started by a subscribe ends, reports the error
at `waitUntilStarted`, and logs it once. A replay started by a resume or a takeover stops there, with one error log and
no backoff. A later `resumeSubscription`, `start(true)` or lease acquisition under competing consumers runs it again and
fails again.

### The oplog window must exceed the longest catch-up replay

The oplog window is how far back MongoDB's oplog still has entries, and so how far back a change stream can resume. A
catch-up goes live from a live start it read before the replay, so in both modes the oplog window must exceed the
longest catch-up replay. A replay that outlasts it loses its live start, and then the table above decides. The terminal
message names the longest replay, measured from the T0 read to the `canResumeFrom` check, and says the change stream
history must reach back at least that far.

### A history loss in the live phase stops the subscription by default

A catch-up owns every subscription made through it, live-only ones included, from just before the wrapped model opens
until the subscription is cancelled. When MongoDB no longer has the history to resume a subscription, the catch-up
decides:

1. A loss of a live start the catch-up already took over lets the takeover go on.
2. A loss of the live start the catch-up handed over, as the wrapped model first opens, starts the takeover of ADR 146.
3. A loss on a subscription that replayed or continued a replay from the beginning in this run and opted in with
   `REPLAY_FROM_BEGINNING` replays from the beginning, with the replay count starting from 1.
4. Every other loss on an owned subscription pauses it with `CatchupCannotResumeException`.
5. A subscription no catch-up owns is left to the subscription model, as before.

A paused subscription logs one error, and `resumeSubscription` throws `CatchupCannotResumeException` until it's
cancelled. On the blocking stack `start(true)` throws it too, and on the reactor stack it pauses the subscription again
with one more error. An unnamed `Flux` fails with it. `restart-on-change-stream-history-lost` no longer affects a
subscription a catch-up runs, which in Spring Boot is every subscription by default, `NOW` and `DEFAULT` included.

Telling row 2 from row 4 needs to know whether the wrapped model has opened before:

- Both `HistoryLossListener` interfaces get a default `handleHistoryLoss(String subscriptionId, Checkpoint lostFrom,
  boolean openedBefore)`. The two-argument form answers true.
- `HistoryLostException` gets a constructor `(Checkpoint lostFrom, Throwable cause, boolean openedBefore)` and
  `openedBefore()`.
- The reactor `HistoryLossReportingSubscriptions` gets an abstract `subscribeFailingOnHistoryLoss(@Nullable
  SubscriptionFilter filter, StartAt startAt)`, which breaks its implementers. The released
  `subscribeFailingIfStartLost` is unchanged.

The opt-in for row 3:

- `org.occurrent.annotation.HistoryLossBehavior`, with `DEFAULT` and `REPLAY_FROM_BEGINNING`, is the attribute
  `historyLossBehavior` on `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@Projection`, `@Saga` and
  `@Snapshot`. It requires `startAt = BEGINNING`. Spring Boot collects the answers in a `HistoryLossRecoveries` bean.
- `org.occurrent.subscription.HistoryLossRecovery`, with `STOP` and `REPLAY_FROM_BEGINNING`, is set programmatically
  with `CatchupSubscriptionModelConfig.historyLossRecovery(Function<String, HistoryLossRecovery>)`, the reactor
  constructor parameter `historyLossRecovery`, or the unnamed `subscribe(filter, startAt, historyLossRecovery)`.

Stopping is the default because a replay runs the handler again for every event, including any mail it sends. A
subscription that resumed from a stored checkpoint after a restart stops even when it opted in, since its origin isn't
known. A recovery stops with `CatchupCannotResumeException` once it loses its own live start three times before the
subscription is live again. Under competing consumers, a lease that moves to another node turns a recovery into a stop.
An unnamed `Flux` taken through `ReactorDurableSubscriptionModel` or the reactor `DcbSubscriptions` unnamed forms can't
opt in.

A recovery replays from the beginning, and not from some bounded time before the loss, because nothing bounds how long
an append takes from reserving its position to committing:

- The position is reserved outside the transaction, so the transaction lifetime bounds start to commit, not reservation
  to commit. Retries can take 120 seconds for each of 15 attempts, and a stalled thread has no bound at all.
- `transactionLifetimeLimitSeconds` is a server setting the store can't control.

ADR 148 dropped its design 4 for the same reason.

Keeping the origin across a restart would need a new live checkpoint form, and I'm not proposing one.

### The application can store the resume point itself

Every replayed event on the blocking stack is delivered as a `CheckpointAwareCloudEvent`, as on the reactor stack
already, so the event's class and its `equals` change. The checkpoint is read with `EventMetadata.getCheckpoint()`,
which returns `Optional<String>` and keeps it outside the extension map, with `DcbEventMetadata.checkpoint()`, or with
`CheckpointAwareCloudEvent.getCheckpoint(event)`. The application stores the string with its own state, in the same
transaction, and passes it back:

```java
StartAt.checkpoint(new StringBasedCheckpoint(stored))
DcbStartAt.checkpoint(stored)
```

In Spring Boot, a `SubscriptionResumePoints` bean is asked for every subscription's start position, and a non-null
answer wins over Boot's own checkpoint storage:

```java
@FunctionalInterface
interface SubscriptionResumePoints {
    @Nullable String resumePointFor(String subscriptionId);
}
```

Under competing consumers, read the stored string each time the lease is acquired, with `StartAt.dynamic(ctx ->
StartAt.checkpoint(new StringBasedCheckpoint(readFromMyStore())))`. A marked string given to a bare MongoDB subscription
model opens at the model's default with no error. A live checkpoint resumes only while the oplog still has it.

## Alternatives considered

- **Reserve positions inside the transaction**, design 3 in ADR 148. Every append conflicts on one counter, the
  contention [ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) removed.
- **A complete-through position with an age rule**, designs 4 to 6 in ADR 148. An age rule is unsafe for the reasons
  above. A gap-free prefix is sound, but it stalls for good at the first abandoned block, since gaps are permanent.
- **A commit-ordered finalizer.** It needs a leader-elected process that must run, a write per event, and every event
  shows up again in the change stream (ADR 148).
- **A ledger of committed blocks.** It needs a write on every failure exit that 0.34 writers don't make, so it isn't
  sound during a rolling upgrade.
- **A commit position written on each event.** Nothing but the append may write to the event collection.
- **A late-commit handle with a drain at the end of a no-live run.** The no-live run handed over live at the present, so
  the events that committed after its last read were delivered by nothing. Handing over at T0 fixes that, and that fixed
  version is this decision.
- **An optional log of late events for replays longer than the oplog window.** Failing loudly with the sizing rule
  replaces it.
- **T0 from `globalCheckpoint()` or `hostInfo`.** Restricted roles and Atlas M0 and Flex refuse it.
- **A `GlobalCheckpoint` instance for the marked form.** The MongoDB checkpoint storage writes `catchup*` fields for it,
  which 0.34 resumes from with the live start dropped.
- **A new `Checkpoint` type for the marked form.** It reads back as a `StringBasedCheckpoint`, so a type check gives a
  different answer before and after a round trip.
- **Refuse every 0.34 time checkpoint.** That breaks every user on the default sort, whose time checkpoints are safe.
- **Route a position start with no filter to the agnostic model.** It changes what an existing DCB caller gets, with no
  error.
- **The opt-in as a boolean attribute, inside `StartAt`, or as a per-call option.** A per-call option would have to pass
  through competing consumers and both durable models.
- **Replay from the beginning by default for every subscription whose origin is the beginning.** It repeats side effects
  nobody asked to repeat.
- **The checkpoint in a CloudEvent extension.** It leaks into every event the application forwards or appends again.
- **A helper at each `EventMetadata.from` call site.** Every call site in user code would miss it.
- **A default method for `subscribeFailingOnHistoryLoss`.** It moves the signal from compile time to run time and still
  breaks implementers, and a default that throws `UnsupportedOperationException` claims a capability the model doesn't
  have.
- **A default that falls back to `subscribeFailingIfStartLost` with a WARN.** It skips events with a log line.
- **Widening `subscribeFailingIfStartLost`.** It promises to leave a later loss to `subscribe`, and that's released
  behaviour.

## Consequences

- A catch-up over MongoDB doesn't skip an event that commits below a position it already replayed past, in either mode,
  as long as A15 is true and the oplog window exceeds the longest replay. A15 isn't verified and can't be tested
  deterministically.
- Two more assumptions aren't verified. A transaction over 16 MB may label its early events with a staging time before
  its commit time. A change stream accepts `startAtOperationTime` one increment above the node's last oplog entry, which
  the catch-up already relies on.
- Sharded clusters aren't checked.
- A server session idles out after 30 minutes. A gap of more than 30 minutes between two replay reads fails the next
  read loudly.
- An application that stores bare positions, or resumes from a 0.34 `position:N`, now fails loudly instead of resuming
  with a warning. The fix is to store the opaque checkpoint, or to replay from the beginning.
- A tracing, metrics or tenancy decorator around `EventStoreQueries` can no longer be handed to a catch-up. It goes
  around the subscription instead.
- While 0.34 and 0.35 nodes run side by side, a 0.34 node can still skip a late event on every checkpoint form it reads.
  Closing that would stop every catch-up during the upgrade. Rolling back to 0.34 needs the marked checkpoints deleted
  first.
- A 0.34 time checkpoint written under a sort that doesn't lead with time ascending can already be past an event its run
  never delivered, and nothing in the string says which sort wrote it. 0.35 resumes it as stored.
- The reactor push model isn't checked for the defect.
- A subscription no catch-up owns still restarts from now on a history loss, for example in Spring Boot with no catch-up
  configured, or on a programmatic subscription model with restart set to true. That's left to a separate issue.
- A programmatic catch-up over a model that doesn't restart ends with `CatchupCannotResumeException` instead of the raw
  `ChangeStreamHistoryLost` error, code 286.
- A subscription model outside Occurrent that calls the two-argument `handleHistoryLoss` gets a stop instead of the
  first-open takeover.
- When `ReactorMongoSubscriptionModel` reads ahead, and on an unnamed `Flux`, a transient error at the first attempt
  followed by `ChangeStreamHistoryLost` stops the subscription, or recovers it when opted in, where ADR 146 replayed.
- The catch-up no longer needs `hostInfo`.
- A recovery with `REPLAY_FROM_BEGINNING` costs a full replay.
- A no-live run hands over at T0 instead of at the present, so it delivers more duplicates.
