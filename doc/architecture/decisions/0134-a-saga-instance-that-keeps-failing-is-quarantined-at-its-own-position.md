# 134. A saga instance that keeps failing is quarantined at its own position

Date: 2026-08-22

## Status

Accepted at the design gate for #818, which this decides rather than closes, because the implementation is separate
work. 0133 is the current maximum, re-audited across every remote branch at write time per the max-plus-one rule.

The three questions this decision could not settle on its own were ruled at that gate and are recorded in
**Rulings at the design gate** near the end of this file. One of them, the non-replayable source, ships as a
narrowing rather than a closure, and [#918](https://github.com/johanhaleby/occurrent/issues/918) is its recorded
path.

## Context

`SagaExecution.onCloudEvent` runs the saga and catches nothing. `SagaExecutionSupport.process` calls
`saga.evolve(previousState, input)` and then `saga.react(nextState, input)` with nothing catching either, so a
throwing `evolve`, a throwing `react` or a throwing command dispatcher reaches the subscription model unchanged.

What the subscription model then does is worse than the plain redelivery the issue describes, on the models most
sagas actually run on. `NativeMongoSubscriptionModel`, `SpringMongoSubscriptionModel` and `InMemorySubscription` are
the only blocking models that wrap the handler in `executeWithRetry`. The default on both MongoDB models backs off
exponentially from 100 ms to 2 seconds and never stops, so the failing event is retried forever on the
subscription's own cursor thread. `DurableSubscriptionModel`, `CompetingConsumerSubscriptionModel` and the catchup
models add no retrying of their own, but `DurableSubscriptionModel` can only wrap a `CheckpointAwareSubscriptionModel`
and the only two implementations of that are the MongoDB models, so behind it the retrying happens anyway. In `NativeMongoSubscriptionModel` the `currentStartAt.set(...)` call sits after the handler call in
the same lambda, so the in-memory position does not move either, and `DurableSubscriptionModel` writes its checkpoint
only after the action returns, so the durable checkpoint does not move. One instance whose input will never succeed
stops the whole subscription, and a saga has exactly one subscription, so it stops every other instance of that saga.

This is the first of AGENTS.md's design intentions, and it is written there as a constraint rather than a preference.
No saga, projection or subscription may be blocked by another one being faulty, and no design may lose events. The
shipped event path breaks the first half of that. A fix must not buy it back by breaking the second half.

The timer path is the model the issue points at, and it is the right one to start from. `SagaExecution.pollTimers`
catches a failing timeout per instance, logs it, and the timer stays due for the next poll, so an instance that
cannot fire its timer costs one stuck instance instead of the poller.

### Why the event path cannot copy it

The symmetry is not free, and the reason is what the rest of this decision is built on.

A timer is durable input the saga already owns. It sits in `SagaEnvelope.timers()`, addressed by saga id, so leaving
it due discards nothing. The next poll finds it in the same place.

An event is not the saga's to own. It arrives on one ordered channel shared by every instance of the saga, and the
only handle on that channel is the subscription's position. Skipping an event moves a position that belongs to all of
those instances at once, and once it has moved the saga has no way to ask for that event a second time.

So "catch it and move on" is a complete answer on the timer path and only half an answer on the event path. The
missing half is a durable record of what was skipped and where the instance stopped.

### An input that will never succeed and a transient failure are the same exception

Nothing distinguishes them. ADR 128's refusal for a step that was renamed or removed throws `IllegalStateException`.
A dispatcher that cannot reach MongoDB during a thirty second outage also throws. Both are a `RuntimeException` out
of code Occurrent does not own, so classifying by exception type would mean enumerating every exception a user's
`evolve`, a user's `react` and an arbitrary store can raise. Occurrent cannot do that, and guessing wrong in the
transient direction is the expensive direction, because it quarantines every instance of the saga during any outage.

The design therefore does not classify. It measures how long the failure has lasted.

## Decision

### 1. A quarantine suspends one instance. It does not remove an event

The failing event is not copied into the saga's store and it is not moved to a holding area. It stays where it
already is durably, in the event store or on the feed. What the saga records is the position it stopped that instance
at, so the instance can be resumed from there later.

That distinction is what keeps the no-loss half of the isolation rule intact. Removing an event from a channel and
keeping it somewhere else is a second copy with its own retention and its own failure modes. Recording where an
instance stopped is a statement about the instance, and the events behind it stay under the event store's existing
guarantees.

**This reasoning holds only where the source can still hand the event back, and nothing in the sentences above
says so.** It
argues against a second copy on the ground that the first copy is still there. Where the source cannot replay, the
first copy is not still there, so the argument does not reach that case and this section must not be read as closing
it. Decision point 7 and [#918](https://github.com/johanhaleby/occurrent/issues/918) cover what happens there, and a
holding area is a live option in that issue precisely because the reason it is rejected here does not apply.

It also rules out an alternative that looks tempting and is wrong. A per-event holding area would let the instance keep
handling later events while one earlier event sits aside. For a saga that is incorrect, not merely untidy. A saga is
a state machine, so handling event four against state that never saw event three produces state derived from a gap,
and no later reaction can tell that the gap is there. Suspension is per instance because correctness is per instance.

### 2. The record lives on `SagaEnvelope`

The question to answer first is what `SagaEnvelope` is for, not whether a field would fit on it.

`SagaEnvelope` is the whole durable record of one instance, and its eleven components fall into four groups rather
than the two an earlier draft of this section claimed. Identity is `sagaId`. Lifecycle is `status`, `createdAt`,
`updatedAt` and `completedAt`. The saga's own data is `state`. The executor's delivery bookkeeping is `version`,
`timers`, `streamWatermarks` and `positionWatermark`. And then there is `currentStep`, which is none of those. It is
denormalised out of `state` so that an operator can read which step an instance sits in without the store decoding
the state at all.

That last one decides this question, and it is worth being clear that it looked at first like a counter-example.
`currentStep` is a field the envelope holds purely so an observation query can answer an operational question
cheaply, kept correct by the compact constructor re-deriving it from `state`. A quarantine record works the same way.
Its position and version are delivery bookkeeping, its status is lifecycle, and its reason exists so an operator can
see why an instance stopped without loading anything. Every one of those groups is already on this type, and the
observation group is on it for exactly the reason quarantine needs.

The record also has to be written in the same compare-and-set as the decision not to advance. Write it separately and
there are two failure windows, one where the event was skipped with no record, which is loss, and one where the
record exists but the instance moved on, which is a false report of a stuck instance.

Three other owners were considered and rejected on what the type is for.

**The subscription model or its checkpoint storage.** This is the mistake ADR 115 recorded in its original form, a
fencing token attached to a storage type that had no business knowing what a lease was. A checkpoint knows a
position. It does not know that saga instances exist. Quarantine is per instance by definition, so a per-subscription
type could only hold it by inventing an instance concept it should not have.

**A separate `SagaQuarantineStore` capability.** Every store implementer would write a second store, and the record
could no longer be saved atomically with the envelope, which is the one property that makes it correct.

**`SagaInstances`.** ADR 70 made that facade read-only deliberately, and its javadoc says so. It has to show a
quarantine. It must not hold one.

### 3. Quarantine is the end of a time budget, not the answer to an exception

The first failure of an input records when it started failing and rethrows, which is exactly today's behaviour.
Every later failure of the same input compares the elapsed time against a configured budget and keeps rethrowing
while it is under it. Only past the budget does the executor record the quarantine, stop rethrowing, and let the
position advance.

The budget is a `Duration` rather than an attempt count, and the reason is stronger than the retry cadence being
tunable. **The retry loop is not always Occurrent's at all.** On the MongoDB models it is a `RetryStrategy` the user
can replace. Behind the shipped Kafka bridge it is not a `RetryStrategy` in any form. Under the default `REDELIVER`
policy `KafkaCloudEventBridge` catches the exception, declines to stage the offset, seeks the consumer back to that
record and pauses the partition for roughly one poll timeout before offering it again, without limit. Those two are
unrelated mechanisms running at unrelated rates, so an attempt count means a different amount of time on each, while
five minutes means five minutes on both.

**The default budget is five minutes.** Once the MongoDB backoff saturates it retries every two seconds, so five
minutes is on the order of a hundred and fifty attempts, which is ample evidence that an input is not going to
succeed. It also spans the failures worth surviving without quarantining anything. A replica-set election takes
seconds and a rolling restart takes a minute or two, and both finish well inside it. Against that, it holds the block
on the rest of the saga's instances to five minutes instead of forever.

**A transport that never re-offers the input cannot be quarantined by this mechanism, and the design does not pretend
otherwise.** `PushSubscriptionModel` has no retrying, no checkpoint and no position, and its javadoc says a handler
exception propagates to the caller. Fed by a bare in-process `accept(...)` with nothing retrying behind it, the first
failure is also the last, no second failure ever arrives, and a budget measured across repeated failures never
elapses. Such a saga keeps today's behaviour. Quarantine is available on the transports that re-offer the input,
which is the MongoDB models, the in-memory model, and a push feed behind a bridge that redelivers, and it is
unavailable on the ones that do not.

Two details keep the budget cheap. The elapsed time is measured from a value already read, because `process` loads
the envelope on every attempt anyway, so no extra read is needed. And only the first failure of an input writes, so
the cost is one store write per failing input rather than one per retry. When the store itself is what is failing,
that first write fails too, the executor rethrows, and the behaviour is exactly today's until the store recovers,
which is correct, because a saga with no reachable store cannot make progress in any case.

The executor does not retry in process, it rethrows, so it depends on something else re-offering the input. A user
who replaces a MongoDB model's `RetryStrategy` with a limited one has the same problem as the bare push feed above,
and the runner should say so at startup where it can see the policy rather than discovering it during an incident.
The executor owning the retrying itself was the alternative, and it was ruled against at the design gate, because
owning it means holding the subscription thread for the whole budget, which is a shorter version of the block this
decision exists to remove.

The failure write uses the same compare-and-set as every other write to the instance, and a lost one is meaningful
rather than something to retry. Losing it means another input advanced the instance while it was failing, most
likely a timer that fired successfully, so the failing input is now being applied to different state and may well
succeed. The failure record is therefore discarded on a lost compare-and-set and the budget starts over.

The identity of "the same input" is the redelivery key `EventMeta` already computes, the stream id with its version,
or the global position. An input the saga cannot recognise a redelivery of is already refused or warned about by
ADR 109's `RedeliveryDetection`, so nothing new is needed there.

### 4. An instance that has never started needs start detection to stop keying on document existence

A start event whose `evolve` or `onStart` throws has no envelope to record anything against, so the first failure
write has to insert one. That insert breaks the executor unless start detection changes with it.
`SagaExecutionSupport.startEventOrNull` returns null whenever `current != null`, and that null is what gates
`saga.onStart`. So once a quarantine-only document exists, the instance is permanently treated as already started, and
a later redelivery of its start event is skipped and `onStart` never runs, with no error anywhere.

The decision is that the executor keys start detection on an explicit marker of whether the instance has ever been
started, rather than on whether a document exists for it. A quarantine-only envelope is then honestly what it is, a
record that this instance failed before it began, and a later redelivery of its start event starts it properly.

The alternative was to exclude start-event failures from quarantine and leave them on today's path. That was rejected
because it is a hole in exactly the rule this decision exists to keep. A saga whose first event throws for one
correlation id would still stop every other instance, and the isolation rule in AGENTS.md has no severity ladder.

### 5. A quarantined instance is inert, and its watermarks stop moving

While quarantined, the instance skips every event addressed to it and its due timers are not fired. Skipping reuses
the existing `Outcome.skip()` path. The timers stop on their own, because `SpringMongoSagaStateStore.findWithDueTimers`
filters on `STATUS = ACTIVE` in the query rather than on "not terminal", so a quarantined instance drops out of the
poll with no change to that method. `SagaExecution.hasDueTimer` gains an explicit check as a second layer. That check
is new code, not a reuse of the existing completed-instance check, which is worth saying plainly because the first
draft of this decision claimed otherwise.

The invariant that makes this safe is narrow enough to state as a property. **For an instance in `QUARANTINED`, no
input advances `streamWatermarks` or `positionWatermark`, and no input dispatches a command.** If a skipped input
advanced a watermark, a later replay would treat it as already handled and skip it a second time, and that is the
loss this design exists to avoid. The watermarks are what make the recorded position meaningful.

The same no-loss property holds on the transports that are not checkpoint-based, enforced by the transport rather
than by a checkpoint. Under the Kafka bridge's default `REDELIVER` policy the offset is not staged for a record whose
handler threw, so the record stays available for exactly as long as the instance keeps failing on it.

Being inert also has a consequence worth naming, because it is what makes the next section possible. Nothing writes
to a quarantined instance, so an external write to it races nothing.

### 6. `SagaStatus` gains `QUARANTINED`, and `SagaInstance` gains one accessor

`SagaStatus` is documented as where an instance is in its lifecycle, and quarantine is a distinct position in it. An
instance that no longer handles its events is not `ACTIVE`, and it has not reached a terminal state, so it is not
`COMPLETED` either. Reporting it as `ACTIVE` would be a false answer to the exact question `SagaInstance` exists to
answer, which its own javadoc states as whether the instance is still running and whether it has stopped moving.

Discovery therefore needs no new query. `SagaInstances.findByStatus(QUARANTINED, Instant.now(), limit)` is the
existing enumeration, and the Spring stack already publishes a `SagaInstances` per saga.

`SagaInstance` gains one nullable accessor returning the quarantine record, holding the position the instance stopped
at, the failing exception's class name and message, and when it started failing. The narrowness rule from ADR 70
holds. The event payload and the stack trace stay out, because neither is lifecycle.

Two implementation constraints follow from ADR 70's invariant that every envelope answers every `SagaInstance`
member with no exemption. The quarantine fields are stored as top-level document fields in
`SpringMongoSagaStateStore`, next to the existing `currentStep` and `nextTimerFiresAt`, and they are added to both
enumeration projections. An instance whose state cannot be decoded is exactly the instance an operator is looking
for, so a quarantined instance must be enumerable without reading its state.

### 7. Release clears the record and restarts the subscription at the recorded position, and 0.34.0 does not ship it

Release is two things and both are needed. Clear the record alone and the instance handles new events against state
with a gap in it. Restarting the subscription alone re-runs the quarantine.

They also cannot just be ordered, which the first draft did not say. Clearing first lets the live subscription
apply a newer event to state that still has the gap. Rewinding first means the replayed events arrive while the
instance is still quarantined and are skipped. So release needs a third state between quarantined and active,
meaning released and awaiting its replay, in which the instance accepts an input only once the replay has reached the
position it stopped at. Defining that state, and how it behaves across competing nodes, is implementation work this
decision requires rather than a detail it can leave open.

So it is one operation, on `SagaSubscription`, which is the closest thing to a handle that owns both halves.
`SagaInstances` stays read-only. This needs new plumbing rather than a new method over what is already there.
`SagaSubscription` holds the live `Subscription`, the timer poller and the instances, and `Subscription` itself
exposes only `id()` and `waitUntilStarted(...)`. The `Subscribable`, the filter and the action `SagaRunner` used to
build it are local variables that are never stored, so `SagaRunner` has to retain enough to subscribe again before
release can exist at all.

Restarting the shared subscription from one instance's recorded position replays events every other instance already
handled, and no command is dispatched a second time. The watermarks are per instance, so every other instance
recognises those events as redeliveries and `process` returns `Outcome.skip()` before `react` runs.

**Release also pauses the saga's subscription, and the first draft of this decision was wrong to call the cost
wasted reads alone.** Repositioning a
MongoDB subscription requires it to be paused first, since `doResumeSubscription` refuses a subscription that is
already running. So a release stops delivery to every instance of that saga until the catch-up finishes. That is a
real pause of the shared channel, which is the same property this decision exists to protect, and the difference that
makes it acceptable is that it is finite and initiated by an operator who chose it, rather than indefinite and caused
by one faulty instance. It is still a cost an operator has to be told about, and the trade behind accepting it is
ADR 57's wasted work over loss.

Running several nodes makes this harder and the design does not yet answer it. `CompetingConsumerSubscriptionModel`
is a wrapper rather than a repositionable model itself, and its own javadoc says a cluster-wide pause means calling
`pauseSubscription` on every node. Nothing in `SagaRunner` or `SagaSubscription` coordinates that today, and
unwrapping to the delegate to reposition would go around the wrapper's own lock bookkeeping. Implementation has to
settle whether release is restricted to the node holding the lease or whether it coordinates across nodes, and that
is named here as open work rather than assumed to fall out.

**A source that cannot replay does not get quarantine at all, and refusing only the release would have been wrong.**
The first draft refused the release and allowed the quarantine, which loses the event at the moment of quarantine
rather than at the release. Returning normally acknowledges the input to the source, and for a push-fed saga
configured with `catchup = NONE` under ADR 96 there is no local history holding it, so on a queue it can be gone
immediately. Nothing later can replay what was never retained.

This turns on whether the source retains history, not on which retry loop re-offers the input, so it is unchanged by
the transport differences in Decision point 3. A push feed behind the Kafka bridge does re-offer a failing record,
which is enough to reach the budget, and it is still not enough to release afterwards, because recording the
quarantine is what stages the offset and moves past the record.

So for such a source the executor keeps rethrowing and the instance keeps blocking, which is today's behaviour.

**For this configuration the two halves of the isolation rule cannot both hold, and this decision keeps the
no-loss half.** AGENTS.md states the rule as both at once, no design may lose events and no consumer may be blocked
by another being faulty. Here quarantining would break the first and refusing to quarantine breaks the second, so
there is no answer that keeps both, and between them the loss is the one that cannot be undone afterwards.

**That is a narrowing, not an end state, and recording it as settled is the move AGENTS.md specifically forbids.**
The rule has no severity ladder, a loss window that is narrow, documented and warn-logged is still a loss, and a
change that narrows one is a step on a recorded path to closing it rather than the accepted answer. Every other
transport gains isolation from this decision and this transport does not, which makes the remaining gap smaller and no
more acceptable. [#918](https://github.com/johanhaleby/occurrent/issues/918), milestone 0.35.0, is that recorded
path. It holds the two candidate closings, holding the event for this case or refusing the topology the way ADR 90
refused a shared acknowledgement, and choosing between them is its work rather than this decision's.

**Until it closes, the limitation is stated at startup rather than discovered during an incident.** A saga wired to a
source that cannot replay says so where an operator sees it when the application comes up. The difference between
this configuration and every other one first matters in the middle of an outage, which is the worst moment to learn
that quarantine was never available here.

The replay boundary is inclusive of the recorded position, and this needs saying because the existing vocabulary
points the other way. `GlobalCheckpoint.of(p)` means resume after `p`, so restarting from the position of the event
that failed would skip permanently the one event the release exists to reprocess. The record therefore holds the
predecessor position, or the release uses an inclusive start, and either way the first position of a feed is a
defined boundary case rather than an accident.

Not every subscription model accepts a chosen start either. `CatchupThenPushSubscriptionModel.subscribe` rejects
every non-default start and always replays from the beginning, so release has to name the replay capability it needs
and what it does per model when that capability is absent, rather than assuming a `Subscribable` can be restarted
anywhere.

**0.34.0 does not ship release, and the reason is the capability named just above.** Implementation went looking for
a subscription model that guarantees the replay this section requires, meaning one that hands the instance back the
exact event it stopped on, and no model in this repository provides it. `RepositionableSubscriptions` is the closest
thing and it is not that guarantee. `CatchupSubscriptionModel` implements the interface whatever it wraps and throws
`UnsupportedOperationException` when the wrapped model cannot reposition, it answers `isRunning(id)` true while a
catch-up child is running but hands `pauseSubscription(id)` to the live model where the subscription is not
registered until handover, and a `GlobalCheckpoint` resume on the default MongoDB starter reaches
`MongoCommons.applyStartPosition`, which treats a checkpoint it does not recognise as the model default.

So the quarantine half ships and the release half does not. Quarantine on its own is what removes the block this
decision exists to remove, and it needs no replay to do it. Release needs a replay capability that has to be
designed and built in the subscription models rather than in the saga DSL, which is work of its own rather than a
detail of this decision. `SagaStateStore.delete(sagaId)` is the only way out of quarantine in 0.34.0.

The durable record identifies the failing input by its redelivery key rather than by a global position, which is what
keeps that later work cheap. A position is a number one subscription model assigns, and the same event has a
different one on a different replay path, or none at all. The redelivery key belongs to the event, so release can be
added on top of instances written by 0.34.0 without migrating them.

`SagaStateStore.delete(sagaId)`, the escape hatch ADR 128 already names, stays available throughout. It abandons the
instance deliberately instead of quietly.

### 8. The migration treatment for the shipped API this breaks

`SagaStatus`, `SagaEnvelope`, `SagaInstance` and `SagaRunnerConfig` all shipped in `occurrent-0.33.0`, verified with
`git ls-tree` and `git show` against the tag rather than inferred, so this decision breaks shipped API in five places.
A new `SagaEnvelope` component changes its canonical constructor and record-pattern arity. A new `SagaInstance`
accessor breaks anyone implementing that interface. The budget in decision point 3 is a new `SagaRunnerConfig`
component, which changes that record's canonical constructor and record-pattern arity the same way, and this break was
missed when the decision was drafted and added during implementation after checking the tag. And the new `SagaStatus`
constant breaks in two further ways of its own.

The visible half is that an exhaustive Java `switch` or Kotlin `when` over `SagaStatus` stops compiling. Nothing in
this repository does that, every reference here is an equality comparison or a `findByStatus` call, so the compile
break is hypothetical for code written outside it. That matters when judging the break, and it is not
evidence that nobody writes such a switch, because the library rule in AGENTS.md says the callers are not
observable from here.

The silent half is worse. `findByStatus(ACTIVE, ...)` is the documented way to find instances that have stopped moving, and
after this change the instances it was built to find are the ones it no longer returns.

An `org.occurrent.UpgradeToOccurrent_*` recipe cannot reach either. It cannot know what a user's new
`case QUARANTINED` branch should do, and it cannot rewrite the meaning of a `findByStatus(ACTIVE, ...)` call, because
whether that call site wants quarantined instances included is a question about the caller's intent. The repository
has no precedent for a search-only reporting recipe either. `org.openrewrite.*.search.*` appears in
`subscription-mode-0_32.yml` and `store-neutral-mongodb-config-0_34.yml` only as a precondition on a recipe that then
changes something.

This is the exception the migration convention names, so it was ruled at the design gate rather than guessed. **One
section in `doc/migration/upgrading-to-0.34.0.md` covering all four breaks, and no recipe.** AGENTS.md asks for a
recipe ideally rather than always, and a recipe that cannot do the work is worse than an honest note. That this
repository has no precedent for a search-only reporting recipe is part of the answer rather than a footnote, since
it is what makes no recipe a considered choice instead of an omission.

## Consequences

At-least-once delivery is unchanged for every instance except a quarantined one. The checkpoint is shared by the
whole subscription, so advancing it past a position means no instance re-receives events up to that position after a
restart. For every other instance those events were already handled, so at-least-once already held for them and
advancing costs them nothing.

For the quarantined instance, at-least-once through the subscription channel is given up at that position and
replaced by the recorded position, which a release will later replay from. This is the real trade in this decision and
it should be read as such. What the instance gets in exchange is that the property becomes explicit, durable and visible in
`findByStatus`, rather than implicit in a channel that is no longer moving.

A long store outage quarantines instances. Past the budget the design cannot tell an outage from an input that will
never succeed, so it treats it as the latter, and an outage longer than the budget quarantines a set of instances.
Until release ships they cannot be brought back through the saga API, so the budget's default has to be chosen with
that in mind.

Releasing an instance would pause the saga's subscription while the replay catches up, so it is an operation with a
visible cost rather than a background one. 0.34.0 does not ship it. See Decision point 7.

Dispatch amplification is reduced rather than introduced. Today an input that will never succeed re-dispatches its
whole command list on every one of the subscription's unlimited retries. Under the budget that stops when the
quarantine is recorded.

A quarantined instance's timers stop firing, so a saga that uses a timeout as a safety net does not get that net
while quarantined. That is deliberate, since firing a timeout would advance state across the gap, but it means a
quarantine has to be noticed rather than left alone.

**The change is not additive for out-of-tree callers, and the first draft was wrong to say it was.** The
`SagaStateStore` methods keep their signatures, but `SagaEnvelope` is a public record, so a new component changes its
canonical constructor and the arity of any record pattern over it, and a store built outside this repository
constructs envelopes. `SagaRunnerConfig` is a public record too, and the budget adds a component to it.
`SagaInstance` is a public interface, so a new accessor breaks anyone implementing it. All three are shipped API and
all three break at compile time.

Where a delegating or default member can absorb the break it should, and where it cannot the break is real and
belongs in the migration guide next to the enum constant.

## Rejected alternatives

**A dead-letter store for failed events**, meaning a separate collection the failing event is copied into so the
subscription can move on. Rejected because it makes a second durable copy of data the event store already holds, with
its own retention, its own indexes and its own way of going wrong, and because it does not answer the question that
actually matters. The instance still must not handle later events against a gap, so the suspension is needed anyway,
and once it is there the copy earns nothing.

**Skipping the failing event and continuing the instance**, logging loudly. Rejected because it is the loss AGENTS.md
rules out, and because the design intentions say explicitly that a loss window which is narrow, documented and
warn-logged is still a loss.

**Classifying exceptions into retryable and terminal**, so a `SagaRefusalException` quarantines immediately while
everything else retries forever. Rejected because it only works for the refusals Occurrent itself throws. The
failures that motivated the issue come from user code and from dispatchers, and a classification that covers
Occurrent's own exceptions and nothing else would read as a general mechanism while handling the rare case.

**An attempt-count budget instead of a duration.** Rejected because the retry cadence belongs to the subscription
model's `RetryStrategy`, which the saga does not choose and cannot read, so a count maps to an unknown amount of
wall-clock time. See Decision point 3.

**A per-instance holding area that lets the instance keep going.** Rejected in Decision point 1. It produces state
derived from a gap, and nothing downstream can detect that.

## Rulings at the design gate

Three questions were left for the gate rather than decided in the drafting, and all three are now closed. The
budget's default was never among them, it is decided at five minutes in Decision point 3.

1. **Migration.** One section in `doc/migration/upgrading-to-0.34.0.md` covering all five shipped breaks, meaning the
   `SagaEnvelope` component, the `SagaRunnerConfig` component, the `SagaInstance` accessor, the exhaustive switch over
   `SagaStatus` and the silent change to what `findByStatus(ACTIVE, ...)` returns. No OpenRewrite recipe.
   Decision point 8.
2. **Retrying.** The executor keeps rethrowing and depends on something else re-offering the input. Owning the
   retrying itself would hold the subscription thread for the whole budget, which is a shorter version of the block
   this decision removes. A transport that never re-offers the input therefore cannot reach the budget and keeps
   today's behaviour, which Decision point 3 states rather than implies.
3. **A non-replayable source.** The behaviour stands, meaning the quarantine is refused and the instance keeps
   blocking. The framing does not. This ships as a narrowing of the isolation rule rather than as its end state, and
   [#918](https://github.com/johanhaleby/occurrent/issues/918) on milestone 0.35.0 is the recorded path to closing
   it. Decision point 7.
