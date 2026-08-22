# 134. A saga instance that keeps failing is quarantined at its own position

Date: 2026-08-22

## Status

Proposed. Resolves #818. 0133 is the current maximum, re-audited across every remote branch at write time per the
max-plus-one rule.

One question in the Decision below is not settled here and needs a ruling before implementation starts. It is marked
**Open** and repeated at the end of this file.

## Context

`SagaExecution.onCloudEvent` runs the saga and catches nothing. `SagaExecutionSupport.process` calls
`saga.evolve(previousState, input)` and then `saga.react(nextState, input)` with nothing catching either, so a
throwing `evolve`, a throwing `react` or a throwing command dispatcher reaches the subscription model unchanged.

What the subscription model then does is worse than the plain redelivery the issue describes. Every blocking model
wraps the handler in `executeWithRetry`, and the default retry strategy on both MongoDB models backs off
exponentially from 100 ms to 2 seconds and never stops. So the failing event is retried forever on the subscription's
own cursor thread. In `NativeMongoSubscriptionModel` the `currentStartAt.set(...)` call sits after the handler call in
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

It also rules out an alternative that looks tempting and is wrong. A per-event holding area would let the instance keep
handling later events while one earlier event sits aside. For a saga that is incorrect, not merely untidy. A saga is
a state machine, so handling event four against state that never saw event three produces state derived from a gap,
and no later reaction can tell that the gap is there. Suspension is per instance because correctness is per instance.

### 2. The record lives on `SagaEnvelope`

The question to answer first is what `SagaEnvelope` is for, not whether a field would fit on it.

`SagaEnvelope` is the one durable record of everything the executor needs to run a single instance safely under
at-least-once delivery. Its components are the instance's state, its lifecycle status, the optimistic-lock version,
`timers()`, which is input the executor still owes work on, and `streamWatermarks()` and `positionWatermark()`, which
are what the instance has already consumed. Its javadoc already says the delivery bookkeeping exists so the executor
is safe under at-least-once delivery and means nothing outside it.

A quarantine record is both of those categories at once. It is input the executor still owes work on, and it is a
statement about what the instance has and has not consumed. It is the same kind of thing as a timer entry, kept for
the same reason, and it has to be written in the same compare-and-set as the decision not to advance. Write it
separately and there are two failure windows, one where the event was skipped with no record, which is loss, and one
where the record exists but the instance moved on, which is a false report of a stuck instance.

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

The budget is a `Duration` rather than an attempt count, and the reason is that the retry cadence is not the saga's
to see. It belongs to the subscription model's own `RetryStrategy`, which defaults to unlimited attempts with
exponential backoff, and a user can replace it. Ten attempts is an unknown amount of wall-clock time under a strategy
the saga did not choose, while five minutes means five minutes under any of them.

Two details keep the budget cheap. The elapsed time is measured from a value already read, because `process` loads
the envelope on every attempt anyway, so no extra read is needed. And only the first failure of an input writes, so
the cost is one store write per failing input rather than one per retry. When the store itself is what is failing,
that first write fails too, the executor rethrows, and the behaviour is exactly today's until the store recovers,
which is correct, because a saga with no reachable store cannot make progress in any case.

The identity of "the same input" is the redelivery key `EventMeta` already computes, the stream id with its version,
or the global position. An input the saga cannot recognise a redelivery of is already refused or warned about by
ADR 109's `RedeliveryDetection`, so nothing new is needed there.

### 4. A quarantined instance is inert, and its watermarks stop moving

While quarantined, the instance skips every event addressed to it and its due timers are not fired. Skipping reuses
the existing `Outcome.skip()` path, and holding the timers reuses the existing `hasDueTimer` check that already
excludes a completed instance.

The invariant that makes this safe is narrow enough to state as a property. **For an instance in `QUARANTINED`, no
input advances `streamWatermarks` or `positionWatermark`, and no input dispatches a command.** If a skipped input
advanced a watermark, a later replay would treat it as already handled and skip it a second time, and that is the
loss this design exists to avoid. The watermarks are what make the recorded position meaningful.

Being inert also has a consequence worth naming, because it is what makes the next section possible. Nothing writes
to a quarantined instance, so an external write to it races nothing.

### 5. `SagaStatus` gains `QUARANTINED`, and `SagaInstance` gains one accessor

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

### 6. Release clears the record and restarts the subscription at the recorded position

Release is two things and both are needed. Clear the record alone and the instance handles new events against state
with a gap in it. Restarting the subscription alone re-runs the quarantine.

So it is one operation, on `SagaSubscription`, which is the only handle that owns both halves. It holds the live
`Subscription`, and it was built by `SagaRunner` from the `Subscribable` and the saga's filter, so it can subscribe
again from a chosen position. `SagaInstances` stays read-only.

Restarting the shared subscription from one instance's recorded position replays events every other instance already
handled, and that is safe rather than merely tolerable. The watermarks are per instance, so every other instance
recognises those events as redeliveries, `process` returns `Outcome.skip()` before `react` runs, and no command is
dispatched a second time. The cost is wasted reads, which is the trade ADR 57 already settled in favour of wasted
work over loss.

Release is refused, rather than silently doing half the job, on a saga whose subscription cannot replay history. A
push-fed saga configured with `catchup = NONE` under ADR 96 has no local history to restart from, so releasing it
would resume the instance at the next live event and drop everything since the recorded position. The refusal names
the remedy that already ships, `SagaStateStore.delete(sagaId)`, which is the same escape hatch ADR 128 names, and
which abandons the instance deliberately instead of quietly.

### Open: the migration treatment for the new `SagaStatus` constant

`SagaStatus` shipped in `occurrent-0.33.0`, verified with `git ls-tree` against the tag rather than inferred, so
adding a constant is a breaking change under the release-status rule, and it breaks in two ways.

The visible half is that an exhaustive Java `switch` or Kotlin `when` over `SagaStatus` stops compiling. The silent
half is worse. `findByStatus(ACTIVE, ...)` is the documented way to find instances that have stopped moving, and
after this change the instances it was built to find are the ones it no longer returns.

An `org.occurrent.UpgradeToOccurrent_*` recipe cannot reach either. It cannot know what a user's new
`case QUARANTINED` branch should do, and it cannot rewrite the meaning of a `findByStatus(ACTIVE, ...)` call, because
whether that call site wants quarantined instances included is a question about the caller's intent. The repository
has no precedent for a search-only reporting recipe either. `org.openrewrite.*.search.*` appears in
`subscription-mode-0_32.yml` and `store-neutral-mongodb-config-0_34.yml` only as a precondition on a recipe that then
changes something.

This is the exception the migration convention names, so it is a ruling rather than a guess. The recommendation is a
section in `doc/migration/upgrading-to-0.34.0.md` covering both halves, and no recipe, on the grounds that AGENTS.md
asks for a recipe ideally rather than always and a recipe that cannot do the work is worse than an honest note.

## Consequences

At-least-once delivery is unchanged for every instance except a quarantined one. The checkpoint is shared by the
whole subscription, so advancing it past a position means no instance re-receives events up to that position after a
restart. For every other instance those events were already handled, so at-least-once already held for them and
advancing costs them nothing.

For the quarantined instance, at-least-once through the subscription channel is given up at that position and
replaced by the recorded position plus a release. This is the real trade in this decision and it should be read as
such. What the instance gets in exchange is that the property becomes explicit, durable and visible in
`findByStatus`, rather than implicit in a channel that is no longer moving.

A long store outage quarantines instances. Past the budget the design cannot tell an outage from an input that will
never succeed, so it treats it as the latter, and an outage longer than the budget quarantines a set of instances. They are all recoverable by release, so this is operational work rather than lost data, but it is a
real cost and the budget's default has to be chosen with it in mind.

Dispatch amplification is reduced rather than introduced. Today an input that will never succeed re-dispatches its
whole command list on every one of the subscription's unlimited retries. Under the budget that stops when the
quarantine is recorded.

A quarantined instance's timers stop firing, so a saga that uses a timeout as a safety net does not get that net
while quarantined. That is deliberate, since firing a timeout would advance state across the gap, but it means a
quarantine has to be noticed rather than left alone.

The change is additive to `SagaStateStore`. The SPI methods keep their signatures and the new fields ride on
`SagaEnvelope`, which every store already round-trips, so an out-of-tree store implementation compiles unchanged. It
will not persist the new fields until it is updated, which degrades that store to today's behaviour rather than
breaking it.

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

## Open question repeated

The migration treatment for adding `QUARANTINED` to the shipped `SagaStatus` enum, covered above. Recommendation is a
migration-guide section covering both the compile break and the silent `findByStatus(ACTIVE, ...)` change, with no
OpenRewrite recipe.
