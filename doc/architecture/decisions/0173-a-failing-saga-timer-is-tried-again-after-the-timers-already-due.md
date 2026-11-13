# 173. A failing saga timer is tried again after the timers already due

Date: 2026-11-13

## Status

Accepted. Resolves [#1003](https://github.com/johanhaleby/occurrent/issues/1003). Fixes the first of the two timer
mechanisms in the Context of
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md), which says it is not fixed in
0.34.0. ADR 134 shipped in 0.34.0, so it is corrected by reference here and gets a pointer in its Status section.

## Context

`SagaExecution.pollTimers` asks the state store for at most `timerBatchLimit` instances with a due timer, a hundred by
default, and fires them. A timer whose reaction throws is logged and stays due at the same `firesAt`. In 0.34.0
`findWithDueTimers` returned the due instances in any order. Once `timerBatchLimit` instances kept failing their
timers, every poll could pick the same instances, and no other instance's timer fired.

ADR 134 tried a hold-off on the instance and withdrew it. A record on the instance that names a failing timer is not
cleared when that timer is cancelled, it delays the instance's other timers, and a later failure of a healthy timer
inherits its clock. [#998](https://github.com/johanhaleby/occurrent/issues/998) rejected ordering the query by the
instance's last attempt, because that needs a second index and makes the poll walk every active instance.

## Decision

**Each timer records when it last failed, and a timer is due at the later of `firesAt` and that instant.**
`SagaEnvelope.TimerEntry` gains `nextAttemptAtEpochMilli`, which is null until the timer has failed. When a timer's
reaction throws anything except an `OutOfMemoryError`, the poller writes the instant one millisecond after its poll
to that timer. The write
reads the instance again and compares and sets, up to `SagaRunnerConfig.maxCasAttempts()` attempts. Nothing is
written when the timer was cancelled, consumed or re-armed to another instant in the meantime, or when the instance
is no longer active.
When every attempt loses, or the store fails, the poller logs a `WARN` and goes on with the next instance, and the
timer keeps the due time it had.

**`findWithDueTimers` returns the instance with the earliest due time first.** An instance's due time is the earliest
due time of its timers, `SagaEnvelope.earliestDueAtEpochMilli()`. The MongoDB store writes it to the
`nextTimerFiresAt` field it already keeps and indexes, sorts on it, and the existing `(status, nextTimerFiresAt)` index
serves the sort, so no index is added. The in-memory store sorts on it too, with the saga id breaking a tie. The
contract does not say in which order instances due at the same instant come back.

A failed timer is then due just after the poll that tried it, which puts it behind every instance that was already
due at that poll, one due at exactly the poll's instant included. Every instance ahead of a healthy due instance is tried at most once before it, so the
healthy instance fires within `ceil(ahead / timerBatchLimit) + 1` polls, where `ahead` is the number of instances
due before it, however many of them fail. That holds while every failure is recorded and nothing removes the record
before the timer is tried again. An event saved after a record removes it when the event read the instance before the
record, so an instance that fails its timer and receives such an event between two polls is tried ahead of its turn
once more.

The record belongs to the timer, so the objections to the instance hold-off in ADR 134 do not apply. Cancelling,
consuming or re-arming a timer to another instant replaces its entry, and the record goes with it. A re-arm to the
same instant keeps the record, since it is the same timer. The instance's other timers keep
their own due times, and a failure of one timer writes nothing to another.

**There is no backoff and no new setting.** A failing timer is still tried on every poll where its turn comes, as in
0.34.0. A backoff later is a change to the instant written into the same field.

I rejected three other designs.

- **A per timer hold-off that grows with each failure, capped by a new `maxTimerBackoff`.** It spreads the retries
  out but does not give a healthy instance a turn. Past roughly `timerBatchLimit × maxTimerBackoff / pollInterval`
  failing instances, the retries fill every batch again. The cap would also be a new setting, and
  `SagaAnnotationRegistrar` would have to pass it from `@Saga` to the runner.
- **Ordering by the instance's last attempt.** That is the design #998 rejected for its second index and its walk
  over every active instance. Putting the attempt into the due time is what lets the existing index do the ordering.
- **Remembering failed instances in the poller's memory.** The query cannot skip them without fetching
  `timerBatchLimit` plus the failing instances on every poll, and the memory is lost when the competing consumer's
  lease moves to another node.

## Consequences

- **`SagaEnvelope.TimerEntry` gains a component.** The two-argument constructor stays and sets it to null. A record
  pattern on `TimerEntry` has to name the new component, and `equals` compares it too. The changelog lists it
  under Breaking changes.
- **`SagaInstance.nextTimerAt()` keeps its 0.34.0 meaning, the earliest `firesAt`.** An instance stuck on a failing
  timer still shows a time well in the past, and the write does not move `updatedAt` either. The MongoDB field
  `nextTimerFiresAt` holds the due time instead, so a query on that field sees the last failed attempt.
- **The `SagaStateStore` contract asks for the earliest due instance first.** A store of your own that ignores the
  order keeps the starvation of 0.34.0. It still gets a write on every failed fire, which buys it nothing.
- **Every failed attempt is a write.** It costs a read and a compare and set, at most `timerBatchLimit` of them per
  poll.
- **The write keeps the instance's version.** It compares on the version it read and stores that same version. When it
  raised the version, an event whose delivery took longer than the poll interval read the instance before a record and
  saved after it, so it lost its compare and set on every attempt and failed with `SagaConcurrencyException`. Now that
  event saves, and the timers it writes are the ones it read, so the record is gone and the timer is due at its
  `firesAt` again until it fails once more. Every other write the executor makes raises the version, so a record can
  only replace another record made at the same version, never another write. The version is no longer a count of every
  write, and nothing in Occurrent reads it as one. A `SagaStateStore` of your own has to store the version in the
  envelope even when it equals `expectedVersion`. One that refuses that save refuses every record, the poller logs a
  `WARN` for each failed fire, and that store keeps the order of 0.34.0.
- **A 0.34.0 MongoDB document needs no migration.** Its timer subdocuments have no `nextAttemptAtEpochMilli`, which
  reads as a timer that has not failed, and its `nextTimerFiresAt` is already the earliest due time.
- **A rolling deploy works both ways.** 0.34.0 reads only `name` and `firesAtEpochMilli` from a timer subdocument, so
  it ignores the new field. It selects by `nextTimerFiresAt`, which for a failed timer is one millisecond after the poll
  that tried it, so a later poll still finds the timer. A 0.34.0 write rewrites the timers and recomputes `nextTimerFiresAt` from
  `firesAt`, which drops the record and gives that instance the 0.34.0 order again. No timer is lost.
- **The recorded instant comes from the poller's clock.** A poller whose clock runs ahead records instants that far
  ahead, and after the competing consumer's lease moves to a node with a correct clock, the instance waits that long
  before it is tried again.
