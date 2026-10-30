# 157. A saga's quarantine budget runs per input

Date: 2026-10-29

## Status

Proposed. Resolves [#1318](https://github.com/johanhaleby/occurrent/issues/1318). Changes how
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md) measures its budget. ADR 134
shipped in 0.34.0, so it is corrected by reference here and gets a pointer in its Status section.

## Context

ADR 134 quarantines an instance once it has been failing for longer than `quarantineAfter`. The first failure of an input
writes a `SagaFailure` to the instance with a `firstFailedAt` instant, and every later failure measures the elapsed time
from that instant.

In 0.34.0 the instant belongs to the instance rather than to the input. When a different input fails, `onFailure` replaces the
record and keeps the old `firstFailedAt`. That keeps one rule working. An instance where two inputs fail in turn, version
7, then 8, then 7, then 8, still reaches its budget. Restarting the clock for every new input would let that instance fail
forever.

The instance clock goes wrong under the broker's PARK failure action. Kafka and RabbitMQ republish the failing event to a parking destination
and move on, so the event is not offered again and nothing clears its record. A failure of an unrelated input days later
then finds the old `firstFailedAt`, and the first failure of that second input quarantines the instance.

## Decision

**Each failing input keeps its own clock.** A version listed as pending (ADR 154) records the instant its input first
failed, next to the version. Wherever the saga needs to know how long an input has been failing, it takes:

1. the instant on the input's pending entry, when it has one,
2. otherwise the record's `firstFailedAt`, when the record names this input,
3. otherwise the current time.

The same rule decides the budget check, the `firstFailedAt` written to a new record and the instant written to a new
pending entry. So a write never moves an input's clock, and the clock is gone only when the input is applied.

The three cases ADR 134 had to keep apart now come out like this:

| Sequence | What happens |
|---|---|
| Versions 7, 8, 7, 8 fail in turn | Both stay pending with their own instants, so the third failure measures 7 from its first failure and reaches the budget as before |
| Version 7 fails and is parked, then version 8 fails once a week later | Version 8 is measured from its own first failure, so it does not quarantine the instance |
| Version 7 is parked and replayed after `quarantineAfter`, and fails again | It is measured from its first failure a week ago, so it quarantines the instance on that first replay failure |

The last row is intended. The parked event has been failing for longer than the budget, and a replay that fails again is
the evidence the budget asks for. The runbook tells an operator who replays a parked event what a failure then costs.

**A replaced record hands its instant to the pending entry.** An instance stored before pending existed can have a record
whose version is not listed. When a different input replaces it, the version moves to the list of failures of unknown
type, and its `firstFailedAt` moves with it, so that input does not start again from zero when it fails next.

**The pending entry also keeps the clock when the record's write is lost.** A compare-and-set on the record that loses
to a concurrent writer still ends with the version pending with its instant, through the pending write that follows it. That
narrows [#977](https://github.com/johanhaleby/occurrent/issues/977), where a lost write can keep the budget from ever
starting, without closing it, since the record itself is still not written.

**At the pending cap the record is the clock.** An instance that lists 10 000 pending versions lists no more, and a
version that fails then is self-held instead ([ADR 158](0158-a-saga-holds-an-event-that-arrives-before-its-instance-starts.md)).
It has no pending entry, so its clock is the record's `firstFailedAt`, and the record is still written after the
self-hold, so the input reaches its budget.

## Consequences

- **Behaviour change for 0.34.0 users with quarantine on.** An instance whose failures come from different inputs is
  quarantined later than before, or not at all, and a parked input that is replayed past its budget is quarantined on its
  first replay failure. The changelog lists it under Breaking changes.
- The stored instance grows by one instant per pending version. Nothing is dropped from pending except by applying the
  version (ADR 154, as amended for [#1330](https://github.com/johanhaleby/occurrent/issues/1330)), so the instants are
  bounded by the same limit as the pending versions.
- An instance stored by 0.34.0 has no pending versions, since pending arrived with ADR 154. The input its record names
  keeps the record's `firstFailedAt`, and any other input starts its clock on its next failure.
- An event without a stream id and version writes no record, so it is never quarantined, as in 0.34.0.
