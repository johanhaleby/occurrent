# 174. A saga forgets an unroutable event it has not been offered for two intervals

Date: 2026-11-12

## Status

Accepted. Resolves [#1063](https://github.com/johanhaleby/occurrent/issues/1063). Changes how often the
unroutable-delivery `ERROR` in Decision point 3 of
[ADR 134](0134-a-saga-instance-that-keeps-failing-is-quarantined-at-its-own-position.md) repeats. ADR 134 shipped in
0.34.0, so it is corrected by reference here and gets a pointer in its Status section.

## Context

When the converter or the id extractor throws on an event, the saga can't tell which instance the event belongs to, so
it refuses the event every time it is offered. ADR 134 paces the logging of that refusal. It logs a `WARN` on the
first failure and an `ERROR` once per interval after that, for as long as the event keeps being offered. The interval is the
quarantine budget when one is configured, and five minutes when it is not.

To pace it, `SagaExecution` remembers each refused event in memory, by its redelivery key, and in 0.34.0 dropped the
entry only when the event routed on the same node. An event that never routes there again kept its entry for the life
of the runner. That happens when another node repairs it after a competing consumer's lease moved, or when a retry
strategy gives up on it. If the same event then failed again weeks later, the saga logged an `ERROR` straight away,
with the time since the first failure.

## Decision

**The saga forgets a refused event it hasn't been offered for more than twice the interval.** When the event is offered
again after that, it is a new failure, so it gets a `WARN` and its time is measured from then. The entries of other
forgotten events are dropped by the next refusal of any event, at most once per interval, so memory stays bounded
while the saga keeps refusing something.

The limit is twice the interval rather than one interval because of the models that offer the event again more slowly
than the interval. With one interval, a model offering the event every six minutes against a five-minute interval
would get a `WARN` on every offer and never the `ERROR` an operator alerts on. With two, any model that offers the event
again within ten minutes keeps getting one `ERROR` per offer.

I did not drop the entries when the subscription stops instead. A cancelled subscription makes the whole
`SagaExecution` unreachable, map included, so there is nothing to clear. A moved lease and a retry strategy that gives
up both keep the same `SagaExecution` running, and only a time limit reaches those. A cap on the number of entries
alone would bound the memory but keep the stale `ERROR`.

## Consequences

- **The cadence ADR 134 promised changes.** A model that offers a refused event more than twice the interval apart
  gets a `WARN` on every offer and never an `ERROR`, so an operator alerting on `ERROR` hears nothing from it. The
  runbook for a quarantined instance says so.
- An event offered exactly twice the interval after its last offer is still remembered and gets an `ERROR`.
- Entries are dropped only when the saga refuses an event. After the last refusal, what stays in memory is what was
  refused in the three intervals before it.
- Nothing is persisted, so there is nothing to migrate. A restart forgets every entry, as it did in 0.34.0.
