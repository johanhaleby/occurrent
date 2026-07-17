# Closing the books (snapshot example)

A small, in-memory, Docker-free example of Occurrent's snapshot support, built on a `Decider` for an account ledger. It
shows the two snapshot styles the snapshot DSL supports.

## Technical snapshot

A snapshot is a cached fold result at a known version. Instead of folding a whole stream on every command, the application
service loads the latest snapshot and folds only the events after it. `SnapshotDeciderApplicationService` does this for a
`Decider`, and a `SnapshotPolicy` decides when to write a new snapshot. Here the policy is `everyNEvents(100)`, so a
long-lived account keeps its replay bounded.

A snapshot is a discardable optimization. It is versioned by a `schemaVersion`, so a changed state shape falls back to a
full replay rather than deserializing a stale value, and a lost snapshot only makes the next replay longer. It is never a
source of truth.

## Closing the books

`isTerminal` is the ledger's "close the books" signal. When `CloseBooks` is issued the state becomes terminal, and the
policy `SnapshotPolicies.whenTerminal(decider)` writes a snapshot of the closing balance at that boundary.

The closing balance is carried into the next period as a real domain event. The next period is a new stream whose first
command, `SetOpeningBalance`, records the carried balance as an `OpeningBalanceSet` event. Because the opening balance is a
real event in the new period, the previous period's detailed events can be archived with
`EventStoreOperations.deleteEventStream` without losing money. The snapshot of the closed period stays a discardable
optimization, and the authoritative balance lives in the event log of the period that follows.

## Run it

```
mvn -pl example/snapshot/closing-the-books -am test
```

`ClosingTheBooksTest` asserts both flows, and `ClosingTheBooksDemo` runs the same flow from a `main` and prints what
happens.
