# 61. First-class snapshot support

Date: 2026-07-17

## Status

Accepted

## Context

Occurrent had no shipped snapshot support. The only guidance was a do-it-yourself section in the documentation built on
illustrative `Snapshot` and `SnapshotRepository` types that Occurrent does not ship, and it covered the classic stream-id
case only, with nothing for the Dynamic Consistency Boundary (DCB).

The write model already carried the pieces a real snapshot needs. `Decider.decideOnState(...)` exists precisely to run a
command against an already known state "for example from a snapshot", `Decider.isTerminal` is the lifecycle-end signal that
Jeremie Chassaing frames as "close the books, and start a new fiscal year", and `View`/`MaterializedView` already fold and
persist state. What was missing was the plumbing that loads a saved state at a known version, folds only the events after it,
and persists the result, plus an opt-in, storage-neutral API for it.

The recently added Projection DSL (ADR 58) and `DcbDecider` (ADR 52) set the read-side and write-side shapes to mirror, and
the read direction and limit work (ADR 56) established reading a tail "without folding the whole stream" as a supported idea.
A snapshot is, in Chassaing's terms, a cached fold result at a known version. Rebuilding is folding the events after that
version onto it. That makes a snapshot a pure, discardable optimization, which drives the safety requirements below.

## Decision

**A snapshot is a discardable, schema-versioned optimization, never a source of truth.** The stored value is
`Snapshot<S>(S state, long version, int schemaVersion)`, where `version` is the stream version on the stream path and the
global DCB position on the DCB path. A snapshot whose `schemaVersion` does not match the descriptor's declared version, or one
that is absent, is treated as no snapshot at all, and execution falls back to a full replay. State shapes change over time, so
a stale snapshot must never be deserialized into the new shape. Persistence through the DSL executors is best-effort. The
snapshot is written after the command's events commit, a save failure is logged and never fails the command, and losing a
snapshot only costs a fuller replay next time. A snapshot that must stay consistent on the write path is maintained instead
by a stream `@Snapshot(mode = SYNCHRONOUS)` or from a synchronous subscription, which folds it inside the write transaction
(ADR 57). A DCB snapshot has no synchronous mode, so DCB write-path consistency comes only from a synchronous subscription.

**Snapshotting lives in the DSL layer where state and folds already live, not in the core application service.** The only
change to the core is a state-agnostic read offset, `ExecuteOptions.fromStreamVersion(long)` and
`DcbExecuteOptions.fromPosition(long)`. When set, the application service reads only the events after that version or position
and hands them to the domain function, but it still writes at the stream's true current version and still captures the whole
boundary's DCB consistency token, so optimistic concurrency and the DCB append condition are unaffected. This was verified
against the in-memory and Spring Mongo stores, where a suffix read returns the true head version, and against the DCB stores,
where a position-bounded read still returns the store head as its consistency boundary. Keeping the offset in the core means a
snapshot-accelerated command reuses the one execute path that already owns retry, transactions, and synchronous dispatch,
rather than forking a second path that would drift. The application service still knows only events, never state.

**The fold-onto-a-base-state primitive is exposed on `Decider` as `evolve(S state, List<E> events)`,** mirroring
`View.evolve(S, List)`. It reuses the existing terminal-aware fold. The snapshot executor resumes by folding the tail onto the
snapshot state and then calling `decideOnState`, so no new decision path is introduced and terminal states are respected
during the tail fold.

**One higher-order `SnapshotPolicy` unifies technical and domain-driven triggers.** It exposes `everyNEvents(n)` (driven by
the version delta since the last snapshot), `onEvent(type)`, `whenState(predicate)`, `always()`, `never()`, and `or(...)`.
`whenTerminal(decider)` is `whenState(decider::isTerminal)` and is the "closing the books" trigger. There is no per-command
flag and no mode attribute, so a snapshot is a property of the handler, not of the call site.

**Closing the books is a policy plus an archival pattern, not a separate module.** The recommended model is to emit a real
domain event for the period boundary, carrying the closing balance that becomes the opening balance of the next period, so the
snapshot itself stays a discardable optimization. When prior events are genuinely no longer needed, they can be archived with
the existing `EventStoreOperations.deleteEventStream(String)` for a single stream, or `delete(Filter)` for a broader delete,
both irreversible and documented as such. `SnapshotStore.delete` defaults to failing loud rather than doing nothing, so a
store that cannot delete does not let archival believe a snapshot is gone.

**Storage is a small, storage-neutral `SnapshotStore<S>` capability** with `findLatest`, `save`, and `delete`, keyed by a
`String`. On the stream path the key is the stream id. On the DCB path the key defaults to a canonical string form of the
command's `DcbCriteria`, so tag and type order does not change the key, with an override hook for callers who want a shorter
or custom key. An
in-memory implementation ships in the common module, and a Spring Data Mongo implementation ships in the starters. The Mongo
document is a new envelope carrying `state`, `version`, and `schemaVersion`, because the existing `ViewStateRepository` stores
only the state value with no version marker. The deciders-free path uses a `SnapshotView<S,E>` descriptor and works with no
dependency on the decider modules, which is why the descriptor and the store live in the common module and the decider-aware
executors live in the blocking and reactor modules.

**`@Snapshot` mirrors `@Projection`.** A factory method annotated with `@Snapshot` returns a `SnapshotView<S,E>`, and the
bean-post-processor maintains a `SnapshotStore`-backed, resume-ready snapshot for it, keyed per stream, through the same
subscription infrastructure `@Projection` uses (catch-up and durable resume for `ASYNC`, write-path for `SYNCHRONOUS`). Store
resolution mirrors `@Projection`: a referenced store bean by type or name, else a unique `SnapshotStore` bean, else a
zero-config Mongo store. The write-side decider acceleration stays in the DSL through
`SnapshotDeciderApplicationService`, which consumes the same `SnapshotStore`, so the annotation declares and maintains the
snapshot while the DSL uses it. Because a `SnapshotStore<S>` is typed on the state, there is no single shared store bean.
The registrar builds a per-`@Snapshot` `SpringMongoSnapshotStore` in a namespaced collection, and DSL callers construct
their own store (in-memory or `SpringMongoSnapshotStore`).

The declarative `@Snapshot` works on both the blocking and reactor stacks, for stream and DCB. A stream `@Snapshot`
maintains one snapshot per stream. A DCB `@Snapshot` (a factory returning a `DcbSnapshotView`) maintains one snapshot per
boundary, keyed by the canonical form of its `DcbCriteria` so tag order does not change the key. The reactor registrar uses
a `ReactiveSnapshotStore` (a `ReactiveMongoOperations`-backed `ReactiveSpringMongoSnapshotStore` for the zero-config path),
because a reactive application has no blocking `MongoOperations`. The DSL executors remain the programmatic path for ad-hoc,
non-annotated use. A DCB `@Snapshot` does not support `mode = SYNCHRONOUS` (that mode is stream only), so a DCB snapshot
that must be current for read-your-writes is maintained through the DSL executor from a synchronous subscription instead.

## Amendment (2026-07-20): a snapshot beyond the observed head is discarded, not trusted

The original decision leaned on "a stale snapshot only makes the next replay longer" to argue the feature is safe by
construction. That holds for a snapshot behind the true head. It does not hold for a snapshot *ahead* of the true head,
which happens when a stream is reset (truncated) below a snapshot that was written against the longer stream. Resuming
from such a snapshot folds onto state the stream no longer holds. This only affects the **stream** paths: a DCB "version"
is a global, monotonic position that never resets, so a DCB snapshot can never be ahead of its head and no guard is needed
there.

Three changes make the stated safety guarantee true on the stream paths:

- **A snapshot whose version exceeds the observed head is discarded, not trusted.** `SnapshotSupport.resolveBase` and
  `isRedelivery` gain an `observedHead` argument: a loaded snapshot is used only when its version is at or below that head,
  otherwise the base folds from the initial state and a delivery that looked like a redelivery is treated as a rebuild. The
  original three-argument forms are preserved and delegate with an unbounded head, so a caller that already knows the
  snapshot cannot be ahead keeps the exact prior behavior.
- **The maintained `@Snapshot` stream path probes the head only in the ambiguous case.** When the delivered version is
  beyond the snapshot (the happy path) nothing extra is read. Only when the delivered version is at or below the snapshot
  version does the registrar probe the true head with a suffix read (`read(key, snapshotVersion, 1).version()`, which
  returns the real stream version regardless of skip and limit) to tell a genuine redelivery from a reset. A head below the
  snapshot version demotes to the initial state, and the existing range-fold rebuilds and self-heals by overwriting the
  stale snapshot at the reset version. Without this, a reset froze the maintainer forever: every delivery looked like a
  redelivery and was skipped.
- **The DSL stream executor is fail-safe and self-healing.** After the write, when the snapshot base is ahead of
  `WriteResult.oldStreamVersion()` (the true head before this write), the executor skips the save and deletes the stale
  snapshot with `SnapshotStore.delete`, logging it, so the next command folds fresh. `oldStreamVersion()` is already
  available post-write, so this needs no extra read and no change to the released `ApplicationService.execute` API.

The `eventsSinceSnapshot` arithmetic (the `requireInt` narrowing of the version delta) now runs **inside** the best-effort
boundary rather than at the call site. A negative or overflowing delta after a reset would otherwise throw from an
already-committed command; building the `SnapshotDecision` lazily inside `maybeSaveBestEffort` keeps every step after the
commit best-effort, consistent with the "a snapshot save never fails a committed command" rule stated below.

**Operational best practice:** pair a stream reset with `SnapshotStore.delete` for that key. The head guard is the safety
net for when that pairing is missed, not a substitute for it. On the executor path the accepted residual under that misuse
is one loud, self-healing bad decision: the first post-reset command decides against stale state once, commits, logs, then
deletes the snapshot so every command after it is correct.

## Consequences

- Snapshots are entirely opt-in. An application that does not use them pays nothing, because `fromStreamVersion` and
  `fromPosition` default to unset and the read then reduces to exactly the prior whole-stream read.
- A snapshot-accelerated command pays one snapshot load and one tail read per execute, in exchange for not folding the whole
  history. This cost is stated in the Javadoc, the documentation, and here.
- Snapshots are safe by construction. A changed state shape fails safe to a full replay through the schema version, and a lost
  snapshot only makes the next replay longer.
- The feature works with a `Decider` and with a plain `View`, and it covers stream and DCB on both the blocking and reactor
  stacks.
- Snapshot persistence through the DSL executors is best-effort: the snapshot is saved after the write commits, a save
  failure is logged rather than propagated so it never fails an already-committed command, and a lost snapshot only costs a
  fuller replay. Write-path consistency comes from maintaining the snapshot with a stream `@Snapshot(mode = SYNCHRONOUS)`
  or a synchronous subscription, which folds inside the write transaction (ADR 57), not from the DSL executors.
- A DCB `@Snapshot` does not support `mode = SYNCHRONOUS`. Registering a `DcbSnapshotView` with a synchronous mode is
  rejected, so a DCB snapshot is always maintained asynchronously through the declarative path. A DCB snapshot that must
  stay current for read-your-writes is maintained instead through the DSL executor from a synchronous subscription, or
  by folding it directly inside a synchronous subscription handler.
- The read-side reader passes the folded tail as the decision's events, so `always()` and `onEvent(...)` behave sensibly on a
  read and `everyNEvents` rides the version delta.
- One method was added to the public `Decider` API (`evolve(S, List<E>)`) and one option each to `ExecuteOptions` and
  `DcbExecuteOptions`. All three are additive and backward compatible.
- The declarative `@Snapshot` annotation works on both the blocking and reactor stacks, for stream and DCB. The Mongo
  `SnapshotStore` reads never throw on a stale or unreadable snapshot (they degrade to a full replay), and both scalar and
  POJO state round-trip.
