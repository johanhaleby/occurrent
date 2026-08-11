# 122. An applied position is not a completed prefix, so the applied-position feature is withdrawn

Date: 2026-08-11

## Status

Accepted. Withdraws the feature introduced by [ADR 111](0111-a-projection-records-the-position-it-has-applied.md),
which never shipped. Resolves review finding 1 (blocker) and finding 6 (ADR drift) from the pre-0.33.0 design
review. Reopens [#361](https://github.com/johanhaleby/occurrent/issues/361), which ADR 111 closed.

## Context

ADR 111 built `AppliedProjectionPositionStore` so a caller holding a position (one a command handed back) could
ask whether a projection has caught up to it, and wait if not. `waitUntilApplied(projectionId, position, timeout)`
answers `true` once the store's recorded position for that projection is at or beyond `position`
([AppliedProjectionPositionStore.java:121-125](https://github.com/johanhaleby/occurrent/blob/96f6db174301976e90e7026624f25aad171ff729/dsl/projection-dsl/common/src/main/java/org/occurrent/dsl/projection/AppliedProjectionPositionStore.java#L121-L125)),
and the recorded position only ever moves forward
([:166](https://github.com/johanhaleby/occurrent/blob/96f6db174301976e90e7026624f25aad171ff729/dsl/projection-dsl/common/src/main/java/org/occurrent/dsl/projection/AppliedProjectionPositionStore.java#L166)).
The Mongo store does the same with a single `Update().max` write
([MongoAppliedProjectionPositionStore.java:123](https://github.com/johanhaleby/occurrent/blob/96f6db174301976e90e7026624f25aad171ff729/framework/spring-boot-starter-mongodb/src/main/java/org/occurrent/springboot/mongo/blocking/MongoAppliedProjectionPositionStore.java#L123)),
pinned to the commit before this withdrawal, since the file does not survive it.

Reading a maximum as an answer to "has everything up to here been applied" treats it as a completed prefix. That
reading is sound only when a projection is fed its events in position order, which ADR 111 decision 7 asserted:
"That holds when the projection is given its events in position order, which is what a subscription does." The
javadoc carried the same claim.

### The precondition fails on every delivery source Occurrent ships

Position order equalling commit order is required for the claim to hold, and it does not hold on MongoDB. Positions
are reserved before commit, deliberately, so the shared counter does not become a transaction conflict
([SpringMongoEventStore.java:162-171](../../../eventstore/mongodb/spring/blocking/src/main/java/org/occurrent/eventstore/mongodb/spring/blocking/SpringMongoEventStore.java)).
[ADR 84](0084-what-a-position-guarantees.md) states the consequence directly: "A position is not commit order...
two concurrent writers can commit in the opposite order to their positions." Change streams deliver in commit
order. So a writer that reserves position 471 and stalls can be overtaken by a writer that reserves 500 and
commits first, and a projection watching the stream records 500 before 471 has arrived. `waitUntilApplied(471)`
then answers `true` while the caller's own event is still on its way. The failure is silent, and its window is the
full propagation latency of the caller's own write, which is exactly the latency the API exists to hide.

[ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) had already ruled on this question for the DCB write
path, in the other direction: "The token is distinct from the read's `lastSequencePosition`... the two are
different types (`DcbConsistencyToken` versus `long`) so a caller cannot pass one where the other is required."
`waitUntilApplied(String, long, Duration)` repeats the mistake ADR 21 built a type-level guard against.

The in-memory store fails the same precondition for an unrelated reason. It assigns positions inside the lock it
writes state under, but publishes to subscribers outside that lock
(`InMemoryEventStore.write`, lock closes at line 215, `listener.accept` at line 221), so two concurrent writers can
be delivered out of position order there too.

A second, larger instance of the same defect sits in the catch-up replay.
`RecordingMaterializedView.replayCompleted()` writes the highest position seen during the replay in one shot
(`:90-97`). [ADR 28](0028-dcb-catch-up-captures-resume-token-before-replay.md) establishes, as accepted fact, that
an event below the replay's captured head can still be in flight and is delivered later on the live stream rather
than seen by the replay. The recorder's watermark therefore claims a prefix the replay provably has not applied in
full, on every restart under concurrent writes, not only in a rare race.

A third defect is that `source = PUSH` was never guarded. ADR 111 decision 7 names "a push feed driven by several threads
at once" as the case where the recorded value "would then claim more than the read model has," and nothing refuses
that configuration. The registrars wrap it unconditionally, and a push event carries a position, so decision 6's
refusal of a missing position never fires.

A fourth defect is that the feature's own opening scenario ("a client that ran a command and got back the global
position 4711") corresponds to no stream-path API. `WriteResult` is
`record WriteResult(String streamId, long oldStreamVersion, long newStreamVersion)`, with no position field.
Only `DcbAppendResult` returns one.

### Why the completed-prefix family cannot be salvaged here

A contiguous high-water mark, the standard remedy for this problem elsewhere, needs an unfiltered view of the
sequence to detect a gap and either an exact signal or a timeout to decide a gap is permanent. Occurrent pushes
subscription filters server-side into the change stream
([SpringMongoSubscriptionModel.java:201](../../../subscription/mongodb/spring/blocking/src/main/java/org/occurrent/subscription/mongodb/spring/blocking/SpringMongoSubscriptionModel.java),
the same call on the reactor and native models), so a filtered projection never observes the positions it filters
out and cannot tell a filtered position from an abandoned one from one still in flight. MongoDB also reports no
queryable in-flight transaction boundary, so even an unfiltered probe run separately from delivery cannot resolve
that ambiguity. [ADR 62](0062-pluggable-projection-event-source.md) already ruled a feed-derived contiguity
watermark unworkable for the same reason a gap cannot be told apart from one that is merely slow: "it stalls
forever at the first permanent gap, and any timeout that advances it past a still-uncommitted position reintroduces
the event loss it was meant to prevent." A naive gap-closure design fails for the same reason.

Prior art in the field confirms rather than contradicts this. Axon's `GapAwareTrackingToken` treats a gap as
permanent after a fixed timeout and a fixed distance, without consulting the database, so a slow commit can be
skipped permanently. It has no positional read-your-writes API at all, only push-based subscription queries the
application correlates itself. Marten's `HighWaterDetector` is the strongest of this family. It holds a gap until
`pg_current_snapshot()` and `pg_stat_activity` prove no live transaction can still fill it, which needs a queryable
in-flight boundary MongoDB does not expose, and it has shipped a silent-skip bug in exactly this failure shape
(advancing past committed events during concurrent appends, fixed in 9.16.1). Emmett is exact on PostgreSQL,
comparing a transaction id against `pg_snapshot_xmin(pg_current_snapshot())`, and for the same reason deliberately
carries no global position at all on its MongoDB store, riding change-stream order instead. None of the three
tags an append with an identity and has the subscriber track which identities it has seen. On a store whose
delivery is commit-ordered but whose positions are not comparable across appends, none of the three has a
read-your-writes answer.

An exact-position wait (answer for position P itself rather than "at or beyond P") needs no ordering assumption,
but it needs a record per position rather than one per projection, with a retention window that becomes a
correctness parameter rather than a housekeeping one, and it times out whenever a multi-event append's last event
is one the projection does not handle, which is the shape ADR 111 decision 2 designed the per-projection value
against.

## Decision

Withdraw `AppliedProjectionPositionStore`, `Projections.recordingAppliedPosition` on both stacks,
`RecordingMaterializedView`, `RecordingReactiveUpdate`, both Mongo-backed stores, `@Projection`'s
`recordAppliedPosition` attribute, and the Spring properties that pace it, from 0.33.0. None of these types shipped
in a previous release, so removing them is not a breaking change and needs no migration path
([AGENTS.md](../../../AGENTS.md), the changelog section on unreleased capabilities). Every configuration Occurrent
could build was unsound, so nothing of value survives keeping the interface as a primitive with a corrected
precondition and no wiring. That would be a public abstraction whose only real consumer has just been deleted, the
shape [AGENTS.md](../../../AGENTS.md) rules out ("inventing a public interface for a design that is not settled
yet... build it where its first real use lands"). Documenting the precondition honestly and shipping the feature
anyway was considered and rejected the same way, because the gap is correctness, not convenience, and the
project's own convention is to prevent a footgun at the type level or fail loud rather than rely on a caller
reading the docs. An unconditional startup refusal for `recordAppliedPosition = true` was considered next, and it
is strictly worse than withdrawal, since nothing at Spring registration time can distinguish a sound configuration
from an unsound one to refuse precisely, so the refusal would fire on every configuration and the annotation
attribute, interface, beans, and properties would all exist only to always throw.

A correct mechanism exists and is not signature-preserving, so it is out of scope for 0.33.0. Two designs were
considered.

**Commit-ordered positions, opt-in.** A `positionsAreCommitOrdered()` capability on `PositionOrderedReader`,
answered `true` by a store that reserves its position counter inside the append transaction instead of before it,
with a registrar refusal when the answer is `false`. This makes the existing `waitUntilApplied` contract correct as
written, at the cost of serializing every write against a single counter document, exactly what ADR 21 and ADR 84
built the current scheme to avoid, and it does nothing for a multi-threaded push feed, which still needs the
`source = PUSH` refusal regardless of how positions are numbered.

**Append identity.** Mint one identifier per append, stamp it on every event of that append the way
`OccurrentCloudEventExtension` already stamps a position, return it from `WriteResult` and `DcbAppendResult`, and
let a recording projection track which identifiers it has applied. The wait becomes "has this projection applied
my append," a membership question rather than an ordering one, so it needs no assumption about commit order,
delivery order, or position order, and it is the mechanism a server-side-filtered subscriber can actually answer,
since membership does not require observing the events that were filtered out. It is also the mechanism no
delivery source's fast writer-side counter can defeat, portable unchanged to a future PostgreSQL or MySQL store,
where `BIGSERIAL` and `AUTO_INCREMENT` are non-transactional for the same reason and suffer the identical
position-versus-commit-order gap. This is the direction recorded for a follow-up epic
([#740](https://github.com/johanhaleby/occurrent/issues/740)), not built here, because it needs a replay
membership rule (a full replay would otherwise have to track every historical append id), a stated migration story
for `WriteResult` and `DcbAppendResult` construction outside this repository, and it reaches the cloud-event
extension and all four event stores rather than one DSL module.

## Consequences

- 0.33.0 ships no read-your-writes primitive for asynchronous projections. Nothing regresses, because no shipped
  configuration of the withdrawn feature was ever sound.
- [ADR 111](0111-a-projection-records-the-position-it-has-applied.md) is marked withdrawn, with amendment
  blockquotes at its Status section and at decisions 5 and 7 pointing here, rather than rewritten silently.
- [#361](https://github.com/johanhaleby/occurrent/issues/361) reopens, and
  [#740](https://github.com/johanhaleby/occurrent/issues/740) tracks the append-identity design that resolves it.
- Occurrent's own ADR corpus already forbids the mistake this feature made: ADR 21 separated a commit-order token
  from `long` at the type level for exactly this reason, on the write path. This ADR is the read-path correction to
  the same principle.
- A store that later wants dense, commit-ordered positions as a documented capability is free to add
  `positionsAreCommitOrdered()` when a real use for it lands. This ADR does not build it speculatively.
