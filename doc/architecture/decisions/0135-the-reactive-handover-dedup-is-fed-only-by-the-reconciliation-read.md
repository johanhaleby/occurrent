# 135. The reactive handover dedup is fed only by the reconciliation read

Date: 2026-08-22

## Status

Accepted. Supersedes the section "The one divergence from blocking" in
[ADR 38](0038-reactive-dcb-catch-up.md), which decided the opposite and has shipped, so it is corrected here rather
than edited. Resolves [#891](https://github.com/johanhaleby/occurrent/issues/891) and removes one of the two losses
[ADR 132](0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md) recorded as remaining.

## Context

A projection declared `@Projection(recordAppliedAppends = true)` on the reactor stack could update its read model
from an event and never record the append that event came from, so `AppliedAppendStore.waitUntilApplied` kept
answering `false` for a write that had committed until the caller's wait timed out.

The reactive catch-up reads the head first, then reads the events up to that head in windows, which is the history
replay, then reconciles up to a freshly read head. ADR 132 decision 6 records nothing during the history read and
records everything after it, because for some of what follows the catch-up is the only delivery there will ever be.

A position is reserved before its write commits ([ADR 84](0084-what-a-position-guarantees.md)), so a write can hold
a position at or below that first head and still commit after the head was read. The window reads all run after the
head read, so a history window reads that event. The projection updates its read model from it while it is still
replaying, and nothing is recorded. `PositionCatchupPipeline` then added every id it emitted, the history windows
included, to the same cache the live delivery filters on, so the change stream's own delivery of that event was
dropped and no second delivery was left to record it on.

The blocking stacks do not have this. Their history reads pass a null cache and only their reconciliation reads fill
it, so a second delivery follows and records the append.

### Why the divergence existed

ADR 38 introduced the reactive caching deliberately, in a section titled "The one divergence from blocking", on this
ground: "The blocking catch-up caches only the reconciliation tail, because its live subscription resumes after the
bulk head. The reactive `globalSubscriptionPosition()` token resumes inclusively, so the live change stream
re-delivers boundary events the replay already emitted."

That ground does not describe the code as it now stands, and this ADR claims only the present tense. Whether it
described the code in June 2026 could not be established, because the blocking DCB model's history does not reach
past `8a9311fd6`, which added the whole `catchup-subscription` module as new files, and following renames finds
nothing earlier. So this is a decision overtaken or a decision mistaken, and the evidence available does not say
which.

What the code does now, verified rather than inferred:

- The blocking position path and the blocking DCB path capture a global checkpoint before their bulk replay and
  resume live from it, exactly as the reactive one does, and both still pass a null cache for their history windows.
- The blocking time path captures its checkpoint after the bulk replay instead, deliberately, so the token cannot
  age out of the oplog during a long replay, and `StreamCatchupSubscriptionModel` says so where it does it. There no
  history event can be in the live stream at all, so its null cache costs nothing, and the events written during its
  replay come back through the count-based delta, which does fill the cache. Different mechanism, same outcome. A
  reader checking that path should not read its ordering as a counterexample to the two above.
- All three shipped Mongo subscription models take that checkpoint as the server operation time with its increment
  raised by one, and on that branch an event already committed when the checkpoint was captured is outside the live
  stream's range. The exception is a server that prohibits the `hostInfo` command, which a shared Atlas cluster
  does. There the models answer null or empty instead, and the catch-up either fails loudly or falls back to the
  caller's own start position, so the plus-one reasoning does not apply to that branch at all.

The repository also contradicted itself in writing. `PositionCatchupPipeline`'s own javadoc justified caching the
replayed ids, the bulk tail included, because "the reactive global position resumes inclusively and the live change
stream re-delivers boundary events already emitted". `startAtOperationTime` is indeed inclusive. The plus-one is
what moves the start strictly past every already-committed event, which that sentence does not mention, and it is
the whole of why the boundary event it worries about is not re-delivered.

ADR 38's own text concedes the rest. It says bulk events far below the token are never re-delivered, and that the
only ids the cache usefully holds are the boundary events and the commits made during the replay near the
head. Those commits are precisely the appends #891 loses.

## Decision

**The history windows fill no dedup cache. Only the reconciliation read does.**

`PositionCatchupPipeline.windows` takes a nullable cache, the two history calls pass null, and the reconciliation
call passes the cache. That is the whole change, and it makes the reactive pipeline fill its cache from the same
read the blocking one does.

The invariant it establishes, stated over interleavings rather than as a description of the change:

> The handover dedup cache only ever suppresses a live delivery of an event that a recordable read already
> delivered.

Equivalently, for a catch-up that captures its live resume checkpoint, then reads its bulk head, then reads history
windows, then signals that the history has been read, then reconciles, then runs live. If an event was committed
after the checkpoint was captured, at least one delivery of it reaches the projection in a phase where the recorder
is recording. An event committed at or before the checkpoint is history by construction, and ADR 132 already states
that a wait for an append applied before a reset times out.

### It does not decide by position

Nothing here compares a position to decide whether an append was applied, which is what
[ADR 122](0122-an-applied-position-is-not-a-completed-prefix.md) refutes. The change removes a suppression and adds
no predicate. The position bounds that remain decide what to read, never what to record, which is the classification
ADR 132 sanctions when it says a delivery is classified by which read produced it and that every closure classifying
by position ran into what ADR 122 refuted.

Put sharply, ADR 84 says a position is not commit order, so the history read's upper bound is wrong about which
events are history, and ADR 122 says MongoDB exposes no in-flight transaction boundary that could make it right.
This does not try to make the bound accurate. It makes the bound's inaccuracy harmless, by refusing to let a read
that may have been wrong about an event remove the only delivery that could have recorded it.

### It does not record during a replay

ADR 132 decision 6's three reasons stand untouched. A full replay still inserts nothing, so the volume argument is
unaffected. A coalescing view still buffers only during a replay in which nothing is recorded. An abandoned replay
still has nothing recorded to become untrue. The extra record is written by the live subscription, after the
history-read signal, in the phase that already records, and decision 7's clear precondition applies to it as it does
to any other live delivery.

## Consequences

- An append whose position was reserved before the replay read the head, and which committed after it, is delivered
  again by the live subscription and recorded there. `waitUntilApplied` answers for it instead of timing out.
- That same event updates the read model twice. The set is exactly the events a history window read whose commit
  came after the resume checkpoint, so the cost equals the benefit and reaches nothing else. In a store with no
  concurrent writes it is empty. Delivery here is at-least-once already, and this composition re-delivers a whole
  replay when a stopped catch-up is started again.
- The reactive catch-up models shipped in 0.31.0 and 0.32.0, so this changes observable delivery for callers who
  never touch `recordAppliedAppends`. It stays inside the at-least-once contract, so it needs no migration recipe.
- `handoverCacheSize` now sizes the reconciliation overlap alone, which is what the blocking `cacheSize` already
  means and what the property's own javadoc already claimed. Under the previous behaviour a rebuild larger than the
  cache filled it with history ids and evicted them again, so the caching ADR 38 asked for did not happen at all for
  any replay longer than the cache.
- The two stacks converge. Teaching the blocking stacks the reactive caching instead would reintroduce #891 on three
  more paths and was not a candidate.
- The blocking stacks need no change and get none.

### Limits of the evidence

Stated rather than left to be rediscovered, because the cost analysis rests on the exclusivity of that plus-one
checkpoint.

- It was reproduced against a single-node replica set. Under a secondary read preference or a sharded `mongos`,
  `operationTime` can lag committed oplog entries, which is the one case that would break the inference. This ADR
  claims the single-primary case and no more.
- Nothing in the repository tests the exclusivity direction. The TCK conformance test asserts that a written event
  is present, never that a pre-checkpoint event is absent.
- The regression coverage is a pipeline unit test and a Mongo test that combines a small handover cache with a write
  committing during the replay, the case nothing covered before. Both reproduce the race by having the reader
  report a head above the last committed position, which ADR 84 permits since `currentPosition()` is a
  high-watermark. Neither drives a genuinely uncommitted write across a head read the store actually performed,
  which would need a
  transaction held open across it.

### Alternatives rejected

- **Record from the history read for the ambiguous events.** Needs to tell an ambiguous event from a genuine history
  event, which can only be done by comparing positions. ADR 122 refutes it.
- **Make the history read's upper bound accurate.** Needs a queryable in-flight transaction boundary. ADR 122
  established MongoDB exposes none, which is why it ruled out the completed-prefix family.
- **Keep the suppression but make it record-aware,** letting a live delivery of an already applied event record
  without updating the read model. It costs no duplicate update and is the cleaner answer in the abstract, since the
  projection genuinely did apply that event. It needs a new channel from the subscription layer to the projection
  layer meaning "already applied, record only", which is new public API, and it changes decision 6's rule about what
  may be recorded. Disproportionate to a defect this size.
- **Drop the cache only on the named path,** keeping it on the cold `catchup(..)` entry point. It would leave the
  two entry points of one class disagreeing about at-least-once for no reason a reader could derive.
