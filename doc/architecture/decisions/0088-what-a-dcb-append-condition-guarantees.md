# 88. What a DCB append condition guarantees, and the one thing it does not

Date: 2026-08-02

## Status

Accepted. Part of #394, TCK phase 4.

## Context

DCB is the biggest block of untested-in-common behaviour left in #394: roughly 5,300 lines of DCB test code across the
four stores, with the largest test files in the repository sitting in it. Deduplicating that is worth doing on its own,
but DCB is also the first phase where the stores disagree about *behaviour* rather than about representation, so the
question the TCK has to answer first is which disagreements are contract and which are variation.

Two of them are real, and they are not independent. Occurrent has two ways of answering a token-qualified
`DcbAppendCondition`:

- The in-memory store compares the events committed after the token against the condition's criteria, exactly as a read
  would.
- The three MongoDB stores compare version markers kept per consistency boundary, so the check is a single conditional
  write rather than a scan (ADR 21).

The marker model is coarser, and coarseness cuts both ways:

| Scenario | Exact criteria | Tag markers |
|---|---|---|
| An excluded type sharing a boundary tag, committed after the read | no conflict | conflicts |
| `wholeStoreLock(token)` against a later tag-scoped append | conflicts | no conflict |

The first is a false conflict, sound and self-healing: the application service re-reads the still-excluded boundary and
retries. The second is a false pass, which is the limitation `wholeStoreLock()` already documents in as many words and
the reason it is correct only for a single writer or an empty-store guard (ADR 30).

A third divergence looked like a candidate and is not one. In-memory advances its position counter only on commit, so a
rejected append consumes no position, while the MongoDB stores reserve a block outside the write transaction and
abandon it, leaving a permanent gap. Three per-store tests pin their own answer to that.

## Decision

**One fixture declaration, `appendConditionModel()`, returning `EXACT_CRITERIA` or `TAG_MARKER`.** The suite asserts
the documented outcome for whichever is declared, and both branches run in this repository, so neither is a claim
nobody checks.

One declaration rather than one per symptom, because the two symptoms are one fact seen from either side. A model that
is coarse over-approximates for a criteria narrower than its boundary and under-approximates for a boundary wider than
the markers a scoped append touches. The two constants therefore give opposite answers to both rows of the table above,
which is why one answer settles them both. It is an enum rather than a boolean because both models deserve a name, and
it has no default because there is no answer that is right often enough to inherit: getting it wrong makes the suite
assert the opposite of what the store does.

**It is a declaration rather than a question put to the store**, which is the side of ADR 84's line that
`timePrecision()` sits on. The model is a property of how the write path is built and no method on `DcbEventStore`
reports it, so there is nothing to ask. The rule stands unchanged: declare what cannot be asked, ask everything else.

**The position-gap divergence gets no declaration at all.** ADR 84 already settled that a position is positive, unique
and strictly increasing, and explicitly not dense. A gap after a rejected append is that contract being honoured, not
a variation on it, so the suite asserts neither outcome and asserts contiguity only *within* one append, which is what
`DcbAppendResult` actually promises. A fixture flag here would have recorded two stores' implementation details as if
they were behaviour.

**The reactive bridge learns DCB.** `BlockingEventStoreOverReactive` already presents the reactive stream API as a
blocking one so the behavioural suites are written once; it now presents the reactive `DcbEventStore` the same way. The
two APIs mirror each other method for method and share every value type, so the delegation is one to one.

This is not tidiness. The reactor store shares the marker model with the two MongoDB stores that are thoroughly
tested, and is itself the least tested of the four: no type-versus-tag skew case, no multi-marker boundary, no
tokenless guard, and same-boundary contention at 3 iterations against the other two stores' 50. Running it through the
shared suite closes that in one move rather than by hand-writing the missing tests a second time.

**A second suite, `DcbStreamInteropConformance`, requires STREAM and DCB together.** Six assertions need both
capabilities: that a DCB read never returns a stream-written event and vice versa, that a stream write refuses an event
carrying DCB tags, that one global position sequence covers both modes, and that a condition with no token means
"currently exists" rather than "ever appended", which can only be shown by deleting the match. Requiring the pair in
its own suite keeps `DcbEventStoreConformance` runnable by a DCB-only store, and keeps a store's reason for not running
these a visible missing subclass rather than a skip.

## Consequences

A store's fixture now answers one more question, and answering it wrong is a loud failure rather than a silent one: the
suite asserts the other model's outcome and the assertion message names which model it expected. A store that changes
its write path has to change the declaration in the same commit.

The list of things the suite deliberately does not assert is longer here than in any earlier phase, and it is written
into the suite's own javadoc rather than only here, because several of the omissions are tempting:

- **A consistency token's value.** Opaque and store-internal, so it is round-tripped and never read. Two tokens from
  the same store are compared for equality where the contract promises they are equal, which is a different thing from
  interpreting one.
- **`lastSequencePosition` as a concurrency boundary.** It is the store's DCB head, not the highest matched position,
  and it reports the head even for a read that matched nothing. A store assigning positions before its events commit
  can report a head ahead of what a reader can see, which is why the token type exists at all.
- **Which storage stream a DCB event landed in.** Placement is derived from tags and `DcbStreamIdGenerator`'s own
  javadoc calls it a storage placement choice, not part of the DCB contract.
- **How `exists` and `count` are implemented.** Both document a full read as their default and invite an implementation
  to do better, so asserting call counts or efficiency would pin the default rather than the contract.

Left per store on purpose, unchanged from earlier phases: Mongo index and explain-plan assertions, transaction
ownership (ADR 74), driver exception translation, the read-watermark and unconditional-marker regression tests, which
are regressions against the marker implementation rather than against the contract, and the capability-gating tests
that belong to phase 5.

One thing the suite does assert verbatim that a reader might expect it to loosen: the message on
`DcbAppendConditionNotFulfilledException`, and the message a stream write produces when handed a DCB-tagged event. Both
are fixed literals with nothing interpolated, and both are built independently in four places while coming out
identical, so a store drifting away from either is worth a failing test. That is the same call made for
`WriteConditionNotFulfilledException` and the opposite of the one made for `DuplicateCloudEventException`, whose
message carries raw driver text on the MongoDB stores and nothing in memory.
