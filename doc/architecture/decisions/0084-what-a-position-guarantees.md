# 84. What a position guarantees, and what it does not

Date: 2026-07-31

## Status

Accepted. Adds the position conformance suites to the TCK ([ADR 77](0077-a-published-tck-for-occurrent-contracts.md))
and settles a contract that three shipped test files contradicted. Resolves #485 and #486.

## Context

[ADR 45](0045-unified-global-position.md) made `position` a first-class property of every event and gave stream and
DCB one shared ordering axis. It did not say, in one place, what a caller may rely on. The answer was spread across
`DcbAppendResult` javadoc, two older ADRs, and a comment in one store, and the tests disagreed with each other.

All four stores have a hand-written `*PositionTest`, 47 tests between them. Three of the four assert that positions
come out contiguous from 1:

```java
assertThat(firstStreamEventPosition).isEqualTo(1L);
assertThat(dcbEventPosition).isEqualTo(2L);
assertThat(secondStreamEventPosition).isEqualTo(3L);
```

The native-driver test refuses to, and explains why in a comment citing ADR 0021: a retried write may reserve and
abandon a block, so only strict monotonic ordering is guaranteed.

The native one is right, and the API already said so. `DcbAppendResult.java:24-27` states that "Across separate
appends only the relative ordering of positions is guaranteed, so callers must compare positions for ordering and
must not assume the positions of different appends are contiguous."

The three over-specified files pass only because a sequential test with no contention never abandons a block. That
was measured rather than assumed: writing an event, then a write rejected by its write condition, then another event
produces positions 1 and 3. The rejected write reserved position 2 at `MongoEventStore.java:288`, before the
condition is checked, and abandoned it permanently.

## Decision

State the contract once, assert exactly it, and assert nothing stronger.

**A position is positive, unique, and strictly increasing.** A later write gets a higher position than an earlier
one. That is the whole guarantee for ordering.

**A position is not dense.** Gaps are permanent and expected. The block is reserved outside the write transaction so
the shared counter does not become a transaction conflict, which is the same reason it cannot be gap-free: a write
that never commits has already taken its numbers.

**A position is not commit order.** Reservation happens before commit, so two concurrent writers can commit in the
opposite order to their positions.

**`currentPosition()` is a high-watermark, not a fence.** It may be ahead of what a reader can currently see, so the
suite never asserts it equals the last visible event's position under concurrency. In a sequential test it asserts
only that it is at least the highest position written.

The suite therefore reads actual positions off the events and compares them to each other. It asserts no literal
position value anywhere, which is the mechanical rule that keeps the contract honest.

### The suite asks the store whether it writes positions

`EventStoreFixture.writesPosition()` is deleted. `writesPosition()` is a real predicate on `PositionOrderedReader`,
and every fixture already hands the suite the store that answers it.

A declaration here could also be wrong, which is the stronger argument. ADR 45 has a MongoDB store turn position off
by itself at startup when it finds events written before position existed, so a fixture declaring `true` can describe
a store that reports `false`.

This is not in tension with `EventStoreCapability`, which stays declared. That one is construction-time config the
store does not expose. Position is the opposite, and the rule is worth stating plainly: **declare what cannot be
asked, ask everything else.**

Covering the position-off branch then needs a factory rather than a flag. `storeWithoutPosition()` returns an empty
`Optional` by default, and an implementation opts in by building one. It returns both the `EventStore` and the
`PositionOrderedReader` view, because `EventStore` does not extend `PositionOrderedReader` and a suite that
downcast would throw `ClassCastException` on an implementation where those are separate objects.

### One backfill message, and two different ones

The startup guard's message was written out by hand in each store and drifted, so the same situation produced
"position backfill migration" in one and "position-backfill" in another. It now lives in one place.

The warn path and the fail path deliberately say different things. The store starts on the warn path, so telling the
reader it "is configured to require backfilled positions" would be false, and advising them to turn that setting off
would be advice about a setting that is already off. The warn message names what is silently lost instead, that
position-ordered reads and catch-up skip events without a position. Both name the runbook.

## Consequences

Anyone implementing an event store outside this repository now has one statement of what a position must do, and the
suite fails an implementation that provides less. It equally does not fail one that provides more: the in-memory
store happens to be dense, because it assigns from an `AtomicLong` inside its critical section, and that is allowed.
Callers still must not rely on it.

The three over-specified tests are corrected rather than deleted, and the correction is the case where changing a
test is right: shipped javadoc proves the assertion claimed more than the contract does. A reader comparing the four
files can now tell that contiguity is an accident rather than a promise.

What this does not settle: whether stream and DCB events share one sequence is asserted only by per-store tests
until the DCB fixtures arrive, so those tests stay. Mongo index creation, the startup auto-disable and the backfill
guard's warn-versus-fail behaviour also stay per store, since they are storage-specific rather than contract.
