# 80. What concurrent writes to one stream guarantee

Date: 2026-07-30

## Status

Accepted. Adds `StreamConcurrencyConformance` to the TCK ([ADR 77](0077-a-published-tck-for-occurrent-contracts.md)) and
changes the reactive event store's write path, which has shipped. Resolves #467, #474 and #475.

## Context

Optimistic concurrency is the reason an event store takes a `WriteCondition` at all, and until now it was the least
verified part of the write path.

Five tests covered it, all inside `ParallelWritesToEventStoreReturns` in the three MongoDB store test classes, and every
one of them carried `@EnabledOnOs(MAC)`. Every CI shard runs `ubuntu-latest`, so those five ran on one laptop and
nowhere else. The in-memory store had no such test at all.

Nothing records why the gate went on. Not a commit message, not `changelog.md`, not this repository's docs, not an issue
or a pull request. What survives is a sequence from 2021-04-16: the test arrived as a plain `@Test`, hours later it
became `@RepeatedIfExceptionsTest(repeats = 10)` under the message "Hopefully fixing race conditions in test", and hours
after that the gate appeared while repeats dropped to 5. A retry escalation that did not settle it, then a gate.

The premise a reader would assume, green on CI and red locally, is the wrong way round. CI in April 2021 was already
`ubuntu-latest` and already ran these tests under Surefire, so the gate excluded CI from the moment it was written. The
two later occurrences (2022 and 2024) copy the annotation from the sibling above them. The DCB concurrency tests use the
identical replica-set fixture, are not gated, and pass on Linux CI today.

Writing the suite then turned up two defects that the gate had been hiding, both fixed here.

`ReactorMongoEventStore.write` had no retry on a version race, so two concurrent writes under `anyStreamVersion()` could
leave one caller with a `WriteConditionNotFulfilledException` that the condition promises cannot happen. Both blocking
stores retry. Commit `fe99e0bdf` names only those two, so the reactive store was a known omission from October 2024
rather than an oversight here.

And the two DCB concurrency test classes joined `futA.get()` before `futB.get()`, so a throw from the first abandoned
the second join while `shutdown()` left that worker running. The test method then returned with a thread still appending
into a store the next class asserts on. Every wait in both files was unbounded, backed only by a class-level
`@Timeout(180)`, which reports a hang after three minutes without saying which worker wedged.

## Decision

State the contract in one place, assert it in the TCK against all four stores, and fix whatever fails.

**A conditional write from two threads leaves exactly one winner.** The loser gets
`WriteConditionNotFulfilledException` and writes nothing. Asserted on a count, not on `contains`, because `contains`
passes on a store that wrote both.

**An unconditional write does not fail on a version race, for up to 15 retries.** `anyStreamVersion()` means a write
that loses a race retries rather than surfacing the conflict, with backoff, up to those 15 attempts. The suite
exercises 6 threads racing on one stream over 5 iterations, which 15 backed-off retries clear easily, so every writer
succeeds and every event is present exactly once. A stream with more contention than that can clear still exhausts the
retries and throws.

The reactive store gets the retry the blocking ones have, routed through the existing
transaction-ownership check ([ADR 74](0074-retry-only-where-the-transaction-is-owned.md)) so it only retries a
transaction it opened.

### The in-memory guarantee comes from `Map.compute`, not from the surrounding block

Worth writing down, because the obvious reading is wrong and this was checked by breaking it rather than by reading it.

`InMemoryEventStore.write` holds `synchronized (state)` around a `state.compute` call. Removing that block does not
break exactly-one-winner: `state` is a `Collections.synchronizedMap`, where `compute` is already atomic on its own, and
the loser writes nothing because its `WriteConditionNotFulfilledException` is thrown from inside the remapping function,
before `compute` can install a value. The outer block is what makes the read-then-write of the position counter atomic
with the append, and in `delete(Filter)` it is what guards iterating a view of the map, which `compute` does not cover.

So the property that makes the suite pass is narrower than the code's shape suggests. A future reader trimming the
`synchronized` as redundant would keep this test green and break position assignment.

### Every wait in the TCK is bounded, because the shards have no retry

The barrier and every join take a timeout, every future is joined before anything is asserted, the pool is shut down
with `shutdownNow()`, and `ExecutionException` is unwrapped so an assertion sees the store's exception rather than the
executor's wrapper. The discipline is taken from `SpringMongoMaterializedViewConfigTest.kt`, which already had it.

This is not tidiness. The eventstore shards deliberately carry no `surefire.rerunFailingTestsCount`, so a flake there is
a red build rather than a silent second attempt, and a suite that hangs costs the shard's whole 20 minute timeout. The
same treatment goes to the two DCB classes, whose abandoned join is a live contamination source in CI today: its symptom
is a failure in an unrelated class, which is close to undiagnosable from a log.

### The gated tests are deleted rather than un-gated

They add nothing the build did not already have, since they ran on no CI platform, and they are weak where they did run.
Both racing threads write one `AtomicReference` with last-write-wins, no thread is joined, the any-version variants
assert `contains` rather than a count, and an Awaitility poll can only fail by timing out.

Three `@EnabledOnJre(JAVA_8)` tests go with them. The project targets 21, so they are dead rather than gated.

A script guards the class of problem instead of the instances: it parses `runs-on` and the `java:` matrix out of
`maven.yml`, parses `@EnabledOnOs` and `@EnabledOnJre` out of `src/test`, and fails naming any test no CI job can run.
It reads the workflow rather than keeping a second list, which is what makes `verify-shard-coverage` durable. Deleting
the eight instances is what lets the guard start from zero instead of shipping with an allow-list.

`SuiteNeverSkipsTest` covers the new suite too, since a grep cannot see an engine-level skip.

## Consequences

A concurrent unconditional write behaves the same on all four stores now, so code written against the blocking stores
ports to the reactive one without acquiring a failure mode. The retry is bounded (15 attempts, 10ms backoff capped at
500ms), so a genuinely contended stream fails eventually rather than never.

The suite runs 5 iterations per test and passes in under 5 seconds per store, well inside the 240 second shard budget.
`eventstore/mongodb/spring` is the shard to watch as the TCK grows: it is a single path carrying both the blocking and
the reactive store, so if it goes over budget there is nothing to move out of it.

The in-memory store's exactly-one-winner property is now asserted rather than incidental, which is the first test of
concurrent same-stream writes it has ever had.

An implementation outside this repository that cannot retry an any-version write cannot pass the suite. That is
deliberate: the alternative is a fixture flag recording the gap as an intended difference, which is the pattern #470
removed two instances of.

What this does not cover: DCB append concurrency, which the two DCB classes still own. Their waiting discipline is fixed
here because it is a live defect, but they already cover DCB thoroughly on Linux CI, so folding them into the TCK is
deduplication rather than coverage and belongs with the rest of the DCB phase.
