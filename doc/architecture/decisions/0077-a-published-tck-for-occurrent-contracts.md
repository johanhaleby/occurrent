# 77. A published TCK for Occurrent contracts

Date: 2026-07-28

## Status

Accepted. Landing incrementally, see the module layout below for what ships in this pull request
versus what follows with #395.

## Context

Occurrent has four event stores and several subscription models, each covered by its own
hand-written tests for what is substantially the same contract. Measured on `main` at `5f8a7ac8d`:

| Store family | Test LOC | `@Test` in the main class | `@Nested` groups |
|---|---|---|---|
| `eventstore/mongodb/spring/blocking` | 4616 | 88 | 24 |
| `eventstore/mongodb/native` | 4396 | 86 | 23 |
| `eventstore/mongodb/spring/reactor` | 3031 | 86 | 23 |
| `eventstore/inmemory` | 2298 | 81 | 23 |

That is roughly 14 300 lines of test code with no shared base class anywhere. The near-identical
`@Test` and `@Nested` counts are the signature of hand-copied variants of one suite, not four
independently designed ones, and nothing in the repository proves the four stores actually agree
on the contract they are each separately asserting.

The subscription side has the opposite shape: uneven coverage rather than duplication.
`SpringMongoSubscriptionModelTest` has 33 tests, the native model 25, the in-memory model 13, and
`ReactorMongoSubscriptionModelTest` 8. A shared suite there raises the floor rather than deleting
duplication.

A third requirement, beyond deleting duplication and proving agreement, is packaging. An event
store or subscription model living outside this repository has no way to run the same contract
tests, because `test-support` is unpublished. This is tracked under #393 (umbrella), #394 (event
stores), #395 (subscription), and #396 (whatever divergences the suite exposes once it runs
everywhere). It also absorbs and closes #75, the original request for shared event-store test
infrastructure.

## Decision

**A TCK exists as a published artifact, not as an internal refactor of `test-support`.**
`test-support` stays unpublished and keeps its existing role. The TCK is new, separately published
modules whose only purpose is to be depended on by an implementation this repository does not
contain.

**Suites are abstract classes over a fixture, not JUnit 5 test interfaces.** `@Nested` cannot exist
in an interface, and the tests being replaced carry 23-24 nested groups each, and losing that grouping
would cost report legibility and diff legibility that the current suites already rely on. Each
suite is `protected abstract class FooConformance` with `protected abstract FooFixture
createFixture()`, and an implementer writes one small subclass per capability suite plus one
fixture class, mirroring the existing `*Test` / `*DcbTest` / `*PositionTest` / `*CapabilityTest`
layout so the diff against today's tests stays legible. The cost of this choice, paid deliberately,
is that an interface's other property is lost too: with `implements FooConformance,
BarConformance`, an unsupported capability would be a compile error from a missing method
implementation. An abstract class gives up that compile-time guarantee. Capability coverage is
enforced at runtime instead, by the anti-silent-skip rules below. The rejected alternative was test
interfaces composed with `implements`, which was ruled out specifically because of the `@Nested`
incompatibility, not the capability-omission property it would have kept.

**The reactive event store is covered through a blocking bridge the TCK ships, plus a small
reactive-only contract for what a bridge hides.** The behavioral suite is written once, against the
blocking `EventStore` contract, and an adapter in the TCK wraps the reactor `EventStore` behind it.
A separate, small suite covers exactly what a synchronous bridge cannot observe: errors arriving as
`Mono.error` rather than being thrown, nothing happening before subscribe, and cancellation.
Rejected alternatives: twin suites written against a shared scenario description, which is a second
test framework built to avoid writing a bridge, and a hand-written reactor suite parallel to the
blocking one, which re-creates exactly the duplication this ADR exists to remove.

**One published artifact per contract family, under a top-level flat `tck/` aggregator.** The
decided layout is five leaves:

| Directory | Artifact | Ships |
|---|---|---|
| `tck/common` | `occurrent-tck-common` | this pull request |
| `tck/eventstore-blocking` | `occurrent-tck-eventstore-blocking` | this pull request |
| `tck/eventstore-reactor` | `occurrent-tck-eventstore-reactor` | this pull request |
| `tck/subscription-blocking` | `occurrent-tck-subscription-blocking` | with #395 |
| `tck/subscription-reactor` | `occurrent-tck-subscription-reactor` | with #395 |

Only the first three exist as of this ADR. The two subscription artifacts are decided layout, not
yet-built modules, and arrive when #395 lands. The reason for five separate artifacts rather than
fewer is that a store implementer must not be forced to drag the reactor API or the subscription
API onto its compile path just to run the blocking event-store suite. This matches the repository's
existing convention of small capability modules over one monolith. The rejected alternative was a
single `occurrent-tck` covering everything, which fails that test the moment a blocking-only store
implementer inherits reactor and subscription dependencies it has no use for.

**Naming is `occurrent-tck-*`, not `occurrent-eventstore-tck-*`.** ADR 55 established the
`occurrent-` prefix on every published leaf but does not say anything about where a TCK's name
should sit relative to the subsystem it tests, so that choice is recorded here. The five artifacts
group together under one `occurrent-tck-` stem for anyone searching Maven Central for the TCK,
rather than being scattered across `occurrent-eventstore-*` and `occurrent-subscription-*`. A TCK
is also a different class of artifact from the runtime API surface it tests, one an implementer
depends on only in test scope, and its name should say so rather than pretend to be another store
or subscription module. ADR 55's standing rule, that every new published leaf gets the
`occurrent-` prefix, is honoured, and only the position of `tck` in the name is new.

**Suites ship from `src/main/java`, and the TCK exports its test framework at compile scope.** No
module in this repository produces a `test-jar`. `test-support` already carries
`junit-jupiter-engine` at compile scope for exactly this reason, because a JUnit 5 base class has to
be visible as ordinary production code to whatever depends on it. The TCK follows the same shape,
with one difference from `test-support`: it is published. The consequence, stated plainly because
it is a real cost and not a detail: a consumer of `occurrent-tck-eventstore-blocking` gets JUnit
5.11.3 and AssertJ 3.27.4 on its compile path, and a consumer of the subscription TCK additionally
gets Awaitility 4.2.2. That is inherent to the design, not an oversight to fix later, because the
suite's public surface *is* JUnit annotations. There is no way to expose an abstract `@Test`-bearing
class without exposing JUnit. This triggers the bookkeeping AGENTS.md already specifies for any new
publishable leaf: each of the three (eventually five) TCK leaves is added to `bom/pom.xml`'s
`<dependencyManagement>` and must **not** appear in `<excludeArtifacts>`, since a leaf is published
by default. The `tck` aggregator itself is a `pom`-packaged grouping module with no artifact of its
own, so it **must** appear in `<excludeArtifacts>`, the same way every other aggregator in this repo
does.

**Capabilities are declared by the fixture, because nothing on the API reports them.**
`EventStoreCapability` is a construction-time argument to `EventStoreConfig`. There is no
`capabilities()` accessor on `EventStore` for a suite to query at runtime. The fixture SPI therefore
declares what the store under test supports, and hands the suite typed accessors rather than one
`Object` the suites downcast:

```java
public interface EventStoreFixture {
    Set<EventStoreCapability> capabilities();

    EventStore eventStore();
    EventStoreQueries queries();
    EventStoreOperations operations();
    ReadEventStreamWithFilter filteredReader();
    DcbEventStore dcbEventStore();            // required iff DCB is declared
    PositionOrderedReader positionOrderedReader();

    default void close() {}
}
```

This is the shape at the time of this decision. The interface has since grown more default-valued accessors as new
capabilities and store variations were declared, and `writesPosition()` moved from here onto
`PositionOrderedReader`, which `positionOrderedReader()` already exposed.

Every one of the four existing event stores already implements all of these interfaces on a single
object, so a concrete fixture is one line per accessor. The fixture contract is that
`createFixture()` returns a store containing no events. Each implementation was responsible for its
own cleanup between tests, which is what `FlushMongoDBExtension` did at the time. [ADR 97](0097-a-test-container-owns-its-database.md)
later deleted it in favour of the published `OccurrentMongoFlush`.

**Four rules stop the suite skipping silently.** #393 is explicit that a suite quietly testing
nothing is worse than no suite at all, and that principle is load-bearing enough to state as four
separate rules rather than leave implicit in the fixture design:

(a) A suite fails fast, in `@BeforeEach`, if the fixture does not declare the capability that suite
requires. Running `DcbEventStoreConformance` against a fixture that only declares `STREAM` is an
error the moment the test starts, not a runtime skip.

(b) **`Assumptions` are banned in the TCK.** Every optional behavior is a fixture-declared branch
that asserts the documented behavior on both sides, never a call that skips the test when a
capability is absent. `writesPosition() == false` does not skip the position-reading tests. It
asserts that `currentPosition()` and `readInPositionOrder` fail in the way that behavior is
documented to fail.

(c) `CapabilityGuardConformance` verifies the negative directly: a store that does not declare DCB
rejects DCB calls, and a DCB-only store rejects stream writes. This is what
`MongoEventStoreCapabilityTest` already covers for two of the four stores today, generalized into a
suite that runs against all of them.

(d) Choosing not to run a suite against an implementation is the visible absence of a subclass, a
greppable and deliberate act, rather than something a test run can decide quietly on its own.

**One async waiting convention, owned by the subscription TCK rather than `occurrent-tck-common`.**
ADR 94 supersedes this paragraph in part: the factory below still ships, but as the fallback rather
than the default, behind a deterministic start position and a start barrier. The reason is
correctness rather than tidiness, and ADR 78 had already recorded it.

There is no such convention today. Across the `subscription/` modules: 202 `await()` call sites in 30
files, 47 of them with no `atMost` and so inheriting Awaitility's 10 second default, `atMost` values
of 1, 2, 4, 5, 10 and 40 seconds, four files each declaring their own `AT_MOST` constant, and three
places using a bare `Thread.sleep(200)` with a comment apologizing for it. (Re-measured while phase 6
was written, and scoped explicitly this time. The counts this ADR first carried, 260 sites across 27
files with a 1 to 30 second spread and three constants, had drifted.) The subscription TCK ships
`Conformance.await()`, a configured Awaitility factory, and
`awaitNothing()` for the must-not-arrive case that today is spelled out by hand as
`await().during(ofMillis(200)).atMost(ofSeconds(2))`, plus a fixture-supplied multiplier so a slow
subscription model can widen the budget without inventing its own constant. Ownership sits with
`occurrent-tck-subscription-blocking`, not `occurrent-tck-common`, so that Awaitility does not land
on the compile path of an event-store implementer who has no async waiting to do. That leaves
`occurrent-tck-common` holding only what both the event-store and subscription sides need: the
shared CloudEvent fixtures, self-contained rather than pulling in a JSON library, matching the shape
the DCB tests already use (`event(String type)` with a fixed source and `"{}"` data, and
`taggedEvent(String type, String... tags)` over `DcbCloudEvents.withTags`).

**Scope boundary: the TCK tests contracts, not implementations.** Nothing MongoDB-specific moves
into a suite. Index creation and absence, `TimeRepresentation`, transaction and `ClientSession`
behavior, change-stream error codes, oplog details, the startup backfill guard, Bson and Json filter
specifications, and the in-memory-only write `Listener` all stay in their own modules' own tests.
A divergence the TCK exposes between implementations becomes a child issue under #396, labelled bug
or documented variation. It is never resolved by loosening an assertion in the shared suite.

## Consequences

Running the event-store suites against every implementation closes coverage gaps that exist today
for free, because these sibling test classes do not currently exist:
`InMemoryEventStoreCapabilityTest`, `InMemoryEventStoreDcbConcurrencyTest`,
`ReactorMongoEventStoreCapabilityTest`, `ReactorMongoEventStoreDcbConcurrencyTest`, and
`ReactorMongoEventStoreDcbUnconditionalMarkerTest`.

The subscription suites, once they land with #395, add CI wall-clock time rather than removing any,
because subscription coverage today is uneven rather than duplicated: 33 tests for the Spring Mongo
model, 25 for the native model, 13 for in-memory, and 8 for the reactor model. Raising all four to
the shared suite's floor is real additional test time against a pinned shard count of 10 and a
`SHARD_BUDGET_SECONDS` of 240. That cost is accepted because the alternative is leaving three of the
four models under-tested indefinitely.

The anti-silent-skip rules mean a missing suite subclass is discoverable by grep rather than by a
green build that quietly tested nothing, and any behavioral divergence the suites find is routed to
#396 as an issue rather than papered over in the shared assertion.
