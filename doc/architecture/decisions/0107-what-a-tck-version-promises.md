# 107. What a TCK version promises

Date: 2026-08-07

## Status

Accepted. Closes #581. Amends [ADR 77](0077-a-published-tck-for-occurrent-contracts.md), which decided that the TCK
is a published artifact and settled its layout, naming, publishing and anti-skip rules, and said nothing about what a
version of it promises.

## Context

The TCK ships for the first time with 0.32.0. `git ls-tree -r occurrent-0.31.0 -- tck` is empty, so nobody outside
this repository has a version to bump from yet, and every statement below costs nothing to make today and would cost
a migration to make later. That is the reason to make it now rather than the first time somebody asks.

Nothing anywhere says what a bump may do, and the consequences are structural for a downstream implementer.

**Every added test turns their build red on any bump.** That is what a TCK is for, but it has to be stated, because
the alternative reading is that this artifact is a regression suite which occasionally regresses.

**A new fixture member can be a compile error.** `SubscriptionModelFixture` has five abstract members and three
defaults. Adding a sixth abstract one breaks every implementation at source level, and nothing said whether that was
allowed.

**There is no escape hatch, by design.** `Assumptions` are banned by ADR 77 rule (b), enforced on compiled bytes by
`SkipMechanismScan`, and the `@Nested` suite classes are package-private. An implementer cannot disable one group
while fixing something a bump has just shown to be wrong.

A fourth question turned up while this was written, and it is the same one from the other side. The suites' wait
budgets were fixed constants in five places, so a model that has to reach a broker before it can deliver had no
supported way to pass. Both questions are about what an implementation is entitled to expect from a version of this
artifact, so they are settled together.

Two of those constants carried arithmetic in their javadoc, and both had drifted out of true.
`SubscriptionModelConformance` said its longest test chained four waits. The longest chains twelve:
`stop_pauses_and_start_resumes_every_one_of_several_running_subscriptions` subscribes six times, waiting for each to
start, then waits for a delivery on each of the six. It arrived with #509, after the javadoc was written.
`RestartConformance` said three waits and two rebuilds, where its longest test has four waits and one rebuild.
`Arrivals.awaitAtLeast` and `awaitUntil` return at the deadline rather than throwing, so a slow wait is paid in full.
At the ten second budget that made the first test's worst case 120 seconds against a 60 second class timeout, which
is the `TimeoutException`-instead-of-an-assertion failure the javadoc existed to prevent, live in the suite.

## Decision

**A minor bump may add suites and tighten assertions.** Bumping the TCK can turn a previously green implementation
red. That is the artifact working as a ratchet rather than regressing, and an implementer who goes red has learned
something true about their implementation that nobody could tell them before. The two supported responses are to fix
the implementation, or to stay on the Occurrent version they were on.

Say the second one honestly, because it is weaker than it sounds. Holding the TCK back on its own is not a hatch. Every leaf
declares its runtime dependencies at `${project.version}` and its suites are compiled against that API, so an old TCK
against a newer runtime is a Maven override nothing validates. Staying put means staying put on both.

A patch bump may loosen an assertion that was wrong or flaky. It may not tighten one, because a patch is what a
person reaches for when they want the fix and not the argument.

Rejected: **new assertions only in a major.** Predictable for anyone who bumps often, and it makes the TCK useless as
a ratchet, since a contract gap found after a release would wait for a major and Occurrent has had two majors in
years. Rejected: **opt-in conformance levels**, a fixture-declared level an implementer raises when ready. It
contradicts ADR 77 rules (b) and (d) directly, and ADR 94 already rejected a declaration on exactly this ground, that
a level is a switch for disabling the only test of a property, sitting in the file belonging to whoever changed the
model.

**The fixture SPI does not break at source level in a minor.** Every member added to a fixture in a minor arrives as
a `default`. Where a returned value would be a lie, it arrives as a `default` whose body throws, naming the member
and what made it required, which is what `EventStoreFixture` already does eight times through its `notOverridden`
helper.

State the cost rather than claim this is free. A throwing default converts a compile error, which is immediate and
complete, into a runtime failure of whichever tests reach the member, which is later and noisier. It is the better
trade for a TCK, because the thing being traded away is a compile error in a test-scope dependency and the thing
bought is that an implementer can bump without their build refusing to build at all. It is still a trade.

Note also what does not transfer. `EventStoreFixture`'s throwing defaults name a capability, because `capabilities()`
is what made the accessor required. The subscription fixtures have no capability set, so a throwing default there
names the suite that reached it instead.

Removing a member, changing its signature, or removing a suite is a major bump.

Rejected: **abstract where a default would be vacuous**, keeping ADR 94's rule as the deciding one and paying for it
with a changelog entry and a migration-guide section. `aCheckpointToStartFrom()` stays abstract because it already is
and this is free before the tag, but a future member in its position gets the throwing default, which gets the same
signal without the compile error. Rejected: **no promise at all**, on the strength of AGENTS.md's pre-1.0 rule. That
would leave the artifact whose whole purpose is to state a contract as the one thing here with no stated contract.

**No `@since` tags.** There are none in any `src/main` in this repository and no japicmp, revapi or animal-sniffer to
enforce one. The changelog entry plus `doc/migration/upgrading-to-*.md` is already the record, which is what AGENTS.md
points at. Adopting the convention in one leaf would make it inconsistent with the other 149 packages and it would
drift the first time somebody forgot, with nothing failing.

**There is no way to disable one test group, and that is the answer rather than a gap.** ADR 77 rules (b) and (d)
already settled it. Declining a suite is the visible absence of a subclass, and there is no per-test hatch. What an
implementer does inside their own build to ship red is their build's decision and not conformance this artifact
sanctions.

**A suite's wait budget is declared by the fixture, and the class timeout is the implementer's to raise.**
`SubscriptionModelFixture.deliveryTimeout()` defaults to the ten seconds every model here runs on, and
`ReactiveSubscriptionModelFixture.deliveryTimeout()` to the twenty its own suite used. Both are checked for null and
positive before the first assertion, and neither is capped.

Neither is capped because JUnit's `@Timeout` is `@Inherited` and a directly declared one wins, so an implementer who
declares a longer budget puts `@Timeout` on their own conformance subclass and it overrides the suite's. A ceiling
would have been a number invented here to protect a relationship the implementer can see and control themselves, and
`CompetingConsumerStrategyFixture.timeToConverge()` has been an uncapped fixture-declared budget on a 60 second suite
since the competing-consumer suite was written, validated only for null.

`deliveryTimeout()` carries a value default where `timeToConverge()` is abstract, and the difference is not
inconsistency. A coordination schedule has no answer that is right by default, because it depends on a mechanism the
interface does not report. A delivery budget does have one, namely what every model in this repository already runs
on.

The live defect gets a method-level `@Timeout` on the one test whose twelve chained waits overrun the class value,
rather than a raise on the class. `SubscriptionModelConformance` has 24 test methods, and at 60 seconds each a model
that hangs on all of them already runs a shard past its 20 minute kill, where it produces no report at all. Raising
the class value would make that outcome the default rather than the edge case.

## Consequences

`DELIVERY_TIMEOUT` and `START_TIMEOUT` are deleted rather than deprecated, and that is the first worked example of
the policy above. Both were reachable from a subclass, so both were part of the published API, and AGENTS.md's release-status
rule makes them free to remove because the feature they belong to has never shipped in a versioned section. After the
tag the same deletion would need a migration path.

Three fixed budgets remain, deliberately. `BlockingSubscriptionOverReactive.CHECKPOINT_TIMEOUT` bounds
`globalCheckpoint()`, which is a store query rather than a delivery, so sourcing it from a delivery budget would be a
category error, and it has one call site reached by one fixture. `ConcurrentRendezvous.DEFAULT_BARRIER_TIMEOUT` and
`DEFAULT_TASK_TIMEOUT` are already `public` and already overridable per call. `PRACTICALLY_FOREVER` is an overflow
guard and not a budget. The event-store leaves keep fixed budgets throughout, because a store write is not waiting on
a broker to connect.

The implementer-facing contract now lives in `package-info.java` in `tck/subscription-blocking` rather than only in
this ADR and ADR 94. It names the three kinds of member a fixture holds, which the issue behind this ADR conflated:
an accessor hands over the thing under test and owes nothing further, a declaration is a difference nothing on the
API reports and is asserted on both branches, and a budget bounds a wait on a schedule the interface does not
publish. The other three leaves have no package doc yet and get the same treatment when their fixture families next
change.

`ReactiveSubscriptionModelConformance` still carries no class `@Timeout`, which is what it shipped with. Adding one
now would be an assertion change this ADR did not set out to make, and its budget is validated as positive, so a
declaration cannot make a wait unbounded. It is worth revisiting the next time that suite is touched.
