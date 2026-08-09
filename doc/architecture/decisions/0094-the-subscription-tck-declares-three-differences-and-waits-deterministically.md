# 94. The subscription TCK declares three differences, and waits deterministically

Date: 2026-08-04

## Status

Accepted. #395, TCK phases 6 to 8.

The checkpoint-storage contract, the subscription-model contract and the module they live in have landed. The
competing-consumer contract and the reactor leaf are being built to the decisions below, so those parts record intent
rather than something already proved by code. Where writing one of them shows a decision here to be wrong, amend this
ADR rather than quietly diverging from it. That has already happened once: writing the subscription-model suite added two
fixture declarations, rejected a third, and turned up a contract gap, all marked as amendments below.

## Context

ADR 77 reserved two artifacts for this and settled the layout. What it did not settle is how the reactor stack is
covered, what the fixture is allowed to declare, or what a suite waits on. Those are the decisions here.

The value on this side of the TCK is different from the event-store side, and worth stating plainly. Event-store
coverage was duplicated, so the suites deleted thousands of lines. Subscription coverage is *uneven* rather than duplicated: the Spring
MongoDB model carries more than twice the test methods the in-memory one does, and the reactor models fewer still. So this
raises the floor and adds CI time rather than removing it. There is real duplication to collect on the way (10 copies of
one `serialize` helper in the subscription modules alone), but that is a side effect and not the point.

Three things shape the design:

**The two stacks do not have the same shape.** Blocking `SubscriptionModel extends Subscribable,
SubscriptionModelLifeCycle`; the reactor one extends nothing and hands back a bare cold `Flux`. Five capability
interfaces exist only on the blocking side. What does line up is the part that matters for a bridge: reactor's
`SubscriptionModelLifeCycle` has the same nine members, and they are already `void` rather than `Mono`.

**Storages disagree about what a checkpoint is on the way back out.** The MongoDB storages recognise their own two
checkpoint types and rebuild them; everything else is stored as the string it reports and read back a
`StringBasedCheckpoint`. The Redis storage does that to everything, including the MongoDB types, which is the
combination a MongoDB event store with its checkpoints in Redis actually runs. Nothing on `CheckpointStorage` reports
which way a given storage goes.

**Waiting in the existing corpus is inconsistent enough to be a correctness problem.** ADR 77 carries the measurement,
re-taken while this was written. The short version: a fifth of the `await()` calls in the `subscription/` modules set no
budget at all and inherit Awaitility's 10 second default, the ones that do set one spread from 1 to 40 seconds for the
same wait shape, and most `waitUntilStarted()` calls are the unbounded no-argument form.

## Decision

**The reactor stack goes through a bridge, and the gaps get closed rather than declared.**
`BlockingSubscriptionOverReactive` ships from `tck/subscription-reactor`, so the behavioural suites are written once
against the blocking contract, the same way `BlockingEventStoreOverReactive` already serves the stores. Translation is
mechanical because the lifecycle is already `void` on both stacks and the action type wraps as
`cloudEvent -> Mono.fromRunnable(() -> consumer.accept(cloudEvent))`.

A small reactive-only contract covers what a bridge cannot see, mirroring `ReactiveEventStoreConformance` from ADR 93:
nothing happening before subscribe, a failure travelling through the publisher rather than out of the assembling call,
and cancellation. It adds one the store side did not have to think about, that reactor's
`Subscription.waitUntilStarted()` returns a `Mono<Void>` whose own javadoc warns that "started" promises less than the
blocking model's does.

**Reactor gains `IntrospectableSubscriptionModel`.** `subscriptionIds()` is a real contract, decided in ADR 83, with
zero reactor implementors. Without the addition, the suite would have to declare it blocking-only, which is a fixture flag recording
an oversight as intended behaviour, and that is the one thing these suites are not allowed to do. The addition is
additive, and `ManualStartSubscriptionModel` and `startAll()` already depend on introspection on the blocking side, so
the gap is visible in the framework and not only in the TCK.

The three reactor catch-up models that expose only a cold `Flux` keep a contract of their own. That is not a
concession. A bare reactive primitive and a named lifecycle-managed subscription are two concepts, and reactor already
documents the second as the counterpart to the first. Forcing the primitive through the bridge would mean inventing a
lifecycle the type does not have.

**This paragraph describes the state as of this ADR, not the current one.** [ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md)
later gave the three catch-up models the same named, lifecycle-managed contract every other reactor model has, once
#547 showed a durable model wrapping one had nothing to delegate to. They still also expose the cold `Flux` primitive
this paragraph describes, the promotion added the lifecycle-managed path alongside it rather than replacing it.
[ADR 101](0101-a-durable-reactor-subscription-delegates-when-the-model-it-wraps-is-named.md) builds on that promotion
but does not amend this ADR.

**Deterministic waiting first, polling only where a change stream leaves no alternative.** This supersedes ADR 77's
waiting paragraph in part. **Amended once phase 6 was written: `Conformance.await()` and `awaitNothing()` were never
built, and nothing needs them.** What ships is `RecordedEvents`, a recording handler that blocks on a queue with one
deadline for a whole expected set, so a wait wakes on arrival rather than polling an assertion and Awaitility stays off
the published compile path. The order of preference in a suite is:

1. A deterministic start position, `StartAt.checkpoint(globalCheckpoint())`, rather than a start barrier. Already the
   established fix at 7 sites, and the corpus records why `waitUntilStarted()` is not enough for a change-stream
   model: it says the `Flux` was subscribed to, not that the server acknowledged the command and positioned the cursor.
2. `OccurrentSubscriptionsExtension.start(id)`, which blocks until the subscription is listening. It is published and
   no `subscription/**` test uses it today.
3. The in-memory drain, `waitUntilAllEventsProcessed`, where the model under test is the in-memory one.
4. For "this must not arrive", a marker published afterwards and waited for, then an assertion on the whole recorded
   list. There is no `awaitNothing()`: a wait for a quiet period passes just as well against a subscription that was
   never listening, and any constant short enough to keep a test quick is short enough to flake on a loaded runner.

The reason is correctness rather than taste. ADR 78 already recorded it: polling hides the difference between
synchronous and asynchronous delivery, so a model wired to deliver asynchronously when it should be synchronous passes
silently, and a real regression waits out the whole budget and then reports the last assertion failure instead of
saying delivery never happened. Every suite method carries a `@Timeout`, because an unbounded `waitUntilStarted()`
otherwise hangs a shard for its full 20 minutes and no subscription shard has a rerun backstop.

**The fixture declares three differences, one per contract, and every declaration costs something to give.** The line is
the one `EventStoreFixture.timePrecision()` sits on: declare what cannot be asked, ask everything else.

- **Whether a checkpoint keeps its type.** `CheckpointStorageFixture.preservesCheckpointType(Checkpoint)`. Both answers
  are asserted, and in both directions: `true` means the type has to come back, `false` means it has to *not* come back.
  Without the second half a fixture could answer `false` everywhere and never be asked to prove any of it, which is a
  flag hiding behaviour rather than declaring a difference. A storage answering `false` still owes `asString()`
  faithfully, which is the whole of what `Checkpoint` promises and what `MongoCommons.applyStartPosition` has left to
  rebuild a start position from.
- **What `pause` means.** The register-only models implement the full lifecycle but drop rather than defer, per ADR 85.
  A suite asserting redelivery after resume would fail them correctly and uselessly. The fixture declares which, and
  the suite asserts the documented outcome either way: a deferring model redelivers, a dropping model does not.
  The primary axis is whether the model has a position to resume from at all: a model that dispatches as events arrive
  has nowhere to hold them and owes nothing, while a model reading a log or a change stream could resume where it left
  off. **The two MongoDB models sat on the same side of that axis and still answered differently, and settling it took
  three attempts.** This ADR first called it an intended difference on the strength of a code comment in each model,
  which was too weak a reason, since the Spring comment is about avoiding replay and not about the paused window. It was
  then treated as a plain bug in the Spring model and fixed there, and the fix made
  `CompetingConsumerSubscriptionModelTest.pausing_and_resuming_both_competing_subscription_models_several_times`
  deliver an event twice. That is the cost nobody had measured: a competing consumer is paused precisely because
  another consumer holds the lease, that consumer has already delivered the events in the gap, and a gap-free resume
  hands them over again. That fix was reverted with the measurement recorded on #522, since resuming at the present
  loses the paused window, resuming from the position read duplicates under competing consumers, and a model cannot see
  whether a competing-consumer wrapper sits above it, so it cannot choose per case.

  **Resolved on #522, 2026-08-05: gap-free resume is the contract and the duplicate is its price.** The two costs are
  not symmetric, which is what the third pass added. Losing the paused window is a lost event, which the isolation rule
  in `AGENTS.md` forbids outright, and ADR 57 already recorded which way to err when a design has to pick one: wasted
  work beats loss. A duplicate is absorbed by an idempotent handler, and at-least-once is what every wrapper above
  these models already delivers. So `SpringMongoSubscriptionModel` now tracks the change-stream position it has read to
  and rebuilds from there, the way the native and reactor models always did, both MongoDB models declare
  `deliversEventsPublishedWhilePaused()` as `true`, and all three document delivery across a pause as at least once.
  Two competing-consumer tests assert every event arriving in order while tolerating a redelivery, rather than an exact
  sequence. Relaxing those expectations is a correction to what the contract promises, not a weakened assertion, and the
  events must still all arrive and still arrive in order. The second one was not in the original measurement: stopping a
  model pauses its subscriptions, so `stopping_and_starting_both_competing_subscription_models_several_times` reaches
  the same `1, 2, 3, 4, 5, 4, 5` as the pause and resume one. The declaration stays, because the register-only models
  answer `false` for a reason that has nothing to do with this (ADR 85), which is the axis it was always about.
  **The lesson to carry: a declaration must not park a bug, reaching for the word bug before measuring what the fix
  costs is the other way to get this wrong, and a measured cost is what a decision is made against rather than an
  argument about which comment sounded more deliberate.**

  **Amended 2026-08-09 by [ADR 117](0117-a-resumed-competing-consumer-continues-from-the-checkpoint.md).** This
  bullet describes what the wrapped Mongo model itself does on resume, reopening from the change-stream position
  it had already read. That is still true of the wrapped model in isolation, but a `DurableSubscriptionModel`
  sitting above it now re-reads the stored checkpoint and repositions the wrapped model there first, whenever one
  is stored and the model underneath can be repositioned. A regained lease resumes from the checkpoint another
  node may have advanced while this node held no lease, not always from the position this node's own delegate
  last read.
- **Which `StartAt` variants a model accepts.** A sealed set of four, asserted as delivery for the accepted ones and
  refusal for the rest. This is phase 8's declared restriction mechanism, and it is where the per-wrapper deliberate
  refusals get asserted rather than rediscovered.

`globalCheckpoint()` is asked rather than declared, because `PositionOrderedReader.writesPosition()` set that
precedent and a declaration can go stale. The contract is the one #395 calls easy to miss and expensive to get wrong.

**Amended when that suite was actually written: half of what this paragraph promised cannot be asserted from the TCK.**
It said the suite would show that a model answering null cannot sit behind catch-up. Driving catch-up needs a
`CatchupSubscriptionModel`, which lives in a wrapper module, and the TCK leaf depends on `occurrent-subscription-api-blocking`
and nothing else on purpose. Reaching for the wrapper to assert the refusal would put a wrapper dependency into the
contract module for every implementor, which is a worse trade than moving the assertion. So it moves to the wrapper
suites, which have the catch-up models in scope anyway.

What `CheckpointAwareSubscriptionModelConformance` does assert is the half that matters most and was never checked:
a position read before a write, used as `StartAt.checkpoint(..)`, yields a subscription that receives that write. That
is exactly the handover a catch-up subscription performs at the end of its replay, and a model failing it loses every
event written while history replayed. Also that asking twice does not consume the position, since catch-up reads it per
subscription rather than per model. The null answer is asserted as what it honestly means, that the model is still a
working live model but cannot seed a handover.

**Amended while phase 6 PR 2 was written: two more differences turned out to need declaring, both found by running the
suite rather than by reading the interfaces.**

- **Whether a failing handler is retried or the exception reaches whoever published the event.** The three models that
  deliver asynchronously wrap the handler in a `RetryStrategy`, and the two that deliver on the publishing thread let the
  exception through, which `RegisteringSubscribable` documents as "A handler exception propagates to the caller". Both
  answers cost something: a retrying model owes a later call to the handler, a propagating one owes the exception out of
  the publish call. A consequence worth stating, because it is a trap: the suite must never install a handler that throws
  forever, since `RetryStrategy` defaults to an infinite number of attempts and the test would wait out its whole timeout
  instead of failing with a reason.
- **Whether the model accepts more than one subscription.** `PushSubscriptionModel` refuses a second one, per ADR 90, and
  nothing reports that: `Consumers` has no accessor, and finding out by subscribing twice leaves the model in whichever
  state the attempt produced. A model answering `false` owes the refusal, one answering `true` owes two subscriptions
  that receive independently.

**Amended when the competing-consumer suite was written. Its fixture declares one thing, and it is not a difference
between implementations.** `CompetingConsumerStrategyFixture.timeToConverge()` is a bound on how long the suite waits
for a strategy's own coordination to settle who holds a lock when nothing told it directly. Three assertions need it,
and they are one property from three sides: a rival takes over from a holder that stopped coordinating, a registration
that lost wins later without registering again, and a released consumer takes its lock back. In none of them does
anybody call into the strategy that has to change its answer, so the change arrives on a schedule the interface does
not report. It is closer to `EventStoreFixture.timePrecision()` than to `preservesCheckpointType`: it describes what an
implementation can do rather than which of two behaviours it chose, so there are not two branches to assert. Both
MongoDB fixtures declare five seconds against a worst case of one and a half, because the suite stops waiting the
moment the condition holds and a generous bound is therefore only ever paid in full by a test that was going to fail.

**No lease reaches the published contract, and that is the whole design of this suite.** Both of Occurrent's strategies
keep a lease in MongoDB, and a suite written around one would be a description of MongoDB rather than a contract: a
strategy backed by a leader election or a broker owes the same guarantees and has no lease to expire. So the suite
asserts crash liveness generically, that a holder which stops coordinating without releasing or unregistering loses the
lock to a rival before long, which is the one property the whole pattern exists for and the property a lease is one way
of providing. `shutdown()` is what the suite uses to stop a strategy coordinating, since that is the closest a test can
get to a process going away through published API, and it asserts nothing about what a shut-down strategy answers
afterwards. When the lease itself is up, and what refreshing one does, is covered separately and deterministically in
`MongoLeaseTimingTest` against the shared support class. That took one prefactor: the support class now takes its
`ScheduledRefresh` beside the `Clock` it already took, so a test holds both halves and no time passes while it runs.
The alternative, a short lease and a sleep, cannot tell "the lease was up" from "the machine was busy", which is what
the existing competing-consumer tests had to live with.

**The fixture hands over a factory rather than a second strategy.** Contention is external and coordinated only through
storage the interface does not mention, so a single reference has no notion of a rival and the suite has to build them.
A factory rather than one extra accessor because some of what the suite asserts needs a third instance that outlives a
rival it deliberately shuts down. Constructing several strategies over one storage therefore becomes an explicit,
documented demand on an implementor.

**This is the one suite that polls, and only where the API leaves nothing to block on.** `CompetingConsumerSubscriptionModel`
registers a listener and reacts to what it is told, while `SagaRunner` registers a consumer, never adds a listener, and
asks `hasLock` on every tick. Both styles are covered, and the second one has nothing but a question to ask, so the
suite asks it on a loop until the bound runs out. That is not the wait for a quiet period the waiting rules ban: it
waits for something that must happen and stops when it does, rather than for a period in which nothing must. Everything
observable through a listener still blocks on a queue, through `RecordedLockChanges`, which is `RecordedEvents` for
lock changes.

**The green anti-skip run is a second implementation rather than a copy of one.** `WorkingCompetingConsumerStrategy`
elects the longest-standing live candidate out of a shared map and treats one that stopped heart-beating as gone, which
is nothing like a lease. That is deliberate on top of what a green run is for here: if a suite written against a lease
could only be satisfied by a lease, a second implementation is the cheapest way to find out. It is around 150 lines
against `WorkingCheckpointStorage`'s nine, which is more than that precedent but nowhere near the thousand-line copy
the event-store leaves were quoted, and since it is a design of its own there is nothing for a production change to
drift away from.

**Writing the suite found two defects in the MongoDB strategies, and both are fixed rather than declared.** Releasing a
consumer left it recorded as still holding the lease, so `hasLock` answered yes for a subscriber that had just given
the lease up, which would have let a saga's timer poller keep firing timers on a lease it no longer held. And the next
refresh then found its commit rejected and reported the loss a second time, though nothing had changed since the first
report. A listener is a change feed, which the `CompetingConsumerListener` javadoc now says, and the suite asserts that
giving a lock up and taking it back is reported as exactly two changes rather than one per round of coordination. The
release path now marks the consumer as having stood down for a round, which fixes both and keeps the head start a rival
had before, since a consumer that gave a lease up and took it straight back before anybody else looked has not given it
up in any useful sense.

**Fixing `hasLock` uncovered a third defect, in `CompetingConsumerSubscriptionModel`, and it is worth recording because
the stale answer was hiding it.** `resumeSubscription` asks `hasLock` and, when the answer is no, registers the consumer
instead. Registering can be granted the lock there and then, `onConsumeGranted` resumes a paused consumer itself, and
the caller then resumed a second time and failed on a delegate that was no longer paused. Nothing reached that path
before, because after a system pause `hasLock` answered yes and the model took the other branch. The order is what
fixes it: the consumer is recorded as running before it registers, so a callback arriving during the call finds nothing
to resume, and the old state goes back if registering did not win. `CompetingConsumerSubscriptionModelTest.can_resume_after_consume_prohibited`
is what fails without it.

**And one candidate declaration was rejected, which is worth recording because it looked obviously right.** Synchronous
delivery is not declared. A `deliversSynchronously()` flag cannot be held to anything on its `false` branch: "the handler
had not run when publishing returned" is untrue even for a model that queues, whose consumer thread may legitimately have
got there first, so the only honest `false`-branch assertion is "the handler eventually runs", which every other test
already makes. A declaration free on one branch is a switch for disabling the only test of the property, and it would sit
in the file belonging to whoever changed the model. So the in-process contract is its own suite,
`InProcessDeliveryConformance`, that only the in-process models extend, and declining is the visible absence ADR 77 rule
(d) already relies on. It carries no `instanceof` guard either: guarding on `RegisteringSubscribable` would shut out an
out-of-tree model that satisfies the contract, and the first assertion fails against an asynchronous model anyway.

**`RegisteringSubscribable` becomes a `SubscriptionModel`.** `SynchronousSubscriptionModel` and `PushSubscriptionModel`
had every member `SubscriptionModel` requires and did not declare it, with no ADR recording a reason. The suite would
otherwise have had to be typed on `Subscribable` and `SubscriptionModelLifeCycle` separately and hand every out-of-tree
implementer that asymmetry. Adding it is one token, declares no new members, and nothing in main sources tests for the
interface, so no behaviour changes. Two wrappers gain a capability as a side effect, and both are sound:
`ManualStartSubscriptionModel.stoppedByDefault` and `StreamSubscriptionModel.from` now accept the two register-only
models, which already support registering while stopped and already accept a `StreamSubscriptionFilter`. Same reasoning as
giving reactor `IntrospectableSubscriptionModel` above, and the same clause of AGENTS.md.

**Amended 2026-08-05: the reactor side of that argument was carried further, in [ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md).**
This ADR said the two stacks do not have the same shape, that reactor `SubscriptionModel` hands back a bare cold
`Flux` while the blocking one is the empty combination of `Subscribable` and `SubscriptionModelLifeCycle`. That was a
true description and it did not have to stay true. Reactor now has `SubscriptionModel` meaning exactly what blocking
means, and the Flux-returning primitive is `FluxSubscriptionModel`. Phase 7 consumes that interface rather than the
plan of record's proposed `ManagedSubscriptionModel`, so the bridge names the type it bridges instead of handing every
out-of-tree reactor implementor an asymmetry. Read ADR 98 for the naming, the bean-selector audit it required, and why
the two interfaces stay separate rather than merging.

**Only introspection is a per-model capability of the base contract.** Five interfaces looked like capabilities and one
is. `IntrospectableSubscriptionModel` gets its own suite, worth having because both MongoDB models implement
`subscriptionIds()` and neither had a test for it. `DelegatingSubscriptionModel` and `ManualStartSubscriptionModel` wrap
another model rather than being something a model has, so they are phase 8. `StreamSubscriptionModel` and
`DcbSubscriptionModel` are one shared adapter reached through `from(SubscriptionModel)`, so a suite per model would test
the same code five times. There is no capability enum: `EventStoreCapability` exists because a store's capabilities are
construction-time config with nothing on the API reporting them back, and here the interface is the declaration.

**At-least-once delivery and resuming after a restart are not this contract's promises.** Both need a checkpoint that
survives the restart, which belongs to the models that wrap one, and the register-only models make `shutdown()`
deliberately irreversible. They move to phase 8. Keeping them in the base suite would have needed a durability
declaration, which is the shape #470 deleted.

**An in-memory `CheckpointStorage` gets published, in the module that already has one of everything else.** Four
independent private copies in test classes were the demand signal, and the in-memory stack already ships an event store
and a subscription model. It goes in `subscription/inmemory` rather than a new leaf: that artifact is already published
and already depends on `occurrent-subscription-api-blocking`, so this costs no new artifact, no BOM entry and no
`<excludeArtifacts>` change. A new leaf would have bought nothing but the publishing checklist.

**A third contract gap, found by the suite failing.** `SubscriptionModelLifeCycle.resumeSubscription` promises a stopped
model's subscription comes back "on its own without starting the rest", and three implementations set the model-wide
running flag instead. Writing the suite showed why, and it is not carelessness: after `stop()`, a resumed subscription
cannot deliver unless either resuming clears the model-wide flag or delivery consults only the per-subscription one. The
javadoc's promise forces one of the two and says which of them it means nowhere. So the suite asserts the outcome nobody
disputes, that the resumed subscription delivers again, and asserts nothing about the flag. The choice is a child issue
under #396.

**Two contract gaps become issues rather than loosened assertions.** `unregisterCompetingConsumer` and
`releaseCompetingConsumer` carry byte-identical javadoc, so a suite cannot assert the difference between them. Reactor
`globalCheckpoint()` never documents the empty-`Mono` case that `ReactorMongoSubscriptionModel` returns. Both get child
issues under #396, filed as #516 and #517.

**Amended when the competing-consumer suite was written: #516 is answered, and the answer is that the contract does
distinguish the two, in a way neither javadoc mentioned.** Unregistering forgets the consumer, so it never holds the
lock again without registering a second time, and somebody else gets the lock if anybody is waiting. Releasing keeps it
registered, so it is one of the candidates for the lock it just gave up and may win it back with nobody registering it
again. That is exactly the difference `CompetingConsumerSubscriptionModel` was already relying on, unwritten, when it
unregisters a subscription a user paused and releases one the system paused. So this is a javadoc fix on
`CompetingConsumerStrategy` rather than a divergence under #396, and the suite asserts both halves.

The suite asserts one further thing about releasing, and what it does *not* assert is the interesting part. It does not
assert that a rival gets the lock, even though that is what the old javadoc promised, because the consumer that
released is still competing for it and which of them wins is a race between two schedules the contract says nothing
about. A test demanding the rival would be asserting the phase two background threads happened to be in, and the MongoDB
strategy loses that race about as often as it wins it. What a release may never do is leave the lock unheld, and a
caller who needs the stronger guarantee has `unregisterCompetingConsumer`, which the suite does hold to it. The
javadoc now says that rather than promising a handover it cannot make.

## Consequences

The four anti-silent-skip rules from ADR 77 carry over unchanged, and each leaf needs its own `SuiteNeverSkipsTest`
because one leaf's version cannot see a suite in another module.

**A run against an implementation that honours nothing does not earn the no-skipping claim, and a leaf needs a second
mechanism that does.** Against an implementation that throws from every method, each test dies on its first call, so an
`Assumptions` call placed anywhere after that is never reached and the skipped count stays zero whether or not the rule
was followed. Which second mechanism depends on what a working implementation costs in that leaf.

*Where one is cheap, run the suite green against it.* That is the subscription leaf: `WorkingCheckpointStorage` is nine
lines of `HashMap`, and a green run reaches every line of the suite body, so a skip anywhere in it shows up. The reactor
leaf's version has to run the anti-skip case *through the bridge*, not only against a blocking model: this bridge
carries a lifecycle and hands events to a consumer on some thread, so the risk that the bridge rather than the model
becomes the thing under test is real, and a bridge that swallows a failure or never delivers is exactly what a reactive
model honouring nothing would expose.

*Where one is not, scan the compiled suites for a reference to anything that can skip.* That is both event-store
leaves, and the arithmetic is what decides it. `InMemoryEventStore` is 778 lines and cannot be depended on, because
`eventstore/inmemory` already test-depends on `occurrent-tck-eventstore-blocking` and Maven's reactor cycle check
ignores scope; no reactive in-memory event store exists at all, and `ReactiveEventStoreConformance`'s laziness tests
rule out a `Mono.just(blockingCall())` wrapper over a blocking one. So a green run costs on the order of a thousand
lines of store logic copied into test sources, drifting from the published stores it copies, where the subscription
leaf's copy cost nine. Worse, it would still be partial: a green run only reaches the lines one fixture's declarations
reach, and `EventStoreQueriesConformance`, `EventStoreTimePrecisionConformance` and `DcbEventStoreConformance` all
branch on declarations, so an assumption inside an unvisited branch survives it. `SkipMechanismScan` reads the class
files a leaf compiles and fails on a reference to `Assumptions`, `TestAbortedException`, `@Disabled` or a
`@DisabledIf` condition. It works on bytes because comments do not survive compilation, so the javadoc discussing the
ban cannot trip it, and it covers every line of every suite in the module rather than the ones an enumeration
remembers. That includes the three the blocking leaf's failing runs never selected, and anything added later. The check
is a copy per leaf, for the reason `WorkingCheckpointStorage` already records for its own copy: a test-sources class in
one module is invisible from another.

The two mechanisms answer different questions and neither subsumes the other. A green run also proves the suite is
satisfiable at all, which a scan cannot; a scan proves the ban over the whole suite, which a green run cannot. Where
both are affordable, have both. Nothing here licenses dropping the failing run: that is what shows a suite asserts
something rather than nothing.

What has landed so far, and what it found: `CheckpointStorageConformance` holds 15 assertions, and all four blocking
storages passed on first run. That is the outcome to expect from a phase writing down what the storages already agreed
on. The one disagreement is the type-preservation one, which is now declared and asserted in both directions rather
than being folklore about which storage rebuilds what. The four private in-memory copies are gone. Four copies remain
on the reactor side and they stay, since reactor `CheckpointStorage` is a different interface with no `exists` and a
`Mono` return, so the published blocking class cannot replace them: in `subscription/util/reactor/durable-subscription`,
`subscription/push/reactor`, and two under `dsl/projection-dsl/reactor`. Three of those class names have blocking twins,
so match on the path rather than the name. A fifth copy is blocking and cannot be deduplicated either:
`ManualStartSubscriptionModelTest`'s recording storage lives in `subscription/api/blocking`, which
`subscription/inmemory` depends on, so the dependency would have to run backwards. Phase 7 decides whether a reactor
counterpart is worth publishing.

**This constrains #392 and #388.** A SQL or broker-backed `CheckpointStorage` that leaves a tombstone after `delete`,
or whose `exists` and `read` disagree, now fails a suite rather than being found by a subscription that silently
replays. So does one that cannot store a checkpoint again for a subscription that was cancelled and registered anew.

The cost is CI time, and it is a real cost. These suites add wall-clock to the subscription shards rather than removing
it, and unlike the event-store phases there is no deletion to pay for it. The per-shard budget of 240 seconds only
warns, but the 20 minute timeout does not.

**Amended when phase 7 was built.** The reactor leaf reuses the blocking suites through
`BlockingSubscriptionOverReactive` in `tck/subscription-reactor`, which presents a reactor `SubscriptionModel` (the
combining interface from #543) as a blocking one, and a small `ReactiveSubscriptionModelConformance` covers what a
bridge cannot see: that the action's `Mono` does its work when subscribed rather than when assembled, that a failing
action fails through the model and leaves it running, that `waitUntilStarted()` answers and answers again, and that
disposing a wait leaves the subscription working. Two wirings were deliberately held back rather than declared around.
`CatchupThenPushSubscriptionModel` replays its whole history for every new subscription id by documented contract, so
the general suite's assumption that a fresh subscription starts at the present fails it structurally; that is exactly
the restriction phase 8's `StartAt` declaration exists to express, so its general-conformance wiring waits for phase 8
while its introspection and reactive-only wirings run now. And `ReactorDurableSubscriptionModel` reads the wrapped
model's cold `Flux` primitive through its own delivery pipeline instead of delegating to the named `subscribe` the way
the blocking `DurableSubscriptionModel` does, so it inherits neither retry nor synchronous filter validation and
cannot pass those assertions by any focused change. Whether it gains its own retry policy or is reshaped to delegate
like its blocking twin is a design decision recorded as its own issue, not something a fixture flag may paper over.

**Amended when phase 8 was built, which is where this ADR's `StartAt` paragraph finally got implemented, and it needed
a second declaration beside it.**

The mechanism is the one this ADR described: `StartAtVariant`, a TCK enum with one constant per permitted
implementation of the sealed `StartAt`, and `SubscriptionModelFixture.acceptedStartAtVariants()`. Every accepted
variant owes a subscription that receives what is published after it, every variant left out owes an
`IllegalArgumentException` from `subscribe`, and one test walks all four on every model so a variant left out is a
claim rather than an excuse. The enum exists instead of a set of `StartAt` instances because a declaration has to name
the variants a model refuses, and a refused variant has no instance a fixture would want to build. It cannot fall
behind `StartAt` without the compiler saying so.

Two models refuse something, and only one of them was known in advance. `CatchupThenPushSubscriptionModel` accepts the
default and nothing else, on both stacks, which is the refusal this mechanism was designed around. The other was found
by running the suite: `InMemorySubscriptionModel` refuses a checkpoint, since it keeps no history and would otherwise
accept a position it could only ignore. That is worth recording as evidence rather than as trivia. A declaration whose
non-default branch has one implementation is one bad refactor away from being untested, and this one has two.

**The `StartAt` declaration alone did not unblock the wiring phase 7 held back, and finding that out is the part worth
writing down.** The phase-7 amendment above says `CatchupThenPushSubscriptionModel`'s general conformance waits for
this mechanism. It does, but not only for it. Refusing three variants says nothing about what the fourth one does, and
what fails that model is the suite's assumption that a subscription id it has not seen before starts at the present:
`a_cancelled_subscription_stops_receiving` creates a second subscription after events exist, and this model replays
them. So a second declaration ships beside the first,
`SubscriptionModelFixture.replaysHistoryToANewSubscription()`. It is not a flag holding a bug, and both branches cost
something: `false` owes a new subscription that does *not* receive what was published before it existed, which every
change-stream and in-process model now proves, and `true` owes exactly the opposite plus, in the cancellation test, the
replay arriving *before* the live event. The `true` branch therefore asserts more than the `false` branch it replaces
rather than less. Both stacks are wired, and the blocking one is this model's first conformance coverage of any kind.

**At-least-once and resuming after a restart became their own suite, `RestartConformance`, rather than the declaration
on the base fixture the plan of record proposed.** The reason is the one this ADR already used to reject
`deliversSynchronously()`. A model whose events arrive by being handed to it cannot be rebuilt over durable state it
does not have, and cannot be handed an event while it is down, so a base-fixture declaration would have a branch that
asserts nothing at all for the in-process and register-only models, and a declaration that is free on one branch is a
switch for turning off the only test of a property. Declining by not extending the suite is the visible absence rule
(d) already relies on. A model that *can* answer still declares which way it goes, through
`RestartableSubscriptionModelFixture.resumesAfterARestart()`, because that is a real difference between two models with
identical durable state underneath them: a change-stream model reads from wherever the server is now, and the same model
wrapped in one that keeps a checkpoint reads from where the checkpoint says. That declaration has two assertable
branches and both run. Delivery here is at-least-once and the suite says so: a model resuming from the last position it
stored rather than from just after it redelivers that event, so the assertions are about what must arrive and never
about what must not repeat.

**The assertion this ADR moved out of phase 6 has landed, and where it landed says something about what it could
assert.** It is `StreamCatchupSubscriptionModelTest.a_model_that_reports_no_checkpoint_cannot_sit_behind_catchup`, in
the wrapper module, driving a real `StreamCatchupSubscriptionModel` over a model whose `globalCheckpoint()` answers
null. The production code already refused, loudly, in `AbstractCatchupSubscriptionModel.captureLiveResumeCheckpoint`;
what was missing was anything holding it to that. The shape the test had to take is the interesting part.
`CatchupSubscription.waitUntilStarted` catches whatever the replay threw, logs it at WARN and answers `false`, so the
observable contract is that the caller is told the subscription did not start and that nothing was replayed, with the
reason reachable through the replay's own `Future`. All three are asserted, because the failure worth preventing is the
quiet one: a handover falling back to "now" would deliver the history, never go live, and leave a read model looking up
to date while it stopped moving.

**`MongoCommons.applyStartPosition` was the other half of #524, and it was still lazy.** #524 hoisted the *filter*
check into `subscribe` on `NativeMongoSubscriptionModel` and phase 7 did the same on `ReactorMongoSubscriptionModel`,
and both left the start position where it was, the native one on its dispatcher thread and the reactor one inside a
`Flux.defer`. Reachability was listed as unverified and is now verified, in both directions. The
`IllegalArgumentException("Unrecognized StartAt implementation")` in that method is dead code through published API:
`StartAt` is sealed to four types and `Dynamic.get` recursively unwraps, so nothing a caller can build reaches it. What
*is* reachable is the parsing underneath, because `StringBasedCheckpoint` has a public constructor taking any string
and a storage hands checkpoints back as strings, so a value containing `resumeToken` or `operationTime` in the wrong
shape throws from inside the deferred work. Both models then retry it forever, `waitUntilStarted()` never answers and
`isRunning(id)` keeps saying yes. So it is fixed the same way the filter was, through a new
`MongoCommons.checkStartPosition` that runs the whole resolution and discards the result rather than growing a second
copy of the parsing that could drift. `SpringMongoSubscriptionModel` needed nothing: it was always eager, by
construction rather than by decision. **A dynamic position is deliberately left lazy on both.** Resolving one means
calling the caller's own function, the model calls it again when it subscribes for real, and calling an arbitrary
caller's function twice to validate it is a worse trade than the narrower check.

**One wiring is held back, for the reason phase 7 held one back, and the suite was not touched to avoid it.**
`CompetingConsumerSubscriptionModel` fails two assertions of the general suite that every other model passes: it accepts
a subscription id already in use, and it accepts a pause of a subscription that is not running. Its introspection
wiring ships and its general-conformance wiring waits, recorded as #553 under #396. The second of the two looks like a
plain omission, but the first is a real contract question rather than an obvious bug, which is exactly why it goes to
an issue instead of being fixed in passing: sharing one subscription id across competing consumers is the whole point
of the pattern, so the contract has to say whether "one id means one subscription" is a statement about a model
instance or about the id everywhere, and the suite currently assumes the former. Weakening the assertion so the model
passes was never an option: four models honour it today, and the one test of the property would have been turned off
for all of them to accommodate the fifth.

**Amended 2026-08-06: #553 is answered, and the answer is the reading the suite assumed.** Uniqueness is per model
instance, and the model moved rather than the suite, so the held-back wiring ships as
`CompetingConsumerSubscriptionModelConformanceTest`. What settles it is that a node *is* an instance, in the Spring
wiring and in every test that simulates contention, and that two consumers for one id inside one instance could never
have worked: the wrapper resolves a consumer by subscription id alone, so the second one was unreachable through
cancel, pause and resume alike. Reasoning in
[ADR 102](0102-a-subscription-id-is-unique-per-subscription-model-instance.md).

**And a claim in this ADR was wrong, so it is corrected here rather than left to rot.** Above, under what is a
per-model capability, it says `StreamSubscriptionModel` and `DcbSubscriptionModel` "are one shared adapter reached
through `from(SubscriptionModel)`, so a suite per model would test the same code five times". What they share is
`AbstractDelegatingSubscriptionModelAdapter`, which forwards the life cycle and nothing else.
`StreamSubscriptionModelAdapter` and `DcbSubscriptionModelAdapter` translate `subscribe` separately, and the DCB one
additionally re-filters what it delivers so a catch-up replay stays inside the subscription's own criteria. Neither is
a `SubscriptionModel` (one takes a `Filter`, the other a `DcbCriteria` and a `DcbStartAt`), so no suite here applies to
either of them, and what phase 8 does instead follows the corrected reading exactly: the forwarding is asserted once,
since it really is one piece of code, and the per-facade `subscribe` translation stays covered per facade.

The question this ADR left to phase 7 is answered: the reactor in-memory checkpoint storage is published. It lives at
`org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage` in the already-shipped
`occurrent-subscription-inmemory` artifact, beside its blocking twin, with `occurrent-subscription-api-reactor` as an
optional dependency. The same simple name in a `reactor` subpackage mirrors how the two api modules already name their
halves, no new artifact is minted for one class (the handover-engine reversal set that bar), and the optional scope
keeps reactor-core off blocking-only consumers' classpaths, the pattern `occurrent-command-dispatch` established for
its decider dependency. One behavioural difference from the four private copies it replaces, all now deleted: the
published publishers are cold. The copies stored the checkpoint when `save(..)` was called; the published class does
nothing until subscription, which is the contract ADR 93 holds every published reactive publisher to, and its test
asserts both the cold branch and the eager argument validation.
