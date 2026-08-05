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
  off. **The two MongoDB models sit on the same side of that axis and still answer differently, and the reason is a
  genuine open question rather than a bug on one side.** That took two attempts to establish. This ADR first called it an
  intended difference on the strength of a code comment in each model, which was too weak a reason, since the Spring
  comment is about avoiding replay and not about the paused window. It was then treated as a plain bug in the Spring
  model and fixed there, and the fix made
  `CompetingConsumerSubscriptionModelTest.pausing_and_resuming_both_competing_subscription_models_several_times`
  deliver `1, 2, 3, 4, 5, 4, 5`. That is the cost nobody had measured: a competing consumer is paused precisely because
  another consumer holds the lease, that consumer has already delivered the events in the gap, and a gap-free resume
  hands them over again. Resuming at the present loses the paused window, resuming from the position read duplicates
  under competing consumers, and a model cannot see whether a competing-consumer wrapper sits above it. So both branches
  stay asserted and #522 owns the choice, with the duplicate delivery recorded there as evidence.
  **The lesson to carry: a declaration must not park a bug, and reaching for the word bug before measuring what the fix
  costs is the other way to get this wrong.**
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
