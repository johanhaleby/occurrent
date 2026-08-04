# 94. The subscription TCK declares three differences, and waits deterministically

Date: 2026-08-04

## Status

Accepted. #395, TCK phases 6 to 8.

The checkpoint-storage contract and the module it lives in have landed. The subscription-model contract, the
competing-consumer contract and the reactor leaf are being built to the decisions below, so those parts record intent
rather than something already proved by code. Where writing one of them shows a decision here to be wrong, amend this
ADR rather than quietly diverging from it.

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
waiting paragraph in part. `Conformance.await()` still ships, but as the fallback rather than the default, and the
order of preference in a suite is:

1. A deterministic start position, `StartAt.checkpoint(globalCheckpoint())`, rather than a start barrier. Already the
   established fix at 7 sites, and the corpus records why `waitUntilStarted()` is not enough for a change-stream
   model: it says the `Flux` was subscribed to, not that the server acknowledged the command and positioned the cursor.
2. `OccurrentSubscriptionsExtension.start(id)`, which blocks until the subscription is listening. It is published and
   no `subscription/**` test uses it today.
3. The in-memory drain, `waitUntilAllEventsProcessed`, where the model under test is the in-memory one.
4. `Conformance.await()` and `awaitNothing()` for the change-stream cases where nothing deterministic exists, with the
   fixture-supplied multiplier ADR 77 describes.

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
- **Which `StartAt` variants a model accepts.** A sealed set of four, asserted as delivery for the accepted ones and
  refusal for the rest. This is phase 8's declared restriction mechanism, and it is where the per-wrapper deliberate
  refusals get asserted rather than rediscovered.

`globalCheckpoint()` is asked rather than declared, because `PositionOrderedReader.writesPosition()` set that
precedent and a declaration can go stale. The suite pins that a model answering null cannot sit behind catch-up, which
is the contract #395 calls easy to miss and expensive to get wrong.

**An in-memory `CheckpointStorage` gets published, in the module that already has one of everything else.** Four
independent private copies in test classes were the demand signal, and the in-memory stack already ships an event store
and a subscription model. It goes in `subscription/inmemory` rather than a new leaf: that artifact is already published
and already depends on `occurrent-subscription-api-blocking`, so this costs no new artifact, no BOM entry and no
`<excludeArtifacts>` change. A new leaf would have bought nothing but the publishing checklist.

**Two contract gaps become issues rather than loosened assertions.** `unregisterCompetingConsumer` and
`releaseCompetingConsumer` carry byte-identical javadoc, so a suite cannot assert the difference between them. Reactor
`globalCheckpoint()` never documents the empty-`Mono` case that `ReactorMongoSubscriptionModel` returns. Both get child
issues under #396.

## Consequences

The four anti-silent-skip rules from ADR 77 carry over unchanged, and each leaf needs its own `SuiteNeverSkipsTest`
because one leaf's version cannot see a suite in another module.

**A leaf's `SuiteNeverSkipsTest` runs the suite twice, and the second run is the one that earns the no-skipping claim.**
Against an implementation that throws from every method, each test dies on its first call, so an `Assumptions` call
placed anywhere after that is never reached and the skipped count stays zero whether or not the rule was followed.
Running the suite green against a working implementation reaches every line, so a skip anywhere in the suite body shows
up. The event-store leaves have only the failing run today, which is a smaller guarantee than their javadoc claims. The reactor leaf's version has to run the anti-skip
case *through the bridge*, not only against a blocking model: this bridge carries a lifecycle and hands events to a
consumer on some thread, so the risk that the bridge rather than the model becomes the thing under test is real, and a
bridge that swallows a failure or never delivers is exactly what a reactive model honouring nothing would expose.

What has landed so far, and what it found: `CheckpointStorageConformance` holds 15 assertions, and all four blocking
storages passed on first run. That is the outcome to expect from a phase writing down what the storages already agreed
on. The one disagreement is the type-preservation one, which is now declared and asserted in both directions rather
than being folklore about which storage rebuilds what. The four private in-memory copies are gone. The three remaining
copies are reactor ones, and they stay: reactor `CheckpointStorage` is a different interface with no `exists` and a
`Mono` return, so the published blocking class cannot replace them. Phase 7 decides whether a reactor counterpart is
worth publishing.

**This constrains #392 and #388.** A SQL or broker-backed `CheckpointStorage` that leaves a tombstone after `delete`,
or whose `exists` and `read` disagree, now fails a suite rather than being found by a subscription that silently
replays. So does one that cannot store a checkpoint again for a subscription that was cancelled and registered anew.

The cost is CI time, and it is a real cost. These suites add wall-clock to the subscription shards rather than removing
it, and unlike the event-store phases there is no deletion to pay for it. The per-shard budget of 240 seconds only
warns, but the 20 minute timeout does not.
