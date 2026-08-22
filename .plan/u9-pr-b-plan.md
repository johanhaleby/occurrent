# U9 PR B: epic-end fixpoint on the shared push-handover files

Branch `brk/u9-fixpoint-b`. Created from `origin/main` at `feeaeedde` and rebased onto `ae24a9dfc` after an
`origin/main` re-read during the approval round; the three commits in between touch only `.context/ORCHESTRATOR.md`
and `.context/lessons.md`, so no source moved and every line reference below still holds. `feeaeedde` is itself a
`[ci skip]` bookkeeping commit over `50bd66ecd`, ayi's PR 892 squash, which is the tree every line reference below
is against unless stated otherwise.

## 0. What the baseline already changed, and how it was read

`git show --stat 50bd66ecd -- subscription/api/blocking subscription/api/reactor subscription/push/blocking subscription/push/reactor`
touches ten files, 652 insertions. The design it implements is ADR 132 decision 6 to 8 plus the pushed catch-up
signals. Its relevant shape, read directly rather than taken from the brief:

* `ReactiveHandover.Source` gained `historyDone()` (`ReactiveHandover.java:137`) **and** `liveDrained()`
  (`:149`). `BlockingHandover.Source` gained `historyDone()` only (`BlockingHandover.java:133`); there is no
  `liveDrained()` on the blocking engine. The brief's statement that "both handovers call `liveDrained()` after the
  drain" is wrong for the blocking stack: the blocking push model does its post-drain bookkeeping in its own
  `FutureTask` after `handover.catchUp(source)` returns
  (`push/blocking/CatchupThenPushSubscriptionModel.java:341` to `:348`). This matters for item A, because it means
  the reactor model's `forget` runs from inside the engine (`push/reactor/.../CatchupThenPushSubscriptionModel.java:215`)
  while the blocking model's runs from the replay task, and only the blocking one is inside a monitor today.
* Both handovers call `source.historyDone()` at the one point every drain passes: `BlockingHandover.java:441`
  (top of `drainBufferAndGoLive`), `ReactiveHandover.java:396` (before the marker and before the live flip).
* Both push models announce `catchupStarted` inside the step that takes the id: blocking under
  `synchronized (this)` (`push/blocking/...:357` to `:365`), reactor inside `replayingSubscriptions.computeIfPresent`
  on the id (`push/reactor/...:179` to `:187`), which `cancelSubscription`'s own `remove` on the same key also
  serialises against per bin.
* There is no per-id reconciliation marker any more. The per-attempt object is the token: `Future<Boolean> replay`
  on blocking, `Sinks.One<Boolean> replayDone` on reactor.

The token exists on both stacks. What differs is **how much of each stack actually compares it**, which is what
items A, B, C and F turn on.

## 1. Demonstrator tests: all three still fail on the baseline

Extracted the three named hunks out of `demonstrator-tests.patch` (hunks starting at lines 1231, 1396 and 1571)
into `scratchpad/u9-three-demonstrators.patch`, applied cleanly, built with
`./mvnw -o -pl subscription/push/blocking,subscription/push/reactor -am -DskipTests install`, ran, then reverted
the tree to clean.

| Test | Result on `50bd66ecd` | What it pins |
|---|---|---|
| reactor `CatchupThenPushSubscriptionModelOwnershipVerificationTest` | **FAIL** at line 116: `Expecting value to be false but was true` on the catch-up marker | item A: `markCaughtUp` has no ownership check |
| reactor `CatchupThenPushSubscriptionModelPauseStopRaceVerificationTest` | **FAIL** at line 104: `expectComplete` got `onError(SubscriptionNotRunningException: Subscription sub is not running.)` | item D: `applyPendingPauseIfAny` has no `isRunning` guard |
| blocking `CatchupThenPushSubscriptionModelStopStartRaceVerificationTest` | **FAIL** at line 117: folded `["1"]`, expected to contain `"2"` | item F: `start(true)` racing the replay's own `forget` is lost |

U11 closed none of the three. All three are adopted verbatim as this PR's falsifiers.

Note on the reactor ownership test: it fails on the **first** of its four assertions, so it currently proves only
the marker half of item A. The other three assertions (`isCatchingUp`, `feed.isPaused`, `model.isPaused`) are not
yet demonstrated to fail. The plan therefore does not claim them as demonstrated; each gets its own falsifier in
step S3 (see the test table).

## 2. Patch inventory

* `verifier-guard-tests.patch`: **already merged**. `git apply --check` conflicts, and all three method names it
  adds are present in
  `subscription/push/blocking/src/test/java/.../CatchupThenPushSubscriptionModelTest.java` at lines 1062, 1130 and
  1197. Dropped. Its existing
  `a_completing_replay_and_a_concurrent_cancel_plus_resubscribe_never_deadlock_on_the_models_monitor` is kept in
  mind as the guard against this PR widening that monitor into a deadlock (items B, C, F).
* `verifier-895-tests.patch`: six new files, none present on the baseline, `git apply --check` passes. Four of the
  six target findings outside this PR's item list (`CatchupThenPushReadinessAmbiguityTest` and
  `CatchupThenPushReadinessEqualsOverrideTest` are #893 items 2 and 14 in the two starters;
  `KafkaCloudEventBridgeLifecyclePauseRewindTest` and
  `RabbitMqCloudEventBridgePermanentStopReleasesHeldPrefetchTwoTest` are bridge-local). Two,
  `KafkaCloudEventBridgeNestedRefusalRedeliverTest` and `RabbitMqCloudEventBridgeNestedRefusalRedeliverTest`, are
  exactly item H's subject (a refusal that escaped a reentrant handler must not stop the bridge) and are adopted.
  The other four are re-filed on #893 as still-open follow-ups rather than pulled into a PR scoped to the shared
  push-handover files; adopting them would mean adopting their fixes too, which are starter and bridge changes with
  no relation to any item in this brief.

## 3. Assumptions, each verified

| # | Assumption | Status |
|---|---|---|
| 1 | The per-attempt token exists on both stacks and `forget` is identity-scoped on both | VERIFIED: `push/blocking/...:393-397`, `push/reactor/...:286-288` (`remove(key, value)`) |
| 2 | Reactor `markCaughtUp` is not ownership-checked | VERIFIED: `push/reactor/...:206-208` calls `this.markCaughtUp(subscriptionId)` with no guard; blocking wraps it in `completeIfStillOwned` at `:287` |
| 3 | Reactor `keepReplaying` compares by key, not identity | VERIFIED: `push/reactor/...:294-296` `replayingSubscriptions.containsKey(id)`; blocking `:436-438` compares `get(id) == replay` |
| 4 | Reactor `interruptibleReplays.remove` is by key | VERIFIED: `push/reactor/...:234` and `:256`; blocking uses `remove(id, ownLaunch.get())` at `:322` and `:341` |
| 5 | The whole `subscribe` tail is outside any monitor on both stacks | VERIFIED: blocking `:202` (live-feed registration) to `:229`, with only `:357` inside `synchronized (this)`; reactor `:151` to `:157` with nothing synchronized at all |
| 6 | `pauseSubscription` is unsynchronized on both stacks | VERIFIED: blocking `:537-547`, reactor `:389-399`; the completion side is synchronized on blocking (`:405`) and unsynchronized on reactor |
| 7 | Reactor `applyPendingPauseIfAny` lacks the `isRunning` guard | VERIFIED: reactor `:298-302` versus blocking `:443-447`, plus the failing demonstrator |
| 8 | `ReactiveHandover` emits to a unicast sink with no serialisation and one message for every failure kind | VERIFIED: `ReactiveHandover.java:338-341`; `HandoverMessages.bufferOverflow(int, Object)` (`HandoverMessages.java:54-56`) appends the `EmitResult` but keeps the "rebuild offline" wording for all of them |
| 9 | A live-phase failure sets `terminalError` but reaches no log and no caller | VERIFIED: `ReactiveHandover.java:439` does set it, and `bufferOrDeliverLive:306-309` reads it, so round-2 finding 9's "every later `accept` errors with the wrong message forever" is **already closed**. What is open is only the missing log: `catchupDone.tryEmitError` at `:440` is a no-op once `:413` has emitted, and the module has no logger. The code says so itself at `:434-437` |
| 10 | `subscription/api/reactor` has no slf4j dependency | VERIFIED: `subscription/api/reactor/pom.xml` lists core, inmemory-filter-matching, eventstore-api-dcb, reactor-core, jspecify, cloudevents-core plus test scope. `subscription/push/blocking/pom.xml:38-41` is the precedent for adding it |
| 11 | `PARK` already applies to a genuine filter failure on the baseline | VERIFIED: a throwing matcher reports `NOT_DELIVERABLE` and then rethrows (`api/blocking/RegisteringSubscribable.java:404-417`); the exception reaches the bridge's `catch (RuntimeException \| AssertionError)` which calls `routeFailure` (`RabbitMqCloudEventBridge.java:461-467`, `KafkaCloudEventBridge.java:510-517`). Item G's stated motivation is therefore already satisfied; see the ruling |
| 12 | Every `NOT_DELIVERABLE` that returns normally is a lifecycle state today | VERIFIED, and already written down: ADR 133's 2026-08-21 amendment, "a normal-return `NOT_DELIVERABLE` is *always* a lifecycle state today, never a failure" |
| 13 | ADR 133 already promises the enum split as a follow-up | VERIFIED: same amendment, "**The cleaner long-term shape is a distinct `RoutingOutcome` value for a lifecycle refusal** ... Widening the enum is tracked as a follow-up rather than bundled in" |
| 14 | The domain bridges catch the refusal by type with no ownership check | VERIFIED: `domain/RabbitMqDomainEventBridge.java:430` and `domain/KafkaDomainEventBridge.java` catch `BlockingHandover.PreDispatchRefusalException` and call the permanent stop unconditionally |
| 15 | `DomainEventFeed.acceptCloudEvent` never returns `NOT_DELIVERABLE` | VERIFIED: `dsl/projection-dsl/blocking/.../DomainEventFeed.java:287-301` returns `FILTERED`, `DELIVERED` or `DEFERRED` and throws for an unregistered feed |
| 16 | `CatchupProjectionFeed` holds the handover and is package-private-accessible from `DomainEventFeed` | VERIFIED: `CatchupProjectionFeed.java:81` field, `:233` package-private `isReadyForLiveDelivery()`, same package as `DomainEventFeed` |
| 17 | The blocking `DomainEventFeed` "only ever buffers" sentence is gone | VERIFIED: `dsl/projection-dsl/blocking/.../DomainEventFeed.java:178-186` now reads "is never buffered: it is refused outright". Round-2 MINOR closed by PR 889/895, dropped from item J |
| 18 | No reactor consumer needs `acceptRedeliverable` | VERIFIED: all four bridges live under `broker/rabbitmq/blocking` and `broker/kafka/blocking`; `subscription/push/reactor/.../PushSubscriptionModel.java` exposes only `accept`. Dropped from item J |
| 19 | `ReactiveHandoverTest` has no timeout anywhere | VERIFIED: 13 `verifyComplete()` calls with no duration (lines 128, 280, 332, 347, 358, 361, 406, 420, 424, 434, 437, 440, 480) and no `@Timeout` in the class or anywhere in these four test trees |
| 20 | The blocking marker write runs under the model monitor | VERIFIED: `completeIfStillOwned` is `synchronized` (`:405`) and calls `markCaughtUp` (`:415-420`), which does `reader.currentPosition()` and `catchupMarker.save(..)` |
| 21 | A throwing drain delivery discards the rest of the buffer and leaks its `inFlight` keys | VERIFIED: `BlockingHandover.java:444-456` reserves every key under the lock, `:461-463` delivers with no `finally`, so a throw at index `i` leaves keys `i+1..n` in `inFlight` forever and their payloads undelivered |

## 4. Item rulings

Each ruling is against `50bd66ecd`, not against the round-2 report's own line numbers.

### A. Reactor stale-completion ownership (#893 item 9, round-2 blocker 2). FIX HERE, narrowed.

U11 closed **one** of the five sub-defects: `forget` is identity-scoped (`push/reactor/...:286-288`), and
`catchupStarted` / `historyRead` carry the attempt token so a recorder ignores a stale one (`:179-187`, `:222-225`).

Still open, each verified above:

1. `markCaughtUp(subscriptionId)` at `:206-208`: no ownership check. **Demonstrated failing.**
2. `shouldKeepReplaying` at `:294-296`: `containsKey`, so a stale replay keeps folding into a cancelled
   subscription's action once a replacement has installed its own entry.
3. `interruptibleReplays.remove(subscriptionId)` at `:234` and `:256`: by key, so a stale completion evicts the
   replacement's launcher and the replacement can never be relaunched by `start(true)`.
4. `applyPendingPauseIfAny(subscriptionId)` at `:237`: outside any ownership step, so a stale completion applies a
   pause meant for the replacement.
5. `cancelSubscription` at `:430-439`: no monitor, so it cannot be ordered against a completion or a subscribe tail.

**The change**: port the blocking design's *shape*, with one deliberate departure that the reactor engine's own
contract forces. The departure is stated first, because a verbatim port would be a defect.

**Why a verbatim port is wrong here.** The blocking model's ownership token and its "a replay is in flight" record
are the same map entry, because nothing releases that entry until the replay task's own last step. The reactor
model cannot do that: U11 deliberately moved `forget` into the engine's `liveDrained()` hook
(`push/reactor/...:210-216`, "Kept registered until here rather than dropped when the catch-up reports done,
because the payloads buffered while the history was read are delivered after that"), so that `isCatchingUp(id)`
stays true across the drain, which is ADR 132 decision 6. And the engine calls `liveDrained()` **before** it
completes the catch-up signal when nothing was buffered:

```
ReactiveHandover.java:410-413
                    if (remainingInDrain.get() == 0L) {
                        source.liveDrained();
                    }
                    catchupDone.tryEmitValue(true);
```

So for the ordinary case, a catch-up with no live event committed during the replay, `replayingSubscriptions` no
longer holds the token by the time the success branch at `:232-237` runs. A `completeIfStillOwned` gated on
`replayingSubscriptions.get(id) == replayDone` would never fire there, silently stopping `interruptibleReplays`
eviction and the pending pause from ever running. With a non-empty buffer it would fire, because `liveDrained`
then arrives from `countTowardsDrain` (`ReactiveHandover.java:486-488`) on the live stage, after the signal. A
guard that fires or not depending on whether anything was written during the replay is worse than no guard.

**The departure**: the reactor model gets a second, single-purpose map,

```
// Who owns each id's current catch-up attempt. Separate from replayingSubscriptions because that entry is
// released at the drain, by design, while ownership has to outlive it: the success branch runs after the drain
// for an empty buffer and before it for a full one.
private final ConcurrentMap<String, Sinks.One<Boolean>> catchupOwners = new ConcurrentHashMap<>();
```

`catchupOwners` is written only under `synchronized (this)`: put in `launchReplay` beside the
`replayingSubscriptions.put`, removed by `cancelSubscription`, by `shutdown`, and by each of the attempt's own
three exit branches by identity. `replayingSubscriptions` keeps its existing meaning untouched, so `isCatchingUp`,
`isRunning`, `isPaused`, `pauseSubscription`, `relaunchInterruptedReplay` and `awaitReplays` all read exactly what
they read today. Every ownership question moves to `catchupOwners`.

The blocking model does **not** get this second map. It does not need one, and adding it there would be a
speculative symmetry with no defect behind it.

* Add `AtomicReference<Supplier<Sinks.One<Boolean>>> ownLaunch` in `subscribe`, set before
  `interruptibleReplays.put`, passed into `launchReplay`, mirroring blocking `:226-229`.
* Add `private synchronized void completeIfStillOwned(String, Sinks.One<Boolean>, Runnable)`, the same shape as
  blocking `:405-409` but reading `catchupOwners`.
* The source's `markCaughtUp()` becomes

  ```
  return Mono.fromRunnable(() -> completeIfStillOwned(subscriptionId, replayDone,
          () -> CatchupThenPushSubscriptionModel.this.markCaughtUp(subscriptionId).block()));
  ```

  The ownership check and the write are one step under the monitor, which is the blocking twin's design. A shape
  that decides under the monitor and returns the marker `Mono` for the engine to subscribe afterwards was
  considered and rejected: it narrows the stale-write window from unbounded (a parked attempt can sit there for
  minutes) to one store round trip, but it does not close it, and A1 is stated as a closure, not a narrowing. The
  reactor marker write has no second line of defence either, unlike the blocking one, which carries
  `writeConditionFor(subscriptionId)` (`push/blocking/...:418`); the reactor `CheckpointStorage.save` this model
  calls takes no write condition (`push/reactor/...:493-495`). That asymmetry is recorded on #893 rather than
  fixed here, because closing it means a `CheckpointWriteVersionSource` constructor parameter the reactor model
  does not have.

  The `block()` is legitimate here and nowhere else: the whole pipeline already runs on `boundedElastic`, which
  `ReactiveHandover.java:416-419` says exists precisely because "the replay folds through blocking bridges". The
  cost is that the reactor stack now shares the blocking stack's item J property, a store call under the model
  monitor. Item J records it once, for both stacks, with the reason.
* `shouldKeepReplaying(subscriptionId, replayDone)` compares `catchupOwners.get(id) == replayDone` (plus the
  existing `shuttingDown` and `stopped` flags).
* Both `interruptibleReplays.remove` calls become `remove(subscriptionId, ownLaunch.get())`.
* The success branch at `:232-237` becomes
  `completeIfStillOwned(id, replayDone, () -> { interruptibleReplays.remove(id, ownLaunch.get()); applyPendingPauseIfAny(id); })`,
  followed by `catchupOwners.remove(id, replayDone)`. `forget` is **not** moved here; it stays in `liveDrained()`
  where U11 put it, already identity-scoped.
* The stop branch (`:238-244`) and the failure branch (`:247-259`) each add `catchupOwners.remove(id, replayDone)`
  beside their existing `forget`.
* `cancelSubscription`'s `replayingSubscriptions.remove` moves inside `synchronized (this)`, matching blocking
  `:582-584`, and takes `catchupOwners.remove(id)` with it. The `catchupStarted` announcement moves out of
  `computeIfPresent` into the same `synchronized (this)` block as the two puts, matching blocking `:357-365`. The
  `computeIfPresent` trick is then dead and goes; keeping both would be two mutually unaware serialisation schemes
  on the same state.
* `shutdown` clears `catchupOwners` alongside the maps it already clears (`:453-457`).

### B. The `subscribe` tail outside the monitor (#893 item 1, round-2 finding 5). FIX HERE, as stated in finding 5, not as stated in #893.

Verified wider than #893 item 1 says. On blocking, a `cancelSubscription(id)` landing after `:209` and before
`:358` leaves three stale entries, not one: the handover (`:215`, so `isReadyForLiveDelivery(id)` answers for a
cancelled id and the map leaks), the launcher (`:229`, so a later `start(true)` relaunches a cancelled
subscription), and the replay itself, which folds the whole history through the cancelled action and writes the
marker. Reactor has the same hole at `:151-157` with no monitor at all.

**The change**: hold `synchronized (this)` across the whole `subscribe` tail from the live-feed registration
through the `replayingSubscriptions.put`, on both stacks, and move `cancelSubscription`'s remaining removals
(`interruptibleReplays`, `handoversBySubscriptionId`, `pauseRequestedDuringReplay`, and the `liveFeed` cancel)
inside the same monitor so a cancel is one step against a subscribe.

Lock-order argument, which is what makes this safe: every path that holds `this` and then reaches the live feed
takes the live feed's `registrationLock` second, and no path takes `registrationLock` first and then `this`,
because `RegisteringSubscribable` knows nothing about the wrapper. `registrationLock` is acquired at exactly five
sites, `api/blocking/RegisteringSubscribable.java:212, 236, 253, 327, 342`, and none of them calls out to
caller-supplied code; in particular neither `route(CloudEvent)` (`:472-486`) nor
`routeReportingMatch` (`:386-458`) holds it while running a matcher or an action. The existing merged guard test
`a_completing_replay_and_a_concurrent_cancel_plus_resubscribe_never_deadlock_on_the_models_monitor`
(`CatchupThenPushSubscriptionModelTest.java:1062`) is the standing regression for this and must stay green.

Widening the tail nests `launchReplay` inside the monitor, so `Thread.ofVirtual()...start(replay)` (blocking
`:366`) and `handover.catchUp(..)` (reactor `:189`, which subscribes its own pipeline via
`subscribeOn(Schedulers.boundedElastic())` at `ReactiveHandover.java:419`) both end up inside it. That is safe and
the plan keeps them there rather than restructuring: neither call joins anything. A virtual-thread `start` returns
immediately, and `subscribe` on a `subscribeOn`ed pipeline returns immediately. The replay itself needs the monitor
only at its own boundaries, by which time the subscribing thread has long released it.

This is not new. `relaunchInterruptedReplay` is already `synchronized` on both stacks (blocking `:378`, reactor
`:272`) and already calls `launch.get()` (blocking `:389`, reactor `:283`) under the monitor, for the
check-and-launch atomicity its own comment at blocking `:374-377` exists to give. Item F's snippet does the same
thing from the replay's own stop path, so it inherits a property the baseline already has rather than introducing
one.

What must **not** happen is a thread holding the monitor while *waiting* for a fold. The only method that does
that today is `shutdown()`, via `awaitReplays` (blocking `:604`, reactor `:452`). `shutdown()` stays
unsynchronized for exactly that reason, and the plan says so rather than leaving it to be discovered.

### C. `pauseSubscription`'s request write outside the monitor (#893 item 4, round-2 finding 6). FIX HERE.

Verified on both stacks. Blocking `:537-547` reads `replayingSubscriptions.containsKey` and writes
`pauseRequestedDuringReplay` with no monitor, while the completion consumes it under one (`:405`, `:443-447`).
Reactor `:389-399` has the same shape and no monitor to lose.

**The change**: make `pauseSubscription` `synchronized` on both stacks, and `resumeSubscription` too, because its
own read-then-remove of `pauseRequestedDuringReplay` (blocking `:559-573`, reactor `:411-425`) reads the replay map
in between and would otherwise disagree with a pause taken under the monitor. `relaunchInterruptedReplay` is
already `synchronized` and reentrancy makes that a no-op change for it.

### D. Reactor `applyPendingPauseIfAny` lacks the `isRunning` guard (round-2 finding 7). FIX HERE. Demonstrated.

`push/reactor/...:298-302` versus blocking `:443-447`, whose comment states exactly why the guard exists.

**The change**: `if (pauseRequestedDuringReplay.remove(id) != null && liveFeed.isRunning(id))`, one condition,
matching the blocking twin verbatim. The demonstrator
`CatchupThenPushSubscriptionModelPauseStopRaceVerificationTest` flips from red to green on it.

### E. `ReactiveHandover` emit classification and the unlogged live-phase failure (#893 item 10, round-2 findings 8 and 9). FIX HERE, narrowed on two counts.

Narrowing 1: the message already carries the `EmitResult` (`ReactiveHandover.java:340` calls
`HandoverMessages.bufferOverflow(maxBufferedEvents, result)`), so an operator is no longer diagnosing blind. What
is still wrong is the wording and the outcome: every failure is still called a buffer overflow that wants an
offline rebuild, and every one still becomes `NOT_DELIVERABLE`.

Narrowing 2: round-2 finding 9's second half is already closed. The error handler sets `terminalError`
(`:439`) and `bufferOrDeliverLive` reads it first (`:306-309`), so a later payload gets the honest catch-up-failed
refusal, not the bogus overflow message. The open half is only that the live-phase error reaches no log and no
caller, because `catchupDone.tryEmitError` at `:440` is a no-op once `:413` emitted.

**The change**:

* Serialise the emission. `Sinks.many().unicast()` requires externally serialised producers and
  `bufferOrDeliverLive` is called from every live thread. Wrap the `tryEmitNext` in a dedicated
  `private final Object emitLock = new Object()`, not `Sinks.unsafe` plus a busy handler: the lock is held for one
  queue offer, never across a fold, so it cannot serialise delivery, and a busy-spin handler would turn a genuine
  overflow into a livelock.
* Classify the result rather than calling everything an overflow:
  * `FAIL_OVERFLOW` and `FAIL_ZERO_SUBSCRIBER`: the existing overflow refusal, unchanged.
  * `FAIL_TERMINATED` and `FAIL_CANCELLED`: `ackSink.success(false)`. The pipeline is gone, so nothing will fold
    this payload; that is the dropped-not-deferred answer `stopped` already gives two branches earlier, reached
    here only as a race against that same flag.
  * `FAIL_NON_SERIALIZED`: with the lock above this is unreachable, so it gets its own message naming an engine
    defect rather than telling an operator to rebuild a read model offline. A new
    `HandoverMessages.concurrentEmission()` string, so the two reactor callers cannot drift.
* Give `subscription/api/reactor` an slf4j-api dependency (precedent: `subscription/push/blocking/pom.xml:38-41`)
  and log the live-phase error at error level in the `catchUp` error handler, replacing the "Known gap" comment at
  `:434-437` with what the code now does.

Not doing: moving the completion signal off the marker phase so the live phase can report through `catchupDone`.
That is a reordering of the documented, deliberate difference between the two engines
(`ReactiveHandover.java:53-62`), it would change when a caller's `catchUp` `Mono` completes, and the ADR 132
work that just landed depends on that ordering. Logging plus the already-present `terminalError` closes the loss;
the reordering is an ergonomics change and is filed on #893.

### F. `stop()` then `start(true)` racing the replay's own `forget` (round-2 finding 10). FIX HERE. Demonstrated on blocking.

Blocking: `relaunchInterruptedReplay` (`:378-390`) returns null while `replayingSubscriptions.containsKey(id)` is
still true, and the stop path clears that entry afterwards at `:333`. The window spans
`abandonReplayWithoutMasking` (`BlockingHandover.java:396`), which for a replay-aware view is a store call. Net
state: launcher present, nothing replaying, `stopped == false`, nothing will ever call the launcher again. Reactor
has the identical shape at `:243`.

**The change**: make the stop path's own clear and re-check one step under the model monitor. In the
`!caughtUp` branch (blocking `:326-335`, reactor `:238-244`):

```
synchronized (this) {
    forget(subscriptionId, self.get());
    if (!stopped && !shuttingDown) {
        relaunchInterruptedReplay(subscriptionId);
    }
}
```

and have `start(boolean)` set `stopped = false` under the same monitor before it iterates `interruptibleReplays`.
Then exactly one of the two relaunches: whichever takes the monitor second sees `replayingSubscriptions` already
holding an entry (`relaunchInterruptedReplay` checks it, `:380`) and returns null. `relaunchInterruptedReplay` is
already `synchronized`, so the nesting is reentrant.

The alternative shape, a pending-relaunch flag `start(true)` sets and the completion honours, was rejected: it adds
a fourth per-id map for a state the two existing maps already encode jointly, and the joint reading
("launcher present, nothing replaying") is what the code documents at blocking `:330-332`.

### G. A distinct lifecycle `RoutingOutcome` (#893 item 12). FIX HERE, with the motivation corrected.

**The stated motivation is already satisfied on the baseline.** "PARK can apply to a genuine filter failure again"
is true today: a throwing matcher reports `NOT_DELIVERABLE` and then rethrows
(`api/blocking/RegisteringSubscribable.java:404-417`), and the rethrow lands in each bridge's
`catch (RuntimeException | AssertionError)`, which calls the failure policy
(`RabbitMqCloudEventBridge.java:458` catch and `:463` `routeFailure`, `KafkaCloudEventBridge.java:510-517`). PR A did not make every
`NOT_DELIVERABLE` hold-and-redeliver; it made every **quietly returned** `NOT_DELIVERABLE` hold-and-redeliver, and
the loud ones never reach that branch. So this item is not a live defect.

**It is still worth doing, for a reason the repository already wrote down.** ADR 133's 2026-08-21 amendment states
the invariant the four bridges depend on ("a normal-return `NOT_DELIVERABLE` is *always* a lifecycle state today,
never a failure") and names the follow-up ("The cleaner long-term shape is a distinct `RoutingOutcome` value for a
lifecycle refusal ... Widening the enum is tracked as a follow-up rather than bundled in"). Today that invariant is
prose in an ADR and in two bridge class javadocs, describing another module's control flow. The enum is unreleased.
This is the last cheap moment to move it into the type.

**Shape decided: two new enum constants, not a richer report.** A richer report would change
`BiConsumer<CloudEvent, RoutingOutcome>` on both stacks, both `PushObserver`s and every observer implementation, to
carry information that most values do not have. Rejected.

Today's `NOT_DELIVERABLE` covers three situations that call for three different caller actions, and the enum's own
javadoc says the difference between values "is only what a caller should do next". They split three ways:

> **`UNAVAILABLE`** (new). No registration was in a state to be asked: the model is not running, the sole
> subscription is paused, or nothing is registered. No filter and no handler ran, and nothing was thrown. Always
> safe to offer again, and the reason can change on its own, so a caller paces rather than parking.
>
> **`NOT_DELIVERABLE`** (narrowed). The matcher was asked and threw instead of answering, so the event was neither
> declined nor delivered. The matcher's exception propagates. A caller's own failure policy decides what happens
> next, which is what this value has always meant in its first sentence.
>
> **`REFUSED`** (new). A registered action refused this event before attempting dispatch, reported as
> `RoutingAction.Refusal`. The wrapped cause propagates. The engine that raised it already knows the refusal is
> not going to resolve by redelivery, so a caller stops rather than parking or redelivering into it.

**Why `REFUSED` earns its place, having first been rejected.** The plan's earlier draft added `UNAVAILABLE` alone
and argued the two CloudEvent bridges' `catch (BlockingHandover.PreDispatchRefusalException)` plus
`outcome != NOT_DELIVERABLE` check (`RabbitMqCloudEventBridge.java:435-441`, `KafkaCloudEventBridge.java:486-494`)
was exact enough. The fresh-context review showed it is not. A *matcher* that throws a
`PreDispatchRefusalException` also reports `NOT_DELIVERABLE` and also rethrows
(`api/blocking/RegisteringSubscribable.java:404-406`, `:416`), so it satisfies both halves of the bridge's test and
the bridge stops permanently for a filter defect it should have parked. Item H's identity check does not reach
that branch, because it is on the action side. With `REFUSED`, the bridge tests `outcome == REFUSED`, which is
reported at exactly one site per stack, and the matcher branch cannot satisfy it.

That also removes the wart ADR 133's amendment apologised for. With the decision made on the outcome, neither
CloudEvent bridge needs `catch (BlockingHandover.PreDispatchRefusalException)` at all: one
`catch (RuntimeException | AssertionError)` plus `outcome == REFUSED` replaces both branches, and the
`org.occurrent.subscription.api.blocking.internal` import goes with it.

**What `REFUSED` actually implies at a bridge, checked rather than assumed.** `PreDispatchRefusalException` has
three throw sites (`BlockingHandover.java:233` catch-up failed, `:256` buffer overflow, `:470` null dedup key). A
bridge reaches the handover only through `acceptIfLive`, on all four bridges: the two CloudEvent bridges call
`model.acceptRedeliverable(cloudEvent)` (`RabbitMqCloudEventBridge.java:430`, `KafkaCloudEventBridge.java:484`),
which passes `bufferIfNotLive` false and so selects `acceptIfLive` (`push/blocking/...:205`); the two domain
bridges call `feed.acceptCloudEvent(cloudEvent)` (`domain/RabbitMqDomainEventBridge.java:421`,
`domain/KafkaDomainEventBridge.java:478`), which reaches `catchupFeed.acceptIfLive(..)`
(`DomainEventFeed.java:300`). `acceptIfLive` (`BlockingHandover.java:285-315`) never touches the buffer, so `:256`
is unreachable from any of them. The two reachable causes are a permanently failed catch-up and a dedup-key function
that returned null, both permanent, so "stop, never park" is right for both. The null-key case is unreachable for
the push models, whose key function is `CloudEvent::getId`, but is reachable for a projection feed, whose key
function is caller-supplied (`CatchupProjectionFeed.java:98-99`). `REFUSED`'s javadoc says "not going to resolve
by redelivery" rather than "the catch-up failed", so it covers both without overclaiming.

**Consumer-side adaptation.** The list below comes from `grep -rln RoutingOutcome .` over the whole repository
excluding `target/`, which returns 63 files. Production sources, docs and the tests that assert an outcome are all
enumerated; the `pom.xml` hits are dependency comments and the `.context/` hits are orchestrator bookkeeping,
neither of which is a consumer.

* **`subscription/core/.../RoutingOutcome.java` itself, which is the primary edit of this item.** Its class javadoc
  says "All four come out of one evaluation" (`:32`) and describes `NOT_DELIVERABLE` as covering "every other
  reason the event was not actually consumed" (`:25-27`). `NOT_DELIVERABLE`'s own javadoc leads with the exact
  lifecycle sentence being moved out: "Nothing is registered, the model is not running, or the sole subscription is
  paused, so the filter was never asked" (`:59-60`), followed by "A filter that was asked and threw instead of
  answering reports this too" (`:60-62`), which is the sentence that stays. Six values, six javadoc blocks, one
  class javadoc.
* The two lifecycle report sites on each stack become `UNAVAILABLE`: `api/blocking/RegisteringSubscribable.java:398`
  (paused) and `:457` (not running, or nothing registered); `api/reactor/RegisteringSubscribable.java:393` and
  `:460`. The refusal site on each stack becomes `REFUSED`: blocking `:431`, reactor `:434`. The matcher-threw site
  keeps `NOT_DELIVERABLE`: blocking `:406`, reactor `:401`. The method javadoc on each stack (blocking `:358`,
  `:360`; reactor `:352`, `:354`) and the `Refusal` javadoc (blocking `:126`, reactor `:121`) are edited with them.
* `RabbitMqCloudEventBridge` and `KafkaCloudEventBridge`: the held-and-paced branch
  (`RabbitMqCloudEventBridge.java:487`, `KafkaCloudEventBridge.java:534`) becomes `DEFERRED || UNAVAILABLE`. The
  `catch (BlockingHandover.PreDispatchRefusalException)` branch (`RabbitMqCloudEventBridge.java:435-459`,
  `KafkaCloudEventBridge.java:486-509`) is deleted and folded into the generic
  `catch (RuntimeException | AssertionError)`, which now reads the outcome and takes the permanent stop when it is
  `REFUSED`. The `import ...api.blocking.internal.BlockingHandover` goes from both files, and the class-javadoc
  paragraphs that apologise for it (`RabbitMqCloudEventBridge.java:118-133`, `KafkaCloudEventBridge.java:151-161`)
  are rewritten to describe the outcome instead. The defensive `else` (`RabbitMqCloudEventBridge.java:500-506`,
  `KafkaCloudEventBridge.java:544-548`) keeps its shape; its comment stops saying "exhaustively DELIVERED,
  FILTERED, DEFERRED or NOT_DELIVERABLE today".
* Both `PushObserver` javadocs and both `PushObserverNoop`s: the outcome enumeration.
* `RabbitMqDeliveryFailureAction.java:190-191`, whose javadoc names "a lifecycle
  {@link RoutingOutcome#NOT_DELIVERABLE}" as one of the two things the held-tag path exists for. That is now
  `UNAVAILABLE`.
* Both `RoutingOutcomeChannel` javadocs (`broker/rabbitmq/blocking/.../RoutingOutcomeChannel.java` and the Kafka
  copy). Round-2's MINOR that these still name `accept(...)` is **already closed**: `:41`, `:46`, `:80` and `:81`
  all read `acceptRedeliverable(...)` today. Only the outcome enumeration needs the new values.
* Both `DomainEventFeed`s' "refused rather than reported as `NOT_DELIVERABLE`" sentences (blocking `:262-266`,
  reactor `:266`): the accurate value is now `UNAVAILABLE`.
* Both domain bridges' class javadocs, which describe a `NOT_DELIVERABLE` they can never receive
  (`domain/RabbitMqDomainEventBridge.java:69`, `domain/KafkaDomainEventBridge.java:65`) and their matching
  "Unreachable today: DomainEventFeed#acceptCloudEvent never returns NOT_DELIVERABLE" comments
  (`domain/RabbitMqDomainEventBridge.java:462`, `domain/KafkaDomainEventBridge.java:514`). Corrected to name the
  `IllegalStateException` instead, which is round-2 finding 17's second half.
* The three starter files per transport that name the enum (`CatchupThenPushReadiness`,
  `*CloudEventBridgeFactory`, `Default*CloudEventBridgeFactory`), javadoc only; none of them branches on a value.
* `example/broker/rabbitmq/.../RabbitMqCloudEventLevelBootstrap.java`, javadoc and wiring comment only.
* Tests that assert an outcome, on both stacks and in both bridge modules:
  `RegisteringSubscribableRouteReportingMatchTest` (blocking and reactor), `PushSubscriptionModelTest` (both),
  `CatchupThenPushSubscriptionModelTest` (both), `DomainEventFeedTest` (both), and the nine bridge and starter
  tests that reference `RoutingOutcome` under `broker/**`.
* ADR 133 in three places: a new amendment recording that the follow-up its 2026-08-21 lifecycle amendment named
  (`:904-911`) is now done; decision 1's own enumeration at `:123-124` ("`DELIVERED`, `FILTERED` and
  `NOT_DELIVERABLE` at the time this decision was written (a fourth value, `DEFERRED`, was added by the 2026-08-21
  amendment below)"); and `:810`, in the *other* 2026-08-21 amendment, which reads "Decision 1's own prose above,
  describing a three-valued outcome reported before the matched action ran". The earlier draft of this plan
  attributed that phrase to the lifecycle amendment; it is not there.
* `changelog.md` lines 27 and 31, both of which spell the outcome semantics out at length and both of which name
  `NOT_DELIVERABLE` as the lifecycle value. Line 27: "`NOT_DELIVERABLE` when there was no running, unpaused
  subscription for the event to reach at all ... A filter that throws while being evaluated also reports
  `NOT_DELIVERABLE`". Line 31: "A lifecycle `NOT_DELIVERABLE`, the sole subscription paused or the model not
  running at all, is held and redelivered paced the same way `DEFERRED` already is". Both are edited in place
  rather than given a new entry, since none of this has shipped.

### H. The refusal carries its owner (#893 items 5 and 13). FIX HERE, split in two, because the two halves need different mechanisms.

**H1, #893 item 5, the push models' `Refusal` wrap.** Blocking `push/blocking/...:206` catches
`BlockingHandover.PreDispatchRefusalException` **by type**, so one thrown by a different handover that a reentrant
handler touched is wrapped as `Refusal` and reported `NOT_DELIVERABLE` although this handler genuinely ran. The
reactor twin has the same defect in a different spelling: `onErrorMap(ReactiveHandover.PreDispatchRefusalException.class, Refusal::new)`
at `push/reactor/...:150`. With item G in place this misclassification is what makes a CloudEvent bridge stop
permanently on a failure that is not its own, so H1 is a prerequisite for G rather than a nicety.

The change: both `PreDispatchRefusalException`s carry the handover that threw them, and gain
`public boolean thrownBy(BlockingHandover<?> handover)` (reactor: `ReactiveHandover<?>`), comparing by identity.
Both constructors take the owner. Both push models compare before wrapping; an exception from a foreign handover
propagates untouched and is reported `DELIVERED`, which is what a handler that ran and threw has always meant.

**H2, #893 item 13, the domain bridges.** `domain/RabbitMqDomainEventBridge.java:430` and
`domain/KafkaDomainEventBridge.java:487` both catch the refusal by type and stop permanently with no ownership
check ("The projection registered on this feed has a permanently failed catch-up. Stopping this bridge and leaving
its consumer group ..." at `KafkaDomainEventBridge.java:491-494`, unconditional), so a foreign refusal escaping a
handler stops a healthy bridge for good. They have no `RoutingOutcomeChannel` to read:
`DomainEventFeed.acceptCloudEvent` delivers inline and returns its outcome (`:287-301`).

The change: expose the fact the bridge actually needs, monotone so that reading it after the catch cannot be wrong.
`BlockingHandover` gains `public boolean hasFailedCatchUp()` (`catchUpFailure != null`, under the lock),
`CatchupProjectionFeed` a package-private pass-through beside its existing `isReadyForLiveDelivery()` (`:233`), and
`DomainEventFeed` a public `hasFailedCatchUp()`. Each domain bridge then drops its
`catch (BlockingHandover.PreDispatchRefusalException)` entirely, along with the internal import, and its generic
`catch (RuntimeException | AssertionError)` takes the permanent stop when `feed.hasFailedCatchUp()` is true and the
ordinary failure policy otherwise. That also covers a refusal that arrives as some other exception type, which the
type-based catch could not.

For an unregistered feed `hasFailedCatchUp()` returns `false`, following `isReadyForLiveDelivery()` (`:191-192`)
rather than the `IllegalStateException` `accept(..)` throws, for the reason that method's javadoc already gives: a
listener has to be able to ask both together before anything is registered.

`isReadyForLiveDelivery()` is deliberately **not** reused for this: it is also false while a replay is running
(`DomainEventFeed.java:184-186` documents exactly that), so a foreign refusal arriving in that window would stop
the bridge for someone else's failure. `hasFailedCatchUp()` is false until this feed's own catch-up throws and true
forever after (`catchUpFailure` is written once at `BlockingHandover.java:422` and cleared nowhere), so a read
taken after the catch is either the answer that was true at throw time or a later one that also justifies
stopping. That monotonicity is the whole argument for reading it out of band, and it is stated in its javadoc.

**Blocking only.** There is no reactor domain bridge, so `ReactiveHandover`, the reactor `CatchupProjectionFeed`
and the reactor `DomainEventFeed` get no `hasFailedCatchUp()`. That is the same rule item J applies to
`acceptRedeliverable`, and an earlier draft of this plan broke it. `ReactiveHandover` would also have had to answer
it from `terminalError` (`:190`), which the same handler sets for a live-phase failure as for a catch-up-phase one
(`:432-439`), so the name would have claimed "catch-up" for something else. `thrownBy` on the reactor
`PreDispatchRefusalException` still ships, because H1's reactor half genuinely uses it.

**Why the two halves use different mechanisms, stated rather than left to be noticed.** A CloudEvent bridge feeds
a model that performs a routing evaluation, and that evaluation reports an outcome, so `REFUSED` is available to
it. A domain bridge feeds a `DomainEventFeed`, which delivers inline; its returned `RoutingOutcome` describes the
delivery, not a routing decision, and it never sees a `RoutingAction.Refusal` at all. So the feed exposes the state
instead. Both bridges end up deciding from something the owning component computed, and neither imports an
`internal` type.

The two adopted tests from `verifier-895-tests.patch`
(`RabbitMqCloudEventBridgeNestedRefusalRedeliverTest`, `KafkaCloudEventBridgeNestedRefusalRedeliverTest`) cover the
CloudEvent side of the same property; the domain side gets two new twins.

### I. The timeout-less `StepVerifier` (#893 item 11). FIX HERE, widened by one line.

`ReactiveHandoverTest` has 13 `verifyComplete()` calls with no duration and no `@Timeout` anywhere. Fixing only
`acceptIfLive_refuses_without_buffering_when_not_live` (`:332`) would leave 12 ways to hang the build.

The change: `@Timeout(30)` on the class. One line, bounds every method including the latch awaits, and does not
require touching 13 assertions.

### J. The round-2 MINORs on these files.

| Sub-item | Ruling |
|---|---|
| `drainBufferAndGoLive` discards remaining buffered payloads and leaks `inFlight` keys on a throwing delivery (`BlockingHandover.java:444-463`) | **FIX HERE.** Wrap the delivery loop so a throw releases the reservations for the payloads it never reached. The payloads themselves stay undelivered, which is correct: `catchUp` records `catchUpFailure` for the same throw, every later payload is refused, and the source redelivers. Leaking the keys is what would make that redelivery silently skipped, so releasing them is the fix, not redelivering inline |
| `markCaughtUp` runs a store write under `synchronized (this)` (blocking `:405`, `:415-420`), and after item A the reactor port shares that property | **NOT HERE, recorded, and now true of both stacks.** Moving the write outside the monitor reopens exactly the stale-marker defect item A exists to close (see item A's rejected alternative), and the fix that would allow both (a per-subscription monitor instead of the model-wide one) is a redesign of the monitor that items A, B, C and F all build on. It is a liveness cost against a hung store, bounded to the one subscription's model, not a correctness defect. Filed on #893 with this reasoning, naming the per-subscription monitor as the shape that closes it |
| Blocking `DomainEventFeed` "only ever buffers" javadoc | **ALREADY CLOSED.** `dsl/projection-dsl/blocking/.../DomainEventFeed.java:178-186` now says "is never buffered: it is refused outright". Dropped |
| Reactor asymmetries (no `acceptRedeliverable` on the reactor `PushSubscriptionModel`) | **DROPPED, no consumer.** All four bridges are blocking. Adding it would be an API for a caller that does not exist |

## 5. Invariant table

Each row is a property over interleavings, followed by every surface it is checked on. "Attempt" means one
`launchReplay` call and the token it created.

| # | Invariant | Checked on |
|---|---|---|
| A1 | For any interleaving of `cancelSubscription(id)`, `subscribe(id, ..)` and a replay completion, an attempt that no longer owns `id` performs no side effect scoped to `id`: no marker write, no `interruptibleReplays` eviction, no pause application, no `replayingSubscriptions` removal, and no further fold | reactor `completeIfStillOwned` over `catchupOwners`, identity `shouldKeepReplaying`, `remove(key,value)` on `interruptibleReplays` and `replayingSubscriptions`; blocking already has all three over its single map; reactor ownership demonstrator plus four new falsifiers (one per assertion it does not yet reach, plus the empty-buffer one); blocking `CatchupThenPushSubscriptionModelTest:1062,1130,1197` stay green |
| A2 | A marker write happens only for the attempt that holds `id` for the whole duration of the write, on both stacks. There is no interleaving in which an attempt observes ownership and then writes after losing it. This is the same property as A1's first clause, kept as its own row only because it is the one the adopted demonstrator currently reaches | ownership check and write in one monitored step on both stacks; reactor ownership demonstrator, which fails today at its line 116 |
| B1 | For any interleaving, a `cancelSubscription(id)` either observes none of a concurrent `subscribe(id, ..)`'s state (handover, launcher, replay entry, live registration) or all of it. There is no observable state in which some are installed and others are not | blocking and reactor `subscribe` tail under the monitor, `cancelSubscription` fully under it; a new falsifier per stack parking a cancel in the window |
| B2 | **Reasoned, not tested.** No thread holds the model monitor while waiting for a fold or a replay to finish. A virtual-thread start and a `subscribeOn`-deferred `subscribe` under the monitor are permitted, because neither joins; `relaunchInterruptedReplay` already does both on the baseline. Every path that holds the monitor and reaches the live feed takes `registrationLock` second, and no path takes `registrationLock` first | Read of every `synchronized` method against `RegisteringSubscribable`'s five `registrationLock` acquisitions (`api/blocking/RegisteringSubscribable.java:212, 236, 253, 327, 342`), none of which calls caller-supplied code; `shutdown()` left unsynchronized. The merged `...never_deadlock_on_the_models_monitor` test is a deadlock guard, not a monitor-extent guard: it catches a violation that deadlocks and nothing else, and the plan does not claim more from it |
| C1 | A pause requested for `id` is either applied to the live feed or still pending in `pauseRequestedDuringReplay`; it is never both dropped and reported by `isPaused(id)` | `pauseSubscription` and `resumeSubscription` synchronized on both stacks; a new falsifier per stack parking the pause against the completion |
| D1 | A pending pause is applied only when the live feed reports the subscription running, so a completed catch-up is never reported failed because the pause threw | reactor `applyPendingPauseIfAny` guard; reactor pause-stop demonstrator |
| E1 | **Reasoned, not tested.** Every `tryEmitNext` on the live sink is serialised against every other, so `FAIL_NON_SERIALIZED` is unreachable | `emitLock` is the only path to `tryEmitNext`, which is a read of one method. A concurrent-emit test is added as a smoke check but is explicitly **not** this row's falsifier: removing `emitLock` would not reliably turn it red, so claiming it as a mutation-proven test would be false |
| E2 | A live payload is refused with a message that names what actually failed, and a terminated sink completes the ack `false` rather than erroring with an overflow diagnosis | the `switch` on `EmitResult`; one test per branch driving the sink into each state |
| E3 | A live-phase failure is logged at error exactly once and every later payload is refused with the catch-up-failed message | the new logger plus the already-present `terminalError`; a test asserting both |
| F1 | For any interleaving of `stop()`, a replay's own decision to stop, and `start(true)`, the end state is never "launcher present, nothing replaying, model started". Exactly one relaunch happens | the monitor-guarded clear-then-recheck plus `start`'s flag write under the same monitor; blocking stop-start demonstrator plus a reactor twin |
| G1 | A `RoutingOutcome` reported without a propagating exception is `DELIVERED`, `FILTERED`, `DEFERRED` or `UNAVAILABLE`, never `NOT_DELIVERABLE` | both `routeReportingMatch` tests, extended to assert the value **and** whether an exception propagated, for every reporting branch on each stack: paused, matcher declined, matcher threw, action refused, action threw, action landed, action declined, and the not-running fallthrough |
| G2 | A bridge acknowledges only on `DELIVERED` or `FILTERED`, holds on `DEFERRED` or `UNAVAILABLE`, stops permanently only on `REFUSED`, and applies its failure policy on everything else | the four bridges' existing outcome tests, extended for `UNAVAILABLE` and `REFUSED` |
| G3 | A matcher that throws a `PreDispatchRefusalException` never makes a CloudEvent bridge stop permanently | a new test per CloudEvent bridge whose `DataFieldReader` throws that exact type from the matcher; must park or redeliver, never stop |
| H1 | A `PreDispatchRefusalException` is wrapped as `RoutingAction.Refusal` only by the handover that threw it | `thrownBy` identity check on both stacks; the two adopted nested-refusal bridge tests plus a unit test per push model |
| H2 | A domain bridge stops permanently only when its own feed's catch-up has failed, and `hasFailedCatchUp()` never goes back to false | `hasFailedCatchUp` pass-through; two new domain-bridge nested-refusal tests |
| J1 | A delivery that throws mid-drain leaves no dedup key reserved, so a redelivery of any undelivered buffered payload is still evaluated | `BlockingHandover` drain `finally`; a test that throws on the second of three buffered payloads and then redelivers all three |

## 6. Public API summary

Every signature is decided here. Nothing is left to implementation.

**New, in `subscription/core`:**

```java
public enum RoutingOutcome { DELIVERED, FILTERED, NOT_DELIVERABLE, DEFERRED, UNAVAILABLE, REFUSED }
```

Six values, one caller action each: acknowledge (`DELIVERED`, `FILTERED`), hold and pace (`DEFERRED`,
`UNAVAILABLE`), apply the failure policy (`NOT_DELIVERABLE`), stop (`REFUSED`).

**New, in `subscription/core` `HandoverMessages`:**

```java
public static String concurrentEmission();
```

**New, in `subscription/api/blocking` `BlockingHandover`:**

```java
public boolean hasFailedCatchUp();
public static final class PreDispatchRefusalException extends IllegalStateException {
    public boolean thrownBy(BlockingHandover<?> handover);
}
```

**New, in `subscription/api/reactor` `ReactiveHandover`:**

```java
public static final class PreDispatchRefusalException extends IllegalStateException {
    public boolean thrownBy(ReactiveHandover<?> handover);
}
```

No `hasFailedCatchUp()` on the reactor handover: there is no reactor domain bridge to call it, and it would have
to answer from `terminalError`, which the same handler sets for a live-phase failure (`ReactiveHandover.java:432-439`).

Both `PreDispatchRefusalException` constructors gain the owner as their first parameter. They stay package-private,
so this is not a source-compatible concern for anyone outside the engine.

**New, in `dsl/projection-dsl/blocking` `DomainEventFeed`:**

```java
public boolean hasFailedCatchUp();   // false for an unregistered feed, like isReadyForLiveDelivery()
```

**New, package-private, in the blocking `CatchupProjectionFeed`:**

```java
boolean hasFailedCatchUp();
```

**New, in `subscription/push/reactor` `CatchupThenPushSubscriptionModel`:** nothing public. The `catchupOwners` map
item A adds is private.

**Changed signatures:** none. **Removed:** none. **Behaviour changes visible to a caller:** `routeReportingMatch`
reports `UNAVAILABLE` where it previously reported `NOT_DELIVERABLE` at two call sites per stack covering three
states (paused; model not running; nothing registered), and `REFUSED` at one call site per stack. Everything else
is a refinement of when an existing value is reported.

Every addition above has a caller inside this PR, so item J's "no consumer, dropped" rule for the reactor
`acceptRedeliverable` and this section do not contradict each other.

## 7. Sequencing

Each step leaves the tree buildable and its own tests green. Each is one commit.

**S1. `RoutingOutcome.UNAVAILABLE` and `REFUSED`, and every consumer named in item G.** The two constants and the
enum's own javadoc first, then both `routeReportingMatch` implementations, both `PushObserver` javadocs, both
`RoutingOutcomeChannel` javadocs, `RabbitMqDeliveryFailureAction`'s javadoc, both `DomainEventFeed` javadocs, both
domain bridges' javadocs and unreachability comments, the two starters' javadoc mentions, the example bootstrap,
and every test that asserts the old value. For a bridge, `UNAVAILABLE` is held and paced exactly where a quiet
`NOT_DELIVERABLE` was, so that half is behaviour-preserving. `REFUSED` is not: the two CloudEvent bridges lose
their `catch (BlockingHandover.PreDispatchRefusalException)` and the `internal` import with it, and take the
permanent stop from the outcome. Buildable throughout because the enum grows rather than changes and no consumer
switches over it.

**S2. The refusal carries its owner (H1), and the domain bridges' gate (H2).** Both handovers' exception gains its
owner and `thrownBy`; both push models' `Refusal` wrap identity-checks; `BlockingHandover.hasFailedCatchUp()`, the
blocking `CatchupProjectionFeed` pass-through and the blocking `DomainEventFeed` accessor; both domain bridges drop
their type-based catch for the gated one. Adopt the two nested-refusal bridge tests from `verifier-895-tests.patch`,
add the two domain twins, and add the G3 matcher test. Ordered after S1 because the G3 test asserts on `REFUSED`.

**S3. The reactor ownership work (A) plus the reactor monitor (B, C reactor half, F reactor half).** One step
because they are the same monitor: introducing `synchronized (this)` on the reactor model without also moving
`cancelSubscription`, `pauseSubscription`, `resumeSubscription` and the stop-path recheck onto it would leave a
half-guarded model, which is worse than none. The `catchupOwners` map is introduced here, with its own field
comment stating why the reactor stack needs a second map and the blocking stack does not. Adopts the reactor
ownership demonstrator and the reactor pause-stop demonstrator, plus the four new ownership falsifiers, the reactor
subscribe-tail falsifier, the reactor pause-race falsifier and the reactor stop-start falsifier.

**S4. The blocking monitor widening (B blocking half, C blocking half, F blocking half).** The subscribe tail and
`cancelSubscription` under one monitor, `pauseSubscription` and `resumeSubscription` synchronized, the stop path's
clear-then-recheck. Adopts the blocking stop-start demonstrator plus the blocking subscribe-tail and pause-race
falsifiers. Separate from S3 so a deadlock introduced by widening one stack's monitor is bisectable to that stack.

**S5. `ReactiveHandover` emit classification and logging (E), and the blocking drain `finally` (J1).** The
`emitLock`, the `EmitResult` switch, `HandoverMessages.concurrentEmission()`, the slf4j dependency and the live-phase
log, plus `drainBufferAndGoLive`'s reservation release. Both are handover-engine-local and touch no model.

**S6. `@Timeout` on `ReactiveHandoverTest` (I), docs and changelog.** The three ADR 133 edits named in item G, the
changelog edits to lines 27 and 31, and the #893 comment recording what this PR does not do (the marker-write
monitor on both stacks, the reactor marker's missing write condition, the four unadopted
`verifier-895-tests.patch` files, the `catchupDone` reordering).

No prefactoring is proposed. The one candidate, extracting a shared ownership base for the two models, is refused:
they are parallel implementations in different modules over different primitives (a `Future` and a
`Sinks.One`), and item A establishes that their ownership records are not even the same shape, since the reactor
engine releases its replaying entry at the drain and the blocking one does not. A shared base would have to hide
that difference, which is the abstraction that caused the first draft of this plan to specify a guard that would
never fire.

## 8. Tests

Every new test is mutation-tested with copy-restore (copy the production file to the scratchpad, mutate, run,
restore from the copy, never `git restore`, per the recorded lesson). The mutation each test must fail against is
named below rather than left to implementation, and the one row that cannot be mutation-proven says so instead of
pretending otherwise.

| Test | Stack | Item | Origin | Mutation it must fail against |
|---|---|---|---|---|
| `CatchupThenPushSubscriptionModelOwnershipVerificationTest` | reactor | A | adopted, fails today | drop the `completeIfStillOwned` wrapper from the source's `markCaughtUp()` |
| ownership falsifier: `keepReplaying` identity | reactor | A | new | revert `shouldKeepReplaying` to `catchupOwners.containsKey(id)` |
| ownership falsifier: launcher eviction | reactor | A | new | revert `interruptibleReplays.remove(id, ownLaunch.get())` to `remove(id)` |
| ownership falsifier: stale pause application | reactor | A | new | move `applyPendingPauseIfAny` back outside `completeIfStillOwned` |
| ownership falsifier: an empty live buffer still evicts the launcher and applies the pause | reactor | A | new | gate the success branch on `replayingSubscriptions` instead of `catchupOwners` (the exact defect the first draft of this plan had) |
| `CatchupThenPushSubscriptionModelPauseStopRaceVerificationTest` | reactor | D | adopted, fails today | drop `&& liveFeed.isRunning(id)` |
| `CatchupThenPushSubscriptionModelStopStartRaceVerificationTest` | blocking | F | adopted, fails today | remove the `!stopped` recheck from the stop path |
| stop-start race twin | reactor | F | new | same |
| subscribe-tail cancel falsifier | both | B | new | remove `synchronized` from `cancelSubscription` |
| pause-during-completion falsifier | both | C | new | remove `synchronized` from `pauseSubscription` |
| `RabbitMqCloudEventBridgeNestedRefusalRedeliverTest`, `KafkaCloudEventBridgeNestedRefusalRedeliverTest` | blocking | H1 | adopted from `verifier-895-tests.patch` | drop the `thrownBy` check from the push model's `Refusal` wrap |
| matcher throwing a `PreDispatchRefusalException` does not stop a bridge | blocking | G3 | new | report `REFUSED` from the matcher-threw branch instead of `NOT_DELIVERABLE` |
| domain-bridge nested-refusal twins | blocking | H2 | new | drop the `feed.hasFailedCatchUp()` gate |
| `EmitResult` classification, one per branch | reactor | E | new | collapse the `switch` back to one overflow refusal |
| concurrent-emit smoke check | reactor | E | new | **none: this row is reasoned, not mutation-proven.** See invariant E1 |
| live-phase failure logged once | reactor | E | new | remove the `log.error` from the error handler |
| drain throw releases reservations | blocking | J1 | new | remove the `finally` that releases keys `i+1..n` |
| `routeReportingMatch`: the value and whether an exception propagated, for every reporting branch | both | G1 | extends `RegisteringSubscribableRouteReportingMatchTest`. Sized during the approval round: each stack has 13 tests today and exactly one asserts `NOT_DELIVERABLE` (blocking `:211`, the refusal case, which becomes `REFUSED`). There is no existing test at all for the paused branch, the not-running branch, the nothing-registered branch or the matcher-threw outcome value, so four new cases per stack are added rather than edited | report `NOT_DELIVERABLE` from either lifecycle branch |
| bridge held-and-paced on `UNAVAILABLE`, permanent stop on `REFUSED` | blocking | G2 | extends the four bridges' outcome tests | swap the two branches |
| `@Timeout(30)` | reactor | I | class annotation | not applicable, this is a guard against a hang, verified by the class still passing |

Baseline recorded during planning, so a regression in phase 2 is attributable:
`./mvnw -o -pl subscription/api/blocking,subscription/api/reactor,subscription/push/blocking,subscription/push/reactor test`
is BUILD SUCCESS with 121, 48, 143 and 133 tests, no failures, no errors, no skips. `subscription/core` is green
too, 58 tests. The two dsl modules need `DOCKER_HOST` exported in the shell Maven runs in (the recorded "export
Colima env per shell" lesson; Colima itself is up, `colima status` reports "colima is running using macOS
Virtualization.Framework"). With it exported, `dsl/projection-dsl/reactor` is BUILD SUCCESS with 90 tests.
`dsl/projection-dsl/blocking` is BUILD SUCCESS with 113 tests once the same two env vars are set. An earlier run
of this plan attributed its single failure to the recorded local Mongo container failure mode; that was wrong, and
is corrected here rather than left. The cause was the same Ryuk socket-mount problem as the RabbitMQ module's, and
`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` clears it. Nothing on this baseline is red.
The blocking dsl module also needs `-am` to compile at all against a `~/.m2` another session has moved, which the
recorded lesson already says and which reproduced here.

Both bridge modules are green on the baseline once both env vars are set:
`broker/kafka/blocking` 150 tests, `broker/rabbitmq/blocking` 104 tests, both BUILD SUCCESS. Without
`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` the RabbitMQ module fails one test with
`InternalServerErrorException: Status 500: ... mkdir /Users/johan/.colima/default/docker.sock: operation not
supported` out of Ryuk, which is an environment problem and not a test defect. Phase 2 sets both env vars for
every bridge run. Both starters are green too, `broker/kafka/spring-boot-starter` 35 tests and
`broker/rabbitmq/spring-boot-starter` 31 tests, which matters because item G edits javadoc in both.

`example/broker/rabbitmq` is the one module with a genuinely red test on this baseline, and it is a known one.
It needs `-Pexamples-module` (the examples are not in the default reactor) and `-am` (without it,
`RabbitMqDomainEventLevelBrokerExampleTest` dies with `ClassNotFoundException:
org.occurrent.dsl.projection.ReplayPhase` out of a stale `~/.m2`, which is the recorded lesson reproducing
exactly). With both, 21 tests run and one fails:
`RabbitMqBrokerExampleBootstrapSmokeTest.the_leak_probe_itself_sees_a_forwarder_that_is_actually_still_running`
times out awaiting `expected: 1` at `:213`. That is #893 item 20 verbatim, "passes alone and in CI but fails when
the whole class runs against reused local containers, because the bootstrap smoke tests share fixed exchange and
queue names and the probe binds everything". It is pre-existing, filed, and outside this PR's items. Phase 2
reports it as such rather than fixing it or treating it as a regression.

Validation: `./mvnw -pl <modules> -am test` for each step, plus a full
`./mvnw -pl subscription/api/blocking,subscription/api/reactor,subscription/push/blocking,subscription/push/reactor,subscription/core,dsl/projection-dsl/blocking,dsl/projection-dsl/reactor,broker/rabbitmq/blocking,broker/kafka/blocking -am test`
before the PR. Always `-am`, per the recorded lesson about stale `~/.m2` artifacts. Docker is needed only for the
bridge tests that use Testcontainers; the shared-file tests do not need it.

## 9. Risks

1. **Monitor widening deadlocks.** The two monitors this touches are the model's `this` and the live feed's
   `registrationLock`, always taken in that order, never the reverse (B2). The merged deadlock guard test stands.
   The residual risk is a fold that calls back into the model from inside a replay while some thread holds the
   monitor and waits for that fold. No such wait exists: `shutdown()` is the only method that waits for a replay
   and it is deliberately not synchronized. This is reasoning plus a read of five lock sites, not a test, and B2
   says so.
2. **A new value reaches a branch that does not handle it.** `grep -rn "case DELIVERED\|case FILTERED\|case
   NOT_DELIVERABLE\|case DEFERRED"` over the repository, excluding `target`, returns nothing: there is no `switch`
   over `RoutingOutcome` anywhere, only if-else chains, so adding a constant cannot break compilation. What it can
   do is fall into an else. Both CloudEvent bridges have an explicit else that routes an unknown value as a
   failure (`RabbitMqCloudEventBridge.java:500-506`, `KafkaCloudEventBridge.java:544-548`), so a missed lifecycle
   path would become a park rather than a hold, and a missed `REFUSED` would become a park rather than a stop. S1
   changes every branch together and the extended `routeReportingMatch` tests assert the value for every reporting
   branch, so a missed path fails a test rather than shipping. Both domain bridges are unaffected:
   `DomainEventFeed.acceptCloudEvent` cannot return either new value any more than it can return
   `NOT_DELIVERABLE`.
2b. **`REFUSED` is the one behaviour change in an area PR A just settled.** Deleting the two CloudEvent bridges'
   type-based catch replaces code that shipped days ago and has tests. It is done in S1 with those tests extended
   rather than rewritten, and G2 plus G3 are the falsifiers. If it proves contentious in review, the fallback is
   to keep the type-based catch and read `outcome == REFUSED` inside it, which is strictly narrower than today and
   costs only the `internal` import.
3. **The reactor marker write blocks under the monitor.** Item A puts a `.block()` on the marker `Mono` inside a
   `synchronized` method, on a `boundedElastic` thread. That is a deliberate trade: it buys A1's marker clause as a
   closure rather than a narrowing, at the cost of extending item J's liveness property to the reactor stack. The
   residual risk is a store that hangs forever, which would hold that model's monitor forever. The blocking stack
   has had exactly this property since before this epic, so the change does not introduce a new failure mode, it
   makes the two stacks share one. Recorded on #893 with the per-subscription monitor as the shape that removes it
   from both, and with the reactor marker's missing write condition beside it.
4. **The `catchupOwners` map is a second source of truth about one id.** Two maps can drift. The mitigation is that
   every write to either happens under `synchronized (this)` in the same block, and that each map answers exactly
   one question: `replayingSubscriptions` answers "is a replay in flight", read by `isCatchingUp`, `isRunning`,
   `isPaused`, `pauseSubscription`, `relaunchInterruptedReplay` and `awaitReplays`; `catchupOwners` answers "which
   attempt owns this id", read only by the ownership guards. No reader consults both. The alternative, one map, is
   what the first draft of this plan tried, and item A shows why it cannot work on the reactor stack.
5. **Test flakiness from the adopted demonstrators.** The reactor ownership test uses a fixed `Thread.sleep(1500)`.
   It is kept as adopted for this PR, since it currently fails deterministically, and the four new falsifiers that
   cover its other assertions are latch-gated rather than sleep-gated.

## 10. Plan review

Passes run: one self-review against the johan-plan checklist, which caught three of its own citation errors (the
`RoutingOutcomeChannel` `accept(...)` MINOR already being closed, the wrong `else`-branch line numbers, and the
duplicated `-am` sentence), then one fresh-context reviewer: a general-purpose opus subagent, read-only, given
only the plan and the repository, told to quote every cited line verbatim and to spawn nothing. It was waited for
in the foreground.

**Reviewer verdict: `VERDICT: 7 findings above the line`, plus six below.** It spot-checked all 21 assumption rows
and confirmed every one, including the two ADR quotations, and it independently confirmed section 0's reading of
the baseline as "the most valuable observation in the plan". It could not verify section 1's demonstrator results,
because the patch files live outside the worktree and it was read-only; it confirmed instead that the guards those
three tests pin are genuinely absent.

All seven above-the-line findings are folded in. What changed:

1. **The reactor success branch cannot be gated on `replayingSubscriptions`.** The reviewer showed that U11 moved
   `forget` into `liveDrained()` and that the engine calls `liveDrained()` *before* completing the catch-up signal
   when nothing was buffered (`ReactiveHandover.java:410-413`), so a verbatim port of the blocking guard would
   never fire on the ordinary path. This was a real defect in the plan, not a wording problem. Item A is rewritten
   around a second map, `catchupOwners`, with the departure from the blocking design stated first and a dedicated
   falsifier for the empty-buffer case.
2. **`markerClaimed` did not close the window it existed for.** Already replaced by the blocking-verbatim
   `.block()` under the monitor during the self-review pass; the reviewer's independent derivation of the same
   interleaving confirms it. The reactor marker's missing write condition, which the reviewer also found, is now
   named in item A and filed on #893.
3. **B2 was false as stated, and item F contradicted item B.** Both restated. B2 is now the property that is
   actually true ("no thread holds the monitor while *waiting* for a fold") and is marked reasoned, not tested,
   with the note that the merged deadlock guard cannot catch a non-deadlocking violation. Item B records that
   `relaunchInterruptedReplay` already calls `launch.get()` under the monitor on the baseline, so item F inherits
   the property rather than introducing it.
4. **The consumer list was not exhaustive and omitted the enum's own javadoc.** Re-enumerated with
   `grep -rln RoutingOutcome .` over the whole repository (63 files). `RoutingOutcome.java` is now named as the
   primary edit, and `RabbitMqDeliveryFailureAction:190-191`, the two domain bridges' unreachability comments, the
   starters, the example bootstrap and ADR 133 decision 1 are added. The domain-bridge javadoc line numbers are
   corrected from `:64` to `:69` and `:65`, and the ADR "three-valued outcome" phrase is re-attributed from the
   lifecycle amendment to `:810`, where it actually is.
5. **"Exact after item H" was overclaimed.** The reviewer showed a matcher throwing a `PreDispatchRefusalException`
   satisfies both halves of the bridge's test and stops it permanently for a filter defect, which H1 cannot reach.
   This is what restored the `REFUSED` constant the earlier draft had rejected. Item G now derives both constants,
   records the reversal, enumerates the exception's three throw sites and which are reachable through
   `acceptIfLive`, and adds invariant G3 with its own test.
6. **The reactor `hasFailedCatchUp()` had no caller and would have misnamed `terminalError`.** Dropped, restoring
   consistency with item J's "no consumer" rule. The asymmetry between the CloudEvent and domain halves of item H
   now carries its reason instead of being left to be noticed.
7. **The invariant table was partly restated prose.** B2 and E1 are marked reasoned, not tested. A2 is marked as
   the clause of A1 the adopted demonstrator happens to reach. The test table gains a column naming the exact
   mutation each test must fail against, and the one row that cannot be mutation-proven says so.

Below-the-line findings, also fixed: the `RegisteringSubscribable.java:154` citation pointed at the field
declaration rather than the acquisition sites; the `RabbitMqCloudEventBridge` catch is at `:458`, not `:461`; G1
said "six branches" where there are seven report sites; and `DomainEventFeed.hasFailedCatchUp()`'s answer for an
unregistered feed was unspecified and is now `false`, following `isReadyForLiveDelivery()`.

No second reviewer pass was run over the revision. That is a deliberate stop: the revision is large and a fresh
read would very likely find something, but the orchestrator's gate is the right place to decide whether to spend
another round rather than this worker deciding it alone.

**Severity line proposed to the orchestrator**: above the line is anything that could cause an incorrect
implementation, a missed in-scope defect, an unsafe sequence, or a wrong public API decision. Below the line is
wording, ordering preference and optional coverage.

### Known, not acting

* Moving the marker write off the model monitor, on either stack. Reason in item J.
* The reactor marker write having no `CheckpointWriteCondition`, unlike the blocking one. Reason in item A: closing
  it needs a constructor parameter the reactor model does not have. Filed on #893.
* Moving `catchupDone` off the marker phase so a reactor live-phase failure can reach a caller. Reason in item E.
* The four unadopted files in `verifier-895-tests.patch`. Reason in section 2.
* Round-2 findings 1, 3, 4, 11 to 21 and the MINORs outside these files. Reason: out of this PR's scope, and
  findings 1 and 3 were closed by PR A (verified: `RabbitMqCloudEventBridge.java:435-459` and `:487-499`).
* The reviewer's observation that assumption 11's fact ("a throwing matcher reports `NOT_DELIVERABLE` and then
  rethrows") is used in item G both to argue the item is not a live defect and, via finding 5, to show the bridge
  guard is inexact. Both uses are correct and they are not in tension: the first is about the *failure policy*
  applying, the second about the *permanent stop* firing. Item G now separates them explicitly, so nothing is
  outstanding here, but it is recorded because a later reader may notice the same shape.

## 11. Implementation tier

Recommended for phase 2: **Opus, effort high**, for the whole PR rather than tiered per step. The work is
concurrency correctness across two parallel stacks whose ownership records are deliberately not the same shape,
every test names the mutation it must survive, and S3 and S4 both change a lock's extent. S1 and S6 are
mechanical enough for Sonnet in isolation, but S1 now carries the `REFUSED` bridge change and the enum's own
javadoc, which is the PR's public API surface, and splitting the run across tiers would put the two halves of item
G in different heads. The failure this guards against is the one the fresh-context review just caught: a guard
specified in good faith that never fires because the engine's contract released the state first.

## 12. The documentation site, found during the approval round

AGENTS.md line 80 onwards makes the separate Jekyll repository part of this change, not follow-up work: "A change
that affects what a user can do needs both: the changelog entry in this repository, and in the docs repository the
reference documentation in `pages/docs/docs.md`". Adding two `RoutingOutcome` values is such a change.

Checked, in `/Users/johan/devtools/java/projects/occurrent-org.github.io`:

* `git grep -c "RoutingOutcome" origin/docs/421-broker-modules -- pages/docs/docs.md` returns 5. That is the held
  branch for this epic's feature, and it is the one to patch.
* `docs/802-push-observation-hook` has no hits, so it needs nothing from this item.
* Two of the five hits are already stale independently of this PR: `:3267` says `outcome` "is one of three values"
  and that the observer "runs once per event, before delivery is attempted", both of which PR 889 and PR 894
  already falsified. `:3508` documents the acknowledgement rule in terms of `accept(...)` and three outcomes.

Phase 2 therefore adds, on `docs/421-broker-modules` and never on `main`: the six values, the corrected "told once
the matched registration's action has run" wording, and the acknowledgement rule as it actually is (acknowledge on
`DELIVERED` and `FILTERED`; hold and pace on `DEFERRED` and `UNAVAILABLE`; failure policy on `NOT_DELIVERABLE`;
stop on `REFUSED`). Written as settled fact, with no "not yet released" framing, per the same section. The two
pre-existing staleness items are corrected in the same pass rather than left, since AGENTS.md says a correction is
complete only when every surface carrying the claim is corrected together.

This is a second repository and a second pull request. It does not enter this PR's diff, which is exactly why
AGENTS.md warns it gets forgotten, so it is listed in the DELIVERY_RESULT as its own line rather than folded into
the item rulings.
