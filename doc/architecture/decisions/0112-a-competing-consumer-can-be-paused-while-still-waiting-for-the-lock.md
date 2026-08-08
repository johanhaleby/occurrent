# 112. A competing consumer can be paused while still waiting for the lock

Date: 2026-08-08

## Status

Accepted. Fixes #565. Builds on the has-started answer [ADR 105](0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md)
already gave for a competing consumer that has not won the lock, and on the refusal
types [ADR 106](0106-a-refused-subscription-call-says-which-condition-it-hit.md) settled.

## Context

`CompetingConsumerSubscriptionModel.pauseSubscription(id)` had one branch it never finished. A consumer
registered with the strategy but not yet granted the lock, the `Waiting` state, hit a branch that logged
"will ignore" and returned as if the pause had happened. Three things followed from that. The strategy
still granted the lock to it later and the model started it anyway. `isPaused(id)` kept delegating to the
wrapped model, which had never heard of a consumer that has not subscribed yet, so it answered `false` for
a subscription the caller had just paused. And an operator had no way to say "do not take over" on a node
that had not won the lock, which on a healthy cluster is every node but one.

ADR 105 already answers the question that looks like it should decide this. A competing consumer that has
not won the lock has started, because it is registered and the strategy will call it back, and that answer
has to stay `true` or every non-leader node using `WAIT_UNTIL_STARTED` would block. That settles
`waitUntilStarted`. It does not settle `isRunning(id)` or `isPaused(id)`, which ADR 105 itself separates
from "has started" as the present-moment questions, and it says nothing about what pausing a consumer that
has started but is not delivering should do.

`isRunning(id)` carries a hard constraint from outside this module. `SagaAnnotationRegistrar` gates a saga's
timer poller on `lifeCycle.isRunning(subscriptionId)`, and `CompetingConsumerSubscriptionModel` is the
`@Primary` bean the Spring Boot starter wires by default, so this is the gate for every framework-driven
saga on a competing-consumer subscription. A separate poller-lease gate exists, but it is off when
`occurrent.saga.competing-consumer.enabled=false` or no strategy bean is present, so `isRunning(id)` is the
only thing standing between a non-leader node and firing commands from a saga that is not actually
consuming. Whatever this fix does, `isRunning(id)` must keep answering `false` for a consumer that is not
delivering.

## Decision

**`isRunning(id)` does not change.** It already delegates to the wrapped model and already answers `false`
for a waiting consumer, which is exactly what the saga gate needs. Nothing here makes it answer `true` for
a consumer that is not delivering.

**`isPaused(id)` also asks the model's own map, not only the delegate.** A consumer paused before it ever
won the lock is only known here, the same reason `subscriptionIds()` already merges its own map with the
delegate's. For every consumer that did start, the two sources agree, since a `Paused` entry is only ever
written next to a `delegate.pauseSubscription` call, so no already-running behaviour moves.

**A fourth sealed state, `PausedWhileWaiting`, replaces the ignored branch.** It keeps the `Waiting` it was
paused from, because that is the only place the start supplier lives. A consumer in this state never
subscribed to the delegate, so there is nothing for `delegate.resumeSubscription` to resume, and resuming it
means restoring the kept `Waiting` and registering again.

**Pausing a waiting consumer unregisters it from the strategy, it does not leave it registered.** This
contradicts what #565 assumed, and the correction is not a judgement call, it follows from what the shipped
Mongo strategy actually does. Its refresh loop re-registers every consumer that lacks the lock on every
round. A consumer left registered while paused would win the lease back within one round, and the model
would then skip starting it, so it would sit on the lock consuming nothing while the wrapper reports it
paused. Every other node would stay locked out until this one is explicitly resumed, which turns a
per-instance pause into a cluster-wide stall, the opposite of what ADR 102 scopes a competing consumer
pause to mean. Unregistering also matches what the strategy's own contract already says unregistering is
for, handing the lock on until an explicit resume asks for it back, which is exactly what the existing
user-paused-while-running branch already does for the same reason.

**A consumer that is granted the lock while paused hands it back, it does not just skip starting.**
Unregistering on pause is necessary but not sufficient on its own. The shipped strategy's
`registerCompetingConsumer` reads the old lock status, makes a Mongo round trip, and then writes the new
status unconditionally, with no synchronization against its own refresh thread (filed separately as #651,
since it is a strategy-level defect this fix works around rather than closes). A pause can land in the
middle of that sequence. The refresh thread has already read the old status and is waiting on Mongo when
the pause unregisters the consumer, and the refresh thread's write then re-inserts it as holding the lock
and calls the grant callback anyway. If that callback only logged and returned, the model would believe the
consumer was paused and unregistered while the strategy kept refreshing its lease forever, which is the
same cluster-wide stall the unregister-on-pause decision exists to prevent, reached by a different road. So
every branch of `onConsumeGranted` that would otherwise skip a consumer, a `Waiting` one while the model is
stopped, a `Paused` one paused by the user, and the new `PausedWhileWaiting` case, now unregisters instead.
That bounds the exposure to one extra lease round rather than an indefinite one.

**The new state carries no `pausedByUser` flag.** Only a user pause can ever reach a waiting consumer. The
one caller that pauses with `pausedByUser=false` is `onConsumeProhibited`, and it only reaches a consumer it
already found running, which a waiting one never is.

**`SubscriptionModelLifeCycle.pauseSubscription`'s javadoc is amended.** It said the refusal covers a
subscription that "is not running, because it is already paused, was never started, or the whole model is
stopped." A waiting consumer is none of those three, it has started per ADR 105, so accepting its pause
would read as a contract violation unless the javadoc says so. `ManualStartSubscriptionModel` genuinely
refuses to pause a registration that has not started yet, with its own message saying so, so that refusal
stays. What changes is that not currently delivering is no longer, by itself, a reason to refuse. A
subscription that has started can be paused even while nothing is coming through it right now. This also
settles a case a narrower fix would have left inconsistent. A consumer that `stop()` left waiting can still
be paused after the model is stopped, while a consumer that `stop()` already paused refuses a second pause
as expected, and both of those follow the same "already paused or never started" rule rather than a third
one for the model being stopped.

**No OpenRewrite recipe.** `pauseSubscription(id)`'s call site is unchanged before and after this fix, only
the runtime behaviour moves from a silent no-op to an actual pause. ADR 105 reached the same conclusion for
its four behaviour changes, that there is no source edit for a recipe to make when nothing at the call site
changes shape.

**The TCK does not grow a contested-consumer surface.** The fixture SPI in `tck/subscription-blocking` has
no member for standing up a rival model instance, and only one model family, this one, would use it. The
module's own hand-written suite is where this is tested, not the shared conformance suite.

**Left open.** `resumeSubscription` on a plain `Waiting` consumer still succeeds even though that consumer
is neither running nor paused, the mirror image of the question this ADR settles for pause. No code changes
for it here, it is recorded so the next person who finds one asks about the other.

## Consequences

The state table for a competing consumer is now:

| state | isRunning(id) | isPaused(id) | waitUntilStarted |
|---|---|---|---|
| Running | true | false | true |
| Waiting | false | false | true |
| Paused | false | true | true |
| PausedWhileWaiting | false | true | true |

**This is a behaviour change on a shipped method, and it goes in the changelog under `#### Changes`, not
`#### Breaking changes`.** Nothing stops compiling and no call site needs to change shape. The behaviour
being replaced was an undocumented silent no-op that no caller could have deliberately relied on, so this is
a defect fix rather than a design being revised, which is what the breaking-changes convention is for.

The module's two conformance tests build a single, uncontested model instance, so no existing conformance
assertion ever exercises a `Waiting` consumer, and the `isPaused(id)` widening cannot change any answer they
already check.

The strategy-level race this fix works around, not closes, is tracked separately as #651.
