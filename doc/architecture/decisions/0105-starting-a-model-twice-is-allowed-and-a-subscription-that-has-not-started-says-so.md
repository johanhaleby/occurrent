# 105. Starting a model twice is allowed, and a subscription that has not started says so

Date: 2026-08-06

## Status

Accepted. Wave A of the post-0.31.0 API review remediation arc (#579), which gates the 0.32.0 tag.

Numbered 105 rather than 103 or 104 because `main` carries a duplicated `0102` pair, PR #595 renumbers
the dispatcher one to 0103, and PR #596 takes 0104.

## Context

Two lifecycle questions had shipped answers that disagreed with each other, and neither `Subscription`
nor `SubscriptionModelLifeCycle` said which answer was right.

### Calling `start()` on a model that is already started

`CompetingConsumerSubscriptionModel.start(boolean)` throws `IllegalStateException` when
`isRunning()`. A census of all 18 implementations across the blocking and reactor stacks found no
other model that refuses. Every one of the other 17 either passes the call further down or lets it
run again over state that is already correct.

Three things made that one refusal expensive rather than merely inconsistent.

It is the model the blocking starter wires `@Primary`, so it is what an application gets by default.

`ManualStartSubscriptionModel` hides it behind `if (!delegate.isRunning())`, so
`occurrent.subscription.mode=auto` and `mode=manual` answer differently for the same caller. Manual
mode exists to bring subscriptions up behind a leader election or a health check, and those are
exactly the callers that cannot know whether they already started.

The same class already treats the other half of its own lifecycle as safe to repeat. `stop()` on a
stopped model returns early instead of throwing. So one class refuses a repeated `start()` while
accepting a repeated `stop()`, and nothing explains the difference.

Two more pieces of the corpus had already decided this without saying so.
`SpringMongoSubscriptionModel` implements Spring's `SmartLifecycle`, whose `start()` must not throw
when the component is already running, so an Occurrent model has already promised Spring the answer
the wrapper above it refuses. And `ReactorCatchupSubscriptionModel` fans `start(..)` out to every
inner model with a comment saying it relies on repeating a model-wide lifecycle call being safe.

`SubscriptionModelConformance` recorded the disagreement rather than settling it, in a list of what
the suite deliberately does not assert.

### What `waitUntilStarted` promises for a registration that has not started

The interface promises only that the call returns once the subscription has started. Seven answers
shipped.

1. A registration withheld under manual mode returns `true` at once
   (`ManualStartSubscriptionModel.DeferredSubscription`).
2. A competing consumer that has not won the lock returns `true` at once, because it has no delegate
   subscription to ask (`CompetingConsumerSubscription`).
3. A push replay that `stop()` interrupted returns `false` (`CatchupThenPushSubscriptionModel`,
   decided in ADR 104).
4. A push catch-up that failed throws (same class, same ADR).
5. A stream or DCB catch-up that failed returns `false`, because `CatchupSubscription` catches
   `Exception` broadly and logs a warning. That same method also discards its delegate's answer and
   returns `true` whenever the delegate did not throw, so it reports started for a subscription the
   delegate said was not.
6. A catch-up cancelled before it handed over returns `true` (`CancelledSubscription`, and the
   reactor twin completes its `Mono` for the same case).
7. A subscription registered while a push or synchronous model is stopped returns `true` at once
   (`RegisteringSubscribable.AlreadyStartedSubscription`), on both stacks, even though ADR 85 says a
   stopped model of that kind drops every event it is handed rather than holding it.

The reactor stack had meanwhile answered the first question for itself, twice, deliberately.
`ReactorMongoSubscriptionModel` and `ReactorDurableSubscriptionModel` both park `waitUntilStarted` on
`Mono.never()` for a subscription registered while the model is not running, each with the same
sentence next to the code saying the wait must not complete for a subscription that will deliver
nothing until something starts it.

The framework had answered it too, in a comment that turned out to describe behaviour the code did
not have. `SubscriptionAnnotations.subscriptionsStartOnTheirOwn` skips the wait entirely under
`MANUAL`, on the stated grounds that waiting there would block until it timed out. It would not have.
It would have returned `true` immediately. The guard is right and its reason was wrong, which is a
good sign of which answer the API was supposed to have.

## Decision

**Starting a subscription model is a goal, not a transition.** A second `start()` is accepted and
leaves the model running with everything it can bring up brought up. Only `shutdown()` is one way.

**A `Subscription` handle reports on the one start it was created for, and it reports started once
nothing further is required of the application for that subscription to deliver.**

The second sentence is not a claim about the present moment. A subscription that has started can
afterwards be paused, be stopped, or lose a lock race, and its handle keeps answering `true`.
`isRunning(id)` and `isPaused(id)` answer the present question. A start that cannot complete until
the application acts has not completed, so the wait runs to its timeout and answers `false`. A start
that failed and will not be retried throws.

On the blocking stack that is `true`, `false`, or a thrown exception. On the reactor stack the `Mono`
completes when the start completed and errors when it failed, so "not started" is a `Mono` that has
not completed, which the interface's own timeout overload already turns into `false`.

### What "nothing further is required of the application" settles

It is what separates the two cases that look alike, and it is why one released behaviour changes and
the other does not.

A competing consumer that lost the lock needs nothing from the application. It is registered with the
strategy and the strategy calls it back when the lock frees, so the race it is in is with other
nodes, not with its own process. It has started. That answer stays `true`, and it has to, because the
competing consumer model is the `@Primary` bean and calling a lock-losing consumer "not started"
would block every non-leader node that uses `@Subscription(startupMode = WAIT_UNTIL_STARTED)`.

A registration withheld under manual mode needs an explicit `start()` or `resumeSubscription(id)`
that only the application can make. Nothing has been handed to the wrapped model, no lock is taken,
no history is replayed and no feed is opened. It has not started, so it answers `false`, and it
answers at once rather than waiting out the caller's timeout, because the model already knows that
nothing will change until the application asks. A caller that wants to wait for a leader election
waits on the handle `resumeSubscription(id)` gives back, which is the started one.

Answering at once is also what keeps this from stopping released code dead.
`DcbProjectionRunner.project(..)` waits with no timeout and no way to opt out, and its three sibling
runners wait by default, so a manual-mode application would have blocked at its first projection
rather than being told the truth about it.

The other five answers follow without argument. A stopped replay, a cancelled catch-up, and a
subscription registered on a stopped push or synchronous model all need the application to act, so
all three answer `false`. That is what ADR 104 already decided for the stopped replay. A failed
catch-up throws on both catch-up families rather than on one of them.

### The two stacks answer a restart differently, and that stays

On the blocking stack a handle whose replay `stop()` interrupted keeps answering `false` after a
later `start(true)` launches a fresh replay, because the handle cannot see the new one. ADR 104
decided that, on the grounds that threading a live handle through a restart would mean a mutable
subscription handle.

On the reactor stack the same handle completes when the restart happens.
`NamedCatchupSupport.start(..)` relaunches a parked replay through the very sink the handle already
holds, so no mutable handle is needed to get the better answer, and none was written.

Both are correct under the rule above, since neither reports started while the application still has
to act. The difference is worth stating rather than removing, because degrading the reactor answer to
match would cost a real signal and buy only symmetry.

### Why the model-wide verbs and the per-subscription verbs differ

`pauseSubscription(id)` and `resumeSubscription(id)` keep refusing a call that does not match the
subscription's state, and that asymmetry with `start()` and `stop()` is deliberate. A verb that takes
no argument and addresses the whole model is a goal the caller wants reached, and the caller is
usually a leader election, a health check or Spring, none of which can observe the current state. A
verb that names one subscription is a transition of that subscription, and a wrong id or a wrong
state there is a mistake in application code worth reporting. Unit A3 (#580) owns which exception
type those refusals use.

> **Amended on 2026-08-07 by [ADR 106](0106-a-refused-subscription-call-says-which-condition-it-hit.md).** They use a
> sealed family under `IllegalArgumentException`, so `pauseSubscription` throws `SubscriptionNotRunningException` and
> `resumeSubscription` throws `SubscriptionAlreadyRunningException`, with `UnknownSubscriptionException` for an id the
> model does not have at all. The asymmetry described above is unchanged. What ADR 106 adds is that the refusal now
> says which of the three it is, and that a competing consumer whose lock another node holds stays an
> `IllegalStateException`, because that is not a mistake in the calling code.

> **Extended on 2026-08-08 by [ADR 112](0112-a-competing-consumer-can-be-paused-while-still-waiting-for-the-lock.md).**
> The has-started answer for a competing consumer that has not won the lock, decided above, stays exactly as
> written. ADR 112 builds the paused-while-waiting state on top of it, and answers the two questions this ADR left
> open for that state, what `isRunning(id)` and `isPaused(id)` say about it, and what pausing it does.

## Consequences

**Four released behaviours change, and each gets its own changelog entry.** All four shipped in
0.31.0, verified against the tag rather than the changelog heading. `CompetingConsumerSubscriptionModel`
accepts a second `start()`. `CatchupSubscription` lets a failed replay reach the caller instead of
logging it and answering `false`, and it returns its delegate's answer instead of `true`.
`CancelledSubscription` answers `false`. A subscription registered while a `PushSubscriptionModel` or
`SynchronousSubscriptionModel` is stopped answers `false` until that model is started. There is no
OpenRewrite recipe for any of them, because these are behaviour changes at unchanged call sites and
there is no source edit for a recipe to make. ADR 90 and ADR 104 set that precedent in this same
area. The migration guide carries them instead.

**Nothing starts blocking that did not block before.** A withheld registration answers `false`
straight away rather than waiting, so no caller waits longer than it used to. That was the deciding
constraint rather than a nicety. `DcbProjectionRunner.project(..)` waits on the returned subscription
with no timeout and no parameter to turn it off, and `ProjectionRunner`, `SagaRunner` and
`DcbSubscriptions` all default their wait to on, so any answer that blocked here would have hung a
manual-mode application at its first projection.

**The no-argument `waitUntilStarted()` throws away the answer, so those four runners cannot see the
`false`.** They behave exactly as before, which is to carry on without a started subscription. That
is not made worse here, and giving the no-argument overload a default timeout would change a released
method for every caller to address something only manual mode has, so it keeps waiting forever.
Unit A8 (#585) owns the shape of that overload if it is worth revisiting.

**`SubscriptionModelConformance` asserts both halves.** The bullet saying the suite does not assert
that `start()` can be called twice is gone, replaced by tests that a second `start()` on a running
model neither throws nor disturbs delivery, and that it resumes a subscription that was paused on its
own.

**A second `start()` reaches the same state, but it does not always cost the same.** Repeating the
call is safe, which is the point, and on two models it is not free. `CatchupThenPushSubscriptionModel`
replays the whole history again for a replay that `stop()` interrupted, per ADR 104, and
`CompetingConsumerSubscriptionModel` issues one lease write per consumer that is still waiting for
its lock. A health check that calls `start()` on every tick therefore pays per tick. That is worth
knowing before wiring one, and it is not a reason to make the call refuse, since a caller that has to
ask `isRunning()` first is the thing that was broken.

**`isRunning()` is still not a substitute for asking each subscription.** Accepting a second
`start()` does not change what ADR 85 and the `stop()` javadoc already say about a model that owns a
single running flag. A `true` from `isRunning()` after a partial resume still does not mean every
subscription is delivering.

**The competing consumer model needs one guard the refusal was hiding.** With the throw gone,
`start(true)` on a model that is already running reaches
`nonCompetingConsumersSubscriptions.forEach(delegate::resumeSubscription)`, and resuming a
subscription that is already running is itself refused by the wrapped model. It resumes only what the
delegate reports paused. The competing consumer loop below it already skips consumers that are
running, and registering a consumer again is a lease refresh plus a map write that notifies listeners
only when the lock status actually changed, so re-entering it is safe.
