# 91. A push catch-up replays off the startup path

Date: 2026-08-02

## Status

Accepted. Written for the blocking stack; the reactor model and the domain-event feed followed shortly after, see the
2026-08-02 amendment at the end. `@Saga(source = PUSH)` followed separately in #349, see
[ADR 96](0096-a-push-fed-saga-may-have-no-history-to-replay.md).

## Context

`@Projection(source = PUSH)` replays the whole event store before it hands over to the live feed. That replay ran
inside `CatchupThenPushSubscriptionModel.subscribe`, on the Spring refresh thread, and the handle it returned hardcoded
`waitUntilStarted` to `return true`. An application with a large history was therefore held out of service for the
length of the replay, with no way to opt out.

`StartupMode.BACKGROUND` exists for exactly this, and every event-store-backed subscription honours it. Push was the
one source that could not, which is backwards: a push catch-up always starts at the beginning, so its replay is the
largest one Occurrent runs.

ADR 62 rejected `startupMode` for a push source alongside `startAt`, `startAtGlobalPosition` and `resumeBehavior`, on
the grounds that "the catch-up always replays from the beginning and live-resume is the broker's responsibility". That
reasoning holds for the other three. It does not hold for `startupMode`, which is not about where the replay starts but
about whether the application waits for it.

ADR 86 solved an adjacent problem a different way. Under `occurrent.subscription.mode = manual`,
`ManualStartPushSources` (named `ManualStartProjections` when this was written, see
[ADR 96](0096-a-push-fed-saga-may-have-no-history-to-replay.md)) withholds a push projection's startup work until the
application asks for it. That is an
escape hatch rather than this: `start(id)` runs the withheld work with `startup.run()` on the caller's thread, so the
replay still blocks whoever calls it. Deferring is not backgrounding.

## Decision

**The replay runs on its own virtual thread and `waitUntilStarted` is the only thing that joins it.** This copies
`AbstractCatchupSubscriptionModel.startCatchupAsync` and `CatchupSubscription`, which have solved the same problem for
the event-store catch-up since before this model existed: a `FutureTask` on `Thread.ofVirtual()`, the id registered on
the caller's thread before the fork so `isRunning(id)` answers immediately, and a timeout split through
`DurationToTimeoutConverter` so the `ChronoUnit.FOREVER` that every no-argument `waitUntilStarted()` passes does not
overflow.

**Argument validation and the live-feed registration stay on the calling thread.** Only the replay moves. Registering
on the live feed first is what captures an event committing mid-replay, and moving it would open a window where the
application is live and the handler is not registered.

**The handle throws a replay failure rather than reporting `false`.** This is the one place the template is not
copied. `CatchupSubscription.waitUntilStarted` logs and returns `false`, and `ProjectionRunner` discards that return
value, so a failure would start an application whose read model is silently empty. `ProjectionPushCatchupTunablesTest`
depends on the failure reaching `context.getStartupFailure()`, and would otherwise pass for the wrong reason.

**A stop is signalled by throwing out of the replay, not by ending the stream.** `BlockingHandover.catchUp` drains its
buffer, goes live and writes the one-shot catch-up marker as soon as the replay completes normally. A `takeWhile` that
short-circuits completes normally, so a stopped replay would record a partial replay as a finished one, and the marker
is what makes the next start skip the replay entirely. Throwing puts it on the handover's failure path instead:
nothing marked, nothing live, full replay next time.

**The model gets a life cycle, and it had to come with the threading rather than after.** A replay on its own thread
that nothing can stop outlives the context and folds into a store that is closing. `CatchupThenPushSubscriptionModel`
now implements `SubscriptionModel`, fans the life cycle out to the live feed, and `shutdown()` stops replays in flight
and waits for them to unwind. `ProjectionAnnotationRegistrar` retains its models and closes them from the bean post
processor's `destroy`, which it previously had no hook for at all.

ADR 85 left both catch-up wrappers without a life cycle by omission rather than by argument: its scope was
`RegisteringSubscribable` and its four subclasses. This closes it for one of the two.

**Stop is the right verb here even though ADR 86 says it is the wrong one.** That ADR is about *withholding*, where
stopping does not work because a stopped model has already been handed every subscription and a history-reading layer
can still deliver. Shutdown is a different problem: there is nothing to withhold, only something already running to
halt. The flag is set by `stop()` and never before registration, so the `CancelledSubscription` trap ADR 86 describes
is not reachable from here.

**`DEFAULT` keeps waiting.** The registrar keys off `startupMode != BACKGROUND` rather than reusing
`SubscriptionAnnotations.shouldWaitUntilStarted`, which maps `DEFAULT` to "background if it replays history". A push
catch-up always replays history, so reusing it would have moved every existing push projection off the startup path
without anyone asking for it.

## Consequences

**A replay failure now surfaces from `waitUntilStarted` rather than from `subscribe`.** This is a released contract:
the model shipped in 0.31.0. A caller that wrapped `subscribe` in a `try`/`catch` sees nothing and starts with an empty
read model. Nothing in the annotation path changes, because both runners wait. Adoption is realistically near zero,
since the entire push-source feature shipped in that same release five days before this, which is why the contract was
moved rather than a second opt-in path carried alongside it forever.

**A stopped replay is not resumable.** It leaves the live registration released and the marker unwritten, so recovery
is a fresh subscribe and the next one replays the whole history. The event-store catch-up behaves the same way, and for
the same reason: resuming mid-replay would mean persisting the exact replay cursor, which neither model does. The
`SubscriptionModelLifeCycle.stop()` contract says a running subscription is left paused, and an interrupted replay does
not honour that. Fixing it means not routing the stop through the handover's failure path, since that records a stored
failure the handover would then rethrow for every later event, which is more than this change is worth.

**A pause does not interrupt a replay.** It is recorded and applied at the handover instead, so `isPaused(id)` is
honest while the history keeps folding. Same trade as the event-store model.

**`isRunning(id)` is true while a replay is in flight**, matching the event-store catch-up. This is load-bearing beyond
introspection once `@Saga(source = PUSH)` exists: `SagaAnnotationRegistrar.timersEnabledFor` gates a saga's timer
poller on `isRunning(id)`, and a saga must not fire timers against half-rebuilt state. Keeping `isRunning` honest means
that gate needs a separate handover-complete signal rather than an overloaded one, which belongs with #349.

**In background mode the bounded live buffer fills at full broker rate for the whole replay**, rather than only while a
caller blocks. It is bounded and fails loud, and for a push feed a loud failure reaches the listener, which nacks, so
overflow degrades to redelivery rather than loss. The dial is
`occurrent.subscription.catchup-then-live.max-buffered-events`.

**Two defects in the template were not copied.** The event-store model never removes an interrupted replay's id from
`runningCatchupSubscriptions`, so `isRunning(id)` returns true forever afterwards for a subscription that no longer
exists, and nothing covers it. This model removes it. Whether to fix the original is left open.

## Amendment (2026-08-02): the reactor model and the domain feed catch up in the background too

The decision above holds unchanged. What follows is the other two thirds of it, plus one thing the original got wrong.

**The reactor model's replay was never actually off the calling thread.** The status line said "blocking stack only"
because the reactor half was unwritten, but the working assumption behind that was that reactor was already
asynchronous by construction. It was not. `ReactiveHandover.catchUp` subscribes its own pipeline inline, so with a
synchronous reader the whole replay ran before `subscribe` returned, on the Spring refresh thread. The fix is
`subscribeOn(Schedulers.boundedElastic())` on that internal subscribe, which moves the replay for both reactor callers
at once. `boundedElastic` is the only scheduler in main code and the one ADRs 59 and 62 name, and the replay folds
through blocking bridges anyway. There is no precedent here for `subscribeOn` moving a pipeline's *start* rather than a
stage of it, only because there is no other pipeline in this codebase that starts itself.

**The reactor model implements `Subscribable, SubscriptionModelLifeCycle`, not `SubscriptionModel`.** The reactor
`SubscriptionModel` is the bare `Flux`-returning change-stream primitive, which a register-and-wrap model cannot
honour. The reactor `SubscriptionModelLifeCycle` returns `void` like the blocking one, so the fan-out mirrors the
blocking model closely. `RegisteringSubscribable`'s life-cycle methods are `final`, so the model delegates to the live
feed rather than subclassing it. The `DcbSubscriptionModelAdapter` `instanceof` gate that `.context/ORCHESTRATOR.md`
flagged as a risk here is a non-issue: its delegate is typed as the reactor `SubscriptionModel`, which this model does
not implement.

**`ReactiveProjectionRunner` gains no `waitUntilStarted` parameter**, unlike the blocking `ProjectionRunner`. It never
blocks, and the house rule is already written down in `dsl/subscription-dsl/reactor/.../Subscriptions.kt`: reactor
returns the handle and the caller composes. The Spring registrar gates its own existing `.block()` instead.

**The domain feed's background catch-up is the registrar's, not the feed's.** `DomainEventFeed` gained `stopCatchUp()`
and no background overload. Stopping is the thing a caller cannot do for itself; backgrounding is not, since a caller
can already run `catchUpAll()` on a thread it owns. The Spring registrar is that caller, because it is what knows the
`startupMode`: it runs the catch-up on a virtual thread it owns and stops the feed from `close()`. On reactor it needs
no thread at all, since subscribing without blocking is enough once the handover schedules itself. ADR 90, which made a
feed carry exactly one projection, is what makes this well defined: `startupMode` on a domain-feed projection is now
unambiguously per-projection.

**A background failure needs somewhere to go, and an `ERROR` log is not enough on its own.** Under `BACKGROUND` nobody
waits, so the failure surfaces from nothing: the context refreshed long ago and the projection is left with an empty
read model on a healthy-looking application. Both starters now contribute a `BackgroundCatchupFailures` bean, written
by the annotation processor and injected by the application, the same shape as `ManualStartPushSources` and
`SagaInstancesRegistry`. Deliberately not a Spring `ApplicationEvent`: Occurrent publishes none anywhere, and the only
hits in the repository are inside `example/`. The log stays as the backstop. On the `PushSubscriptionModel` path the
registrar joins the subscription on a thread of its own purely to record the failure, because registration has to stay
on the refresh thread and only the replay may move.

**`DEFAULT` keeps waiting on every one of these paths**, for the reason the original decision gives.
