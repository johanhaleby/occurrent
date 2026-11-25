# 177. A lifecycle call takes effect at one instant, as in some serial order

Date: 2026-11-14

## Status

Accepted. Part of [#1257](https://github.com/johanhaleby/occurrent/issues/1257). Amends
[ADR 131](0131-a-per-id-lock-closes-the-blocking-catchup-handover-race.md), whose handover lock registry is never
reclaimed, and [ADR 146](0146-a-catch-up-takes-over-a-live-start-the-wrapped-model-loses-as-it-opens.md), whose
takeover resume fails when the wrapped model holds the subscription paused again. Applies to the blocking and reactor
catch-up models, to `ManualStartSubscriptionModel` when it wraps one, and to the two handover engines,
`BlockingHandover` and `ReactiveHandover`, with the push models and projection feeds built on them. The order of the
reactor catch-up marker is amended by
[ADR 179](0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md).

## Context

Many 0.35.0 issues on the catch-up models and the handover engines describe the same bug. Two lifecycle calls, or a
call and the thread that runs a replay, interleave and leave a state that no single-threaded run of the same calls
could leave. Four examples:

* [#1237](https://github.com/johanhaleby/occurrent/issues/1237): a `stop()` during a blocking catch-up `start(..)` can
  leave the wrapped model running and every new catch-up subscription paused.
* [#1255](https://github.com/johanhaleby/occurrent/issues/1255): `isRunning(id)` and `isPaused(id)` are both false while
  a subscribe-started catch-up hands over.
* [#1239](https://github.com/johanhaleby/occurrent/issues/1239): a reactor catch-up action can handle the same event
  twice at once when `start(true)` follows a `stop()`.
* [#893](https://github.com/johanhaleby/occurrent/issues/893) item 32: a stop that comes while a handover delivers what
  it buffered during the replay still lets the catch-up report success, and the reactive one delivers those events
  after `stop()` returned. [#1293](https://github.com/johanhaleby/occurrent/issues/1293) lists the items of #893 that
  are still open for 0.35.0, under the same numbers.

The fixes so far each closed the window their issue found. On the blocking catch-up that added `lifecycleGeneration`,
`liveDelegateResumes` and `stoppedByFailedStart` to `AbstractCatchupSubscriptionModel`, next to the per-id claim and
the handover lock from ADR 131. Each fix was tested against its own window, because nothing wrote down what a correct end
state is when calls overlap.

Three implementation units in 0.35.0 change this code next. L2 changes the blocking catch-up, L3 the reactor catch-up
and HO1 the handover engines. They need one rule to build against and a test that checks it.

## Decision

### The rule

A lifecycle call on a subscription id X, or on the model, takes effect at one instant between the moment it's called
and the moment it returns. A step that a replay thread or a handover engine takes on its own also takes effect at one
instant. Once every call has returned and every replay has finished, X's observables are the ones that some serial
order of those calls and steps gives. That order keeps each thread's calls in the order the thread made them, and it
puts a call that returned before another call was made ahead of that call.

The blocking catch-up makes one exception. A `start(..)`, `stop()`, `shutdown()`, `stopReplay()`, `resumeReplay()` or
`resumeSubscription(id)` made from inside the action of a live call or a replay returns at once and hands its work to
a lifecycle thread, which runs it once the action has returned, as [Blocking catch-up](#blocking-catch-up) describes.
So does a `pauseSubscription(id)`, `resumeSubscription(id, startAt)`, `cancelSubscription(id)`, `subscribe` or
`subscribePaused` made there after the action handed a call over.
Such a call takes effect at that later instant and not between its call and its return, so the serial order puts it
after the calls the same thread made later in the action and ran at once. A `shutdown()` made there forgets the
catch-ups before it returns, and only its call to the wrapped model's `shutdown()` comes later.

A read (`isRunning`, `isPaused`, `isCatchingUp`, `isHeldPausedFor`) answers the state at some instant during the
read.

A call that times out also has a stated serial result. It took effect as a stated first part of itself, usually
nothing, and the exception message says which part. These are the four results `AbstractCatchupSubscriptionModel`
already reports after a 10 second wait:

| Result | Meaning |
| --- | --- |
| `NOTHING_CHANGED` | the call took no effect |
| `STARTED_BUT_NOT_RESUMED` | the model started, X was not resumed |
| `STARTED_BUT_NOT_RUN_AGAIN` | the model started, a parked replay was not run again |
| `STOPPED_ALL_THE_SAME` | the model stopped, a replay it waited for had not finished |

### Two safety properties

Two properties are true at every instant, not only at the end:

* **S1.** At most one call to X's action is running at a time, counting the replay thread and the wrapped model's
  live delivery together.
* **S2.** No call to X's action starts after `stop()` or `pauseSubscription(X)` returned, until a later
  `resumeSubscription(X)` or `start(true)` takes effect. Once `cancelSubscription(X)` has returned, no call to the
  action of the subscription it cancelled starts. A later `subscribe(X)` is a new subscription, and S2 doesn't hold
  back its action. A `stop()` the blocking catch-up hands to its lifecycle thread counts from the instant it takes
  effect there, not from its return, so the action can be called again until then.

S2 covers pause in every phase. A `pauseSubscription(X)` that returns while X still replays its history stops the
replay before the next event, and X is then held paused. Today a pause during a subscribe-started catch-up takes effect
only at the handover ([#1256](https://github.com/johanhaleby/occurrent/issues/1256)). A `resumeSubscription(X)` takes
such a pause back under X's handover lock, which the handover holds while it applies the pause. So the handover either
goes live paused, and the resume then has the wrapped model resume X, or finds no pause and goes live running. Taken
back before the resume waits for that lock, the handover could go live running while the resume still waited, and the
wrapped model's resume then threw `SubscriptionAlreadyRunningException` for a subscription that ran.

A call to the action that a replay started before a `cancelSubscription(X)` or `stop()` took effect can still be
running when that call has taken effect. Once the call has let go of the handover lock, it waits up
to 1 second for the action to return. Past that it returns all the same and logs a `WARN` naming X. A cancel then also
waits, with no time limit, for a catch-up position store the replay began before the cancel took effect, so that store
has returned before the cancel deletes the stored position. Only the wait for the action ends after a second. The
replay decides under the handover lock whether to store and stores after it lets go of the lock, so a pause or a stop
doesn't wait for the store. A `resumeSubscription(X, startAt)` that comes after the stop waits up to 10
seconds for that store before it stores the position it was given, and throws when the store takes longer. After a cancel the
replay delivers no further event, and after a stop none until `start(true)` or `resumeSubscription(X)` runs it again. S2 holds either way, because the replay let the event through before the call took effect. A cancel made from inside
X's action doesn't wait for that action, which can only return after the cancel does. In the blocking catch-up a
`stop()` made there is handed to the lifecycle thread and takes effect once the action has returned, so it has no
action of X to wait for. A cancel of X after a `stop()`
parked its replay doesn't wait again, since the stop did. `shutdown()` doesn't wait. A `start(..)` whose wrapped model
throws before it started parks a replay a subscribe began while it ran, and doesn't wait either, since it takes back
nothing a caller asked for. Like a stop, it tells the call to X's action that the event is no longer wanted, so an
action that waits for a competing consumer's lease returns, and a later `start(true)` runs the replay again instead of
waiting behind that action. A `resumeSubscription(X, startAt)` that holds a running replay an earlier resume started
also tells the call to X's action that the event is no longer wanted, before it waits for that action, so an action
that waits for a competing consumer's lease returns instead of keeping the resume waiting. A `pauseSubscription(X)` doesn't
wait either. It ends a replay at once only while the replay waits to run again after a failure, when no action runs,
and otherwise takes effect at the handover ([#1256](https://github.com/johanhaleby/occurrent/issues/1256)), after the
replay's last call to the action has returned.

### Blocking catch-up

What the rule compares for each id:

| Observable | Values |
| --- | --- |
| phase | absent, replaying, parked (stopped model, ADR 141), held paused (including held because its live start was lost), live and running, live and paused |
| stored checkpoint | none, a catch-up checkpoint, or the live position |
| the handle | started, waiting, or failed with which exception |
| delivered events | which events reached the action, and how often |

For the model, the rule compares whether it's running and whether it was shut down.

The serial result of each call, as one step on that state:

| Call | From | Result |
| --- | --- | --- |
| `subscribe(X)` | absent, model running | replaying, then live and running after the handover |
| `subscribe(X)` | absent, model stopped | parked |
| `subscribe(X)` | any other phase | `DuplicateSubscriptionIdException`, nothing changes |
| `cancelSubscription(X)` | any | absent, stored checkpoint removed |
| `pauseSubscription(X)` | absent | `UnknownSubscriptionException`, nothing changes |
| `pauseSubscription(X)` | live and running | live and paused |
| `pauseSubscription(X)` | replaying | held paused, and the replay delivers nothing more |
| `pauseSubscription(X)` | parked, held paused, or live and paused | `SubscriptionNotRunningException`, nothing changes |
| `resumeSubscription(X)` | absent | `UnknownSubscriptionException`, nothing changes |
| `resumeSubscription(X)` | parked or held paused | the model running, X replaying, then live and running |
| `resumeSubscription(X)` | live and paused | the model running, X live and running |
| `resumeSubscription(X)` | replaying | nothing changes |
| `resumeSubscription(X)` | live and running | `SubscriptionAlreadyRunningException`, nothing changes |
| `resumeSubscription(X, startAt)` | known, not running | live and running from `startAt` |
| `resumeSubscription(X, startAt)` | running | `SubscriptionAlreadyRunningException`, nothing changes |
| `stop()` | any | model stopped, a replay becomes parked, a live subscription becomes paused |
| `start(false)` | any | model running, nothing resumed |
| `start(true)` | any | model running, parked replays and paused subscriptions run again |

Four more rows decide what a failure means:

* A `start(..)` whose wrapped model throws before it started leaves the model stopped, as if the call never happened.
  One that throws after the wrapped model started leaves everything as a successful `start(..)` would, and the
  caller still gets the exception.
* A `resumeSubscription(X)` on a stopped model starts the model first. When the wrapped model throws before it
  started, nothing changes. When it throws after it started, the model runs, X stays as it was, and the caller gets
  the exception.
* A replay whose live start is lost is held paused. A resume of it with an origin above the beginning refuses with
  `CatchupCannotResumeException` until it is cancelled or resumed at a given position.
* A takeover that the wrapped model holds paused again takes over again. Each takeover counts toward the 3 replays
  `MAX_REPLAYS_AGAIN` allows, and once those are used, the resume fails as a takeover fails today when every replay lost
  its live start. This replaces the ADR 146 rule that the resume fails at once
  ([#1269](https://github.com/johanhaleby/occurrent/issues/1269)).

A `cancelSubscription(X)` or `stop()` that takes effect while the replay thread retries `canResumeFrom` ends the
retry, and the replay ends as cancelled or parked. For that the two methods on the blocking
`CheckpointAwareSubscriptionModel` get an overload that takes a `BooleanSupplier stillWanted`. Each default
implementation calls the existing method, so a model written outside Occurrent keeps compiling and gets no early end.
The MongoDB models check `stillWanted` before each new attempt of `canResumeFrom`. Their `globalCheckpoint` doesn't
retry, so its overload exists for a model written outside Occurrent that does. The MongoDB models pass `stillWanted` to
their one-argument `canResumeFrom` through a `ThreadLocal`, so a subclass that overrides only that method is still
called ([#1266](https://github.com/johanhaleby/occurrent/issues/1266) item 1).

```java
default boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
    return canResumeFrom(checkpoint);
}
```

`ManualStartSubscriptionModel` is a layer of its own around the catch-up. Its `start(..)` and `stop()` take effect at
one instant across its own state and the delegate's `start(..)` or `stop()`, and its registration reads
`globalCheckpoint()` inside that same instant.

### Blocking locks

The rule needs one lock for each scope, taken in a fixed order:

| Order | Lock | Taken by |
| --- | --- | --- |
| 1 | `ManualStartSubscriptionModel` model lock | its `start(..)`, `stop()` and registration |
| 2 | catch-up model lock, read and write | `start(..)`, `stop()` and `shutdown()` exclusively, across the loop over every id and the wrapped model's own `start(..)` or `stop()`. Every per-id call shared, around its claim, apart from `resumeSubscription(X)`, which takes it exclusively. Replay threads never take it. The composite `CatchupSubscriptionModel` and its children share one model lock, as they share the claims. Its `start(..)` and `stop()` take it exclusively across every child's `beginStart()` or `stopReplay()` and the wrapped model's call |
| 3 | per-id claim (`lifecycleClaims`) | each lifecycle call on X, as today |
| 4 | per-id handover lock | the replay thread at every step it takes on its own, including the check that lets each event through to the action and the check that decides to store a catch-up checkpoint and records the store as under way, but never while it runs the action or across a catch-up checkpoint store |
| 5 | `lifecycleLock` | short reads and writes of shared fields, nothing taken under it |

The model lock replaces the reasoning behind `lifecycleGeneration`, `liveDelegateResumes` and `stoppedByFailedStart`.
A `start(..)` or `stop()` waits for the per-id calls in flight and then runs alone. A per-id call waits for a
`start(..)` or `stop()` in flight.

A per-id call waits at most 10 seconds for the shared model lock, as it waits at most 10 seconds for its claim. A call
that times out on either wait took no effect (`NOTHING_CHANGED`). The bound matters for an action that makes a per-id
call. A `stop()` holds the model lock exclusively while the wrapped model's `stop()` waits for the actions that run,
and an action that calls `pauseSubscription(Y)` in that time waits for the same lock. The MongoDB change-stream
models' `stop()` gives up on a running action after 1 second, so the pause gets the lock after at most that long. With
a wrapped model whose `stop()` waits for its actions without a deadline, the pause throws after 10 seconds, the action
ends, and the `stop()` goes on.

The exclusive wait has no deadline. It ends once every per-id call in flight has returned. A per-id call holds the
shared lock while it waits for the handover lock, the wrapped model and the checkpoint storage, and none of those waits
has a deadline, so a wrapped model or a checkpoint storage that never returns holds up `start(..)` and `stop()` too.

The one bound on the exclusive wait comes from code the caller supplied, and so does one of the two bounds on a
resume's wait behind the handed calls. The other is the wait for the actions that handed those calls over, described
under the lifecycle thread below. The lifecycle thread asks the `StartAt.dynamic(..)` function of a subscribe handed over from an action under
the model lock, and a resume that relaunches a parked replay calls a `CatchupListener` under it. Such code can wait for
a `start(..)` or `stop()` made on another thread, and that call waits for the lock the code holds. The catch-up marks
the code while it runs under the model lock. A `start(..)`, `stop()`, `stopReplay()`, `resumeReplay()` or
`resumeSubscription(..)` from another thread gives up once the marked code and its own wait have both run for 10
seconds, and throws (`NOTHING_CHANGED`). A `shutdown()` that gives up throws too. It ends the catch-up replays and
marks the model as shutting down before it waits for the lock, as described below, so those two have happened by then,
but the wrapped model isn't shut down yet. A `shutdown()` that found a `start(..)` inside the wrapped model's `start(..)`
has also shut the wrapped model down without the lock to end that start.

Before it throws, the `shutdown()` starts a thread of its own that shuts the wrapped model down under the model lock.
That thread waits for the lock with no deadline and for nothing else, so it runs once the marked code has returned, and
no further call is needed. The exception says so. This matters when the Spring context closes. The Spring Boot starter
builds the MongoDB, durable, catch-up and competing consumer models inside one bean method, and the bean's destroy
method is the competing consumer model's `shutdown()`, which calls the catch-up model's `shutdown()`. No other destroy
method reaches the models inside, so nothing calls `shutdown()` again.

The handed shutdown doesn't run on the lifecycle thread, because a call queued there ahead of it can wait for the model
lock that the marked code holds. A call an action handed over waits for that lock with no deadline, whether it is a
`start(..)`, `stop()`, `stopReplay()`, `resumeReplay()`, `pauseSubscription(id)` or `cancelSubscription(id)`, a resume
or a subscribe, or a `shutdown()`. Marked code that waits for the wrapped model to be shut down would then keep a
handed shutdown queued behind that call from ever running.

When the marked code never returns, the handed shutdown waits for good, so the wrapped model goes on running. A second
`shutdown()` held up the same way before the handed one has run hands over no second one. A `shutdown()` that gets the
lock before the handed one shuts the wrapped model down itself, and the handed one then doesn't call it again. One that
gets the lock after the handed one returns without calling the wrapped model again, so the wrapped model is shut down
once either way. A `start(..)`, `stop()` or `resumeSubscription(id)` made before the handed one has returned changes
nothing, since a `shutdown()` has begun. Made from another thread, it runs on the lifecycle thread behind the calls
queued there. When none was queued as it was made, it waits for the model lock on its own thread, and it still changes
nothing when it gets the lock only after the handed one has shut the wrapped model down. Made on the thread that holds
the lock, it changes nothing either. The catch-up counts the shutdowns handed over and those of them that have
returned, and a call compares the two under the model lock with the second count as it was when the call was made, so
what the call read before the hand-over doesn't decide it. A `start(..)` made once the handed one has returned starts
the wrapped model again. The handed shutdown's thread logs an `ERROR` for what it throws, and the next
`shutdown()` calls the wrapped model's `shutdown()` again.

A `shutdown()` held up by the marked code doesn't shut the wrapped model down without the lock. The marked code can
be inside the wrapped model, which can ask the function again in its own `subscribe(..)` and register the subscription
once the function returns, so the wrapped model would keep a subscription that a shutdown without the lock had already
removed. A wrapped model that asks the function while it holds a monitor its `shutdown()` takes too, as
`ChangeStreamSubscriptions` takes `synchronized (monitor)` in its `shutdown()`, `stop()` and `start(..)`, would also
hold a shutdown without the lock there for as long as the function runs. Running the code the caller supplied outside
the model lock would let these calls go on without waiting for it. This release doesn't do that, so a function or a
listener that waits for a lifecycle call made on another thread makes that call throw after 10 seconds.

A call waiting behind the handed calls gives up only while the lifecycle thread hasn't begun it, and then the lifecycle
thread skips it, so the calls that do run keep their order. The cost is that code the caller supplied that runs longer
than 10 seconds for other reasons makes these calls throw where they would have waited. A per-id call already has the
same 10 second bound on its claim and on the model lock.

A `start(..)`, `stop()`, `stopReplay()` or `resumeReplay()` called on a replay thread outside the subscriber's action,
such as in a `CatchupListener`, or from a thread that holds a claim, throws at once, because it would wait for a per-id
call that can only finish on that thread.

The same call made from the action of a live call or a replay never waits for the model lock, because the call holding
it can be waiting for that action to return, in the wrapped model's `stop()` or for the replay a `stop()` parks. It
doesn't throw, since 0.34.0 ran such a call on the calling thread and an action can't tell whether it runs in a replay
or live. It doesn't take the lock when the lock is free either, since then its outcome would depend on whether another
thread held the lock at that instant. It returns at once and hands the call to a lifecycle thread the children of one
dispatcher share. The lifecycle thread gets the calls an action handed over once that action has returned, in the
order the action made them, and runs what it gets one at a time, in the order it came. It waits for the model lock and
then runs the call, so the call takes effect after the action returns and in turn with the others. Calls handed over
from two actions run in the order the actions returned, which is the order in which they take effect. Until then the
wrapped model can go on delivering to the action, and the replay can go on to its next event. The lifecycle thread doesn't run a call once a `shutdown()` has begun, and logs an `ERROR` naming the
model for a call that throws. It inherits no inheritable thread-local from the thread that happens to create it. The
catch-up marks the thread while it runs the action of a live call or a replay, with or without a record for the
subscription, and a call made inside nested actions of these children waits for the outermost of them. An action of
another dispatcher's children isn't counted, so a call on this model from there runs on the calling thread. An action
that never returns keeps the calls handed over from it from taking effect. The lifecycle thread never waits for an
action, so the calls handed over from another subscription's action still run once that action has returned.

A `start(..)` or `stop()` made on any other thread while a handed call hasn't run yet is handed to the lifecycle thread
too, and the caller waits for it there and gets what it throws. It first waits for every action that had handed a call
over and hadn't returned when the call was made, so a call made after the action signalled the calling thread takes
effect after the call the action handed over. When an action calls `stop()` and then asks another thread for a
`start(..)`, the model ends up started.

An action can wait for the call that waits for it, as one does that calls `stop()` and then waits for the `start(..)`
it asked another thread for. So the wait for the actions gives up after 10 seconds, and the call throws
`IllegalStateException` (`NOTHING_CHANGED`). An action of subscription A that never returns after it called
`resumeSubscription("A")` holds up a `resumeSubscription("B")` from another thread for those 10 seconds, and the resume
of B then throws. Without the bound it would wait for good, so a faulty subscriber would keep a healthy one from
resuming. A call made while no action holds a handed call doesn't wait for any action.

A `resumeSubscription(X)` made on such a thread runs in turn with `start(..)`, `stop()` and `shutdown()` too. It waits
for the calls handed over before it, and then holds the model lock while it reads whether the model is stopped and X
paused, starts the model when both hold, and has the wrapped model resume X. With that read outside the lock, a
`stop()` could complete between the read and the wrapped model's resume. The wrapped model could then run X again after the
stop had paused it, the model read as stopped, and a subscription made afterwards from the start of the history stayed
parked until the next start. The resume takes X's claim under the model lock, in the order a `stop()` takes them when
it parks the replays. A thread that holds a claim never takes the model lock, so the two orders can't cross. The resume
holds the lock while it waits up to 10 seconds for X's claim or for a replay a failed cancel ended, and a `start(..)`,
`stop()` or `shutdown()` waits for it that long.

A resume from a replay's thread outside the action, such as in a `CatchupListener`, or from a thread in the middle of a
call for a subscription, neither takes the model lock nor waits. A handed `stop()` parks the replays under their claims
and handover locks, and such a thread can hold one of them, as a replay run again after a failure does while it tells
its listener. Such a resume takes effect before the handed calls, and a `stop()` from another thread can complete while
it runs. When the model is stopped by then and X is paused, the resume would start the model, so it throws
`IllegalStateException` and changes nothing, as `start(..)` does on such a thread.

A `resumeSubscription(X)` from the action of a live call or a replay goes to the lifecycle thread whole, whatever state
the model is in, and the lifecycle thread runs it as above once the action has returned. Run on the action's thread,
it would take effect before a `stop()` the action handed over earlier, and that `stop()` would then pause X again while
the resume's handle reported X started. Choosing on the action's thread whether to hand it over would read the model's
state without the lock, which a `stop()` from another thread may hold while it waits for the action. It returns at
once a handle that starts once the lifecycle thread has run the resume. A `pauseSubscription(X)`,
`resumeSubscription(X, startAt)`, `cancelSubscription(X)`, `subscribe` or `subscribePaused` from an action that has
already handed a call over goes to the lifecycle thread behind that call. Run at once, each would take effect before
the call handed over earlier. A pause after a handed resume of X would find X still paused and throw. A
`resumeSubscription(X, startAt)` would resume X from `startAt` before the handed resume. A cancel would leave the
handed resume no subscription to resume, and its handle threw `UnknownSubscriptionException`. A subscribe after a
handed `start(false)` on a model a `stop()` from another thread had stopped would find the model stopped, and its
replay stayed parked after the start had run. A subscribe or a resume at a position returns at once a handle whose
`waitUntilStarted` throws what the call threw on the lifecycle thread, and a cancel or a pause returns at once.
Before the action hands anything over, they all run on its thread as in 0.34.0. A thread already holding the model
lock runs its call at once instead, since the lifecycle thread would wait for that thread to give the lock back.

A `shutdown()` doesn't wait for a `start(..)` that is inside the wrapped model's `start(..)`, because a wrapped model can
hold its start until it is shut down. The MongoDB change-stream models retry their start until then, for example while
MongoDB is unreachable. A `shutdown()` first forgets the catch-ups and flags the model as shutting down, without the
model lock. On an ordinary thread it then calls the wrapped model's `shutdown()` at once when a `start(..)` is inside the
wrapped model's `start(..)`, which ends that start, and calls it again under the model lock, so that call runs in turn
with the other `start(..)`, `stop()` and `shutdown()` calls. It doesn't wait for the calls queued on the lifecycle
thread, handed over by an action or made on another thread behind one, which run nothing once the model is flagged.
Otherwise a `start(..)` queued behind a handed call would run after the `shutdown()` had returned, and start the wrapped
model it shut down. A `start(..)` that begins while a `shutdown()` is under way doesn't
call the wrapped model's `start(..)`, and one under way that finds the flag set after the wrapped model's `start(..)`
resumes nothing.

A `shutdown()` from the action of a live call or a replay, or from a thread that holds a claim, doesn't throw and
doesn't wait for the model lock, since a `start(..)` or `stop()` under way may be waiting for that same thread to return
from the action or to give back its claim. It forgets the catch-ups and flags the model at once as above, and hands the
rest to the lifecycle thread, which runs it once the action has returned. A `start(..)` handed over earlier that is
stuck in the wrapped model's `start(..)` on the lifecycle thread holds that `shutdown()` up, and only a `shutdown()` on
an ordinary thread ends it.

A `shutdown()` made on the thread of a `start(..)`, from code that start runs, can't wait for it either. The start holds
the model lock on that thread, so the `shutdown()` takes the lock at once, and the start can't return before the
`shutdown()` does. A `StartAt.dynamic` function the start asks gets there, and so does a wrapped model whose `start(..)`
calls back into code that shuts the catch-up model down. Made before the wrapped model's `start(..)`, the `shutdown()`
shuts the wrapped model down at once, and the start then doesn't call the wrapped model's `start(..)`. Made inside the
wrapped model's `start(..)`, it calls the wrapped model's `shutdown()` at once as above, and the start makes the call
under the lock instead, since the wrapped model may go on to start after the `shutdown()` has returned. The start makes
that call once the wrapped model's `start(..)` has returned or thrown, before the start itself returns. The `shutdown()`
counts as under way until then, so a `start(..)` made in between doesn't call the wrapped model's `start(..)`. The
start returns or throws what it would have without the call. When that call throws, it is logged at `ERROR`, and the
next `shutdown()` calls the wrapped model's `shutdown()` again.

A `stop()` made on the thread of a `start(..)`, from the same code, also takes the lock at once and takes effect at
once. The model then ends stopped with the wrapped model stopped, as if the stop came after the start. Made before the
wrapped model's `start(..)`, the stop has already stopped the wrapped model, so the start doesn't call the wrapped
model's `start(..)` and resumes nothing. Made inside it, or from a method of the wrapped model the start calls after it,
such as `isPaused(..)`, the stop keeps the start from resuming anything after it. Once the wrapped model's `start(..)`
and the resumes have returned or thrown, the start makes the whole stop again under the lock, because the wrapped model
may have gone on to start after the stop returned. The start returns or throws what it would have without the stop, and
a throw from the repeated stop is logged at `ERROR`. A `start(..)` made on the same thread after that stop is the later
call, and the model ends started. When that later `start(..)` throws and stops the model again, as a start that finds
the model stopped does when the wrapped model's `start(..)` throws, it has changed nothing, so the stop is still the
last call and the model ends stopped. A `stop()` from a `CatchupListener`, or from a `StartAt.dynamic` function asked
while the start resumes one subscription, throws `IllegalStateException` and changes nothing, since that thread runs a
replay or holds the subscription's claim.

The start decides by the last call made from inside it. The children of one dispatcher share a record of the last
`start(..)`, `stop()` or `resumeReplay()`, numbered in the order they came, with whether it left the replays and the
wrapped model stopped. A `start(..)` that throws and stops the model again turns its own entry into a stop. Every one of
these calls runs under the model lock, so an entry numbered after the start that holds the lock was made on that
start's thread. The start runs the stop again exactly when the last entry left the wrapped model stopped and no
`shutdown()` is under way or has begun since the start began. A `shutdown()` that returned before the start began
doesn't count, so a start made after a completed shutdown treats a stop made from inside it as any other start does.

A `stopReplay()` made on the thread of a `start(..)` doesn't touch the wrapped model, and the start goes on to resume
subscriptions. When the wrapped model's own `start(true)` resumes what it holds paused, the replays the `stopReplay()`
parked stay parked and the model ends stopped for replays, as if the `stopReplay()` came after the start. When the start
resumes the subscriptions one at a time, the first of those resumes finds the model stopped and starts it again, as
`resumeSubscription(id)` does, so the model ends started, as if the `stopReplay()` came before the start. A
`resumeReplay()` made there after a `stop()` made there lets replays run again, so the start stops only the wrapped
model again, and the model ends as after the start, the `stop()` and the `resumeReplay()`.

Once the wrapped model's `shutdown()` has returned normally under the lock, a later `shutdown()` doesn't call it there
again. One that threw left the wrapped model running, so the next `shutdown()` calls it again. A `start(..)` after a
`shutdown()`, with no handed call left to wait for, calls the wrapped model's `start(..)`, as 0.34.0 did, and the next `shutdown()` calls its `shutdown()`
again. The `shuttingDown` flag is never cleared, as in 0.34.0. So after such a `start(..)` the wrapped model runs, a new
replay counts as no longer wanted from its start, as in 0.34.0, and the lifecycle thread runs no call queued on it,
which is new. A shut-down
catch-up model isn't meant to be started again.

No thread waits for itself under these rules. A replay thread and a thread holding a claim of these children never take
the model lock, since they throw or hand the call to the lifecycle thread. A thread running an action of these children
doesn't take it for a call made in that action. So only a caller's own thread, the lifecycle thread and the thread of a handed shutdown take it. The
actions a caller's thread waits for while it holds the lock run on other threads. The lifecycle thread gets a handed
call only after the action that made it has returned, so it waits for no action, and no thread holding the lock waits
for the lifecycle thread. A caller's thread that waits for the actions holding handed calls, or for the lifecycle
thread, holds neither the model lock nor a claim and runs no action of the children, so those actions run on other
threads. An action that waits for that caller's thread, after it handed a call over, waits for it at most 10 seconds,
since the call on the caller's thread then gives up and throws. A `shutdown()` that calls the wrapped model's `shutdown()`
without the lock waits for nothing the start holds. The shutdown a held-up `shutdown()` hands over waits on
its own thread for the model lock with no deadline, and for nothing else. The marked code holds the lock on another
thread, the lifecycle thread included, so the handed shutdown runs once that code returns. A call from another thread
queued on the lifecycle thread meanwhile gives up after 10 seconds while the marked code runs, as above.

Handover locks are counted and removed like claims. ADR 131 says its registry of handover locks is never reclaimed.
Under this rule it keeps an entry only while a call or a replay thread uses it ([#1257](https://github.com/johanhaleby/occurrent/issues/1257)).
A replay thread holds the lock for one decision at a time, to let one event through or to begin one store, so between
two events the entry is gone, and a
cancel, a pause or a stop takes the lock even when no entry exists.

### Reactor catch-up

The rule, the observables and the serial results are those of the blocking catch-up. Only the locks differ, because a
Reactor thread must not wait on a lock that a caller thread holds across a call to the wrapped model.

* Per id, `NamedCatchupSupport` runs every lifecycle step for X one at a time, in arrival order, without blocking.
  A step from a Reactor thread is queued and the thread moves on. A public call on a caller's thread queues its step and
  waits for that step only. This replaces both `synchronized(state)` across the wrapped model's calls in `handOver` and
  the blocking id lock that Reactor-thread paths take today.
* For the model, `start(..)`, `stop()` and `shutdown()` take `startLock`, above the per-id queues. `stop()`
  takes no `startLock` today.
* A cancel or stop reaches a `canResumeFrom` in flight by disposing the replay's subscription, so the reactor API
  needs no new overload.

### Handover engines

What the rule compares for each engine:

* its phase, which is idle, replaying, delivering what it buffered during the replay, live, stopped, failing or failed
* whether the catch-up marker exists
* the answer each accepted payload got, which is delivered, refused with a reason, or stopped
* the order in which payloads reached the `deliver` callback the engine was created with

The calls are `catchUp`, the three accept calls (`accept`, `acceptReportingDelivery`, `acceptIfLive`),
`stopIfNotCatchingUp`, the stop and cancel calls of the models and feeds built on the engine, a feed's `register`, a
registrar's `close()`, and the reset [#1291](https://github.com/johanhaleby/occurrent/issues/1291) asks for.

The catch-up marker is written in this order on the blocking stack:

1. Read the marker, and skip the replay when it is there.
2. Replay the history.
3. Deliver the payloads buffered during the replay.
4. Write the marker.

On the reactor stack `ReactiveHandover` asks its source for the marker, and ADR 179's "What a reactor catch-up start
replays" says what a start then replays. When the handover writes the marker, it writes it between steps 2 and 3.
[ADR 179](0179-a-catch-up-marker-means-done-on-blocking-and-a-resume-position-on-reactor.md) keeps that write where
it is on purpose, since the broker delivers again a live event of its own that step 3 did not deliver. An event the
handler's own code fed back, on the handler's thread or composed into the `Mono` the handler returns, can have no
broker copy, so the handover does not write the marker while such an event waits in the buffer and the replay did not
deliver it, as ADR 179's "Fed-back events on the reactor stack" says. An event another thread feeds after the handler
has returned is not the handler's own code. The handover takes it like any other live event, and the code that fed it
has to deliver it again when that delivery fails. ADR 179 says what a feed that runs on more than one replica needs.
[#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 31, which would move the reactor write after step
3, no longer applies to the reactor stack. A stop or cancel that takes effect before the marker write means no marker
is written and the catch-up ends as stopped, not as done (#1293 item 32). A failure that starts the
handover failing is permanent, and `ReactiveHandover` then asks the source to delete the marker, as ADR 179's
"Fed-back events on the reactor stack" lists. A failed delivery that only fails the answer a caller waits for does
neither. A marker write that the store applies after that delete is
[#1150](https://github.com/johanhaleby/occurrent/issues/1150), and ADR 179 decides it.

Every accepted payload gets an answer. The replay turn, which lets one catch-up at a time replay on an engine, is
given back on every path. That includes a JVM-fatal error
from `deliver`, a payload the buffer discards, a recording callback that throws and a replay thread that fails to start.

The engine's lock and its replay turn are the only locks the rule needs here. Step 4 runs inside the replay turn and
checks under the engine lock that the turn is still its own. The blocking push model's `markerLocks` are replaced by
that turn. On both push models the `synchronized(this)` monitor becomes a lock that nothing waits under, stores nothing
and calls no engine method
([#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 33).

A `DomainEventFeed` registration is a lifecycle call on the feed's id. A failed engine leaves the failed phase only by a
reset ([#1291](https://github.com/johanhaleby/occurrent/issues/1291)). A registrar `close()` takes effect before or
after each manual-start registration, and a registration that takes effect after it is undone
([#1032](https://github.com/johanhaleby/occurrent/issues/1032)).

### The property harness

A jqwik harness checks the rule against the code, in two test trees:

* `subscription/util/blocking/catchup-subscription`, package `org.occurrent.subscription.blocking.durable.catchup.lifecycle`,
  for `CatchupSubscriptionModel` and `StreamCatchupSubscriptionModel`.
* `subscription/api/blocking`, next to `BlockingHandoverTest`, for `BlockingHandover`.

For the catch-up models, jqwik generates 2 or 3 threads, each with 1 to 3 calls, over 1 or 2 ids. A runner starts the
threads together, waits until every call returned and every replay finished, and reads the observables. A reference
model in the test tree tries every order of the same calls that keeps each thread's order and puts a call that returned
before another call was made ahead of it. For that the runner records when each call was made and when it returned. The
run passes when one of
those orders gives the same call results, the same `isRunning()` of the model, and the same `isRunning(id)` and
`isPaused(id)` for each id. It doesn't compare the stored checkpoint, the handle or the delivered events.

The reference model covers `subscribe`, `cancelSubscription`, `pauseSubscription`, `resumeSubscription(id)`,
`start(false)`, `start(true)` and `stop()`, and a start that fails before or after it takes effect. It doesn't cover
`shutdown()`, `subscribePaused`, `resumeSubscription(id, startAt)`, a live start that is gone or a takeover, so the
harness checks none of those rows yet.

S1 and S2 are checked while the run is going, by a wrapper around the action. A pause or a stop holds back only the
subscriptions whose subscribe returned before it was called. The S2 check lifts it for a `resumeSubscription(X)` that
returned normally or a `start(true)` that didn't fail before it took effect, when that call ended after the pause or
the stop was called or hasn't ended yet, because it can take effect after the pause or the stop. The harness doesn't
catch an action that starts in that window. A resume or a `start(true)` that failed without resuming lifts nothing.

For `BlockingHandover`, a runner steps one replay thread through its history reads and the drain, and runs each accept
call on a thread of its own. There is no reference model of the handover. Its properties check what holds in every
order. The marker is written only for a catch-up that nothing cancelled. A live payload is delivered once, and a
history payload at most once for each replay that read the history. Every accept call gets an answer that agrees with
what was delivered, with one exception. An `acceptReportingDelivery` can report a buffered payload that a failure then
loses, so the harness excuses that loss when an injected failure fired after the call was answered, in a catch-up
that ended after the call. It counts those catch-ups in the order they start, up to and including the first one that
wasn't stopped, because a stopped catch-up either keeps the buffer for the next one or delivers it itself. A failure
anywhere else in the run excuses nothing. A later catch-up gets the replay turn.

A property the code breaks today stays in the harness, disabled with the number of the issue that owns it. The unit
that fixes that issue enables it. The timeout results are not exercised, because the 10 second wait is a constant.

These are disabled today:

* [#1237](https://github.com/johanhaleby/occurrent/issues/1237), a `stop()` that overlaps a `start(..)` ends with the
  model running and a replay parked. Both mixed serial-order properties. Also S2 without a pause, where a
  `start(false)` that runs inside a `stop()` lets the parked replay's action start after the stop returned.
* [#1256](https://github.com/johanhaleby/occurrent/issues/1256), a pause during a replay that a subscribe started
  returns, and the replay keeps delivering to the action until the handover. S2 with a pause, the mixed serial-order
  property with a pause, and 1 example.
* [#1516](https://github.com/johanhaleby/occurrent/issues/1516), `resumeSubscription(id)` after `stop()` returns
  normally and the model stays stopped, when the subscribe it resumes overlapped the stop. The mixed serial-order
  property without a pause, and 1 example.
* [#1520](https://github.com/johanhaleby/occurrent/issues/1520), a `start(true)` whose wrapped model throws after it
  started keeps the parked replays parked. The serial property on scripts that can hold such a call, and 1 example.
* [#1524](https://github.com/johanhaleby/occurrent/issues/1524), a second `pauseSubscription(X)` during a replay that a
  subscribe started returns normally instead of throwing `SubscriptionNotRunningException`. Both serial-order
  properties with a pause, and 1 example.
* [#1293](https://github.com/johanhaleby/occurrent/issues/1293) item 32, a catch-up that is cancelled once its history
  has been read still returns true and writes the marker. 2 handover properties and 2 examples.

Each disabled serial-order property has an enabled copy that runs the same check on scripts without the calls its
issues need, such as a second pause of one id, or a `stop()` in the same script as a `start(..)` or a resume. The
copy's name says which calls it drops. Both S1 properties are enabled. Of the S2 properties two are enabled.
`s2_with_only_subscribe_and_stop_calls_no_action_starts_after_a_stop_has_returned` runs scripts of only subscribes and
a `stop()`, and `s2_with_only_subscribe_cancel_and_stop_calls_no_action_starts_after_a_cancel_or_a_stop_has_returned`
adds cancels to them, so S2 after a cancel is checked only without a pause, a start or a resume in the script.

The reactor harness comes with L3.

### Issues the rule closes

An issue closes by construction when any change that meets the rule and passes the harness closes it.

| Unit | Closes by construction | Doesn't |
| --- | --- | --- |
| L2, blocking catch-up | [#1225](https://github.com/johanhaleby/occurrent/issues/1225), [#1226](https://github.com/johanhaleby/occurrent/issues/1226), [#1237](https://github.com/johanhaleby/occurrent/issues/1237), [#1252](https://github.com/johanhaleby/occurrent/issues/1252), [#1255](https://github.com/johanhaleby/occurrent/issues/1255), [#1256](https://github.com/johanhaleby/occurrent/issues/1256), [#1257](https://github.com/johanhaleby/occurrent/issues/1257), [#1258](https://github.com/johanhaleby/occurrent/issues/1258), [#1269](https://github.com/johanhaleby/occurrent/issues/1269), [#1271](https://github.com/johanhaleby/occurrent/issues/1271), [#1515](https://github.com/johanhaleby/occurrent/issues/1515), [#1516](https://github.com/johanhaleby/occurrent/issues/1516), [#1517](https://github.com/johanhaleby/occurrent/issues/1517), [#1520](https://github.com/johanhaleby/occurrent/issues/1520), [#1524](https://github.com/johanhaleby/occurrent/issues/1524), [#1526](https://github.com/johanhaleby/occurrent/issues/1526), [#1266](https://github.com/johanhaleby/occurrent/issues/1266) item 1 | [#946](https://github.com/johanhaleby/occurrent/issues/946) (javadoc), [#1272](https://github.com/johanhaleby/occurrent/issues/1272) (test gaps), #1266 items 2 to 5 |
| L3, reactor catch-up | [#1156](https://github.com/johanhaleby/occurrent/issues/1156), [#1239](https://github.com/johanhaleby/occurrent/issues/1239), [#1268](https://github.com/johanhaleby/occurrent/issues/1268), [#1270](https://github.com/johanhaleby/occurrent/issues/1270) | |
| HO1, handover engines | #1293 items 31, 32 and 33, [#1058](https://github.com/johanhaleby/occurrent/issues/1058), [#1062](https://github.com/johanhaleby/occurrent/issues/1062), [#1105](https://github.com/johanhaleby/occurrent/issues/1105), [#1134](https://github.com/johanhaleby/occurrent/issues/1134), [#1240](https://github.com/johanhaleby/occurrent/issues/1240), [#1278](https://github.com/johanhaleby/occurrent/issues/1278), [#1280](https://github.com/johanhaleby/occurrent/issues/1280), the `replayOpen` item of [#1107](https://github.com/johanhaleby/occurrent/issues/1107) | #1293 item 29 (an API change), [#1057](https://github.com/johanhaleby/occurrent/issues/1057) (test timing), #1293 item 11 (a test) |

Four more issues are owned outside these units and built against the rule. #1291 and #1032 are the feed's, #1262 is
`ManualStartSubscriptionModel`'s ([#1262](https://github.com/johanhaleby/occurrent/issues/1262)), and #1264 uses the
`stillWanted` overload ([#1264](https://github.com/johanhaleby/occurrent/issues/1264)).

## Alternatives considered

* **An epoch counter instead of the model lock.** Each `start(..)` and `stop()` would bump a counter, and a per-id call
  would check it before and after its work. That is the `lifecycleGeneration` approach generalised. It needs a check at
  every place a call can be overtaken, and a missed check is the kind of bug this ADR exists to stop.
* **Only order `start(..)` against `stop()`.** That closes #1237 but not #1225, #1226 or #1271, where the race
  condition is between a model call and a per-id call.
* **Interrupt the replay thread to end a `canResumeFrom` retry.** An interrupt reaches every blocking call the replay
  thread is in, the action and the event store read included, not only the retry.
* **Change the `canResumeFrom` signature.** Every model written outside Occurrent would stop compiling, for a signal it
  can ignore.
* **Let a pause during a replay take effect at the handover.** That keeps #1256, and the action can still run after
  `pauseSubscription(X)` returned.
* **Keep the ADR 146 refusal.** A wrapped model that pauses a takeover's resume once more would then need a manual
  resume, although the takeover could take the new live start over the same way it took the first.

## Consequences

* L2, L3 and HO1 are checked against the harness, not only against the window each issue found. A property that a
  unit's issue owns is enabled in that unit's PR.
* A blocking `start(..)` or `stop()` waits for every per-id call in flight for as long as that call takes. A per-id
  call waits at most 10 seconds for a `start(..)` or `stop()` in flight, which includes the wrapped model's own
  `start(..)` or `stop()`, and then fails with `NOTHING_CHANGED`.
* The handover lock registry stops growing with every id a model ever ran a catch-up for.
* The `stillWanted` overloads are new public API on `CheckpointAwareSubscriptionModel`. A model written outside
  Occurrent that doesn't override them keeps today's behaviour, where the retry runs on after a cancel until it gives up.
* A reactor catch-up no longer blocks a Reactor thread on a lifecycle lock.
* The harness doesn't test the timeout results. L2 can test them if it makes the 10 second wait configurable in tests.
* The catch-up reference model has no rows for `shutdown()`, `subscribePaused`, `resumeSubscription(id, startAt)`, a
  live start that is gone or a takeover, so no property checks #1269 yet. No property checks #1252, #1255, #1257 or
  #1258 either. They need a replay thread that fails to start, a read of `isRunning(id)` during the handover, a look
  at the handover lock registry and a look at the checkpoint store. L2 adds what its issues need before it claims them
  closed.
