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
  back its action. A `stop()` or a `pauseSubscription(X)` the blocking catch-up hands over from an action counts from
  the instant it takes effect, not from its return, so the action can be called again until then. The replay whose
  action handed over a `stop()` doesn't call that action again once it has returned, also before the `stop()` takes
  effect. A resume that a
  `start(true)` left until after its turn counts as taking effect in that turn, so it doesn't undo a `stop()`, a
  pause or a cancel that came after the turn.

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
replay asks the predicate configured with `PersistCheckpointDuringCatchupPhase` whether to store before it takes the
handover lock, checks under the lock that the replay may still store, and stores after it lets go of the lock. So a
pause or a stop doesn't wait for the store, and a predicate that waits for a `stop()` made on another thread doesn't
hold that stop up. A `resumeSubscription(X, startAt)` that comes after the stop waits up to 10
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

`ManualStartSubscriptionModel` is a layer of its own around the catch-up, and this decision doesn't cover it. Its
`start(..)` and `stop()` change its own state under its own lock and call the delegate's `start(..)` or `stop()`
outside that lock, so a `start(..)` and a `stop()` made at the same time can leave the two disagreeing. Its
registration of a subscription at the model's default start reads `globalCheckpoint()` before it takes that lock,
when the model has a position source and a checkpoint storage.

### Blocking locks

The rule needs one lock for each scope, taken in a fixed order:

| Order | Lock | Taken by |
| --- | --- | --- |
| 1 | `ManualStartSubscriptionModel` model lock | its `start(..)`, `stop()` and registration |
| 2 | catch-up model lock | `start(..)`, `stop()` and `shutdown()`, across the loop over every id and the wrapped model's own `start(..)`, `stop()` or `shutdown()`, and the turn of `resumeSubscription(X)`. No other per-id call takes it, and replay threads never take it. The composite `CatchupSubscriptionModel` and its children share one model lock, as they share the claims. Its `start(..)` and `stop()` take it across every child's `beginStart()` or `stopReplay()` and the wrapped model's call |
| 3 | per-id claim (`lifecycleClaims`) | each lifecycle call on X, and a `stop()` for each X whose replay it parks |
| 4 | per-id handover lock | the replay thread at every step it takes on its own, including the check that lets each event through to the action and the check that the replay may still store a catch-up checkpoint, which records the store as under way, but never while it runs the action, asks the predicate that decides whether to store, or stores |
| 5 | `lifecycleLock` | short reads and writes of shared fields, nothing taken under it |

The model lock replaces the reasoning behind `lifecycleGeneration`, `liveDelegateResumes` and `stoppedByFailedStart`.
A `start(..)`, a `stop()`, a `shutdown()` and the turn of a resume run one at a time, apart from a `shutdown()` that
shuts the wrapped model down without the lock, as described below. A resume that takes its subscription's claim
after such a `shutdown()` began changes nothing. A `stop()` takes the claim and
the handover lock of each subscription whose replay it parks, so it waits for a call for that subscription or a step of
its replay that is under way.

No thread holds the model lock across a checkpoint storage call, a call to the wrapped model for one subscription that
can wait for I/O, or code the caller supplied, apart from the three cases at the end of this part. So a checkpoint
storage that never answers for subscription X holds up only X. A resume of Y runs and delivers, a `start(true)`
resumes the other subscriptions, and `shutdown()` shuts the wrapped model down. A `stop()` waits up to 10 seconds for
that storage at each of its waits. Behind a `start(true)` handed over from an action, the reads before its turn and
the resumes after it hold the lifecycle thread up for 10 seconds at most in all, apart from the turn itself.

A resume reads before its turn, without the lock. It reads the position each catch-up model stored for X, and, when
the wrapped model implements `PreparedResumeSubscriptions`, what the wrapped model's resume of X reads, by calling
`prepareResume(X)`. The turn decides from what was read, and resumes the wrapped model with
`resumeAsPrepared(X, prepared)`, which reads nothing. `DurableSubscriptionModel` implements it. A subclass of it
that overrides `resumeSubscription(X)` and not `resumeAsPrepared(..)` doesn't answer the capability, so the catch-up
calls that override, as in 0.34.0.

The turn uses what was read only while X has the same registration, no write or delete of X's stored position by the
catch-up, or by the wrapped model on its behalf, has returned since the read began, and no `stop()` has taken effect
since. Otherwise the resume reads again and runs another turn. A write by another node, or by another model instance
over the same storage, isn't seen, and the turn uses what was read. So no `stop()` completes between what a resume reads and
the wrapped model's resume. A resume that was reading when a `stop()` took effect takes effect after that stop, as a
resume made once the stop had returned would.

A `start(true)` makes the same read before its turn for every subscription it may resume, one thread per
subscription, and waits up to 10 seconds for those reads. In its turn it resumes each subscription whose read came in
time and is still current. It resumes every other one once its turn is over, without the lock, unless a `stop()` or a
`shutdown()` came after the turn. It does so also when a resume in its turn threw. After a `stopReplay()` since the
turn it reads again and resumes the subscription without starting the model again, and a replay it starts parks at
once, as the replay the turn would have started was parked by that `stopReplay()`. It parks also after a
`resumeReplay()` or `start(false)` that came since, and runs once a `start(true)` or a `resumeSubscription(X)` runs
it again. A pause, a cancel or a new subscribe of X after the turn takes the resume of X off, and it changes nothing
then. A pause or a cancel of X made on the resuming thread, from a `StartAt.dynamic(..)` function or a
`CatchupListener` the resume calls, keeps the resume from resuming X too, since the resume asks again after each of
them, before it resumes X. A pause or a cancel of X that throws changed nothing, so the resume stays owed. A pause of X while that resume is still owed
returns, as a pause made once the resume had run would, and after a `stop()` since the turn it throws
`SubscriptionNotRunningException`, as a pause of X in a stopped model does. A `resumeSubscription(X)` while that
resume is still owed throws `SubscriptionAlreadyRunningException` and changes nothing, as a resume of a running X
does, and after a `stop()` since the turn it resumes X. Each of those resumes runs on a thread of its own, at the same time as the others, so a checkpoint storage that never answers for X holds up the
resume of X and no other. A resume whose read didn't come in time waits for that read and resumes from it. It reads
again when that read failed, or when its subscription's registration or stored position changed after the read
began. So the `start(true)` returns only once those resumes have returned or thrown, but it holds the lock for none
of them. From an action, the lifecycle thread reads nothing once a `shutdown()` has begun. Otherwise it hands each
of them over as described next, starts them all at once, and waits at most 10 seconds in all for them to begin. It
withdraws none of them, since the start they belong to has taken effect. One whose read takes longer resumes once
that read answers, after the calls handed over after the start.

A `start(true)` made on a thread that already holds the lock, from code another start runs such as a
`StartAt.dynamic(..)` function or the wrapped model's `start(..)`, reads nothing before its turn. It resumes each of
those subscriptions after its turn on that same thread, one after another, and each of them reads under the lock. A
thread of their own would wait for the lock while the `start(true)` waited for them, and neither would return. So
those reads run under the lock that the wrapped model's own `start(..)` or the `StartAt.dynamic(..)` function already
holds, two of the cases listed below that still hold the model lock. A `shutdown()` from another thread that waits
for the lock behind one of those reads shuts the wrapped model down without the lock, and throws after 10 seconds, as
described below. When the wrapped model lost the history from the live start of one of those subscriptions and holds
it paused for a takeover, the takeover stores the position its replay starts from, and that store doesn't run under
the lock. So the takeover runs on a thread of its own once the thread that holds the lock has let go of it, and the
`start(true)` returns without waiting for it. That thread runs the takeover and no other turn of the resume, because
the `start(true)` has returned by then, and a turn that resumed the subscription there would take effect after it. A
`pauseSubscription(id)` made after the `start(true)` returned, also from the wrapped model's `start(..)` before the
lock is free, then keeps the subscription paused, as a pause made while a takeover waits does, and the takeover
stores the position it would have replayed from. A pause made once the wrapped model holds the subscription paused
for a takeover counts as one made while the takeover waits, also before the wrapped model has told the catch-up it
paused it. So does a pause made once the catch-up has answered the wrapped model that it takes the subscription over,
before the wrapped model has paused it. That pause goes to the wrapped model too, which can pause the subscription for
the takeover in between and then refuse the pause as one of a paused subscription, and the catch-up takes that refusal
as the pause. When the wrapped model pauses the subscription for that pause instead, it drops the takeover without
telling the catch-up, as `HistoryLossHandling.takeOver(..)` describes. So the catch-up starts the takeover itself once it
has recorded the pause, on a thread of its own, and the takeover stores the position its replay starts from and keeps
the subscription paused. A catch-up made after a restart, over the same checkpoint storage, then replays the lost
history from that position. When the
wrapped model also tells the catch-up it paused the subscription, only the first of the two starts the takeover.
`CatchupSubscriptionModel` sends such a pause to the child that answered.

A call for X that an action handed over, a subscribe, a resume, a positioned resume, a pause or a cancel, runs on a
thread of its own without the model lock, as the same call made from another thread does. The lifecycle thread waits
for it to begin, which is when it has taken its claim, or for a resume when its turn is over. It waits at most 10
seconds, so the call keeps its place among the handed calls. Then it logs at `ERROR` and goes on with the calls
handed over after it. It withdraws the call unless the call has entered a turn or taken its claim by then. A
withdrawn call throws `IllegalStateException` at its claim or its turn and changes nothing. One that has entered a
turn isn't withdrawn. A resume that reads again runs a second turn, and its first turn may already have started the
model or a takeover. It takes effect once it gets to that second turn, also after the calls handed over after it. So
the `StartAt.dynamic(..)` function of a handed subscribe runs without the lock, and a `start(..)`, `stop()` or
`shutdown()` made meanwhile doesn't wait for it. Another call for X waits for X's claim, at most 10 seconds.

A thread that holds the model lock and waits for one subscription gives up after 10 seconds, apart from the reads of
a `start(true)` made on a thread that already holds the lock, above, and the wrapped model's resume in case 2 below,
which have no deadline of their own. That covers a `stop()` waiting for the claim or the handover lock of a replay it parks, while the replay waits for its
checkpoint storage or the wrapped model, and a resume's turn waiting for a cancelled replay to return. A `stop()` that
gives up has stopped the model all the same (`STOPPED_ALL_THE_SAME`), and that replay parks itself, as described below.
A replay a resume started can be in its handover then, waiting for its storage to give it the live start. Once the read
returns, it finds the model stopped and parks instead of resuming the wrapped model. A resume in the wrapped model that
blocks until after the stop gave up still resumes the subscription there.
A `shutdown()` that waits for the lock while its holder has waited 100 milliseconds or more for one subscription shuts
the wrapped model down without the lock. When that wait has no deadline of its own, and it and the `shutdown()`'s own
wait have both run for 10 seconds, the `shutdown()` throws (`SHUT_DOWN_PAST_A_WAIT`), and a thread of its own shuts the
wrapped model down again under the lock, as behind code the caller supplied, described below.

The three cases that still hold the model lock across I/O or code the caller supplied are these:

1. The wrapped model's own `start(..)`, `stop()` and `shutdown()`, and whatever they read. That includes the checkpoint
   reads of a `start(true)` made inside the wrapped model's `start(..)`, described above.
2. The wrapped model's `resumeSubscription(X)`, which the turn calls. A wrapped model that doesn't implement
   `PreparedResumeSubscriptions` reads in it. A wrapped model that isn't part of Occurrent and whose resume blocks holds
   the lock until it returns, also when a `DurableSubscriptionModel` around it prepared the resume. A
   `shutdown()` that waits for the lock meanwhile shuts the wrapped model down without the lock, as above, so the
   wrapped model then has a resume and a `shutdown()` running at once, as it would with two threads calling it. After
   10 seconds the `shutdown()` throws, as above. The MongoDB subscription models read nothing in the resume that
   `DurableSubscriptionModel.resumeAsPrepared(..)` calls, but it waits for the MongoDB model's monitor. Their
   `pauseSubscription(..)` and `cancelSubscription(..)` of another subscription hold that monitor while they close its
   change stream cursor, and closing one can wait for the server. So the turn can wait under the lock until that close
   returns. A `shutdown()` made meanwhile shuts the MongoDB model down without the lock, and the MongoDB model's
   `shutdown()` waits for the same monitor, so it returns once the close has.
3. A `StartAt.dynamic(..)` function that a start's or a resume's turn asks, and a `CatchupListener` that a resume
   relaunching a parked replay tells. Both run under the lock, bounded as follows. So do the checkpoint reads of a
   `start(true)` made inside such a function, described above.

For a `start(..)`, `stop()` or resume made on a thread that runs no action, the one bound on the wait for the lock
comes from code the caller supplied. A call handed over from an action waits for the lock with no deadline. A
`shutdown()` has a second bound, a wait for one subscription that has lasted 10 seconds, such as a resume that blocks
in the wrapped model or the reads of a `start(true)` made inside another start, after which it throws. Code
the caller supplied is also one of the two bounds on a resume's wait behind the handed calls. The other is the wait
for the actions that handed those calls over, described under the lifecycle thread below. Such code can wait for
a `start(..)` or `stop()` made on another thread, and that call waits for the lock the code holds. The catch-up marks
the code while it runs under the model lock. A `start(..)`, `stop()`, `stopReplay()`, `resumeReplay()` or
`resumeSubscription(..)` from another thread gives up once the marked code and its own wait have both run for 10
seconds, and throws (`NOTHING_CHANGED`). A `start(true)` whose turn has taken effect and whose resume after the
turn gives up that way throws saying that the start took effect (`START_TOOK_EFFECT`). A `shutdown()` that gives up
throws too. It ends the catch-up replays and
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
lock that the marked code holds. A `start(..)`, `stop()`, `stopReplay()`, `resumeReplay()` or `shutdown()` an action
handed over waits for that lock on the lifecycle thread with no deadline. Marked code that waits for the wrapped model
to be shut down would then keep a handed shutdown queued behind that call from ever running. A handed call for one
subscription holds the lifecycle thread up for at most 10 seconds, as described above.

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
the model lock would let these calls go on without waiting for it. This release does that for a subscribe handed over
from an action, as described above, but not for a function a start's or a resume's turn asks, or for a listener a
relaunch tells. Such a function or listener that waits for a lifecycle call made on another thread makes that call
throw after 10 seconds.

The replay thread also tells a `CatchupListener` while it holds the subscription's handover lock, with no model lock
held. It does so when the replay runs again from its origin because the live start left the history, and when a replay
a resume started runs again after a failure. A listener there that waits for a `stop()` made on another thread would
hold that stop as long as it waits. The stop holds the model lock and waits for the handover lock to park the replay,
so every other lifecycle call and `shutdown()` would wait behind it too. The catch-up marks code the caller supplied
that runs under a handover lock, per subscription. A lifecycle call for that subscription holds its claim, and it gives
up once the marked code and its own wait for the handover lock have both run for 10 seconds. A call that also holds the
model lock, such as a `stop()`, gives up after 10 seconds of waiting for the handover lock whatever holds it, as
described above. Either throws `IllegalStateException` saying what it did before it took the claim. A `stop()` that gives up has stopped the model (`STOPPED_ALL_THE_SAME`),
and the replay parks itself once the listener returns. It then stays parked, as a replay the stop parked does, until a
`start(true)` or a resume of its own subscription. A `start(false)` made before the listener returns, such as the one a
resume of another subscription can make in a stopped model, doesn't let it go on. A call for another subscription that
waits behind that stop waits those 10 seconds, and then goes on.

A call waiting behind the handed calls gives up only while the lifecycle thread hasn't begun it, and then the lifecycle
thread skips it, so the calls that do run keep their order. The cost is that code the caller supplied that runs longer
than 10 seconds for other reasons makes these calls throw where they would have waited. A per-id call already has the
same 10 second bound on its claim.

A `start(..)`, `stop()`, `stopReplay()` or `resumeReplay()` called on a replay thread outside the subscriber's action,
such as in a `CatchupListener`, or from a thread that holds a claim, throws at once, because it would wait for a per-id
call that can only finish on that thread.

The same call made from the action of a live call or a replay never waits for the model lock, because the call holding
it can be waiting for that action to return, in the wrapped model's `stop()` or for the replay a `stop()` parks. It
doesn't throw, since 0.34.0 ran such a call on the calling thread and an action can't tell whether it runs in a replay
or live. It doesn't take the lock when the lock is free either, since then its outcome would depend on whether another
thread held the lock at that instant. It returns at once and hands the call to a lifecycle thread the children of one
dispatcher share. The lifecycle thread gets the calls an action handed over once that action has returned, in the
order the action made them, and runs what it gets one at a time, in the order it came. For a call of the model it
waits for the model lock and then runs the call. A call for one subscription it starts on a thread of its own, and
waits for that call to begin, as described above. So the call takes effect after the action returns and in turn with
the others. Calls handed over
from two actions run in the order the actions returned, which is the order in which they take effect, apart from a
call for one subscription that didn't finish in its 10 seconds and wasn't withdrawn, as described above. Until then the
wrapped model can go on delivering to the action, and other replays can go on to their next event. A replay whose
action handed over a `stop()` or a `stopReplay()` of its own model delivers no further event once the action has
returned, whether or not the call has run by then. A `start(true)` or a resume made after the call runs that replay again. The lifecycle thread begins no call once a `shutdown()` has begun, and a resume a handed `start(true)` left until
after its turn changes nothing once it takes its claim after that. The lifecycle thread logs an `ERROR` naming the
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
`IllegalStateException` (`NOTHING_CHANGED`). A call made while no action holds a handed call doesn't wait for any
action.

A `resumeSubscription(X)` from another thread waits only for the actions whose handed calls concern X. Those are the
actions of X, the actions that handed over a pause, a cancel, a resume or a subscribe for X, and the actions that handed
over a `start(..)` or a `stop()`, which concern every subscription. An action of subscription A that never returns
after it called `resumeSubscription("A")` doesn't hold up a `resumeSubscription("B")`, which resumes B at once, and B
gets its events. A `resumeSubscription("A")` waits for that action for 10 seconds and then throws. A `start(..)` or
`stop()` from another thread waits for every action that holds a handed call, since each handed call takes effect
before it. So while A's action doesn't return, those calls throw after 10 seconds and change nothing, and no other
subscription's delivery or resume waits for them.

A `resumeSubscription(X)` made on such a thread runs in turn with `start(..)`, `stop()` and `shutdown()` too. It waits
for the calls handed over before it that concern X, and reads what X's resume needs without the lock, as described
above. Then, in its turn, it holds the model lock while it reads whether the model is stopped and X paused, starts the
model when both hold, and has the wrapped model resume X from what was read. If a `stop()` completed between the read
and the wrapped model's resume, the wrapped model could run X again after the stop had paused it, the model read as
stopped, and a subscription made afterwards from the start of the history stayed parked until the next start. So the
turn reads again when a stop came after the read. The resume takes X's claim under the model lock, in the order a
`stop()` takes them when it parks the replays. A thread that holds a claim never takes the model lock, so the two
orders can't cross. The resume holds the lock while it waits up to 10 seconds for X's claim or for a replay a failed
cancel ended. A `start(..)` or `stop()` waits for it that long, and a `shutdown()` shuts the wrapped model down without
the lock once that wait has run for 100 milliseconds.

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
doesn't take it for a call made in that action. So no thread that takes the model lock is a replay thread, holds a
claim of these children, or takes it for a call its own action made. The actions a caller's thread
waits for while it holds the lock run on other threads. The lifecycle thread gets a handed call only after the action
that made it has returned, so it waits for no action, and no thread holding the lock waits for the lifecycle thread.
The lifecycle thread holds no lock while it waits for a handed call for one subscription to begin, and it waits at most
10 seconds. A caller's thread that waits for the actions holding handed calls, or for the lifecycle
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
copy's name says which calls it drops. Both S1 properties are enabled. Of the four S2 properties three are enabled.
`s2_without_pause_no_action_starts_after_a_cancel_or_a_stop_has_returned` runs every call but a pause,
`s2_with_only_subscribe_and_stop_calls_no_action_starts_after_a_stop_has_returned` runs scripts of only subscribes and
a `stop()`, and `s2_with_only_subscribe_cancel_and_stop_calls_no_action_starts_after_a_cancel_or_a_stop_has_returned`
adds cancels to them. `s2_no_action_starts_after_a_cancel_a_pause_or_a_stop_has_returned` is disabled for #1256, so
S2 after a pause isn't checked by a property.

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
* A blocking `start(..)` or `stop()` waits for the per-id calls in flight. A `stop()` waits at most 10 seconds for
  each claim or handover lock it needs and then throws with the model stopped all the same (`STOPPED_ALL_THE_SAME`). A
  `resumeSubscription(X)` waits for a `start(..)`, `stop()` or `shutdown()` in flight with no deadline, unless code
  the caller supplied, or an action that handed that call over and hasn't returned, holds that call up. Another per-id call waits at most 10 seconds for X's claim, which a `stop()`
  holds while it parks X's replay, and then fails with `NOTHING_CHANGED`.
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
