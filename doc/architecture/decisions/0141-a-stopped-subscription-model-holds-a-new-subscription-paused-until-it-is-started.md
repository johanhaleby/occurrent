# 141. A stopped subscription model holds a new subscription paused until it is started

Date: 2026-09-29

## Status

Accepted. Resolves part of [#1166](https://github.com/johanhaleby/occurrent/issues/1166). Amends
[ADR 98](0098-reactor-subscriptionmodel-means-what-blocking-subscriptionmodel-means.md), which says the blocking
catch-up model abandons a replay that `stop()` cuts short.

## Context

`CompetingConsumerSubscriptionModel` wraps another subscription model, and after `stop()` it has to keep a new
subscription from delivering anything until the node is started again and wins the lease. It can only do that if the
model it wraps behaves the same way for a subscription made while that model is stopped.

`DurableSubscriptionModel` decides where a new subscription starts in `subscribe(..)`, when it has no stored position
for it. `ManualStartSubscriptionModel` stores that position when a subscription is registered for the same reason
(ADR 130). A competing consumer model that passed the subscription on only once the node had won the lease therefore
started it from the position at that time, and skipped every event written between `subscribe(..)` and `start()`.

Passing it on straight away is only safe when the wrapped model holds it paused. Two models did not.
`NativeMongoSubscriptionModel` opened a change stream for it straight away, until
[#1171](https://github.com/johanhaleby/occurrent/pull/1171) changed that. The blocking
`CatchupSubscriptionModel` ended the replay of a subscription made while it was stopped as soon as the replay began,
and did the same to a replay that `stop()` cut short. In both cases the subscription still counted as running a
catch-up, so `isRunning()` returned `true` for good, the replay never went on to the live model, and nothing ran it
again.

Behind a `CompetingConsumerSubscriptionModel` that also broke the next subscription. The competing consumer model
starts the wrapped model before it hands over a subscription whose lease the node won, but only when the wrapped model
says it is not running. After such a stop it never started it again, so a subscription whose lease the node won later
went to a stopped live model and received nothing, while the node kept refreshing the lease. Every `start(true)` also
threw `SubscriptionAlreadyRunningException` for the subscription whose replay was cut short.

## Decision

**Every subscription model that `CompetingConsumerSubscriptionModel` wraps holds a subscription made while it is
stopped paused.** It registers the subscription, delivers nothing, returns `false` from `isRunning(id)` and `true` from
`isPaused(id)`, and runs it once when it is started. Its own `isRunning()` says whether it runs.

Where such a subscription starts is up to each model:

| Model | Where a subscription made while it is stopped starts, for `StartAt.now()` or the model default |
|---|---|
| `DurableSubscriptionModel` | The position it records in `subscribe(..)` when it has none stored |
| `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel` | The operation time MongoDB answers with, asked for when `subscribe(..)` is called |
| `InMemorySubscriptionModel` | The first event fed to it after it is resumed |
| The blocking catch-up models | The replay's own start position, and the live model's for a subscription with no replay |

**`SubscriptionModel.subscribePaused(..)` holds a new subscription paused the same way, whether or not the model
runs.** `SpringMongoSubscriptionModel`, `NativeMongoSubscriptionModel`, `InMemorySubscriptionModel`,
`DurableSubscriptionModel`, `ManualStartSubscriptionModel`, the blocking push, synchronous and catch-up-then-push
models, and the blocking catch-up models implement it. The default implementation throws
`UnsupportedOperationException`. A default that subscribed while the model is stopped would ask `isRunning()` first and
subscribe after, and a `resumeSubscription(..)` or a lease grant on another thread can start the model between the two
calls, so the subscription would run on a node that holds no lease for it. Subscribing and pausing straight after fails
the same way, since the model can deliver an event between the two calls.

**`CompetingConsumerSubscriptionModel` hands a subscription made while it is stopped to the wrapped model in
`subscribe(..)`, through `subscribePaused(..)`, and does not register it with the lease strategy.** The wrapped model
then holds it paused also when a `resumeSubscription(..)` since `stop()` has started it again, or when its own `stop()`
threw. A lease won while stopped would lock every other node out of a subscription this node does not serve. `start()`
makes it compete for the lease whether or not it resumes subscriptions automatically, since nobody paused it, and
winning the lease resumes it in the wrapped model.

**A subscription made while `CompetingConsumerSubscriptionModel` runs goes to the wrapped model through
`subscribePaused(..)` too, once the node has won its lease, and is resumed there only when the node still holds the
lease after that.** A subscription whose lease goes to another node while the wrapped model makes it then stays
paused there, and waits for a grant. Subscribed with `subscribe(..)`, it would deliver until the node noticed the loss.

**A wrapped model that refuses `subscribePaused(..)` with `UnsupportedOperationException` gets the subscription the
way it did in 0.33.0, the one case where a subscription made while the node is stopped can deliver events before
`start()` without being resumed.** The node competes for the lease straight away, and when it wins, it subscribes the
subscription in the wrapped model without starting that model. When the wrapped model runs it, `subscribe(..)` records
it as running, so it delivers events before `start()`, and `start()` finds it started. From then on it competes as a
subscription the user resumed since `stop()` does. A lease the node loses pauses it and keeps it competing, and winning
the lease back resumes it. When the node loses the lease in `subscribe(..)`, or when the wrapped model holds the
subscription paused because that model is stopped, it gives up its registration and waits for `start()`, since a
stopped node competes for nothing it does not deliver. Waiting for `start()` also after winning would start it where the
wrapped model starts a subscription at that moment, and lose every event written in between. Losing no event ranks above
what `stop()` promises, so the rule for this case is that for any wrapped model, nothing that works in 0.33.0 throws or
delivers fewer events, and nothing is delivered without the lease.

**`CompetingConsumerSubscriptionModel.stop()` pauses a subscription in the wrapped model when that model still runs it
before it gives up the lease.** A wrapped model that threw from its own `stop()` can still run every subscription, and
a node delivers only while it holds the lease. A subscription the wrapped model still runs after
`pauseSubscription(..)` has returned keeps its lease and stays running, and `stop()` throws. When the wrapped model's
own `stop()` threw, `stop()` throws an `IllegalStateException` with that failure as its cause, which names the
subscriptions it paused and says that `start(true)` resumes them while `start(false)` keeps them paused.

`stop()` also gives up the lease of a subscription that `subscribe(..)` is making on another thread once its
registration has returned, unless the wrapped model already runs that subscription. That one, and one whose
registration is still under way, give the lease up at the next step of their `subscribe(..)`, so each can hold it after
`stop()` has returned until then. Waiting for them would make `stop()` wait for a registration or a subscribe in the
wrapped model. A `subscribe(..)` whose registration `stop()` gave up registers again at its next step once the model
has been started, so a `stop()` and a `start(..)` that both overtake it do not keep it waiting for a grant it is not
registered for.

**While `CompetingConsumerSubscriptionModel` is started, a subscription that is neither cancelled nor paused by the
user ends up registered with the lease strategy, and runs in the wrapped model only while the node holds its lease.**
While the model is stopped, nothing of it stays registered or runs, apart from the exceptions in this decision. A call
to the lease strategy or the wrapped model that throws delays that end state. It never replaces it with another state,
and it never needs a call from the user to recover.

After a call fails, a thread of its own tries the subscription again, with the backoff the MongoDB lease strategies use
by default, until it has reached the end state. A call to the wrapped model can take effect before it throws, so the
model's record of the subscription is no longer evidence of what the wrapped model does. Where such a call throws, a
subscription recorded as running that the wrapped model no longer runs is recorded as paused, before the thread starts.
Each try decides under the monitor from what holds at that moment, which includes what the wrapped model does, and
not only what the model recorded, which of these it does next. It makes that call without the monitor, and then decides
again, until nothing is left to do:

- It registers the subscription when it should compete and is not registered.
- It pauses the subscription in the wrapped model when it runs there without the lease, and records it as paused by
  the loss of its lease.
- It resumes the subscription, or subscribes it in the wrapped model, when the node holds the lease and it should run.
  That includes one recorded as running that the wrapped model does not run.
- It unregisters the subscription when it should not compete, and first pauses it in the wrapped model when that model
  runs it. One recorded as running is recorded as paused, as `stop()` records it. When the unregister throws while the
  node still holds the lease, it gives the lease back.

A subscription recorded as running that the wrapped model does not run is never where a try stops. A grant for one
that may run resumes it, where 0.33.0 did nothing because it found the subscription recorded as running.

A try that fails again is followed by another, and every fifth try that fails is logged as a warning, so a subscription
that delivers without its lease, or one that is not registered, stays visible. At most one thread tries a given
subscription. One that `subscribe(..)` is still making is tried once that `subscribe(..)` returns or throws. The thread
removes its record of the subscription on every path that ends it, an interrupt included, so the next failure starts a
new thread.

`subscribe(..)`, `start(..)` and `resumeSubscription(..)` log such a failure as a warning, record the subscription, and
return. `start(..)` still throws what a subscription that does not compete threw, since nothing tries that one again.
`stop()` and `pauseSubscription(..)` throw what failed, and the thread tries the subscription again all the same.
`cancelSubscription(..)` throws too. When the wrapped model still holds the subscription afterwards, the thread tries it
again. When it does not, the model forgets the subscription and gives up its registration, as a cancel that returns
does. `onConsumeGranted(..)` logs a lease check that throws as a warning, and
hands the subscription to the thread. In 0.33.0 a lease-loss callback whose pause failed threw, and the subscription
went on delivering. `start(..)` and `resumeSubscription(..)` threw what the registration threw. A wrapped model that
returns from `pauseSubscription(..)` normally and keeps running the subscription goes on delivering it. When the thread
tries such a subscription without the lease, it pauses it again on every try, until the node holds the lease again.

A start or a resume in the wrapped model that throws, for a subscription whose lease the node holds, gives the lease
back, so the next grant tries again. When the wrapped model runs the subscription after the throw and did not run it
before the call, the node keeps the lease and records the subscription as running instead, since giving the lease back
would let another node run it too. One the wrapped model ran before the call, such as a subscription made there directly
under the same id, is not what the call started, so the lease goes back.

**`CompetingConsumerSubscriptionModel` calls the lease strategy and the wrapped model for a subscription only while it
holds a lock kept for that subscription, except for two steps of `subscribe(..)` and `shutdown()`, which hold no lock.
It never holds its monitor during such a call.** The monitor is held only for a moment, to read and write what several
subscriptions share, such as the ids being made and the `start(..)` and `stop()` calls a subscription handed to a
thread of its own still has to get. These threads act on it:

| Thread | Under the subscription's lock | Without it |
|---|---|---|
| A user's `pauseSubscription(..)`, `resumeSubscription(..)` and `cancelSubscription(..)`, also when an action calls one on the wrapped model's delivery thread | Everything, including registering with the lease strategy in `resumeSubscription(..)` | Nothing |
| A user's `start(..)` and `stop()` | Applying the call to each subscription whose lock is free, on a thread of its own for each subscription | Recording that the model is started or stopped, starting or stopping the wrapped model, and handing each subscription whose lock is taken to a thread of its own |
| `onConsumeGranted(..)` and `onConsumeProhibited(..)`, which the lease strategy calls on the notifier for that subscription or on the thread that registers | Everything, including subscribing, resuming or pausing in the wrapped model | Handing the subscription to the thread that tries it again, when another thread holds the lock, also when that thread is a `subscribe(..)` making the subscription, and returning when the callback comes out of that try's own call |
| `subscribe(..)` | Deciding the next step from what holds at that moment, and recording the subscription | Registering with the lease strategy, and making the subscription in the wrapped model |
| The thread that tries a subscription again after a call failed | Every try, including each call to the lease strategy or the wrapped model | Waiting between two tries |
| `shutdown()` | Nothing | Everything, which is shutting the lease strategy down, then the wrapped model, and then giving up each lease once |

The MongoDB lease strategies retry a registration for as long as MongoDB cannot be reached, and making a subscription
in the wrapped model can take as long as opening a change stream. A lifecycle call or a lease callback that waited for
either would hang for as long, so `subscribe(..)` holds nothing while it does them. After each of the two steps it takes
the subscription's lock and decides the next one from what holds then, whatever held when it began. That is whether the model is
shut down or stopped, whether the node holds the lease, and whether the wrapped model runs what it made or holds it
paused.

- The id cancelled with `cancelSubscription(..)`: it cancels what it made in the wrapped model, since the user asked
  for that, gives up its registration and throws `IllegalStateException`.
- Shut down: it gives up its registration and throws `IllegalStateException`. What it made is left to the wrapped
  model's own `shutdown()`, and paused first when the wrapped model runs it. A `subscribe(..)` after `shutdown()`
  throws the same exception before it does anything.
- Stopped, nothing made yet: it gives up a registration that a `stop()` overtook, and makes the subscription with
  `subscribePaused(..)`. A wrapped model that refuses gets it as described above.
- Stopped, something made: it pauses the subscription when the wrapped model runs it, gives up its registration, and
  records it as waiting for `start()`. One the wrapped model still runs after the pause keeps its lease and is recorded
  as running, and registers again first when a `stop()` gave up its registration before the wrapped model ran it. One
  that a model refusing `subscribePaused(..)` runs is recorded as running too.
- Started, not registered: it registers. That includes one whose registration a `stop()` gave up while the wrapped
  model made it, also when the unregister threw in that `stop()`, which the thread above tries again.
- Started, lease held, nothing made yet: it starts the wrapped model when that model is stopped, and makes the
  subscription there with `subscribePaused(..)`, or with `subscribe(..)` when the wrapped model refuses that. A start
  that throws gives up the registration, and `subscribe(..)` throws.
- Started, lease held, something made: it resumes what the wrapped model holds paused, and starts that model first when
  it is stopped. A resume that throws keeps the subscription paused there, records it as waiting and gives the lease
  back, so it stays a candidate and the next grant tries again, and `subscribe(..)` returns. When giving the lease back
  throws too, `subscribe(..)` returns all the same. The thread above then resumes the subscription while the node
  still holds the lease, and otherwise it waits for a grant.
- Started, lease not held: it records the subscription as waiting for a grant, still registered. When the wrapped model
  runs what it made, which only a model refusing `subscribePaused(..)` does, it pauses it there first. One the wrapped
  model still runs after the pause is recorded as running and stays registered, the same as one whose lease-loss
  callback cannot pause it, and a pause that threw is tried again.

A step that throws before the wrapped model has made anything gives up the registration, records nothing, and
`subscribe(..)` throws. Once the wrapped model has made the subscription, a step that throws makes `subscribe(..)` log
a warning and return instead. The model records the subscription as running when the wrapped model runs it, and as
waiting for a grant otherwise, and the thread above brings it to the end state. A `subscribe(..)` whose model is shut
down, or whose id is cancelled, while it makes the subscription still throws as described above.

A lease callback for a subscription that `subscribe(..)` has not recorded yet, and whose lock is free, finds nothing
and does nothing, and the step after it asks the strategy whether the node holds the lease. One that finds the lock
taken by a step of that `subscribe(..)` is handed to the thread that tries the subscription again, which starts once
`subscribe(..)` has returned and decides from what the step recorded. The wrapped model is started, or a subscription
resumed or subscribed there, only after a check that the model is not stopped. The check runs under a lock that
`stop()` also takes to record that it is stopped, and `stop()` stops the wrapped model only once every call let through
before that has returned. So a `subscribe(..)`, a try or a lease callback that a `stop()` overtook never starts the
wrapped model, or runs a subscription there, after that `stop()` has returned.

These rules follow:

1. A subscription delivers only while the node holds its lease, except for two cases. A wrapped model that refuses
   `subscribePaused(..)` runs a subscription whose lease is lost while that model makes it, until the step after
   pauses it. A wrapped model that returns from `pauseSubscription(..)` normally but keeps running the subscription
   goes on delivering it. A pause that throws is tried again until the wrapped model no longer runs the subscription
   or the node holds the lease again, so it delivers without the lease only until a try succeeds. 0.33.0 never paused
   a subscription whose lease went while the wrapped model made it, and never tried a failed pause again.
2. Once `subscribe(..)` has returned, the model records the id, whatever ran on other threads in the meantime. When a
   call failed on the way, the thread that tries it again brings the registration and the wrapped model to the end
   state above. Once `subscribe(..)` has thrown, the model records nothing, and the wrapped model holds nothing, except
   after a shutdown, when its own `shutdown()` ends what it holds.
3. No lifecycle call and no lease callback waits for a registration or a subscribe in the wrapped model that
   `subscribe(..)` does on another thread.
4. A failure that goes away delays the end state above and never replaces it. The next try after the failure has
   gone away reaches the end state, with no `start(..)`, `resumeSubscription(..)` or `subscribe(..)` from the user. That
   includes a registration that failed in `start(..)` or `resumeSubscription(..)`, and one that failed in a
   `subscribe(..)` after the wrapped model had made the subscription.
5. Only a user's `cancelSubscription(..)` cancels a subscription in the wrapped model. Cancelling a
   `DurableSubscriptionModel` subscription deletes the position it stored, so a shutdown during `subscribe(..)` that
   cancelled it would start the next run from the present.

While a call for one subscription waits for the lease strategy or the wrapped model, no call, grant or lease loss for
another subscription waits for it. In 0.33.0 every such call ran under the monitor, so a `start(..)` or
`resumeSubscription(..)` whose registration retried through a MongoDB outage held up every other subscription on the
node for as long.

A lease callback never waits for a subscription's lock. When another thread holds the lock, the callback hands the
subscription to the thread that tries it again, which takes the lock as soon as it is free and decides from what holds
then. It does not wait for the backoff first, since nothing failed, and a thread already waiting for the backoff after
a failure stops waiting.
That includes a subscription a `subscribe(..)` is still making, so a grant or a loss that arrives while a step holds
the lock is acted on too. The MongoDB lease strategies tell a listener about a lease only when it changes, so a callback
the model dropped would never come again. A callback that a `stop()` overtook on the lease strategy's notifier is left to that try too, instead of throwing
into the lease strategy.

The MongoDB lease strategies tell the listeners about each subscription on a notifier of its own, in the order a
refresh round decided the changes (ADR 140). So a grant whose callback resumes subscription A in the wrapped model,
which opens a change stream, holds up the later callbacks for A and none for subscription B. Each subscription whose
callback waits takes a thread of the notifier's, a virtual one on Java 24 and later and a platform one on Java 21 to 23,
where a virtual thread that waits inside `synchronized`, as the pause of the MongoDB subscription models does, would
keep the platform thread it runs on.

`start(..)` and `stop()` never wait for a subscription's lock either. Each applies itself to every subscription at
once, on a thread of its own for each subscription, and returns once it has taken care of each subscription whose lock
was free and released that lock. So a call for one subscription that waits for the lease strategy or the wrapped model
through an outage holds up no other subscription, and holds up the return of `start(..)` or `stop()` for as long as it
waits. A lease callback right after `start(..)` or `stop()` returns finds the lock free. A subscription whose lock is
taken is handed to a thread of its own. That thread waits for the lock and applies each `start(..)` and `stop()` not
yet applied to the subscription, oldest first, without letting go of the lock in between. The subscription then ends
where it would have with its lock free. That covers whether it is paused, whether a later `start(false)` keeps it
paused, whether the wrapped model runs it, whether this node holds its lease, and whether the wrapped model runs at
all. A later `start(..)` or `stop()` that finds the lock free before that thread does applies those calls first, and
then its own. A resume that such a `start(..)` asks for never lets the subscription run while the model is stopped.
Only a resume the user asks for does that.

`start(..)` and `stop()` calls are ordered by when they began. Any other call for a subscription, a grant, a try or a
resume, is ordered by when it took the subscription's lock. So a call that holds the lock when a `stop()` begins comes
before that `stop()`, and so does a `start(..)` that began before it. Once the `stop()` has begun, such a call runs
nothing in the wrapped model, also when a `start(..)` after the `stop()` has begun too, and the `stop()` then pauses
the subscription as paused by the user. That is where the subscription would be had the call run first and the
`stop()` paused it after. Letting the call run instead would start the wrapped model after a `stop()` and a
`start(false)` that leave it stopped with their lock free.

When applying a call fails, a competing subscription is tried again by the thread that tries it again after any failed
call, and any other subscription by the handed over thread, with the same backoff, until it succeeds or the model is
shut down. A later `start(..)` or `stop()` that fails to apply such a call lets that thread try it again, together with
its own call, and never counts it as applied. A pause, resume or cancel of the subscription that fails to apply such a
call gives it up instead, when the call began before the pause, resume or cancel took the lock, so the handed over
thread does not try it again. It then applies the calls after it and makes its own call, so it throws what fails in
its own call, unless the call it gave up threw an `Error`. It throws that `Error` once its own call is made, as the
MongoDB lease strategies throw an `Error` from one listener once they have told the others. In 0.33.0 a cancel of such a subscription worked, and throwing instead would refuse
every pause, resume and cancel of it for as long as applying the call went on failing. Giving up a `start(..)` gives
up only what it does for that subscription. Starting the wrapped model is a step for the whole model, which no single
subscription can give up, so a thread of its own goes on trying it, with the same backoff, until it succeeds or a
`stop()` that began after that `start(..)`, or `shutdown()`, comes. A handed over thread or a try that is still
waiting for the lock when `shutdown()` runs ends within 100 milliseconds.

A `start(..)` or `stop()` that begins while another one runs waits for it to return, and they run in the order they
began. The one exception is a `start(..)` waiting behind a `stop()` that has calls already in the wrapped model to
wait for, described below. Each `start(..)` and `stop()` takes the subscriptions the model knows under the monitor, in the same
step that makes it visible to the other calls. A `subscribe(..)` records its subscription before it releases the id,
which it does under the monitor, so that step finds each id the model knows, recorded or still being made. A
`subscribe(..)` that reserves its id after that step reads the new `start(..)` or `stop()` at its next step, under the
subscription's lock, and decides from it. Nothing delivers after a `stop()` has returned, since it stops the wrapped
model as described above, unless a `start(..)` began before it returned. A handed over subscription can stay
registered, and hold its lease, until its thread has the lock, and then unregisters. Unregistering it without the lock would let the call
holding the lock register it again right after. A `start(..)` that has returned has registered every subscription whose lock was
free, and the thread it handed each other one to registers that one once the lock is free.

`stop()` waits for every call already in the wrapped model when it began, for any subscription, for as long as that
call takes, and an interrupt does not end that wait. The thread's interrupt flag is set again once `stop()` returns.
That is the only way to keep a call it overtook from delivering after it returns, since a late resume in
`SpringMongoSubscriptionModel` starts its message listener container again. The same goes for the `subscribe(..)` in
the wrapped model that a `subscribe(..)` step makes when that model refuses `subscribePaused(..)`, since it runs the
subscription straight away. For a `DurableSubscriptionModel` over
`SpringMongoCheckpointStorage` the call includes reading the stored position, which by default retries for as long as
MongoDB cannot be reached, so the wait has no upper bound during an outage. That was already so in 0.33.0, where
`stop()` and every lease callback held the monitor, so `stop()` waited for a grant's resume just as long, and for a
registration retrying through the outage too. Now it waits only for calls in the wrapped model. A call that `stop()` refuses is refused at
once. Only a call allowed while stopped, such as a resume that takes the subscription's lock after `stop()` began,
waits until the wrapped model is stopped, and then runs. A resume that took the lock before `stop()` began ends paused
by the user, as in 0.33.0, where `stop()` waited for the monitor the resume held and then paused the subscription. `stop()` waits for those calls only while no `start(..)` is waiting behind
it. Once one is, also one that was waiting before `stop()` got to those calls, `stop()` returns without stopping
anything, since the `start(..)` then decides for every subscription. A second `stop()` waits for the first to return
instead.

`shutdown()` takes neither the monitor nor any subscription's lock. It shuts the lease strategy down, which ends a
registration waiting between two attempts and makes each later unregister a single attempt, removes the model as a
listener, and shuts the wrapped model down. It then gives up each lease once, each on a thread of its own, and waits at
most five seconds for them. It does not wait for a registration under way, and one that returns once `shutdown()` has
begun makes one attempt to give up the lease it took. A lease that is not given up by then, or whose release throws,
expires after the lease time, and another node can take the subscription over then. When the wrapped model throws from its own `shutdown()`, no lease
is given up, since that model may still deliver, and `shutdown()` throws.

The alternative is one serial executor that runs every change of state, calls the wrapped model and the lease strategy
outside it, and applies each result only when nothing changed while the call ran. I did not choose it. Every call moved
outside the serial path would need its own answer to a stop, a start, a cancel or a lease change that came while it
ran, for the lease callbacks and the lifecycle calls as much as for `subscribe(..)`. Keeping every call for a
subscription under that subscription's lock means that `stop()`, `start(..)`, a pause, a resume, a lease callback and a
try never overlap for one subscription, and a call for one subscription never waits for a call for another. The steps
that go outside the lock are the two steps of `subscribe(..)` that can take as long as an outage. One rule answers for
them and for the tries, which is to decide again from what holds now.

**The blocking catch-up model keeps a replay it cannot run, instead of ending it.** That covers a replay subscribed
while the model is stopped and one that `stop()` cuts short. The model keeps the replay together with the handle its
subscriber already holds, and counts the subscription as paused. An event whose action completed before the stop may
still have its position stored, and nothing past it is. `start(true)` runs every kept replay again from the last
position it stored, or from where it started when it stored none, and `resumeSubscription(id)` runs that one, first starting the model without
resuming anything else when it is stopped. `start(false)` does not run them, the same way the live model it wraps
does not resume its paused subscriptions on `start(false)`. Cancelling the subscription or shutting the model down
drops the replay, and `waitUntilStarted()` on its handle then returns `false`. Neither stores the present as the
position of a replay it cut short, since that position lies past the history the replay has not read.

## Consequences

A replay that `stop()` cut short runs again from the last position it stored, so the events it delivered after that
position are delivered again, and the stored position never moves back. For a time position the replay also delivers
the events stored at that exact time again, since other events can have the same time down to the millisecond.

The blocking catch-up model runs a kept replay again only on `start(true)` or a resume, where the reactor catch-up
models in ADR 98 run it on any `start(..)`.

A subscription made on a stopped `CompetingConsumerSubscriptionModel` over a bare `SpringMongoSubscriptionModel`, with
`StartAt.now()` or the model default, starts at the operation time MongoDB answers with when `subscribe(..)` asks
([ADR 142](0142-a-quiet-mongodb-subscription-moves-its-position-from-the-empty-batch.md)). An event written after
MongoDB has answered is delivered to it once the node wins the lease, as long as the oplog still holds that time.

A subscription model of your own that you wrap in a `CompetingConsumerSubscriptionModel` has to hold a subscription
made while it is stopped paused. One that delivers it straight away delivers it on a node that holds no lease for it.
Unless it implements `subscribePaused(..)`, a subscription made while the competing consumer model is stopped reaches
your model as soon as the node wins its lease, and when your model runs, it delivers events before `start()`. A model
whose subscription still runs after `pauseSubscription(..)` has returned makes `stop()` throw, and the node keeps that
subscription's lease.

A wrapped model that refuses `subscribePaused(..)` and loses the lease while it makes a subscription in `subscribe(..)`
delivers that subscription until `subscribe(..)` takes the subscription's lock again and pauses it.

`start(..)` and `resumeSubscription(..)` return when the lease strategy or the wrapped model throws for a competing
subscription, and so does `subscribe(..)` once the wrapped model has made the subscription. 0.33.0 threw. The failure
shows up as a warning in the log, and a caller that caught the exception to call again no longer needs to. A caller
that needs to know whether the subscription runs reads `isRunning(id)` and `isPaused(id)`.

A subscription that a call keeps failing for holds a thread of its own, until a try reaches the end state or the model
is shut down. It logs a warning on every fifth try that fails, which is every ten seconds once the backoff has reached
two seconds.

A `start(..)` or `stop()` runs a thread for each subscription the model knows until it returns, and does not pool
them, since a pool would queue the other subscriptions behind one whose call waits through an outage. The wrapped
MongoDB models already ask for a thread for each subscription they run.

`shutdown()` returns within about five seconds of shutting the wrapped model down, also during a MongoDB outage. In
0.33.0 it waited for the monitor, which a registration retrying through the outage held, and then gave up the leases
one after another, so the first release that threw left the rest held and the model still registered as a listener.

A call that waits for MongoDB through an outage holds up the calls for its own subscription and no call for any other,
except while a `stop()` or `shutdown()` waits for it. It still holds up the return of a `start(..)` or `stop()` that
applies itself to that subscription, and a `stop()` also waits for a call already in the wrapped model for any
subscription, as it did in 0.33.0 for every call under the monitor. While a `stop()` waits for such a call, a call for
another subscription that may run while the model is stopped, such as a resume the user asks for, waits too. While
`shutdown()` waits for one, a loss of the lease of another subscription is not acted on, and that subscription goes on
delivering until `shutdown()` has shut the wrapped model down. A subscription that `start(..)` or `stop()` handed to a thread of its own gets the call only once the lock is
free, and a failure there is tried again until it succeeds. After a `stop()` it can hold its lease until then, so no
other node can take the subscription over meanwhile, although this node delivers nothing for it.
