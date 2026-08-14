# 86. A manual subscription is registered, not started

Date: 2026-08-02

## Status

Accepted. This is the second slice of #481, after ADR 85. The OpenRewrite recipe for the renamed property
and the reactive stack follow separately.

## Context

ADR 82 shipped deny-by-default subscription testing, and ADR 85 gave the last four models a life cycle so
that stopping one really does stop everything. What neither could reach is the cost of starting in the
first place. A JUnit extension runs after the context is refreshed, by which time the annotation bean post
processors have registered and started every subscription, so a test still pays for a change stream it
immediately closes. #481 asked for a way to prevent that, and it is a production capability rather than a
test trick: an application bringing subscriptions up behind a leader election wants exactly the same thing.

The obvious implementation is to stop the model once the context is up. It was built that way first, and
it does not hold. A subscription resuming from a stored checkpoint does not go straight to live delivery,
it goes through the catch-up model, which replays out of the event store on a background thread rather
than reading the change stream. So events reached handlers while the model reported the subscription
paused. It only showed up across a restart, which is to say on the second run of any test suite.

Setting the catch-up model's stopped flag before registration is worse than the bug. `shouldKeepReplaying`
then returns false on entry, and the subscription comes back as a `CancelledSubscription` that was never
registered with the delegate. It is lost rather than paused, and nothing can resume it.

## Decision

**Stopping is the wrong verb.** A stopped model has already been handed every subscription, so any layer
that reads history rather than a live feed can still deliver. Withholding has to happen before the model
is told anything at all.

**`ManualStartSubscriptionModel` wraps the model and defers the whole `subscribe` call.** It records the
subscription id, filter, start position and handler, hands back a placeholder, and passes nothing down.
`resumeSubscription(id)` performs the real subscribe. Until then no competing consumer lock is taken, no
history is replayed and no change stream is opened, which is the startup cost #481 was about.

**The reactive stack already worked this way, by accident of layer order.** `ReactorMongoSubscriptionModel`
and `ReactorDurableSubscriptionModel` both decline to subscribe while stopped, and the reactive starter
happens to put the durable model outermost, above catch-up. The blocking starter stacks the same layers in
the opposite order and its durable model is a pure delegate. The wrapper gives the blocking stack the
property the reactive one gets for free, without reordering anything.

**It lives in `occurrent-subscription-api-blocking` and knows nothing about Spring.** That module already
holds concrete wrappers with no dependencies of their own, and this one needs none either, so it costs no
new Maven coordinate. The Boot property is wiring, which is how the rest of Occurrent is built.

**A first run starts from where it was registered, not from where it was started.** This is the part worth
reading twice. `DurableSubscriptionModel` pins a subscription's resume position the first time its start
position is resolved, and `SpringMongoSubscriptionModel` forces that resolution inside `subscribe`. Under
`auto` that happens at boot. Deferring `subscribe` would move it to resume, so a subscription running for
the very first time would silently skip everything written while it waited. So the wrapper captures the
position when the subscription is registered and writes it when the subscription starts, if nothing is
stored yet. Waiting withholds events rather than losing them, which is the only promise worth making.

A subscription that has run before is unaffected either way: it resumes from its own checkpoint. And the
capture is optional, because it needs a checkpoint storage and something able to report a position. Given
neither, the wrapper still works and a first run starts from the moment it is started.

> **Amended for 0.33.0.** "Writes it when the subscription starts" described where the implementation put
> the write, not what it should have been. `DurableSubscriptionModel` pins the position the moment its own
> `subscribe` resolves it, forced eagerly by `SpringMongoSubscriptionModel`. That is what would have happened
> here too, immediately at registration, had this wrapper never deferred `subscribe` in the first place.
> Writing the pin later, at start, left a gap. Two nodes registering minutes apart during a rolling deploy
> could see whichever started first win the pin, which need not be whichever registered first, silently
> skipping events between the two registrations (see #669 and ADR 116's amendments). The wrapper now writes
> at registration, which is not a safer ordering chosen on its own terms. It is what makes this wrapper a
> faithful stand-in for the `subscribe` call it withholds, matching what this section's opening sentence
> already promised. Two things bound that promise. It covers a subscription registered with the default start
> position, since that is the one a wrapped model reads a stored checkpoint for, and registering with an explicit
> `StartAt` writes the position without the wrapped model ever reading it. And it does not cover two nodes
> registering the same subscription for the first time, where only one of the two positions can be stored and
> neither node can tell which of them is earlier, whether the two register together or one of them is delayed
> between capturing its position and writing it. ADR 116's third amendment states what happens then, and #771
> tracks the question of closing it.

> **Amended again, before 0.33.0 shipped.** The amendment above says the write covers a subscription registered with
> the default start position, and then says a registration with an explicit `StartAt` writes the position anyway. Those
> two do not fit together, and the second one was what the code did. A checkpoint written for a registration nothing
> reads a checkpoint for is not harmless. `StartPosition.BEGINNING` under the default resume behaviour asks to replay on
> a first run and to resume afterwards, and it decides which of the two by asking whether this same storage holds a
> checkpoint, so a position written at registration answered that question with a resume and the replay the caller asked
> for never happened. The write now happens only when the caller's start position resolves to the subscription model
> default, through whatever a dynamic position stands for, since the annotation default is one of those. That
> resolution comes before the existence read and before the position is captured, which is what leaves a first-run
> question with nothing of this registration's own to find. A dynamic function therefore runs a few more times than it
> used to, which `StartAt.dynamic` already allows for, and the wrapped model still receives the caller's own
> `StartAt` object.
>
> **A dynamic position is resolved layer by layer, not once against the model this wrapper was handed.** Each layer is
> asked for its own answer, a layer answering with nothing leaves the subscription to the model it wraps and the model
> below is asked next, and the first answer that is not nothing decides. That is what the wrapped models do to the same
> position when the subscription starts, and reproducing it is the only way this wrapper can tell whether a checkpoint
> will be read at all. One ask would have been wrong for the start position the annotations build, which answers with
> nothing for a catch-up layer and with the model default for everything else. The Spring Boot starter's own stack
> answers on the first ask, since it always puts a competing consumer layer on top, but its subscription model bean is
> `@ConditionalOnMissingBean` and a stack wired by hand can put the catch-up layer outermost. There, one ask would
> record nothing and leave the durable model to record a position when the subscription starts, which is the skip #669
> was about.
>
> When the walk ends with nothing to record, every layer is asked again under each class it inherits from, and the
> model default from any of those answers records the position. A model resolves the position against a class literal
> of its own, so a subclass of it, including a proxy built by subclassing it, is asked here under a name
> `hasSubscriptionModelType` does not match, since that method compares for equality. A proxy that only implements the
> model's interfaces is out of reach either way, since its class inherits from `java.lang.reflect.Proxy` and never
> names the model at all. Without that second pass, a
> function written with that method records nothing, whichever way round it is written, and leaves the durable model to
> record a position at start, which is #669 again and a regression from the write this replaces. The second pass runs
> only where nothing would be recorded otherwise, and recording there can only add a write the unconditional one it
> replaces already made.
>
> Any inherited class answering with the model default is enough, rather than the nearest one deciding as a layer's own
> answer does. A model's class literal can be any of the classes its runtime class inherits from, and this cannot tell
> which, so both rules are wrong for some function written against a subclassed model. The one that records is wrong by
> writing a position that is read only where the model default is what gets resolved, and the one that stops at the
> nearest answer is wrong by leaving a subscription to record its position at start, which is the skip this exists to
> prevent.
>
> It asks classes rather than layers, so it does not resume the walk. A subclassed wrapper answering with a position of
> its own, where the class it inherits from would have handed the question down, ends the walk there and the model
> below is never asked. Closing that needs each layer to say which class it resolves against, which belongs on the
> subscription model interfaces rather than in this wrapper, and reaching it takes a caller who both subclasses a
> wrapper layer and writes a function answering three different ways down one stack.
>
> The walk asks every layer, including one that passes the position down without deciding anything for itself, so a
> function answering with the model default for such a layer and with something else for the model that does read the
> checkpoint is read as a position to record when it is not one. Telling the two kinds of layer apart needs the layers
> to say which of them consumes a start position, which is a capability `SubscriptionModel` does not have, and no start
> position Occurrent builds answers that way, since they either ignore the model type or branch on the catch-up,
> competing consumer and durable layers, all three of which do decide for themselves. A position recorded off the back
> of an answer no layer consumes is not harmless either, since a function that reads this storage to decide between
> replaying and resuming then finds it and resumes. That is the outcome the unconditional write this replaces produced
> for every registration, so such a function is left where it already was rather than made worse, and every function
> Occurrent builds is moved out of it.
>
> The three-argument factory also refuses a `CheckpointStorage` that answers false to `evaluatesWriteConditions()`. The
> position is written with `ifAbsent()`, so this wrapper needs a storage that evaluates that condition, and that method
> is the only thing it has to ask. Refusing at wiring time is cheaper than finding out later, either on the first
> registration or from a subscription that resumed from a position it should never have started at. The one-argument
> factory keeps such a storage, at the cost of a first run starting from the moment it is started. A caller that passed
> a storage of its own in 0.32.0 is refused until it answers true, which the changelog records as a breaking change.
>
> That question is coarser than this write needs. `evaluatesWriteConditions()` answers for `notOlderThan` and
> `ifAbsent` together, so a storage that evaluates `ifAbsent` and refuses `notOlderThan` has to answer false and is
> refused here even though the write it would be asked for is one it can do. Asking per condition means another method
> on `CheckpointStorage`, which every implementation outside this repository would then have to answer, and that is a
> decision for the interface rather than for this wrapper. The refusal names the method it asked and what this model
> needs, so a caller in that position can see why.

> **Amended a third time, before 0.33.0 shipped.** The amendment above says no start position Occurrent builds
> records a position off an answer no layer consumes, "since they either ignore the model type or branch on the
> catch-up, competing consumer and durable layers, all three of which do decide for themselves". That is wrong about
> the competing consumer layer. `CompetingConsumerSubscriptionModel` resolves the position to find out whether to
> compete for the subscription, and then hands the caller's own `StartAt` to the model it wraps in both branches, so
> the model below resolves the same position again and is the one that settles where the subscription starts. The
> walk read that layer's answer as final and stopped there. A function answering for each layer separately, with the
> model default for the durable layer and something else above it, therefore recorded nothing, and the durable model
> recorded a position when the subscription started instead. That is the skip this section's decision exists to
> prevent, on the Spring Boot starter's own stack, and a regression from the unconditional write this replaces, which
> covered that input. It is not #669, which is two nodes racing a read against a write for a checkpoint neither finds
> yet. Every start position Occurrent itself builds is safe, since those answer with nothing or with the model default
> for the competing consumer layer, so reaching this takes a function written by hand.
>
> **A layer now says whether its own answer decides where the subscription starts**, with
> `SubscriptionModelWrapper.decidesWhereTheSubscriptionStarts()`. It answers true by default,
> `CompetingConsumerSubscriptionModel` and `ManualStartSubscriptionModel` answer false, and the walk passes over a
> layer that answers false rather than asking it at all. That closes both directions the amendment above described for
> a layer that says so. Such a layer no longer ends the walk with an answer it does not act on, and no position is
> recorded off its answer either, since it is not asked for one.
>
> Two shapes are left rather than none, and they fail in opposite directions. A wrapper written outside this
> repository whose own answer is not what decides the start, whether it resolves the position for a decision of its
> own or hands it down without resolving it at all, and that does not say so, is read the way each layer was read
> before, so it ends the walk and leaves the model below to record a position when the subscription starts. And the walk asks a
> layer under its runtime class while a model resolves the position under a class literal of its own, so a function
> answering the model default for a named subclass and something else for the class that subclass extends has a
> position recorded from an answer the model below never acts on. That second one is the cost the amendment above
> already names for a position recorded off an answer no layer consumes, reached by splitting the answer across two
> class names rather than across two layers.
>
> The question is deliberately not whether a layer hands the caller's own `StartAt` object down, which was the first
> wording and is wrong for two of the layers here. `CatchupSubscriptionModel` hands the object to its children, and
> those children resolve it under `CatchupSubscriptionModel.class` rather than their own, so the answer given for the
> catch-up layer is the one acted on and that layer answers true. `DurableSubscriptionModel` hands the object on too,
> on the branch where its own answer was nothing, which is the branch the walk already covers by descending. Both
> would have answered true under the object wording and been skipped, and a registration under either would then have
> had no position recorded. A wrapper written outside this repository could have read the same wording the same way,
> which is why the method names the decision rather than the object.
>
> Naming `CompetingConsumerSubscriptionModel` in the walk was the other way to fix it, and the module dependencies
> rule it out, since the competing consumer model depends on the subscription api module the wrapper lives in.
> Documenting the bound rather than fixing it was rejected by the same no-regression rule that justified the second
> pass over inherited classes.
>
> Two things the amendment above says about that second pass are narrower than they sound. Every layer is not asked
> again, only the ones the walk actually reached, which is a single layer when the walk ended on its first ask. And
> the classes a layer inherits from stop short of `Object`, which no subscription model resolves its position
> against. A proxy that only implements the model's interfaces is asked here after all, under
> `java.lang.reflect.Proxy`, and a wrapper written as a record under `java.lang.Record`. The default start position
> the annotations build does answer for both of those names, since it answers with the model default for any class it
> is asked about that is not a catch-up one. "Out of reach either way" still holds for what a subscription gets, for a
> different reason than the one first written here. Under that position the first layer below the catch-up one
> answers the model default on its own ask, so the walk records the position there and this second pass does not run.

**`isPaused(id)` is true for a subscription that is registered and not started.** It is the question a
caller is really asking, and `OccurrentSubscriptionsExtension.startAll()` filters on it. For the same
reason the wrapper reports its own ids from `subscriptionIds()`, merged with the delegate's, since
`IntrospectableSubscriptionModel.of` stops at the first model that implements it.

**`isRunning()` is false while withholding, even though the listener container and the lease refresh thread
below it are running.** It answers whether this model is delivering, not whether its resources exist.

**Registering the same id twice fails here.** Nothing else catches two `@Subscription`s sharing an id
except the innermost Mongo model, which under this mode is never reached. Without the check a duplicate
would quietly replace the first handler and the application would boot green with a subscription missing.

**Starting a subscription resumes it if the delegate parked it.** Subscribing to a stopped model registers
a paused subscription rather than a running one, so a plain subscribe would hand back something that never
delivers. This is what makes stop-then-start work, which is exactly what the JUnit extension does between
tests.

**The three paths that never went through the subscription model are closed too.** A saga's timer poller
could dispatch commands and write events on its first tick regardless of whether the saga's subscription
was running, and two projection paths replayed history during registration. They were already doing this
before this change, so `manual` merely made them visible.

## Consequences

Boot no longer validates subscription wiring. A bad filter or an unsupported start position used to fail
during context refresh and now fails at the first resume, which for a leader election means in production,
some time after deploy. The duplicate id check and the eager argument checks recover the cheapest of those.
The rest cannot be recovered without subscribing, which is the thing being avoided.

Nothing below the wrapper knows a withheld subscription exists. No lease is held for it, so an operator
looking at who is consuming what sees nothing until the subscription is started. In a mixed deployment
that is an improvement, since a manual node no longer takes a lock it then refuses to act on.

State has two authorities, split by id: the wrapper answers for a withheld subscription and the delegate
for a started one. That is safe only because a started subscription is pure pass-through. The delegate
changes state on its own when a lease is lost or change stream history is lost, so caching an answer here
would go stale.

The bean's runtime type differs by mode, so code reaching for the concrete
`CompetingConsumerSubscriptionModel` works under `auto` and fails under `manual`. The starter's bean is
also `@ConditionalOnMissingBean`, so an application defining its own subscription model does not get this
and `manual` silently does not apply to it.

This does not pay the underlying debt. `CatchupSubscriptionModel` still cannot pause, resume or list a
subscription whose replay is in flight, and its dispatcher still does not fan those calls out to the
children holding the state. `manual` routes around that rather than fixing it, and the gap is live again
for exactly the subscriptions the application has just started. The subscription conformance suite in #395
should pin it.

`manual` is blocking only until the reactive starter gets the same wiring.

**Amended for 0.33.0: `IntrospectableSubscriptionModel` is renamed to `IntrospectableSubscriptions`.** The
capability is unchanged, only the name moved, since the interface never extended `SubscriptionModel`.
