/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpointSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a subscription model so that subscribing only registers a subscription, and nothing starts until the
 * application asks for it with {@link #resumeSubscription(String)} or {@link #start(boolean)}. Use it to bring
 * subscriptions up behind a leader election or a health check, or in a test that chooses which subscriptions run.
 * <p>
 * The difference from stopping a model is where the withholding happens. A stopped model has already been handed every
 * subscription, so a layer that reads history rather than a live feed, such as a catch-up model, can still deliver
 * events. This model hands the wrapped one nothing at all, so no lock is taken, no history is replayed and no feed is
 * opened until a subscription is started.
 * <p>
 * <b>Where a subscription starts from.</b> A subscription that has run before resumes from its stored checkpoint and
 * loses nothing. One that has never run has no checkpoint to resume from, and would otherwise start wherever the
 * wrapped model's default points at the moment it is started, silently skipping everything written since. Give this
 * model a position source and a checkpoint storage and it saves that position as the checkpoint when the subscription
 * is registered, instead of waiting until it starts. That way, starting a subscription late still delivers the events
 * written since registration instead of skipping them. Only a registration that asks for the default start position is
 * written, since that is the one a wrapped model reads a stored checkpoint for, so a registration naming a position of
 * its own still starts where it asked to. Two nodes registering the same subscription for the first time can still
 * race for the position that gets stored. A storage able to compare the two, such as the MongoDB storages this
 * library ships, resolves that race by which position is earlier rather than by which write reached storage first,
 * so both nodes end up agreeing on the earlier of the two and nothing is lost between them. A storage unable to
 * compare falls back to the older, narrower rule: a stored position already there when a registration checked for
 * one is taken without comparison, and one that only appeared afterwards is refused with
 * {@link StartPositionAlreadyPinnedException} unless it turns out to be the very position that registration read.
 * That refusal also covers a stored position this class cannot read back to compare, whether reading it failed or
 * found nothing. A single node reaches those last two on its own when its storage answers from somewhere that has not
 * seen the write. A position source that answers nothing refuses the registration too, with an
 * {@link IllegalStateException}, since there is then no position to hold it to. Without a position source and a
 * checkpoint storage, a first run starts from the moment it is started. See ADR 130.
 *
 * @see #stoppedByDefault(SubscriptionModel)
 */
@NullMarked
public final class ManualStartSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, IntrospectableSubscriptions {

    private final SubscriptionModel delegate;
    private final @Nullable GlobalCheckpointSource<@Nullable Checkpoint> positionSource;
    private final @Nullable CheckpointStorage checkpointStorage;

    private final ConcurrentMap<String, Registration> registrations = new ConcurrentHashMap<>();
    // Registration order, so start(true) brings subscriptions up in the order they were declared rather than in
    // whatever order the map happens to iterate.
    private final List<String> registrationOrder = new CopyOnWriteArrayList<>();

    // NOT_STARTED and STOPPED both withhold a new registration, but only STOPPED is undone by resuming a
    // subscription. A model that has never been started keeps withholding the rest, which is what this class is for.
    private enum State {NOT_STARTED, RUNNING, STOPPED}

    private volatile State state = State.NOT_STARTED;
    private volatile boolean shutdown = false;

    // Orders subscribe's choice between withholding and passing through against the state transitions. Without it,
    // a registration can be stored as deferred just after start(true) walked past its claimed id, and then nothing
    // ever starts it.
    private final Object stateLock = new Object();

    private ManualStartSubscriptionModel(SubscriptionModel delegate, @Nullable GlobalCheckpointSource<@Nullable Checkpoint> positionSource,
                                         @Nullable CheckpointStorage checkpointStorage) {
        this.delegate = requireNonNull(delegate, SubscriptionModel.class.getSimpleName() + " cannot be null");
        this.positionSource = positionSource;
        this.checkpointStorage = checkpointStorage;
    }

    /**
     * A model that registers subscriptions without starting them. A subscription running for the first time will start
     * from wherever {@code delegate} starts by default at the moment it is started, so events written between
     * registration and that moment do not reach it. Use
     * {@link #stoppedByDefault(SubscriptionModel, GlobalCheckpointSource, CheckpointStorage)} to record the
     * position at registration instead.
     *
     * @param delegate The subscription model to register with once a subscription is started.
     */
    public static ManualStartSubscriptionModel stoppedByDefault(SubscriptionModel delegate) {
        return new ManualStartSubscriptionModel(delegate, null, null);
    }

    /**
     * A model that registers subscriptions without starting them, and records where a subscription running for the
     * first time will start from, so that starting it later still delivers the events written since registration
     * instead of skipping them.
     * <p>
     * The recorded position is pinned with {@link CheckpointWriteCondition#ifAbsent() ifAbsent()} at registration,
     * which only ever writes a subscription's very first checkpoint against nothing stored. A fence condition has
     * nothing to add to a write like that, so this factory takes no {@link CheckpointWriteVersionSource}
     * (see ADR 116).
     * <p>
     * Two nodes registering the same subscription id, minutes apart during a rolling deploy or close enough together
     * to overlap, both attempt this write, and only the first to reach storage succeeds. The node that loses asks
     * {@link CheckpointStorage#resolveFirstCheckpointRace(String, Checkpoint) checkpointStorage.resolveFirstCheckpointRace}
     * to settle the race by position instead, which a storage able to compare the two, such as the MongoDB storages
     * this library ships, does atomically with any write it calls for. Both nodes end up agreeing on whichever
     * position is earlier, so nothing between them is skipped and neither node sees an exception. A storage unable
     * to make that comparison, or a candidate {@code resolveFirstCheckpointRace} cannot make sense of, answers
     * empty, and this factory falls back to the narrower rule 0.33.0 shipped. A node that already finds a checkpoint
     * stored when it registers accepts it there, which costs it nothing as long as {@code positionSource} hands out
     * positions in the order it is asked for them, as the MongoDB subscription models do. A node whose write is
     * refused by a checkpoint that arrived only after it read whether one existed is refused in turn, with
     * {@link StartPositionAlreadyPinnedException}, unless reading that checkpoint back shows it holds the very
     * position this node read. Two positions read on different nodes cannot be ordered by this fallback, since
     * {@link Checkpoint} gives it no way to tell which is earlier, so accepting one would risk starting the
     * subscription past the events written between them and saying nothing about it. A checkpoint this fallback
     * cannot read back to compare, because reading it failed or found nothing, is refused for the weaker reason that
     * nothing here can show the two agree. One node reaches those two on its own, when its storage answers from
     * somewhere that has not seen the write, or retries a write whose answer it never heard. A refusal costs a
     * registration that fails and can be made again, and a node that registers again once a position is stored
     * takes it and starts. See ADR 130.
     * <p>
     * The same refusal applies to a registration this model hands straight to the wrapped model because it is
     * already running, so such a registration is dropped rather than started. That is the same answer as for a
     * registration this model withholds, which is what keeps the promise this factory makes independent of when
     * the model happened to be started.
     * <p>
     * A {@code positionSource} that answers {@code null} refuses the registration too, with an
     * {@link IllegalStateException}. That answer is how a source reports a problem it cannot resolve, so there is no
     * position to record and nothing to hold the registration to, and letting it through would start the subscription
     * from wherever the feed has reached once it is started rather than from where it was registered. The id is left
     * free, so registering again once the source can answer is what a node does. A subscription that already has a
     * checkpoint stored is not refused, since that checkpoint is where it starts and nothing would have been recorded
     * over it anyway. Use {@link #stoppedByDefault(SubscriptionModel)}, or a {@link StartAt} of your own, to register
     * without recording a position at all, neither of which carries the guarantee this factory makes.
     * <p>
     * The position is recorded only for a registration that asks for {@link StartAt#subscriptionModelDefault()},
     * since that is the one a wrapped model reads a stored checkpoint for. Registering with a position of your own,
     * or with {@link StartAt#now()}, writes nothing, so a replay you asked for stays a replay instead of becoming a
     * resume. A {@link StartAt#dynamic(java.util.function.Supplier) dynamic} start position is resolved at
     * registration to find out which of the two it is, layer by layer down the wrapped models, following what those
     * models do to the same position when the subscription starts. A layer answering with {@code null} leaves the
     * subscription to the model it wraps, so the next model down is asked, and the first answer that is not
     * {@code null} decides. A layer that answers
     * {@link SubscriptionModelWrapper#decidesWhereTheSubscriptionStarts()} with {@code false} is passed over, since
     * what the position resolves to under that layer's class decides something other than where the subscription
     * starts, and the model below it settles that instead. All of that happens before anything is read or written,
     * so a function that answers the first-run question by looking for a stored checkpoint sees what was stored
     * before this registration. The wrapped model still receives the {@code StartAt} the caller passed, whatever
     * those resolutions answered.
     * <p>
     * What decides this is the answer that position gives at registration. A dynamic one is allowed to answer
     * differently over the life of a subscription model, and a function that does so between registration and
     * start has a position recorded for a start that then reads none, or none recorded for a start that then
     * reads one. Answering the first-run question from the checkpoint storage is the shape
     * {@link StartAt#dynamic(java.util.function.Supplier)} names, and that one answers the same way here as it
     * does at start, unless the recorded position is itself what changes its answer.
     * <p>
     * When that walk ends with no position to record, each layer it asked is asked again under each class that layer
     * inherits from, stopping before {@link Object}, since a model resolves the position against a class literal of
     * its own and a subclass of it, including a proxy built by subclassing, is otherwise asked under a name
     * {@link StartAt.SubscriptionModelContext#hasSubscriptionModelType(Class)} does not match. A walk that ended on
     * its first ask has that one layer to ask again. The model default from any of those answers records the
     * position. A dynamic function therefore runs at most once for each layer the walk asked, plus at most once for
     * each class those layers inherit from when that second pass runs, on top of the calls it already gets when the
     * subscription starts. Two shapes are
     * still read differently here than the model that starts the subscription reads it. A layer that passes the
     * position down without deciding where the subscription starts and does not say so is asked all the same, and a
     * proxy that only implements a model's interfaces never shows that model's own class here, so a function naming
     * it exactly is not recognised through one. ADR 86 has what each costs a subscription.
     *
     * @param delegate          The subscription model to register with once a subscription is started.
     * @param positionSource    Supplies the position to record. Typically the innermost model, the one reading the feed.
     *                          Only {@link GlobalCheckpointSource#globalCheckpoint()} is called, so a caller can pass
     *                          anything that exposes it, such as a {@link CheckpointAwareSubscriptionModel}, without
     *                          this method demanding the full subscription model that happens to implement it.
     * @param checkpointStorage Where the recorded position is written, which must be the storage the wrapped models read.
     * @throws IllegalArgumentException If {@code checkpointStorage} does not
     *                                  {@link CheckpointStorage#evaluatesWriteConditions() evaluate write conditions}.
     *                                  The position recorded at registration is written with
     *                                  {@link CheckpointWriteCondition#ifAbsent() ifAbsent()}, so this model needs a
     *                                  storage that evaluates that condition.
     */
    public static ManualStartSubscriptionModel stoppedByDefault(SubscriptionModel delegate, GlobalCheckpointSource<@Nullable Checkpoint> positionSource,
                                                                CheckpointStorage checkpointStorage) {
        requireNonNull(positionSource, GlobalCheckpointSource.class.getSimpleName() + " cannot be null");
        requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        if (!checkpointStorage.evaluatesWriteConditions()) {
            throw new IllegalArgumentException("The start position recorded at registration is written with ifAbsent(), " +
                                               "so this model needs a checkpoint storage that evaluates that condition, " +
                                               "and " + checkpointStorage.getClass().getName() + " answers false to " +
                                               "evaluatesWriteConditions(). Use a storage that evaluates it, or " +
                                               ManualStartSubscriptionModel.class.getSimpleName() +
                                               ".stoppedByDefault(SubscriptionModel), which records no position at all.");
        }
        return new ManualStartSubscriptionModel(delegate, positionSource, checkpointStorage);
    }

    /**
     * Register a subscription. While this model is withholding, nothing is passed to the wrapped model and the returned
     * {@link SubscriptionHandle} is a placeholder standing for the registration. Its {@code waitUntilStarted} answers
     * {@code false} straight away, since the subscription has not started and will not until you ask. Once the
     * subscription is started, {@link #resumeSubscription(String)} returns the wrapped model's own subscription, which
     * is the handle to wait on.
     *
     * @throws DuplicateSubscriptionIdException          If {@code subscriptionId} is already registered.
     * @throws StartPositionAlreadyPinnedException       If this model records start positions, a position was
     *                                                   stored for this subscription id while the registration was
     *                                                   under way, and it could not be confirmed to be the one this
     *                                                   registration read, whether it differed, read back as
     *                                                   nothing, or could not be read. The id is left free, so
     *                                                   registering it again is what a node does once a position
     *                                                   is stored. Withheld and passed-through registrations
     *                                                   answer the same way.
     * @throws IllegalStateException                     If this model records start positions and the position source
     *                                                   answered nothing, which is how it reports a problem it cannot
     *                                                   resolve. The id is left free, so registering it again once
     *                                                   the source can answer is what a node does.
     */
    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        requireNonNull(action, "Action cannot be null");
        if (shutdown) {
            throw new IllegalStateException(ManualStartSubscriptionModel.class.getSimpleName() + " is shutdown");
        }

        claim(subscriptionId);
        try {
            // Pinned before taking the lock, so a slow position source or storage write cannot block start(boolean),
            // and before the registration becomes visible to it, so a resumeSubscription racing in right after
            // cannot get there before the pin does and let the wrapped model capture its own, later position
            // instead. That ordering requirement holds regardless of whether this registration ends up deferred or
            // live, which is why the pin happens here rather than only on the deferred path.
            pinStartPosition(subscriptionId, startAt);
            synchronized (stateLock) {
                if (state != State.RUNNING) {
                    registrations.put(subscriptionId, new Registration.Deferred(filter, startAt, action));
                    return new DeferredSubscription(subscriptionId);
                }
            }
            SubscriptionHandle subscription = delegate.subscribe(subscriptionId, filter, startAt, action);
            registrations.put(subscriptionId, new Registration.Live(subscription));
            return subscription;
        } catch (RuntimeException e) {
            forget(subscriptionId);
            throw e;
        }
    }

    /**
     * Start a registered subscription, or resume one the wrapped model has paused. Returns the wrapped model's
     * subscription, so waiting on it waits for the real thing.
     * <p>
     * After {@link #stop()}, resuming one subscription also makes {@link #isRunning()} report {@code true} again, and
     * a subscription registered after that is started rather than withheld. Every other subscription {@code stop()}
     * paused stays paused until it too is resumed. On a model that has never been started, resuming one subscription
     * starts only that one and the rest keep waiting.
     *
     * @throws UnknownSubscriptionException       If neither this model nor the wrapped model has that subscription.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running, including when another
     *                                             thread started this registration first.
     */
    @Override
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Registration registration = registrations.get(subscriptionId);
        // Another thread is between claiming this registration and subscribing it, so the wrapped model does not have
        // the id yet and would answer that it knows nothing about it. Answer for the registration this model holds.
        if (registration instanceof Registration.Starting) {
            throw new SubscriptionAlreadyRunningException(subscriptionId,
                    "Subscription " + subscriptionId + " is already being started by another thread.");
        }
        if (!(registration instanceof Registration.Deferred deferred)) {
            SubscriptionHandle subscription = delegate.resumeSubscription(subscriptionId);
            reopenAfterStop();
            return subscription;
        }
        // Claim it before touching the wrapped model, so two threads starting the same subscription cannot both subscribe.
        if (!registrations.replace(subscriptionId, deferred, Registration.STARTING)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId,
                    "Subscription " + subscriptionId + " is already being started by another thread.");
        }

        try {
            SubscriptionHandle subscription = delegate.subscribe(subscriptionId, deferred.filter(), deferred.startAt(), deferred.action());
            // Subscribing to a stopped model registers a paused subscription rather than a running one, so this is what
            // makes starting work after stop(). Without it the caller is handed a subscription that never delivers.
            if (delegate.isPaused(subscriptionId)) {
                subscription = delegate.resumeSubscription(subscriptionId);
            }
            registrations.put(subscriptionId, new Registration.Live(subscription));
            reopenAfterStop();
            return subscription;
        } catch (RuntimeException e) {
            // Put it back so a subscription that failed to start can be started again.
            registrations.put(subscriptionId, deferred);
            throw e;
        }
    }

    /**
     * @see SubscriptionModelLifeCycle#pauseSubscription(String)
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (isWithheld(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId,
                    "Subscription " + subscriptionId + " is not running, it is registered but has not been started.");
        }
        delegate.pauseSubscription(subscriptionId);
    }

    /**
     * @see CancellableSubscriptions#cancelSubscription(String)
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        forget(subscriptionId);
        delegate.cancelSubscription(subscriptionId);
    }

    /**
     * Stop the wrapped model, and withhold again, so a subscription registered after this point also waits to be
     * started, until {@link #start(boolean)} or {@link #resumeSubscription(String)} is called.
     */
    @Override
    public void stop() {
        synchronized (stateLock) {
            state = State.STOPPED;
        }
        delegate.stop();
    }

    /**
     * @see SubscriptionModelLifeCycle#start(boolean)
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        // Unconditional, so a subscription the wrapped model has paused is resumed even when that model is already
        // running. Guarding on isRunning() was only ever working around a model that refused a second start.
        delegate.start(resumeSubscriptionsAutomatically);
        synchronized (stateLock) {
            state = State.RUNNING;
        }
        if (resumeSubscriptionsAutomatically) {
            // Nothing new can be stored as deferred once the transition above is taken, so this walk sees every
            // deferred registration, and a registration racing this method starts itself when it sees the running
            // state. Fails on the first subscription that cannot start, leaving the rest registered. A partially
            // started model is the honest outcome, and swallowing the failure to start the others would hide a
            // broken subscription.
            for (String subscriptionId : registrationOrder) {
                if (registrations.get(subscriptionId) instanceof Registration.Deferred) {
                    resumeSubscription(subscriptionId);
                }
            }
        }
    }

    /**
     * @return {@code false} until this model is started, even though the wrapped model may be running, because nothing
     * of its own has been handed to it yet, and {@code false} again after {@link #stop()} until {@link #start(boolean)}
     * or {@link #resumeSubscription(String)} is called.
     */
    @Override
    public boolean isRunning() {
        return state == State.RUNNING && delegate.isRunning();
    }

    /**
     * @see SubscriptionModelLifeCycle#isRunning(String)
     */
    @Override
    public boolean isRunning(String subscriptionId) {
        return !isWithheld(subscriptionId) && delegate.isRunning(subscriptionId);
    }

    /**
     * @return {@code true} for a subscription that is registered but not started, so that a caller starting everything
     * that is paused finds it.
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        if (shutdown) {
            return false;
        }
        return isWithheld(subscriptionId) || delegate.isPaused(subscriptionId);
    }

    /**
     * @see IntrospectableSubscriptions#subscriptionIds()
     */
    @Override
    public Set<String> subscriptionIds() {
        Set<String> ids = new HashSet<>(registrations.keySet());
        IntrospectableSubscriptions.findIn(delegate)
                .map(IntrospectableSubscriptions::subscriptionIds)
                .ifPresent(ids::addAll);
        return Set.copyOf(ids);
    }

    /**
     * @see SubscriptionModelWrapper#getWrappedSubscriptionModel()
     */
    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return delegate;
    }

    /**
     * This model resolves the start position to work out whether to record one when a subscription is registered. The
     * wrapped model receives the caller's own {@link StartAt} and resolves it under its own class, so where the
     * subscription starts is settled there rather than here.
     *
     * @return {@code false}
     * @see SubscriptionModelWrapper#decidesWhereTheSubscriptionStarts()
     */
    @Override
    public boolean decidesWhereTheSubscriptionStarts() {
        return false;
    }

    /**
     * @see SubscriptionModelLifeCycle#shutdown()
     */
    @Override
    public void shutdown() {
        shutdown = true;
        registrations.clear();
        registrationOrder.clear();
        delegate.shutdown();
    }

    @Override
    public String toString() {
        return ManualStartSubscriptionModel.class.getSimpleName() + "[delegate=" + delegate + ", state=" + state
                + ", registrations=" + registrations + ", pinsStartPosition=" + (positionSource != null) + "]";
    }

    // Resuming a subscription makes the wrapped model report itself running again, so this one must not disagree
    // while one of its own subscriptions is delivering. Nothing to undo when it was never started.
    private void reopenAfterStop() {
        synchronized (stateLock) {
            if (state == State.STOPPED) {
                state = State.RUNNING;
            }
        }
    }

    private void claim(String subscriptionId) {
        if (registrations.putIfAbsent(subscriptionId, Registration.STARTING) != null) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        registrationOrder.add(subscriptionId);
    }

    private void forget(String subscriptionId) {
        registrations.remove(subscriptionId);
        registrationOrder.remove(subscriptionId);
    }

    private boolean isWithheld(String subscriptionId) {
        Registration registration = registrations.get(subscriptionId);
        return registration instanceof Registration.Deferred || registration == Registration.STARTING;
    }

    // Written with ifAbsent() rather than an exists() check followed by a save(), because those are two calls with
    // nothing holding them together, so two nodes registering the same subscription id at the same moment would
    // both see nothing stored and both write.
    //
    // checkpointAlreadyExisted answers a presence question, not an identity one, and the checkpoint it read can be
    // gone by the time the write below runs, replaced by an unrelated one a cancelSubscription-then-register
    // elsewhere raced in. resolveFirstCheckpointRace is asked first specifically because it settles this by
    // comparing positions atomically with any write it makes, so it is never fooled by that replacement the way
    // trusting checkpointAlreadyExisted alone would be. checkpointAlreadyExisted stays as the fallback's own
    // signal, for a storage that answered resolveFirstCheckpointRace empty. See ADR 130.
    private void pinStartPosition(String subscriptionId, StartAt startAt) {
        if (positionSource == null || checkpointStorage == null) {
            return;
        }
        // Only the model default reads a stored checkpoint further down, so writing for any other position would
        // store a checkpoint nothing starts from, over a subscription the caller asked to replay. Resolved before
        // the read below, because a function answering the first-run question from this same storage must not find
        // this registration's own write.
        if (!startsAtTheModelDefault(startAt)) {
            return;
        }
        // Read before the position is captured, which is what tells a checkpoint that was already there apart from
        // one that arrived during this registration. The first was written before this capture, so starting from it
        // skips nothing while the source hands out positions in the order it is asked for them. The second can be a
        // later position than this node's own.
        boolean checkpointAlreadyExisted = checkpointStorage.exists(subscriptionId);
        @Nullable Checkpoint positionToPin = positionSource.globalCheckpoint();
        if (positionToPin == null) {
            // Answering null is how the source reports a problem it cannot resolve, so there is no position to record
            // and nothing to hold this registration to. Letting it through would start the subscription from wherever
            // the feed has reached once it is started, skipping everything written while it waited, which is the whole
            // of what recording at registration is for. A checkpoint that was already there is where the subscription
            // starts and nothing would have been recorded over it anyway, so that one is left alone.
            if (checkpointAlreadyExisted) {
                return;
            }
            throw positionSourceAnsweredNothing(subscriptionId);
        }
        try {
            checkpointStorage.save(subscriptionId, positionToPin, CheckpointWriteCondition.ifAbsent());
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            if (checkpointStorage.resolveFirstCheckpointRace(subscriptionId, positionToPin).isPresent()) {
                // Settled by position: either this node's own position was durably written in place of a stored one
                // it proved later, or the stored one was confirmed earlier than or equal to this node's own. Neither
                // outcome can have skipped anything this node's position would have covered.
                return;
            }
            if (checkpointAlreadyExisted) {
                return;
            }
            refuseUnlessTheStoredPositionIsTheOneRead(checkpointStorage, subscriptionId, positionToPin);
        }
    }

    // No original throwable to carry here, since answering nothing is how the source reports a problem it cannot
    // resolve, so this is what names the subscription and the way past it.
    private IllegalStateException positionSourceAnsweredNothing(String subscriptionId) {
        return new IllegalStateException("The position source " + requireNonNull(positionSource).getClass().getName() +
                                         " answered nothing when asked for the current position while registering subscription " +
                                         subscriptionId + ", which is how it reports a problem it cannot resolve, and no " +
                                         "checkpoint is stored for it either, so the registration is refused rather than " +
                                         "started from wherever the feed has reached by then, which would skip whatever " +
                                         "was written while it waited. Register again once the source can answer, " +
                                         "or use " + ManualStartSubscriptionModel.class.getSimpleName() +
                                         ".stoppedByDefault(SubscriptionModel), or a StartAt of your own, neither of which " +
                                         "records a position and neither of which carries such a guarantee.");
    }

    // Asks the question the way the wrapped models answer it when the subscription starts. Each layer resolves the
    // position for itself, and a layer answering with nothing leaves the subscription to the model it wraps, so the
    // answer that decides this is the first one a layer gives. Asking only the outermost model would read the start
    // position the annotations build, which answers with nothing for a catch-up layer, as a registration with no
    // position to record, and the durable model below it would then record one when the subscription starts.
    // A layer whose own resolution decides something other than where the subscription starts is not asked, since
    // the model below it settles that instead.
    private boolean startsAtTheModelDefault(StartAt startAt) {
        List<Class<?>> layersAsked = new ArrayList<>();
        SubscriptionModel model = delegate;
        while (true) {
            if (decidesWhereTheSubscriptionStarts(model)) {
                Class<?> modelType = model.getClass();
                layersAsked.add(modelType);
                @Nullable StartAt startAtToUse = startAt.get(new SubscriptionModelContext(modelType));
                if (startAtToUse != null && startAtToUse.isDefault()) {
                    return true;
                }
                if (startAtToUse != null) {
                    break;
                }
            }
            if (!(model instanceof SubscriptionModelWrapper wrapper)) {
                break;
            }
            model = wrapper.getWrappedSubscriptionModel();
        }
        // Nothing to record according to the walk above, and a model resolves the position against a class literal of
        // its own, so a subclass of a model, or a proxy standing in for one, was asked under a name that
        // hasSubscriptionModelType does not match, since that method compares for equality. Every class each layer
        // inherits from is asked before settling on recording nothing.
        return layersAsked.stream().anyMatch(modelType -> aClassItInheritsAnswersTheModelDefault(startAt, modelType));
    }

    private static boolean decidesWhereTheSubscriptionStarts(SubscriptionModel model) {
        return !(model instanceof SubscriptionModelWrapper wrapper) || wrapper.decidesWhereTheSubscriptionStarts();
    }

    // Object is left out since no subscription model resolves its start position against it. A proxy built by
    // java.lang.reflect.Proxy is asked under Proxy and a record under Record, which no start position Occurrent
    // builds answers for.
    private boolean aClassItInheritsAnswersTheModelDefault(StartAt startAt, Class<?> modelType) {
        for (Class<?> type = modelType.getSuperclass(); type != null && type != Object.class; type = type.getSuperclass()) {
            @Nullable StartAt startAtToUse = startAt.get(new SubscriptionModelContext(type));
            if (startAtToUse != null && startAtToUse.isDefault()) {
                return true;
            }
        }
        return false;
    }

    // Something was stored between the read above and this write, so it was written where this class cannot order
    // it against the position it read. Reading it back answers the only question that settles the registration,
    // whether it holds that same position. Anything else is refused rather than started from a position this
    // registration never read, which would skip whatever lies between the two.
    private void refuseUnlessTheStoredPositionIsTheOneRead(CheckpointStorage storage, String subscriptionId, Checkpoint positionRead) {
        @Nullable Checkpoint stored;
        try {
            stored = storage.read(subscriptionId);
        } catch (RuntimeException e) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFailed(subscriptionId, positionRead, e);
        }
        if (stored == null) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead);
        }
        if (positionRead.asString().equals(stored.asString())) {
            return;
        }
        throw new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored);
    }

    private sealed interface Registration {
        Registration STARTING = new Starting();

        record Deferred(@Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) implements Registration {
        }

        record Starting() implements Registration {
        }

        record Live(SubscriptionHandle subscription) implements Registration {
        }
    }

    // Stands for a registration that has not been started. It answers at once instead of waiting out the timeout,
    // because nothing changes until the application starts the subscription, and waiting would hang every caller that
    // uses the no-argument waitUntilStarted().
    private record DeferredSubscription(String id) implements SubscriptionHandle {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return false;
        }
    }
}
