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
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

import java.time.Duration;
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
 * its own still starts where it asked to. Two nodes registering the same subscription for the first time are the
 * exception, since only one of their two positions can be stored and neither node can tell which of them is earlier.
 * Without a position source and a checkpoint storage, a first run starts from the moment it is started.
 *
 * @see #stoppedByDefault(SubscriptionModel)
 */
@NullMarked
public final class ManualStartSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, IntrospectableSubscriptions {

    private static final System.Logger log = System.getLogger(ManualStartSubscriptionModel.class.getName());

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
     * to overlap, both attempt this write, and only the first to reach storage succeeds. A node that already finds a
     * checkpoint stored when it registers accepts it, which costs it nothing as long as {@code positionSource} hands
     * out positions in the order it is asked for them, as the MongoDB subscription models do. A node whose write is
     * refused by a checkpoint that arrived while it was registering accepts it too, and logs a warning when that
     * checkpoint holds a position other than the one it captured itself, because the two were captured on different
     * nodes and {@link Checkpoint} gives this class no way to tell which of them is earlier. Events written between
     * the two positions may not reach the subscription.
     * <p>
     * The position is recorded only for a registration that asks for {@link StartAt#subscriptionModelDefault()},
     * since that is the one a wrapped model reads a stored checkpoint for. Registering with a position of your own,
     * or with {@link StartAt#now()}, writes nothing, so a replay you asked for stays a replay instead of becoming a
     * resume. A {@link StartAt#dynamic(java.util.function.Supplier) dynamic} start position is resolved once at
     * registration to find out which of the two it is, before anything is read or written, so a function that answers
     * the first-run question by looking for a stored checkpoint sees what was stored before this registration. Such a
     * function therefore runs one more time than it used to. The wrapped model still receives the {@code StartAt} the
     * caller passed, whatever that resolution answered.
     *
     * @param delegate          The subscription model to register with once a subscription is started.
     * @param positionSource    Supplies the position to record. Typically the innermost model, the one reading the feed.
     *                          Only {@link GlobalCheckpointSource#globalCheckpoint()} is called, so a caller can pass
     *                          anything that exposes it, such as a {@link CheckpointAwareSubscriptionModel}, without
     *                          this method demanding the full subscription model that happens to implement it.
     * @param checkpointStorage Where the recorded position is written, which must be the storage the wrapped models read.
     * @throws IllegalArgumentException If {@code checkpointStorage} does not
     *                                  {@link CheckpointStorage#evaluatesWriteConditions() evaluate write conditions},
     *                                  since the {@code ifAbsent()} write this model makes at registration is then
     *                                  either refused outright or carried out over a checkpoint another node had
     *                                  already stored.
     */
    public static ManualStartSubscriptionModel stoppedByDefault(SubscriptionModel delegate, GlobalCheckpointSource<@Nullable Checkpoint> positionSource,
                                                                CheckpointStorage checkpointStorage) {
        requireNonNull(positionSource, GlobalCheckpointSource.class.getSimpleName() + " cannot be null");
        requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        if (!checkpointStorage.evaluatesWriteConditions()) {
            throw new IllegalArgumentException(checkpointStorage.getClass().getName() + " does not evaluate checkpoint write " +
                                               "conditions, so the ifAbsent() write that records a start position at " +
                                               "registration is either refused outright or carried out over a checkpoint " +
                                               "another node had already stored. Use " +
                                               ManualStartSubscriptionModel.class.getSimpleName() +
                                               ".stoppedByDefault(SubscriptionModel) instead, or a checkpoint storage that " +
                                               "evaluates them.");
        }
        return new ManualStartSubscriptionModel(delegate, positionSource, checkpointStorage);
    }

    /**
     * Register a subscription. While this model is withholding, nothing is passed to the wrapped model and the returned
     * {@link Subscription} is a placeholder standing for the registration. Its {@code waitUntilStarted} answers
     * {@code false} straight away, since the subscription has not started and will not until you ask. Once the
     * subscription is started, {@link #resumeSubscription(String)} returns the wrapped model's own subscription, which
     * is the handle to wait on.
     *
     * @throws DuplicateSubscriptionIdException If {@code subscriptionId} is already registered.
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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
            Subscription subscription = delegate.subscribe(subscriptionId, filter, startAt, action);
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
    public Subscription resumeSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Registration registration = registrations.get(subscriptionId);
        // Another thread is between claiming this registration and subscribing it, so the wrapped model does not have
        // the id yet and would answer that it knows nothing about it. Answer for the registration this model holds.
        if (registration instanceof Registration.Starting) {
            throw new SubscriptionAlreadyRunningException(subscriptionId,
                    "Subscription " + subscriptionId + " is already being started by another thread.");
        }
        if (!(registration instanceof Registration.Deferred deferred)) {
            Subscription subscription = delegate.resumeSubscription(subscriptionId);
            reopenAfterStop();
            return subscription;
        }
        // Claim it before touching the wrapped model, so two threads starting the same subscription cannot both subscribe.
        if (!registrations.replace(subscriptionId, deferred, Registration.STARTING)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId,
                    "Subscription " + subscriptionId + " is already being started by another thread.");
        }

        try {
            Subscription subscription = delegate.subscribe(subscriptionId, deferred.filter(), deferred.startAt(), deferred.action());
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
    private void pinStartPosition(String subscriptionId, StartAt startAt) {
        if (positionSource == null || checkpointStorage == null) {
            return;
        }
        // Only the model default reads a stored checkpoint further down, so writing for any other position would
        // store a checkpoint nothing starts from, over a subscription the caller asked to replay. Resolved through
        // whatever a dynamic position stands for, and resolved before the read below, because a function answering
        // the first-run question from this same storage must not find this registration's own write.
        @Nullable StartAt startAtToUse = startAt.get(new SubscriptionModelContext(delegate.getClass()));
        if (startAtToUse == null || !startAtToUse.isDefault()) {
            return;
        }
        // Read before the position is captured, which is what tells a checkpoint that was already there apart from
        // one that arrived during this registration. The first was written before this capture, so starting from it
        // skips nothing while the source hands out positions in the order it is asked for them. The second can be a
        // later position than this node's own.
        boolean checkpointAlreadyExisted = checkpointStorage.exists(subscriptionId);
        @Nullable Checkpoint positionToPin = positionSource.globalCheckpoint();
        if (positionToPin == null) {
            return;
        }
        try {
            checkpointStorage.save(subscriptionId, positionToPin, CheckpointWriteCondition.ifAbsent());
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            if (checkpointAlreadyExisted) {
                return;
            }
            warnThatAnotherRegistrationWon(checkpointStorage, subscriptionId, positionToPin);
        }
    }

    // The write has already been refused and the stored position already accepted, so reading it back only decides
    // what the warning can name. A failure here must not fail a registration whose outcome is settled either way.
    private void warnThatAnotherRegistrationWon(CheckpointStorage storage, String subscriptionId, Checkpoint positionToPin) {
        @Nullable Checkpoint stored;
        try {
            stored = storage.read(subscriptionId);
        } catch (RuntimeException e) {
            log.log(System.Logger.Level.WARNING,
                    "Subscription " + subscriptionId + " registered at position " + positionToPin.asString() +
                    " but another registration reached storage first, and reading that position back to name it " +
                    "here failed.", e);
            return;
        }
        if (stored == null) {
            log.log(System.Logger.Level.WARNING,
                    "Subscription " + subscriptionId + " registered at position " + positionToPin.asString() +
                    " but another registration reached storage first and that checkpoint has since been removed, " +
                    "so the position that won cannot be named here.");
            return;
        }
        if (positionToPin.asString().equals(stored.asString())) {
            return;
        }
        log.log(System.Logger.Level.WARNING,
                "Subscription " + subscriptionId + " registered at position " + positionToPin.asString() +
                " but another registration reached storage first with " + stored.asString() + ". The two positions " +
                "were captured on different nodes and cannot be compared, so a subscription that resumes from its " +
                "stored checkpoint starts from the stored position, and any events between the two may not reach " +
                "it. Recovering them means replaying that interval, which is only safe while this subscription is " +
                "not running anywhere.");
    }

    private sealed interface Registration {
        Registration STARTING = new Starting();

        record Deferred(@Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) implements Registration {
        }

        record Starting() implements Registration {
        }

        record Live(Subscription subscription) implements Registration {
        }
    }

    // Stands for a registration that has not been started. It answers at once instead of waiting out the timeout,
    // because nothing changes until the application starts the subscription, and waiting would hang every caller that
    // uses the no-argument waitUntilStarted().
    private record DeferredSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return false;
        }
    }
}
