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
 * written since registration instead of skipping them. Without them, a first run starts from the moment it is started.
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
     * The recorded position is pinned with {@link CheckpointWriteCondition#ifAbsent() ifAbsent()}, which only ever
     * writes a subscription's very first checkpoint against nothing stored. A fence condition has nothing to add to a
     * write like that, so this factory takes no {@link CheckpointWriteVersionSource} (see ADR 116).
     * <p>
     * A checkpoint that already existed when this subscription registered always wins, whatever position it holds,
     * because that means the subscription has run before, or another node already pinned and started it. A
     * checkpoint that only appears later either belongs to another node's own first-run pin, at the same position
     * or one captured minutes apart during a rolling deploy, or to a subscription a different node has been running
     * for a while by the time this one finally starts, which is exactly what starting behind a leader election
     * allows. This node cannot tell those two apart, since {@link Checkpoint} exposes nothing but
     * {@link Checkpoint#asString() asString()}, so the stored position always wins either way. A position that
     * differs from the one this node captured is logged at {@code WARNING} rather than accepted silently, since the
     * first case can mean events were skipped between the two registrations and is worth an operator's attention.
     *
     * @param delegate          The subscription model to register with once a subscription is started.
     * @param positionSource    Supplies the position to record. Typically the innermost model, the one reading the feed.
     *                          Only {@link GlobalCheckpointSource#globalCheckpoint()} is called, so a caller can pass
     *                          anything that exposes it, such as a {@link CheckpointAwareSubscriptionModel}, without
     *                          this method demanding the full subscription model that happens to implement it.
     * @param checkpointStorage Where the recorded position is written, which must be the storage the wrapped models read.
     */
    public static ManualStartSubscriptionModel stoppedByDefault(SubscriptionModel delegate, GlobalCheckpointSource<@Nullable Checkpoint> positionSource,
                                                                CheckpointStorage checkpointStorage) {
        return new ManualStartSubscriptionModel(delegate,
                requireNonNull(positionSource, GlobalCheckpointSource.class.getSimpleName() + " cannot be null"),
                requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null"));
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
            // Checked before capturing the position, not after, so a checkpoint written in between is never missed.
            // Reading it the other way round could observe no checkpoint, then have one land before the position
            // is captured, and wrongly treat that write as a first-run pin to compare against later instead of the
            // existing checkpoint it actually is.
            boolean checkpointAlreadyExisted = checkpointStorage != null && checkpointStorage.exists(subscriptionId);
            // Captured before taking the lock, so a slow position source cannot block start(boolean), and captured
            // whatever the state looks like from here, because a stop() landing in between would otherwise leave this
            // registration deferred with no position to start from.
            @Nullable Checkpoint positionToPin = capturePositionToPin();
            synchronized (stateLock) {
                if (state != State.RUNNING) {
                    registrations.put(subscriptionId, new Registration.Deferred(filter, startAt, action, positionToPin, checkpointAlreadyExisted));
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
            pinStartPosition(subscriptionId, deferred.positionToPin(), deferred.checkpointAlreadyExisted());
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

    private @Nullable Checkpoint capturePositionToPin() {
        return positionSource == null ? null : positionSource.globalCheckpoint();
    }

    // Written when the subscription starts rather than when it is registered, so a subscription that is never started
    // leaves nothing behind.
    //
    // The write is ifAbsent() rather than an exists() check followed by a save(), because those are two calls with
    // nothing holding them together. Two nodes racing to start the same subscription would both see nothing stored
    // and both write, and whichever wrote second would silently win, losing the events between the two positions
    // (see #669). ifAbsent() folds the check and the write into one call the storage evaluates atomically, so only
    // the first node's write can succeed.
    //
    // A refusal means this node lost the write, and what that means depends on whether a checkpoint already existed
    // when this node registered. If one did, this subscription has run before, or another node already pinned and
    // started it, and that stored position wins regardless of what this node captured for itself, with nothing
    // logged since that is the ordinary case. If none did, the write that beat this one either read the same
    // source position, which is harmless, or belongs to a subscription that has been running under a different
    // node ever since, which this node cannot tell apart from the harmless case: Checkpoint exposes only
    // asString(), and starting late behind a leader election is exactly this class's purpose, so an arbitrary
    // amount of real history can sit between this node's registration and its first attempt to start. A stored
    // position always wins regardless, since guessing wrong risks skipping events either way, but a position that
    // differs from what this node captured is logged at WARN so the discrepancy is visible instead of silent.
    private void pinStartPosition(String subscriptionId, @Nullable Checkpoint positionToPin, boolean checkpointAlreadyExisted) {
        if (positionToPin == null || checkpointStorage == null) {
            return;
        }
        try {
            checkpointStorage.save(subscriptionId, positionToPin, CheckpointWriteCondition.ifAbsent());
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            if (checkpointAlreadyExisted) {
                return;
            }
            @Nullable Checkpoint stored = checkpointStorage.read(subscriptionId);
            @Nullable String storedAsString = stored == null ? null : stored.asString();
            if (!positionToPin.asString().equals(storedAsString)) {
                log.log(System.Logger.Level.WARNING,
                        "Starting subscription " + subscriptionId + " from a position another node already pinned. " +
                        "This node captured " + positionToPin.asString() + ", storage has " + storedAsString + ". " +
                        "If the other node pinned this only moments ago, its events since this node's registration " +
                        "may have been skipped. If it has been running since, this is expected.");
            }
        }
    }

    private sealed interface Registration {
        Registration STARTING = new Starting();

        record Deferred(@Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action,
                        @Nullable Checkpoint positionToPin, boolean checkpointAlreadyExisted) implements Registration {
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
