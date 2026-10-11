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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Shared plumbing for the mode-specific catch-up subscription models ({@link StreamCatchupSubscriptionModel} and the
 * DCB catch-up model): the live delegate, config, running-catch-up bookkeeping, shutdown flag, and lifecycle
 * delegation. Replay and {@code subscribe(...)} routing stay in each subclass. DCB-free so it can live in the
 * stream module both modes build against.
 */
@NullMarked
abstract class AbstractCatchupSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, ReplayAwareSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);

    protected final CheckpointAwareSubscriptionModel subscriptionModel;
    protected final CatchupSubscriptionModelConfig config;
    protected final Class<?> subscriptionModelContextType;
    protected final ConcurrentMap<String, Boolean> runningCatchupSubscriptions;
    // Pause requested for a subscriptionId while its replay is still in-flight, before the delegate knows the id.
    // Applied via applyPendingPauseIfAny once the live delegate subscription exists.
    protected final ConcurrentMap<String, Boolean> pauseRequestedDuringCatchup = new ConcurrentHashMap<>();
    protected volatile boolean shuttingDown = false;
    // Set by stop(), and by a start whose live delegate failed on a stopped model. Cleared by start(...), and by a
    // resume of the live delegate after such a failed start. Checked by the replay loops so stop() interrupts an
    // in-flight replay, not just the delegate the replay has not registered with yet. Written only under lifecycleLock.
    protected volatile boolean stopped = false;
    // A leaf lock, held only to read or write stopped and the fields below, so no method is called and no other lock
    // is taken while it is held
    private final Object lifecycleLock = new Object();
    // Moves on with every start and stop, so a start whose live delegate failed can tell whether another came after it
    private long lifecycleGeneration = 0;
    // Moves on with every resume of the live delegate that returned, so a start whose live delegate failed can tell
    // whether one came after it
    private long liveDelegateResumes = 0;
    // Whether stopped was set by a start whose live delegate failed, rather than by a stop
    private boolean stoppedByFailedStart = false;
    // Replays subscribed while this model was stopped, and replays a stop() cut short. The live delegate knows none of
    // them until start(true) or a resume runs the replay again.
    private final ConcurrentMap<String, ParkedReplay> parkedReplays = new ConcurrentHashMap<>();
    // Identifies which attempt currently owns a subscriptionId, kept separately from runningCatchupSubscriptions
    // (which stays a plain presence marker, its shipped shape) so a cancelled attempt's replay thread, resuming
    // after a later attempt has taken the id over, can tell it is no longer current instead of clobbering the
    // later attempt's bookkeeping. CURRENT_ATTEMPT carries the calling attempt's identity across this same call
    // without threading it through every method signature; safe because startCatchupAsync gives each attempt its
    // own dedicated virtual thread, never reused, and clears the value in a finally block.
    private final ConcurrentMap<String, CatchupAttempt> currentAttempt;
    private static final ThreadLocal<@Nullable CatchupAttempt> CURRENT_ATTEMPT = new ThreadLocal<>();
    // How often a replay run again checks, while an earlier attempt's action still runs, whether it should still wait
    private static final long EARLIER_ATTEMPT_POLL_MILLIS = 100;
    // How many times a position replay may run again from its origin after the first replay, when the wrapped model
    // keeps losing the live start while the replay runs
    private static final int MAX_REPLAYS_AGAIN = 3;
    // The backoff before a replay a resume started runs again after it failed, as RetryStrategy.exponentialBackoff
    // with these values, the default of the subscription models and checkpoint storages
    private static final Duration FIRST_BACKOFF = Duration.ofMillis(100);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(2);
    // Who to tell about each id's catch-up boundaries, registered before the subscription that produces them.
    // Kept until this model shuts down, since the registration outlives any one catch-up: a stop and start, a
    // resume, or a cancel and re-subscribe all run another catch-up for the same id, and a recorder that stopped
    // being told would record that catch-up's history as though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // One lock per subscriptionId, guarding a fresh attempt's registration (startCatchupAsync), a finishing
    // attempt's checkpoint cleanup and delegate subscribe (or its cancelled-cleanup branch), and
    // cancelRunningCatchup. The identity check above only made the ownership decision itself atomic, not what
    // followed it, so a cancellation or a fresh registration could land in the gap after an attempt decided it was
    // still current but before it finished acting on that. Held only across those short transitions, never across
    // an in-flight replay, so a long catch-up is never serialized by this lock. Entries are never removed for an id
    // this instance has actually run a catch-up for, since a subscriptionId is application-defined and
    // low-cardinality here, unlike a per-event or per-request key; cancelRunningCatchup never creates one for an id
    // it has not seen, so an arbitrary or unknown id passed to cancelSubscription costs nothing.
    private final ConcurrentMap<String, ReentrantLock> handoverLocks;
    // What each subscription asked for when it was last made through this model or another child of the same
    // dispatcher, so a resume can replay from a position a catch-up stored for it. Removed when it is cancelled.
    private final ConcurrentMap<String, Subscribed> subscribed;

    protected AbstractCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this(subscriptionModel, config, subscriptionModelContextType, new SharedCatchupState());
    }

    /**
     * @param sharedState The per-id registries ({@link #lockHandover}'s locks, {@link #currentAttempt},
     *                     {@link #runningCatchupSubscriptions}) this instance draws from. A dispatcher over several
     *                     children that route the same id to a different one of them on different calls passes the
     *                     same state to every child, so a handover on one child and a fresh registration on another
     *                     still serialize and still see the same current owner for that id, both of which a
     *                     registry private to each child cannot give. Every other caller passes a fresh one,
     *                     private to this instance.
     */
    protected AbstractCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, SharedCatchupState sharedState) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscriptionModelContextType = Objects.requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
        Objects.requireNonNull(sharedState, "sharedState cannot be null");
        this.handoverLocks = sharedState.handoverLocks;
        this.currentAttempt = sharedState.currentAttempt;
        this.runningCatchupSubscriptions = sharedState.runningCatchupSubscriptions;
        this.subscribed = sharedState.subscribed;
    }

    /**
     * The per-id registries {@link #handoverLocks}, {@link #currentAttempt}, and {@link #runningCatchupSubscriptions}
     * bundled into one unit, so a dispatcher sharing them across its children shares all three together rather than
     * risking one shared and another forgotten. All three need to move together: the lock alone only keeps two
     * children's handovers from running at the same instant, it does not stop a stale one, once it finally runs,
     * from still finding itself "current" in a registry only it can see.
     */
    static final class SharedCatchupState {
        private final ConcurrentMap<String, ReentrantLock> handoverLocks = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CatchupAttempt> currentAttempt = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Subscribed> subscribed = new ConcurrentHashMap<>();
    }

    /**
     * A subscription as it was made, and the child of a dispatcher it was made through.
     */
    private record Subscribed(AbstractCatchupSubscriptionModel owner, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
    }

    // Reports subscriptionModelContextType (the dispatcher's type when wrapped) so a caller's StartAt.dynamic
    // pattern-matching on the public dispatcher type keeps working regardless of which subclass runs underneath.
    protected SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(subscriptionModelContextType);
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribeAndRemember(subscriptionId, filter, startAt, action, false);
    }

    /**
     * Keeps a replay waiting to run as a stopped model does, and has the live delegate hold a subscription with no
     * replay paused, so nothing is read or delivered until {@link #resumeSubscription(String)} or {@link #start(boolean) start(true)}.
     */
    @Override
    public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribeAndRemember(subscriptionId, filter, startAt, action, true);
    }

    // Locked, so a cancel comes either before the subscription is made, or after it is remembered and removes it.
    // Remembered after subscribing, since a subscription that starts live cancels any catch-up for the id once the
    // live delegate has it. A subscription made here that the live delegate holds is refused before anything kept for
    // it changes, since the live delegate refuses it too, at the latest when a replay hands over. Asked before the
    // lock, so a subscribe that comes while a replay for the id hands over isn't refused here, and its own replay
    // fails once it hands over.
    private Subscription subscribeAndRemember(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        if (subscribed.containsKey(subscriptionId) && (liveDelegate.isRunning(subscriptionId) || liveDelegate.isPaused(subscriptionId))) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            Subscription subscription = subscribe(subscriptionId, filter, startAt, action, holdPaused);
            subscribed.put(subscriptionId, new Subscribed(this, filter, startAt, action));
            return subscription;
        }
    }

    /**
     * Subscribes, holding the subscription paused when {@code holdPaused} is set, as {@link #subscribePaused} describes.
     */
    protected abstract Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused);

    /**
     * Hands {@code subscriptionId} to the live delegate, held paused there when {@code holdPaused} is set.
     */
    protected Subscription subscribeInTheWrappedModel(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        return holdPaused
                ? getWrappedSubscriptionModel().subscribePaused(subscriptionId, filter, startAt, action)
                : getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
    }

    @Override
    public void stop() {
        stopReplay();
        getWrappedSubscriptionModel().stop();
    }

    /**
     * Starts the live delegate and, with {@code resumeSubscriptionsAutomatically}, runs every parked replay again, as
     * the delegate resumes what it holds paused. Without it a parked replay waits for {@link #resumeSubscription(String)}.
     * <p>
     * When starting the live delegate throws, a model that was stopped before the call stops again and parks a replay
     * that ran in the meantime, as {@link #stop()} does. It doesn't stop again when the delegate runs afterwards, or
     * when another start or stop of this model, or a resume of the delegate, came while the delegate was starting.
     * Once a later resume of the delegate returns, the model is started again, unless a {@link #stop()} came first.
     * When the delegate throws on being asked whether it runs, the model stops again, and what the delegate threw is
     * added to the start's failure as suppressed.
     * <p>
     * When a subscription the live delegate holds paused would replay on {@link #resumeSubscription(String)}, the live
     * delegate is started without resuming anything, and each subscription it holds paused is then resumed as
     * {@link #resumeSubscription(String)} resumes it. That one replays from the stored position first, and the live
     * delegate resumes each of the others as its own {@code resumeSubscription(String)} does, without this waiting for
     * it to open. One that runs by the time it is resumed counts as resumed. The first resume that throws is thrown
     * once all of them are resumed. Reading the stored positions to decide this counts as part of starting the live
     * delegate, so when it throws, the model stops again as it does when the live delegate throws.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        StartAttempt attempt = beginStart();
        final boolean resumeEachHere;
        try {
            resumeEachHere = resumeSubscriptionsAutomatically && anyResumeReplays();
            getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically && !resumeEachHere);
        } catch (Throwable e) {
            undoStart(attempt, attempt.wasStopped() && runsAfterFailedStart(getWrappedSubscriptionModel(), e));
            throw e;
        }
        if (resumeSubscriptionsAutomatically) {
            relaunchParkedReplays();
        }
        if (resumeEachHere) {
            resumeEach(subscriptionsTheLiveDelegateHoldsPaused(), this::resumeSubscription);
        }
    }

    /**
     * Whether a subscription made through this model would replay on {@link #resumeSubscription(String)}, as
     * {@link #replayToResume(String)} describes, or such a replay runs or waits on this model. The live delegate holds
     * the subscription of each of them paused, and must not resume it itself.
     */
    boolean anyResumeReplays() {
        return currentAttempt.values().stream().anyMatch(attempt -> attempt.owner == this && attempt.replay.resuming)
                || parkedReplays.values().stream().anyMatch(parked -> parked.replay().resuming)
                || subscribed.entrySet().stream().anyMatch(entry -> entry.getValue().owner() == this && replayThatResumeRuns(entry.getKey()) != null);
    }

    /**
     * The subscriptions the live delegate holds paused, of those it lists, or of those made through this model or
     * another child of the same dispatcher when it can't list them.
     */
    Set<String> subscriptionsTheLiveDelegateHoldsPaused() {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        Set<String> known = IntrospectableSubscriptions.findIn(liveDelegate).map(IntrospectableSubscriptions::subscriptionIds)
                .orElseGet(() -> Set.copyOf(subscribed.keySet()));
        return known.stream().filter(liveDelegate::isPaused).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Resumes each of {@code subscriptionIds} with {@code resume}, also when an earlier one throws, and then throws the
     * first failure with the others added as suppressed. One that already runs, such as one whose replay handed over
     * after it was listed, counts as resumed.
     */
    static void resumeEach(Set<String> subscriptionIds, Consumer<String> resume) {
        List<RuntimeException> failures = new ArrayList<>();
        for (String subscriptionId : subscriptionIds) {
            try {
                resume.accept(subscriptionId);
            } catch (SubscriptionAlreadyRunningException ignored) {
                // Resumed by the time this got to it, which is what this was asked to do
            } catch (RuntimeException e) {
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            RuntimeException first = failures.getFirst();
            failures.subList(1, failures.size()).forEach(first::addSuppressed);
            throw first;
        }
    }

    @Override
    public boolean isRunning() {
        return !runningCatchupSubscriptions.isEmpty() || getWrappedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return runningCatchupSubscriptions.containsKey(subscriptionId) || getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return runningCatchupSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        catchupListeners.put(subscriptionId, listener);
        return true;
    }

    /**
     * Tells a listener that this attempt has read the history it set out to read, so what follows was written since
     * it started. Called by a subclass once its history read has delivered everything, and not at all when a stop
     * truncated it. The attempt itself is the episode, so a listener that has since been started by a later attempt
     * for the same id ignores this, and no lock is needed to keep a stale attempt from speaking. Only meaningful on
     * the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected void historyRead(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        CatchupListener listener = catchupListeners.get(subscriptionId);
        if (listener != null) {
            listener.historyRead(attempt);
        }
    }

    // The live delegate holds a subscription paused while a resume replays for it, and it runs all the same
    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringCatchup.containsKey(subscriptionId) || parkedReplays.containsKey(subscriptionId)
                || (!isReplayingToResume(subscriptionId) && getWrappedSubscriptionModel().isPaused(subscriptionId));
    }

    /**
     * Runs a parked replay again and passes any other subscription to the live delegate. When this model is stopped and
     * the subscription is paused, this first starts the model without resuming anything else, as resuming a
     * subscription starts the live delegate, so a subscription made afterwards replays at once.
     * <p>
     * When the position stored for the subscription is one a catch-up replays from, such as the position another
     * node's catch-up stored while it replayed, this replays from there before the live delegate resumes it, as
     * {@link #replayToResume(String)} describes.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        if (stopped && isPaused(subscriptionId)) {
            start(false);
        }
        pauseRequestedDuringCatchup.remove(subscriptionId);
        if (hasParkedReplay(subscriptionId)) {
            Subscription relaunched = relaunchParkedReplay(subscriptionId);
            if (relaunched != null) {
                return relaunched;
            }
        }
        Subscription replaying = replayToResume(subscriptionId);
        if (replaying != null) {
            return replaying;
        }
        Subscription resumed = getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
        liveDelegateResumed();
        return resumed;
    }

    /**
     * Replays from the position stored for {@code subscriptionId} when it is one a catch-up replays from, and resumes
     * the subscription the live delegate holds paused once the replay has delivered the history, from the live start
     * the stored position holds, or one read before the replay when it holds none. Returns the handle, or {@code null}
     * when this model has nothing to replay, so the live delegate resumes the subscription itself.
     * <p>
     * The live delegate can't open its live feed at such a position, and would resume from its own position instead,
     * which for a subscription it never opened is the present it read when the subscription was made. The events
     * between the stored position and that present would then be delivered to no one. Replaying delivers them, along
     * with events this node may already have delivered, so some arrive again.
     * <p>
     * Asked only for a subscription made through this model or another child of the same dispatcher, that the live
     * delegate holds paused, and whose start lets this model catch up. A resume while such a replay runs returns the
     * handle of that replay, also one that comes while the first resume decides to replay, since both decide under the
     * handover lock.
     *
     * @throws IllegalStateException when the subscription would replay but the live delegate can't resume a
     *                               subscription from a given position. It would resume from its own position, which can
     *                               lie past the history the replay reads, and the events in between would be lost.
     */
    @Nullable Subscription replayToResume(String subscriptionId) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            if (running != null && running.replay.resuming) {
                return new CatchupSubscription(subscriptionId, running.replay.result);
            }
            ResumeReplay resumeReplay = replayThatResumeRuns(subscriptionId);
            if (resumeReplay == null) {
                return null;
            }
            repositionableLiveDelegate(subscriptionId);
            warnIfStoredWithoutLiveStart(subscriptionId, resumeReplay.from());
            return new CatchupSubscription(subscriptionId, startCatchupAsync(subscriptionId, resumeReplay.replay(), false, true));
        }
    }

    private record ResumeReplay(CatchupReplay replay, Checkpoint from) {
    }

    // The replay a resume of subscriptionId runs before the live delegate resumes it, or null when it runs none
    private @Nullable ResumeReplay replayThatResumeRuns(String subscriptionId) {
        Subscribed subscription = subscribed.get(subscriptionId);
        if (subscription == null || subscription.owner() != this || shuttingDown || !getWrappedSubscriptionModel().isPaused(subscriptionId)
                || (subscription.startAt().isDynamic() && subscription.startAt().get(generateSubscriptionModelContext()) == null)) {
            return null;
        }
        Checkpoint stored = returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
        if (stored == null) {
            return null;
        }
        CatchupReplay replay = replayToResume(subscriptionId, subscription.filter(), subscription.startAt(), subscription.action(), stored);
        return replay == null ? null : new ResumeReplay(replay, stored);
    }

    /**
     * The replay from {@code stored} that {@link #replayToResume(String)} describes, or {@code null} when {@code stored}
     * is not a position this model replays from. This model has none.
     */
    @Nullable CatchupReplay replayToResume(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, Checkpoint stored) {
        return null;
    }

    private RepositionableSubscriptions repositionableLiveDelegate(String subscriptionId) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        return RepositionableSubscriptions.findIn(liveDelegate).orElseThrow(() -> new IllegalStateException("Cannot resume subscription " + subscriptionId
                + " from the position a catch-up stored for it, because " + liveDelegate.getClass().getName() + " can't resume a subscription from a given position."
                + " It would resume from its own position, which can lie past the history the catch-up replays, and the events in between would not be delivered."
                + " Use a subscription model that implements " + RepositionableSubscriptions.class.getSimpleName() + "."));
    }

    boolean isReplayingToResume(String subscriptionId) {
        CatchupAttempt running = currentAttempt.get(subscriptionId);
        return running != null && running.replay.resuming;
    }

    // Whether this model, rather than another child of the same dispatcher, runs the replay a resume started
    boolean replaysToResumeHere(String subscriptionId) {
        CatchupAttempt running = currentAttempt.get(subscriptionId);
        return running != null && running.replay.resuming && running.owner == this;
    }

    /**
     * Hands {@code subscriptionId} over to the live delegate once its replay has delivered the history. A replay a
     * subscribe started subscribes it there from {@code startAtToUse}. A replay a resume started resumes the
     * subscription the live delegate holds paused, from the position {@code startAtToUse} resolves to, which stores
     * the live start when the stored position is the catch-up's own. That subscription keeps the action it was made
     * with, so {@code liveConsumer} goes unused and an event the replay delivered can arrive again. The replay a resume
     * started ends once the live delegate has resumed the subscription, so what throws before that, reading or saving
     * the position included, fails the replay, which then runs again as {@link #runAgainAfter} decides. A pause asked
     * for during the replay pauses the subscription once it is handed over. Called with the handover lock held, on the
     * virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    Subscription handOver(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAtToUse, Consumer<CloudEvent> liveConsumer) {
        CatchupAttempt attempt = Objects.requireNonNull(CURRENT_ATTEMPT.get());
        final Subscription subscription;
        if (attempt.replay.resuming) {
            subscription = resumeTheLiveDelegate(subscriptionId, startAtToUse);
            if (currentAttempt.remove(subscriptionId, attempt)) {
                runningCatchupSubscriptions.remove(subscriptionId);
            }
        } else {
            subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAtToUse, liveConsumer);
        }
        applyPendingPauseIfAny(subscriptionId);
        return subscription;
    }

    // Repositioned, since a live delegate that reads no stored position resumes from where it last read, which can lie
    // past the history the replay read
    private Subscription resumeTheLiveDelegate(String subscriptionId, StartAt startAtToUse) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        StartAt liveStart = startAtToUse.get(new SubscriptionModelContext(liveDelegate.getClass()));
        Subscription resumed = liveStart instanceof StartAtCheckpoint
                ? repositionableLiveDelegate(subscriptionId).resumeSubscription(subscriptionId, liveStart)
                : liveDelegate.resumeSubscription(subscriptionId);
        liveDelegateResumed();
        return resumed;
    }

    /**
     * Pauses {@code subscriptionId}, once it is handed over when its replay still runs. Takes the handover lock, so a
     * pause that comes while a finishing replay hands over waits until the live delegate has the subscription. A replay
     * a resume started that waits to run again after it failed is held instead, so it doesn't run again until the
     * subscription is resumed.
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        HandoverLock lock = tryLockHandover(subscriptionId);
        try {
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            if (running != null && running.backingOff) {
                hold(subscriptionId, running);
            } else if (runningCatchupSubscriptions.containsKey(subscriptionId)) {
                // Delegate does not know this id yet, so record the request and apply it in applyPendingPauseIfAny
                // once the live subscription exists. The replay itself keeps running until the handover since
                // interrupting and resuming it would require persisting the exact replay cursor, which this class does not do.
                pauseRequestedDuringCatchup.put(subscriptionId, true);
            } else {
                getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
            }
        } finally {
            if (lock != null) {
                lock.close();
            }
        }
    }

    /**
     * Applies a pause requested via {@link #pauseSubscription(String)} while {@code subscriptionId}'s replay was
     * still in-flight, now that the live delegate subscription for it exists. A no-op if no pause was requested.
     */
    protected void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringCatchup.remove(subscriptionId) != null) {
            getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
        }
    }

    /**
     * Identifies one catch-up attempt for a subscription id, so a cancelled attempt whose replay thread has not
     * noticed yet can be told apart from a later attempt for the same id. Identity is the point of the class itself,
     * reference equality is exactly what every check here wants, but {@link #cancelled} also needs to reach this
     * exact attempt's own thread without touching {@link #currentAttempt}'s entry for it: flagging the attempt
     * object in place, instead of swapping in a sentinel, keeps the map entry itself untouched by cancellation, so
     * {@link #endReplayIfStillCurrent} remains the only remover and a cancelled id cannot linger in the map for the
     * rest of the model's lifetime the way a shared sentinel value would.
     */
    private static final class CatchupAttempt {
        // Set by parkIfStillCurrent, under the handover lock, once a stop() has taken this attempt's replay away from
        // it. It stores the position of an event whose action it completes, and does not complete the caller's
        // handle, which the parked replay completes.
        private volatile boolean parked = false;
        // Set under the handover lock while a replay a resume started waits to run again after it failed, so a pause
        // holds it instead of waiting for a handover
        private volatile boolean backingOff = false;
        private final AbstractCatchupSubscriptionModel owner;
        private final ReplayState replay;
        // Done once the thread of every earlier attempt at this replay has returned, so none of them is still inside
        // the subscriber's action
        private final CompletableFuture<Void> earlierAttemptsDone;
        // Done once this attempt's thread and every earlier attempt's thread have returned
        private final CompletableFuture<Void> done = new CompletableFuture<>();

        private CatchupAttempt(AbstractCatchupSubscriptionModel owner, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone) {
            this.owner = owner;
            this.replay = replay;
            this.earlierAttemptsDone = earlierAttemptsDone;
        }

        private boolean abandoned() {
            return replay.cancelled || parked;
        }
    }

    /**
     * One replay, shared by every attempt at it, the first one and each one that runs it again after a stop.
     */
    private static final class ReplayState {
        private final CatchupReplay catchup;
        // The handle its subscriber holds, completed by the attempt that hands the replay over
        private final CompletableFuture<Subscription> result;
        // Set by cancelRunningCatchup, and when a new subscription for the id drops the replay, so no attempt at it
        // stores a position any more, also one that stop() took the replay from
        private volatile boolean cancelled = false;
        // The last position an attempt at this replay stored, which the next attempt replays from
        private final AtomicReference<@Nullable Checkpoint> lastStored = new AtomicReference<>();
        // Whether a resume started it, so it resumes the subscription the live delegate holds instead of subscribing it
        private final boolean resuming;

        // How many attempts at a replay a resume started have failed in a row
        private int failures = 0;

        private ReplayState(CatchupReplay catchup, CompletableFuture<Subscription> result, boolean resuming) {
            this.catchup = catchup;
            this.result = result;
            this.resuming = resuming;
        }

        // FIRST_BACKOFF, doubled with each failure in a row up to MAX_BACKOFF. Called with the handover lock held.
        private Duration backoffAfterFailure() {
            failures++;
            long millis = FIRST_BACKOFF.toMillis() << Math.min(failures - 1, 16);
            return Duration.ofMillis(Math.min(millis, MAX_BACKOFF.toMillis()));
        }
    }

    /**
     * A replay waiting for this model to run again, so running it again completes the handle its subscriber holds.
     * {@code earlierAttemptsDone} completes once no earlier attempt at it is still running the subscriber's action,
     * which a replay that {@code stop()} cut short can still be doing when it is run again.
     */
    private record ParkedReplay(ReplayState replay, CompletableFuture<Void> earlierAttemptsDone) {
    }

    /**
     * Replays the history of one subscription and hands it over to the live subscription model.
     */
    @FunctionalInterface
    protected interface CatchupReplay {
        /**
         * @param lastStored The last position an earlier attempt at this replay stored through
         *                   {@link #saveCatchupCheckpoint}, which this attempt replays from, or {@code null} to replay
         *                   from where the subscription asked to start.
         */
        Subscription replayFrom(@Nullable Checkpoint lastStored) throws Exception;
    }

    /**
     * Whether persisting a checkpoint for an event this call's own attempt already delivered is still safe, which it is
     * when its replay was not cancelled and no attempt at another replay has taken the id over since. {@code null} (nobody
     * registered, for example {@link #markShuttingDown} clearing this same attempt's own entry, or a stop() taking the
     * replay from it) counts as safe, because a shutdown or a stop triggered by the very event being persisted does
     * not put a newer attempt's position at risk and deletes nothing itself. An attempt that runs this same replay again
     * counts as safe too, since it waits for this attempt's thread to return and then replays from the position this
     * attempt stored. This is deliberately looser than {@link #shouldKeepReplaying}, which needs exact identity to
     * decide whether to keep replaying at all, not whether one already-delivered event's position is still safe to
     * persist. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected boolean isSafeToPersistFor(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (attempt.replay.cancelled) {
            return false;
        }
        CatchupAttempt owner = currentAttempt.get(subscriptionId);
        return owner == null || owner.replay == attempt.replay;
    }

    /**
     * Stores {@code checkpoint} as the position the calling attempt's replay has delivered through, with the write
     * condition {@link #writeConditionFor} gives, and remembers it, so an attempt that runs this replay again after a
     * stop replays from there. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this
     * attempt.
     */
    protected void saveCatchupCheckpoint(String subscriptionId, UseCheckpointInStorage cfg, Checkpoint checkpoint) {
        cfg.storage().save(subscriptionId, checkpoint, writeConditionFor(cfg, subscriptionId));
        CURRENT_ATTEMPT.get().replay.lastStored.set(checkpoint);
    }

    /**
     * Whether the calling replay loop should keep going: not shutting down, not stopped, this call's own attempt was
     * not itself explicitly cancelled, and it is still the current one registered for {@code subscriptionId}. Checks
     * exact identity, not mere presence and not {@link #isSafeToPersistFor}'s looser null-is-fine rule, so an
     * attempt superseded by a later one for the same id (cancelled, then resubscribed before this attempt's thread
     * noticed), or simply cancelled outright with nothing yet taking its place, correctly stops instead of running
     * to completion or clobbering the later attempt's bookkeeping. Only meaningful on the virtual thread
     * {@link #startCatchupAsync} started for this attempt.
     * <p>
     * A replay that finds this model stopped parks itself before this returns, for the case where it registered after
     * {@link #stopReplay()} had parked the others.
     */
    protected boolean shouldKeepReplaying(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (stopped && !shuttingDown) {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                parkIfStillCurrent(subscriptionId, attempt);
            }
            return false;
        }
        return !shuttingDown && !attempt.abandoned() && currentAttempt.get(subscriptionId) == attempt;
    }

    /**
     * Ends the calling attempt's ownership of {@code subscriptionId} and reports whether it completed normally, that
     * is {@link #shouldKeepReplaying} was still true for it right before this ran. A replay calls this exactly once,
     * at the point where it decides whether it completed normally or was superseded, cancelled, or stopped, closing
     * the same identity race {@link #shouldKeepReplaying} closes for the loop checks, for the one-time final
     * decision. The map entry is atomically removed whenever it is still this attempt's own, whether ending
     * normally, cancelled, or stopped, so an id does not linger in {@link #currentAttempt} for the rest of the
     * model's lifetime once this attempt is done with it. Left alone when a later attempt has already taken the id
     * over, since only that later attempt may remove its own entry. A replay a resume started keeps its entry until
     * {@link #handOver} has resumed the live delegate, so what fails before that is handled as a failed replay.
     * <p>
     * A stopped model parks the attempt instead of ending it, so the replay runs again once this model is started or
     * the subscription resumed. Called with {@code subscriptionId}'s handover lock held.
     */
    protected boolean endReplayIfStillCurrent(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (shuttingDown) {
            return false;
        }
        if (stopped) {
            parkIfStillCurrent(subscriptionId, attempt);
            return false;
        }
        if (currentAttempt.get(subscriptionId) != attempt) {
            return false;
        }
        // A replay a resume started stays current through the handover, which ends it, so a handover that throws
        // fails the replay as a read that throws does. It no longer catches up, since its history replay has ended.
        if (attempt.replay.resuming && !attempt.abandoned()) {
            runningCatchupSubscriptions.remove(subscriptionId);
            return true;
        }
        currentAttempt.remove(subscriptionId, attempt);
        runningCatchupSubscriptions.remove(subscriptionId);
        return !attempt.abandoned();
    }

    /**
     * Takes {@code subscriptionId}'s replay away from {@code attempt} and parks it, if {@code attempt} is still the
     * current one. The subscription then counts as paused, not running, until the parked replay runs again. A
     * cancelled attempt is only ended, since nothing is to run it again. Called with the handover lock held, so a
     * finishing attempt either hands over before this or finds itself parked.
     * <p>
     * A start can allow replays to run again between the stop this parks for and the park itself, and start(true) then
     * runs what was parked before this replay was. So this asks whether the model is stopped once more after parking,
     * and runs the replay again when it is not, without resuming a pause asked for while it ran. Anything else that
     * allows replays in that window runs it again too, start(false) included.
     */
    private void parkIfStillCurrent(String subscriptionId, CatchupAttempt attempt) {
        if (attempt.replay.cancelled) {
            if (currentAttempt.remove(subscriptionId, attempt)) {
                runningCatchupSubscriptions.remove(subscriptionId);
            }
        } else if (hold(subscriptionId, attempt)) {
            AbstractCatchupSubscriptionModel owner = attempt.owner;
            // Read after the put, so a start that allows replays from here on finds this replay parked
            if (!owner.stopped && !owner.shuttingDown) {
                owner.relaunchParkedReplay(subscriptionId, false);
            }
        }
    }

    /**
     * Takes {@code subscriptionId}'s replay away from {@code attempt} and parks it, if {@code attempt} is still the
     * current one, until a resume or a start that resumes runs it again. Returns whether it did. Called with the
     * handover lock held.
     */
    private boolean hold(String subscriptionId, CatchupAttempt attempt) {
        if (!currentAttempt.remove(subscriptionId, attempt)) {
            return false;
        }
        runningCatchupSubscriptions.remove(subscriptionId);
        attempt.parked = true;
        attempt.owner.parkedReplays.put(subscriptionId, new ParkedReplay(attempt.replay, attempt.done));
        return true;
    }

    boolean hasParkedReplay(String subscriptionId) {
        return parkedReplays.containsKey(subscriptionId);
    }
    /**
     * Runs every replay this model has parked again, or parks it once more if this model is stopped by then.
     */
    public void relaunchParkedReplays() {
        for (String subscriptionId : parkedReplays.keySet()) {
            relaunchParkedReplay(subscriptionId);
        }
    }

    /**
     * Runs {@code subscriptionId}'s parked replay again and returns the handle its subscriber already holds, or
     * {@code null} when this model has no parked replay for it.
     */
    @Nullable Subscription relaunchParkedReplay(String subscriptionId) {
        return relaunchParkedReplay(subscriptionId, true);
    }

    /**
     * Runs {@code subscriptionId}'s parked replay again as {@link #relaunchParkedReplay(String)} does, and with
     * {@code resuming} also undoes a pause asked for before it was parked.
     */
    private @Nullable Subscription relaunchParkedReplay(String subscriptionId, boolean resuming) {
        final CatchupAttempt attempt;
        final ParkedReplay parked;
        // Locked, so a cancelRunningCatchup either finds the replay still parked or finds the attempt that runs it
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            parked = parkedReplays.remove(subscriptionId);
            if (parked == null) {
                return null;
            }
            if (resuming) {
                pauseRequestedDuringCatchup.remove(subscriptionId);
            }
            attempt = registerOrPark(subscriptionId, parked.replay(), parked.earlierAttemptsDone(), false);
        }
        if (attempt != null) {
            runOnItsOwnThread(subscriptionId, attempt);
        }
        return new CatchupSubscription(subscriptionId, parked.replay().result);
    }

    /**
     * A held {@link #lockHandover} lock. {@code close()} declares no checked exception (unlike plain
     * {@link AutoCloseable}), so a try-with-resources releasing one needs no catch clause; {@link ReentrantLock#unlock()}
     * never throws one.
     */
    protected interface HandoverLock extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Acquires {@code subscriptionId}'s handover lock for the duration of a try-with-resources block, held by
     * {@link #startCatchupAsync}'s registration, a subclass's replay-completion code from
     * {@link #endReplayIfStillCurrent} through its checkpoint cleanup and delegate {@code subscribe} call (or the
     * cancelled-cleanup branch), and {@link #cancelRunningCatchup}. A {@link ReentrantLock}, not
     * {@code synchronized}, because every caller here runs on a {@link #startCatchupAsync virtual thread} and a
     * handover span can block on storage or delegate I/O. Blocking inside a {@code synchronized} block would pin
     * the carrier thread for that whole span, a plain lock does not.
     */
    protected HandoverLock lockHandover(String subscriptionId) {
        ReentrantLock lock = handoverLocks.computeIfAbsent(subscriptionId, id -> new ReentrantLock());
        lock.lock();
        return lock::unlock;
    }

    /**
     * Acquires {@code subscriptionId}'s handover lock only if one already exists, {@code null} otherwise, without
     * creating one. Registration ({@link #startCatchupAsync}) is the only place a lock is created for an id, and it
     * creates the lock before it creates that id's {@link #currentAttempt} entry, so a missing lock here means no
     * attempt has ever been registered for this id and there is nothing for {@link #cancelRunningCatchup} to
     * coordinate with. Lets a cancellation for an id this instance has never run a catch-up for, including an
     * arbitrary or unknown one a caller passes to {@code cancelSubscription} defensively, stay free of the registry
     * instead of permanently reserving a lock for it.
     */
    protected @Nullable HandoverLock tryLockHandover(String subscriptionId) {
        ReentrantLock lock = handoverLocks.get(subscriptionId);
        if (lock == null) {
            return null;
        }
        lock.lock();
        return lock::unlock;
    }

    /**
     * Captures the live resume checkpoint handed over to live delivery. Every catch-up captures it before its bulk
     * replay, so an event that commits during the replay, whatever its position or time, is delivered live.
     * Returns null when the delegate must not run ({@code delegatedStartAt} null, catch-up owns the position
     * entirely). Fails loudly if the delegate has no checkpoint rather than silently resuming at "now" and
     * dropping events committed during replay.
     */
    protected @Nullable Checkpoint captureLiveResumeCheckpoint(@Nullable StartAt delegatedStartAt) {
        if (delegatedStartAt == null) {
            return null;
        }
        Checkpoint checkpoint = subscriptionModel.globalCheckpoint();
        if (checkpoint == null) {
            throw new IllegalStateException("Cannot run a catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.");
        }
        return checkpoint;
    }

    /**
     * Where a position replay starts, where live delivery picks up after it, and what the replay stores on the way.
     *
     * @param replayFrom   The global sequence position the replay reads after
     * @param liveFrom     The live start handed over to live delivery, or null when the catch-up owns the position
     *                     entirely and nothing goes live after it
     * @param replayOrigin The global sequence position the first attempt at this replay started from
     * @param replayTo     The head of the global sequence an earlier attempt read after it read {@code liveFrom}, or
     *                     null when this attempt reads the head itself
     */
    protected record PositionReplayStart(long replayFrom, @Nullable Checkpoint liveFrom, long replayOrigin, @Nullable Long replayTo) implements ReplayStart<PositionReplayStart> {

        /**
         * The checkpoint stored once the replay has delivered through {@code position}, for a replay whose head was
         * {@code replayTo}. It holds the live start, so a resume goes live from where this replay would have, not
         * from a live start read after the restart.
         */
        GlobalCheckpoint checkpointAt(long position, long replayTo) {
            return liveFrom == null ? GlobalCheckpoint.of(position) : GlobalCheckpoint.of(position, liveFrom, replayOrigin, replayTo);
        }

        @Override
        public PositionReplayStart fromOrigin(@Nullable Checkpoint newLiveFrom) {
            return new PositionReplayStart(replayOrigin, newLiveFrom, replayOrigin, null);
        }

        @Override
        public String describeOrigin() {
            return "position " + replayOrigin;
        }
    }

    /**
     * Where a time replay starts, where live delivery picks up after it, and what the replay stores on the way.
     *
     * @param replayFrom   The time the replay reads from, inclusive
     * @param liveFrom     The live start handed over to live delivery, or null when the catch-up owns the position
     *                     entirely and nothing goes live after it
     * @param replayOrigin The time the first attempt at this replay started from
     */
    protected record TimeReplayStart(TimeBasedCheckpoint replayFrom, @Nullable Checkpoint liveFrom, TimeBasedCheckpoint replayOrigin) implements ReplayStart<TimeReplayStart> {

        /**
         * The checkpoint stored once the replay has delivered an event at {@code time}. It holds the live start, so a
         * resume goes live from where this replay would have, not from a live start read after the restart.
         */
        Checkpoint checkpointAt(OffsetDateTime time) {
            TimeBasedCheckpoint at = TimeBasedCheckpoint.from(time);
            return liveFrom == null ? at : CatchupTimeCheckpoint.of(at.asString(), liveFrom, replayOrigin.asString());
        }

        @Override
        public TimeReplayStart fromOrigin(@Nullable Checkpoint newLiveFrom) {
            return new TimeReplayStart(replayOrigin, newLiveFrom, replayOrigin);
        }

        @Override
        public String describeOrigin() {
            return "time " + replayOrigin.asString();
        }
    }

    /**
     * A replay start that {@link #replayUntilLiveStartHolds} can start over from its origin.
     */
    protected interface ReplayStart<S extends ReplayStart<S>> {
        /**
         * The live start handed over to live delivery, or null when nothing goes live after the replay
         */
        @Nullable Checkpoint liveFrom();

        /**
         * A start that replays again from where the first attempt at this replay started, and goes live from
         * {@code newLiveFrom}
         */
        S fromOrigin(@Nullable Checkpoint newLiveFrom);

        /**
         * Where the first attempt at this replay started, as a log message names it
         */
        String describeOrigin();
    }

    /**
     * Resolves where a time replay starting at {@code start} reads from and goes live from.
     * <p>
     * A {@code start} that has a live start, stored by an earlier attempt at this replay, keeps it, so an event whose
     * time is earlier than the stored time but which was written after that attempt read past it is delivered live.
     * When the wrapped model no longer has the history from that live start, the replay starts over from the time the
     * first attempt started from, with a live start read now, and redelivers what it already delivered. A
     * {@code start} without a live start gets one read now, before the replay.
     */
    protected TimeReplayStart timeReplayStart(String subscriptionId, Checkpoint start, @Nullable StartAt delegatedStartAt) {
        if (!CatchupTimeCheckpoint.isCatchupTimeCheckpoint(start)) {
            TimeBasedCheckpoint time = start instanceof TimeBasedCheckpoint timeBasedCheckpoint ? timeBasedCheckpoint : timeOf(start.asString());
            return new TimeReplayStart(time, captureLiveResumeCheckpoint(delegatedStartAt), time);
        }
        CatchupTimeCheckpoint stored = CatchupTimeCheckpoint.parse(start);
        TimeBasedCheckpoint time = timeOf(stored.time());
        TimeBasedCheckpoint replayOrigin = timeOf(stored.replayOrigin());
        if (delegatedStartAt == null) {
            return new TimeReplayStart(time, null, time);
        }
        if (subscriptionModel.canResumeFrom(stored.liveFrom())) {
            return new TimeReplayStart(time, stored.liveFrom(), replayOrigin);
        }
        log.warn("The subscription model no longer has the history from the live start stored for catch-up subscription {}, so the catch-up replays again from time {} instead of {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayOrigin.asString(), time.asString(), stored.liveFrom().asString());
        return new TimeReplayStart(replayOrigin, captureLiveResumeCheckpoint(delegatedStartAt), replayOrigin);
    }

    private static TimeBasedCheckpoint timeOf(String time) {
        return TimeBasedCheckpoint.from(OffsetDateTime.parse(time, RFC_3339_DATE_TIME_FORMATTER));
    }

    /**
     * Resolves where a position replay starting at {@code start} reads from and goes live from.
     * <p>
     * A {@code start} that has a live start, stored by an earlier attempt at this replay, keeps it and the head that
     * attempt read, so an event whose position was reserved below the stored position but written after that attempt
     * read past it is delivered live. When the wrapped model no longer has the history from that live start, the
     * replay starts over from the position the first attempt started from, with a live start read now, and
     * redelivers what it already delivered. A {@code start} without a live start gets one read now, before the
     * replay, as in earlier versions.
     */
    protected PositionReplayStart positionReplayStart(String subscriptionId, Checkpoint start, @Nullable StartAt delegatedStartAt) {
        GlobalCheckpoint global = GlobalCheckpoint.parse(start);
        if (delegatedStartAt == null) {
            return new PositionReplayStart(global.position(), null, global.position(), null);
        }
        Checkpoint storedLiveFrom = global.liveFrom().orElse(null);
        if (storedLiveFrom == null) {
            return new PositionReplayStart(global.position(), captureLiveResumeCheckpoint(delegatedStartAt), global.position(), null);
        }
        long replayOrigin = global.replayOrigin().orElse(global.position());
        if (subscriptionModel.canResumeFrom(storedLiveFrom)) {
            return new PositionReplayStart(global.position(), storedLiveFrom, replayOrigin, global.replayTo().orElseThrow());
        }
        log.warn("The subscription model no longer has the history from the live start stored for catch-up subscription {}, so the catch-up replays again from position {} instead of {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayOrigin, global.position(), storedLiveFrom.asString());
        return new PositionReplayStart(replayOrigin, captureLiveResumeCheckpoint(delegatedStartAt), replayOrigin, null);
    }

    /**
     * Runs {@code replay} from {@code first} and returns the start whose live start the catch-up hands over to. After
     * each replay this asks the wrapped model whether it can still resume from that live start. When it can't, the
     * live start left the change stream history during the replay, and {@code replay} runs again from the position
     * or time the first attempt started from, with a live start read now. Handing the lost live start over instead would leave
     * it to the wrapped model, and a MongoDB model that restarts on lost history goes live from the present and skips
     * every event between the live start and the restart. A live start read now is asked about after its own replay
     * too, since a long replay can lose it as well.
     * <p>
     * So the catch-up goes live only from a live start the wrapped model accepted after the last replay, or throws
     * {@link IllegalStateException} once the replay ran {@value #MAX_REPLAYS_AGAIN} times again and lost the live
     * start each time. It never hands over a live start the wrapped model answered false for, but delivers the events
     * it replays again more than once. The check and the handover are two calls, so a live start that leaves the
     * history between them is still handed over, and what happens then is up to the wrapped model's handling of lost
     * history. A MongoDB model that restarts on lost history skips the events in between. A replay that was stopped,
     * cancelled or taken over is not asked about, and its start is returned as it is for the handover to deal with.
     * <p>
     * A {@link CatchupListener} is told the catch-up started again before each replay run again, since what that
     * replay delivers is history read again and not events written since the catch-up started. Only meaningful on the
     * virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected <S extends ReplayStart<S>> S replayUntilLiveStartHolds(String subscriptionId, S first, @Nullable StartAt delegatedStartAt, Consumer<S> replay) {
        S replayed = first;
        replay.accept(replayed);
        for (int replaysAgain = 0; ; replaysAgain++) {
            S again = replayAgainIfLiveStartLost(subscriptionId, replayed, delegatedStartAt, replaysAgain);
            if (again == null) {
                return replayed;
            }
            replayed = again;
            replay.accept(replayed);
        }
    }

    // Null when the wrapped model can still resume from the live start, or when this attempt should no longer replay
    private <S extends ReplayStart<S>> @Nullable S replayAgainIfLiveStartLost(String subscriptionId, S replayed, @Nullable StartAt delegatedStartAt, int replaysAgain) {
        Checkpoint liveFrom = replayed.liveFrom();
        if (liveFrom == null || !shouldKeepReplaying(subscriptionId) || subscriptionModel.canResumeFrom(liveFrom)) {
            return null;
        }
        if (replaysAgain >= MAX_REPLAYS_AGAIN) {
            throw new IllegalStateException("Cannot hand catch-up subscription " + subscriptionId + " over to live delivery, because the subscription model lost the live start during each of its "
                    + (replaysAgain + 1) + " replays. Size the change stream history, such as the MongoDB oplog, so that it outlasts the longest replay. Last live start: " + liveFrom.asString());
        }
        log.warn("The live start of catch-up subscription {} left the subscription model's history during the replay, so the catch-up replays again from {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayed.describeOrigin(), liveFrom.asString());
        S again = replayed.fromOrigin(captureLiveResumeCheckpoint(delegatedStartAt));
        // Told only while this attempt still owns the id, as on registration, so an attempt that lost the id cannot
        // reset what its replacement told the listener
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            if (currentAttempt.get(subscriptionId) != attempt) {
                return null;
            }
            CatchupListener listener = catchupListeners.get(subscriptionId);
            if (listener != null) {
                listener.catchupStarted(attempt);
            }
        }
        return again;
    }

    /**
     * Whether {@code checkpoint} is a global position or a time, with or without a live start, which is what a
     * catch-up stores during its replay. The handover replaces any of them with the live start, whichever kind of
     * catch-up wrote it, since the MongoDB subscription models don't recognize either and open at the present.
     */
    protected static boolean isCatchupCheckpoint(Checkpoint checkpoint) {
        return GlobalCheckpoint.isGlobalCheckpoint(checkpoint) || StreamCatchupSubscriptionModel.isTimeBasedCheckpoint(checkpoint);
    }

    /**
     * Logs a warning when {@code stored} is a global position or a time without a live start, which a catch-up stored
     * before the live start was kept. The resume replays from it and goes live from a live start read now, which can
     * miss an event whose position was reserved below the stored position, or whose time is earlier than the stored
     * time, but which was written after the earlier replay read past it.
     */
    protected static void warnIfStoredWithoutLiveStart(String subscriptionId, @Nullable Checkpoint stored) {
        if (stored == null) {
            return;
        }
        if (GlobalCheckpoint.isGlobalCheckpoint(stored) && GlobalCheckpoint.parse(stored).liveFrom().isEmpty()) {
            log.warn("Catch-up subscription {} resumes from stored checkpoint \"{}\", which has no live start. An event written to a position below it after the earlier replay read past that position is not delivered. See the 0.34.0 upgrade guide.",
                    subscriptionId, stored.asString());
        } else if (StreamCatchupSubscriptionModel.isTimeBasedCheckpoint(stored) && !CatchupTimeCheckpoint.isCatchupTimeCheckpoint(stored)) {
            log.warn("Catch-up subscription {} resumes from stored checkpoint \"{}\", which has no live start. An event with an earlier time written after the earlier replay read past that time is not delivered. See the 0.34.0 upgrade guide.",
                    subscriptionId, stored.asString());
        }
    }

    /**
     * Cancel a catch-up running for {@code subscriptionId}. A no-op if this class has no catch-up running for that id
     * (for example because it belongs to the other path in a dual-mode dispatcher). Does not touch the shared live
     * delegate or position storage; the dispatcher owns those since both paths share the same delegate.
     */
    public void cancelRunningCatchup(String subscriptionId) {
        // Locked, when a lock already exists for this id, so this call lands either strictly before or strictly
        // after a handover attempt's own lockHandover span for the same id, never inside it. Unlocked, it could run
        // in the gap after that attempt decided it was still current but before it acted on that, finding nothing
        // left to flag and losing the cancellation. tryLockHandover deliberately does not create a lock for an id
        // that has none yet, since that means no attempt has ever been registered for it and the operations below
        // are then no-ops with or without a lock, including for an arbitrary or unknown id a caller passes here.
        HandoverLock lock = tryLockHandover(subscriptionId);
        try {
            runningCatchupSubscriptions.remove(subscriptionId);
            // Flags whichever attempt is currently registered, atomically with respect to a concurrent resubscribe
            // for the same id, instead of removing or replacing the entry: a dual-mode dispatcher calls this on both
            // inner models for every cancellation, and the one with nothing running for this id must stay a no-op
            // rather than start tracking an id that is not its concern. The entry itself is left for the flagged
            // attempt's own endReplayIfStillCurrent to remove, so this call can never race a newer attempt's map
            // removal.
            currentAttempt.computeIfPresent(subscriptionId, (id, attempt) -> {
                attempt.replay.cancelled = true;
                return attempt;
            });
            pauseRequestedDuringCatchup.remove(subscriptionId);
            subscribed.remove(subscriptionId);
            // A parked replay never runs again, and waitUntilStarted on its subscriber's handle returns false. Flagged
            // too, since the attempt that stop() took it from can still be in the subscriber's action and must not
            // store a position once that returns.
            ParkedReplay parked = parkedReplays.remove(subscriptionId);
            if (parked != null) {
                parked.replay().cancelled = true;
                parked.replay().result.cancel(false);
            }
        } finally {
            if (lock != null) {
                lock.close();
            }
        }
    }

    /**
     * Ends a replay a resume started for {@code subscriptionId}, running or parked on this model, without touching
     * what is kept for the subscription, so a resume at a position the caller gives wins over it. The replay stores
     * nothing more and doesn't hand over, and a later {@link #resumeSubscription(String)} can replay again.
     */
    void endReplayToResume(String subscriptionId) {
        HandoverLock lock = tryLockHandover(subscriptionId);
        try {
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            if (running != null && running.replay.resuming) {
                running.replay.cancelled = true;
                runningCatchupSubscriptions.remove(subscriptionId);
                pauseRequestedDuringCatchup.remove(subscriptionId);
            }
            ParkedReplay parked = parkedReplays.get(subscriptionId);
            if (parked != null && parked.replay().resuming) {
                parkedReplays.remove(subscriptionId);
                parked.replay().cancelled = true;
                parked.replay().result.cancel(false);
            }
        } finally {
            if (lock != null) {
                lock.close();
            }
        }
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
        catchupListeners.clear();
        currentAttempt.clear();
        pauseRequestedDuringCatchup.clear();
        subscribed.clear();
        parkedReplays.values().forEach(parked -> parked.replay().result.cancel(false));
        parkedReplays.clear();
    }

    /**
     * Parks the replays in flight on this model, and every replay subscribed until the next start, without touching
     * the shared live delegate. A parked subscription is paused, not running, and runs its replay again once
     * {@link #relaunchParkedReplays()} or {@link #resumeSubscription(String)} runs it, from the last position the replay
     * stored, or from where it started when it stored none.
     */
    public void stopReplay() {
        synchronized (lifecycleLock) {
            stopped = true;
            stoppedByFailedStart = false;
            lifecycleGeneration++;
        }
        parkReplaysInFlight();
    }

    private void parkReplaysInFlight() {
        if (shuttingDown) {
            return;
        }
        for (var entry : currentAttempt.entrySet()) {
            CatchupAttempt attempt = entry.getValue();
            if (attempt.owner == this) {
                try (HandoverLock ignored = lockHandover(entry.getKey())) {
                    parkIfStillCurrent(entry.getKey(), attempt);
                }
            }
        }
    }

    /**
     * Allows the next replay on this model to run, without touching the shared live delegate. Does not run a replay
     * {@link #stopReplay()} parked, which {@link #relaunchParkedReplays()} does, apart from one whose replay thread is
     * still parking it.
     */
    public void resumeReplay() {
        synchronized (lifecycleLock) {
            stopped = false;
            stoppedByFailedStart = false;
            lifecycleGeneration++;
        }
    }

    /**
     * Whether a start found this model stopped, and where the lifecycle and the resumes of the live delegate stood
     * once it allowed replays to run again.
     */
    record StartAttempt(boolean wasStopped, long generation, long liveDelegateResumes) {
    }

    /**
     * Allows the next replay on this model to run, as {@link #resumeReplay()} does, before the live delegate is
     * started. Hand the result to {@link #undoStart(StartAttempt, boolean)} when that start throws.
     */
    StartAttempt beginStart() {
        synchronized (lifecycleLock) {
            StartAttempt attempt = new StartAttempt(stopped, ++lifecycleGeneration, liveDelegateResumes);
            stopped = false;
            stoppedByFailedStart = false;
            return attempt;
        }
    }

    /**
     * Whether {@code liveDelegate} runs after its start threw {@code startFailure}. A failure to tell counts as not
     * running and is added to {@code startFailure} as suppressed, so the caller still gets the start's own failure.
     */
    static boolean runsAfterFailedStart(SubscriptionModel liveDelegate, Throwable startFailure) {
        try {
            return liveDelegate.isRunning();
        } catch (Throwable e) {
            startFailure.addSuppressed(e);
            return false;
        }
    }

    /**
     * Stops this model again after starting the live delegate threw, when the model was stopped before, the delegate
     * is not running, and no other start or stop of this model and no resume of the delegate came since. A delegate
     * that runs after all keeps this model started, so a later subscription replays and hands over to it.
     *
     * @param liveDelegateRuns What {@link #runsAfterFailedStart} answered, asked before this is called, so the
     *                         lifecycle lock is never held while the delegate is called
     */
    void undoStart(StartAttempt attempt, boolean liveDelegateRuns) {
        if (!attempt.wasStopped() || liveDelegateRuns) {
            return;
        }
        synchronized (lifecycleLock) {
            // A resume of the delegate that returned after it was asked has moved liveDelegateResumes by now, and one
            // still under way clears this stop in liveDelegateResumed() once it returns
            if (attempt.generation() != lifecycleGeneration || attempt.liveDelegateResumes() != liveDelegateResumes) {
                return;
            }
            stopped = true;
            stoppedByFailedStart = true;
            lifecycleGeneration++;
        }
        parkReplaysInFlight();
    }

    /**
     * Records that a resume of the live delegate returned, and allows replays to run again when a start whose live
     * delegate failed stopped this model again, as resuming a subscription starts the delegate.
     */
    void liveDelegateResumed() {
        synchronized (lifecycleLock) {
            liveDelegateResumes++;
            if (stopped && stoppedByFailedStart) {
                stopped = false;
                stoppedByFailedStart = false;
                lifecycleGeneration++;
            }
        }
    }

    /**
     * Delete {@code subscriptionId}'s position from the configured position storage, if any. Exposed so the
     * dispatcher can delete it exactly once when cancelling a subscription that could belong to either mode, since
     * the position storage config (and the storage instance it wraps) is shared, not owned per mode.
     */
    public void deletePositionFromStorage(String subscriptionId) {
        doIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().delete(subscriptionId));
    }

    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return subscriptionModel;
    }

    /**
     * The {@link CheckpointWriteCondition} to stamp a checkpoint write triggered by {@code cfg} with. A version from
     * {@link UseCheckpointInStorage#checkpointWriteVersionSource()} becomes
     * {@link CheckpointWriteCondition#notOlderThan(long)}, and an empty answer or no source at all becomes
     * {@link CheckpointWriteCondition#any()}. Both {@link StreamCatchupSubscriptionModel} and the DCB catch-up model
     * call this for every checkpoint write, whichever config subtype triggered it, always through the 3-arg
     * {@code CheckpointStorage.save} rather than choosing between that and the 2-arg one.
     */
    protected CheckpointWriteCondition writeConditionFor(UseCheckpointInStorage cfg, String subscriptionId) {
        CheckpointWriteVersionSource source = cfg.checkpointWriteVersionSource();
        if (source == null) {
            return CheckpointWriteCondition.any();
        }
        OptionalLong version = source.writeVersion(subscriptionId);
        return version.isPresent() ? CheckpointWriteCondition.notOlderThan(version.getAsLong()) : CheckpointWriteCondition.any();
    }

    protected <T, C extends CheckpointStorageConfig> Optional<T> returnIfCheckpointStorageConfigIs(Class<C> cls, Function<C, @Nullable T> fn) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            return Optional.ofNullable(fn.apply(cls.cast(config.subscriptionStorageConfig)));
        }
        return Optional.empty();
    }

    protected <C extends CheckpointStorageConfig> void doIfCheckpointStorageConfigIs(Class<C> cls, Consumer<C> consumer) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            consumer.accept(cls.cast(config.subscriptionStorageConfig));
        }
    }

    /**
     * Starts {@code catchup} as {@link #startCatchupAsync(String, CatchupReplay, boolean)} does with {@code holdPaused}
     * unset. A replay that stop() cut short runs {@code catchup} again from the start.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, Callable<Subscription> catchup) {
        return startCatchupAsync(subscriptionId, lastStored -> catchup.call(), false);
    }

    /**
     * Registers a fresh attempt as the running catch-up for {@code subscriptionId} and runs {@code catchup} on its
     * own dedicated virtual thread, never reused, which is what lets {@link #shouldKeepReplaying} and
     * {@link #endReplayIfStillCurrent} read the attempt's identity from {@link #CURRENT_ATTEMPT} instead of a
     * parameter. This and {@link #relaunchParkedReplay} are the only places that put into
     * {@link #runningCatchupSubscriptions}.
     * <p>
     * A stopped model keeps {@code catchup} waiting to run instead, without reading anything, and so does
     * {@code holdPaused}. The returned future then completes once the replay has run and handed over.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused) {
        return startCatchupAsync(subscriptionId, catchup, holdPaused, false);
    }

    private Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused, boolean resuming) {
        CompletableFuture<Subscription> result = new CompletableFuture<>();
        ReplayState replay = new ReplayState(catchup, result, resuming);
        final CatchupAttempt attempt;
        // Locked so this registration cannot land inside a still-finishing earlier attempt's own lockHandover span
        // for the same id. Unlocked, this attempt could start, and its replay could reach a checkpoint save, before
        // the earlier attempt's late checkpoint delete runs, wiping out what this attempt just wrote instead of
        // its own.
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            attempt = registerOrPark(subscriptionId, replay, CompletableFuture.completedFuture(null), holdPaused);
        }
        if (attempt != null) {
            runOnItsOwnThread(subscriptionId, attempt);
        }
        return result;
    }

    /**
     * Registers a fresh attempt for {@code catchup} as the running catch-up for {@code subscriptionId}, or parks
     * {@code catchup} and returns {@code null} while this model is stopped or {@code holdPaused} is set. Called with the
     * handover lock held. A replay still parked for the id is dropped, and waitUntilStarted on its handle returns false.
     */
    private @Nullable CatchupAttempt registerOrPark(String subscriptionId, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone, boolean holdPaused) {
        ParkedReplay superseded = parkedReplays.remove(subscriptionId);
        if (superseded != null && superseded.replay() != replay) {
            superseded.replay().cancelled = true;
            superseded.replay().result.cancel(false);
        }
        if ((stopped || holdPaused) && !shuttingDown) {
            parkedReplays.put(subscriptionId, new ParkedReplay(replay, earlierAttemptsDone));
            // Asked again after the put, as parkIfStillCurrent does, so a start that allowed replays to run again
            // before it does not leave this replay parked
            if (holdPaused || stopped) {
                return null;
            }
            parkedReplays.remove(subscriptionId);
        }
        CatchupAttempt attempt = new CatchupAttempt(this, replay, earlierAttemptsDone);
        runningCatchupSubscriptions.put(subscriptionId, true);
        currentAttempt.put(subscriptionId, attempt);
        // Sent here, inside the same lock that takes ownership of the id and before the attempt's thread starts, so
        // it always precedes anything this attempt delivers.
        CatchupListener listener = catchupListeners.get(subscriptionId);
        if (listener != null) {
            listener.catchupStarted(attempt);
        }
        return attempt;
    }

    private void runOnItsOwnThread(String subscriptionId, CatchupAttempt attempt) {
        // The replay ends its attempt's ownership once it completes (in endReplayIfStillCurrent, or in handOver for a
        // replay a resume started), and keeps it when shouldKeepReplaying already turned false, so a cancel can still
        // be told apart from a completion. A replay or a handover that throws goes to runAgainAfter, which decides
        // for both modes whether it runs again.
        //
        // A parked attempt does not complete the handle. The parked replay completes it once it runs again, and the
        // flag is final by the time it is read, since parking takes the attempt out of currentAttempt under the
        // handover lock and a finishing attempt takes that lock too.
        Thread.ofVirtual().name("occurrent-catchup-" + subscriptionId).start(() -> {
            CURRENT_ATTEMPT.set(attempt);
            try {
                Subscription subscription = replayUntilHandedOver(subscriptionId, attempt);
                if (!attempt.parked) {
                    attempt.replay.result.complete(subscription);
                }
            } catch (Throwable failure) {
                if (!attempt.parked) {
                    attempt.replay.result.completeExceptionally(failure);
                }
            } finally {
                CURRENT_ATTEMPT.remove();
                attempt.earlierAttemptsDone.whenComplete((ignored, alsoIgnored) -> attempt.done.complete(null));
            }
        });
    }

    // Runs the replay, and runs it again after each failure for as long as runAgainAfter says so
    private Subscription replayUntilHandedOver(String subscriptionId, CatchupAttempt attempt) throws Throwable {
        while (true) {
            try {
                // Returns at once after the first round, since every earlier attempt has returned by then
                awaitEarlierAttempts(subscriptionId, attempt);
                // Read once every earlier attempt has returned, so it holds the last position any of them stored
                return attempt.replay.catchup.replayFrom(attempt.replay.lastStored.get());
            } catch (Throwable failure) {
                Duration backoff = runAgainAfter(subscriptionId, attempt, failure);
                if (backoff == null || !waitOutBackoff(subscriptionId, attempt, backoff)) {
                    throw failure;
                }
            }
        }
    }

    /**
     * Decides what comes of an attempt whose replay or handover failed, and is asked again once its backoff has passed.
     * Called with {@code failure} when the attempt fails, which this logs at {@code ERROR}, and with {@code null} once
     * the backoff has passed. Returns the backoff before the replay runs again, {@link Duration#ZERO} to run it again
     * now, or {@code null} when this attempt doesn't run it again. Taken under the handover lock, so every lifecycle
     * call for the id either comes before this decides or finds what it decided.
     * <p>
     * Only a replay a resume started runs again, since the live delegate holds its subscription paused and nothing else
     * would resume it. It runs again while it is still the current attempt, and nothing asked it to stop. A cancel, a
     * resume at a given position or a shutdown ends it, which the caller asked for. A stop, or a pause asked for during
     * the replay or while it waits, holds it until a resume or a start that resumes. A live delegate that already runs
     * the subscription ends it too, since running the replay again would only hand over to it once more.
     */
    private @Nullable Duration runAgainAfter(String subscriptionId, CatchupAttempt attempt, @Nullable Throwable failure) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            CatchupAttempt owner = currentAttempt.get(subscriptionId);
            boolean current = owner == attempt;
            if (current && attempt.replay.resuming && !attempt.abandoned() && !shuttingDown && !(failure instanceof SubscriptionAlreadyRunningException)) {
                if (stopped) {
                    attempt.backingOff = false;
                    parkIfStillCurrent(subscriptionId, attempt);
                    logFailure(subscriptionId, failure, "it runs again once the subscription is resumed or this model started");
                    return null;
                }
                if (pauseRequestedDuringCatchup.remove(subscriptionId) != null || Thread.currentThread().isInterrupted()) {
                    attempt.backingOff = false;
                    hold(subscriptionId, attempt);
                    logFailure(subscriptionId, failure, "it runs again once the subscription is resumed");
                    return null;
                }
                if (failure == null) {
                    attempt.backingOff = false;
                    // A replay run again delivers history again, as one replayUntilLiveStartHolds runs again does
                    CatchupListener listener = catchupListeners.get(subscriptionId);
                    if (listener != null) {
                        listener.catchupStarted(attempt);
                    }
                    return Duration.ZERO;
                }
                attempt.backingOff = true;
                // Put back when the handover failed, so the subscription runs and catches up until the replay ends
                runningCatchupSubscriptions.put(subscriptionId, true);
                Duration backoff = attempt.replay.backoffAfterFailure();
                logFailure(subscriptionId, failure, "it runs again in " + backoff.toMillis() + " ms");
                return backoff;
            }
            // Only while this attempt is still the current one, since an attempt a later subscribe for the same id
            // superseded must not remove that attempt's running marker, nor a pause its caller asked for
            if (current) {
                currentAttempt.remove(subscriptionId, attempt);
                runningCatchupSubscriptions.remove(subscriptionId);
                pauseRequestedDuringCatchup.remove(subscriptionId);
            }
            // Logged as well as reported, since a caller that never calls waitUntilStarted, such as the Spring Boot
            // starter by default for a subscription that replays history, would otherwise lose the subscription
            // without a trace. A cancelled, superseded or shut down attempt is not logged, since it was asked to stop.
            if (failure != null && !attempt.replay.cancelled && !shuttingDown && (owner == null || owner.replay == attempt.replay)) {
                logFailure(subscriptionId, failure, attempt.parked ? "it runs again once the subscription is resumed or this model started"
                        : failure instanceof SubscriptionAlreadyRunningException ? "it doesn't run again, since the wrapped subscription model already runs the subscription"
                        : "the subscription did not go live");
            }
            return null;
        }
    }

    private static void logFailure(String subscriptionId, @Nullable Throwable failure, String outcome) {
        if (failure != null) {
            log.error("The catch-up replay for subscription {} failed, so {}.", subscriptionId, outcome, failure);
        }
    }

    /**
     * Waits out {@code backoff}, or until {@code attempt} is no longer the current one, a stop came, or the model shuts
     * down, and then asks {@link #runAgainAfter} once more. Returns whether the replay runs again now. A pause holds the
     * attempt as soon as it comes, so this needs no polling for it.
     */
    private boolean waitOutBackoff(String subscriptionId, CatchupAttempt attempt, Duration backoff) {
        long deadline = System.nanoTime() + backoff.toNanos();
        while (currentAttempt.get(subscriptionId) == attempt && !attempt.abandoned() && !shuttingDown && !stopped) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMillis <= 0) {
                break;
            }
            try {
                Thread.sleep(Math.min(remainingMillis, EARLIER_ATTEMPT_POLL_MILLIS));
            } catch (InterruptedException e) {
                // Held by runAgainAfter, so a resume runs the replay again
                Thread.currentThread().interrupt();
                break;
            }
        }
        return runAgainAfter(subscriptionId, attempt, null) != null;
    }

    /**
     * Waits until no earlier attempt at this replay is still running the subscriber's action, so the action never runs
     * alongside itself. The wait is on this attempt's own thread, so {@code start()} and {@code resumeSubscription(..)}
     * return without it. It ends early once this attempt should no longer replay, so a cancel, a stop or a shutdown
     * is not held up by an action that does not return. The replay then runs as it would for a stop that came before
     * its first event, delivering nothing. An action that never returns keeps the subscription from replaying, as it
     * would keep the live subscription model from delivering its next event.
     */
    private void awaitEarlierAttempts(String subscriptionId, CatchupAttempt attempt) throws InterruptedException {
        while (!attempt.earlierAttemptsDone.isDone() && shouldKeepReplaying(subscriptionId)) {
            try {
                attempt.earlierAttemptsDone.get(EARLIER_ATTEMPT_POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException ignored) {
                // Checked again on the next round
            }
        }
    }
}
