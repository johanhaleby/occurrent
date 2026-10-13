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
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Shared plumbing for the mode-specific catch-up subscription models ({@link StreamCatchupSubscriptionModel} and the
 * DCB catch-up model): the live delegate, config, running-catch-up bookkeeping, shutdown flag, and lifecycle
 * delegation. Replay and {@code subscribe(...)} routing stay in each subclass. DCB-free so it can live in the
 * stream module both modes build against.
 */
@NullMarked
abstract class AbstractCatchupSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, ReplayAwareSubscriptions, RepositionableSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);

    protected final CheckpointAwareSubscriptionModel subscriptionModel;
    protected final CatchupSubscriptionModelConfig config;
    protected final Class<?> subscriptionModelContextType;
    protected final ConcurrentMap<String, Boolean> runningCatchupSubscriptions;
    // Pause requested for a subscriptionId while its replay is still in-flight. The handover of a replay a subscribe
    // started applies it once the live delegate has the subscription, and a replay a resume started is held instead.
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
    // How long a lifecycle call waits for another call for the same id to return, and a resume at a given position for
    // the action a replay is in. The same as InMemorySubscriptionModel.waitUntilAllEventsProcessed(), which also waits
    // for actions to return and throws IllegalStateException when they don't. The live MongoDB models wait a second for
    // a running action, but they go on once it has passed, and a wait that fails the call needs room for a slow action.
    // A competing consumer that loses its lease during a resume at a given position waits up to these 10 seconds in its
    // pause, on the thread that delivered the loss. With the MongoDB lease strategy the next lease change for that
    // subscription waits behind it.
    static final Duration LIFECYCLE_WAIT = Duration.ofSeconds(10);
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
    // this instance has run a catch-up for, so an application that subscribes and cancels ever new ids keeps one lock
    // for each of them until this model is gone. cancelRunningCatchup and a pause never create one for an id that has
    // none, so cancelSubscription or pauseSubscription for an unknown id adds no lock.
    private final ConcurrentMap<String, ReentrantLock> handoverLocks;
    // One claim per subscriptionId, held by a lifecycle call for as long as it runs, see claim(..). An entry exists
    // while a thread holds the claim or waits for it, and is removed once none does, so the map holds nothing for an id
    // whose calls have all returned.
    //
    // Lock order: the claim, then the handover lock, then lifecycleLock. A lifecycle call takes the handover lock while
    // it holds the claim, never the reverse. A replay thread takes the handover lock for its registration, its handover
    // and its failure decisions without the claim, and takes the claim only to run again after a backoff, before it
    // takes the handover lock. So no thread waits for a claim while it holds a handover lock.
    //
    // A thread holds the claim of one id at a time, apart from taking the one it holds again, so no thread waits for a
    // claim while it holds another. A claim holder waits for the handover lock, for the live delegate and the checkpoint
    // storage, and in a resume at a given position for the action a replay is in. Two ids can still wait on each other
    // through their actions. A resume at a given position for one id holds its claim and waits for its action, which
    // calls a lifecycle method of a second id and waits for that claim, held by a resume at a given position for the
    // second id that waits for an action calling a lifecycle method of the first. Such a cycle ends only when a wait
    // gives up after LIFECYCLE_WAIT, and the calls in it then throw IllegalStateException.
    //
    // None of the waits above call into the model that wraps this model. A CompetingConsumerSubscriptionModel calls this
    // model from its own threads while it holds its own locks, and reaches back only through the subscriber's action,
    // which runs on a replay thread or the live delegate's thread and never on one that holds a claim. Its action
    // wrapper waits for the lease without its locks, and lets an event through while it calls this model for the
    // subscription. A cycle through it has to pass through a wait for an action, which LIFECYCLE_WAIT bounds, and so
    // does one through any other caller.
    private final ConcurrentMap<String, ClaimLock> lifecycleClaims;
    // The ids a resume at a given position runs for that read as running when it took their claim, so they keep reading
    // as running and not paused until it returns, also while it holds a replay in the way. The subscription runs once
    // it returns, and a caller that saw it paused meanwhile would leave a pause undone, as a competing consumer that
    // loses its lease then does. One that read as paused isn't marked and reads as paused, so a caller that resumes
    // what is paused still resumes it once the call returns, whether or not the call resumed it.
    private final Set<String> repositioning;
    // What each subscription asked for when it was last made through this model or another child of the same
    // dispatcher, so a resume can replay from a position a catch-up stored for it. Removed once the live delegate has
    // cancelled it, and kept through a cancel the live delegate fails.
    private final ConcurrentMap<String, Subscribed> subscribed;
    // The replays a cancel that hasn't returned yet ended for each id. Kept from before the cancel takes them away until
    // it returns, and then moved to what is kept for the subscription when the cancel threw, so start(true) and every
    // resume find them all through the cancel, also when the subscription wasn't made through this model.
    private final ConcurrentMap<String, CancelUnderWay> cancelsUnderWay;
    // How many cancels have started, each counted once it is in cancelsUnderWay and before it changes anything else
    private final AtomicLong cancelsStarted;
    // The replays a resume started on this model, from before the replay is registered until its handle completes.
    // Running, waiting to run again or held, a replay stays in here, so start(true) finds it however it moves meanwhile.
    private final Set<ReplayState> resumeReplays = ConcurrentHashMap.newKeySet();
    // The live start the last replay of each id handed over, kept so that when the live delegate loses the history
    // from exactly that live start as it opens, the catch-up replays again instead of leaving the subscription to the
    // live delegate. Removed by a cancel, a resume at a given position, a shutdown and the takeover itself, and
    // replaced by the next handover.
    private final ConcurrentMap<String, HandedOver> handedOver;
    // Kept so markShuttingDown removes the same instance the constructor added
    private final HistoryLossListener historyLossListener = new HistoryLossListener() {
        @Override
        public HistoryLossHandling handleHistoryLoss(String subscriptionId, Checkpoint lostFrom) {
            return takeOverIfHandedOverAt(subscriptionId, lostFrom);
        }

        @Override
        public void restartingAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent) {
        }

        @Override
        public boolean storesRestartPositionOf(String subscriptionId) {
            return false;
        }
    };

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
        this.lifecycleClaims = sharedState.lifecycleClaims;
        this.repositioning = sharedState.repositioning;
        this.cancelsUnderWay = sharedState.cancelsUnderWay;
        this.cancelsStarted = sharedState.cancelsStarted;
        this.handedOver = sharedState.handedOver;
        HistoryLossReportingSubscriptions.findIn(subscriptionModel).ifPresent(model -> model.addHistoryLossListener(historyLossListener));
    }

    /**
     * The per-id registries {@link #handoverLocks}, {@link #currentAttempt}, {@link #runningCatchupSubscriptions},
     * {@link #subscribed}, {@link #lifecycleClaims}, {@link #repositioning}, {@link #cancelsUnderWay},
     * {@link #cancelsStarted} and {@link #handedOver} bundled into one unit, so a dispatcher sharing them across its children shares all of them
     * together rather than risking one shared and another forgotten. They need to move together. The lock alone only
     * keeps two children's handovers from running at the same instant, it does not stop a stale one, once it finally
     * runs, from still finding itself "current" in a registry only it can see, and a claim private to each child would
     * let two children's calls for the same id run at once.
     */
    static final class SharedCatchupState {
        private final ConcurrentMap<String, ReentrantLock> handoverLocks = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CatchupAttempt> currentAttempt = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Subscribed> subscribed = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, ClaimLock> lifecycleClaims = new ConcurrentHashMap<>();
        private final Set<String> repositioning = ConcurrentHashMap.newKeySet();
        private final ConcurrentMap<String, CancelUnderWay> cancelsUnderWay = new ConcurrentHashMap<>();
        private final AtomicLong cancelsStarted = new AtomicLong();
        private final ConcurrentMap<String, HandedOver> handedOver = new ConcurrentHashMap<>();
    }

    /**
     * The live start a replay handed over for a subscription, and what a takeover needs when the live delegate loses
     * the history from it as it opens.
     *
     * @param subscribed      The subscription as it was made when the replay handed over, so a takeover after a
     *                        cancel and a new subscribe of the id doesn't replay for the new one
     * @param liveStart       The live start as the live delegate got it
     * @param replayAgainFrom The checkpoint a resume replays from, which holds {@code liveStart} and the position or
     *                        time the replay started from
     * @param replay          The replay that handed over, which holds how many times it ran again and the handle its
     *                        subscriber holds
     * @param takeOverAsked   Set once the live delegate has lost the history from {@code liveStart} and this model has
     *                        answered that it takes the subscription over, before the live delegate pauses it
     * @param takingOver      Set once the live delegate has paused the subscription for the takeover
     */
    private record HandedOver(AbstractCatchupSubscriptionModel owner, Subscribed subscribed, Checkpoint liveStart, Checkpoint replayAgainFrom, ReplayState replay,
                              AtomicBoolean takeOverAsked, AtomicBoolean takingOver) {
    }

    // The replays a cancel that hasn't returned yet ended for one id, added to under the handover lock of the id by the
    // cancel that holds its claim
    private static final class CancelUnderWay {
        private volatile CompletableFuture<Void> replaysReturned;
        // What a cancel of the id that threw kept for the subscription during this cancel
        private volatile @Nullable Subscribed kept;

        private CancelUnderWay(CompletableFuture<Void> replaysReturned) {
            this.replaysReturned = replaysReturned;
        }

        private void add(CompletableFuture<Void> replayReturned) {
            replaysReturned = CompletableFuture.allOf(replaysReturned, replayReturned);
        }
    }

    // The lock behind a claim, and how many threads hold it or wait for it
    private static final class ClaimLock {
        private final ReentrantLock lock = new ReentrantLock();
        // Changed only inside compute and computeIfPresent of lifecycleClaims, which run one at a time for an id
        private int users = 1;
    }

    /**
     * A subscription as it was made, and the child of a dispatcher it was made through.
     *
     * @param cancelledReplayReturned Done once the thread of every replay a cancel that threw ended for the subscription
     *                                has returned, so a later resume doesn't run the action alongside it
     * @param lostLiveStart           The live start the live delegate lost when this model last took the subscription
     *                                over, and {@code null} before that. Only a run that a start(true) of the live
     *                                delegate resumed there reports a loss of it again.
     */
    private record Subscribed(AbstractCatchupSubscriptionModel owner, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action,
                              CompletableFuture<Void> cancelledReplayReturned, AtomicReference<@Nullable Checkpoint> lostLiveStart) {

        private Subscribed(AbstractCatchupSubscriptionModel owner, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this(owner, filter, startAt, action, CompletableFuture.completedFuture(null), new AtomicReference<>());
        }
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

    // Under the handover lock as well as the claim, so a handover or a failure of a replay for the id comes either before
    // the refusal is decided or after the subscription is remembered.
    private Subscription subscribeAndRemember(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        try (Claim ignored = claim(subscriptionId); HandoverLock alsoIgnored = lockHandover(subscriptionId)) {
            if (isInUse(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            Subscription subscription = subscribe(subscriptionId, filter, startAt, action, holdPaused);
            subscribed.put(subscriptionId, new Subscribed(this, filter, startAt, action));
            return subscription;
        }
    }

    // Running or paused means a replay for the id reads history, hands over, waits to run again or is held, or the live
    // delegate runs or holds paused a subscription made through this model or another child of the same dispatcher. A
    // cancelled one is not, and neither is one whose subscribe ran a replay that failed before the live delegate got it.
    private boolean isInUse(String subscriptionId) {
        CatchupAttempt running = currentAttempt.get(subscriptionId);
        if (running != null && !running.abandoned()) {
            return true;
        }
        Subscribed made = subscribed.get(subscriptionId);
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        return made != null && (made.owner().hasParkedReplay(subscriptionId) || liveDelegate.isRunning(subscriptionId) || liveDelegate.isPaused(subscriptionId));
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

    /**
     * Stops the live delegate and parks every replay in flight, as {@link #stopReplay()} describes, also when parking
     * one throws.
     */
    @Override
    public void stop() {
        runEach(List.<Runnable>of(this::stopReplay, getWrappedSubscriptionModel()::stop), Runnable::run);
    }

    /**
     * Starts the live delegate and, with {@code resumeSubscriptionsAutomatically}, runs every parked replay again, as
     * the delegate resumes what it holds paused. Without it a parked replay waits for {@link #resumeSubscription(String)}.
     * A parked replay whose subscription another call still runs for after {@link #LIFECYCLE_WAIT} isn't run again, and
     * the {@link IllegalStateException} that {@link #claim} throws for it is thrown once every other parked replay has
     * run and every other subscription is resumed. This model is started all the same.
     * <p>
     * When starting the live delegate throws, a model that was stopped before the call stops again and parks a replay
     * that ran in the meantime, as {@link #stop()} does. It doesn't stop again when the delegate runs afterwards, or
     * when another start or stop of this model, or a resume of the delegate, came while the delegate was starting.
     * Once a later resume of the delegate returns, the model is started again, unless a {@link #stop()} came first.
     * When the delegate throws on being asked whether it runs, the model stops again, and what the delegate threw is
     * added to the start's failure as suppressed.
     * <p>
     * When a subscription the live delegate holds paused would replay on {@link #resumeSubscription(String)}, a replay
     * that a cancel ended for one hasn't returned yet, or a {@link #cancelSubscription(String)} is under way, the live
     * delegate is started without resuming anything, and each subscription it holds paused is then resumed as
     * {@link #resumeSubscription(String)} resumes it, one at a time, and so is each one a cancel is under way for while
     * they are listed, once that cancel has returned. One that replays does so from the stored position
     * first, one whose cancelled replay hasn't returned waits for it as {@link #resumeSubscription(String)} does, and the
     * live delegate resumes each of the others as its own {@code resumeSubscription(String)} does, without this waiting
     * for it to open. One whose cancelled replay doesn't return holds up the ones after it for up to ten seconds, and
     * its resume then throws {@link IllegalStateException}. One that runs by the time it is resumed counts as resumed,
     * and one cancelled by then is skipped. The first resume that throws is thrown once all of them are resumed. Reading
     * the stored positions to decide this counts as part of starting the live delegate, so when it throws, the model
     * stops again as it does when the live delegate throws.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        StartAttempt attempt = beginStart();
        final boolean resumeEachHere;
        try {
            resumeEachHere = resumeSubscriptionsAutomatically && anyResumeReplays();
            getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically && !resumeEachHere);
        } catch (Throwable e) {
            undoStart(attempt, attempt.wasStopped() && runsAfterFailedStart(getWrappedSubscriptionModel(), e), e);
            throw e;
        }
        List<Runnable> resumes = new ArrayList<>();
        if (resumeSubscriptionsAutomatically) {
            resumes.add(this::relaunchParkedReplays);
        }
        if (resumeEachHere) {
            resumes.add(() -> resumeEach(subscriptionsTheLiveDelegateHoldsPaused(), this::resumeSubscription));
        }
        runEach(resumes, Runnable::run);
    }

    /**
     * Whether a subscription made through this model would replay on {@link #resumeSubscription(String)}, as
     * {@link #replayToResume(String, String)} describes, or such a replay runs or waits on this model, or the live
     * delegate lost the history from the live start of one and this model takes it over, or a replay a
     * cancel that threw ended for one hasn't returned yet, or a cancel of any subscription is under way. The live
     * delegate holds the subscription of each of them paused, and must not resume it itself.
     * <p>
     * Returns true whenever a cancel through this model or another child of the same dispatcher is under way at any
     * moment while this reads, whatever the live delegate reports during its own cancel.
     */
    boolean anyResumeReplays() {
        // A resume puts its replay in resumeReplays before the replay stores the live start over the catch-up's
        // position, and the replay stays there until its handle completes. Every change a cancel makes falls between
        // its put into cancelsUnderWay and its removal, and the count moves in between. So a cancel that overlaps the
        // reads is under way at the first read of cancelsUnderWay, under way at the last, or moved the count, whatever
        // the live delegate reported during it.
        long cancelsStartedBefore = cancelsStarted.get();
        if (!cancelsUnderWay.isEmpty()) {
            return true;
        }
        boolean any = subscribed.entrySet().stream().anyMatch(entry -> entry.getValue().owner() == this && replayThatResumeRuns(entry.getKey()) != null)
                || !resumeReplays.isEmpty()
                || handedOver.values().stream().anyMatch(made -> made.owner() == this && (made.takeOverAsked().get() || made.takingOver().get()))
                || !cancelsUnderWay.isEmpty()
                || subscribed.values().stream().anyMatch(made -> made.owner() == this && !made.cancelledReplayReturned().isDone());
        return any || cancelsStarted.get() != cancelsStartedBefore;
    }

    /**
     * The subscriptions the live delegate holds paused, of those it lists, or of those made through this model or
     * another child of the same dispatcher when it can't list them, and every subscription a cancel is under way for at
     * any moment while they are read. A resume of one whose cancel is under way waits for the cancel, as every call for
     * it does, so one the live delegate reports as running during its own cancel and holds paused once that cancel
     * throws is resumed too.
     */
    Set<String> subscriptionsTheLiveDelegateHoldsPaused() {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        Set<String> paused = new LinkedHashSet<>();
        long cancelsStartedBefore;
        // Read again while the count moves, so a cancel that starts and ends between the two reads of cancelsUnderWay
        // still puts its subscription in the set. While a cancel starts during every round, start(true) stays in this
        // loop and resumes nothing until a round passes with none.
        do {
            cancelsStartedBefore = cancelsStarted.get();
            paused.addAll(cancelsUnderWay.keySet());
            Set<String> known = IntrospectableSubscriptions.findIn(liveDelegate).map(IntrospectableSubscriptions::subscriptionIds)
                    .orElseGet(() -> Set.copyOf(subscribed.keySet()));
            known.stream().filter(liveDelegate::isPaused).forEach(paused::add);
            paused.addAll(cancelsUnderWay.keySet());
        } while (cancelsStarted.get() != cancelsStartedBefore);
        return paused;
    }

    /**
     * Resumes each of {@code subscriptionIds} with {@code resume}, also when an earlier one throws, and then throws the
     * first failure with the others added as suppressed. One that already runs, such as one whose replay handed over
     * after it was listed, counts as resumed. One the live delegate no longer knows, such as one cancelled after it was
     * listed, is skipped.
     */
    static void resumeEach(Set<String> subscriptionIds, Consumer<String> resume) {
        runEach(subscriptionIds, subscriptionId -> {
            try {
                resume.accept(subscriptionId);
            } catch (SubscriptionAlreadyRunningException ignored) {
                // Resumed by the time this got to it, which is what this was asked to do
            } catch (UnknownSubscriptionException ignored) {
                // Cancelled by the time this got to it, so nothing is left to resume
            }
        });
    }

    /**
     * Runs {@code step} for each of {@code items}, also when an earlier one throws, an {@link Error} included, and then
     * throws the first {@link Error} when a step threw one, and the first failure otherwise, with the others added as
     * suppressed. A failure thrown more than once, such as a preallocated {@link OutOfMemoryError}, is thrown and not
     * added to itself.
     */
    static <T> void runEach(Iterable<T> items, Consumer<T> step) {
        List<Throwable> failures = new ArrayList<>();
        for (T item : items) {
            try {
                step.accept(item);
            } catch (RuntimeException | Error e) {
                failures.add(e);
            }
        }
        if (failures.isEmpty()) {
            return;
        }
        Throwable thrown = failures.stream().filter(Error.class::isInstance).findFirst().orElse(failures.getFirst());
        failures.forEach(failure -> addSuppressed(thrown, failure));
        if (thrown instanceof Error error) {
            throw error;
        }
        throw (RuntimeException) thrown;
    }

    // Throwable.addSuppressed throws when given the failure itself, and that exception would be thrown in its place
    static void addSuppressed(Throwable failure, Throwable suppressed) {
        if (suppressed != failure) {
            failure.addSuppressed(suppressed);
        }
    }

    @Override
    public boolean isRunning() {
        return !runningCatchupSubscriptions.isEmpty() || getWrappedSubscriptionModel().isRunning();
    }

    // Running while a resume replays for it, also while that replay hands over and isn't catching up any more, since
    // isPaused ignores the live delegate's pause until then, and while a resume at a given position runs for it
    @Override
    public boolean isRunning(String subscriptionId) {
        return runningCatchupSubscriptions.containsKey(subscriptionId) || isReplayingToResume(subscriptionId)
                || isRepositioning(subscriptionId) || getWrappedSubscriptionModel().isRunning(subscriptionId);
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

    // The live delegate holds a subscription paused while a resume replays for it, and it runs all the same. Not paused
    // while a resume at a given position runs for one that ran either, though that resume holds a replay in the way and
    // the live delegate holds the subscription paused until it resumes it.
    @Override
    public boolean isPaused(String subscriptionId) {
        return !isRepositioning(subscriptionId) && (pauseRequestedDuringCatchup.containsKey(subscriptionId) || parkedReplays.containsKey(subscriptionId)
                || (!isReplayingToResume(subscriptionId) && getWrappedSubscriptionModel().isPaused(subscriptionId)));
    }

    // Whether a resume at a given position runs for subscriptionId, on this model or another child of the same dispatcher
    boolean isRepositioning(String subscriptionId) {
        return repositioning.contains(subscriptionId);
    }

    /**
     * Runs a parked replay again and passes any other subscription to the live delegate. When this model is stopped and
     * the subscription is paused, this first starts the model without resuming anything else, as resuming a
     * subscription starts the live delegate, so a subscription made afterwards replays at once.
     * <p>
     * When the position stored for the subscription is one a catch-up replays from, such as the position another
     * node's catch-up stored while it replayed, this replays from there before the live delegate resumes it, as
     * {@link #replayToResume(String, String)} describes.
     * <p>
     * After a {@link #cancelSubscription(String)} that threw, the replay that cancel ended can still be in the
     * subscription's action or in a read of the history, also when a {@link #stop()} came before or after the cancel. A
     * replay this starts waits on its own thread until that replay has returned. When this doesn't replay, it waits up
     * to ten seconds for that replay to return before the live delegate resumes the subscription, and throws
     * {@link IllegalStateException} without resuming it when the replay hasn't returned by then, or at once when called
     * from inside the action. Like every other call for the subscription it runs alone, so a
     * {@link #pauseSubscription(String)} or {@link #cancelSubscription(String)} for the subscription that comes meanwhile
     * doesn't run until it returns, and a {@link #stop()} can be held up the same way.
     * <p>
     * A resume while a replay for the subscription runs returns the handle of that replay. A pause asked while the replay
     * still reads history is dropped, so the replay hands over and the subscription runs.
     * <p>
     * When another call for the subscription still runs after {@link #LIFECYCLE_WAIT}, this throws
     * {@link IllegalStateException} without resuming the subscription. A model that was stopped is started by then.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        // Started before the claim is taken, since a start that fails parks the replays of other ids under their claims
        boolean started = stopped && isPaused(subscriptionId);
        if (started) {
            start(false);
        }
        try (Claim ignored = claim(subscriptionId, started ? STARTED_BUT_NOT_RESUMED : NOTHING_CHANGED)) {
            pauseRequestedDuringCatchup.remove(subscriptionId);
            if (hasParkedReplay(subscriptionId)) {
                Subscription relaunched = relaunchParkedReplay(subscriptionId);
                if (relaunched != null) {
                    return relaunched;
                }
            }
            String whatChanged = started ? STARTED_BUT_NOT_RESUMED : NOTHING_CHANGED;
            Subscription replaying = replayToResume(subscriptionId, whatChanged);
            if (replaying != null) {
                return replaying;
            }
            awaitTheCancelledReplay(subscriptionId, whatChanged);
            Subscription resumed = getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
            liveDelegateResumed();
            return resumed;
        }
    }

    /**
     * Resumes the subscription the live delegate holds paused at {@code startAt}, through whichever
     * {@link RepositionableSubscriptions} the live delegate resolves to. Unlike {@link #resumeSubscription(String)}, it
     * doesn't start this model first or run a parked replay, and it never replays, also not at a position a catch-up
     * stores. The live delegate handles such a position as one it cannot open its live feed at. A replay that
     * {@link #resumeSubscription(String)} started for the subscription, running or waiting to run again, is held first
     * and ends once the live delegate has resumed the subscription, so the position given here wins over it, and a later
     * {@link #resumeSubscription(String)} can replay again. When the live delegate throws, a replay this held runs again
     * as it was, and this throws what the live delegate threw. When the replay is in the subscription's action or reads
     * history, this holds the replay and waits up to ten seconds for it to return before the live delegate resumes the
     * subscription, so the replay's action doesn't run alongside the action of the resumed subscription. After a
     * {@link #cancelSubscription(String)} that threw, it waits the same way for the replay that cancel
     * ended, which nothing runs again, also when a {@link #stop()} came before or after the cancel. When the replay
     * doesn't return in time, this logs at {@code ERROR}, runs a replay it held again as it was and throws
     * {@link IllegalStateException}, with what running it again threw added as suppressed. A replay this held stays held
     * instead of running again when this model was stopped meanwhile or the {@link CatchupListener} throws as it starts,
     * and the {@link IllegalStateException} says so. It runs again on a resume or a start that resumes. Called from
     * inside the action, on the replay's own thread, this throws {@link IllegalStateException} at once and changes
     * nothing. An action that hands the call to another thread and waits for it fails once the ten seconds have passed.
     * <p>
     * Like every other call that changes the subscription, it runs alone. A pause, resume, cancel or subscribe for the
     * same id waits until it returns, for up to ten seconds, and then acts on what it left. While it runs, a subscription
     * that read as running when it began reads as running and not paused, also while it waits for the action, so a
     * caller that pauses what runs calls {@link #pauseSubscription(String)}, which pauses it once this has returned. One
     * that read as paused reads as paused until this returns.
     * <p>
     * Once the live delegate returns from the resume, this model is started again if a start whose live delegate threw
     * stopped it, so a subscription made afterwards replays at once. After {@link #stop()} it stays stopped.
     * <p>
     * {@link RepositionableSubscriptions#findIn} finds this model only when the live delegate is repositionable.
     *
     * @throws UnsupportedOperationException if the live delegate is not itself repositionable.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        return resumeSubscription(subscriptionId, startAt, List.of(this), () -> isRunning(subscriptionId) && !isPaused(subscriptionId));
    }

    /**
     * Resumes {@code subscriptionId} at {@code startAt} as {@link #resumeSubscription(String, StartAt)} describes, ending
     * a replay in the way on any of {@code models}, which are this model or every child of the dispatcher this model is
     * a child of. They share the claim, the live delegate and what runs a resume at a given position. {@code runs} tells
     * whether the subscription reads as running and not paused on the model the caller called.
     */
    Subscription resumeSubscription(String subscriptionId, StartAt startAt, List<AbstractCatchupSubscriptionModel> models, BooleanSupplier runs) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(liveDelegate)
                .orElseThrow(() -> new UnsupportedOperationException(liveDelegate.getClass().getSimpleName() + " is not repositionable"));
        try (Claim ignored = claim(subscriptionId)) {
            // Only one that runs, so an unknown or paused one reads as it did, also when this fails
            boolean marked = runs.getAsBoolean() && repositioning.add(subscriptionId);
            try {
                List<ReplayInTheWay> inTheWay = new ArrayList<>();
                try {
                    for (AbstractCatchupSubscriptionModel model : models) {
                        ReplayInTheWay replay = model.holdReplayToResume(subscriptionId);
                        if (replay != null) {
                            inTheWay.add(replay);
                        }
                    }
                } catch (RuntimeException | Error e) {
                    putBack(inTheWay, e);
                    throw e;
                }
                // Waited for with no handover lock held, since a held replay takes that lock once its action returns
                awaitTheActions(subscriptionId, inTheWay);
                // The position given here wins over the live start the last replay handed over, and over a takeover
                // the live delegate's loss of it asked for, which is asked for again when the live delegate throws
                HandedOver dropped = handedOver.remove(subscriptionId);
                // A held replay delivers nothing, and only a call holding the claim runs it again, so it ends only once
                // the live delegate has resumed and is put back when that throws
                Subscription resumed;
                try {
                    resumed = repositionable.resumeSubscription(subscriptionId, startAt);
                } catch (RuntimeException | Error e) {
                    if (dropped != null) {
                        handedOver.putIfAbsent(subscriptionId, dropped);
                    }
                    putBack(inTheWay, e);
                    throw e;
                }
                inTheWay.forEach(ReplayInTheWay::end);
                models.forEach(AbstractCatchupSubscriptionModel::liveDelegateResumed);
                return resumed;
            } finally {
                if (marked) {
                    repositioning.remove(subscriptionId);
                }
            }
        }
    }

    // Runs each replay in the way as it was before the resume at a given position held it, with what fails added to e
    private static void putBack(List<ReplayInTheWay> inTheWay, Throwable e) {
        try {
            runEach(inTheWay, ReplayInTheWay::putBack);
        } catch (RuntimeException | Error putBackFailure) {
            addSuppressed(e, putBackFailure);
        }
    }

    /**
     * Answers {@link RepositionableSubscriptions} only when the live delegate has it, since
     * {@link #resumeSubscription(String, StartAt)} resumes the subscription there, so this model is found where the live
     * delegate would be, and nothing is found where nothing was. Answers every other capability as any model does.
     */
    @Override
    public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
        if (type == RepositionableSubscriptions.class && !getWrappedSubscriptionModel().hasCapability(RepositionableSubscriptions.class)) {
            return Optional.empty();
        }
        return SubscriptionModel.super.capability(type);
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
     * delegate holds paused or a cancel is under way for, and whose start lets this model catch up. A resume while such
     * a replay runs returns the handle of that replay, also one that comes while the first resume decides to replay,
     * since both decide under the handover lock. So does a resume while a replay a subscribe started runs, since the
     * live delegate doesn't have the subscription yet.
     *
     * <p>
     * Only a call on the thread that cancels the subscription gets here while the cancel is under way, since that
     * thread holds the claim. It then throws whenever the stored position is one a catch-up replays from, whatever the
     * live delegate reports as paused. {@code whatChanged} says what the caller did before, which the exception repeats.
     *
     * @throws IllegalStateException when the subscription would replay but the live delegate can't resume a
     *                               subscription from a given position. It would resume from its own position, which can
     *                               lie past the history the replay reads, and the events in between would be lost.
     *                               Also when it would replay while a cancel of it is under way, and nothing is changed
     *                               then.
     */
    @Nullable Subscription replayToResume(String subscriptionId, String whatChanged) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            Subscription takenOver = takeOverIfAsked(subscriptionId);
            if (takenOver != null) {
                return takenOver;
            }
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            if (running != null && !running.abandoned()) {
                return new CatchupSubscription(subscriptionId, running.replay.result);
            }
            // After a cancel the live delegate failed, the subscription replays again, while the replay the cancel
            // ended may still be in the action
            ResumeReplay resumeReplay = replayThatResumeRuns(subscriptionId);
            // Not the handle of a cancelled replay, which never hands over
            if (resumeReplay == null) {
                return null;
            }
            // Not from inside a cancel of the subscription, on the thread that cancels, which alone holds the claim while
            // the cancel is under way. The live delegate can still cancel the subscription a replay would resume, and
            // handing the subscription to it instead resumes it past the stored position.
            if (cancelsUnderWay.containsKey(subscriptionId)) {
                throw new IllegalStateException("Cannot resume subscription " + subscriptionId + " while a cancel of it is under way, because it replays from the position a catch-up stored for it,"
                        + " and the subscription model would resume it past that position instead. Resume it once the cancel has returned. " + whatChanged);
            }
            repositionableLiveDelegate(subscriptionId);
            warnIfStoredWithoutLiveStart(subscriptionId, resumeReplay.from());
            CompletableFuture<Void> endedReplayDone = CompletableFuture.allOf(running == null ? CompletableFuture.completedFuture(null) : running.done, resumeReplay.cancelledReplayReturned());
            return new CatchupSubscription(subscriptionId, startCatchupAsync(subscriptionId, resumeReplay.replay(), false, true, endedReplayDone));
        }
    }

    private record ResumeReplay(CatchupReplay replay, Checkpoint from, CompletableFuture<Void> cancelledReplayReturned) {
    }

    // The replay a resume of subscriptionId runs before the live delegate resumes it, or null when it runs none. While a
    // cancel of it is under way the stored position alone decides, whatever the live delegate reports as paused.
    private @Nullable ResumeReplay replayThatResumeRuns(String subscriptionId) {
        Subscribed subscription = subscribed.get(subscriptionId);
        if (subscription == null || subscription.owner() != this || shuttingDown
                || (!cancelsUnderWay.containsKey(subscriptionId) && !getWrappedSubscriptionModel().isPaused(subscriptionId))
                || (subscription.startAt().isDynamic() && subscription.startAt().get(generateSubscriptionModelContext()) == null)) {
            return null;
        }
        Checkpoint stored = returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
        if (stored == null) {
            return null;
        }
        CatchupReplay replay = replayToResume(subscriptionId, subscription.filter(), subscription.startAt(), subscription.action(), stored);
        return replay == null ? null : new ResumeReplay(replay, stored, subscription.cancelledReplayReturned());
    }

    /**
     * The replay from {@code stored} that {@link #replayToResume(String, String)} describes, or {@code null} when
     * {@code stored} is not a position this model replays from. This model has none.
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

    // From registration until the live delegate has resumed the subscription, or the replay is held or ended. Also from
    // when the live delegate paused the subscription for a takeover until the takeover has started its replay.
    boolean isReplayingToResume(String subscriptionId) {
        CatchupAttempt running = currentAttempt.get(subscriptionId);
        return (running != null && running.replay.resuming && !running.abandoned()) || isTakingOver(subscriptionId);
    }

    // Whether the live delegate has paused the subscription for a takeover, on this model or another child of the same
    // dispatcher, and the takeover hasn't started its replay yet
    private boolean isTakingOver(String subscriptionId) {
        HandedOver made = handedOver.get(subscriptionId);
        return made != null && made.takingOver().get();
    }

    // Whether this model, rather than another child of the same dispatcher, runs or holds a replay for the subscription,
    // whether a subscribe or a resume started it, or is about to start one for a takeover. A cancelled replay that has
    // not returned yet doesn't count.
    boolean runsReplayHere(String subscriptionId) {
        CatchupAttempt running = currentAttempt.get(subscriptionId);
        HandedOver made = handedOver.get(subscriptionId);
        return (running != null && running.owner == this && !running.abandoned()) || parkedReplays.containsKey(subscriptionId)
                || (made != null && made.owner() == this && made.takingOver().get());
    }

    /**
     * Hands {@code subscriptionId} over to the live delegate once its replay has delivered the history.
     * <p>
     * A replay a subscribe started subscribes it there from {@code startAtToUse}, and pauses it there when a pause was
     * asked for during the replay.
     * <p>
     * A replay a resume started resumes the subscription the live delegate holds paused, from the position
     * {@code startAtToUse} resolves to, which stores the live start when the stored position is the catch-up's own.
     * That subscription keeps the action it was made with, so {@code liveConsumer} goes unused and an event the replay
     * delivered can arrive again. When a pause was asked for during the replay, the live delegate isn't resumed and
     * keeps the subscription paused, and the replay is held, so it runs again once the subscription is resumed. The
     * replay ends once the live delegate has resumed the subscription, so what throws before that, reading or saving
     * the position included, fails the replay, which then runs again as {@link #runAgainAfter} decides.
     * <p>
     * Called within {@link #handOverUnderLock}, on the virtual thread {@link #startCatchupAsync} started for this
     * attempt.
     */
    Subscription handOver(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAtToUse, Consumer<CloudEvent> liveConsumer) {
        CatchupAttempt attempt = Objects.requireNonNull(CURRENT_ATTEMPT.get());
        // Taken before the live delegate gets the subscription, since a takeover can follow its loss of the live start
        // before the call returns
        CompletableFuture<Subscription> live = attempt.replay.handle.live();
        if (attempt.replay.resuming) {
            if (pauseRequestedDuringCatchup.containsKey(subscriptionId)) {
                // Held before the pause is taken off, so isPaused stays true in between
                hold(subscriptionId, attempt);
                pauseRequestedDuringCatchup.remove(subscriptionId);
                return new CatchupSubscription(subscriptionId, attempt.replay.result);
            }
            Subscription resumed = resumeTheLiveDelegate(subscriptionId, startAtToUse, attempt.replay.takesOver);
            if (currentAttempt.remove(subscriptionId, attempt)) {
                runningCatchupSubscriptions.remove(subscriptionId);
            }
            live.complete(resumed);
            return attempt.replay.handle;
        }
        Subscription subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAtToUse, liveConsumer);
        attempt.wentLive = true;
        live.complete(subscription);
        applyPendingPauseIfAny(subscriptionId);
        return attempt.replay.handle;
    }

    /**
     * Runs {@code handover}, the handover of the calling attempt, under the handover lock of {@code subscriptionId}.
     * When it throws, what comes of the attempt is decided before the lock is released, as {@link #runAgainAfter}
     * decides, so no lifecycle call finds a replay whose handover failed and whose next step isn't decided yet. Only
     * meaningful on the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    Subscription handOverUnderLock(String subscriptionId, Supplier<Subscription> handover) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            try {
                return handover.get();
            } catch (RuntimeException | Error failure) {
                CatchupAttempt attempt = Objects.requireNonNull(CURRENT_ATTEMPT.get());
                if (attempt.handingOver) {
                    // Asked for the subscription the live delegate didn't get, so it goes with it. Still under the lock
                    // the pause was recorded under, so no later subscription for the id has asked for one yet.
                    pauseRequestedDuringCatchup.remove(subscriptionId);
                }
                attempt.decidedFailure = new DecidedFailure(failure, runAgainAfter(subscriptionId, attempt, failure));
                throw failure;
            }
        }
    }

    /**
     * What {@link #runAgainAfter} decided for {@code failure} while the handover lock was still held.
     */
    private record DecidedFailure(Throwable failure, @Nullable Duration backoff) {
    }

    // Repositioned, since a live delegate that reads no stored position resumes from where it last read, which can lie
    // past the history the replay read
    private Subscription resumeTheLiveDelegate(String subscriptionId, StartAt startAtToUse, boolean takesOver) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        StartAt liveStart = startAtToUse.get(new SubscriptionModelContext(liveDelegate.getClass()));
        Subscription resumed;
        if (!(liveStart instanceof StartAtCheckpoint)) {
            resumed = liveDelegate.resumeSubscription(subscriptionId);
        } else if (takesOver) {
            resumed = resumeWhereTheTakeoverGoesLive(subscriptionId, liveStart);
        } else {
            resumed = repositionableLiveDelegate(subscriptionId).resumeSubscription(subscriptionId, liveStart);
        }
        liveDelegateResumed();
        return resumed;
    }

    // A start(true) of the live delegate can resume the subscription where the live delegate lost the history until the
    // resume here returns, and it is paused again each time. Throws when the resume is refused while the live delegate
    // doesn't run the subscription.
    private Subscription resumeWhereTheTakeoverGoesLive(String subscriptionId, StartAt liveStart) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        RepositionableSubscriptions repositionable = repositionableLiveDelegate(subscriptionId);
        SubscriptionAlreadyRunningException resumedMeanwhile = null;
        while (true) {
            if (liveDelegate.isRunning(subscriptionId)) {
                try {
                    liveDelegate.pauseSubscription(subscriptionId);
                } catch (SubscriptionNotRunningException pausedOrEndedMeanwhile) {
                    // The resume below says which
                }
            } else if (resumedMeanwhile != null) {
                throw resumedMeanwhile;
            }
            try {
                return repositionable.resumeSubscription(subscriptionId, liveStart);
            } catch (SubscriptionAlreadyRunningException e) {
                resumedMeanwhile = e;
            }
        }
    }

    /**
     * The position the live delegate goes live from once a replay whose live start is the one in {@code handoverStart}
     * hands over, resolved each time the live delegate asks, so a retry picks up the latest checkpoint. With checkpoint
     * storage it stores the live start when nothing is stored, or when a catch-up of either kind stored a position or a
     * time, since the live delegate may not support either, and returns what is stored otherwise. Without checkpoint
     * storage it returns the live start. Without a live start it returns {@code startAt} when the live delegate must not
     * run, and the live delegate's default otherwise.
     * <p>
     * Each time it returns the live start, it remembers it as the one handed over. When the live delegate then loses
     * the history from exactly that live start as it opens, this model takes the subscription over and replays again,
     * as {@link #takeOverIfAsked} describes, instead of leaving the live delegate to restart it from the present or to
     * stop it. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected StartAt startAtToUse(String subscriptionId, StartAt startAt, @Nullable StartAt delegatedStartAt, ReplayStart<?> handoverStart) {
        Checkpoint liveStart = handoverStart.liveFrom();
        ReplayState replay = CURRENT_ATTEMPT.get().replay;
        Subscribed made = subscribed.get(subscriptionId);
        StartAt withoutLiveStart = delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
        Function<Checkpoint, StartAt> goLiveFrom = position -> {
            if (liveStart != null && takesOverLostLiveStarts() && made != null && made.owner() == this && position.asString().equals(liveStart.asString())
                    && subscribed.get(subscriptionId) == made) {
                handedOver.put(subscriptionId, new HandedOver(this, made, position, handoverStart.replayAgainFrom(position), replay, new AtomicBoolean(), new AtomicBoolean()));
            }
            return StartAt.checkpoint(position);
        };
        return StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                        cfg -> () -> {
                            Checkpoint position = cfg.storage().read(subscriptionId);
                            if ((position == null || isCatchupCheckpoint(position)) && liveStart != null) {
                                position = cfg.storage().save(subscriptionId, liveStart, writeConditionFor(cfg, subscriptionId));
                            } else if (position == null) {
                                return withoutLiveStart;
                            }
                            return goLiveFrom.apply(position);
                        })
                .orElse(() -> liveStart == null ? withoutLiveStart : goLiveFrom.apply(liveStart)));
    }

    // Whether the live delegate tells this model about lost history, and can resume a subscription at a live start a
    // takeover reads again
    private boolean takesOverLostLiveStarts() {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        return liveDelegate.hasCapability(HistoryLossReportingSubscriptions.class) && liveDelegate.hasCapability(RepositionableSubscriptions.class);
    }

    // Asked by the live delegate, on the thread that found the history lost and without its lock. Takes the
    // subscription over only when what was lost is exactly the live start the last replay handed over, so a loss after
    // the subscription read past it is left to the live delegate.
    private HistoryLossHandling takeOverIfHandedOverAt(String subscriptionId, Checkpoint lostFrom) {
        Subscribed subscription = subscribed.get(subscriptionId);
        // The live delegate resumed the subscription where it lost the history after this model took it over, as a
        // start(true) of it can. Paused again. A replay of this model that still runs resumes it at the live start it
        // reads, and otherwise it stays paused until it is resumed.
        if (subscription != null && subscription.owner() == this && !shuttingDown && lostFrom.equals(subscription.lostLiveStart().get())) {
            return HistoryLossHandling.takeOver(() -> {
                CatchupAttempt running = currentAttempt.get(subscriptionId);
                if (running == null || running.abandoned()) {
                    log.warn("The subscription model lost the history from live start {} of catch-up subscription {} again after the catch-up took it over, and paused it. No replay of the catch-up resumes it now. {}",
                            lostFrom.asString(), subscriptionId, whenNoReplayResumesIt(subscriptionId));
                }
            });
        }
        HandedOver made = handedOver.get(subscriptionId);
        if (made == null || made.owner() != this || shuttingDown || !made.liveStart().equals(lostFrom) || subscribed.get(subscriptionId) != made.subscribed()) {
            return HistoryLossHandling.leaveToSubscriptionModel();
        }
        // Before the live delegate pauses the subscription, so a resume that finds it paused before the takeover has
        // started replays instead of resuming the live delegate where it lost the history
        made.takeOverAsked().set(true);
        return HistoryLossHandling.takeOver(() -> {
            if (made.takingOver().compareAndSet(false, true)) {
                Thread.ofVirtual().name("occurrent-catchup-takeover-" + subscriptionId).start(() -> takeOver(subscriptionId, made));
            }
        });
    }

    // Under the claim, so no other call for the subscription runs in between. Tried again while the takeover is still
    // asked for, since nothing else would start its replay while the subscription reads as running
    private void takeOver(String subscriptionId, HandedOver made) {
        while (handedOver.get(subscriptionId) == made && !shuttingDown) {
            try (Claim ignored = claim(subscriptionId)) {
                takeOverIfAsked(subscriptionId);
                return;
            } catch (RuntimeException e) {
                log.error("The catch-up couldn't start replaying again for subscription {} after the subscription model lost the history from its live start, so it tries again in {} ms. {}",
                        subscriptionId, MAX_BACKOFF.toMillis(), whileTakenOver(subscriptionId), e);
            }
            try {
                Thread.sleep(MAX_BACKOFF);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("The catch-up was interrupted while it waited to replay again for subscription {}, so it replays again once the subscription is resumed. {}", subscriptionId, whileTakenOver(subscriptionId));
                return;
            }
        }
    }

    // Where the subscription is while a takeover keeps it, for a log. A start(true) of the live delegate can resume it
    // where the live delegate lost the history, which the live delegate then reports again.
    private String whileTakenOver(String subscriptionId) {
        return getWrappedSubscriptionModel().isRunning(subscriptionId)
                ? "The subscription model resumed the subscription where it lost the history, and pauses it again once it reports that loss."
                : "The subscription stays paused meanwhile.";
    }

    // Where the subscription is once no replay of this model resumes it, and what resumes it, for a log
    private String whenNoReplayResumesIt(String subscriptionId) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        if (hasParkedReplay(subscriptionId)) {
            return "The subscription stays paused until it is resumed or start(true) is called on this model, which runs the replay it holds again.";
        } else if (liveDelegate.isRunning(subscriptionId)) {
            return "The subscription model runs the subscription.";
        } else if (liveDelegate.isPaused(subscriptionId)) {
            return "The subscription model holds the subscription paused until it is resumed. Resuming it through the catch-up replays from the position stored for it when that is one the catch-up replays from, and otherwise resumes it where the subscription model holds it. If the subscription model loses that position again, cancel the subscription and subscribe it again.";
        }
        return "The subscription model neither runs the subscription nor holds it paused.";
    }

    /**
     * Starts the replay a takeover asked for, once the live delegate has paused the subscription after it lost the
     * history from the live start a replay on this model handed over, and returns its handle. Returns {@code null} when
     * no takeover waits on this model, and when the takeover keeps the subscription paused. Called with the
     * {@link #claim} held. A resume that comes once this model has answered that it takes the subscription over and the
     * live delegate holds it paused starts the replay itself, also before the live delegate has told this model it
     * paused the subscription, so the live delegate never resumes it where it lost the history.
     * <p>
     * The replay runs as one a resume starts. It replays from the position or time the replay that handed over started
     * from, and the live delegate resumes the subscription it holds paused at a live start read again. The checkpoint the
     * replay starts from is stored first, so a restart in between replays as well. A pause asked for since the live
     * delegate paused the subscription keeps it paused, and the next resume replays from what is stored then, or,
     * without checkpoint storage, runs this replay. It counts towards the {@value #MAX_REPLAYS_AGAIN} times a replay
     * runs again, as {@link #replayUntilLiveStartHolds} describes.
     */
    private @Nullable Subscription takeOverIfAsked(String subscriptionId) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            HandedOver made = handedOver.get(subscriptionId);
            if (made == null || made.owner() != this
                || !(made.takingOver().get() || made.takeOverAsked().get() && getWrappedSubscriptionModel().isPaused(subscriptionId))) {
                return null;
            }
            Subscribed subscription = made.subscribed();
            if (shuttingDown || subscribed.get(subscriptionId) != subscription) {
                handedOver.remove(subscriptionId, made);
                return null;
            }
            boolean stored = this.<Boolean, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                cfg.storage().save(subscriptionId, made.replayAgainFrom(), writeConditionFor(cfg, subscriptionId));
                return true;
            }).orElse(false);
            // Before the record goes, so a loss of the live start reported again is claimed at every moment
            subscription.lostLiveStart().set(made.liveStart());
            handedOver.remove(subscriptionId, made);
            boolean pauseAsked = pauseRequestedDuringCatchup.remove(subscriptionId) != null;
            if (pauseAsked && stored) {
                log.warn("The subscription model lost the history from the live start that catch-up subscription {} went live from, and it was paused meanwhile, so it replays again from \"{}\" once it is resumed. {} Live start: {}",
                        subscriptionId, made.replayAgainFrom().asString(), whileTakenOver(subscriptionId), made.liveStart().asString());
                return null;
            }
            CatchupReplay replay = replayToResume(subscriptionId, subscription.filter(), subscription.startAt(), subscription.action(), made.replayAgainFrom());
            if (replay == null) {
                log.error("The subscription model lost the history from the live start that catch-up subscription {} went live from, but the catch-up can't replay from \"{}\". {} Live start: {}",
                        subscriptionId, made.replayAgainFrom().asString(), whenNoReplayResumesIt(subscriptionId), made.liveStart().asString());
                return null;
            }
            log.warn("The subscription model lost the history from the live start that catch-up subscription {} went live from, so the catch-up replays again from \"{}\" and redelivers the events in between. Live start: {}",
                    subscriptionId, made.replayAgainFrom().asString(), made.liveStart().asString());
            return new CatchupSubscription(subscriptionId, startCatchupAsync(subscriptionId, replay, pauseAsked, true, subscription.cancelledReplayReturned(), made));
        }
    }

    /**
     * Pauses {@code subscriptionId}, under its {@link #claim}. Takes the handover lock, so a pause that comes while a
     * finishing replay hands over waits until the handover has gone live, failed, or held the replay.
     * <p>
     * While a replay a subscribe started still runs, the subscription is paused once the live delegate has it. While a
     * replay a resume started still runs, the replay is held when it hands over, and the live delegate keeps the
     * subscription paused. When that replay waits to run again after it failed, it is held at once. A held replay runs
     * again once the subscription is resumed. It reads as paused, so pausing it throws
     * {@link SubscriptionNotRunningException}.
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        try (Claim ignored = claim(subscriptionId)) {
            pauseHoldingTheClaim(subscriptionId);
        }
    }

    private void pauseHoldingTheClaim(String subscriptionId) {
        HandoverLock lock = tryLockHandover(subscriptionId);
        try {
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            if (running != null && running.backingOff && !running.abandoned()) {
                hold(subscriptionId, running);
            } else if (parkedReplays.containsKey(subscriptionId)) {
                // Refused here, since the live delegate doesn't have the subscription yet when a subscribe started the
                // replay, and would throw UnknownSubscriptionException
                throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
            } else if (runningCatchupSubscriptions.containsKey(subscriptionId)) {
                // Recorded for the handover, or for runAgainAfter when the replay fails first. The replay itself keeps
                // running until the handover since interrupting and resuming it would require persisting the exact
                // replay cursor, which this class does not do.
                pauseRequestedDuringCatchup.put(subscriptionId, true);
            } else if (isTakingOver(subscriptionId)) {
                // The live delegate holds the subscription paused already, and the takeover keeps it paused once it
                // finds the pause
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
        // Set once the live delegate has the subscription of a replay a subscribe started. Read and written only on
        // this attempt's own thread, as is decidedFailure.
        private boolean wentLive = false;
        // Set once a replay a subscribe started gives up the id to hand the subscription to the live delegate, while its
        // handover holds the handover lock. Read and written only on this attempt's own thread.
        private boolean handingOver = false;
        private @Nullable DecidedFailure decidedFailure;
        private final String subscriptionId;
        private final AbstractCatchupSubscriptionModel owner;
        private final ReplayState replay;
        // Done once the thread of every earlier attempt at this replay has returned, so none of them is still inside
        // the subscriber's action
        private final CompletableFuture<Void> earlierAttemptsDone;
        // Done once this attempt's thread and every earlier attempt's thread have returned
        private final CompletableFuture<Void> done = new CompletableFuture<>();

        private CatchupAttempt(String subscriptionId, AbstractCatchupSubscriptionModel owner, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone) {
            this.subscriptionId = subscriptionId;
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
        // Whether a takeover started it, so a start(true) of the live delegate can have resumed the subscription where
        // the live delegate lost the history by the time it hands over
        private final boolean takesOver;

        // How many attempts at a replay a resume started have failed in a row
        private int failures = 0;
        // What its handover returns, which follows the subscription through a takeover. The handle of the replay before
        // it when a takeover started it.
        private final LiveHandle handle;
        // How many times the replays before it ran again from their origin, counting the takeover, which the next attempt
        // at it counts on from. Read and set to zero on the attempt's own thread, once each attempt at it has returned.
        private volatile int replaysAgainBefore;
        // How many times it ran again from its origin before it handed over, counting replaysAgainBefore
        private volatile int replaysAgain;

        private ReplayState(String subscriptionId, CatchupReplay catchup, CompletableFuture<Subscription> result, boolean resuming, @Nullable HandedOver takenOver,
                            boolean takeoversFollow) {
            this.catchup = catchup;
            this.result = result;
            this.resuming = resuming;
            this.takesOver = takenOver != null;
            this.handle = takenOver == null ? new LiveHandle(subscriptionId, takeoversFollow) : takenOver.replay().handle;
            this.replaysAgainBefore = takenOver == null ? 0 : takenOver.replay().replaysAgain + 1;
        }

        // FIRST_BACKOFF, doubled with each failure in a row up to MAX_BACKOFF. Called with the handover lock held.
        private Duration backoffAfterFailure() {
            failures++;
            long millis = FIRST_BACKOFF.toMillis() << Math.min(failures - 1, 16);
            return Duration.ofMillis(Math.min(millis, MAX_BACKOFF.toMillis()));
        }
    }

    /**
     * The handle a replay's handover returns. It reports the start of the subscription the live delegate got, and once a
     * takeover replays again after the live delegate lost the history from the live start, the start of the
     * subscription the takeover's replay hands over to instead. It returns {@code false} when that replay ends without
     * handing over, as one a cancel ends does, and throws what that replay failed with otherwise, as
     * {@link CatchupSubscription} does.
     */
    static final class LiveHandle implements Subscription {
        // How long a wait asks the subscription it follows at a time, so it notices a takeover in between
        private static final Duration FOLLOW_SLICE = Duration.ofMillis(EARLIER_ATTEMPT_POLL_MILLIS);
        // Longer than any wait that ends, so a wait at least this long never ends on its own
        private static final Duration FOREVER = Duration.ofNanos(Long.MAX_VALUE / 2);
        private final String id;
        // False when the live delegate can't have a takeover follow, so a wait gives it the caller's whole timeout
        private final boolean takeoversFollow;
        // Completed with the subscription the live delegate got, or with what ended the takeover's replay
        private volatile CompletableFuture<Subscription> live = new CompletableFuture<>();

        private LiveHandle(String id, boolean takeoversFollow) {
            this.id = id;
            this.takeoversFollow = takeoversFollow;
        }

        CompletableFuture<Subscription> live() {
            return live;
        }

        // Follows the replay a takeover started, whose handover completes what live() returns from now on. When the
        // replay ends without handing over, such as one a cancel ends with a subscription that never starts, so does
        // this handle.
        void follow(CompletableFuture<Subscription> takeoverResult) {
            CompletableFuture<Subscription> next = new CompletableFuture<>();
            live = next;
            takeoverResult.whenComplete((ended, failure) -> {
                if (failure instanceof CancellationException) {
                    next.cancel(false);
                } else if (failure != null) {
                    next.completeExceptionally(failure);
                } else if (ended != this) {
                    next.complete(ended);
                }
            });
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            if (!takeoversFollow) {
                return new CatchupSubscription(id, live).waitUntilStarted(timeout);
            }
            boolean forever = timeout.compareTo(FOREVER) >= 0;
            long deadline = forever ? 0 : System.nanoTime() + timeout.toNanos();
            while (true) {
                CompletableFuture<Subscription> following = live;
                long sliceStarted = System.nanoTime();
                long slice = forever ? FOLLOW_SLICE.toNanos() : Math.max(0, Math.min(FOLLOW_SLICE.toNanos(), deadline - sliceStarted));
                try {
                    Subscription subscription = following.get(slice, TimeUnit.NANOSECONDS);
                    long left = Math.max(0, slice - (System.nanoTime() - sliceStarted));
                    long waitStarted = System.nanoTime();
                    if (subscription.waitUntilStarted(Duration.ofNanos(left))) {
                        return true;
                    }
                    // One that answers false long before the time it was given has passed, such as the handle of a
                    // cancelled replay, won't start, unless a takeover follows it
                    if (following == live && left > 0 && System.nanoTime() - waitStarted < left / 2) {
                        return false;
                    }
                } catch (TimeoutException e) {
                    // Asked again in the next slice
                } catch (CancellationException e) {
                    if (following == live) {
                        return false;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    if (following == live) {
                        Throwable cause = e.getCause();
                        if (cause instanceof RuntimeException runtimeException) {
                            throw runtimeException;
                        } else if (cause instanceof Error error) {
                            throw error;
                        }
                        throw new IllegalStateException("The catch-up for subscription '" + id + "' failed", cause);
                    }
                }
                if (!forever && System.nanoTime() - deadline >= 0) {
                    return false;
                }
            }
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
        attempt.handingOver = !attempt.abandoned();
        return attempt.handingOver;
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
     * Runs every replay this model has parked again, or parks it once more if this model is stopped by then. Each one
     * under its {@link #claim}, and a failure to claim one is thrown once the others have run.
     */
    public void relaunchParkedReplays() {
        runEach(List.copyOf(parkedReplays.keySet()), subscriptionId -> {
            try (Claim ignored = claim(subscriptionId, STARTED_BUT_NOT_RUN_AGAIN)) {
                relaunchParkedReplay(subscriptionId);
            }
        });
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
        RunAgain runAgain = runParkedReplayAgain(subscriptionId, resuming);
        return runAgain == null ? null : new CatchupSubscription(subscriptionId, runAgain.replay().result);
    }

    /**
     * Runs {@code subscriptionId}'s parked replay again as {@link #relaunchParkedReplay(String, boolean)} does, and
     * returns it with whether it runs, or {@code null} when this model has no parked replay for it. It stays parked when
     * this model is stopped by then or the {@link CatchupListener} throws as it starts.
     */
    private @Nullable RunAgain runParkedReplayAgain(String subscriptionId, boolean resuming) {
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
        return new RunAgain(parked.replay(), attempt != null);
    }

    private record RunAgain(ReplayState replay, boolean runs) {
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
     * A held {@link #claim}. {@code close()} releases it and declares no checked exception.
     */
    interface Claim extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Claims {@code subscriptionId} for one lifecycle call, held until the call returns, so only one lifecycle call for
     * the id runs at a time, on this model and on every other child of the same dispatcher. A second call waits for the
     * first to return and then acts on what it left. The thread that holds the claim can take it again.
     * <p>
     * A call from inside the subscriber's action, on the thread of a replay for {@code subscriptionId}, doesn't wait.
     * It goes ahead when no other call holds the claim, and throws {@link IllegalStateException} at once when one does,
     * since that call can be waiting for the action to return. Any other call waits at most {@link #LIFECYCLE_WAIT},
     * and then logs at {@code ERROR} and throws {@link IllegalStateException}. An interrupt doesn't end the wait, and is
     * set again once the claim is taken or the wait is over.
     * <p>
     * The caller changes nothing for the id once the claim fails. {@code whatChanged} says what it did before it asked
     * for the claim, which the log and the exception repeat. That is nothing, apart from a {@link #stop()}, which has
     * stopped this model, a {@link #resumeSubscription(String)} of a stopped model, which has started it, and a
     * {@code start(true)}, which has started this model.
     */
    Claim claim(String subscriptionId, String whatChanged) {
        ReentrantLock claim = useClaimLock(subscriptionId);
        try {
            if (claim.isHeldByCurrentThread()) {
                claim.lock();
            } else if (replaysOnTheCallingThread(subscriptionId)) {
                if (!claim.tryLock()) {
                    throw new IllegalStateException("Cannot change subscription " + subscriptionId + " from inside its own action while its catch-up replay runs and another call for it is under way,"
                            + " because that call can be waiting for the action to return. Change it from another thread. " + whatChanged);
                }
            } else if (!tryLockWithin(claim, LIFECYCLE_WAIT)) {
                log.error("A call for subscription {} waited {} ms for another call for it to return, and gives up. {}", subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
                throw new IllegalStateException("Cannot change subscription " + subscriptionId + ", because another call for it didn't return within " + LIFECYCLE_WAIT.toMillis()
                        + " ms. " + whatChanged);
            }
        } catch (RuntimeException | Error e) {
            releaseClaimLock(subscriptionId);
            throw e;
        }
        return () -> {
            claim.unlock();
            releaseClaimLock(subscriptionId);
        };
    }

    /**
     * Claims {@code subscriptionId} as {@link #claim(String, String)} does, for a call that has changed nothing before it.
     */
    Claim claim(String subscriptionId) {
        return claim(subscriptionId, NOTHING_CHANGED);
    }

    // What a call whose claim fails did before it asked for the claim
    static final String NOTHING_CHANGED = "Nothing was changed.";
    static final String STARTED_BUT_NOT_RESUMED = "The model was stopped and is started, as a resume starts a stopped model, but the subscription wasn't resumed.";
    static final String STARTED_BUT_NOT_RUN_AGAIN = "The catch-up replay held for the subscription wasn't run again. The call doesn't stop a model it started, and it went on to the other subscriptions first.";
    static final String STOPPED_ALL_THE_SAME = "The model is stopped all the same, and a catch-up replay for the subscription delivers no further event until start(true) or resumeSubscription runs it again.";

    // The lock behind the claim of subscriptionId, counting the caller as one that holds it or waits for it until it calls
    // releaseClaimLock
    private ReentrantLock useClaimLock(String subscriptionId) {
        return lifecycleClaims.compute(subscriptionId, (id, used) -> {
            if (used == null) {
                return new ClaimLock();
            }
            used.users++;
            return used;
        }).lock;
    }

    // Removes the claim's entry once no thread holds it or waits for it
    private void releaseClaimLock(String subscriptionId) {
        lifecycleClaims.computeIfPresent(subscriptionId, (id, used) -> --used.users == 0 ? null : used);
    }

    // ReentrantLock.tryLock(timeout) that an interrupt doesn't end. The interrupt is set again once the wait is over.
    private static boolean tryLockWithin(ReentrantLock lock, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return lock.tryLock(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
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
        public Checkpoint replayAgainFrom(Checkpoint lostLiveStart) {
            return GlobalCheckpoint.of(replayOrigin, lostLiveStart, replayOrigin, replayOrigin);
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
        public Checkpoint replayAgainFrom(Checkpoint lostLiveStart) {
            return CatchupTimeCheckpoint.of(replayOrigin.asString(), lostLiveStart, replayOrigin.asString());
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
         * The checkpoint a replay stores before it replays again because the live delegate lost the history from
         * {@code lostLiveStart} as it opened. It holds the lost live start, so a resume from it replays from where the
         * first attempt at this replay started, with a live start read then.
         */
        Checkpoint replayAgainFrom(Checkpoint lostLiveStart);

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
     * it replays again more than once. The check and the handover are two calls, so a live start can still leave the
     * history after the check. A wrapped model that reports lost history and repositions subscriptions, such as the
     * MongoDB ones, asks this model what to do when it loses that live start as it opens, and this model then takes the
     * subscription over and replays again, as {@link #startAtToUse} describes. Any other wrapped model handles the lost
     * history as it would without this model. A replay that was stopped,
     * cancelled or taken over is not asked about, and its start is returned as it is for the handover to deal with.
     * <p>
     * A {@link CatchupListener} is told the catch-up started again before each replay run again, since what that
     * replay delivers is history read again and not events written since the catch-up started. Only meaningful on the
     * virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected <S extends ReplayStart<S>> S replayUntilLiveStartHolds(String subscriptionId, S first, @Nullable StartAt delegatedStartAt, Consumer<S> replay) {
        ReplayState state = CURRENT_ATTEMPT.get().replay;
        // Counted on from the replays before a takeover, and from zero by an attempt that runs again after a failure
        int replaysAgain = state.replaysAgainBefore;
        state.replaysAgainBefore = 0;
        if (replaysAgain > MAX_REPLAYS_AGAIN) {
            throw liveStartLostEachTime(subscriptionId, replaysAgain, first.liveFrom());
        }
        S replayed = first;
        replay.accept(replayed);
        for (; ; replaysAgain++) {
            S again = replayAgainIfLiveStartLost(subscriptionId, replayed, delegatedStartAt, replaysAgain);
            if (again == null) {
                state.replaysAgain = replaysAgain;
                return replayed;
            }
            replayed = again;
            replay.accept(replayed);
        }
    }

    private static IllegalStateException liveStartLostEachTime(String subscriptionId, int replays, @Nullable Checkpoint lastLiveStart) {
        return new IllegalStateException("Cannot hand catch-up subscription " + subscriptionId + " over to live delivery, because the subscription model lost the live start during or right after each of its "
                + replays + " replays. Size the change stream history, such as the MongoDB oplog, so that it outlasts the longest replay. Last live start: "
                + (lastLiveStart == null ? "none" : lastLiveStart.asString()));
    }

    // Null when the wrapped model can still resume from the live start, or when this attempt should no longer replay
    private <S extends ReplayStart<S>> @Nullable S replayAgainIfLiveStartLost(String subscriptionId, S replayed, @Nullable StartAt delegatedStartAt, int replaysAgain) {
        Checkpoint liveFrom = replayed.liveFrom();
        if (liveFrom == null || !shouldKeepReplaying(subscriptionId) || subscriptionModel.canResumeFrom(liveFrom)) {
            return null;
        }
        if (replaysAgain >= MAX_REPLAYS_AGAIN) {
            throw liveStartLostEachTime(subscriptionId, replaysAgain + 1, liveFrom);
        }
        log.warn("The live start of catch-up subscription {} left the subscription model's history during the replay, so the catch-up replays again from {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayed.describeOrigin(), liveFrom.asString());
        S again = replayed.fromOrigin(captureLiveResumeCheckpoint(delegatedStartAt));
        // Told only while this attempt still owns the id, as on registration, so an attempt that lost the id cannot
        // reset what its replacement told the listener. One the listener fails is held, and hands nothing over, as a
        // stopped one does.
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            if (currentAttempt.get(subscriptionId) != attempt) {
                return null;
            }
            if (!tellCatchupStarted(subscriptionId, attempt)) {
                hold(subscriptionId, attempt);
                return null;
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
     * Cancels the catch-up of {@code subscriptionId} and the subscription in the live delegate, and deletes its stored
     * position. Returns without waiting for a replay that is in the subscriber's action, which stores nothing more once
     * it returns. While this runs, {@link #start(boolean) start(true)} doesn't let the live delegate resume the
     * subscription itself.
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        try (Claim ignored = claim(subscriptionId)) {
            boolean nothingKept = cancelKeepingWhatWasSubscribedOnFailure(subscriptionId, () -> {
                cancelRunningCatchup(subscriptionId);
                subscriptionModel.cancelSubscription(subscriptionId);
            });
            if (nothingKept) {
                deletePositionFromStorage(subscriptionId);
            }
        }
    }

    /**
     * Runs {@code cancel}, which ends the catch-up of {@code subscriptionId} and then cancels the subscription in the
     * live delegate, and forgets the subscription as it was made only once {@code cancel} has returned. When
     * {@code cancel} throws, the live delegate can still hold the subscription paused, and a later
     * {@link #resumeSubscription(String)} replays from the position stored for it instead of leaving the live delegate
     * to resume from its own. A replay the cancel ended doesn't run again. From the moment the cancel starts until the
     * thread of that replay has returned, no resume of the subscription and no {@link #start(boolean) start(true)} lets
     * the action run, whether the cancel succeeds or throws, also when a stop held the replay before the cancel or takes
     * it away after. Called with the {@link #claim} held.
     *
     * @return whether nothing is kept for the subscription once {@code cancel} has returned, so its stored position can
     * be deleted. False when a subscribe for the same id on this thread, made while {@code cancel} ran, is kept.
     */
    boolean cancelKeepingWhatWasSubscribedOnFailure(String subscriptionId, Runnable cancel) {
        Subscribed made = subscribed.get(subscriptionId);
        CancelUnderWay underWay = new CancelUnderWay(made == null ? CompletableFuture.completedFuture(null) : made.cancelledReplayReturned());
        // Before the cancel takes the replay away, so start(true) and every resume find it all through the cancel. A
        // cancel the live delegate's cancel makes for the same id on this thread adds to the one already under way.
        CancelUnderWay outer = cancelsUnderWay.putIfAbsent(subscriptionId, underWay);
        CancelUnderWay current = outer == null ? underWay : outer;
        // Once the cancel is in cancelsUnderWay and before it changes anything else, which anyResumeReplays relies on
        cancelsStarted.incrementAndGet();
        try {
            cancel.run();
            // Only now, so start(true) finds the subscription through the cancel for as long as the live delegate can
            // still hold it paused. Only what this cancel started with or what a cancel that threw kept, never what a
            // subscribe the live delegate's cancel made for the same id on this thread put.
            return subscribed.computeIfPresent(subscriptionId, (id, now) -> now == made || now == current.kept ? null : now) == null;
        } catch (RuntimeException | Error e) {
            if (made != null) {
                // Before the cancel under way is removed, so one of them always has the replay. Kept in place, also when
                // a cancel the live delegate's cancel made for the same id on this thread went through.
                Subscribed kept = new Subscribed(made.owner(), made.filter(), made.startAt(), made.action(), current.replaysReturned, made.lostLiveStart());
                if (subscribed.compute(subscriptionId, (id, now) -> now == null || now == made ? kept : now) == kept) {
                    current.kept = kept;
                }
            }
            throw e;
        } finally {
            if (outer == null) {
                cancelsUnderWay.remove(subscriptionId, underWay);
            }
        }
    }

    // Done once the thread of every attempt at the replay that runs or is held for subscriptionId on this model has
    // returned
    private CompletableFuture<Void> replayReturned(String subscriptionId) {
        CatchupAttempt running = currentAttempt.get(subscriptionId);
        ParkedReplay parked = parkedReplays.get(subscriptionId);
        return CompletableFuture.allOf(running == null ? CompletableFuture.completedFuture(null) : running.done,
                parked == null ? CompletableFuture.completedFuture(null) : parked.earlierAttemptsDone());
    }

    /**
     * Cancel a catch-up running for {@code subscriptionId}. A no-op if this class has no catch-up running for that id
     * (for example because it belongs to the other path in a dual-mode dispatcher). Does not touch the shared live
     * delegate or position storage, which the dispatcher owns since both paths share the same delegate. What was kept
     * for the subscription as it was made stays too, and {@link #cancelSubscription(String)} forgets it once the live
     * delegate has cancelled the subscription.
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
            // Read under the lock that a replay run again registers under, so an attempt that runs the replay again
            // just before this is waited for too
            CancelUnderWay underWay = cancelsUnderWay.get(subscriptionId);
            if (underWay != null) {
                underWay.add(replayReturned(subscriptionId));
            }
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
            // So a loss of the live start it handed over is left to the live delegate, which cancels the subscription
            // next, and a takeover asked for already starts no replay
            handedOver.computeIfPresent(subscriptionId, (id, made) -> made.owner() == this ? null : made);
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
     * Takes a replay a resume started for {@code subscriptionId} away from its attempt and holds it, when it runs on this
     * model, so it delivers nothing more, and returns it, or one already held on this model, as a replay in the way of a
     * resume at a position the caller gives. A replay that a cancel that threw ended for a subscription made through this
     * model, or that a cancel still under way ended, which nothing runs again, is returned as one to wait for. Returns
     * {@code null} when there is none. Called with the {@link #claim} held, so no other lifecycle call can run the held
     * replay again before the caller ends it or puts it back.
     *
     * @throws IllegalStateException when called from inside the subscriber's action, on the thread of a replay for
     *                               {@code subscriptionId}, while there is a replay in the way. Nothing is changed then,
     *                               since the wait for the action would never end.
     */
    @Nullable ReplayInTheWay holdReplayToResume(String subscriptionId) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            if (running != null && (running.owner != this || !running.replay.resuming)) {
                running = null;
            }
            ParkedReplay parked = parkedReplays.get(subscriptionId);
            if (parked != null && !parked.replay().resuming) {
                parked = null;
            }
            // A stop takes a cancelled replay out of currentAttempt and parkedReplays while it can still be in the action
            CompletableFuture<Void> cancelledReplayReturned = cancelledReplayReturned(subscriptionId);
            if (running == null && parked == null && cancelledReplayReturned.isDone()) {
                return null;
            }
            if (replaysOnTheCallingThread(subscriptionId)) {
                throw new IllegalStateException("Cannot resume subscription " + subscriptionId + " from a given position from inside its own action while its catch-up replay runs,"
                        + " because the subscription model would deliver events to the action while the replay is still in it. Resume it from another thread.");
            }
            // A cancelled attempt is only waited for, since nothing is to run it again
            CompletableFuture<Void> cancelledAttemptDone = running == null || !running.abandoned() ? CompletableFuture.completedFuture(null) : running.done;
            boolean heldByTheResume = running != null && !running.abandoned() && hold(subscriptionId, running);
            if (heldByTheResume) {
                parked = parkedReplays.get(subscriptionId);
            }
            return new ReplayInTheWay(subscriptionId, parked, heldByTheResume, CompletableFuture.allOf(cancelledAttemptDone, cancelledReplayReturned,
                    parked == null ? CompletableFuture.completedFuture(null) : parked.earlierAttemptsDone()));
        }
    }

    // Done once the thread of every replay has returned that a cancel under way ended for subscriptionId, or a cancel that
    // threw ended for a subscription made through this model
    private CompletableFuture<Void> cancelledReplayReturned(String subscriptionId) {
        Subscribed made = subscribed.get(subscriptionId);
        return CompletableFuture.allOf(made == null || made.owner() != this ? CompletableFuture.completedFuture(null) : made.cancelledReplayReturned(),
                replaysACancelUnderWayEnded(subscriptionId));
    }

    // Done once the thread of every replay a cancel under way ended for subscriptionId has returned, and done when no
    // cancel for it is under way
    private CompletableFuture<Void> replaysACancelUnderWayEnded(String subscriptionId) {
        CancelUnderWay underWay = cancelsUnderWay.get(subscriptionId);
        return underWay == null ? CompletableFuture.completedFuture(null) : underWay.replaysReturned;
    }

    /**
     * A replay a resume started that {@link #holdReplayToResume} found in the way of a resume at a given position.
     * {@code actionReturned} completes once the thread of every attempt at it has returned.
     */
    final class ReplayInTheWay {
        private final String subscriptionId;
        private final @Nullable ParkedReplay held;
        private final boolean heldByTheResume;
        private final CompletableFuture<Void> actionReturned;

        private ReplayInTheWay(String subscriptionId, @Nullable ParkedReplay held, boolean heldByTheResume, CompletableFuture<Void> actionReturned) {
            this.subscriptionId = subscriptionId;
            this.held = held;
            this.heldByTheResume = heldByTheResume;
            this.actionReturned = actionReturned;
        }

        // The replay stores nothing more and doesn't hand over, and waitUntilStarted on its subscriber's handle returns
        // false. What is kept for the subscription stays, so a later resumeSubscription(id) can replay again.
        void end() {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                if (held != null && parkedReplays.remove(subscriptionId, held)) {
                    held.replay().cancelled = true;
                    held.replay().result.cancel(false);
                }
                if (heldByTheResume) {
                    pauseRequestedDuringCatchup.remove(subscriptionId);
                }
            }
        }

        // Runs a replay the resume held again, so it is as it was before, and returns whether it is. Its next attempt
        // waits on its own thread for the thread of the attempt the resume held to return. It stays held when this model
        // is stopped by then or the CatchupListener throws as it starts.
        boolean putBack() {
            if (!heldByTheResume) {
                return true;
            }
            RunAgain runAgain = runParkedReplayAgain(subscriptionId, false);
            return runAgain != null && runAgain.runs();
        }
    }

    /**
     * Waits at most {@link #LIFECYCLE_WAIT} until every replay in {@code inTheWay} has returned. When one hasn't, puts
     * each of them back as it was, logs at {@code ERROR} and throws {@link IllegalStateException}. The exception says
     * nothing was changed only when each of them runs as it did before. It says so when one is still held, because this
     * model was stopped or the {@link CatchupListener} threw as it started, and when one couldn't be run again, with what
     * putting it back threw added as suppressed.
     */
    static void awaitTheActions(String subscriptionId, List<ReplayInTheWay> inTheWay) {
        CompletableFuture<Void> actionsReturned = CompletableFuture.allOf(inTheWay.stream().map(replay -> replay.actionReturned).toArray(CompletableFuture[]::new));
        if (completesWithin(actionsReturned, LIFECYCLE_WAIT)) {
            return;
        }
        Throwable putBackFailure = null;
        AtomicBoolean allRunAsBefore = new AtomicBoolean(true);
        try {
            runEach(inTheWay, replay -> {
                if (!replay.putBack()) {
                    allRunAsBefore.set(false);
                }
            });
        } catch (RuntimeException | Error e) {
            putBackFailure = e;
        }
        String whatChanged = putBackFailure != null ? "A catch-up replay this call held for the subscription couldn't be run again, and the suppressed exception says why."
                : !allRunAsBefore.get() ? "A catch-up replay this call held for the subscription is still held, because the model was stopped or the catch-up listener threw as the replay started again."
                + " It runs again once the subscription is resumed or the model started."
                : NOTHING_CHANGED;
        IllegalStateException timedOut = new IllegalStateException("Cannot resume subscription " + subscriptionId + " from the given position, because its catch-up replay didn't return from the action, the history read or the check of its live start within "
                + LIFECYCLE_WAIT.toMillis() + " ms. " + whatChanged);
        if (putBackFailure != null) {
            timedOut.addSuppressed(putBackFailure);
        }
        log.error("Subscription {} was not resumed from the given position, because its catch-up replay didn't return from the action, the history read or the check of its live start within {} ms. {}",
                subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
        throw timedOut;
    }

    /**
     * Waits at most {@link #LIFECYCLE_WAIT} until every replay has returned that a cancel that threw, or a cancel still
     * under way, ended for {@code subscriptionId}, before the live delegate resumes the subscription without a replay.
     * When one hasn't, logs at {@code ERROR} and throws {@link IllegalStateException}, as it does at once when called
     * from inside the action. {@code whatChanged} says what the caller did before, which the log and the exception
     * repeat.
     */
    void awaitTheCancelledReplay(String subscriptionId, String whatChanged) {
        Subscribed made = subscribed.get(subscriptionId);
        CompletableFuture<Void> cancelledReplayReturned = CompletableFuture.allOf(made == null ? CompletableFuture.completedFuture(null) : made.cancelledReplayReturned(),
                replaysACancelUnderWayEnded(subscriptionId));
        if (cancelledReplayReturned.isDone()) {
            return;
        }
        if (replaysOnTheCallingThread(subscriptionId)) {
            throw new IllegalStateException("Cannot resume subscription " + subscriptionId + " from inside its own action while a cancelled catch-up replay is in it,"
                    + " because the subscription model would deliver events to the action while the replay is still in it. Resume it from another thread. " + whatChanged);
        }
        if (!completesWithin(cancelledReplayReturned, LIFECYCLE_WAIT)) {
            log.error("Subscription {} was not resumed, because its cancelled catch-up replay didn't return from the action, the history read or the check of its live start within {} ms. {}",
                    subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
            throw new IllegalStateException("Cannot resume subscription " + subscriptionId + ", because its cancelled catch-up replay didn't return from the action, the history read or the check of its live start within "
                    + LIFECYCLE_WAIT.toMillis() + " ms. " + whatChanged);
        }
    }

    // Whether done, which only completes normally, completes within timeout. An interrupt doesn't end the wait, and is
    // set again once the wait is over.
    private static boolean completesWithin(CompletableFuture<Void> done, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    done.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                    return true;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    return true;
                } catch (TimeoutException e) {
                    return false;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Whether the calling thread runs a replay for subscriptionId. Such a thread can be inside the action, so a wait
    // there for that replay would never end
    static boolean replaysOnTheCallingThread(String subscriptionId) {
        CatchupAttempt calling = CURRENT_ATTEMPT.get();
        return calling != null && calling.subscriptionId.equals(subscriptionId);
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
        handedOver.values().removeIf(made -> made.owner() == this);
        HistoryLossReportingSubscriptions.findIn(subscriptionModel).ifPresent(model -> model.removeHistoryLossListener(historyLossListener));
        parkedReplays.values().forEach(parked -> parked.replay().result.cancel(false));
        parkedReplays.clear();
    }

    /**
     * Parks the replays in flight on this model, and every replay subscribed until the next start, without touching
     * the shared live delegate. A parked subscription is paused, not running, and runs its replay again once
     * {@link #relaunchParkedReplays()} or {@link #resumeSubscription(String)} runs it, from the last position the replay
     * stored, or from where it started when it stored none.
     * <p>
     * Each replay is parked under its {@link #claim}. A failure to claim one is thrown once the others are parked, and
     * that replay parks itself once it next asks whether to go on.
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
        runEach(List.copyOf(currentAttempt.entrySet()), entry -> {
            if (entry.getValue().owner == this) {
                try (Claim ignored = claim(entry.getKey(), STOPPED_ALL_THE_SAME); HandoverLock alsoIgnored = lockHandover(entry.getKey())) {
                    parkIfStillCurrent(entry.getKey(), entry.getValue());
                }
            }
        });
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
            addSuppressed(startFailure, e);
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
     * @param startFailure     What the start threw, which a failure to park a replay is added to as suppressed
     */
    void undoStart(StartAttempt attempt, boolean liveDelegateRuns, Throwable startFailure) {
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
        try {
            parkReplaysInFlight();
        } catch (RuntimeException | Error e) {
            addSuppressed(startFailure, e);
        }
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
        return startCatchupAsync(subscriptionId, catchup, holdPaused, false, CompletableFuture.completedFuture(null));
    }

    // The replay waits on its own thread for earlierAttemptsDone, so it doesn't run the action alongside an attempt
    // that is still in it
    private Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused, boolean resuming, CompletableFuture<Void> earlierAttemptsDone) {
        return startCatchupAsync(subscriptionId, catchup, holdPaused, resuming, earlierAttemptsDone, null);
    }

    // takenOver is the handover whose live start the live delegate lost, when a takeover starts the replay
    private Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused, boolean resuming, CompletableFuture<Void> earlierAttemptsDone,
                                                   @Nullable HandedOver takenOver) {
        CompletableFuture<Subscription> result = new CompletableFuture<>();
        ReplayState replay = new ReplayState(subscriptionId, catchup, result, resuming, takenOver, takesOverLostLiveStarts());
        if (takenOver != null) {
            replay.handle.follow(result);
        }
        if (resuming) {
            resumeReplays.add(replay);
            result.whenComplete((subscription, failure) -> resumeReplays.remove(replay));
        }
        CatchupAttempt attempt = null;
        try {
            // Locked so this registration cannot land inside a still-finishing earlier attempt's own lockHandover span
            // for the same id. Unlocked, this attempt could start, and its replay could reach a checkpoint save, before
            // the earlier attempt's late checkpoint delete runs, wiping out what this attempt just wrote instead of
            // its own.
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                attempt = registerOrPark(subscriptionId, replay, earlierAttemptsDone, holdPaused);
            }
            if (attempt != null) {
                runOnItsOwnThread(subscriptionId, attempt);
            }
        } catch (RuntimeException | Error e) {
            giveUpUnstarted(subscriptionId, replay, attempt, e);
            throw e;
        }
        return result;
    }

    // No thread runs a replay whose registration or thread failed to start, so its attempt gives up the id and its
    // handle fails, which takes it out of resumeReplays. One a stop parked meanwhile stays parked and runs again later.
    private void giveUpUnstarted(String subscriptionId, ReplayState replay, @Nullable CatchupAttempt attempt, Throwable failure) {
        if (attempt != null) {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                if (currentAttempt.remove(subscriptionId, attempt)) {
                    runningCatchupSubscriptions.remove(subscriptionId);
                }
            } catch (RuntimeException | Error e) {
                addSuppressed(failure, e);
            }
            attempt.earlierAttemptsDone.whenComplete((ignored, alsoIgnored) -> attempt.done.complete(null));
        }
        ParkedReplay parked = parkedReplays.get(subscriptionId);
        if (parked == null || parked.replay() != replay) {
            replay.result.completeExceptionally(failure);
        }
    }

    /**
     * Registers a fresh attempt for {@code catchup} as the running catch-up for {@code subscriptionId}, or parks
     * {@code catchup} and returns {@code null} while this model is stopped or {@code holdPaused} is set, or when the
     * {@link CatchupListener} throws as the replay starts, which is logged at {@code ERROR}. Called with the
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
        CatchupAttempt attempt = new CatchupAttempt(subscriptionId, this, replay, earlierAttemptsDone);
        // Sent here, inside the same lock that takes ownership of the id and before the attempt's thread starts, so
        // it always precedes anything this attempt delivers. Before the attempt is registered, so one the listener
        // fails is never registered without a thread to run it.
        if (!tellCatchupStarted(subscriptionId, attempt)) {
            parkedReplays.put(subscriptionId, new ParkedReplay(replay, earlierAttemptsDone));
            return null;
        }
        runningCatchupSubscriptions.put(subscriptionId, true);
        currentAttempt.put(subscriptionId, attempt);
        return attempt;
    }

    /**
     * Tells the {@link CatchupListener} of {@code subscriptionId} that {@code attempt} starts to replay, and returns
     * false when it throws, which is logged at {@code ERROR}. The caller then holds the replay, so it runs again once
     * the subscription is resumed or {@code start(true)} is called on this model, and no attempt stays registered without a thread to run it.
     * Called with the handover lock held.
     */
    private boolean tellCatchupStarted(String subscriptionId, CatchupAttempt attempt) {
        CatchupListener listener = catchupListeners.get(subscriptionId);
        if (listener == null) {
            return true;
        }
        try {
            listener.catchupStarted(attempt);
            return true;
        } catch (RuntimeException | Error e) {
            log.error("The catch-up listener for subscription {} failed when its replay started, so the replay is held and runs again once the subscription is resumed or start(true) is called on this model.", subscriptionId, e);
            return false;
        }
    }

    private void runOnItsOwnThread(String subscriptionId, CatchupAttempt attempt) {
        // The replay ends its attempt's ownership once it completes (in endReplayIfStillCurrent, or in handOver for a
        // replay a resume started), and keeps it when shouldKeepReplaying already turned false, so a cancel can still
        // be told apart from a completion. A replay or a handover that throws goes to runAgainAfter, which decides
        // for both modes whether it runs again.
        //
        // A parked attempt does not complete the handle. The parked replay completes it once it runs again. Parking
        // takes the attempt out of currentAttempt under the handover lock, and the handovers take it out under that
        // lock before the replay returns, so the flag is final once it returns. One that fails can still be current
        // when what decides its failure throws, so endUndecidedAttempt reads the flag again under the lock.
        Thread.ofVirtual().name("occurrent-catchup-" + subscriptionId).start(() -> {
            CURRENT_ATTEMPT.set(attempt);
            try {
                Subscription subscription = replayUntilHandedOver(subscriptionId, attempt);
                if (!attempt.parked) {
                    attempt.replay.result.complete(subscription);
                }
            } catch (Throwable failure) {
                if (!attempt.parked) {
                    boolean ended = true;
                    try {
                        ended = endUndecidedAttempt(subscriptionId, attempt, failure);
                    } finally {
                        if (ended) {
                            attempt.replay.result.completeExceptionally(failure);
                        }
                    }
                }
            } finally {
                CURRENT_ATTEMPT.remove();
                attempt.earlierAttemptsDone.whenComplete((ignored, alsoIgnored) -> attempt.done.complete(null));
            }
        });
    }

    // A failure runAgainAfter didn't decide on leaves the attempt current. Ending it here lets a resume start the replay
    // anew instead of returning its failed handle. False when a pause or a stop held the attempt before this took the
    // lock, so the replay runs again and completes the handle then. Parking needs the attempt to be current, so it can't
    // come after this.
    private boolean endUndecidedAttempt(String subscriptionId, CatchupAttempt attempt, Throwable failure) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            if (currentAttempt.remove(subscriptionId, attempt)) {
                runningCatchupSubscriptions.remove(subscriptionId);
                pauseRequestedDuringCatchup.remove(subscriptionId);
                logFailure(subscriptionId, failure, "it ended");
            }
            return !attempt.parked;
        }
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
                // A handover that failed was decided before it released the handover lock
                DecidedFailure decided = attempt.decidedFailure;
                attempt.decidedFailure = null;
                Duration backoff = decided != null && decided.failure() == failure ? decided.backoff() : runAgainAfter(subscriptionId, attempt, failure);
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
     * resume at a given position or a shutdown ends it, which the caller asked for. A stop, a pause asked for during the
     * replay or while it waits, or a {@link CatchupListener} that throws as the replay runs again, holds it until a
     * resume or a start that resumes. A resume the live delegate refuses with
     * {@link SubscriptionAlreadyRunningException} ends it too, and the log says whether the live delegate runs the
     * subscription or holds it paused by then, and what resumes it.
     */
    private @Nullable Duration runAgainAfter(String subscriptionId, CatchupAttempt attempt, @Nullable Throwable failure) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            CatchupAttempt owner = currentAttempt.get(subscriptionId);
            boolean current = owner == attempt;
            if (current && attempt.replay.resuming && !attempt.abandoned() && !shuttingDown && !(failure instanceof SubscriptionAlreadyRunningException)) {
                if (stopped) {
                    attempt.backingOff = false;
                    parkIfStillCurrent(subscriptionId, attempt);
                    logFailure(subscriptionId, failure, "it runs again once the subscription is resumed or start(true) is called on this model");
                    return null;
                }
                boolean pauseAsked = pauseRequestedDuringCatchup.remove(subscriptionId) != null;
                if (pauseAsked || Thread.currentThread().isInterrupted()) {
                    attempt.backingOff = false;
                    hold(subscriptionId, attempt);
                    if (failure == null && !pauseAsked) {
                        log.error("The catch-up replay for subscription {} was interrupted while it waited to run again, so it runs again once the subscription is resumed.", subscriptionId);
                    }
                    logFailure(subscriptionId, failure, "it runs again once the subscription is resumed");
                    return null;
                }
                if (failure == null) {
                    attempt.backingOff = false;
                    // A replay run again delivers history again, as one replayUntilLiveStartHolds runs again does
                    if (!tellCatchupStarted(subscriptionId, attempt)) {
                        hold(subscriptionId, attempt);
                        return null;
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
            if (failure != null && attempt.wentLive) {
                log.error("Subscription {} went live once its catch-up replay was done, but pausing it as asked for during the replay failed, so it may not be paused.",
                        subscriptionId, failure);
            } else if (failure != null && !attempt.replay.cancelled && !shuttingDown && (owner == null || owner.replay == attempt.replay)) {
                logFailure(subscriptionId, failure, attempt.parked ? "it was held, and runs again once the subscription is resumed or start(true) is called on this model"
                        : failure instanceof SubscriptionAlreadyRunningException ? "it doesn't run again. " + whenNoReplayResumesIt(subscriptionId)
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
     * down, and then asks {@link #runAgainAfter} once more, under the {@link #claim} while the attempt is still the
     * current one, so the replay runs again only while no lifecycle call for the id runs. Returns whether the replay
     * runs again now. A pause holds the attempt as soon as it comes, so this needs no polling for it.
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
        // Waited for in slices, since a call holding the claim can take the replay from this attempt, and then waits for
        // this thread to return. An interrupt is kept for runAgainAfter.
        ReentrantLock claim = useClaimLock(subscriptionId);
        try {
            while (!tryLockWithin(claim, Duration.ofMillis(EARLIER_ATTEMPT_POLL_MILLIS))) {
                if (currentAttempt.get(subscriptionId) != attempt || attempt.abandoned() || shuttingDown) {
                    return runAgainAfter(subscriptionId, attempt, null) != null;
                }
            }
            try {
                return runAgainAfter(subscriptionId, attempt, null) != null;
            } finally {
                claim.unlock();
            }
        } finally {
            releaseClaimLock(subscriptionId);
        }
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
