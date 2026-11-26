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
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.PreparedResumeSubscriptions;
import org.occurrent.subscription.api.blocking.RedeliveringSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LongestReplay;
import org.occurrent.subscription.internal.LostStartHold;
import org.occurrent.subscription.internal.UnwantedDelivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Shared plumbing for the mode-specific catch-up subscription models ({@link StreamCatchupSubscriptionModel} and the
 * DCB catch-up model): the live delegate, config, running-catch-up bookkeeping, shutdown flag, and lifecycle
 * delegation. Replay and {@code subscribe(...)} routing stay in each subclass. DCB-free so it can live in the
 * stream module both modes build against.
 */
@NullMarked
abstract class AbstractCatchupSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, ReplayAwareSubscriptions, RepositionableSubscriptions, StartRefusalReportingSubscriptions, RedeliveringSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);

    protected final CheckpointAwareSubscriptionModel subscriptionModel;
    protected final CatchupSubscriptionModelConfig config;
    protected final Class<?> subscriptionModelContextType;
    protected final ConcurrentMap<String, Boolean> runningCatchupSubscriptions;
    // Pause requested for a subscriptionId while its replay is still in-flight. The handover of a replay a subscribe
    // started applies it once the live delegate has the subscription, and a replay a resume started is held instead.
    protected final ConcurrentMap<String, Boolean> pauseRequestedDuringCatchup = new ConcurrentHashMap<>();
    protected volatile boolean shuttingDown = false;
    // System.nanoTime() as the shutdown began, 0 until then. Set before markShuttingDown marks any record as forgotten
    // by the shutdown, so a live call that finds the mark also finds this set
    private final AtomicLong shutdownBeganNanos = new AtomicLong();
    // Set by stop(), and by a start whose live delegate failed on a stopped model. Cleared by start(...), and by a
    // resume of the live delegate after such a failed start. Checked by the replay loops so stop() interrupts an
    // in-flight replay, not just the delegate the replay has not registered with yet. Written only under lifecycleLock.
    protected volatile boolean stopped = false;
    // How many relaunchParkedReplays() calls have begun, moved before each one lists the parked replays. Ends the park
    // a stop that gave up left a replay owing, see CatchupAttempt.parkOwedSince, so a replay that parks after the list
    // was taken runs again itself
    private volatile long relaunchesBegun = 0;
    // A leaf lock, held only to read or write stopped and the fields below, so no method is called and no other lock
    // is taken while it is held
    private final Object lifecycleLock = new Object();
    // Orders start(..), stop() and shutdown() of this model and of every other child of the same dispatcher
    private final ReentrantLock modelLock;
    // Runs the calls that runInTurn and shutDownInTurn don't run on the calling thread, shared with every other child of
    // the same dispatcher
    private final Executor lifecycleThread;
    private final AtomicInteger handedCallsNotReturned;
    private final Set<HandedCallsOfAnAction> actionsHoldingHandedCalls;
    private final AtomicLong shutdownsHandedOver;
    private final AtomicLong handedShutdownsReturned;
    private final AtomicLong callersCodeUnderTheLockSince;
    private final ConcurrentMap<String, Long> callersCodeUnderTheHandoverLockSince;
    private final ConcurrentMap<Object, LiveDelegateCalls> liveDelegateCalls;
    private final AtomicReference<LifecycleCall> lastLifecycleCall;
    private final ConcurrentMap<String, Long> positionMoves;
    private final AtomicLong positionMoveCount;
    private final AtomicLong perIdWaitUnderTheLockSince;
    // Moves on with every start and stop, so a start whose live delegate failed can tell whether another came after it
    private long lifecycleGeneration = 0;
    // Moves on with every stop, so a call to the wrapped model can tell whether a stop came while it ran
    private long stops = 0;
    // Moves on with every stop() and stopReplay(), but not when a failed start stops the model again, so a resume that
    // read outside the lock can tell whether one was asked while it read, see resumeReadingFirst
    private long stopsAsked = 0;
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
    // The actions of live calls and replays that run on this thread, so a start(..), stop() or shutdown() from inside
    // one doesn't wait for a call that can be waiting for the action
    private static final ThreadLocal<@Nullable ActionOnThread> IN_AN_ACTION = new ThreadLocal<>();
    // The threads that run a shutdown a held-up shutdown() hands over, at most one at a time for each model
    private static final ThreadFactory HANDED_SHUTDOWN_THREADS = Thread.ofPlatform().daemon().name("occurrent-catchup-handed-shutdown")
            .inheritInheritableThreadLocals(false).factory();
    // The threads that run a call for one subscription handed over from an action, so what it reads or waits for holds
    // up no call of another subscription. Platform threads, as the lifecycle thread is, since the live delegate can wait
    // inside synchronized
    private static final ThreadFactory HANDED_PER_ID_CALL_THREADS = Thread.ofPlatform().daemon().name("occurrent-catchup-handed-call", 0)
            .inheritInheritableThreadLocals(false).factory();
    // The call for one subscription handed over from an action that runs on this thread, see runPerIdCallInOrder
    private static final ThreadLocal<@Nullable HandedPerIdCall> HANDED_PER_ID_CALL = new ThreadLocal<>();
    // The threads that run the resumes a start(true) left until after its turn, one for each subscription, so one whose
    // checkpoint storage doesn't answer holds up no other. Platform threads, as HANDED_PER_ID_CALL_THREADS are, since the
    // live delegate can wait inside synchronized
    private static final ThreadFactory START_RESUME_THREADS = Thread.ofPlatform().daemon().name("occurrent-catchup-start-resume", 0)
            .inheritInheritableThreadLocals(false).factory();
    // How often a replay run again checks, while an earlier attempt's action still runs, whether it should still wait
    private static final long EARLIER_ATTEMPT_POLL_MILLIS = 100;
    // How often a call that waits for the model lock, or for the lifecycle thread, checks whether code a caller supplied
    // holds it up
    private static final Duration CALLERS_CODE_POLL = Duration.ofMillis(100);
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
    // How long a cancel or a stop waits for a call to the subscriber's action that a replay let through before the call
    // took effect, as the live MongoDB models wait a second for a running action. The call has taken effect already,
    // so it returns once this has passed, with a WARN, and the replay lets no further event through.
    static final Duration ACTION_WAIT = Duration.ofSeconds(1);
    // Who to tell about each id's catch-up boundaries, registered before the subscription that produces them.
    // Kept until this model shuts down, since the registration outlives any one catch-up: a stop and start, a
    // resume, or a cancel and re-subscribe all run another catch-up for the same id, and a recorder that stopped
    // being told would record that catch-up's history as though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // Told when this model holds a subscription paused because the live delegate lost the live start of its replay
    private final List<StartRefusalListener> heldPausedListeners = new CopyOnWriteArrayList<>();
    // One lock per subscriptionId, taken for a fresh attempt's registration (startCatchupAsync), each event a replay
    // lets through to the subscriber's action (deliverIfStillWanted), the check before each catch-up checkpoint it
    // stores (saveIfStillSafe), a finishing attempt's checkpoint cleanup and delegate subscribe (or its
    // cancelled-cleanup branch), and cancelRunningCatchup. The identity check above only made the ownership decision
    // itself atomic, not what followed it, so a cancellation or a fresh registration could land in the gap after an
    // attempt decided it was still current but before it finished acting on that. Held across those transitions,
    // never across the replay's call to the subscriber's action or a catch-up checkpoint store, so a slow call or a
    // slow storage never holds up a pause or a stop on this lock. Counted like lifecycleClaims, so an entry is kept only while a thread holds the lock or waits
    // for it.
    private final ConcurrentMap<String, CountedLock> handoverLocks;
    // Runs on the replay thread for each event deliverIfStillWanted lets through, after the handover lock is released
    // and before the call to the action. Exists so a test can stand there, which nothing outside this model can.
    private volatile Runnable betweenHandoverLockAndAction = () -> {
    };
    // Runs on the replay thread for each store saveIfStillSafe begins, after the handover lock is released and before
    // the checkpoint storage is called. Exists for the same reason.
    private volatile Runnable betweenHandoverLockAndStore = () -> {
    };
    // Runs on the calling thread of a start, stop or resume that found no handed call pending, before it waits for the
    // model lock on that thread. Exists for the same reason
    private volatile Runnable betweenTheHandedCallsAndTheLock = () -> {
    };
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
    // wrapper waits for the lease without its locks. Over a model that redelivers, as this model does when its live
    // delegate does, the wrapper keeps an event waiting while it asks or starts this model for the subscription, and
    // withdraws it once a pause, cancel or stop of it has returned or a shutdown has begun, apart from a few rare
    // cases in which it lets the event through. Over any other model it lets the event through while it calls this
    // model for the subscription. A cycle through it has to pass through a wait for an action, which LIFECYCLE_WAIT
    // bounds, and so does one through any other caller.
    private final ConcurrentMap<String, CountedLock> lifecycleClaims;
    // The subscribes whose function is asked with no claim held, shared by every child of the same dispatcher
    private final Set<AskedFirst> askedFirst;
    // The ids a resume at a given position runs for that read as running when it took their claim, so they keep reading
    // as running and not paused until it returns, also while it holds a replay in the way. The subscription runs once
    // it returns, and a caller that saw it paused meanwhile would leave a pause undone, as a competing consumer that
    // loses its lease then does. One that read as paused isn't marked and reads as paused, so a caller that resumes
    // what is paused still resumes it once the call returns, whether or not the call resumed it.
    private final Set<String> repositioning;
    // What each subscription asked for when it was last made through this model or another child of the same
    // dispatcher, so a resume can replay from a position a catch-up stored for it. Removed once the live delegate has
    // cancelled it, and kept through a cancel the live delegate fails. Every write that puts a record in it holds its
    // monitor, as does markShuttingDown while it marks and forgets the records, so a record is put either before that,
    // and gets marked and forgotten, or after, and is never forgotten by the shutdown without the mark.
    private final ConcurrentMap<String, Subscribed> subscribed;
    // Run by markShuttingDown between marking the records and forgetting them, so a test can hold the shutdown there
    // while a subscribe runs. Only StreamCatchupSubscribeDuringShutdownSweepTest sets it, and it is a no-op otherwise
    volatile Runnable betweenShutdownMarksAndClear = () -> {
    };
    // The replays a cancel that hasn't returned yet ended for each id. Kept from before the cancel takes them away until
    // it returns, and then moved to what is kept for the subscription when the cancel threw, so start(true) and every
    // resume find them all through the cancel, also when the subscription wasn't made through this model.
    private final ConcurrentMap<String, CancelUnderWay> cancelsUnderWay;
    // Done once every attempt of a replay that a cancel, a resume or a stop dropped for the id has returned, kept only
    // until then, so the next replay of the id doesn't run the action alongside one of them
    private final ConcurrentMap<String, CompletableFuture<Void>> droppedReplaysReturned;
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
        this.droppedReplaysReturned = sharedState.droppedReplaysReturned;
        this.cancelsStarted = sharedState.cancelsStarted;
        this.handedOver = sharedState.handedOver;
        this.askedFirst = sharedState.askedFirst;
        this.modelLock = sharedState.modelLock;
        this.lifecycleThread = sharedState.lifecycleThread;
        this.handedCallsNotReturned = sharedState.handedCallsNotReturned;
        this.actionsHoldingHandedCalls = sharedState.actionsHoldingHandedCalls;
        this.shutdownsHandedOver = sharedState.shutdownsHandedOver;
        this.handedShutdownsReturned = sharedState.handedShutdownsReturned;
        this.callersCodeUnderTheLockSince = sharedState.callersCodeUnderTheLockSince;
        this.callersCodeUnderTheHandoverLockSince = sharedState.callersCodeUnderTheHandoverLockSince;
        this.liveDelegateCalls = sharedState.liveDelegateCalls;
        this.lastLifecycleCall = sharedState.lastLifecycleCall;
        this.positionMoves = sharedState.positionMoves;
        this.positionMoveCount = sharedState.positionMoveCount;
        this.perIdWaitUnderTheLockSince = sharedState.perIdWaitUnderTheLockSince;
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
        private final ConcurrentMap<String, CountedLock> handoverLocks = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CatchupAttempt> currentAttempt = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Subscribed> subscribed = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountedLock> lifecycleClaims = new ConcurrentHashMap<>();
        private final Set<String> repositioning = ConcurrentHashMap.newKeySet();
        private final ConcurrentMap<String, CancelUnderWay> cancelsUnderWay = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CompletableFuture<Void>> droppedReplaysReturned = new ConcurrentHashMap<>();
        private final AtomicLong cancelsStarted = new AtomicLong();
        private final ConcurrentMap<String, HandedOver> handedOver = new ConcurrentHashMap<>();
        private final Set<AskedFirst> askedFirst = ConcurrentHashMap.newKeySet();
        // Taken by start(..), stop() and shutdown(), so one of them runs at a time across every child
        private final ReentrantLock modelLock = new ReentrantLock();
        // One thread at most, running what it is given in the order it came, and ending a second after the last of it.
        // A platform thread, since a virtual one gains nothing for a single mostly idle thread, and on JDK 21 a wait
        // inside synchronized in the live delegate would hold its carrier thread
        // It inherits no inheritable thread-local, such as a logging context, from the thread that happens to create it
        private final Executor lifecycleThread = new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                Thread.ofPlatform().daemon().name("occurrent-catchup-lifecycle").inheritInheritableThreadLocals(false).factory());
        // The calls handed over from an action or given to lifecycleThread, and the shutdowns a held-up shutdown() gave a
        // thread of their own, that haven't returned yet, so a call from another thread made meanwhile runs behind them on
        // lifecycleThread
        private final AtomicInteger handedCallsNotReturned = new AtomicInteger();
        // For each action that handed a call over and hasn't returned yet, the subscriptions those calls concern and a
        // future completed once they have reached lifecycleThread. A call from another thread waits for those of them
        // that concern what it changes first, so it takes effect after them
        private final Set<HandedCallsOfAnAction> actionsHoldingHandedCalls = ConcurrentHashMap.newKeySet();
        // The shutdowns a held-up shutdown() gave a thread of their own, and those of them that have returned, so a call
        // under modelLock can tell whether one hadn't returned when the call was made
        private final AtomicLong shutdownsHandedOver = new AtomicLong();
        private final AtomicLong handedShutdownsReturned = new AtomicLong();
        // The System.nanoTime() at which the thread holding modelLock began to run code a caller supplied, such as a
        // StartAt.dynamic function or a CatchupListener, and 0 while it runs none. Written only by that thread
        private final AtomicLong callersCodeUnderTheLockSince = new AtomicLong();
        // Per id, the System.nanoTime() at which a thread holding the id's handover lock began to run code a caller
        // supplied, such as a CatchupListener told on the replay thread, with no entry while none runs. Written only by
        // that thread
        private final ConcurrentMap<String, Long> callersCodeUnderTheHandoverLockSince = new ConcurrentHashMap<>();
        // The calls to the live delegate's start(..) and shutdown() of each model that makes them
        private final ConcurrentMap<Object, LiveDelegateCalls> liveDelegateCalls = new ConcurrentHashMap<>();
        // The last lifecycle call of these children or of their dispatcher that started or stopped them, as
        // LifecycleCall describes. Read under modelLock, and written there too, except when liveDelegateResumed() starts
        // a model a failed start stopped again
        private final AtomicReference<LifecycleCall> lastLifecycleCall = new AtomicReference<>(new LifecycleCall(0, false, false));
        // Per id, the number from positionMoveCount of the last call that wrote or deleted the position stored for it,
        // through the checkpoint storage or the live delegate, once that call has returned or thrown. A resume that read
        // what is stored outside the model lock uses it only while this is what it was when the read began
        private final ConcurrentMap<String, Long> positionMoves = new ConcurrentHashMap<>();
        private final AtomicLong positionMoveCount = new AtomicLong();
        // The System.nanoTime() at which the thread holding modelLock began to wait for a call for one subscription, such
        // as for its claim or its handover lock, or began a resume in a live delegate that can't split it, and 0 while it
        // waits for none. A shutdown() waiting for modelLock meanwhile shuts the live delegate down at once. Written only
        // by that thread
        private final AtomicLong perIdWaitUnderTheLockSince = new AtomicLong();
    }

    // The start(..) and shutdown() calls a model makes to its live delegate, so a shutdown() can end a start(..) that the
    // live delegate holds until it is shut down, as a MongoDB model retrying its start through an outage does. The counts
    // are read and written only while holding its own monitor, which nothing holds while it calls the live delegate
    private static final class LiveDelegateCalls {
        // Starts of the live delegate under way
        private int starts;
        // Shutdowns of the model under way, from the moment they forgot the catch-ups until they return, or until the
        // thread a held-up one handed the rest to has run it, or until the start one was made inside has run the rest
        private int shutdowns;
        // Every shutdown of the model that has begun, so a start can tell whether one began while it ran
        private long shutdownsBegun;
        // Whether a shutdown() that code a caller supplied held up has handed a thread of its own a shutdown of the
        // live delegate that hasn't run yet, so a later one held up the same way hands over no second one
        private boolean shutdownHandedOver;
        // Whether a shutdown() has shut the live delegate down, with no start(..) of the model since, so a later
        // shutdown() doesn't shut it down again. Written only with the model lock held, and read under it
        private boolean shutDown;
        // The shutdowns made on the thread of a start(..) of the model while it was in the live delegate's start(..),
        // which can start the live delegate again after them. Each counts in shutdowns until that start has returned
        // and shut the live delegate down for them with shutDownLeftToTheStart
        private int shutdownsLeftToTheStart;
        private @Nullable Runnable shutDownLeftToTheStart;
    }

    // A subscribe whose function is asked with no claim held, kept from before the function is asked until the
    // subscribe returns
    private static final class AskedFirst {
        private final String subscriptionId;
        private final Thread asker = Thread.currentThread();
        private final CompletableFuture<Void> returned = new CompletableFuture<>();
        // Set by a cancel on another thread before the subscribe holds the claim. A cancel that marks it after the
        // subscribe has read it changes nothing, since that cancel finds the subscription the subscribe made
        private volatile boolean cancelled;

        private AskedFirst(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }
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
     * @param noLive          Whether the live delegate must not run, so a takeover keeps {@code replayAgainFrom} in
     *                        memory and stores nothing
     */
    private record HandedOver(AbstractCatchupSubscriptionModel owner, Subscribed subscribed, Checkpoint liveStart, Checkpoint replayAgainFrom, ReplayState replay,
                              AtomicBoolean takeOverAsked, AtomicBoolean takingOver, boolean noLive) {
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

    // The lock behind a claim or a handover lock, and how many threads hold it or wait for it
    static final class CountedLock {
        private final ReentrantLock lock = new ReentrantLock();
        // Changed only inside compute and computeIfPresent of the map that holds it, which run one at a time for an id
        private int users = 1;
        // For a claim, what the call holding it did before it asked for it, as claim(..) describes. Written and read
        // only by the thread holding the claim
        private @Nullable String whatChanged;
    }

    /**
     * A subscription as it was made, and the child of a dispatcher it was made through.
     *
     * @param cancelledReplayReturned Done once the thread of every replay a cancel that threw ended for the subscription
     *                                has returned, so a later resume doesn't run the action alongside it
     * @param lostLiveStart           The live start the live delegate lost when this model last took the subscription
     *                                over, and {@code null} before that. Only a run that a start(true) of the live
     *                                delegate resumed there reports a loss of it again.
     * @param historyLost             The position the live delegate lost the history from, recorded only where a resume
     *                                would skip events, which is when no replay of this model could take the loss over, or when
     *                                a replay that started after the beginning lost its live start. {@code null} before
     *                                that, and when a replay from the beginning loses its live start, since a replay
     *                                again skips nothing. Once set, a resume refuses
     *                                with {@link CatchupCannotResumeException} until the subscription is cancelled, or
     *                                until {@link AbstractCatchupSubscriptionModel#resumeSubscription(String, StartAt)}
     *                                resumes it at a position the live delegate still has the history from.
     * @param didNotGoLive            Set once a replay its subscribe started ended before the live delegate got the
     *                                subscription, at its handover too, and cleared once a replay hands it over, so a
     *                                resume at a given position doesn't refuse it as running.
     * @param liveCalls               The calls the live delegate has in the subscriber's action for the subscription,
     *                                so a later subscribe of the id waits for them once a cancel has forgotten it.
     */
    record Subscribed(AbstractCatchupSubscriptionModel owner, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action,
                      CompletableFuture<Void> cancelledReplayReturned, AtomicReference<@Nullable Checkpoint> lostLiveStart,
                      HistoryLost historyLost, AtomicBoolean didNotGoLive, LiveCalls liveCalls) {

        private Subscribed(AbstractCatchupSubscriptionModel owner, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this(owner, filter, startAt, action, CompletableFuture.completedFuture(null), new AtomicReference<>(), new HistoryLost(), new AtomicBoolean(), new LiveCalls());
        }

        // Put by the same subscribe as made, also as the copy a cancel that threw kept in its place, which takes over
        // made's historyLost
        private boolean isFromTheSameSubscribeAs(Subscribed made) {
            return historyLost == made.historyLost;
        }
    }

    // The calls a subscription's live delegate has let into the subscriber's action and not yet returned from. A call
    // is let in only while the subscription's record is there, in one step with that check, so once a cancel has
    // forgotten the record the count only goes down. Also whether a shutdown forgot the record, as opposed to a cancel,
    // a failed subscribe or a later subscribe of the id, since only the calls of a record a shutdown forgot still reach
    // the action. Shared with the copy a cancel that threw keeps, as the record's other state is
    private static final class LiveCalls {
        private int inAction;
        private @Nullable CompletableFuture<Void> returned;
        private volatile boolean forgottenByShutdown;

        void forgetForShutdown() {
            forgottenByShutdown = true;
        }

        boolean forgottenByShutdown() {
            return forgottenByShutdown;
        }

        synchronized void enter() {
            inAction++;
        }

        void exit() {
            CompletableFuture<Void> done = null;
            synchronized (this) {
                inAction--;
                if (inAction == 0) {
                    done = returned;
                    returned = null;
                }
            }
            if (done != null) {
                done.complete(null);
            }
        }

        // Done once every call let in so far has returned. A failed subscribe puts the record it replaced back, and
        // that record can then let calls in again, so a call let in after this is asked gets a new future
        synchronized CompletableFuture<Void> returned() {
            if (inAction == 0) {
                return CompletableFuture.completedFuture(null);
            }
            if (returned == null) {
                returned = new CompletableFuture<>();
            }
            return returned;
        }
    }

    // The position the live delegate lost the history from, whether it is the live start a replay would go live from,
    // how many times a resume at a given position cleared it, and the position the last such resume gave. A loss
    // reported before such a resume must not be recorded after it, which would mark a subscription it has resumed as
    // lost, unless it is a loss of the position that resume gave.
    private static final class HistoryLost {
        private record State(int clears, @Nullable Checkpoint lostFrom, boolean liveStart, @Nullable String resumedAt) {
        }

        private final AtomicReference<State> state = new AtomicReference<>(new State(0, null, false, null));

        @Nullable Checkpoint get() {
            return state.get().lostFrom();
        }

        // What a resume refuses with while a loss is recorded, or null
        @Nullable CatchupCannotResumeException refusal(String subscriptionId) {
            State seen = state.get();
            return seen.lostFrom() == null ? null : HistoryLossMessages.resumeRefused(subscriptionId, seen.lostFrom(), seen.liveStart());
        }

        int clears() {
            return state.get().clears();
        }

        // False when it was cleared since clearsWhenReported by a resume at a position other than lostFrom. liveStart says
        // lostFrom is the live start a replay would go live from
        boolean recordUnlessCleared(int clearsWhenReported, Checkpoint lostFrom, boolean liveStart) {
            State before = state.getAndUpdate(it -> records(it, clearsWhenReported, lostFrom) ? new State(it.clears(), lostFrom, liveStart, it.resumedAt()) : it);
            return records(before, clearsWhenReported, lostFrom);
        }

        private static boolean records(State seen, int clearsWhenReported, Checkpoint lostFrom) {
            return seen.clears() == clearsWhenReported || isAt(lostFrom, seen.resumedAt());
        }

        // Keeps a loss of resumedAt itself. Returns the loss it cleared, or null.
        @Nullable Checkpoint clear(@Nullable Checkpoint resumedAt) {
            return clearUntilPutBack(resumedAt).lost();
        }

        // As clear, and what putBack needs to undo it
        Cleared clearUntilPutBack(@Nullable Checkpoint resumedAt) {
            String at = resumedAt == null ? null : resumedAt.asString();
            while (true) {
                State seen = state.get();
                boolean kept = isAt(seen.lostFrom(), at);
                State after = new State(seen.clears() + 1, kept ? seen.lostFrom() : null, kept && seen.liveStart(), at);
                if (state.compareAndSet(seen, after)) {
                    return new Cleared(seen, after);
                }
            }
        }

        // Undoes cleared, unless a loss was recorded or cleared since, which then stays
        void putBack(Cleared cleared) {
            state.compareAndSet(cleared.after(), cleared.before());
        }

        record Cleared(State before, State after) {
            // The loss the clear removed, or null when there was none or it was kept
            @Nullable Checkpoint lost() {
                return after.lostFrom() == null ? before.lostFrom() : null;
            }
        }

        private static boolean isAt(@Nullable Checkpoint lostFrom, @Nullable String position) {
            return lostFrom != null && lostFrom.asString().equals(position);
        }
    }

    // Reports subscriptionModelContextType (the dispatcher's type when wrapped) so a caller's StartAt.dynamic
    // pattern-matching on the public dispatcher type keeps working regardless of which subclass runs underneath.
    protected SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(subscriptionModelContextType);
    }

    /**
     * Subscribes {@code subscriptionId}, with a catch-up replay first when {@code startAt} lies in the past.
     * <p>
     * Called from inside the action of a live call or a replay of this model after that action handed a call over, such
     * as a {@link #start(boolean)} or a {@link #stop()}, it returns at once a handle and subscribes on the lifecycle
     * thread, once the action has returned and that call has run. The handle's {@code waitUntilStarted} throws what the
     * subscribe threw there, which is also logged at {@code ERROR}.
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Subscription handedOver = subscriptionBehindTheCallsTheActionHandedOver(this, "subscribe(" + subscriptionId + ")", subscriptionId,
                () -> subscribe(subscriptionId, filter, startAt, action));
        if (handedOver != null) {
            return handedOver;
        }
        return subscribeAndRemember(subscriptionId, filter, startAt, action, false);
    }

    /**
     * Keeps a replay waiting to run as a stopped model does, and has the live delegate hold a subscription with no
     * replay paused, so nothing is read or delivered until {@link #resumeSubscription(String)} or {@link #start(boolean) start(true)}.
     * Called from inside an action after that action handed a call over, it subscribes once that call has run, as
     * {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)} does.
     */
    @Override
    public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Subscription handedOver = subscriptionBehindTheCallsTheActionHandedOver(this, "subscribePaused(" + subscriptionId + ")", subscriptionId,
                () -> subscribePaused(subscriptionId, filter, startAt, action));
        if (handedOver != null) {
            return handedOver;
        }
        return subscribeAndRemember(subscriptionId, filter, startAt, action, true);
    }

    // The caller's function is asked for this model with no claim held, since it can wait for a call for the id on
    // another thread. A cancel that takes the claim after the subscribe is added to askedFirst, and before the subscribe
    // takes the claim again, wins, and nothing reaches the live delegate. A pause, resume or other lifecycle call for the
    // id on another thread waits for a subscribe whose function is asked to return, up to LIFECYCLE_WAIT. A cancel and
    // another subscribe of the id don't wait for the function. A subscribe on the lifecycle thread asks it under the
    // model lock, as askedAsCallersCode describes.
    private Subscription subscribeAndRemember(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt given, Consumer<CloudEvent> action, boolean holdPaused) {
        StartAt startAt = askedAsCallersCode(given);
        // An id in use is refused before the function is asked, as in 0.34.0, so the function of a duplicate holds up no
        // call for the subscription that runs. This waits for the claim as any subscribe of 0.34.0 did, and under the
        // handover lock for the reason given below
        try (Claim ignored = claimWithoutWaitingForAsks(subscriptionId); HandoverLock alsoIgnored = lockHandover(subscriptionId)) {
            if (isInUse(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
        }
        // A start that isn't dynamic asks nothing, so a cancel doesn't stop it, as before the function was asked ahead
        // of the claim
        AskedFirst asked = new AskedFirst(subscriptionId);
        if (startAt.isDynamic()) {
            askedFirst.add(asked);
        }
        try {
            StartAt answered = answeredForThisModel(startAt);
            // Under the handover lock as well as the claim, so a handover or a failure of a replay for the id comes either
            // before the refusal is decided or after the subscription is remembered
            try (Claim ignored = claimWithoutWaitingForAsks(subscriptionId); HandoverLock alsoIgnored = lockHandover(subscriptionId)) {
                if (asked.cancelled) {
                    return new CancelledSubscription(subscriptionId);
                }
                if (isInUse(subscriptionId)) {
                    throw new DuplicateSubscriptionIdException(subscriptionId);
                }
                // Before the live delegate opens, since it can report a loss from another thread before subscribe returns
                Subscribed made = new Subscribed(this, filter, startAt, action);
                Subscribed before;
                synchronized (subscribed) {
                    before = subscribed.put(subscriptionId, made);
                }
                if (before != null) {
                    waitForTheLiveCallsOf(subscriptionId, before);
                }
                try {
                    return subscribe(subscriptionId, filter, answered, action, holdPaused, made);
                } catch (Throwable e) {
                    // Throwable, since a live model written in Kotlin can throw a checked exception
                    // Puts back the record this one replaced, of a subscription no longer in use, as before the subscribe
                    Subscribed putBack;
                    synchronized (subscribed) {
                        putBack = subscribed.compute(subscriptionId, (id, now) -> now == made ? before : now);
                    }
                    if (putBack == before) {
                        forgetPositionMovesUnlessSubscribed(subscriptionId);
                        waitForTheLiveCallsOf(subscriptionId, made);
                    }
                    throw e;
                }
            }
        } finally {
            askedFirst.remove(asked);
            asked.returned.complete(null);
        }
    }

    // Answers what the function answered here when this model asks again, and asks it afresh for any other model, so the
    // live delegate and a model it wraps still get their own answer
    private StartAt answeredForThisModel(StartAt startAt) {
        if (!startAt.isDynamic()) {
            return startAt;
        }
        StartAt answer = startAt.get(generateSubscriptionModelContext());
        return StartAt.dynamic(context -> context.subscriptionModelType() == subscriptionModelContextType ? answer : startAt.get(context));
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
     * This model calls it on the thread that subscribes, or that resumes the subscription at a given position, with
     * the record it keeps for the subscription. A call to {@link #subscribeInTheWrappedModel} or
     * {@link #startCatchupAsync(String, CatchupReplay, boolean)} on that thread hands the subscription to the live
     * delegate behind the check {@link #subscribeInTheWrappedModel} describes.
     */
    protected abstract Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused);

    // Subscribes through the protected subscribe with made, the record kept for the subscription, the one a subscribe
    // just put or the one a resume at a given position subscribes again, on this thread, where recordOfThisSubscribe
    // reads it. The protected signature stays as in 0.34.0, so a subclass written for 0.34.0 still compiles
    final Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused, Subscribed made) {
        return withRecordOnThisThread(subscriptionId, made, () -> subscribe(subscriptionId, filter, startAt, action, holdPaused));
    }

    // The record of the subscription that this thread subscribes through this model, or null on any other thread.
    // An override of subscribe reads it on entry, so a handover that runs later on a replay thread still has it
    @Nullable Subscribed recordOfThisSubscribe(String subscriptionId) {
        StartingFromSubscribe starting = STARTING_FROM_SUBSCRIBE.get();
        return starting != null && starting.made().owner() == this && starting.subscriptionId().equals(subscriptionId) ? starting.made() : null;
    }

    /**
     * Hands {@code subscriptionId} to the live delegate, held paused there when {@code holdPaused} is set. Called with
     * the handover lock held. Called on the thread this model calls {@link #subscribe} on, the live delegate never gets
     * the action itself, neither from here nor from a replay that a subscribe started and that hands the subscription
     * over once it has caught up. Called on another thread, the live delegate gets the action itself, as in 0.34.0.
     * <p>
     * Each call the live delegate makes hands the event to the action only while the record kept for the subscription
     * is still the id's record, and counts the call in that record until the action returns or throws, so a later
     * subscribe of the id waits for it. Once a cancel has forgotten the record, the call throws {@link AttemptNotMade}
     * without handing the event to the action, so a live delegate that stores positions stores none past the event.
     * Once this model begins to shut down, which forgets every record still kept, the event of such a record still
     * reaches the action, without being counted. A call for a record a cancel forgot before the shutdown began throws
     * as it would without the shutdown. A call returns normally only once the action has returned normally for the
     * event.
     * <p>
     * When a replay that a cancel or a stop ended for the id can still be in the subscriber's action, each call first
     * waits for that replay to return, on the live delegate's thread, so the action never runs alongside itself. The
     * subscribe itself returns without that wait. The wait ends once the record is no longer the id's record, also
     * when a later subscribe of the id has put a new one, and the call then throws {@link AttemptNotMade} without
     * handing the event to the action. A cancel that throws keeps the subscription, so the event waits on and reaches
     * the action once the replay has returned. An interrupt that comes during the wait doesn't end it. Such a call hands
     * the event to the action with the thread's interrupt status clear, whenever the status was set, and sets it again
     * once the action has returned or thrown. A call with no such replay left at subscribe calls the action with
     * whatever interrupt status its thread has.
     * <p>
     * Once this model has begun to shut down, a waiting call for a record the shutdown forgot waits on, and reaches the
     * action uncounted once the replay has returned. A second after this model began to shut down, the call throws
     * {@link AttemptNotMade} without handing the event to the action instead, so a live delegate whose shutdown waits
     * for its threads doesn't wait for a replay stuck in the action. A live delegate should not restart a run whose
     * action threw once its shutdown has begun. The MongoDB models don't, and their shutdown begins right after this
     * model's. The throw reaches a run a live delegate still has open when its shutdown begins more than a second after
     * this model's, such as behind a wrapper that does work of its own first, and that delegate may then restart the run
     * or log it as a failure. The event isn't handed to the action either way. An interrupt doesn't end this wait.
     */
    protected Subscription subscribeInTheWrappedModel(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        return subscribeInTheWrappedModel(subscriptionId, filter, startAt, action, holdPaused, recordOfThisSubscribe(subscriptionId));
    }

    // As the protected subscribeInTheWrappedModel, for the subscription whose record is made, read by an override of
    // subscribe on entry, so a handover on a replay thread puts the action behind the same check. Without a record the
    // live delegate gets the action uncounted, as in 0.34.0, and marked as a live call's action all the same
    final Subscription subscribeInTheWrappedModel(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused, @Nullable Subscribed made) {
        Consumer<CloudEvent> afterEndedReplays = made == null ? event -> acceptInAnAction(subscriptionId, action, event) : afterEndedReplaysReturned(subscriptionId, action, made);
        return holdPaused
                ? getWrappedSubscriptionModel().subscribePaused(subscriptionId, filter, startAt, afterEndedReplays)
                : getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, afterEndedReplays);
    }

    // The action, called only while made, the record of the subscription the caller subscribes, is still the record for
    // subscriptionId, and counted in made's live calls. While a replay a cancel or a stop ended for subscriptionId can
    // still be in the action, the call also waits for that replay to return first. The wait ends early once made is
    // gone, so a later subscribe of the same id doesn't count as this subscription still being wanted, and the call then
    // throws AttemptNotMade without handing the event to the action. During a shutdown it ends ACTION_WAIT after the
    // shutdown began. Called with the handover lock held.
    private Consumer<CloudEvent> afterEndedReplaysReturned(String subscriptionId, Consumer<CloudEvent> action, Subscribed made) {
        CompletableFuture<Void> endedReplaysReturned = endedReplaysReturned(subscriptionId, null);
        if (endedReplaysReturned.isDone()) {
            return event -> acceptIfLetIn(subscriptionId, made, action, event);
        }
        return event -> acceptAfterEndedReplays(subscriptionId, made, endedReplaysReturned, action, event);
    }

    // Hands event to action, counted in made's live calls, while made is still the record for subscriptionId. Once a
    // shutdown has forgotten made, which it does for every record before the live delegate stops, the event reaches the
    // action uncounted. Once a cancel or a failed subscribe has forgotten made, the call throws instead, also during a
    // shutdown that began later, since a later subscribe of the id can already be delivering, and returning would let
    // the live delegate store its position past an event the action never got
    private void acceptIfLetIn(String subscriptionId, Subscribed made, Consumer<CloudEvent> action, CloudEvent event) {
        if (letIn(subscriptionId, made)) {
            try {
                acceptInAnAction(subscriptionId, action, event);
            } finally {
                made.liveCalls().exit();
            }
        } else if (made.liveCalls().forgottenByShutdown()) {
            acceptInAnAction(subscriptionId, action, event);
        } else {
            throw liveEventWithdrawn(subscriptionId);
        }
    }

    // Marks the thread as running an action of subscriptionId on this model, of a live call or of a replay, while the
    // action runs, so runInTurn and shutDownInTurn don't wait there for a call that can itself be waiting for the action
    // to return
    private void acceptInAnAction(String subscriptionId, Consumer<CloudEvent> action, CloudEvent event) {
        ActionOnThread outer = IN_AN_ACTION.get();
        ActionOnThread call = new ActionOnThread(subscriptionId, modelLock, lifecycleThread, actionsHoldingHandedCalls, outer);
        IN_AN_ACTION.set(call);
        try {
            action.accept(event);
        } finally {
            call.actionReturned();
            if (outer == null) {
                IN_AN_ACTION.remove();
            } else {
                IN_AN_ACTION.set(outer);
            }
        }
    }

    private static AttemptNotMade liveEventWithdrawn(String subscriptionId) {
        return new AttemptNotMade("A live event of subscription " + subscriptionId + " was withdrawn, since the subscription ended before the action was called");
    }

    // Counts a call in made's live calls when made is still the record for subscriptionId, in one step with the look,
    // so a cancel that forgets the record after this finds the call counted
    private boolean letIn(String subscriptionId, Subscribed made) {
        AtomicBoolean letIn = new AtomicBoolean();
        subscribed.computeIfPresent(subscriptionId, (id, now) -> {
            if (now.isFromTheSameSubscribeAs(made)) {
                made.liveCalls().enter();
                letIn.set(true);
            }
            return now;
        });
        return letIn.get();
    }

    // Whether made is still the record for subscriptionId, also as the copy a cancel that threw kept in its place
    private boolean isStillSubscribed(String subscriptionId, Subscribed made) {
        Subscribed now = subscribed.get(subscriptionId);
        return now != null && now.isFromTheSameSubscribeAs(made);
    }

    // Hands event to action once the replays have returned, and throws AttemptNotMade instead once the subscription is
    // no longer wanted
    private void acceptAfterEndedReplays(String subscriptionId, Subscribed made, CompletableFuture<Void> endedReplaysReturned, Consumer<CloudEvent> action, CloudEvent event) {
        boolean interrupted = false;
        try {
            while (!endedReplaysReturned.isDone()) {
                // Gone once a cancel has returned, since a cancel forgets the subscription after the live delegate's
                // cancel and keeps it when that throws, and once this model begins to shut down. While it is kept, the
                // event waits on and reaches the action, because a live delegate that still runs the subscription won't
                // deliver the event again, and throwing would end its delivery of the id. Once it is gone, the call
                // throws without handing the event to the action, since the ended replay can still be in the action and
                // returning would let the live delegate store its position past the event. Looked at once more as the
                // call is let in after the replays have returned, since they can return between two looks. A shutdown
                // forgets every record before the live delegate closes its runs, and a throw into a run still open
                // makes a live delegate such as the MongoDB one restart it, so the call of a record the shutdown forgot
                // waits on and reaches the action uncounted. One a cancel forgot before the shutdown throws as it would
                // without the shutdown. ACTION_WAIT after the shutdown began it throws,
                // so a live delegate whose shutdown waits for its threads without interrupting them doesn't wait for a
                // stuck replay. By then the MongoDB models, whose shutdown begins right after this model's, ignore a
                // run's failure instead of restarting the run. Only this model's own state ends the wait, since a call
                // into the live delegate here could block while it shuts down
                if (made.liveCalls().forgottenByShutdown()) {
                    if (System.nanoTime() - shutdownBeganNanos.get() >= ACTION_WAIT.toNanos()) {
                        throw liveEventWithdrawn(subscriptionId);
                    }
                } else if (!isStillSubscribed(subscriptionId, made)) {
                    throw liveEventWithdrawn(subscriptionId);
                }
                try {
                    endedReplaysReturned.get(EARLIER_ATTEMPT_POLL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException | ExecutionException ignored) {
                    // Checked again on the next round
                } catch (InterruptedException e) {
                    // Waited on rather than thrown, for the same reason. Set again only once the action has returned or
                    // thrown, since an action that reads it could skip the event as cancelled and the live delegate
                    // would then store its position past the event
                    interrupted = true;
                }
            }
            // Also a status get(..) returned without throwing for, or one set before a call that didn't wait
            interrupted |= Thread.interrupted();
            acceptIfLetIn(subscriptionId, made, action, event);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Stops the live delegate and parks every replay in flight, as {@link #stopReplay()} describes, also when parking
     * one throws. It waits up to a second for an event a replay already handed to the subscriber's action, before the
     * live delegate stops, and logs a {@code WARN} past that.
     * <p>
     * It runs alone, as {@link #runInTurn} describes. A {@link #start(boolean)}, {@code shutdown()} or
     * {@link #resumeSubscription(String)} under way is waited for, with no deadline unless code a caller supplied holds
     * it up as {@link #runInTurn} describes, and so is the live delegate's {@code stop()}. Called from inside the action of a live
     * call or a replay of this model, it returns at once and takes effect on another thread once the action has
     * returned, and what it throws there is logged at {@code ERROR}. Until then the live delegate can go on delivering
     * to the action, and the replay can go on to its next event. From any other thread it also waits for the calls such
     * actions handed over before it, and takes effect after them, or changes nothing when a {@code shutdown()} has
     * begun by then.
     *
     * @throws IllegalStateException when called on a thread that runs a replay of this model outside the subscriber's
     *                               action, such as in a {@link CatchupListener}, or from a thread on which the model
     *                               is in the middle of a call for one of its subscriptions, or when code a caller
     *                               supplied holds up its wait as {@link #runInTurn} describes. Nothing was changed
     *                               then.
     */
    @Override
    public void stop() {
        runInTurn(this, "stop()", this::stopTheReplaysAndTheLiveDelegate);
    }

    // Under the model lock. Also run by a start(..) once more for a stop() made from inside it
    private void stopTheReplaysAndTheLiveDelegate() {
        stopBegins();
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
     * that ran in the meantime, as {@link #stop()} does. Unlike {@link #stop()}, it doesn't wait for a call to the
     * subscriber's action that such a replay let through, though it tells that call its event is no longer wanted, so
     * a competing consumer's wait for its lease in the action ends. It doesn't stop again when the delegate runs
     * afterwards, or when a resume of the delegate came while the delegate was starting.
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
     * <p>
     * It reads those stored positions before it takes the model lock, as {@link StartResumes} describes, so a checkpoint
     * storage that doesn't answer for one subscription holds up that subscription's resume alone. This start resumes
     * such ones once the rest is done, without the lock, also when resuming another one threw, each on a thread of its
     * own, as soon as its own read answers, and returns once all of them have. The live delegate's own
     * {@code start(..)} runs under the lock with whatever it reads. A start made on a thread that already holds the
     * lock, from code another start runs such as a {@code StartAt.dynamic} function or the live delegate's
     * {@code start(..)}, reads none of those positions first. It resumes each of those subscriptions on that thread, one
     * after another, reading under the lock.
     * <p>
     * It runs alone, as {@link #runInTurn} describes, and waits with no deadline for a {@link #stop()},
     * {@code shutdown()} or {@link #resumeSubscription(String)} under way, unless code a caller supplied holds it up as
     * {@link #runInTurn} describes. Called from inside the action of a live call or a replay of this model, it returns at
     * once and takes effect on another thread once the action has returned, and what it throws there is logged at
     * {@code ERROR}. From any other thread it also waits for the calls such actions handed over before it, and takes
     * effect after them, or changes nothing when a {@code shutdown()} has begun by then. A {@code shutdown()} that comes while the live delegate's {@code start(..)} runs shuts the live
     * delegate down without waiting for this start, as {@link #shutDownInTurn} describes, and keeps it from resuming
     * anything once the live delegate's {@code start(..)} has returned. One under way when this start begins keeps it
     * from starting the live delegate.
     * <p>
     * A {@code shutdown()} made on the thread of this start, from code it runs such as a {@code StartAt.dynamic} function
     * or the live delegate's {@code start(..)}, returns without waiting for this start, which can't return before it. When
     * it comes before the live delegate's {@code start(..)}, this start doesn't call it. When it comes inside it, this
     * start shuts the live delegate down again once that {@code start(..)} has returned or thrown, before this start
     * returns or throws, since the live delegate may have gone on to start after the shutdown. That shutdown of the live
     * delegate doesn't change what this start returns or throws. When it throws, it is logged at {@code ERROR}, and the
     * next {@code shutdown()} calls the live delegate's {@code shutdown()} again.
     * <p>
     * A {@link #stop()} made on the thread of this start the same way takes effect at once, and the model ends stopped, as
     * if the stop had come after this start. This start resumes nothing once the stop has returned, and doesn't call the
     * live delegate's {@code start(..)} when the stop came before it. When the stop came inside that {@code start(..)}, or
     * from a method of the live delegate this start calls after it, this start runs the stop again once those calls and the
     * resumes have returned or thrown, before this start returns or throws, since the live delegate may have started after
     * the stop. That second stop doesn't change what this start returns or throws, and when it throws, it is logged at
     * {@code ERROR}. A {@code start(..)} made on the same thread after the stop starts the model again. When that start
     * throws and the model stops again, as it does when the live delegate throws, the model ends stopped all the same.
     * <p>
     * A {@link #stopReplay()} made on the thread of this start leaves the live delegate running, and this start goes on
     * to resume subscriptions. A subscription it resumes one at a time, as described above, is resumed as
     * {@link #resumeSubscription(String)} resumes it, which starts the model again when it finds the model stopped. A
     * {@link #resumeReplay()} made there after a stop made there lets replays run again, and this
     * start then stops only the live delegate again.
     *
     * @throws IllegalStateException when called on a thread that runs a replay of this model outside the subscriber's
     *                               action, such as in a {@link CatchupListener}, or from a thread on which the model
     *                               is in the middle of a call for one of its subscriptions, or when code a caller
     *                               supplied holds up its wait as {@link #runInTurn} describes. Nothing was changed
     *                               then.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        StartResumes startResumes = new StartResumes(this, List.of(this), this::resumeAlone);
        runInTurn(this, "start(..)", () -> {
            if (resumeSubscriptionsAutomatically) {
                startResumes.readBeforeTheTurn();
            }
        }, () -> {
            long shutdownsBegunBefore = shutdownsBegun(this);
            LifecycleCall call = startBegins();
            StartAttempt attempt = beginStart(call);
            final boolean resumeEachHere;
            try {
                resumeEachHere = resumeSubscriptionsAutomatically && anyResumeReplays(startResumes.reads());
            } catch (Throwable e) {
                undoStart(attempt, attempt.wasStopped() && runsAfterFailedStart(getWrappedSubscriptionModel(), e), e);
                throw e;
            }
            startUnlessStoppedFromInside(this, call, shutdownsBegunBefore, this::stopTheReplaysAndTheLiveDelegate, getWrappedSubscriptionModel()::stop, () -> {
                try {
                    startTheLiveDelegate(this, shutdownsBegunBefore, () -> getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically && !resumeEachHere));
                } catch (Throwable e) {
                    undoStart(attempt, attempt.wasStopped() && runsAfterFailedStart(getWrappedSubscriptionModel(), e), e);
                    throw e;
                }
                // A shutdown that didn't wait for this start has forgotten the catch-ups by now, so nothing is resumed
                // after it. A stop() made from inside this start has stopped the live delegate, so nothing is resumed
                // after it either. A stopReplay() made there leaves the live delegate running, and the resumes go on
                if (shutDownSince(this, shutdownsBegunBefore) || lastCallSince(call).liveDelegateStopped()) {
                    return;
                }
                List<Runnable> resumes = new ArrayList<>();
                if (resumeSubscriptionsAutomatically) {
                    resumes.add(this::relaunchParkedReplays);
                }
                if (resumeEachHere) {
                    resumes.add(() -> resumeEach(subscriptionsTheLiveDelegateHoldsPaused(), id -> {
                        if (!shutDownSince(this, shutdownsBegunBefore) && !lastCallSince(call).liveDelegateStopped()) {
                            startResumes.resumeAsReadOrAfterTheTurn(id);
                        }
                    }));
                }
                try {
                    runEach(resumes, Runnable::run);
                } finally {
                    startResumes.turnEnds(call, shutdownsBegunBefore);
                }
            });
        }, startResumes::resumeAfterTheTurn);
    }

    /**
     * The resumes a {@code start(true)} makes one at a time, of {@code models}, all of them children of {@code model}
     * when {@code model} is a dispatcher. Before its turn, outside the model lock, it reads what each resume reads from
     * storage, as {@link ResumeRead} describes, for every subscription the live delegate lists or one of {@code models}
     * made, waiting for those reads up to {@link #LIFECYCLE_WAIT}. In its turn it decides from them, and resumes each one
     * whose read is in time and still current with {@code alone}. It resumes every other one once its turn is over, also
     * when the turn threw, as {@link #resumeReadingFirst} does, unless a stop or a shutdown came since the turn. Each of
     * those runs on a thread of its own, all of them at once, and one not read in time is resumed from its read as soon
     * as that read answers, and read again when that read failed. So a checkpoint storage that doesn't answer for one
     * subscription holds up that subscription's resume alone, which this start then waits for without the lock. On a
     * thread that already holds the lock, it reads nothing before the turn, and resumes every one it left until after
     * the turn on that thread, one after another, since a thread of their own would wait for that lock.
     */
    final class StartResumes {
        private final Object model;
        private final List<AbstractCatchupSubscriptionModel> models;
        private final BiFunction<String, ResumeRead, @Nullable Subscription> alone;
        private Map<String, CompletableFuture<@Nullable ResumeRead>> reading = Map.of();
        private Map<String, ResumeRead> reads = Map.of();
        private final Set<String> afterTheTurn = new LinkedHashSet<>();
        private @Nullable LifecycleCall call;
        private long shutdownsBegunBefore;
        private long stopsAtTheTurn;

        StartResumes(Object model, List<AbstractCatchupSubscriptionModel> models, BiFunction<String, ResumeRead, @Nullable Subscription> alone) {
            this.model = model;
            this.models = models;
            this.alone = alone;
        }

        // Not on a thread that holds the model lock, such as a start made from code a start runs, whose resumes read in
        // their own turn as they did before
        void readBeforeTheTurn() {
            if (modelLock.isHeldByCurrentThread()) {
                return;
            }
            SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
            Set<String> ids = new LinkedHashSet<>(IntrospectableSubscriptions.findIn(liveDelegate).map(IntrospectableSubscriptions::subscriptionIds).orElse(Set.of()));
            for (AbstractCatchupSubscriptionModel child : models) {
                child.subscribed.forEach((id, made) -> {
                    if (made.owner() == child) {
                        ids.add(id);
                    }
                });
            }
            Map<String, CompletableFuture<@Nullable ResumeRead>> reading = new LinkedHashMap<>();
            for (String id : ids) {
                CompletableFuture<@Nullable ResumeRead> read = new CompletableFuture<>();
                Thread.ofVirtual().name("occurrent-catchup-start-read").start(() -> {
                    try {
                        read.complete(ResumeRead.read(id, models, liveDelegate));
                    } catch (Throwable e) {
                        // Every Throwable, and left until after the turn, where the resume reads again and throws it
                        read.complete(null);
                    }
                });
                reading.put(id, read);
            }
            completesWithin(CompletableFuture.allOf(reading.values().toArray(CompletableFuture<?>[]::new)), LIFECYCLE_WAIT);
            Map<String, ResumeRead> read = new HashMap<>();
            reading.forEach((id, future) -> {
                ResumeRead inTime = future.getNow(null);
                if (inTime != null) {
                    read.put(id, inTime);
                }
            });
            this.reading = reading;
            reads = read;
        }

        Map<String, ResumeRead> reads() {
            return reads;
        }

        // In the turn. One not read in time, or whose read moved, is left until after the turn
        void resumeAsReadOrAfterTheTurn(String subscriptionId) {
            ResumeRead read = reads.get(subscriptionId);
            if (read == null) {
                afterTheTurn.add(subscriptionId);
                return;
            }
            try {
                alone.apply(subscriptionId, read);
            } catch (ReadAgain moved) {
                afterTheTurn.add(subscriptionId);
            }
        }

        // At the end of the turn, so a stop after it keeps the resumes left until then from starting the model again
        void turnEnds(LifecycleCall call, long shutdownsBegunBefore) {
            this.call = call;
            this.shutdownsBegunBefore = shutdownsBegunBefore;
            this.stopsAtTheTurn = stopsOf(models);
        }

        // Each on a thread of its own, all of them at once. On the lifecycle thread they run as runPerIdCallsInOrder
        // describes, so they keep their place among the calls handed over, and what each throws is logged. Elsewhere
        // this waits for all of them, and the first failure is thrown. On a thread that still holds the model lock, such
        // as a start made from code another start runs, they run here one after another, since a thread of their own
        // would wait for that lock while this thread waits for them
        void resumeAfterTheTurn(boolean onTheLifecycleThread) {
            LifecycleCall startCall = call;
            if (startCall == null || afterTheTurn.isEmpty()) {
                return;
            }
            Consumer<String> resume = id -> resumeReadingFirst(model, id, models, stopsAtTheTurn, shutdownsBegunBefore, false, lateReadOf(id),
                    read -> lastCallSince(startCall).liveDelegateStopped() ? null : alone.apply(id, read));
            if (modelLock.isHeldByCurrentThread()) {
                resumeEach(afterTheTurn, resume);
                return;
            }
            if (!onTheLifecycleThread) {
                resumeEachAtOnce(afterTheTurn, resume);
                return;
            }
            runPerIdCallsInOrder(afterTheTurn, id -> () -> resumeEach(Set.of(id), resume), e -> logHandedCallThrew(model, "start(..)", e));
        }

        // The read made before the turn of one not read in time, once it has answered, however late. Null for one read
        // in time, which is left until now because its read moved, and for one whose read failed, so the resume reads
        // again
        private @Nullable ResumeRead lateReadOf(String subscriptionId) {
            CompletableFuture<@Nullable ResumeRead> read = reading.get(subscriptionId);
            if (read == null || reads.containsKey(subscriptionId)) {
                return null;
            }
            awaitUninterruptibly(read.thenAccept(answered -> {
            }));
            return read.getNow(null);
        }
    }

    /**
     * Runs {@code body} as {@link #runInTurn(Object, String, Runnable)} does, with {@code readFirst} run before the turn
     * and {@code afterTheTurn} after it, both without the model lock, on the thread that runs the turn or, when the turn
     * runs on the lifecycle thread for an action, there. {@code afterTheTurn} is told which, and runs once {@code body}
     * has begun, also when {@code body} threw, since it may have left work for after the turn before it threw. What
     * {@code body} threw is thrown then, with what {@code afterTheTurn} threw added as suppressed.
     */
    void runInTurn(Object model, String call, Runnable readFirst, Runnable body, Consumer<Boolean> afterTheTurn) {
        ActionOnThread action = outermostActionOnTheCallingThread();
        if (action != null) {
            handToTheLifecycleThread(action, null, () -> {
                readFirst.run();
                AtomicBoolean began = new AtomicBoolean();
                runThenAfterTheTurnOnceBegun(began, () -> underTheModelLock(() -> {
                    if (!shuttingDown) {
                        began.set(true);
                        body.run();
                    }
                }), () -> afterTheTurn.accept(true));
            }, e -> logHandedCallThrew(model, call, e));
            return;
        }
        if (replaysOrClaimsOnTheCallingThread()) {
            throw new IllegalStateException("Cannot call " + call + " from a thread on which the model is in the middle of a call for one of its subscriptions, or that runs a catch-up replay"
                    + " of this model outside the subscriber's action. Call it from another thread. " + NOTHING_CHANGED);
        }
        readFirst.run();
        AtomicBoolean began = new AtomicBoolean();
        runThenAfterTheTurnOnceBegun(began, () -> runAfterTheHandedCalls(call, null, () -> {
            began.set(true);
            body.run();
        }), () -> afterTheTurn.accept(false));
    }

    private static void runThenAfterTheTurnOnceBegun(AtomicBoolean began, Runnable turn, Runnable afterTheTurn) {
        try {
            turn.run();
        } catch (Throwable e) {
            if (began.get()) {
                try {
                    afterTheTurn.run();
                } catch (Throwable afterTheTurnFailed) {
                    e.addSuppressed(afterTheTurnFailed);
                }
            }
            throw e;
        }
        if (began.get()) {
            afterTheTurn.run();
        }
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
    boolean anyResumeReplays(Map<String, ResumeRead> reads) {
        // A resume puts its replay in resumeReplays before the replay stores the live start over the catch-up's
        // position, and the replay stays there until its handle completes. Every change a cancel makes falls between
        // its put into cancelsUnderWay and its removal, and the count moves in between. So a cancel that overlaps the
        // reads is under way at the first read of cancelsUnderWay, under way at the last, or moved the count, whatever
        // the live delegate reported during it.
        long cancelsStartedBefore = cancelsStarted.get();
        if (!cancelsUnderWay.isEmpty()) {
            return true;
        }
        boolean any = subscribed.entrySet().stream().anyMatch(entry -> entry.getValue().owner() == this && resumeWouldReplay(entry.getKey(), reads.get(entry.getKey())))
                || !resumeReplays.isEmpty()
                || handedOver.values().stream().anyMatch(made -> made.owner() == this && (made.takeOverAsked().get() || made.takingOver().get()))
                || !cancelsUnderWay.isEmpty()
                || subscribed.values().stream().anyMatch(made -> made.owner() == this && (!made.cancelledReplayReturned().isDone() || made.historyLost().get() != null));
        return any || cancelsStarted.get() != cancelsStartedBefore;
    }

    // Whether a resume of subscriptionId would replay, decided from what read read before the turn. One not read in time,
    // or whose read moved since, counts as one that would when this model may replay it, so start(true) resumes it one
    // at a time, and reads it again then. A read that threw is thrown here, as it was when this read it itself
    private boolean resumeWouldReplay(String subscriptionId, @Nullable ResumeRead read) {
        if (read == null) {
            return mayReplayToResume(subscriptionId, subscribed.get(subscriptionId));
        }
        try {
            return replayThatResumeRuns(subscriptionId, read) != null;
        } catch (ReadAgain moved) {
            return true;
        }
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
     * Resumes each of {@code subscriptionIds} as {@link #resumeEach} does, each on a thread of its own and all of them
     * at once, so one whose resume waits, such as for its checkpoint storage, holds up no other. Returns once all of
     * them have returned or thrown, and then throws as {@link #resumeEach} does, in the order of {@code subscriptionIds}.
     */
    static void resumeEachAtOnce(Set<String> subscriptionIds, Consumer<String> resume) {
        List<CompletableFuture<@Nullable Throwable>> resumes = new ArrayList<>();
        for (String subscriptionId : subscriptionIds) {
            CompletableFuture<@Nullable Throwable> resumed = new CompletableFuture<>();
            START_RESUME_THREADS.newThread(() -> {
                try {
                    resumeEach(Set.of(subscriptionId), resume);
                    resumed.complete(null);
                } catch (Throwable e) {
                    // Every Throwable, thrown on the calling thread once every resume has returned
                    resumed.complete(e);
                }
            }).start();
            resumes.add(resumed);
        }
        runEach(resumes, resumed -> {
            awaitUninterruptibly(resumed.thenAccept(failed -> {
            }));
            Throwable failed = resumed.getNow(null);
            if (failed != null) {
                throw thrownAsItIs(failed);
            }
        });
    }

    /**
     * Runs {@code step} for each of {@code items}, also when an earlier one throws, an {@link Error} or an undeclared
     * checked exception included, and then throws the first {@link Error} when a step threw one, and the first failure
     * otherwise, with the others added as suppressed. A failure thrown more than once, such as a preallocated
     * {@link OutOfMemoryError}, is thrown and not added to itself.
     */
    static <T> void runEach(Iterable<T> items, Consumer<T> step) {
        List<Throwable> failures = new ArrayList<>();
        for (T item : items) {
            try {
                step.accept(item);
            } catch (Throwable e) {
                failures.add(e);
            }
        }
        if (failures.isEmpty()) {
            return;
        }
        Throwable thrown = failures.stream().filter(Error.class::isInstance).findFirst().orElse(failures.getFirst());
        failures.forEach(failure -> addSuppressed(thrown, failure));
        throw thrownAsItIs(thrown);
    }

    // Throws throwable as it is, a checked exception thrown from Kotlin too, without declaring it. Declared to return,
    // so a caller can write throw thrownAsItIs(throwable)
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException thrownAsItIs(Throwable throwable) throws T {
        throw (T) throwable;
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
            runCallersCode(() -> listener.historyRead(attempt));
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
     * {@link IllegalStateException} without resuming it when the replay hasn't returned by then. It runs alone, so a
     * {@link #pauseSubscription(String)} or {@link #cancelSubscription(String)} for the subscription that comes meanwhile
     * doesn't run until it returns, and neither does a {@link #stop()}.
     * <p>
     * A resume while a replay for the subscription runs returns the handle of that replay. A pause asked while the replay
     * still reads history is dropped, so the replay hands over and the subscription runs.
     * <p>
     * When another call for the subscription still runs after {@link #LIFECYCLE_WAIT}, this throws
     * {@link IllegalStateException} without resuming the subscription. A model that was stopped is started by then.
     * <p>
     * It runs alone against the {@link #start(boolean)}, {@link #stop()} and {@code shutdown()} calls of this model, so
     * none of them takes effect between the moment it finds the model stopped or running and the moment it starts the
     * model or the live delegate resumes the subscription. It waits with no deadline for one of them under way, and one
     * of them made while it runs waits for it the same way. It also waits with no deadline for the calls that actions
     * of the subscription, actions that called {@code pauseSubscription}, {@code cancelSubscription}, a resume or a
     * subscribe for it, and actions that called {@code start(..)} or {@code stop()}, handed over before it, as
     * {@link #awaitTheHandedCalls(String, String)} describes, so a resume made after an action's {@code stop()} takes
     * effect after that stop. It waits up to {@link #LIFECYCLE_WAIT} for such an action to return, and then throws
     * {@link IllegalStateException} and changes nothing. An action of another subscription that handed over only calls
     * for other subscriptions doesn't hold it up, also when it never returns. When it waited for such calls and a {@code shutdown()} began meanwhile, it changes
     * nothing, and the {@code waitUntilStarted} of the handle it returns answers {@code false}.
     * While code a caller supplied, such as a {@code StartAt.dynamic} function, holds either wait up, the wait ends after
     * {@link #LIFECYCLE_WAIT} as {@link #runInTurn} describes, and this throws {@link IllegalStateException} and changes
     * nothing.
     * <p>
     * On a thread that runs a replay of this model outside the action, such as in a {@link CatchupListener}, or that is
     * in the middle of a call for one of its subscriptions, it neither waits nor runs alone, so it takes effect before
     * the calls actions handed over, and a {@link #stop()} from another thread can take effect while it runs. When the
     * model was stopped by then and the subscription is paused, it throws {@link IllegalStateException} there as
     * {@link #start(boolean)} does, and changes nothing.
     * <p>
     * Called from inside the action of a live call or a replay of this model, it returns at once and changes nothing
     * yet, also when the model runs. Once the action has returned, the lifecycle thread resumes the subscription as
     * described above, after every call the action handed over before it. So {@code stop()} and then
     * {@code resumeSubscription(id)} from one action leave the model running and the subscription resumed. A
     * {@link #pauseSubscription(String)}, {@link #resumeSubscription(String, StartAt)}, {@link #cancelSubscription(String)}
     * or subscribe the action makes afterwards takes effect after it too. The handle it
     * returns starts once the lifecycle thread has resumed the subscription, so waiting for it inside the action doesn't
     * see it start. Its {@code waitUntilStarted} answers {@code false} when a {@code shutdown()} has begun by then, and
     * throws what the resume threw there, which is also logged at {@code ERROR}. In 0.34.0 the resume ran on the
     * action's thread before the action returned.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        Subscription handedOver = resumeOnceTheActionHasReturned(this, "resumeSubscription(" + subscriptionId + ")", subscriptionId, () -> resumeSubscription(subscriptionId));
        if (handedOver != null) {
            return handedOver;
        }
        return resumeReadingFirst(this, subscriptionId, List.of(this), read -> resumeAlone(subscriptionId, read));
    }

    // Run in turn with the lifecycle calls, see resumeInTurn, so no start, stop or shutdown completes between the reads
    // of stopped below and the start or the live delegate's resume they decide. Reads no storage, see ResumeRead
    private Subscription resumeAlone(String subscriptionId, ResumeRead read) {
        // Started before the claim is taken, since a start that fails parks the replays of other ids under their claims
        boolean started = stopped && isPaused(subscriptionId);
        if (started) {
            start(false);
        }
        try (Claim ignored = claim(subscriptionId, started ? STARTED_BUT_NOT_RESUMED : NOTHING_CHANGED)) {
            // A replay a stop gave up on goes on once this resume is asked, as one it runs again does
            try (HandoverLock alsoIgnored = lockHandover(subscriptionId)) {
                clearTheParkOwedBy(subscriptionId);
            }
            // A pause asked during a replay is taken off in replayToResume, and by the relaunch of a parked replay
            if (hasParkedReplay(subscriptionId)) {
                Subscription relaunched = relaunchParkedReplay(subscriptionId);
                if (relaunched != null) {
                    return relaunched;
                }
            }
            String whatChanged = started ? STARTED_BUT_NOT_RESUMED : NOTHING_CHANGED;
            Subscription replaying = replayToResume(subscriptionId, whatChanged, read);
            if (replaying != null) {
                return replaying;
            }
            awaitTheCancelledReplay(subscriptionId, whatChanged);
            Subscription resumed = resumeTheLiveDelegateAsRead(read);
            liveDelegateResumed();
            return resumed;
        }
    }

    /**
     * Resumes {@code subscriptionId} with {@code alone}, a resume of {@code model} run in turn as {@link #resumeInTurn}
     * describes, from what a {@link ResumeRead} read before the turn, outside the model lock. So a checkpoint storage
     * that doesn't answer for the subscription holds up this resume alone, and no other subscription's resume, no
     * {@link #stop()} and no {@code shutdown()}.
     * <p>
     * When {@code alone} finds that what was read moved before the turn used it, by throwing {@link ReadAgain}, this
     * reads again and runs another turn. When a stop of any of {@code models} took effect after the read began, it reads
     * again too, so the resume takes effect after that stop, as a resume made once it had returned would, and never
     * from a position read before it. When a {@code shutdown()} of {@code model} began after it, the turn changes
     * nothing, and the handle answers {@code false} from {@code waitUntilStarted}. {@code alone} returning {@code null}
     * does the same.
     */
    Subscription resumeReadingFirst(Object model, String subscriptionId, List<AbstractCatchupSubscriptionModel> models, Function<ResumeRead, @Nullable Subscription> alone) {
        awaitTheHandedCalls(subscriptionId, "resumeSubscription(" + subscriptionId + ")");
        return resumeReadingFirst(model, subscriptionId, models, stopsOf(models), shutdownsBegun(model), true, null, alone);
    }

    // As above, from the stops and shutdowns counted when the caller began. When a stop came since and throwsOnceStopped
    // is false, the turn changes nothing as it does after a shutdown, since the start whose resume this is came before it.
    // The first turn uses readBefore when there is one, a read the caller made, and reads only when it moved
    private Subscription resumeReadingFirst(Object model, String subscriptionId, List<AbstractCatchupSubscriptionModel> models, long stopsBefore, long shutdownsBegunBefore,
                                            boolean throwsOnceStopped, @Nullable ResumeRead readBefore, Function<ResumeRead, @Nullable Subscription> alone) {
        long[] stopsAtTheRead = {stopsBefore};
        ResumeRead notYetUsed = readBefore;
        try {
            while (true) {
                ResumeRead read = notYetUsed != null ? notYetUsed : ResumeRead.read(subscriptionId, models, getWrappedSubscriptionModel());
                notYetUsed = null;
                try {
                    return resumeInTurn(subscriptionId, () -> {
                        if (shutDownSince(model, shutdownsBegunBefore)) {
                            return null;
                        }
                        long stopsNow = stopsOf(models);
                        if (stopsNow != stopsAtTheRead[0]) {
                            if (!throwsOnceStopped) {
                                return null;
                            }
                            // Read after the stop, so the resume takes effect after it, as one made then would
                            stopsAtTheRead[0] = stopsNow;
                            throw ReadAgain.MOVED;
                        }
                        return alone.apply(read);
                    });
                } catch (ReadAgain moved) {
                    moved.beforeReadingAgain.run();
                }
            }
        } finally {
            handedCallBegins(subscriptionId);
        }
    }

    private static long stopsOf(List<AbstractCatchupSubscriptionModel> models) {
        long stops = 0;
        for (AbstractCatchupSubscriptionModel model : models) {
            stops += model.stopsAsked();
        }
        return stops;
    }

    /**
     * What a resume of one subscription reads from storage, read before its turn and outside the model lock, so a
     * checkpoint storage that doesn't answer holds up only that resume. For each catch-up model, the position stored for
     * the subscription that decides whether it replays, and for a live delegate that {@link PreparedResumeSubscriptions}
     * splits, the resume it prepared. A failure to read is kept and thrown where the read is used, as it was thrown when
     * the read was made there.
     * <p>
     * What was read is used in the turn only while the subscription has the same record, and no write or delete of its
     * stored position by these models, or by the live delegate on their behalf, has returned since the read began. A
     * catch-up model checks the first two, and the live delegate checks its own writes in
     * {@link PreparedResumeSubscriptions#resumeAsPrepared}. Otherwise the turn throws {@link ReadAgain}.
     */
    static final class ResumeRead {
        private final String subscriptionId;
        private final IdentityHashMap<AbstractCatchupSubscriptionModel, StoredForResume> stored;
        private final SubscriptionModel liveDelegate;
        private final @Nullable PreparedResumeSubscriptions split;
        private final PreparedResumeSubscriptions.@Nullable PreparedResume prepared;
        private final @Nullable Throwable preparationFailed;

        private ResumeRead(String subscriptionId, IdentityHashMap<AbstractCatchupSubscriptionModel, StoredForResume> stored, SubscriptionModel liveDelegate,
                           @Nullable PreparedResumeSubscriptions split, PreparedResumeSubscriptions.@Nullable PreparedResume prepared, @Nullable Throwable preparationFailed) {
            this.subscriptionId = subscriptionId;
            this.stored = stored;
            this.liveDelegate = liveDelegate;
            this.split = split;
            this.prepared = prepared;
            this.preparationFailed = preparationFailed;
        }

        static ResumeRead read(String subscriptionId, List<AbstractCatchupSubscriptionModel> models, SubscriptionModel liveDelegate) {
            IdentityHashMap<AbstractCatchupSubscriptionModel, StoredForResume> stored = new IdentityHashMap<>();
            for (AbstractCatchupSubscriptionModel model : models) {
                stored.put(model, model.readStoredForResume(subscriptionId));
            }
            PreparedResumeSubscriptions split = PreparedResumeSubscriptions.findIn(liveDelegate).orElse(null);
            PreparedResumeSubscriptions.PreparedResume prepared = null;
            Throwable preparationFailed = null;
            if (split != null) {
                try {
                    prepared = split.prepareResume(subscriptionId);
                } catch (Throwable e) {
                    // Throwable, since a live delegate written in Kotlin can throw a checked exception
                    preparationFailed = e;
                }
            }
            return new ResumeRead(subscriptionId, stored, liveDelegate, split, prepared, preparationFailed);
        }
    }

    // What replayThatResumeRuns reads from storage for one model, with what it was read under. Asked is false when the
    // read found the subscription not one this model would replay, so nothing was read
    private record StoredForResume(@Nullable Subscribed made, @Nullable Long positionMovedAt, boolean asked, @Nullable Checkpoint stored,
                                   @Nullable Throwable failed) {
    }

    /**
     * Thrown in the turn of a resume when what its {@link ResumeRead} read moved before the turn used it, so the resume
     * reads again outside the model lock and runs another turn. {@code beforeReadingAgain} runs first, outside the lock
     * too. Has no stack trace, since it is a signal and never reaches a caller.
     */
    static final class ReadAgain extends RuntimeException {
        private static final ReadAgain MOVED = new ReadAgain(() -> {
        });
        private final Runnable beforeReadingAgain;

        private ReadAgain(Runnable beforeReadingAgain) {
            super(null, null, false, false);
            this.beforeReadingAgain = beforeReadingAgain;
        }
    }

    // Read with no lock held, for every subscription this model made, paused or not, so nothing but the storage is asked
    // here. Whether the live delegate holds it paused, and what a dynamic start answers, are asked in the turn, as before
    private StoredForResume readStoredForResume(String subscriptionId) {
        Long positionMovedAt = positionMoves.get(subscriptionId);
        Subscribed made = subscribed.get(subscriptionId);
        if (made == null || made.owner() != this || shuttingDown) {
            return new StoredForResume(made, positionMovedAt, false, null, null);
        }
        try {
            Checkpoint stored = returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            return new StoredForResume(made, positionMovedAt, true, stored, null);
        } catch (Throwable e) {
            // Throwable, since a storage written in Kotlin can throw a checked exception
            return new StoredForResume(made, positionMovedAt, true, null, e);
        }
    }

    // Whether a resume of subscriptionId, made through this model, may replay from what is stored for it. While a cancel
    // of it is under way the stored position alone decides, whatever the live delegate reports as paused
    private boolean mayReplayToResume(String subscriptionId, @Nullable Subscribed made) {
        return made != null && made.owner() == this && !shuttingDown
               && (cancelsUnderWay.containsKey(subscriptionId) || getWrappedSubscriptionModel().isPaused(subscriptionId));
    }

    /**
     * Resumes the subscription {@code read} was read for in the live delegate, as {@link SubscriptionModel#resumeSubscription(String)}
     * would, without reading storage when the live delegate is a {@link PreparedResumeSubscriptions}. Throws
     * {@link ReadAgain} when the live delegate finds that what it prepared moved, and what preparing it threw. A live
     * delegate that can't split its resume is resumed here, with what it reads, and a {@code shutdown()} waiting for the
     * model lock meanwhile shuts it down at once.
     */
    Subscription resumeTheLiveDelegateAsRead(ResumeRead read) {
        if (read.preparationFailed != null) {
            throw thrownAsItIs(read.preparationFailed);
        }
        if (read.split != null && read.prepared != null) {
            return read.split.resumeAsPrepared(read.subscriptionId, read.prepared).orElseThrow(() -> ReadAgain.MOVED);
        }
        return whileWaitingForOneSubscription(() -> read.liveDelegate.resumeSubscription(read.subscriptionId));
    }

    // Runs wait with perIdWaitUnderTheLockSince set when this thread holds the model lock, so a shutdown() waiting for
    // the lock shuts the live delegate down meanwhile, see shutDownTheLiveDelegateInTurn
    private <T> T whileWaitingForOneSubscription(Supplier<T> wait) {
        boolean marked = modelLock.isHeldByCurrentThread() && perIdWaitUnderTheLockSince.compareAndSet(0, Math.max(1, System.nanoTime()));
        try {
            return wait.get();
        } finally {
            if (marked) {
                perIdWaitUnderTheLockSince.set(0);
            }
        }
    }

    /**
     * Records that the position stored for {@code subscriptionId} may have moved, once a write or a delete of it through
     * the checkpoint storage or the live delegate has returned or thrown, so a resume that read it before drops what it
     * read. See {@link ResumeRead}. Nothing is kept for a subscription these models have no record of, such as one a
     * cancel forgot or an unknown id a cancel was asked for, as {@link #forgetPositionMovesUnlessSubscribed} describes.
     */
    void positionMayHaveMoved(String subscriptionId) {
        positionMoves.put(subscriptionId, positionMoveCount.incrementAndGet());
        forgetPositionMovesUnlessSubscribed(subscriptionId);
    }

    /**
     * Forgets the position moves of {@code subscriptionId} when the subscription has no record in {@link #subscribed},
     * so {@link #positionMoves} keeps an entry only for a subscription these models still have, never for every id
     * they have seen. A read that the forgotten entry would have moved is dropped all the same, since it was read for a
     * record that is gone by then, or for none. The record is looked for while the entry is held, so a write that
     * returns meanwhile puts its entry once this has looked, and is kept.
     */
    void forgetPositionMovesUnlessSubscribed(String subscriptionId) {
        positionMoves.computeIfPresent(subscriptionId, (id, movedAt) -> subscribed.containsKey(id) ? movedAt : null);
    }

    /**
     * Resumes the subscription the live delegate holds paused at {@code startAt}, through whichever
     * {@link RepositionableSubscriptions} the live delegate resolves to. Unlike {@link #resumeSubscription(String)}, it
     * doesn't start this model first or run a parked replay, and for a subscription the live delegate holds it never
     * replays, also not at a position a catch-up stores. The live delegate handles such a position as one it cannot open
     * its live feed at. A subscription held before it reached the live delegate is the exception, described below.
     * <p>
     * A subscription this model made that runs is refused with {@link SubscriptionAlreadyRunningException} before
     * anything is stored and before a dynamic {@code startAt} is asked. It runs when it isn't paused, the replay its
     * {@code subscribe(..)} started didn't end before the live delegate got it, and no replay that
     * {@link #resumeSubscription(String)} started runs for it or is about to start to take over a live start the live
     * delegate lost. That includes one whose {@code subscribe(..)} replay still runs and one the live delegate is still
     * opening. This model decides it from what it knows itself, since a live delegate can run a subscription before it
     * reports it as running. In 0.34.0 the live delegate was asked to resume it and threw its own exception. A
     * subscription paused while its {@code subscribe(..)} replay runs isn't refused, and the live delegate, which doesn't
     * hold it yet, throws its own exception, as in 0.34.0. A subscription whose live delegate gave up restarting it after
     * it went live still runs here, as it did on the MongoDB live delegates in 0.34.0. Pause it before you resume it at a
     * position, or cancel it and subscribe it again.
     * <p>
     * A replay that {@link #resumeSubscription(String)} started for the subscription, running or waiting to run again, is held first
     * and ends once the live delegate has resumed the subscription, so the position given here wins over it. With
     * checkpoint storage, when the position stored for the subscription is one a catch-up replays from, such as the one
     * that replay stored, it is replaced before the live delegate resumes the subscription, so a
     * {@link #resumeSubscription(String)} after a later pause doesn't replay from it. While a replay that
     * {@code subscribe(..)} started runs for a subscription a pause was asked for during that replay, nothing is stored
     * and no present position is read, since that replay reads the stored position to decide where it goes live from
     * when it hands over. It is replaced
     * only by a position a restart starts reading at:
     * <ul>
     * <li>A checkpoint the live delegate answers {@code true} for in {@link RepositionableSubscriptions#restartsAt(Checkpoint)}
     * is stored as given.</li>
     * <li>A checkpoint this model replays from with a live start, or one at the beginning, is stored as given.</li>
     * <li>A time after the beginning that this model replays from without a live start is stored with the present
     * position as its live start. A DCB catch-up doesn't replay from a time, so it stores a time only by the first
     * rule.</li>
     * <li>A start from now stores the present position by the first rule, and the live delegate then resumes the
     * subscription from that position.</li>
     * </ul>
     * The present position is read the way a paragraph below describes for a subscription held before it reached the
     * live delegate. When no position is answered then, this throws {@link IllegalStateException} and changes nothing.
     * A global position above 0 without a live start, a checkpoint the live delegate doesn't restart at and any other
     * start store nothing, and the position stored before stays. A dynamic {@code startAt} is asked once for this
     * model, and the live delegate resumes the subscription at the position it answered. When it answers nothing for
     * this model, the live delegate asks it for itself. When
     * the live delegate throws, the position stored before is put back as for that subscription, a replay this held
     * runs again as it was, and this throws what the live delegate threw. When the replay is in the subscription's action or reads
     * history, this holds the replay and waits up to ten seconds for it to return before the live delegate resumes the
     * subscription, so the replay's action doesn't run alongside the action of the resumed subscription. Like a
     * {@link #stop()}, it first tells the call to the action that the replay let through that its event is no longer
     * wanted, so an action that waits for a competing consumer's lease returns, and the replay delivers that event again
     * if it runs again. After a
     * {@link #cancelSubscription(String)} that threw, it waits the same way for the replay that cancel
     * ended, which nothing runs again, also when a {@link #stop()} came before or after the cancel. After a
     * {@link #stop()}, it waits the same way for a position store that a replay a {@code subscribe(..)} started began
     * with {@link PersistCheckpointDuringCatchupPhase} before the stop, since the stop doesn't wait for it, so the store
     * doesn't write over the position this stores. When the replay or the store doesn't return in time, this logs at
     * {@code ERROR}, runs a replay it held again as it was and throws {@link IllegalStateException}, with what running it
     * again threw added as suppressed. It stores nothing then, so the caller can call it again once the action or the
     * store has returned, for example by retrying after a while. A replay this held stays held
     * instead of running again when this model was stopped meanwhile or the {@link CatchupListener} throws as it starts,
     * and the {@link IllegalStateException} says so. It runs again on a resume or a start that resumes. Called from
     * inside the action, on the replay's own thread, before that action handed a call over, this throws
     * {@link IllegalStateException} at once and changes nothing. An action that hands the call to another thread and
     * waits for it fails once the ten seconds have passed.
     * <p>
     * Called from inside the action of a live call or a replay of this model after that action handed a call over, such
     * as a {@link #stop()} or a {@link #resumeSubscription(String)}, it returns at once a handle and resumes the
     * subscription on the lifecycle thread, once the action has returned and that call has run. The handle's
     * {@code waitUntilStarted} throws what the resume threw there, which is also logged at {@code ERROR}.
     * <p>
     * It runs alone. A pause, resume, cancel or subscribe for the
     * same id waits until it returns, for up to ten seconds, and then acts on what it left. While it runs, a subscription
     * that read as running when it began reads as running and not paused, also while it waits for the action, so a
     * caller that pauses what runs calls {@link #pauseSubscription(String)}, which pauses it once this has returned. One
     * that read as paused reads as paused until this returns.
     * <p>
     * Once the live delegate returns from the resume, this model is started again if a start whose live delegate threw
     * stopped it, so a subscription made afterwards replays at once. After {@link #stop()} it stays stopped.
     * <p>
     * A subscription paused because the live delegate lost the history it resumed from is resumed by the live delegate
     * here too. Once that returns, a later {@link #resumeSubscription(String)} or {@link #start(boolean)} may no longer
     * refuse it, even when the history from {@code startAt} is gone as well. A resume that opens the subscription where
     * the history is gone pauses it again when the live delegate reports the loss again, so nothing is skipped. When the
     * live delegate throws, it stays refused.
     * <p>
     * A subscription is held before it reaches the live delegate when the replay its {@code subscribe(..)} started
     * lost the live start it would go live from. This then subscribes it from {@code startAt}, with the filter and
     * action it was made with, as a subscribe at {@code startAt} would, in place of resuming it in the live delegate.
     * The replay it was held for doesn't run again. The position stored for the subscription is replaced by the rules
     * above, whatever position it is, so a resume after a pause or a restart doesn't replay from where the held replay
     * got to. A start that stores nothing by those rules keeps the position the held replay stored. When it starts now,
     * the present position is the live start the event store reports, or the live delegate's present position when the
     * event store reports none, and the subscription goes live from that position when it is stored, so an event that
     * commits between the store and the subscribe is delivered. When that subscribe throws, the loss stays recorded, so
     * a later {@link #resumeSubscription(String)} still refuses, and the position stored before is put back with the
     * write condition the replacing write had, so a position another lease holder stored since refuses it. When nothing
     * was stored before, the new position stays. When it starts now with checkpoint storage and neither the event store
     * nor the live delegate answers a position, this throws {@link IllegalStateException} and changes nothing.
     * <p>
     * A loss the subscribe in place records, or the replay it starts records, is kept, so a later
     * {@link #resumeSubscription(String)} refuses.
     * <p>
     * {@link RepositionableSubscriptions#findIn} finds this model only when the live delegate is repositionable.
     *
     * @throws UnsupportedOperationException      if the live delegate is not itself repositionable.
     * @throws SubscriptionAlreadyRunningException if this model made the subscription and it runs, as described above.
     * @throws IllegalStateException               if a replay in the way, or a position store a stopped replay began,
     *                                             doesn't return within ten seconds, or if called from inside the
     *                                             action, as described above.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        Subscription handedOver = subscriptionBehindTheCallsTheActionHandedOver(this, "resumeSubscription(" + subscriptionId + ", " + startAt + ")", subscriptionId,
                () -> resumeSubscription(subscriptionId, startAt));
        if (handedOver != null) {
            return handedOver;
        }
        return resumeSubscription(subscriptionId, startAt, List.of(this), () -> isRunning(subscriptionId), () -> isPaused(subscriptionId));
    }

    /**
     * Resumes {@code subscriptionId} at {@code startAt} as {@link #resumeSubscription(String, StartAt)} describes, ending
     * a replay in the way on any of {@code models}, which are this model or every child of the dispatcher this model is
     * a child of. They share the claim, the live delegate and what runs a resume at a given position. {@code running}
     * and {@code paused} tell whether the subscription reads as running and as paused on the model the caller called.
     */
    Subscription resumeSubscription(String subscriptionId, StartAt given, List<AbstractCatchupSubscriptionModel> models, BooleanSupplier running, BooleanSupplier paused) {
        awaitTheHandedCalls(subscriptionId, "resumeSubscription(" + subscriptionId + ", " + given + ")");
        StartAt startAt = askedAsCallersCode(given);
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(liveDelegate)
                .orElseThrow(() -> new UnsupportedOperationException(liveDelegate.getClass().getSimpleName() + " is not repositionable"));
        try (Claim ignored = claim(subscriptionId)) {
            boolean readsAsPaused = paused.getAsBoolean();
            // Decided here and not by the live delegate, which can run the subscription before it reports it as running,
            // as one that opens after its subscribe returned does
            Subscribed known = subscribed.get(subscriptionId);
            if (known != null && !known.didNotGoLive().get() && !readsAsPaused && !isReplayingToResume(subscriptionId)) {
                throw new SubscriptionAlreadyRunningException(subscriptionId);
            }
            // Only one that runs, so an unknown or paused one reads as it did, also when this fails
            boolean marked = !readsAsPaused && running.getAsBoolean() && repositioning.add(subscriptionId);
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
                } finally {
                    // Also when a hold threw, since a shutdown tells only the replays it finds running and nothing else
                    // tells the action of a replay held here
                    inTheWay.forEach(ReplayInTheWay::tellTheActionItIsUnwanted);
                }
                // Waited for with no handover lock held, since a held replay takes that lock once its action returns
                awaitTheActions(subscriptionId, inTheWay);
                // The position given here wins over the live start the last replay handed over, and over a takeover
                // the live delegate's loss of it asked for, which is asked for again when the live delegate throws
                HandedOver dropped = handedOver.remove(subscriptionId);
                // A held replay delivers nothing, and only a call holding the claim runs it again, so it ends only once
                // the live delegate has resumed and is put back when that throws
                // A replay a subscribe started never reached the live delegate, so the resume subscribes the catch-up at
                // startAt in its place
                ReplayInTheWay neverWentLive = inTheWay.stream().filter(ReplayInTheWay::neverWentLive).findFirst().orElse(null);
                Subscription resumed;
                StoredPositionReplaced replaced = null;
                // The checkpoint this resume hands the live delegate, whose loss the clear below keeps. Null for a start
                // the live delegate resolves itself, whose loss it reports again when it resumes the subscription again.
                @Nullable Checkpoint givenPosition = resumedAt(startAt);
                try {
                    if (neverWentLive != null) {
                        resumed = neverWentLive.subscribeInPlaceOfTheHeldReplay(startAt, repositionable);
                    } else {
                        // A position a catch-up replays from makes a later resume replay from it, and the live delegate
                        // may not store one of its own over it, so the position given here replaces it when a restart
                        // starts from it. A start from now goes live from the present position it stores, as the
                        // subscribe in place does. The child
                        // that made the subscription reads that position, since each child reads its own event store.
                        Subscribed owned = subscribed.get(subscriptionId);
                        AbstractCatchupSubscriptionModel owner = owned == null ? this : owned.owner();
                        // A dynamic start is asked once here, and what it answered decides what is stored and where
                        // the live delegate resumes. A wrapper is still asked whether it starts at all, as at a
                        // subscribe. When it answers nothing here, the live delegate asks it for itself.
                        StartAt answered = answeredForThisModel(startAt);
                        @Nullable StartAt forThisModel = answered.isDynamic() ? answered.get(generateSubscriptionModelContext()) : answered;
                        // Nothing is stored while a replay a subscribe started runs after a pause asked for during
                        // it, since its handover reads what is stored to decide where it goes live from
                        replaced = aSubscribeReplays(subscriptionId) ? null : owner.replaceTheStoredPosition(subscriptionId, forThisModel, true, repositionable);
                        Checkpoint present = replaced == null ? null : replaced.present;
                        StartAt liveStartAt = present != null ? StartAt.checkpoint(present) : forThisModel;
                        if (liveStartAt != null) {
                            givenPosition = resumedAt(liveStartAt);
                        }
                        resumed = repositionable.resumeSubscription(subscriptionId, liveStartAt == null ? startAt : goingLiveFrom(answered, liveStartAt));
                    }
                } catch (Throwable e) {
                    // Throwable, since a live delegate written in Kotlin can throw a checked exception
                    if (replaced != null) {
                        replaced.putBack(e);
                    }
                    if (dropped != null) {
                        handedOver.putIfAbsent(subscriptionId, dropped);
                    }
                    putBack(inTheWay, e);
                    throw e;
                }
                // Cleared only once the live delegate has resumed. The run that lost the history was paused before
                // that, so its listener read a generation this clear moves past. Its afterPause records nothing after
                // this clear, and what it recorded before is cleared here, though its error may already be logged, so
                // the log says that error no longer applies. A loss of the position given here is kept, since the
                // run this resume started can report it, unless a late report of the old run replaced it first.
                // The subscribe in place of a held replay cleared it before it started a replay of its own.
                Subscribed made = subscribed.get(subscriptionId);
                if (neverWentLive == null && made != null) {
                    logResumedAfterLoss(subscriptionId, startAt, made.historyLost().clear(givenPosition));
                }
                inTheWay.forEach(ReplayInTheWay::end);
                models.forEach(AbstractCatchupSubscriptionModel::liveDelegateResumed);
                return resumed;
            } finally {
                positionMayHaveMoved(subscriptionId);
                if (marked) {
                    repositioning.remove(subscriptionId);
                }
            }
        }
    }

    // Whether a replay a subscribe started runs for the id on any child of the dispatcher, which share the attempts. Only
    // a replay a resume started runs again, so with the claim held none starts before the caller returns
    private boolean aSubscribeReplays(String subscriptionId) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            CatchupAttempt running = currentAttempt.get(subscriptionId);
            return running != null && !running.replay.resuming;
        }
    }

    // Stores the checkpoint forThisModel gives, or the present position for a start from now, and null when it stores
    // nothing. With onlyOverACatchupPosition only over a position a catch-up replays from, since only that makes a later
    // resume replay. Stores only a position a restart starts from, as restartPosition decides
    private @Nullable StoredPositionReplaced replaceTheStoredPosition(String subscriptionId, @Nullable StartAt forThisModel, boolean onlyOverACatchupPosition,
                                                                      RepositionableSubscriptions liveDelegate) {
        boolean startsNow = forThisModel != null && forThisModel.isNow();
        if (!startsNow && !(forThisModel instanceof StartAt.StartAtCheckpoint)) {
            return null;
        }
        return this.<StoredPositionReplaced, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
            Checkpoint previous = cfg.storage().read(subscriptionId);
            if (onlyOverACatchupPosition && (previous == null || !isCatchupCheckpoint(previous))) {
                return null;
            }
            Checkpoint present = startsNow ? presentPositionToResumeAt(subscriptionId, "StartAt.now()") : null;
            Checkpoint given = forThisModel instanceof StartAt.StartAtCheckpoint at ? at.checkpoint : present;
            Checkpoint position = restartPosition(subscriptionId, given, liveDelegate);
            if (position == null) {
                return null;
            }
            CheckpointWriteCondition writeCondition = writeConditionFor(cfg, subscriptionId);
            try {
                cfg.storage().save(subscriptionId, position, writeCondition);
            } finally {
                positionMayHaveMoved(subscriptionId);
            }
            return new StoredPositionReplaced(subscriptionId, cfg, previous, writeCondition, present);
        }).orElse(null);
    }

    // The position to store for a resume at given so a restart starts where the resume does, or null when none does. A
    // position the live delegate doesn't restart at, and a global position above 0 without a live start, which a
    // catch-up refuses to start after, are not stored, as in DurableSubscriptionModel. A time after the beginning that
    // this model replays from without a live start gets the present position as its live start, since a restart refuses
    // a stored time without one. A checkpoint this model doesn't replay from, such as a time on a DCB catch-up, is
    // stored only when the live delegate restarts at it.
    private @Nullable Checkpoint restartPosition(String subscriptionId, Checkpoint given, RepositionableSubscriptions liveDelegate) {
        if (!replaysFrom(given)) {
            return liveDelegate.restartsAt(given) ? given : null;
        }
        CatchupCheckpoints.Start start = CatchupCheckpoints.parse(given);
        if (start.liveFrom() != null || originAfterTheBeginning(start) == null) {
            return given;
        }
        if (start instanceof CatchupCheckpoints.Time time) {
            return new StringBasedCheckpoint(CatchupCheckpoints.format(time.time(), time.replayOrigin(), presentPositionToResumeAt(subscriptionId, given.asString())));
        }
        return null;
    }

    // Puts back what was stored before, with the condition the replacing write had, so a position another lease holder
    // stored since refuses it. With nothing stored before, the new position stays, since a delete could remove a position
    // another lease holder stored since
    private static final class StoredPositionReplaced {
        private final String subscriptionId;
        private final UseCheckpointInStorage cfg;
        private final @Nullable Checkpoint previous;
        private final CheckpointWriteCondition writeCondition;
        // The present position stored for a start from now, null when a given checkpoint was stored
        private final @Nullable Checkpoint present;

        private StoredPositionReplaced(String subscriptionId, UseCheckpointInStorage cfg, @Nullable Checkpoint previous, CheckpointWriteCondition writeCondition, @Nullable Checkpoint present) {
            this.subscriptionId = subscriptionId;
            this.cfg = cfg;
            this.previous = previous;
            this.writeCondition = writeCondition;
            this.present = present;
        }

        void putBack(Throwable subscribeFailure) {
            if (previous == null) {
                return;
            }
            try {
                cfg.storage().save(subscriptionId, previous, writeCondition);
            } catch (Throwable putBackFailure) {
                // Throwable, since a storage written in Kotlin can throw a checked exception
                if (putBackFailure != subscribeFailure) {
                    subscribeFailure.addSuppressed(putBackFailure);
                }
            }
        }
    }

    // The live start the event store reports, as a replay reads it, or the live delegate's present position when the
    // event store reports none
    private Checkpoint presentPositionToResumeAt(String subscriptionId, String requested) {
        String liveStart;
        try (ReplayScope<?> scope = openScope(null)) {
            liveStart = scope.liveStart();
        }
        Checkpoint present = liveStart == null ? subscriptionModel.globalCheckpoint() : new StringBasedCheckpoint(liveStart);
        if (present == null) {
            throw new IllegalStateException("The live subscription model " + subscriptionModel.getClass().getName() + " answered nothing when asked for the current position, so subscription "
                                            + subscriptionId + " can't be resumed at " + requested + " with the position stored. " + NOTHING_CHANGED);
        }
        return present;
    }

    private static @Nullable Checkpoint resumedAt(StartAt startAt) {
        return startAt instanceof StartAt.StartAtCheckpoint given ? given.checkpoint : null;
    }

    private static void logResumedAfterLoss(String subscriptionId, StartAt startAt, @Nullable Checkpoint cleared) {
        if (cleared != null) {
            String lost = cleared.asString();
            log.info(HistoryLossMessages.RESUMED_AFTER_LOSS_LOG, subscriptionId, startAt, lost);
        }
    }

    // Runs each replay in the way as it was before the resume at a given position held it, with what fails added to e
    private static void putBack(List<ReplayInTheWay> inTheWay, Throwable e) {
        try {
            runEach(inTheWay, ReplayInTheWay::putBack);
        } catch (Throwable putBackFailure) {
            // Throwable, since runEach rethrows what a step threw as it was thrown
            addSuppressed(e, putBackFailure);
        }
    }

    /**
     * Tells {@code listener} about each subscription this model holds paused because the live delegate lost the live
     * start its replay would go live from, and passes it on to the live delegate, so a start position the live delegate
     * refuses still reaches it. This model is found before the live delegate when a caller looks the capability up, so
     * without passing it on, those refusals would never reach the listener.
     */
    @Override
    public void addStartRefusalListener(StartRefusalListener listener) {
        Objects.requireNonNull(listener, StartRefusalListener.class.getSimpleName() + " cannot be null");
        addHeldPausedListener(listener);
        StartRefusalReportingSubscriptions.findIn(getWrappedSubscriptionModel()).ifPresent(liveDelegate -> liveDelegate.addStartRefusalListener(listener));
    }

    @Override
    public void removeStartRefusalListener(StartRefusalListener listener) {
        Objects.requireNonNull(listener, StartRefusalListener.class.getSimpleName() + " cannot be null");
        removeHeldPausedListener(listener);
        StartRefusalReportingSubscriptions.findIn(getWrappedSubscriptionModel()).ifPresent(liveDelegate -> liveDelegate.removeStartRefusalListener(listener));
    }

    /**
     * True while the replay held for {@code refusal} is still parked, so a resume, a {@code start(true)} that runs it
     * again, a resume at a given position or a cancel ends it. Asks the live delegate too, since the listener is passed
     * on to it.
     */
    @Override
    public boolean isHeldPausedFor(String subscriptionId, CatchupCannotResumeException refusal) {
        return holdsTheReplayHeldFor(subscriptionId, refusal)
               || StartRefusalReportingSubscriptions.findIn(getWrappedSubscriptionModel()).map(liveDelegate -> liveDelegate.isHeldPausedFor(subscriptionId, refusal)).orElse(false);
    }

    /**
     * Answers what the live delegate answers, since only the live delegate holds a subscription paused for want of a
     * position at the present, and the listener is passed on to it.
     */
    @Override
    public boolean isHeldPausedFor(String subscriptionId, UnsupportedStartAtException refusal) {
        return StartRefusalReportingSubscriptions.findIn(getWrappedSubscriptionModel()).map(liveDelegate -> liveDelegate.isHeldPausedFor(subscriptionId, refusal)).orElse(false);
    }

    /**
     * Answers what the live delegate answers, since only the live delegate refuses a start position after
     * {@code subscribe} has returned, and {@code false} when it doesn't report its refusals.
     */
    @Override
    public boolean reportsStartRefusals() {
        return StartRefusalReportingSubscriptions.findIn(getWrappedSubscriptionModel()).map(StartRefusalReportingSubscriptions::reportsStartRefusals).orElse(false);
    }

    // Without asking the live delegate, for a dispatcher whose children share one
    boolean holdsTheReplayHeldFor(String subscriptionId, CatchupCannotResumeException refusal) {
        ParkedReplay parked = parkedReplays.get(subscriptionId);
        return parked != null && parked.replay().heldFor == refusal;
    }

    // Without passing it on to the live delegate, for a dispatcher whose children share one
    void addHeldPausedListener(StartRefusalListener listener) {
        heldPausedListeners.add(listener);
    }

    void removeHeldPausedListener(StartRefusalListener listener) {
        heldPausedListeners.remove(listener);
    }

    // On the replay's own thread, with no lock of this model held, as the listener contract says
    private void reportHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
        for (StartRefusalListener listener : heldPausedListeners) {
            try {
                runCallersCode(() -> listener.handleHeldPaused(subscriptionId, refusal));
            } catch (RuntimeException e) {
                log.error("A start refusal listener threw when told that subscription {} is held paused.", subscriptionId, e);
            }
        }
    }

    /**
     * What the live delegate answers, and {@code false} when it doesn't declare it. A replay stores the position of an
     * event only once the action has returned normally. A replay whose action threw {@code AttemptNotMade} is held, and
     * runs again from the last position it stored once the subscription is resumed or this model is started with
     * {@code start(true)}. Once live, the live delegate delivers the event again.
     */
    @Override
    public boolean redeliversAnAttemptNotMade() {
        return RedeliveringSubscriptions.findIn(getWrappedSubscriptionModel()).map(RedeliveringSubscriptions::redeliversAnAttemptNotMade).orElse(false);
    }

    /**
     * Answers {@link RepositionableSubscriptions} and {@link RedeliveringSubscriptions} only when the live delegate has
     * it, since {@link #resumeSubscription(String, StartAt)} resumes the subscription there, and a live event is
     * delivered again only when the live delegate delivers it again. So this model is found where the live delegate
     * would be, and nothing is found where nothing was. Answers every other capability as any model does.
     */
    @Override
    public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
        if ((type == RepositionableSubscriptions.class || type == RedeliveringSubscriptions.class) && !getWrappedSubscriptionModel().hasCapability(type)) {
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
    @Nullable Subscription replayToResume(String subscriptionId, String whatChanged, ResumeRead read) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            // Taken off under the handover lock, so the replay's handover either applies the pause before this, and the
            // live delegate then resumes the subscription, or finds no pause and hands over with the subscription running
            pauseRequestedDuringCatchup.remove(subscriptionId);
            Subscribed subscription = subscribed.get(subscriptionId);
            CatchupCannotResumeException historyLost = subscription == null || subscription.owner() != this ? null : subscription.historyLost().refusal(subscriptionId);
            if (historyLost != null) {
                throw historyLost;
            }
            // A takeover stores the position it replays from, so under the model lock it is run outside the lock first,
            // under the claim, as the takeover's own thread runs it, and the resume then finds its replay running
            if (modelLock.isHeldByCurrentThread() && takeOverStores(subscriptionId)) {
                throw new ReadAgain(() -> {
                    try (Claim alsoIgnored = claim(subscriptionId)) {
                        takeOverIfAsked(subscriptionId);
                    }
                });
            }
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
            ResumeReplay resumeReplay = replayThatResumeRuns(subscriptionId, read);
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
            CatchupReplay replay = resumeReplay.replay();
            Checkpoint from = resumeReplay.from();
            // Refused in the attempt, so the refusal holds the subscription as the resume's other refusals do
            CatchupReplay refusingWithoutLiveStart = lastStored -> {
                refuseIfStoredWithoutLiveStart(subscriptionId, from);
                return replay.replayFrom(lastStored);
            };
            CompletableFuture<Void> endedReplayDone = CompletableFuture.allOf(running == null ? CompletableFuture.completedFuture(null) : running.done, resumeReplay.cancelledReplayReturned());
            return new CatchupSubscription(subscriptionId, startCatchupAsync(subscriptionId, refusingWithoutLiveStart, false, true, endedReplayDone));
        }
    }

    private record ResumeReplay(CatchupReplay replay, Checkpoint from, CompletableFuture<Void> cancelledReplayReturned) {
    }

    // The replay a resume of subscriptionId runs before the live delegate resumes it, or null when it runs none. While a
    // cancel of it is under way the stored position alone decides, whatever the live delegate reports as paused. Decided
    // from what read read outside the lock, which is used only while it is current, see ResumeRead
    private @Nullable ResumeReplay replayThatResumeRuns(String subscriptionId, ResumeRead read) {
        Subscribed subscription = subscribed.get(subscriptionId);
        if (!mayReplayToResume(subscriptionId, subscription)) {
            return null;
        }
        StoredForResume mine = read.stored.get(this);
        if (mine == null || !mine.asked() || mine.made() != subscription || !Objects.equals(positionMoves.get(subscriptionId), mine.positionMovedAt())) {
            throw ReadAgain.MOVED;
        }
        if (subscription.startAt().isDynamic() && subscription.startAt().get(generateSubscriptionModelContext()) == null) {
            return null;
        }
        if (mine.failed() != null) {
            throw thrownAsItIs(mine.failed());
        }
        Checkpoint stored = mine.stored();
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
     * asked for during the replay. The live delegate gets {@code liveConsumer} behind the same gate
     * {@link #subscribeInTheWrappedModel} puts the action behind, so each call it makes is let in and counted by the
     * subscription's record, and throws {@link AttemptNotMade} once a cancel has forgotten that record.
     * <p>
     * A replay a resume started resumes the subscription the live delegate holds paused, from the position
     * {@code startAtToUse} resolves to, which stores the live start when the stored position is the catch-up's own.
     * That subscription keeps the action it was made with, so {@code liveConsumer} goes unused and an event the replay
     * delivered can arrive again. When a pause was asked for during the replay, the live delegate isn't resumed and
     * keeps the subscription paused, and the replay is held, so it runs again once the subscription is resumed. When
     * the model is stopped once the position is resolved, or a stop gave up on the replay meanwhile, the live delegate
     * isn't resumed either, and the replay parks as {@link #endReplayIfStillCurrent} parks it. The replay ends once the live delegate has resumed the subscription, so what throws before that, reading or saving
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
            StartAt liveStart = liveStartOf(startAtToUse);
            // Asked again once the live start is resolved, since resolving it reads checkpoint storage, which can keep
            // it waiting past a stop that gave up on this replay's handover lock. The replay then parks as it would
            // have at endReplayIfStillCurrent, and the live delegate isn't resumed after the stop has returned. A rerun
            // replays from the last catch-up position it stored and resolves the live start again
            if ((stopped || attempt.owesAPark()) && !shuttingDown) {
                parkIfStillCurrent(subscriptionId, attempt);
                return new CatchupSubscription(subscriptionId, attempt.replay.result);
            }
            Subscription resumed = resumeTheLiveDelegate(subscriptionId, liveStart, attempt.replay.takesOver);
            if (currentAttempt.remove(subscriptionId, attempt)) {
                runningCatchupSubscriptions.remove(subscriptionId);
            }
            markDidNotGoLive(subscriptionId, false);
            live.complete(resumed);
            return attempt.replay.handle;
        }
        Subscribed made = attempt.replay.made;
        Consumer<CloudEvent> letInByTheRecord = made == null ? event -> acceptInAnAction(subscriptionId, liveConsumer, event) : afterEndedReplaysReturned(subscriptionId, liveConsumer, made);
        Subscription subscription;
        try {
            subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAtToUse, letInByTheRecord);
        } finally {
            positionMayHaveMoved(subscriptionId);
        }
        attempt.wentLive = true;
        markDidNotGoLive(subscriptionId, false);
        live.complete(subscription);
        applyPendingPauseIfAny(subscriptionId);
        return attempt.replay.handle;
    }

    private void markDidNotGoLive(String subscriptionId, boolean didNotGoLive) {
        Subscribed made = subscribed.get(subscriptionId);
        if (made != null) {
            made.didNotGoLive().set(didNotGoLive);
        }
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
            } catch (Throwable failure) {
                // Throwable, since a live model written in Kotlin can throw a checked exception
                CatchupAttempt attempt = Objects.requireNonNull(CURRENT_ATTEMPT.get());
                if (attempt.handingOver) {
                    // Asked for the subscription the live delegate didn't get, so it goes with it. Still under the lock
                    // the pause was recorded under, so no later subscription for the id has asked for one yet.
                    pauseRequestedDuringCatchup.remove(subscriptionId);
                    if (!attempt.wentLive) {
                        markDidNotGoLive(subscriptionId, true);
                    }
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
    private Subscription resumeTheLiveDelegate(String subscriptionId, StartAt liveStart, boolean takesOver) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
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

    // Resolved for the model that goes live, since a model that wraps it can get the caller's own start instead. Can
    // read and store the subscription's position, see startAtToUse
    private StartAt liveStartOf(StartAt startAtToUse) {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        SubscriptionModel innermost = liveDelegate instanceof SubscriptionModelWrapper wrapper ? wrapper.getWrappedSubscriptionModelRecursively() : liveDelegate;
        return startAtToUse.get(new SubscriptionModelContext(innermost.getClass()));
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
     * time, since the live delegate may not support either. It also stores it over a later stored position when it is the
     * live start kept in the checkpoint {@code startAt} resolves to, since the caller kept that checkpoint with state that
     * holds no event after it. It returns what is stored otherwise. Without checkpoint
     * storage it returns the live start. When the live delegate must not run, it stores nothing, and returns what
     * {@code startAt} resolves to for each model that wraps another and the live start for the innermost one, so a
     * wrapper told to store nothing stores nothing and the innermost model goes live from where the replay read.
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
        boolean noLive = delegatedStartAt == null;
        Function<Checkpoint, StartAt> goLiveFrom = position -> {
            if (takesOverLostLiveStarts() && made != null && made.owner() == this && samePosition(position, liveStart)
                    && subscribed.get(subscriptionId) == made) {
                handedOver.put(subscriptionId, new HandedOver(this, made, position, handoverStart.replayAgainFrom(position), replay, new AtomicBoolean(), new AtomicBoolean(), noLive));
            }
            return StartAt.checkpoint(position);
        };
        if (noLive) {
            return StartAt.dynamic(context -> SubscriptionModelWrapper.class.isAssignableFrom(context.subscriptionModelType()) ? startAt.get(context) : goLiveFrom.apply(liveStart));
        }
        return StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                        cfg -> () -> {
                            Checkpoint position = cfg.storage().read(subscriptionId);
                            if (position == null || isCatchupCheckpoint(position) || keptInTheStart(startAt, liveStart)) {
                                try {
                                    position = cfg.storage().save(subscriptionId, liveStart, writeConditionFor(cfg, subscriptionId));
                                } finally {
                                    positionMayHaveMoved(subscriptionId);
                                }
                            }
                            return goLiveFrom.apply(position);
                        })
                .orElse(() -> goLiveFrom.apply(liveStart)));
    }

    // The start the live delegate gets when the start resolved to firstStartAt and no replay runs. A wrapper startAt
    // answers null for, such as a durable model told to store nothing, still gets null, as at a handover
    static StartAt goingLiveFrom(StartAt startAt, StartAt firstStartAt) {
        if (!startAt.isDynamic()) {
            return firstStartAt;
        }
        return StartAt.dynamic(context -> SubscriptionModelWrapper.class.isAssignableFrom(context.subscriptionModelType()) && startAt.get(context) == null ? null : firstStartAt);
    }

    // Whether liveStart is the live start kept in the checkpoint startAt resolves to for this model, as in a checkpoint the
    // application stored with its state and started the subscription from again
    private boolean keptInTheStart(StartAt startAt, Checkpoint liveStart) {
        StartAt resolved = startAt.get(generateSubscriptionModelContext());
        if (!(resolved instanceof StartAtCheckpoint at) || !isCatchupCheckpoint(at.checkpoint)) {
            return false;
        }
        return samePosition(liveStart, CatchupCheckpoints.parse(at.checkpoint).liveFrom());
    }

    // Whether the live delegate tells this model about lost history, and can resume a subscription at a live start a
    // takeover reads again
    private boolean takesOverLostLiveStarts() {
        SubscriptionModel liveDelegate = getWrappedSubscriptionModel();
        return liveDelegate.hasCapability(HistoryLossReportingSubscriptions.class) && liveDelegate.hasCapability(RepositionableSubscriptions.class);
    }

    // Asked by the live delegate, on the thread that found the history lost and without its lock. A subscription this
    // model didn't make is left to the live delegate. A loss of exactly the live start the last replay handed over
    // replays again, and any other loss pauses the subscription for good.
    private HistoryLossHandling takeOverIfHandedOverAt(String subscriptionId, Checkpoint lostFrom) {
        Subscribed subscription = subscribed.get(subscriptionId);
        // Made on the live delegate directly, or through another child of the same dispatcher, which answers for it
        if (subscription == null || subscription.owner() != this) {
            return HistoryLossHandling.leaveToSubscriptionModel();
        }
        // The live delegate resumed the subscription where it lost the history after this model took it over, as a
        // start(true) of it can. Paused again. A replay of this model that still runs resumes it at the live start it
        // reads, and otherwise it stays paused until it is resumed.
        if (!shuttingDown && samePosition(lostFrom, subscription.lostLiveStart().get())) {
            return HistoryLossHandling.takeOver(() -> {
                CatchupAttempt running = currentAttempt.get(subscriptionId);
                if (running == null || running.abandoned()) {
                    log.warn("The subscription model lost the history from live start {} of catch-up subscription {} again after the catch-up took it over, and paused it. No replay of the catch-up resumes it now. {}",
                            lostFrom.asString(), subscriptionId, whenNoReplayResumesIt(subscriptionId));
                }
            });
        }
        HandedOver made = handedOver.get(subscriptionId);
        if (made != null && made.owner() == this && !shuttingDown && samePosition(lostFrom, made.liveStart()) && subscribed.get(subscriptionId) == made.subscribed()) {
            // Before the live delegate pauses the subscription, so a resume that finds it paused before the takeover has
            // started replays instead of resuming the live delegate where it lost the history
            made.takeOverAsked().set(true);
            return HistoryLossHandling.takeOver(() -> {
                if (made.takingOver().compareAndSet(false, true)) {
                    Thread.ofVirtual().name("occurrent-catchup-takeover-" + subscriptionId).start(() -> takeOver(subscriptionId, made));
                }
            });
        }
        // No replay of this model can take the loss over without skipping events, so the subscription stays paused.
        // Recorded in afterPause, which the live delegate runs only for a run that is still current, so a late report
        // never reaches a subscription made since. A resume at a given position since this report keeps it unrecorded.
        HistoryLost historyLost = subscription.historyLost();
        int clearsWhenReported = historyLost.clears();
        return HistoryLossHandling.takeOver(() -> {
            if (historyLost.recordUnlessCleared(clearsWhenReported, lostFrom, false)) {
                String lost = lostFrom.asString();
                log.error(HistoryLossMessages.TERMINAL_FAILURE_LOG, subscriptionId, lost, lost);
            }
        });
    }

    /**
     * Hands {@code subscriptionId} to the live delegate at {@code firstStartAt} through {@code subscribeLive}, held
     * paused there when {@code holdPaused} is set, unless {@code firstStartAt} is a checkpoint the live delegate no
     * longer has the history back to. The subscribe is then refused with {@link CatchupCannotResumeException}, so
     * nothing is subscribed and no event is skipped. When {@link LostStartHold} asks it on this thread, the live
     * delegate holds the subscription paused instead, and the loss is recorded and logged as when the live delegate
     * reports it for a running subscription. {@link #resumeSubscription(String)} then refuses until the subscription is
     * cancelled or resumed at a given position. A start that names no position, such as now or the model default, and
     * a position a catch-up replays from are not checked.
     */
    protected Subscription subscribeLiveUnlessItsStartIsLost(String subscriptionId, StartAt firstStartAt, boolean holdPaused, Function<Boolean, Subscription> subscribeLive) {
        if (firstStartAt.isDynamic() || !(firstStartAt instanceof StartAtCheckpoint at) || isCatchupCheckpoint(at.checkpoint) || subscriptionModel.canResumeFrom(at.checkpoint)) {
            return subscribeLive.apply(holdPaused);
        }
        return refuseOrHoldPaused(subscriptionId, HistoryLossMessages.cannotStartFrom(subscriptionId, at.checkpoint), at.checkpoint, false, () -> subscribeLive.apply(true));
    }

    /**
     * Refuses the subscribe of {@code subscriptionId} before its replay starts when {@code firstStartAt} is a position a
     * catch-up stored, the live delegate no longer has the history from the live start it holds, and the replay that
     * stored it started after the beginning. A replay again from that origin, with a live start read now, would skip
     * events, so the replay would only fail with the same {@link CatchupCannotResumeException} on its own thread. Found
     * here, it is refused or held paused on the calling thread as {@link #subscribeLiveUnlessItsStartIsLost} does,
     * with the live delegate holding the subscription paused at the lost live start. Returns {@code null} when the replay
     * can start.
     */
    protected @Nullable Subscription refuseOrHoldIfItsStoredLiveStartIsLost(String subscriptionId, StartAt firstStartAt, Function<StartAt, Subscription> subscribeLivePausedAt) {
        if (firstStartAt.isDynamic() || !(firstStartAt instanceof StartAtCheckpoint at) || !isCatchupCheckpoint(at.checkpoint)) {
            return null;
        }
        CatchupCheckpoints.Start stored = CatchupCheckpoints.parse(at.checkpoint);
        Checkpoint liveFrom = stored.liveFrom();
        String origin = originAfterTheBeginning(stored);
        if (liveFrom == null || origin == null || subscriptionModel.canResumeFrom(liveFrom)) {
            return null;
        }
        return refuseOrHoldPaused(subscriptionId, liveStartLostAfterTheBeginning(subscriptionId, origin, liveFrom), liveFrom, true,
                () -> subscribeLivePausedAt.apply(StartAt.checkpoint(liveFrom)));
    }

    // Throws refusal, or, when LostStartHold asks it on this thread, has the live delegate hold the subscription paused
    // and records lost, since a resume would skip events, so a resume refuses until the subscription is cancelled or
    // resumed at a given position. liveStart says lost is the live start the replay would go live from
    private Subscription refuseOrHoldPaused(String subscriptionId, CatchupCannotResumeException refusal, Checkpoint lost, boolean liveStart, Supplier<Subscription> subscribeLivePaused) {
        if (!LostStartHold.isAsked()) {
            throw refusal;
        }
        Subscription heldPaused = subscribeLivePaused.get();
        LostStartHold.heldPaused();
        // The subscribe holds the claim, so no resumeSubscription, pause or cancel of the id comes between the live
        // delegate holding it paused and the loss being recorded. A start(true) takes no claim and can resume it in
        // that window. MongoDB then reports the loss when the change stream opens, which pauses it again
        Subscribed made = subscribed.get(subscriptionId);
        if (made != null && made.owner() == this && made.historyLost().recordUnlessCleared(made.historyLost().clears(), lost, liveStart)) {
            String at = lost.asString();
            log.error(HistoryLossMessages.terminalFailureLog(liveStart), subscriptionId, at, at);
        }
        return heldPaused;
    }

    // By the string form, since a live start is a typed checkpoint where the live delegate reads it and a
    // StringBasedCheckpoint where a storage that keeps strings reads it back
    private static boolean samePosition(Checkpoint checkpoint, @Nullable Checkpoint other) {
        return other != null && checkpoint.asString().equals(other.asString());
    }

    // Under the claim, so no other call for the subscription runs in between. Tried again while the takeover is still
    // asked for, since nothing else would start its replay while the subscription reads as running
    private void takeOver(String subscriptionId, HandedOver made) {
        while (handedOver.get(subscriptionId) == made && !shuttingDown) {
            try (Claim ignored = claim(subscriptionId)) {
                takeOverIfAsked(subscriptionId);
                return;
            } catch (Exception e) {
                // Exception, since a storage or wrapped model written in Kotlin can throw a checked exception
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

    // Whether takeOverIfAsked would store the position its replay starts from, called with the handover lock held
    private boolean takeOverStores(String subscriptionId) {
        HandedOver made = handedOver.get(subscriptionId);
        return made != null && made.owner() == this && !made.noLive() && !shuttingDown && subscribed.get(subscriptionId) == made.subscribed()
               && (made.takingOver().get() || made.takeOverAsked().get() && getWrappedSubscriptionModel().isPaused(subscriptionId))
               && returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> true).orElse(false);
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
     * replay starts from is stored first, so a restart in between replays as well, unless the live delegate must not
     * run, when it is kept in memory only. A pause asked for since the live
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
            // Kept in memory when the live delegate must not run, since the subscription keeps no position once live
            boolean stored = !made.noLive() && this.<Boolean, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                try {
                    cfg.storage().save(subscriptionId, made.replayAgainFrom(), writeConditionFor(cfg, subscriptionId));
                } finally {
                    positionMayHaveMoved(subscriptionId);
                }
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
            return new CatchupSubscription(subscriptionId, startCatchupAsync(subscriptionId, replay, pauseAsked, true, subscription.cancelledReplayReturned(), made, null));
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
     * <p>
     * Called from inside the action of a live call or a replay of this model after that action handed a call over, such
     * as a {@link #stop()} or a {@link #resumeSubscription(String)}, it returns at once and pauses the subscription on
     * the lifecycle thread, once the action has returned and that call has run. What it throws there is logged at
     * {@code ERROR}.
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        if (handedOverBehindTheCallsTheActionHandedOver(this, subscriptionId, "pauseSubscription(" + subscriptionId + ")", () -> pauseSubscription(subscriptionId))) {
            return;
        }
        try (Claim ignored = claim(subscriptionId)) {
            pauseHoldingTheClaim(subscriptionId);
        }
    }

    private void pauseHoldingTheClaim(String subscriptionId) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
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
        // The owner's relaunchesBegun when a stop set out to park this attempt's replay, or NO_PARK_OWED. The stop can
        // give up on the claim or the handover lock before it parks it, and the replay then owes the park for as long as
        // relaunchesBegun still reads the same, so it parks the next time it checks whether to go on, also when a
        // start(false) or a resume of another subscription let replays run again meanwhile. A start(true) moves
        // relaunchesBegun, and a resume of the subscription clears this under the handover lock
        private volatile long parkOwedSince = NO_PARK_OWED;
        // Set under the handover lock while a replay a resume started waits to run again after it failed, so a pause
        // holds it instead of waiting for a handover
        private volatile boolean backingOff = false;
        // Set under the handover lock once runAgainAfter held this attempt's replay because the live delegate lost its
        // live start. The attempt's thread then fails the caller's handle with that loss and reports it.
        private volatile boolean heldForLostLiveStart = false;
        // Set once the live delegate has the subscription of a replay a subscribe started. Read and written only on
        // this attempt's own thread, as is decidedFailure.
        private boolean wentLive = false;
        // Set once a replay a subscribe started gives up the id to hand the subscription to the live delegate, while its
        // handover holds the handover lock. Read and written only on this attempt's own thread.
        private boolean handingOver = false;
        // Set under the handover lock once runAgainAfter found that the action withdrew an event of a replay a subscribe
        // started, so the replay runs again as one a resume started does
        private volatile boolean withdrawn = false;
        // The failure a call to the wrapped model threw once the replay was no longer wanted, or after a stop came
        // during the call. A model that retries throws its last failure once stillWanted answers false, so this
        // failure comes of a cancel, a stop, a shutdown or a later attempt, not of the replay. Read and written only
        // on this attempt's own thread.
        private @Nullable Throwable unwantedFailure;
        private @Nullable DecidedFailure decidedFailure;
        private final String subscriptionId;
        private final AbstractCatchupSubscriptionModel owner;
        private final ReplayState replay;
        // Done once the thread of every earlier attempt at this replay has returned, and the thread of every attempt at
        // another replay of the id that a cancel or a stop ended, or that was dropped, so none of them is still inside
        // the subscriber's action
        private final CompletableFuture<Void> earlierAttemptsDone;
        // Done once this attempt's thread and every thread earlierAttemptsDone waits for have returned
        private final CompletableFuture<Void> done = new CompletableFuture<>();
        // Done once the subscriber's action has returned from the last event this attempt let through, which a cancel
        // or a stop waits for. Written under the handover lock on this attempt's own thread.
        private volatile CompletableFuture<Void> actionReturned = CompletableFuture.completedFuture(null);
        // Completed once a cancel or a stop that ended the replay begins to wait for actionReturned, so an action that
        // waits on it, such as a competing consumer's wait for the lease, withdraws the event and returns. Written
        // along with actionReturned.
        private volatile CompletableFuture<Void> actionUnwanted = CompletableFuture.completedFuture(null);

        private CatchupAttempt(String subscriptionId, AbstractCatchupSubscriptionModel owner, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone) {
            this.subscriptionId = subscriptionId;
            this.owner = owner;
            this.replay = replay;
            this.earlierAttemptsDone = earlierAttemptsDone;
        }

        private boolean abandoned() {
            return replay.cancelled || parked;
        }

        // Whether a stop gave up before it parked this attempt's replay, and no start(true) and no resume of the
        // subscription came since
        private boolean owesAPark() {
            return parkOwedSince == owner.relaunchesBegun;
        }
    }

    private static final long NO_PARK_OWED = -1;

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
        // Set under the handover lock once the live delegate lost the live start it would go live from and the replay
        // is held for it. Its thread fails its handle only after it lets go of that lock, so whatever runs it again
        // reads this, not whether the handle is done yet.
        private volatile boolean lostItsLiveStart = false;
        // What its handle failed with and the held-paused listeners were told, set along with lostItsLiveStart
        private volatile @Nullable CatchupCannotResumeException heldFor;
        // The last position an attempt at this replay stored, which the next attempt replays from
        private final AtomicReference<@Nullable Checkpoint> lastStored = new AtomicReference<>();
        // Done once the position store an attempt at this replay last began has returned, which a cancel waits for
        // before the stored position is deleted, and a resume at a given position before it stores one. Set under the
        // handover lock, in the same span as the check that the replay may still store.
        private volatile CompletableFuture<Void> storeUnderWay = CompletableFuture.completedFuture(null);
        // Whether a resume started it, so it resumes the subscription the live delegate holds instead of subscribing it
        private final boolean resuming;
        // Whether a takeover started it, so a start(true) of the live delegate can have resumed the subscription where
        // the live delegate lost the history by the time it hands over
        private final boolean takesOver;
        // The record of the subscription whose subscribe started it, which the action it hands to the live delegate lets
        // calls in by. Null for a replay a resume started, which resumes a subscription the live delegate already holds
        // with its own action, and for one a subclass started through startCatchupAsync without a record.
        private final @Nullable Subscribed made;

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
        // The longest any replay of it took from reading its live start to checking it, counting the replays before a
        // takeover, which the failure once the live start was lost each time names
        private final LongestReplay longestReplay;

        private ReplayState(String subscriptionId, CatchupReplay catchup, CompletableFuture<Subscription> result, boolean resuming, @Nullable HandedOver takenOver,
                            boolean takeoversFollow, @Nullable Subscribed made) {
            this.catchup = catchup;
            this.result = result;
            this.resuming = resuming;
            this.takesOver = takenOver != null;
            this.made = made;
            this.handle = takenOver == null ? new LiveHandle(subscriptionId, takeoversFollow) : takenOver.replay().handle;
            this.replaysAgainBefore = takenOver == null ? 0 : takenOver.replay().replaysAgain + 1;
            this.longestReplay = takenOver == null ? new LongestReplay() : takenOver.replay().longestReplay;
        }

        private ReplayState(ReplayState held) {
            this.catchup = held.catchup;
            this.result = new CompletableFuture<>();
            this.resuming = held.resuming;
            this.takesOver = held.takesOver;
            this.made = held.made;
            this.handle = held.handle;
            this.longestReplay = held.longestReplay;
            this.lastStored.set(held.lastStored.get());
        }

        // The replay a resume runs again once the live delegate lost the live start of this one and its handle failed
        private ReplayState again() {
            ReplayState again = new ReplayState(this);
            if (takesOver) {
                again.handle.follow(again.result);
            }
            return again;
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
     * {@code earlierAttemptsDone} completes once no earlier attempt at it, and no attempt at another replay of the id
     * that a cancel or a stop ended or that was dropped, is still running the subscriber's action, which a replay that
     * {@code stop()} cut short can still be doing when it is run again.
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
        try {
            cfg.storage().save(subscriptionId, checkpoint, writeConditionFor(cfg, subscriptionId));
        } finally {
            positionMayHaveMoved(subscriptionId);
        }
        CURRENT_ATTEMPT.get().replay.lastStored.set(checkpoint);
    }

    /**
     * Runs {@code delivery}, which hands one replayed event to the subscriber's action, when the calling attempt should
     * keep replaying as {@link #shouldKeepReplaying} answers, and returns whether it ran. The answer and letting the
     * event through are one step under the handover lock, and the action runs after the lock is released. A cancel or a
     * stop that takes effect before that step lets no event through, and one that takes effect after it waits up to a
     * second for the action to return. That cancel or stop first completes the stage this sets for the call through
     * {@link UnwantedDelivery}, so an action that waits on it can withdraw the event and return. The stage names the
     * live delegate, so a competing consumer model reads it only when the live delegate is among the models it wraps,
     * as it is when the competing consumer model wraps this model. A shutdown completes the stage too, and doesn't
     * wait for the action. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    private boolean deliverIfStillWanted(String subscriptionId, Runnable delivery) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        CompletableFuture<Void> actionReturned = new CompletableFuture<>();
        CompletableFuture<Void> actionUnwanted = new CompletableFuture<>();
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            if (!shouldKeepReplaying(subscriptionId)) {
                return false;
            }
            attempt.actionReturned = actionReturned;
            attempt.actionUnwanted = actionUnwanted;
        }
        // markShuttingDown takes no handover lock. It writes shuttingDown before it reads the stage, and this writes the
        // stage before it reads shuttingDown, so a shutdown racing the lines above is seen by one of the two.
        if (shuttingDown) {
            actionUnwanted.complete(null);
        }
        try {
            betweenHandoverLockAndAction.run();
            UnwantedDelivery.run(subscriptionId, getWrappedSubscriptionModel(), actionUnwanted, delivery);
        } finally {
            actionReturned.complete(null);
        }
        return true;
    }

    /**
     * Stores {@code checkpoint} as {@link #saveCatchupCheckpoint} does, with {@link PersistCheckpointDuringCatchupPhase}
     * configured, when {@link #isSafeToPersistFor} allows it and then {@code persist} answers {@code true}.
     * {@code persist} is code the caller supplied, so it is asked before the handover lock is taken, since a
     * {@link #stop()} it waits for on another thread parks the replay under that lock. {@link #isSafeToPersistFor} is
     * checked again under the lock, which also records that the store is under way, and the position is stored after
     * the lock is released, so a pause or a stop doesn't wait for the storage. A cancel marks the replay under the same
     * lock, so it either comes before that check and nothing is stored, or after it and waits for the store to return
     * before the stored position is deleted. Only meaningful on the virtual thread {@link #startCatchupAsync} started for
     * this attempt.
     */
    private void saveIfStillSafe(String subscriptionId, BooleanSupplier persist, Checkpoint checkpoint) {
        PersistCheckpointDuringCatchupPhase cfg = returnIfCheckpointStorageConfigIs(PersistCheckpointDuringCatchupPhase.class, configured -> configured).orElse(null);
        if (cfg == null || !isSafeToPersistFor(subscriptionId) || !persist.getAsBoolean()) {
            return;
        }
        ReplayState replay = CURRENT_ATTEMPT.get().replay;
        CompletableFuture<Void> stored = new CompletableFuture<>();
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            if (!isSafeToPersistFor(subscriptionId)) {
                return;
            }
            replay.storeUnderWay = stored;
        }
        try {
            betweenHandoverLockAndStore.run();
            saveCatchupCheckpoint(subscriptionId, cfg, checkpoint);
        } finally {
            stored.complete(null);
        }
    }

    /**
     * Delivers the replayed {@code cloudEvents} to {@code action} in order until the calling attempt should no longer
     * replay, and stores the position after each event {@code persistDuringCatchup} matches. {@code persistDuringCatchup}
     * is the attempt's own, so its count runs on across windows. With a {@code cache}, an event an earlier
     * reconciliation pass delivered is skipped, since the delta is read again until it stops changing and the passes
     * overlap, and each delivered event is recorded so the live subscription skips it at the handover. Each event is
     * delivered as a {@link CheckpointAwareCloudEvent} with the checkpoint {@code checkpointAfter} derives, once, so
     * the delivered and the stored checkpoint are the same. The source stream is closed also when the replay ends
     * early, so a read backed by a server cursor doesn't leak it. Only meaningful on the virtual thread
     * {@link #startCatchupAsync} started for this attempt.
     */
    void deliverCatchupEvents(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable BoundedIdCache<CatchupEventKey> cache,
                              Predicate<CloudEvent> persistDuringCatchup, Function<CloudEvent, Checkpoint> checkpointAfter) {
        try (cloudEvents) {
            Iterator<CloudEvent> events = cloudEvents.iterator();
            while (events.hasNext()) {
                CloudEvent event = events.next();
                @Nullable CatchupEventKey key = cache == null ? null : CatchupEventKey.of(event);
                if (key != null && cache.contains(key)) {
                    continue;
                }
                Checkpoint checkpoint = checkpointAfter.apply(event);
                boolean delivered = deliverIfStillWanted(subscriptionId, () -> {
                    if (key != null) {
                        cache.add(key);
                    }
                    acceptInAnAction(subscriptionId, action, new CheckpointAwareCloudEvent(event, checkpoint));
                });
                if (!delivered) {
                    return;
                }
                // Not shouldKeepReplaying, since the action has returned, and a stop or a shutdown that took effect
                // meanwhile doesn't keep its position from being stored. A stop the action asked for takes effect once
                // the action has returned, so it parks the replay at this or a later event. A replay that runs again
                // after a stop starts from the stored position.
                saveIfStillSafe(subscriptionId, () -> persistDuringCatchup.test(event), checkpoint);
            }
        }
    }

    // A call to the subscriber's action that a replay of subscriptionId let through, and that a cancel or a stop waits for.
    // unwanted is the stage that call reads from UnwantedDelivery.
    record ActionInFlight(String subscriptionId, CompletableFuture<Void> returned, CompletableFuture<Void> unwanted) {
    }

    // Tells every call in admitted that its event is no longer wanted, then waits up to ACTION_WAIT, in all, for each of
    // them to return. call names the cancel or the stop for the WARN logged when one hasn't.
    static void awaitTheAdmittedActions(List<ActionInFlight> admitted, String call) {
        admitted.forEach(action -> action.unwanted().complete(null));
        CompletableFuture<Void> returned = CompletableFuture.allOf(admitted.stream().map(ActionInFlight::returned).toArray(CompletableFuture[]::new));
        if (returned.isDone() || completesWithin(returned, ACTION_WAIT)) {
            return;
        }
        List<String> stillInTheAction = admitted.stream().filter(action -> !action.returned().isDone()).map(ActionInFlight::subscriptionId).distinct().toList();
        // Each action may have returned since the wait ended
        if (stillInTheAction.isEmpty()) {
            return;
        }
        // A stopped replay runs again on start(true) or a resume, a cancelled one never does
        String afterwards = call.equals("stop")
                ? "The replay lets no further event through until start(true) or resumeSubscription(..) runs it again."
                : "The replay lets no further event through.";
        log.warn("A {} returned while the action of {} still ran for an event its catch-up replay delivered, after it waited {} ms for the action. {}",
                call, stillInTheAction.size() == 1 ? "subscription " + stillInTheAction.getFirst() : "subscriptions " + String.join(", ", stillInTheAction), ACTION_WAIT.toMillis(), afterwards);
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
     * {@link #stopReplay()} had parked the others. So does one a stop gave up on before it parked it, also once a
     * {@code start(false)} or a resume of another subscription let replays run again.
     */
    protected boolean shouldKeepReplaying(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if ((stopped || attempt.owesAPark()) && !shuttingDown) {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                parkIfStillCurrent(subscriptionId, attempt);
            }
            return false;
        }
        return isStillCurrent(subscriptionId, attempt);
    }

    // Not shutting down, not cancelled or parked, and still the attempt registered for subscriptionId
    private boolean isStillCurrent(String subscriptionId, CatchupAttempt attempt) {
        return !shuttingDown && !attempt.abandoned() && currentAttempt.get(subscriptionId) == attempt;
    }

    // What shouldKeepReplaying answers for attempt, without parking a stopped replay. A pure read, as RetryExecution
    // requires of the predicate a retry polls.
    private boolean stillWanted(CatchupAttempt attempt) {
        return !stopped && !attempt.owesAPark() && isStillCurrent(attempt.subscriptionId, attempt);
    }

    // Calls the wrapped model with a stillWanted it can check between the attempts of a retry, so a cancel, stop or
    // shutdown that comes during the retry ends it. The failure the call throws while the replay isn't wanted, before
    // the call or once it throws, or after a stop came during the call, is recorded, and the replay then ends as
    // cancelled, parked, shut down or taken over by a later attempt. Off a replay thread, as when a subclass calls one
    // of the protected methods that call this, stillWanted is always true, as for the calls a subscribe makes.
    private <T> T callWrappedModel(Function<BooleanSupplier, T> call) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (attempt == null) {
            return call.apply(() -> true);
        }
        BeforeTheCall before = beforeTheCall(attempt);
        try {
            return call.apply(() -> stillWanted(attempt));
        } catch (Throwable failure) {
            if (noLongerWantedSince(attempt, before)) {
                attempt.unwantedFailure = failure;
            }
            throw failure;
        }
    }

    // Whether the wrapped model can resume from liveFrom. On a replay thread, a false that came while the replay wasn't
    // wanted, before the call or once it answers, or after a stop came during the call, isn't read as lost history. It
    // ends the call with a failure recorded as unwanted instead, so the replay ends as cancelled, parked, shut down or
    // taken over by a later attempt. start(true) then asks the wrapped model again, and reads a false it answers then
    // as lost history.
    private boolean wrappedModelCanResumeFrom(Checkpoint liveFrom) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (attempt == null) {
            return callWrappedModel(stillWanted -> subscriptionModel.canResumeFrom(liveFrom, stillWanted));
        }
        BeforeTheCall before = beforeTheCall(attempt);
        boolean canResume = callWrappedModel(stillWanted -> subscriptionModel.canResumeFrom(liveFrom, stillWanted));
        if (canResume || !noLongerWantedSince(attempt, before)) {
            return canResume;
        }
        IllegalStateException unwanted = new IllegalStateException("The subscription model answered that it no longer has the history from live start " + liveFrom.asString()
                + " for catch-up subscription " + attempt.subscriptionId + " while the replay wasn't wanted, or after a stop came while it checked, so the answer isn't read as lost history.");
        attempt.unwantedFailure = unwanted;
        throw unwanted;
    }

    // What stillWanted answered for an attempt right before a call to the wrapped model, and what stops answered then
    private record BeforeTheCall(long stops, boolean wanted) {
    }

    // Read under the lifecycle lock, so the stop count and stopped belong to the same stop or start. A start can clear
    // stopped before the call ends without counting a stop, so a replay that registered after a stop parked the others
    // is only known to be unwanted from what stillWanted answered before the call.
    private BeforeTheCall beforeTheCall(CatchupAttempt attempt) {
        synchronized (lifecycleLock) {
            return new BeforeTheCall(stops, stillWanted(attempt));
        }
    }

    // Whether attempt wasn't wanted before the call or isn't wanted now, or a stop came since before was read
    private boolean noLongerWantedSince(CatchupAttempt attempt, BeforeTheCall before) {
        return !before.wanted() || !stillWanted(attempt) || before.stops() != stops();
    }

    // Whether failure is the one a call to the wrapped model threw once the replay was no longer wanted
    private static boolean endedUnwanted(CatchupAttempt attempt, @Nullable Throwable failure) {
        return failure != null && failure == attempt.unwantedFailure;
    }

    private long stops() {
        synchronized (lifecycleLock) {
            return stops;
        }
    }

    private long stopsAsked() {
        synchronized (lifecycleLock) {
            return stopsAsked;
        }
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
        if (stopped || attempt.owesAPark()) {
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
     * allows replays in that window runs it again too, start(false) included, apart from a replay a stop gave up on
     * before it parked it. That one stays parked until a start(true) or a resume of its subscription, as it would had
     * the stop parked it.
     */
    private void parkIfStillCurrent(String subscriptionId, CatchupAttempt attempt) {
        if (attempt.replay.cancelled) {
            if (currentAttempt.remove(subscriptionId, attempt)) {
                runningCatchupSubscriptions.remove(subscriptionId);
                // registerOrPark no longer finds the attempt the cancel ended, whose action or thread can still run
                waitForTheDroppedReplay(subscriptionId, attempt.done);
            }
        } else if (hold(subscriptionId, attempt)) {
            AbstractCatchupSubscriptionModel owner = attempt.owner;
            // Read after the put, so a start that allows replays from here on finds this replay parked. A start(true)
            // moves relaunchesBegun before it lists the parked replays, and a resume of the subscription clears the
            // owed park under this lock, so neither leaves this replay parked
            if (!owner.stopped && !owner.shuttingDown && !attempt.owesAPark()) {
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

    // Package-private for the test that stands where the field describes. Not public, and not part of this model's
    // contract.
    void runBetweenHandoverLockAndAction(Runnable hook) {
        this.betweenHandoverLockAndAction = Objects.requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the same reason as runBetweenHandoverLockAndAction
    void runBetweenHandoverLockAndStore(Runnable hook) {
        this.betweenHandoverLockAndStore = Objects.requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the same reason as runBetweenHandoverLockAndAction
    void runBetweenTheHandedCallsAndTheLock(Runnable hook) {
        this.betweenTheHandedCallsAndTheLock = Objects.requireNonNull(hook, "hook cannot be null");
    }

    boolean hasParkedReplay(String subscriptionId) {
        return parkedReplays.containsKey(subscriptionId);
    }
    /**
     * Runs every replay this model has parked again, or parks it once more if this model is stopped by then. Each one
     * under its {@link #claim}, and a failure to claim one is thrown once the others have run. A replay held after a
     * loss of its live start that a resume would skip events after stays held, and its
     * {@link CatchupCannotResumeException} is thrown the same way.
     * <p>
     * A replay a {@link #stop()} gave up on before it parked it, while code a caller supplied held the replay up, goes on
     * once that code returns, as a replay this runs again does, instead of parking.
     */
    public void relaunchParkedReplays() {
        // Before the list is taken, so a replay that owed a park and parks after it finds the park no longer owed and
        // runs again itself
        synchronized (lifecycleLock) {
            relaunchesBegun++;
        }
        runEach(List.copyOf(parkedReplays.keySet()), subscriptionId -> {
            try (Claim ignored = claim(subscriptionId, STARTED_BUT_NOT_RUN_AGAIN)) {
                Subscribed made = subscribed.get(subscriptionId);
                CatchupCannotResumeException lost = made == null || made.owner() != this ? null : made.historyLost().refusal(subscriptionId);
                if (lost != null && hasParkedReplay(subscriptionId)) {
                    throw lost;
                }
                relaunchParkedReplay(subscriptionId);
            }
        });
    }

    /**
     * Clears the park a stop that gave up left the replay of {@code subscriptionId} owing on this model, for a resume of
     * the subscription, which lets that replay go on. Called with the id's claim and handover lock held, so a replay
     * that parks for it either has parked by then, and the resume runs it again, or finds the park no longer owed and
     * runs again itself.
     */
    void clearTheParkOwedBy(String subscriptionId) {
        CatchupAttempt attempt = currentAttempt.get(subscriptionId);
        if (attempt != null && attempt.owner == this) {
            attempt.parkOwedSince = NO_PARK_OWED;
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
            // A loss a resume would skip events after stays parked, and resumeSubscription refuses it
            if (recordedHistoryLoss(subscriptionId) != null) {
                return null;
            }
            ParkedReplay held = parkedReplays.remove(subscriptionId);
            if (held == null) {
                return null;
            }
            // Its handle fails, or already failed, when the live delegate lost its live start, so it runs again with a
            // handle of its own
            parked = held.replay().lostItsLiveStart || held.replay().result.isDone() ? new ParkedReplay(held.replay().again(), held.earlierAttemptsDone()) : held;
            if (parked.replay() != held.replay() && parked.replay().resuming) {
                ReplayState again = parked.replay();
                resumeReplays.add(again);
                again.result.whenComplete((subscription, failure) -> resumeReplays.remove(again));
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

    // The position the live delegate lost the history from for a subscription of this model, where a resume would skip
    // events, or null
    private @Nullable Checkpoint recordedHistoryLoss(String subscriptionId) {
        Subscribed subscription = subscribed.get(subscriptionId);
        return subscription == null || subscription.owner() != this ? null : subscription.historyLost().get();
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
     * {@link #startCatchupAsync}'s registration, {@link #deliverIfStillWanted} and the check in
     * {@link #saveIfStillSafe} for each replayed event, a subclass's replay-completion code from {@link #endReplayIfStillCurrent} through its checkpoint
     * cleanup and delegate {@code subscribe} call (or the cancelled-cleanup branch), and {@link #cancelRunningCatchup}.
     * The lock is kept for the id only while a thread holds it or waits for it.
     * <p>
     * It waits with no deadline, apart from a lifecycle call for the id, which holds the id's {@link #claim}. That call
     * gives up and throws {@link IllegalStateException} once code a caller supplied has run under the lock for
     * {@link #LIFECYCLE_WAIT}, such as a {@link CatchupListener} the replay thread tells, since that code can be waiting
     * for the call. A lifecycle call that also holds the model lock, such as a {@link #stop()} parking the replay, gives
     * up after {@link #LIFECYCLE_WAIT} whatever holds the handover lock, such as a replay waiting for its checkpoint
     * storage, so no other subscription's call waits longer than that behind it. The exception says what the call did
     * before it asked for its claim, which is all it changed.
     * <p>
     * A {@link ReentrantLock}, not
     * {@code synchronized}, because every caller here runs on a {@link #startCatchupAsync virtual thread} and a
     * handover span can block on storage or delegate I/O. Blocking inside a {@code synchronized} block would pin
     * the carrier thread for that whole span, a plain lock does not.
     */
    protected HandoverLock lockHandover(String subscriptionId) {
        ReentrantLock lock = useLock(handoverLocks, subscriptionId);
        try {
            lockUnlessHeldUpByCallersCode(subscriptionId, lock);
        } catch (RuntimeException | Error e) {
            releaseLock(handoverLocks, subscriptionId);
            throw e;
        }
        return () -> {
            try {
                lock.unlock();
            } finally {
                releaseLock(handoverLocks, subscriptionId);
            }
        };
    }

    // Takes lock, the handover lock of subscriptionId, with no deadline, unless this thread makes a lifecycle call for
    // the id. Such a call holds the id's claim, and gives up once code a caller supplied, such as a CatchupListener, has
    // run under the lock for LIFECYCLE_WAIT, counted as heldUpByCallersCode counts it. That code can be waiting for the
    // call, which holds the model lock as a stop does, so neither would return. A call that also holds the model lock
    // gives up after LIFECYCLE_WAIT whatever holds the handover lock, such as a replay whose handover waits for its
    // checkpoint storage or the wrapped model, since every other lifecycle call waits for it meanwhile. A wait behind
    // anything else has no deadline
    private void lockUnlessHeldUpByCallersCode(String subscriptionId, ReentrantLock lock) {
        String whatChanged = lock.isHeldByCurrentThread() ? null : whatTheClaimOnThisThreadChanged(subscriptionId);
        if (whatChanged == null) {
            lock.lock();
            return;
        }
        long waitingSince = System.nanoTime();
        boolean underTheModelLock = modelLock.isHeldByCurrentThread();
        whileWaitingForOneSubscription(() -> {
            while (!tryLockWithin(lock, CALLERS_CODE_POLL)) {
                Long since = callersCodeUnderTheHandoverLockSince.get(subscriptionId);
                if (since != null && heldUpSince(since, waitingSince)) {
                    log.error("A call for subscription {} waited {} ms for its handover lock while code the caller supplied, such as a CatchupListener, ran under it, and gives up. {}",
                            subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
                    throw new IllegalStateException("Cannot change subscription " + subscriptionId + ", because code the caller supplied, such as a CatchupListener, ran under its handover lock for "
                            + LIFECYCLE_WAIT.toMillis() + " ms while this call waited for that lock, and that code may be waiting for this call. Don't wait inside such code for a lifecycle call"
                            + " made on another thread. " + whatChanged);
                }
                if (underTheModelLock && System.nanoTime() - waitingSince >= LIFECYCLE_WAIT.toNanos()) {
                    log.error("A call for subscription {} that holds the model lock waited {} ms for its handover lock, and gives up. {}", subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
                    throw new IllegalStateException("Cannot change subscription " + subscriptionId + ", because its catch-up replay held its handover lock for " + LIFECYCLE_WAIT.toMillis()
                            + " ms while this call waited for it under the model lock, such as while the replay waited for its checkpoint storage or the wrapped model to answer. "
                            + whatChanged);
                }
            }
            return null;
        });
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
     * and then logs at {@code ERROR} and throws {@link IllegalStateException}. That wait includes a subscribe for the id
     * on another thread whose start-position function is still being asked, since the subscribe asks it with no claim
     * held, so a function that waits for such a call fails that call after {@link #LIFECYCLE_WAIT}. A cancel doesn't
     * wait for the function, see {@link #cancelSubscription(String)}. An interrupt doesn't end the wait, and is set again
     * once the claim is taken or the wait is over.
     * <p>
     * The caller changes nothing for the id once the claim fails. {@code whatChanged} says what it did before it asked
     * for the claim, which the log and the exception repeat. That is nothing, apart from a {@link #stop()}, which has
     * stopped this model, a {@link #resumeSubscription(String)} of a stopped model, which has started it, and a
     * {@code start(true)}, which has started this model.
     */
    Claim claim(String subscriptionId, String whatChanged) {
        return claim(subscriptionId, whatChanged, true);
    }

    // For a cancel, which wins over a subscribe whose function is asked, and for that subscribe itself
    Claim claimWithoutWaitingForAsks(String subscriptionId) {
        return claim(subscriptionId, NOTHING_CHANGED, false);
    }

    private Claim claim(String subscriptionId, String whatChanged, boolean afterAsks) {
        HandedPerIdCall handed = handedPerIdCallOnTheCallingThread();
        if (handed != null && handed.subscriptionId.equals(subscriptionId) && handed.withdrawn()) {
            throw withdrawnCall(subscriptionId, whatChanged);
        }
        ReentrantLock claim = useClaimLock(subscriptionId);
        try {
            long waitingSince = System.nanoTime();
            if (claim.isHeldByCurrentThread()) {
                claim.lock();
            } else if (replaysOnTheCallingThread(subscriptionId)) {
                if (!claim.tryLock()) {
                    throw new IllegalStateException("Cannot change subscription " + subscriptionId + " from inside its own action while its catch-up replay runs and another call for it is under way,"
                            + " because that call can be waiting for the action to return. Change it from another thread. " + whatChanged);
                }
            } else if (!whileWaitingForOneSubscription(() -> (!afterAsks || asksOnOtherThreadsReturnWithin(subscriptionId, LIFECYCLE_WAIT))
                                                              && tryLockWithin(claim, LIFECYCLE_WAIT.minusNanos(System.nanoTime() - waitingSince)))) {
                log.error("A call for subscription {} waited {} ms for another call for it to return, and gives up. {}", subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
                throw new IllegalStateException("Cannot change subscription " + subscriptionId + ", because another call for it didn't return within " + LIFECYCLE_WAIT.toMillis()
                        + " ms. " + whatChanged);
            }
        } catch (RuntimeException | Error e) {
            releaseClaimLock(subscriptionId);
            throw e;
        }
        if (handed != null && !handed.mayClaim(subscriptionId)) {
            claim.unlock();
            releaseClaimLock(subscriptionId);
            throw withdrawnCall(subscriptionId, whatChanged);
        }
        // Present while this thread counts as one of its users
        CountedLock held = lifecycleClaims.get(subscriptionId);
        String before = held.whatChanged;
        held.whatChanged = whatChanged;
        return () -> {
            held.whatChanged = before;
            claim.unlock();
            releaseClaimLock(subscriptionId);
        };
    }

    // What the call on this thread did before it asked for the claim of subscriptionId, or null when this thread holds
    // no such claim
    private @Nullable String whatTheClaimOnThisThreadChanged(String subscriptionId) {
        CountedLock claim = lifecycleClaims.get(subscriptionId);
        return claim != null && claim.lock.isHeldByCurrentThread() ? claim.whatChanged : null;
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
    // What a shutdown() that gives up on the model lock did before it waited for the lock, and what happens once that
    // code returns
    static final String NOT_SHUT_DOWN = "The catch-up replays are ended and the model is marked as shutting down. The wrapped subscription model isn't shut down yet,"
            + " and its subscriptions still run until that code returns. Then the wrapped model is shut down under the model lock, with no further call needed.";
    static final String SHUT_DOWN_PAST_A_WAIT = "The catch-up replays are ended and the model is marked as shutting down. The wrapped subscription model was shut down without"
            + " the model lock, since the call holding it waited for a call for one subscription, and it's shut down again under the lock once that code returns, with no further call needed.";
    static final String SHUT_DOWN_TO_END_A_START = "The catch-up replays are ended and the model is marked as shutting down. The wrapped subscription model was shut down without"
            + " the model lock to end the start(..) inside it, and it's shut down again under the lock once that code returns, with no further call needed.";

    // The lock behind the claim of subscriptionId, counting the caller as one that holds it or waits for it until it calls
    // releaseClaimLock
    private ReentrantLock useClaimLock(String subscriptionId) {
        return useLock(lifecycleClaims, subscriptionId);
    }

    // Removes the claim's entry once no thread holds it or waits for it
    private void releaseClaimLock(String subscriptionId) {
        releaseLock(lifecycleClaims, subscriptionId);
    }

    // The lock in locks for subscriptionId, counting the caller as one that holds it or waits for it until it calls
    // releaseLock
    private static ReentrantLock useLock(ConcurrentMap<String, CountedLock> locks, String subscriptionId) {
        return locks.compute(subscriptionId, (id, used) -> {
            if (used == null) {
                return new CountedLock();
            }
            used.users++;
            return used;
        }).lock;
    }

    // Removes the entry for subscriptionId from locks once no thread holds it or waits for it
    private static void releaseLock(ConcurrentMap<String, CountedLock> locks, String subscriptionId) {
        locks.computeIfPresent(subscriptionId, (id, used) -> --used.users == 0 ? null : used);
    }

    // Whether every subscribe for the id whose function another thread asks with no claim held returns within timeout.
    // The asking thread itself goes ahead, as it did when the function was asked under the claim.
    private boolean asksOnOtherThreadsReturnWithin(String subscriptionId, Duration timeout) {
        Thread current = Thread.currentThread();
        CompletableFuture<?>[] asks = askedFirst.stream()
                .filter(asked -> asked.subscriptionId.equals(subscriptionId) && asked.asker != current)
                .map(asked -> asked.returned)
                .toArray(CompletableFuture[]::new);
        return asks.length == 0 || completesWithin(CompletableFuture.allOf(asks), timeout);
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
     * Acquires {@code subscriptionId}'s handover lock as {@link #lockHandover} does. It never returns {@code null}.
     * It once skipped an id with no lock, but a lock is now kept only while a thread uses it, so a missing one no
     * longer means that no attempt runs for the id, and skipping it would let a cancel mark the replay between its check
     * and the event it lets through. The lock it takes for an unknown id is removed once it is released.
     */
    protected HandoverLock tryLockHandover(String subscriptionId) {
        return lockHandover(subscriptionId);
    }

    /**
     * Captures the live resume checkpoint handed over to live delivery. Every catch-up captures it before its bulk
     * replay, so an event that commits during the replay, whatever its position or time, is delivered live.
     * It is the {@code liveStart} the attempt's {@linkplain ReplayScope read scope} reports, which the event store
     * read in the same session as the replay reads, or the wrapped model's global checkpoint when the store reports
     * none. Read also when the live delegate must not run, since the innermost subscription model still goes live from
     * it. Fails loudly if there is no checkpoint rather than silently resuming at "now" and dropping events
     * committed during replay.
     * <p>
     * Called on the virtual thread {@link #startCatchupAsync} started for an attempt, this hands the wrapped model a
     * {@code stillWanted} that answers {@code false} once the attempt is cancelled, stopped, shut down or taken over by
     * a later attempt, so a retry the wrapped model runs ends then. Called on any other thread, {@code stillWanted}
     * always answers {@code true}, and the wrapped model retries as it does for the calls a subscribe makes.
     */
    protected Checkpoint captureLiveResumeCheckpoint(@Nullable String liveStart) {
        Checkpoint checkpoint = liveStart == null ? callWrappedModel(stillWanted -> subscriptionModel.globalCheckpoint(stillWanted)) : new StringBasedCheckpoint(liveStart);
        if (checkpoint == null) {
            throw new IllegalStateException("Cannot run a catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.");
        }
        return checkpoint;
    }

    /**
     * Where a position replay starts, where live delivery picks up after it, and what the replay stores on the way.
     *
     * @param replayFrom   The global sequence position the replay reads after
     * @param liveFrom     The live start handed over to live delivery
     * @param replayOrigin The global sequence position the first attempt at this replay started from
     * @param replayTo     The head of the global sequence an earlier attempt read after it read {@code liveFrom}, or
     *                     null when this attempt reads the head itself
     * @param noLive       Whether the live delegate must not run, so only the innermost subscription model goes live
     */
    protected record PositionReplayStart(long replayFrom, Checkpoint liveFrom, long replayOrigin, @Nullable Long replayTo, boolean noLive) implements ReplayStart<PositionReplayStart> {

        /**
         * The checkpoint stored once the replay has delivered through {@code position}, for a replay whose head was
         * {@code replayTo}. It holds the live start, so a resume goes live from where this replay would have, not
         * from a live start read after the restart.
         */
        Checkpoint checkpointAt(long position, long replayTo) {
            return marked() ? new StringBasedCheckpoint(CatchupCheckpoints.format(position, replayOrigin, replayTo, liveFrom))
                    : GlobalCheckpoint.of(position, liveFrom, replayOrigin, replayTo);
        }

        @Override
        public PositionReplayStart fromOrigin(Checkpoint newLiveFrom) {
            return new PositionReplayStart(replayOrigin, newLiveFrom, replayOrigin, null, noLive);
        }

        @Override
        public Checkpoint replayAgainFrom(Checkpoint lostLiveStart) {
            return marked() ? new StringBasedCheckpoint(CatchupCheckpoints.format(replayOrigin, replayOrigin, replayOrigin, lostLiveStart))
                    : GlobalCheckpoint.of(replayOrigin, lostLiveStart, replayOrigin, replayOrigin);
        }

        @Override
        public String describeOrigin() {
            return "position " + replayOrigin;
        }

        @Override
        public boolean startsAtTheBeginning() {
            return replayOrigin == 0;
        }

        // Marked where an earlier version would resume the composite without its live start, or replay from an origin
        // above zero with a live start read then
        private boolean marked() {
            return noLive || replayOrigin != 0;
        }
    }

    /**
     * Where a time replay starts, where live delivery picks up after it, and what the replay stores on the way.
     *
     * @param replayFrom   The time the replay reads from, inclusive
     * @param liveFrom     The live start handed over to live delivery
     * @param replayOrigin The time the first attempt at this replay started from
     * @param noLive       Whether the live delegate must not run, so only the innermost subscription model goes live
     */
    protected record TimeReplayStart(TimeBasedCheckpoint replayFrom, Checkpoint liveFrom, TimeBasedCheckpoint replayOrigin, boolean noLive) implements ReplayStart<TimeReplayStart> {

        /**
         * The checkpoint stored once the replay has delivered every event with a time before {@code time}. It holds
         * the live start, so a resume goes live from where this replay would have, not from a live start read after
         * the restart.
         */
        Checkpoint checkpointAt(TimeBasedCheckpoint time) {
            return marked() ? new StringBasedCheckpoint(CatchupCheckpoints.format(time.asString(), replayOrigin.asString(), liveFrom))
                    : CatchupTimeCheckpoint.of(time.asString(), liveFrom, replayOrigin.asString());
        }

        /**
         * The checkpoint stored once the replay has delivered {@code event} in {@code phase}, given that the last one
         * stored was {@code previous}. Every event with a time before the one it holds has been delivered. That is the
         * event's time when the replay reads in ascending time, or reconciles after the bulk read, and the time this
         * attempt reads from otherwise. An event without a time keeps {@code previous}.
         */
        Checkpoint watermarkAt(CloudEvent event, TimeReplayPhase phase, Checkpoint previous) {
            if (phase == TimeReplayPhase.BULK_READ) {
                return checkpointAt(replayFrom);
            }
            OffsetDateTime time = event.getTime();
            return time == null ? previous : checkpointAt(TimeBasedCheckpoint.from(time));
        }

        @Override
        public TimeReplayStart fromOrigin(Checkpoint newLiveFrom) {
            return new TimeReplayStart(replayOrigin, newLiveFrom, replayOrigin, noLive);
        }

        @Override
        public Checkpoint replayAgainFrom(Checkpoint lostLiveStart) {
            return marked() ? new StringBasedCheckpoint(CatchupCheckpoints.format(replayOrigin.asString(), replayOrigin.asString(), lostLiveStart))
                    : CatchupTimeCheckpoint.of(replayOrigin.asString(), lostLiveStart, replayOrigin.asString());
        }

        @Override
        public String describeOrigin() {
            return "time " + replayOrigin.asString();
        }

        @Override
        public boolean startsAtTheBeginning() {
            return replayOrigin.isBeginningOfTime();
        }

        // Marked where an earlier version would resume the composite without its live start, or replay from an origin
        // after the beginning of time with a live start read then
        private boolean marked() {
            return noLive || !replayOrigin.isBeginningOfTime();
        }
    }

    /**
     * Where a time replay is when it delivers an event, which decides what {@link TimeReplayStart#watermarkAt} stores.
     */
    protected enum TimeReplayPhase {
        /**
         * The bulk read, sorted with the time ascending first
         */
        BULK_READ_BY_TIME,
        /**
         * The bulk read, sorted any other way
         */
        BULK_READ,
        /**
         * The reads that reconcile the events written since the bulk read started, once it completed
         */
        RECONCILE
    }

    /**
     * A replay start that {@link #replayUntilLiveStartHolds} can start over from its origin.
     */
    protected interface ReplayStart<S extends ReplayStart<S>> {
        /**
         * The live start handed over to live delivery
         */
        Checkpoint liveFrom();

        /**
         * A start that replays again from where the first attempt at this replay started, and goes live from
         * {@code newLiveFrom}
         */
        S fromOrigin(Checkpoint newLiveFrom);

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

        /**
         * Whether the first attempt at this replay started at position 0 or at the beginning of time, so a replay again
         * from there skips no event, whatever live start it then goes live from
         */
        boolean startsAtTheBeginning();
    }

    /**
     * Resolves where a time replay starting at {@code start} reads from and goes live from.
     * <p>
     * A {@code start} that has a live start, stored by an earlier attempt at this replay, keeps it, so an event whose
     * time is earlier than the stored time but which was written after that attempt read past it is delivered live.
     * When the wrapped model no longer has the history from that live start, and the first attempt started at the
     * beginning of time, the replay starts over from there, with a live start read now, and redelivers what it
     * already delivered. That counts as a replay run again, as {@link #replayUntilLiveStartHolds} counts them. A
     * {@code start} without a live start gets one read now, before the replay.
     * <p>
     * Called on the virtual thread {@link #startCatchupAsync} started for an attempt, this hands the wrapped model a
     * {@code stillWanted} that answers {@code false} once the attempt is cancelled, stopped, shut down or taken over by
     * a later attempt, so a retry the wrapped model runs ends then. Called on any other thread, {@code stillWanted}
     * always answers {@code true}, and the wrapped model retries as it does for the calls a subscribe makes.
     * On that virtual thread, a {@code false} from {@code canResumeFrom} that came while the attempt wasn't wanted,
     * before the call or once it answered, or after a stop came during the call, isn't read as lost history, and the
     * replay is parked or ends instead.
     *
     * @throws CatchupCannotResumeException if the wrapped model no longer has the history from the live start in
     *                                      {@code start}, and the first attempt started after the beginning of time
     * @throws IllegalStateException        on that virtual thread, in place of a {@code false} from
     *                                      {@code canResumeFrom} that isn't read as lost history. The replay parks or
     *                                      ends only when it gets this same instance back, so an override that calls
     *                                      this should rethrow it unwrapped
     */
    protected TimeReplayStart timeReplayStart(String subscriptionId, Checkpoint start, @Nullable String liveStart, @Nullable StartAt delegatedStartAt) {
        if (!(CatchupCheckpoints.parse(start) instanceof CatchupCheckpoints.Time stored)) {
            throw new IllegalArgumentException("Not a time checkpoint: " + start.asString());
        }
        boolean noLive = delegatedStartAt == null;
        TimeBasedCheckpoint time = start instanceof TimeBasedCheckpoint timeBasedCheckpoint ? timeBasedCheckpoint : timeOf(stored.time());
        Checkpoint storedLiveFrom = stored.liveFrom();
        if (storedLiveFrom == null) {
            return new TimeReplayStart(time, captureLiveResumeCheckpoint(liveStart), time, noLive);
        }
        TimeBasedCheckpoint replayOrigin = timeOf(stored.replayOrigin());
        if (wrappedModelCanResumeFrom(storedLiveFrom)) {
            return new TimeReplayStart(time, storedLiveFrom, replayOrigin, noLive);
        }
        String origin = originAfterTheBeginning(stored);
        if (origin != null) {
            throw liveStartLostAfterTheBeginning(subscriptionId, origin, storedLiveFrom);
        }
        log.warn("The subscription model no longer has the history from the live start stored for catch-up subscription {}, so the catch-up replays again from time {} instead of {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayOrigin.asString(), time.asString(), storedLiveFrom.asString());
        countReplayAgainFromOrigin();
        return new TimeReplayStart(replayOrigin, captureLiveResumeCheckpoint(liveStart), replayOrigin, noLive);
    }

    // The origin the replay that stored a checkpoint started from, as a refusal names it, or null when it started at
    // position 0 or the beginning of time, where a replay again skips nothing. One rule for a subscribe that checks a
    // stored checkpoint and for the replay that starts from it.
    private static @Nullable String originAfterTheBeginning(CatchupCheckpoints.Start stored) {
        return switch (stored) {
            case CatchupCheckpoints.Position position -> position.replayOrigin() > 0 ? "position " + position.replayOrigin() : null;
            case CatchupCheckpoints.Time time -> {
                TimeBasedCheckpoint origin = timeOf(time.replayOrigin());
                yield origin.isBeginningOfTime() ? null : "time " + origin.asString();
            }
        };
    }

    private static TimeBasedCheckpoint timeOf(String time) {
        return TimeBasedCheckpoint.from(OffsetDateTime.parse(time, RFC_3339_DATE_TIME_FORMATTER));
    }

    /**
     * Resolves where a position replay starting at {@code start} reads from and goes live from.
     * <p>
     * A {@code start} that has a live start, stored by an earlier attempt at this replay, keeps it and the head that
     * attempt read, so an event whose position was reserved below the stored position but written after that attempt
     * read past it is delivered live. When the wrapped model no longer has the history from that live start, and the
     * first attempt started at position 0, the replay starts over from there, with a live start read now, and
     * redelivers what it already delivered. That counts as a replay run again, as {@link #replayUntilLiveStartHolds}
     * counts them. A {@code start} at position 0 without a live start gets one read now, before the replay.
     * <p>
     * Called on the virtual thread {@link #startCatchupAsync} started for an attempt, this hands the wrapped model a
     * {@code stillWanted} that answers {@code false} once the attempt is cancelled, stopped, shut down or taken over by
     * a later attempt, so a retry the wrapped model runs ends then. Called on any other thread, {@code stillWanted}
     * always answers {@code true}, and the wrapped model retries as it does for the calls a subscribe makes.
     * On that virtual thread, a {@code false} from {@code canResumeFrom} that came while the attempt wasn't wanted,
     * before the call or once it answered, or after a stop came during the call, isn't read as lost history, and the
     * replay is parked or ends instead.
     *
     * @throws CatchupCannotResumeException if {@code start} is a checkpoint a time replay stored, a position above 0
     *                                      without a live start, or one whose live start the wrapped model no longer
     *                                      has the history from while the first attempt started above position 0
     * @throws IllegalStateException        on that virtual thread, in place of a {@code false} from
     *                                      {@code canResumeFrom} that isn't read as lost history. The replay parks or
     *                                      ends only when it gets this same instance back, so an override that calls
     *                                      this should rethrow it unwrapped
     */
    protected PositionReplayStart positionReplayStart(String subscriptionId, Checkpoint start, @Nullable String liveStart, @Nullable StartAt delegatedStartAt) {
        if (!(CatchupCheckpoints.parse(start) instanceof CatchupCheckpoints.Position stored)) {
            throw new CatchupCannotResumeException("Cannot replay catch-up subscription " + subscriptionId + " by position from checkpoint \"" + start.asString()
                    + "\", because a catch-up that replays by time stored it.");
        }
        boolean noLive = delegatedStartAt == null;
        Checkpoint storedLiveFrom = stored.liveFrom();
        if (storedLiveFrom == null) {
            if (stored.position() > 0) {
                throw startsAfterPositionWithoutLiveStart(subscriptionId, stored.position());
            }
            return new PositionReplayStart(stored.position(), captureLiveResumeCheckpoint(liveStart), stored.position(), null, noLive);
        }
        long replayOrigin = stored.replayOrigin();
        if (wrappedModelCanResumeFrom(storedLiveFrom)) {
            return new PositionReplayStart(stored.position(), storedLiveFrom, replayOrigin, stored.replayTo(), noLive);
        }
        String origin = originAfterTheBeginning(stored);
        if (origin != null) {
            throw liveStartLostAfterTheBeginning(subscriptionId, origin, storedLiveFrom);
        }
        log.warn("The subscription model no longer has the history from the live start stored for catch-up subscription {}, so the catch-up replays again from position {} instead of {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayOrigin, stored.position(), storedLiveFrom.asString());
        countReplayAgainFromOrigin();
        return new PositionReplayStart(replayOrigin, captureLiveResumeCheckpoint(liveStart), replayOrigin, null, noLive);
    }

    // A start that falls back to its origin replays again, so it counts once also when no takeover passed a count on.
    // Read by replayUntilLiveStartHolds once the start is resolved, on the attempt's own thread.
    private static void countReplayAgainFromOrigin() {
        ReplayState state = CURRENT_ATTEMPT.get().replay;
        state.replaysAgainBefore = Math.max(state.replaysAgainBefore, 1);
    }

    /**
     * Runs {@code replay} from {@code first} and returns the start whose live start the catch-up hands over to. After
     * each replay this asks the wrapped model whether it can still resume from that live start. When it can't, the
     * live start left the change stream history during the replay, and {@code replay} runs again from the position
     * or time the first attempt started from, with a live start read now, when that is position 0 or the beginning of
     * time. From a later origin it throws {@link CatchupCannotResumeException} instead, since a replay from there with
     * a live start read now would skip an event before the origin that was written after the lost live start was
     * read. Handing the lost live start over instead would leave
     * it to the wrapped model, and a MongoDB model that restarts on lost history goes live from the present and skips
     * every event between the live start and the restart. A live start read now is asked about after its own replay
     * too, since a long replay can lose it as well.
     * <p>
     * So the catch-up goes live only from a live start the wrapped model accepted after the last replay, or throws
     * {@link CatchupCannotResumeException} once the replay ran {@value #MAX_REPLAYS_AGAIN} times again and lost the live
     * start each time. It never hands over a live start the wrapped model answered false for, but delivers the events
     * it replays again more than once. The check and the handover are two calls, so a live start can still leave the
     * history after the check. A wrapped model that reports lost history and repositions subscriptions, such as the
     * MongoDB ones, asks this model what to do when it loses that live start as it opens, and this model then takes the
     * subscription over and replays again, as {@link #startAtToUse} describes. Any other wrapped model handles the lost
     * history as it would without this model. A replay that was stopped,
     * cancelled or taken over is not asked about, and its start is returned as it is for the handover to deal with.
     * <p>
     * Each run of {@code replay} reads through one {@link ReplayScope} that {@code scopes} opens for it. The scope gives
     * the live start, which the event store reads in the session that then makes every read of the replay, and it is
     * closed when that run ends, whether it completes, fails or is cancelled. A replay that runs again opens a new
     * scope. {@code notBefore} is the live start stored for the subscription, if there is one, and the scope never
     * reads a live start from before it.
     * <p>
     * A {@link CatchupListener} is told the catch-up started again before each replay run again, since what that
     * replay delivers is history read again and not events written since the catch-up started. Only meaningful on the
     * virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected <S extends ReplayStart<S>, R> S replayUntilLiveStartHolds(String subscriptionId, ReplayScopes<R> scopes, @Nullable String notBefore, Function<@Nullable String, S> firstStart,
                                                                         BiConsumer<S, R> replay) {
        ReplayState state = CURRENT_ATTEMPT.get().replay;
        // Counted on from the replays before a takeover, and from zero by an attempt that runs again after a failure
        int carried = state.replaysAgainBefore;
        state.replaysAgainBefore = 0;
        int replaysAgain;
        S replayed;
        // One scope for the live start and every read of one replay, closed when it ends, however it ends
        try (ReplayScope<R> scope = scopes.open(notBefore)) {
            state.longestReplay.liveStartRead();
            replayed = firstStart.apply(scope.liveStart());
            // A start that fell back to its origin counted itself as a replay run again
            replaysAgain = Math.max(carried, state.replaysAgainBefore);
            state.replaysAgainBefore = 0;
            if (replaysAgain > MAX_REPLAYS_AGAIN) {
                throw liveStartLostEachTime(subscriptionId, replaysAgain, replayed.liveFrom(), state.longestReplay);
            }
            replay.accept(replayed, scope.reads());
        }
        for (; ; replaysAgain++) {
            S again = replayAgainIfLiveStartLost(subscriptionId, replayed, replaysAgain, scopes, replay);
            if (again == null) {
                state.replaysAgain = replaysAgain;
                return replayed;
            }
            replayed = again;
        }
    }

    /**
     * Returns {@code store} as the {@code spi} a catch-up reads through. A catch-up reads its live start and every
     * replay read in one session of the event store, so it can't read through a store that doesn't implement the SPI,
     * such as a decorator around one.
     *
     * @throws IllegalArgumentException if {@code store} doesn't implement {@code spi}
     */
    protected static <T> T requireCatchupReads(Object store, Class<T> spi) {
        if (!spi.isInstance(store)) {
            throw new IllegalArgumentException("Cannot run a catch-up subscription over " + store.getClass().getName() + ", because it does not implement " + spi.getSimpleName() + " (" + spi.getName()
                    + "). A catch-up reads its live start and every replay read from the event store in one session, which only a store that implements it can do. Hand the catch-up the event store itself and decorate around the subscription instead.");
        }
        return spi.cast(store);
    }

    /**
     * Opens the {@link ReplayScope} of one replay, with the live start the event store reports and the reads that see
     * everything up to it.
     */
    @FunctionalInterface
    protected interface ReplayScopes<R> {
        /**
         * @param notBefore The live start an earlier attempt stored, which the reads must see everything before, or null
         */
        ReplayScope<R> open(@Nullable String notBefore);
    }

    /**
     * Opens the {@link ReplayScope} of one replay, as {@link ReplayScopes#open} describes.
     */
    protected abstract ReplayScope<?> openScope(@Nullable String notBefore);

    /**
     * The reads of one replay and the live start they are consistent with. Closing it releases what the event store
     * holds for the replay.
     *
     * @param reads     What the replay reads through
     * @param liveStart The live start the event store reports for the replay, or null when it reports none
     * @param closer    Ends the scope
     */
    protected record ReplayScope<R>(R reads, @Nullable String liveStart, Runnable closer) implements AutoCloseable {
        @Override
        public void close() {
            closer.run();
        }
    }

    /**
     * The live start stored in a checkpoint by an earlier attempt, or null when it has none
     */
    protected static @Nullable String storedLiveStart(Checkpoint start) {
        Checkpoint liveFrom = CatchupCheckpoints.parse(start).liveFrom();
        return liveFrom == null ? null : liveFrom.asString();
    }

    private static CatchupCannotResumeException liveStartLostEachTime(String subscriptionId, int replays, Checkpoint lastLiveStart, LongestReplay longestReplay) {
        return HistoryLossMessages.liveStartLost("Cannot hand catch-up subscription " + subscriptionId + " over to live delivery, because the subscription model lost the live start during or right after each of its "
                + replays + " replays. The longest replay took " + longestReplay.longest().toMillis() + " ms from reading its live start to checking it, so size the change stream history, such as the MongoDB oplog, to reach back at least that far. Last live start: "
                + lastLiveStart.asString(), lastLiveStart, true);
    }

    private static CatchupCannotResumeException startsAfterPositionWithoutLiveStart(String subscriptionId, long position) {
        return new CatchupCannotResumeException("Cannot start catch-up subscription " + subscriptionId + " after position " + position + ", because the checkpoint holds no live start read before position " + position
                + " was, and an event that committed below position " + position + " after it was read would never be delivered. Cancel the subscription first, which deletes the checkpoint stored for it."
                + " Then start the subscription from the beginning, or from the checkpoint it delivered with the event at position "
                + position + ", which EventMetadata.getCheckpoint() returns.");
    }

    // A replay again from an origin after the beginning, with a live start read now, skips an event before the origin
    // that was written after the lost live start was read
    private static CatchupCannotResumeException liveStartLostAfterTheBeginning(String subscriptionId, String origin, Checkpoint lostLiveStart) {
        return HistoryLossMessages.liveStartLost("Cannot replay catch-up subscription " + subscriptionId + " again from " + origin + ", because the subscription model no longer has the history from its live start,"
                + " and a replay from there with a live start read now would never deliver an event before " + origin + " that was written after the lost live start was read."
                + " Cancel the subscription first, which deletes the checkpoint stored for it. Start the subscription from the beginning again, and size the change stream history, such as the MongoDB oplog, to reach back across the replay. Lost live start: " + lostLiveStart.asString(), lostLiveStart, false);
    }

    // Null when the wrapped model can still resume from the live start, or when this attempt should no longer replay
    private <S extends ReplayStart<S>, R> @Nullable S replayAgainIfLiveStartLost(String subscriptionId, S replayed, int replaysAgain, ReplayScopes<R> scopes, BiConsumer<S, R> replay) {
        Checkpoint liveFrom = replayed.liveFrom();
        if (!shouldKeepReplaying(subscriptionId)) {
            return null;
        }
        LongestReplay longestReplay = CURRENT_ATTEMPT.get().replay.longestReplay;
        longestReplay.liveStartChecked();
        if (wrappedModelCanResumeFrom(liveFrom)) {
            return null;
        }
        if (!replayed.startsAtTheBeginning()) {
            throw liveStartLostAfterTheBeginning(subscriptionId, replayed.describeOrigin(), liveFrom);
        }
        if (replaysAgain >= MAX_REPLAYS_AGAIN) {
            throw liveStartLostEachTime(subscriptionId, replaysAgain + 1, liveFrom, longestReplay);
        }
        log.warn("The live start of catch-up subscription {} left the subscription model's history during the replay, so the catch-up replays again from {} and redelivers the events in between. Live start: {}",
                subscriptionId, replayed.describeOrigin(), liveFrom.asString());
        // The new scope reads the live start this replay goes live from, and every read of the replay
        try (ReplayScope<R> scope = scopes.open(null)) {
            longestReplay.liveStartRead();
            S again = replayed.fromOrigin(captureLiveResumeCheckpoint(scope.liveStart()));
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
            replay.accept(again, scope.reads());
            return again;
        }
    }

    /**
     * Whether {@code checkpoint} is a global position or a time, with or without a live start, which is what a
     * catch-up stores during its replay. The handover replaces any of them with the live start, whichever kind of
     * catch-up wrote it, since the MongoDB subscription models don't recognize either and open at the present.
     */
    protected static boolean isCatchupCheckpoint(Checkpoint checkpoint) {
        return GlobalCheckpoint.isGlobalCheckpoint(checkpoint) || StreamCatchupSubscriptionModel.isTimeBasedCheckpoint(checkpoint);
    }

    // Whether this model replays from given when a subscription starts at it, so a positioned resume stores it as a
    // catch-up position rather than as a position of the live delegate
    protected boolean replaysFrom(Checkpoint given) {
        return isCatchupCheckpoint(given);
    }

    /**
     * Refuses {@code stored} when it is a time without a live start, which a catch-up stored before the live start was
     * kept. A resume from it would go live from a live start read now and miss an event whose time is earlier than the
     * stored time but which was written after the earlier replay read past it. A time the caller starts from instead is
     * a start by event time, which reads its live start before the replay. The beginning of time is not refused, since
     * no event has an earlier time. A stored position without a live start is refused where the replay resolves its
     * start, as {@link #positionReplayStart} describes.
     *
     * @throws CatchupCannotResumeException if {@code stored} is a time after the beginning of time without a live start
     */
    protected static void refuseIfStoredWithoutLiveStart(String subscriptionId, @Nullable Checkpoint stored) {
        // A marked form always holds a live start
        if (stored == null || CatchupCheckpoints.isMarked(stored)) {
            return;
        }
        if (StreamCatchupSubscriptionModel.isTimeBasedCheckpoint(stored) && !CatchupTimeCheckpoint.isCatchupTimeCheckpoint(stored)
                && !timeOf(stored.asString()).isBeginningOfTime()) {
            throw new CatchupCannotResumeException("Cannot resume catch-up subscription " + subscriptionId + " from stored checkpoint \"" + stored.asString() + "\", because it holds no live start,"
                    + " and an event with an earlier time that was written after the earlier replay read past that time would never be delivered."
                    + " Cancel the subscription, which deletes the checkpoint, and start it from the beginning of time, or from a checkpoint it delivered with an event, which EventMetadata.getCheckpoint() returns.");
        }
    }

    /**
     * Cancels the catch-up of {@code subscriptionId} and the subscription in the live delegate, and deletes its stored
     * position. When the replay has already let an event through to the subscriber's action and a stop hasn't parked it
     * since, this waits up to a second for the action to return before it cancels the subscription in the live
     * delegate. When the action hasn't returned by then, it logs a {@code WARN} and goes on. The replay lets no further
     * event through and stores nothing more once the action returns. It doesn't wait either for a subscribe of the id
     * on another thread whose start-position function is asked for this model with no claim held. That subscribe
     * subscribes nothing, and the subscription it returns never starts. A cancel the function makes on the subscribing
     * thread doesn't stop that subscribe, and neither does a cancel that throws. While this runs,
     * {@link #start(boolean) start(true)} doesn't let the live delegate resume the subscription itself.
     * <p>
     * This doesn't wait for a call the live delegate makes to the action, and the live delegate's own cancel need not
     * wait for one either. Such a call, and a replay's action still running past the second, can be in the action when
     * this returns. A call the live delegate starts after this has forgotten the subscription doesn't reach the action,
     * and throws {@link AttemptNotMade}, unless this model has begun to shut down by then. Such a call reaches the action
     * without being counted, as {@link #subscribeInTheWrappedModel} describes for a shutdown.
     * A later subscribe of the id calls the action, from its replay or from the live delegate, only once every call from
     * before this cancel has returned, so the action of one subscription id never runs alongside itself across a cancel
     * and a later subscribe. That subscribe waits with no time limit, so an action from before the cancel that never
     * returns holds back every event of the later subscription.
     * <p>
     * Called from inside the action of a live call or a replay of this model after that action handed a call over, such
     * as a {@link #stop()} or a {@link #resumeSubscription(String)}, it returns at once and cancels the subscription on
     * the lifecycle thread, once the action has returned and that call has run. What it throws there is logged at
     * {@code ERROR}.
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        if (handedOverBehindTheCallsTheActionHandedOver(this, subscriptionId, "cancelSubscription(" + subscriptionId + ")", () -> cancelSubscription(subscriptionId))) {
            return;
        }
        try (Claim ignored = claimWithoutWaitingForAsks(subscriptionId)) {
            Runnable cancelAskedSubscribes = askedSubscribesToCancel(subscriptionId);
            boolean nothingKept = cancelKeepingWhatWasSubscribedOnFailure(subscriptionId, () -> {
                cancelRunningCatchup(subscriptionId);
                try {
                    subscriptionModel.cancelSubscription(subscriptionId);
                } finally {
                    positionMayHaveMoved(subscriptionId);
                }
            });
            if (nothingKept) {
                deletePositionFromStorage(subscriptionId);
            }
            cancelAskedSubscribes.run();
        }
    }

    // Called by a cancel once it holds the claim, for the subscribes of the id whose function another thread asks
    // then. A subscribe made later waits for the cancel and then goes ahead. The cancel runs what this returns only
    // once it has run without throwing, so a subscribe whose function is asked still goes ahead when the cancel
    // throws. A cancel the function makes on the subscribing thread doesn't stop that subscribe, as before the
    // function was asked ahead of the claim
    Runnable askedSubscribesToCancel(String subscriptionId) {
        Thread cancelling = Thread.currentThread();
        List<AskedFirst> pending = askedFirst.stream().filter(asked -> asked.subscriptionId.equals(subscriptionId) && asked.asker != cancelling).toList();
        return () -> pending.forEach(asked -> asked.cancelled = true);
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
            AtomicReference<@Nullable Subscribed> forgotten = new AtomicReference<>();
            boolean nothingKept = subscribed.computeIfPresent(subscriptionId, (id, now) -> {
                if (now == made || now == current.kept) {
                    forgotten.set(now);
                    return null;
                }
                return now;
            }) == null;
            Subscribed forgottenNow = forgotten.get();
            if (forgottenNow != null) {
                forgetPositionMovesUnlessSubscribed(subscriptionId);
                waitForTheLiveCallsOf(subscriptionId, forgottenNow);
            }
            return nothingKept;
        } catch (Throwable e) {
            // Throwable, since a live model written in Kotlin can throw a checked exception
            if (made != null) {
                // Before the cancel under way is removed, so one of them always has the replay. Kept in place, also when
                // a cancel the live delegate's cancel made for the same id on this thread went through.
                Subscribed kept = new Subscribed(made.owner(), made.filter(), made.startAt(), made.action(), current.replaysReturned, made.lostLiveStart(), made.historyLost(), made.didNotGoLive(), made.liveCalls());
                Subscribed now;
                synchronized (subscribed) {
                    now = subscribed.compute(subscriptionId, (id, was) -> was == null || was == made ? kept : was);
                }
                if (now == kept) {
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
     * <p>
     * When the replay has already let an event through to the subscriber's action, and a stop hasn't parked it since,
     * this waits up to a second for the action to return, and logs a {@code WARN} and returns when it hasn't by then.
     * It doesn't wait when called from that action. The replay lets no further event through either way. It then waits,
     * with no time limit, for a catch-up checkpoint store the replay has begun, so that store has returned before the
     * cancel of the subscription deletes the stored position. The store and the delete go to the same
     * checkpoint storage, and the delete has no time limit either, so a storage that doesn't answer holds the wait back
     * until it answers, as it would hold the delete back. With their default {@code RetryStrategy} the MongoDB and Redis
     * checkpoint storages retry both until the storage answers or is shut down. The wait comes before the live delegate
     * cancels the subscription, and the delete after it.
     */
    public void cancelRunningCatchup(String subscriptionId) {
        // Locked, so this call lands either strictly before or strictly after a handover attempt's own lockHandover
        // span for the same id, never inside it. Unlocked, it could run in the gap after that attempt decided it was
        // still current but before it acted on that, finding nothing left to flag and losing the cancellation. The
        // lock it takes for an id no attempt was ever registered for is removed once it is released.
        @Nullable ActionInFlight admitted = null;
        // Read under the lock that a replay records a store under, together with the flag that stops further stores
        CompletableFuture<Void> storesUnderWay = CompletableFuture.completedFuture(null);
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
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
            CatchupAttempt cancelled = currentAttempt.computeIfPresent(subscriptionId, (id, attempt) -> {
                attempt.replay.cancelled = true;
                return attempt;
            });
            // The action and the store waited for only by the child that runs the attempt, since the children share
            // currentAttempt and a dispatcher cancels on each of them. Any child that finds the attempt records the
            // dropped replay below, which is one shared record whichever child adds it
            if (cancelled != null && cancelled.owner == this && cancelled != CURRENT_ATTEMPT.get()) {
                admitted = new ActionInFlight(subscriptionId, cancelled.actionReturned, cancelled.actionUnwanted);
                storesUnderWay = cancelled.replay.storeUnderWay;
            }
            // Kept until the attempt and every earlier one it waits for have returned, whichever path removes it from
            // currentAttempt later. The wait for the action above gives up after a second, and an attempt that never
            // reached the action removes itself at once, so without this a later replay or live event of the id could
            // reach the action while an earlier replay is still in it.
            if (cancelled != null) {
                waitForTheDroppedReplay(subscriptionId, cancelled.done);
            }
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
                waitForTheDroppedReplay(subscriptionId, parked.earlierAttemptsDone());
                storesUnderWay = CompletableFuture.allOf(storesUnderWay, parked.replay().storeUnderWay);
            }
        }
        if (admitted != null) {
            awaitTheAdmittedActions(List.of(admitted), "cancel");
        }
        // With no time limit, as the delete of the stored position that follows would wait for the same storage. A
        // replay stores a position only after its action returned, so none is under way when the action itself cancels.
        if (!replaysOnTheCallingThread(subscriptionId)) {
            awaitUninterruptibly(storesUnderWay);
        }
    }

    // Waits until done, which only completes normally, has completed. An interrupt doesn't end the wait, and is set
    // again once the wait is over.
    private static void awaitUninterruptibly(CompletableFuture<Void> done) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    done.get();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    return;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Takes a replay a resume started for {@code subscriptionId} away from its attempt and holds it, when it runs on this
     * model, so it delivers nothing more, and returns it, or one already held on this model, as a replay in the way of a
     * resume at a position the caller gives. A replay that a cancel that threw ended for a subscription made through this
     * model, or that a cancel still under way ended, which nothing runs again, is returned as one to wait for, and so is
     * a position store that a replay a subscribe started began before a stop parked it. Returns
     * {@code null} when there is none. Called with the {@link #claim} held, so no other lifecycle call can run the held
     * replay again before the caller ends it or puts it back. The caller tells the call to the subscriber's action that
     * the held replay let through that its event is no longer wanted, with
     * {@link ReplayInTheWay#tellTheActionItIsUnwanted()}, once it has let go of the handover lock.
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
            // A replay a subscribe started is in the way only once the live delegate lost its live start, since the
            // live delegate then holds nothing for the id, and the resume subscribes the catch-up at the given position
            // in its place
            CompletableFuture<Void> storeOfAParkedSubscribe = CompletableFuture.completedFuture(null);
            if (parked != null && !parked.replay().resuming && !parked.replay().lostItsLiveStart) {
                // A stop doesn't wait for a position store the replay began, and that store would otherwise write
                // over the position the resume stores
                storeOfAParkedSubscribe = parked.replay().storeUnderWay;
                parked = null;
            }
            // A stop takes a cancelled replay out of currentAttempt and parkedReplays while it can still be in the action
            CompletableFuture<Void> cancelledReplayReturned = cancelledReplayReturned(subscriptionId);
            if (running == null && parked == null && cancelledReplayReturned.isDone() && storeOfAParkedSubscribe.isDone()) {
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
            // Read under the handover lock its delivery sets the stage under
            CompletableFuture<Void> actionUnwanted = heldByTheResume ? running.actionUnwanted : CompletableFuture.completedFuture(null);
            return new ReplayInTheWay(subscriptionId, parked, heldByTheResume, CompletableFuture.allOf(cancelledAttemptDone, cancelledReplayReturned, storeOfAParkedSubscribe,
                    parked == null ? CompletableFuture.completedFuture(null) : parked.earlierAttemptsDone()), actionUnwanted);
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
     * {@code actionReturned} completes once the thread of every attempt at it has returned. {@code actionUnwanted} is the
     * stage of the call to the subscriber's action that the attempt the resume held let through.
     */
    final class ReplayInTheWay {
        private final String subscriptionId;
        private final @Nullable ParkedReplay held;
        private final boolean heldByTheResume;
        private final CompletableFuture<Void> actionReturned;
        private final CompletableFuture<Void> actionUnwanted;

        private ReplayInTheWay(String subscriptionId, @Nullable ParkedReplay held, boolean heldByTheResume, CompletableFuture<Void> actionReturned,
                               CompletableFuture<Void> actionUnwanted) {
            this.subscriptionId = subscriptionId;
            this.held = held;
            this.heldByTheResume = heldByTheResume;
            this.actionReturned = actionReturned;
            this.actionUnwanted = actionUnwanted;
        }

        // Tells the call to the subscriber's action that the held attempt let through that its event is no longer
        // wanted, as a stop does, so an action that waits for a competing consumer's lease withdraws the event and
        // returns. Called with no handover lock held, since completing the stage runs what the action's wait put on it.
        // The replay delivers that event again once it runs again, from the last position it stored.
        void tellTheActionItIsUnwanted() {
            actionUnwanted.complete(null);
        }

        // A replay a subscribe started that is held because the live delegate lost its live start, so the live delegate
        // has no subscription for the id to resume
        boolean neverWentLive() {
            return held != null && !held.replay().resuming;
        }

        // Subscribes the id from startAt with the filter and action it was made with, as a subscribe at startAt would,
        // in place of a resume of the live delegate. Under the handover lock, as a subscribe is, so a replay for the id
        // can't hand over or fail while it subscribes.
        Subscription subscribeInPlaceOfTheHeldReplay(StartAt startAt, RepositionableSubscriptions liveDelegate) {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                Subscribed made = subscribed.get(subscriptionId);
                if (held == null || made == null || made.owner() != AbstractCatchupSubscriptionModel.this || parkedReplays.get(subscriptionId) != held) {
                    throw new IllegalStateException("Cannot resume subscription " + subscriptionId + " from the given position, because the catch-up replay it was held for is gone. " + NOTHING_CHANGED);
                }
                StartAt answered = answeredForThisModel(startAt);
                // The held replay stored the position it replayed to, and a resume after a pause or a restart would
                // replay from there again. The position given here replaces it when a restart starts from it. A start
                // from now goes live from the present position it stores, as a durable model's resume at now does, so an event that commits
                // between the store and the subscribe is delivered. A dynamic start is decided by what it answers this
                // model, which answered holds, so its function doesn't run again here.
                @Nullable StartAt forThisModel = answered.isDynamic() ? answered.get(generateSubscriptionModelContext()) : answered;
                StoredPositionReplaced replaced = replaceTheStoredPosition(subscriptionId, forThisModel, false, liveDelegate);
                // Cleared before the subscribe, so a loss the replay it starts records is kept, whether the subscribe
                // records it on this thread or the replay records it once this lets go of the handover lock
                HistoryLost.Cleared cleared = made.historyLost().clearUntilPutBack(resumedAt(startAt));
                Checkpoint present = replaced == null ? null : replaced.present;
                StartAt subscribeAt = present == null ? answered : goingLiveFrom(answered, StartAt.checkpoint(present));
                Subscription subscription;
                try {
                    subscription = subscribe(subscriptionId, made.filter(), subscribeAt, made.action(), false, made);
                } catch (Throwable e) {
                    // Throwable, since a live model written in Kotlin can throw a checked exception
                    made.historyLost().putBack(cleared);
                    if (replaced != null) {
                        try {
                            replaced.putBack(e);
                        } finally {
                            positionMayHaveMoved(subscriptionId);
                        }
                    }
                    throw e;
                }
                logResumedAfterLoss(subscriptionId, startAt, cleared.lost());
                return subscription;
            }
        }

        // The replay stores nothing more and doesn't hand over, and waitUntilStarted on its subscriber's handle returns
        // false. What is kept for the subscription stays, so a later resumeSubscription(id) can replay again.
        void end() {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                if (held != null && parkedReplays.remove(subscriptionId, held)) {
                    held.replay().cancelled = true;
                    held.replay().result.cancel(false);
                    waitForTheDroppedReplay(subscriptionId, held.earlierAttemptsDone());
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
        } catch (Throwable e) {
            // Throwable, since runEach rethrows what a step threw as it was thrown
            putBackFailure = e;
        }
        String whatChanged = putBackFailure != null ? "A catch-up replay this call held for the subscription couldn't be run again, and the suppressed exception says why."
                : !allRunAsBefore.get() ? "A catch-up replay this call held for the subscription is still held, because the model was stopped or the catch-up listener threw as the replay started again."
                + " It runs again once the subscription is resumed or the model started."
                : NOTHING_CHANGED;
        IllegalStateException timedOut = new IllegalStateException("Cannot resume subscription " + subscriptionId + " from the given position, because its catch-up replay didn't return from the action, a position store, the history read or the check of its live start within "
                + LIFECYCLE_WAIT.toMillis() + " ms. " + whatChanged);
        if (putBackFailure != null) {
            timedOut.addSuppressed(putBackFailure);
        }
        log.error("Subscription {} was not resumed from the given position, because its catch-up replay didn't return from the action, a position store, the history read or the check of its live start within {} ms. {}",
                subscriptionId, LIFECYCLE_WAIT.toMillis(), whatChanged);
        throw timedOut;
    }

    /**
     * Waits at most {@link #LIFECYCLE_WAIT} until every replay has returned that a cancel that threw, or a cancel still
     * under way, ended for {@code subscriptionId}, before the live delegate resumes the subscription without a replay.
     * When one hasn't, logs at {@code ERROR} and throws {@link IllegalStateException}, as it does at once when called on
     * the thread of such a replay, such as in its {@link CatchupListener}. {@code whatChanged} says what the caller did
     * before, which the log and the exception repeat.
     */
    void awaitTheCancelledReplay(String subscriptionId, String whatChanged) {
        Subscribed made = subscribed.get(subscriptionId);
        CompletableFuture<Void> cancelledReplayReturned = CompletableFuture.allOf(made == null ? CompletableFuture.completedFuture(null) : made.cancelledReplayReturned(),
                replaysACancelUnderWayEnded(subscriptionId));
        if (cancelledReplayReturned.isDone()) {
            return;
        }
        if (replaysOnTheCallingThread(subscriptionId)) {
            throw new IllegalStateException("Cannot resume subscription " + subscriptionId + " on the thread of its cancelled catch-up replay, because that replay can't return"
                    + " while the resume waits for it. Resume it from another thread. " + whatChanged);
        }
        if (!whileWaitingForOneSubscription(() -> completesWithin(cancelledReplayReturned, LIFECYCLE_WAIT))) {
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
     * Flags this model as shutting down without forgetting anything yet. A dispatcher calls it on every child before it
     * calls {@link #markShuttingDown()} on any, since the children share the replays that call ends. A replay of a later
     * child that lets an event through just after the first child's {@link #markShuttingDown()} looked at the replays
     * then reads the flag of its own model, finds it set, and marks the event unwanted.
     */
    void flagShuttingDown() {
        shutdownBeganNanos.compareAndSet(0, System.nanoTime());
        shuttingDown = true;
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        flagShuttingDown();
        runningCatchupSubscriptions.clear();
        catchupListeners.clear();
        // Every attempt the clear removes, a sibling's included, since a composite shuts its children down together.
        // An action that waits on the stage returns instead of waiting for a lease no one releases. A delivery that
        // sets its stage after this read sees shuttingDown in deliverIfStillWanted and completes the stage there.
        currentAttempt.values().forEach(attempt -> attempt.actionUnwanted.complete(null));
        currentAttempt.clear();
        pauseRequestedDuringCatchup.clear();
        // Marked before the clear, so a live call that finds its record gone after the clear finds the mark too. Under
        // the monitor every write that puts a record holds, so no record is put between the marks and the clear
        synchronized (subscribed) {
            subscribed.values().forEach(made -> made.liveCalls().forgetForShutdown());
            betweenShutdownMarksAndClear.run();
            subscribed.clear();
        }
        positionMoves.keySet().forEach(this::forgetPositionMovesUnlessSubscribed);
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
     * Each replay is parked under its {@link #claim}. A failure to claim one, or to take its handover lock, is thrown
     * once the others are parked, and that replay parks itself once it next asks whether to go on. It stays parked
     * until {@link #relaunchParkedReplays()} or {@link #resumeSubscription(String)} runs it, as one this parked does,
     * also when a {@code start(false)} or {@link #resumeReplay()} came in between.
     * <p>
     * A replay lets no event through to the subscriber's action once it is parked. A call to the action that a replay
     * started before that is waited for, up to a second for all of them together. Past the second this returns all the
     * same and logs a {@code WARN} naming the subscriptions whose action still runs. It waits for each replay of this
     * model it claims, cancelled or not, so a stop after a cancel whose action still runs waits for that action again,
     * and a cancel after this doesn't wait for a replay this parked.
     * <p>
     * It runs alone, as {@link #runInTurn} describes, and waits with no deadline for a {@link #start(boolean)},
     * {@link #stop()} or {@code shutdown()} under way, unless code a caller supplied holds it up as {@link #runInTurn}
     * describes. Called from inside the action of a live call or a replay of this
     * model, it returns at once and takes effect on another thread once the action has returned, and what it throws
     * there is logged at {@code ERROR}. From any other thread it also waits for the calls such actions handed over
     * before it.
     *
     * @throws IllegalStateException where {@link #stop()} throws it. Nothing was changed then.
     */
    public void stopReplay() {
        runInTurn(this, "stopReplay()", () -> {
            List<ActionInFlight> admitted = new ArrayList<>();
            try {
                stopReplayLeavingTheActions(admitted);
            } finally {
                awaitTheAdmittedActions(admitted, "stop");
            }
        });
    }

    /**
     * Parks every replay in flight as {@link #stopReplay()} does, without waiting for the actions, and adds to
     * {@code admitted} each call to the subscriber's action that one of them let through. A dispatcher stops each child
     * through this and then waits once for all of them, so a stop waits up to a second in all, not a second per child.
     */
    void stopReplayLeavingTheActions(List<ActionInFlight> admitted) {
        synchronized (lifecycleLock) {
            stopped = true;
            stoppedByFailedStart = false;
            lifecycleGeneration++;
            stops++;
            stopsAsked++;
        }
        parkReplaysInFlight(admitted);
    }

    // Adds to admitted each call to the subscriber's action that a parked replay let through, also when parking another
    // one throws. Never called on a replay thread, since runInTurn hands a call from a replay's action to the lifecycle
    // thread and refuses one from a replay thread outside the action
    private void parkReplaysInFlight(List<ActionInFlight> admitted) {
        if (shuttingDown) {
            return;
        }
        runEach(List.copyOf(currentAttempt.entrySet()), entry -> {
            CatchupAttempt attempt = entry.getValue();
            if (attempt.owner == this) {
                // Before the claim and the lock, either of which this can give up on while code a caller supplied holds
                // it, so the replay then parks itself once that code returns, whatever lets replays run again meanwhile
                // short of a start(true) or a resume of the subscription
                attempt.parkOwedSince = relaunchesBegun;
                try (Claim ignored = claim(entry.getKey(), STOPPED_ALL_THE_SAME); HandoverLock alsoIgnored = lockHandover(entry.getKey())) {
                    parkIfStillCurrent(entry.getKey(), attempt);
                    admitted.add(new ActionInFlight(entry.getKey(), attempt.actionReturned, attempt.actionUnwanted));
                }
            }
        });
    }

    /**
     * Allows the next replay on this model to run, without touching the shared live delegate. Does not run a replay
     * {@link #stopReplay()} parked, which {@link #relaunchParkedReplays()} does, apart from one whose replay thread is
     * still parking it.
     * <p>
     * It runs alone and waits with no deadline, as {@link #stopReplay()} does, and like it takes effect once the action
     * has returned when called from inside the action of a live call or a replay of this model.
     *
     * @throws IllegalStateException where {@link #stop()} throws it. Nothing was changed then.
     */
    public void resumeReplay() {
        runInTurn(this, "resumeReplay()", () -> {
            synchronized (lifecycleLock) {
                stopped = false;
                stoppedByFailedStart = false;
                lifecycleGeneration++;
            }
            // Keeps what the call before recorded for the live delegate, which this doesn't touch
            lastLifecycleCall.updateAndGet(last -> last.next(false, last.liveDelegateStopped()));
        });
    }

    /**
     * Runs {@code body}, the {@code call} of {@code model}, while holding the lock that lets one {@link #start(boolean)},
     * {@link #stop()}, {@code shutdown()} or {@link #resumeSubscription(String)} of this model, or of any other child of
     * the same dispatcher, run at a time.
     * It waits for the lock with no deadline, unless code a caller supplied holds the lock up, as described below.
     * <p>
     * Called from inside the action of a live call or a replay of this model or of another child of the same dispatcher,
     * it returns at once without running {@code body}, since a start, stop or shutdown holding the lock can be waiting
     * for that action to return, in the live delegate or for the replay it parks. A replay's action is in the middle of
     * a decision for its subscription, so the call would also wait for a per-id call that can only finish once the
     * action returns. The lifecycle thread the children share runs {@code body} instead, once the action has returned
     * and the lock is free. It gets the calls an action handed over when that action returns, in the order the action
     * made them, so calls handed over from two actions run in the order the actions returned. So the call takes effect
     * once the action has returned, and an action that never returns keeps the calls handed over from it from taking
     * effect, but no call handed over from another action. It doesn't run {@code body} when a {@code shutdown()} has
     * begun by then, and logs an {@code ERROR} naming {@code model} for what {@code body} throws. So the call gets the
     * same outcome whether or not another thread holds the lock when it is made.
     * <p>
     * Called from any other thread while a call handed over, or the shutdown a held-up {@code shutdown()} handed over,
     * hasn't returned, it waits for every action that handed a call over and hasn't returned, hands {@code body} to the
     * lifecycle thread too, waits for it to run there after those calls, and throws what it throws. So a call made after
     * the action that handed one over signalled the calling thread takes effect after the one handed over. Like a handed
     * call, it doesn't run {@code body} when a {@code shutdown()} has begun by then, and returns normally. The calling
     * thread runs no action of these children, so the actions it waits for run on other threads. An action that, after
     * handing a call over, waits for a call it asked another thread to make would then never return, so the wait for
     * the actions gives up after {@link #LIFECYCLE_WAIT} and the call throws {@link IllegalStateException} without
     * running {@code body}. A {@link #resumeSubscription(String)} waits only for the actions whose handed calls concern
     * the subscription it resumes, as {@link #resumeInTurn} describes, so an action of one subscription that handed over
     * only calls for that subscription and doesn't return holds up no resume of another subscription.
     * <p>
     * A call made before the shutdown a held-up {@code shutdown()} handed over has returned doesn't run {@code body} on
     * any thread, and returns normally. This includes a call that found no handed call pending and took the lock only
     * once that shutdown had shut the live delegate down, and a call from the thread that holds the lock. A call
     * made once that shutdown has returned, with no handed call pending, runs {@code body}.
     * <p>
     * The thread that holds the lock, the lifecycle thread or another one, can run code a caller supplied, such as a
     * {@code StartAt.dynamic} function that a subscribe, a resume or the live delegate asks, or a {@link CatchupListener}
     * told that a replay starts. While it does, a call from any other thread that waits for the lock, or for the
     * lifecycle thread to run it, waits up to {@link #LIFECYCLE_WAIT}, counted from when it began to wait or from when
     * that code began, whichever came later. Then it throws {@link IllegalStateException} without running {@code body},
     * since that code may be waiting for this very call, which would then never run. Code that waits for a call it asked
     * another thread to make gets that exception from the call, and the lifecycle thread goes on once the code has
     * returned.
     *
     * @throws IllegalStateException when the calling thread runs a catch-up replay of this model or of another child of
     *                               the same dispatcher outside the subscriber's action, such as in a
     *                               {@link CatchupListener}, or holds the {@link #claim} for one of their
     *                               subscriptions outside such an action, or when code a caller supplied holds the
     *                               lock up, or an action that handed a call over doesn't return, as described
     *                               above. Nothing was changed then.
     */
    void runInTurn(Object model, String call, Runnable body) {
        ActionOnThread action = outermostActionOnTheCallingThread();
        if (action != null) {
            handOver(action, null, model, call, body);
            return;
        }
        if (replaysOrClaimsOnTheCallingThread()) {
            throw new IllegalStateException("Cannot call " + call + " from a thread on which the model is in the middle of a call for one of its subscriptions, or that runs a catch-up replay"
                    + " of this model outside the subscriber's action. Call it from another thread. " + NOTHING_CHANGED);
        }
        runAfterTheHandedCalls(call, null, body);
    }

    /**
     * Hands {@code resume} to the lifecycle thread when the calling thread runs the action of a live call or a replay of
     * these children, as {@link #runInTurn} hands a call over, and returns a handle that starts once {@code resume} has
     * run there and the subscription it returned has started. Returns {@code null} on any other thread. The handle
     * answers {@code false} when a {@code shutdown()} has begun by then, and its {@code waitUntilStarted} throws what
     * {@code resume} threw, which is also logged at {@code ERROR} naming {@code model}.
     */
    @Nullable Subscription resumeOnceTheActionHasReturned(Object model, String call, String subscriptionId, Supplier<Subscription> resume) {
        ActionOnThread action = outermostActionOnTheCallingThread();
        return action == null ? null : resumeOnceTheActionHasReturned(action, model, call, subscriptionId, false, resume);
    }

    /**
     * Hands {@code call}, a resume or a subscribe of {@code subscriptionId}, to the lifecycle thread as
     * {@link #resumeOnceTheActionHasReturned(Object, String, String, Supplier)} does, but only when the calling thread
     * runs the action of a live call or a replay of these children and that action has handed a call over already, so
     * {@code call} takes effect after that call. Returns {@code null} otherwise.
     */
    @Nullable Subscription subscriptionBehindTheCallsTheActionHandedOver(Object model, String name, String subscriptionId, Supplier<Subscription> call) {
        ActionOnThread action = actionThatHandedACallOver();
        return action == null ? null : resumeOnceTheActionHasReturned(action, model, name, subscriptionId, true, call);
    }

    /**
     * Hands {@code body}, the {@code call} of {@code model}, to the lifecycle thread as {@link #runInTurn} hands a call
     * over, but only when the calling thread runs the action of a live call or a replay of these children and that
     * action has handed a call over already, so {@code body} takes effect after that call. Answers whether it handed
     * {@code body} over, and runs nothing when it didn't.
     */
    boolean handedOverBehindTheCallsTheActionHandedOver(Object model, String subscriptionId, String call, Runnable body) {
        ActionOnThread action = actionThatHandedACallOver();
        if (action == null) {
            return false;
        }
        handOver(action, subscriptionId, model, call, body);
        return true;
    }

    /**
     * Runs {@code resume}, a {@code resumeSubscription(String)} of {@code subscriptionId}, in turn with the
     * {@link #start(boolean)}, {@link #stop()} and {@code shutdown()} calls of these children, as {@link #runInTurn}
     * runs a call from a thread that runs no action of theirs, so no such call completes between what {@code resume}
     * reads and what it does. Returns a handle whose {@code waitUntilStarted} answers {@code false} when a
     * {@code shutdown()} kept {@code resume} from running.
     * <p>
     * Unlike {@link #runInTurn}, it waits only for the actions whose handed calls concern {@code subscriptionId}, as
     * {@link HandedCallsOfAnAction} describes, before it hands {@code resume} to the lifecycle thread. An action of
     * another subscription that handed over only calls for other subscriptions and doesn't return holds up those calls,
     * which reach the lifecycle thread only once it returns, and not this resume, so a faulty subscriber keeps no other
     * subscription from resuming. One that handed over a {@code start(..)} or a {@code stop()} holds this resume up,
     * since that call concerns every subscription and the resume takes effect after it.
     * <p>
     * A thread that runs a replay of these children outside the action, or holds a {@link #claim}, runs {@code resume}
     * at once without the lock, since a {@link #stop()} that holds the lock can wait for that replay or that claim. A
     * {@link #start(boolean)} {@code resume} makes there throws as {@link #runInTurn} describes.
     */
    Subscription resumeInTurn(String subscriptionId, Supplier<Subscription> resume) {
        if (replaysOrClaimsOnTheCallingThread()) {
            return resume.get();
        }
        AtomicReference<@Nullable Subscription> resumed = new AtomicReference<>();
        runAfterTheHandedCalls("resumeSubscription(" + subscriptionId + ")", subscriptionId, () -> resumed.set(resume.get()));
        Subscription subscription = resumed.get();
        if (subscription != null) {
            return subscription;
        }
        CompletableFuture<Subscription> keptFromRunning = new CompletableFuture<>();
        keptFromRunning.cancel(false);
        return new CatchupSubscription(subscriptionId, keptFromRunning);
    }

    // A resume, whose call begins once its turn is over, or a subscribe or a positioned resume, which begins at its claim,
    // run as runPerIdCallInOrder describes
    private @Nullable Subscription resumeOnceTheActionHasReturned(ActionOnThread action, Object model, String call, String subscriptionId, boolean beginsAtClaim,
                                                                  Supplier<Subscription> resume) {
        CompletableFuture<Subscription> resumed = new CompletableFuture<>();
        Consumer<Throwable> whenThrows = e -> {
            resumed.completeExceptionally(e);
            logHandedCallThrew(model, call, e);
        };
        handToTheLifecycleThread(action, subscriptionId, () -> runPerIdCallInOrder(subscriptionId, beginsAtClaim, () -> {
            if (shuttingDown) {
                resumed.cancel(false);
            } else {
                resumed.complete(resume.get());
            }
        }, whenThrows), whenThrows);
        return new CatchupSubscription(subscriptionId, resumed);
    }

    // Runs body on the lifecycle thread once the action has returned, after the calls handed over before it, under the
    // model lock. concerning is the subscription a per-id call is made for, which runs as runPerIdCallInOrder describes
    // instead, beginning at its claim, and null for a call of the model
    private void handOver(ActionOnThread action, @Nullable String concerning, Object model, String call, Runnable body) {
        Consumer<Throwable> whenThrows = e -> logHandedCallThrew(model, call, e);
        Runnable unlessShuttingDown = () -> {
            if (!shuttingDown) {
                body.run();
            }
        };
        handToTheLifecycleThread(action, concerning, concerning == null ? () -> underTheModelLock(unlessShuttingDown)
                : () -> runPerIdCallInOrder(concerning, true, unlessShuttingDown, whenThrows), whenThrows);
    }

    /**
     * Runs {@code body}, a call for {@code subscriptionId} an action handed over, on a thread of its own and without
     * the model lock, as a call from a thread that runs no action does, so what it reads or waits for, such as its
     * checkpoint storage, holds up no call for another subscription. Called on the lifecycle thread, which waits until
     * the call has begun, so the call keeps its place among the calls handed over. A call {@code beginsAtClaim} begins
     * once it holds its {@link #claim}, and a resume once its turn is over, see {@link #resumeReadingFirst}. Every call
     * has begun once it has returned or thrown. The lifecycle thread waits up to {@link #LIFECYCLE_WAIT}, and then
     * withdraws the call and logs at {@code ERROR}. A withdrawn call throws {@link IllegalStateException} at its claim,
     * or at its turn, and changes nothing there. What {@code body} throws goes to {@code whenThrows}.
     */
    private void runPerIdCallInOrder(String subscriptionId, boolean beginsAtClaim, Runnable body, Consumer<Throwable> whenThrows) {
        awaitTheBeginOf(List.of(perIdCallOnAThreadOfItsOwn(subscriptionId, beginsAtClaim, body, whenThrows)));
    }

    // The resumes a start(true) left until after its turn, one for each of subscriptionIds, each run as
    // runPerIdCallInOrder runs a resume, all of them at once, so one that doesn't begin holds up no other. The lifecycle
    // thread waits up to LIFECYCLE_WAIT for all of them together
    private void runPerIdCallsInOrder(Set<String> subscriptionIds, Function<String, Runnable> resume, Consumer<Throwable> whenThrows) {
        List<HandedPerIdCall> calls = new ArrayList<>();
        for (String subscriptionId : subscriptionIds) {
            calls.add(perIdCallOnAThreadOfItsOwn(subscriptionId, false, resume.apply(subscriptionId), whenThrows));
        }
        awaitTheBeginOf(calls);
    }

    private HandedPerIdCall perIdCallOnAThreadOfItsOwn(String subscriptionId, boolean beginsAtClaim, Runnable body, Consumer<Throwable> whenThrows) {
        HandedPerIdCall call = new HandedPerIdCall(subscriptionId, modelLock, beginsAtClaim);
        HANDED_PER_ID_CALL_THREADS.newThread(() -> {
            HANDED_PER_ID_CALL.set(call);
            try {
                body.run();
            } catch (Throwable e) {
                // Every Throwable, since one thrown out of here reaches nobody
                whenThrows.accept(e);
            } finally {
                HANDED_PER_ID_CALL.remove();
                call.begin();
            }
        }).start();
        return call;
    }

    private void awaitTheBeginOf(List<HandedPerIdCall> calls) {
        long deadline = System.nanoTime() + LIFECYCLE_WAIT.toNanos();
        for (HandedPerIdCall call : calls) {
            if (!completesWithin(call.begun, Duration.ofNanos(deadline - System.nanoTime())) && call.withdraw()) {
                log.error("A call for subscription {} handed over from an action didn't begin within {} ms, such as while it read the position stored for the subscription, so the calls"
                          + " handed over after it go on, and it changes nothing once it gets that far.", call.subscriptionId, LIFECYCLE_WAIT.toMillis());
            }
        }
    }

    // A call for one subscription that runPerIdCallInOrder runs. Either it begins or the lifecycle thread withdraws it,
    // never both
    private static final class HandedPerIdCall {
        private static final int WAITING = 0;
        private static final int BEGUN = 1;
        private static final int WITHDRAWN = 2;
        private final String subscriptionId;
        private final ReentrantLock modelLock;
        private final boolean beginsAtClaim;
        private final AtomicInteger state = new AtomicInteger(WAITING);
        private final CompletableFuture<Void> begun = new CompletableFuture<>();

        private HandedPerIdCall(String subscriptionId, ReentrantLock modelLock, boolean beginsAtClaim) {
            this.subscriptionId = subscriptionId;
            this.modelLock = modelLock;
            this.beginsAtClaim = beginsAtClaim;
        }

        // Answers whether the call has begun, which it hasn't once it was withdrawn
        private boolean begin() {
            boolean begun = state.compareAndSet(WAITING, BEGUN) || state.get() == BEGUN;
            this.begun.complete(null);
            return begun;
        }

        private boolean withdraw() {
            return state.compareAndSet(WAITING, WITHDRAWN);
        }

        private boolean withdrawn() {
            return state.get() == WITHDRAWN;
        }

        // Whether this call may take the claim of subscriptionId, which begins one that beginsAtClaim
        private boolean mayClaim(String subscriptionId) {
            if (!this.subscriptionId.equals(subscriptionId)) {
                return true;
            }
            return beginsAtClaim ? begin() : !withdrawn();
        }
    }

    // The call runPerIdCallInOrder runs on the calling thread for this model or another child of the same dispatcher
    private @Nullable HandedPerIdCall handedPerIdCallOnTheCallingThread() {
        HandedPerIdCall call = HANDED_PER_ID_CALL.get();
        return call != null && call.modelLock == modelLock ? call : null;
    }

    // A resume run as runPerIdCallInOrder describes begins here, once its turn is over
    private void handedCallBegins(String subscriptionId) {
        HandedPerIdCall call = handedPerIdCallOnTheCallingThread();
        if (call != null && call.subscriptionId.equals(subscriptionId)) {
            call.begin();
        }
    }

    private static IllegalStateException withdrawnCall(String subscriptionId, String whatChanged) {
        return new IllegalStateException("Cannot change subscription " + subscriptionId + ", because this call, handed over from an action, didn't begin within "
                + LIFECYCLE_WAIT.toMillis() + " ms, and the calls handed over after it went on without it. " + whatChanged);
    }

    // The outermost action of these children the calling thread runs, when it has handed a call over. That call runs
    // once the action has returned, so it hasn't run yet
    private @Nullable ActionOnThread actionThatHandedACallOver() {
        ActionOnThread action = outermostActionOnTheCallingThread();
        return action != null && action.handedACallOver() ? action : null;
    }

    // Runs body under the lock on the calling thread, or on the lifecycle thread after the calls handed to it when one
    // hasn't returned, and waits for it there. A thread that holds the lock runs body at once, since the lifecycle thread
    // would wait for it to give the lock back. On the lifecycle thread body doesn't run once a shutdown() has begun, as
    // with a handed call, since a shutdown from another thread doesn't wait for the calls queued there and a start that
    // ran after it would start the live delegate it shut down. Either wait ends as heldUpByCallersCode describes, and
    // call then throws without body having run. On the calling thread body runs as unlessAHandedShutdownOvertook says
    private void runAfterTheHandedCalls(String call, @Nullable String concerning, Runnable body) {
        Runnable inTurn = unlessAHandedShutdownOvertook(body);
        if (modelLock.isHeldByCurrentThread()) {
            underTheModelLock(inTurn);
            return;
        }
        // Already in order behind the calls handed over before it, and the lifecycle thread waits for it to begin
        HandedPerIdCall handed = handedPerIdCallOnTheCallingThread();
        if (handed != null) {
            underTheModelLock(() -> {
                if (handed.withdrawn()) {
                    throw withdrawnCall(handed.subscriptionId, NOTHING_CHANGED);
                }
                if (!shuttingDown) {
                    inTurn.run();
                }
            });
            return;
        }
        if (handedCallsNotReturned.get() > 0) {
            runOnTheLifecycleThreadAndWait(call, concerning, true, body);
            return;
        }
        betweenTheHandedCallsAndTheLock.run();
        if (!lockUnlessHeldUpByCallersCode()) {
            throw callersCodeHeldUp(call);
        }
        try {
            inTurn.run();
        } finally {
            modelLock.unlock();
        }
    }

    // Body, run under the model lock only when every shutdown a held-up shutdown() handed over had returned when this was
    // asked. So a start(..) made before a handed shutdown returned changes nothing, and one made after it starts the live
    // delegate again
    private Runnable unlessAHandedShutdownOvertook(Runnable body) {
        long returnedWhenAsked = handedShutdownsReturned.get();
        return () -> {
            if (shutdownsHandedOver.get() == returnedWhenAsked) {
                body.run();
            }
        };
    }

    /**
     * Waits for the calls handed to the lifecycle thread before this call to return, so a call for
     * {@code subscriptionId} made after an action of it handed a {@link #stop()} over, and signalled the calling thread,
     * takes effect after that stop. Of the actions that handed calls over and haven't returned, it waits only for those
     * whose calls concern {@code subscriptionId}, as {@link HandedCallsOfAnAction} describes, so an action of another
     * subscription that handed over only calls for other subscriptions doesn't hold it up. With {@code subscriptionId} null it waits for every one of
     * them, as a call of the model does. It returns at once when no handed call is pending, when the calling thread holds the model lock, since the lifecycle
     * thread would wait for it, and when the calling thread runs the action of a replay or a live call of these
     * children, since a call handed over from that action waits for it. It also returns at once on a thread that runs a
     * replay of these children outside the action, such as in a {@link CatchupListener}, or holds a {@link #claim},
     * since a handed {@link #stop()} parks the replays under their claims and handover locks, and such a thread can
     * hold one of them, as a replay run again does while it tells its listener.
     *
     * @throws IllegalStateException when code the caller supplied, such as a {@code StartAt.dynamic} function, runs
     *                               under the model lock for {@link #LIFECYCLE_WAIT} while this waits, or an action that
     *                               handed a call concerning {@code subscriptionId} over doesn't return within it, since
     *                               that code or that action can be waiting for {@code call}. Nothing was changed then.
     */
    void awaitTheHandedCalls(@Nullable String subscriptionId, String call) {
        if (handedCallsNotReturned.get() == 0 || modelLock.isHeldByCurrentThread() || outermostActionOnTheCallingThread() != null
                || replaysOrClaimsOnTheCallingThread() || handedPerIdCallOnTheCallingThread() != null) {
            return;
        }
        runOnTheLifecycleThreadAndWait(call, subscriptionId, false, () -> {
        });
    }

    // The lifecycle thread runs what it is handed in the order it came, and body reaches it only once every action that
    // had handed a call concerning it over when this was called has returned, so body runs after every such call handed
    // over before it. concerning is the subscription a per-id call is made for, and null for a call of the model, which
    // waits for every such action. The wait for those actions gives up as actionsHeldUp describes, and until body
    // begins, also while the lifecycle thread waits for the lock for it, the calling thread withdraws it once code a
    // caller supplied holds it up as heldUpByCallersCode describes. Body never runs then
    private void runOnTheLifecycleThreadAndWait(String call, @Nullable String concerning, boolean underTheLock, Runnable body) {
        long waitingSince = System.nanoTime();
        if (!theActionsHoldingHandedCallsReturnWithin(concerning, LIFECYCLE_WAIT)) {
            throw actionsHeldUp(call);
        }
        WaitedCall waited = new WaitedCall();
        handToTheLifecycleThread(null, null, () -> {
            if (underTheLock) {
                runUnderTheLockUnlessWithdrawn(waited, body);
            } else if (waited.begin()) {
                body.run();
            }
            waited.ran.complete(null);
        }, waited.ran::completeExceptionally);
        while (!completesWithin(waited.ran, CALLERS_CODE_POLL)) {
            if (heldUpByCallersCode(waitingSince) && waited.withdraw()) {
                throw callersCodeHeldUp(call);
            }
        }
        try {
            waited.ran.join();
        } catch (CompletionException e) {
            throw thrownAsItIs(e.getCause() == null ? e : e.getCause());
        }
    }

    // Whether every action of these children that had handed a call concerning it over, as HandedCallsOfAnAction
    // describes, and not returned when this was called returns within timeout. An action that returns gives the lifecycle thread the calls it handed over before this sees it
    // return, so a call this thread then gives the lifecycle thread runs behind them
    private boolean theActionsHoldingHandedCallsReturnWithin(@Nullable String concerning, Duration timeout) {
        CompletableFuture<?>[] holding = actionsHoldingHandedCalls.stream().filter(held -> held.holdUp(concerning)).map(held -> held.returned).toArray(CompletableFuture<?>[]::new);
        return holding.length == 0 || completesWithin(CompletableFuture.allOf(holding), timeout);
    }

    // An action is code the caller supplied too, and one that handed a call over can be waiting for the call that waits
    // for it here, so that call gives up once it has waited LIFECYCLE_WAIT for such an action. Only a call of the model
    // or a call for a subscription those handed calls concern waits for it, so an action that handed over only calls for
    // its own subscription and never returns holds up no call for another subscription
    private IllegalStateException actionsHeldUp(String call) {
        log.error("{} waited {} ms for the action of a live call or a replay that handed a lifecycle call over to return, and gives up. {}",
                call, LIFECYCLE_WAIT.toMillis(), NOTHING_CHANGED);
        return new IllegalStateException("Cannot run " + call + ", because the action of a live call or a replay that handed a lifecycle call over before this call was made didn't return within "
                + LIFECYCLE_WAIT.toMillis() + " ms, and this call takes effect after the calls handed over. That action may be waiting for this call. Don't wait inside an action for a"
                + " lifecycle call made on another thread. " + NOTHING_CHANGED);
    }

    // Polls for the lock rather than waiting on it, so a call its caller withdrew meanwhile doesn't keep the lifecycle
    // thread waiting for the code that held it up
    private void runUnderTheLockUnlessWithdrawn(WaitedCall waited, Runnable body) {
        while (!tryLockWithin(modelLock, CALLERS_CODE_POLL)) {
            if (waited.withdrawn()) {
                return;
            }
        }
        try {
            if (waited.begin() && !shuttingDown) {
                body.run();
            }
        } finally {
            modelLock.unlock();
        }
    }

    // A call a thread hands to the lifecycle thread and waits for. Either the lifecycle thread begins it or the waiting
    // thread withdraws it, never both
    private static final class WaitedCall {
        private static final int WAITING = 0;
        private static final int BEGUN = 1;
        private static final int WITHDRAWN = 2;
        private final AtomicInteger state = new AtomicInteger(WAITING);
        private final CompletableFuture<Void> ran = new CompletableFuture<>();

        private boolean begin() {
            return state.compareAndSet(WAITING, BEGUN);
        }

        private boolean withdraw() {
            return state.compareAndSet(WAITING, WITHDRAWN);
        }

        private boolean withdrawn() {
            return state.get() == WITHDRAWN;
        }
    }

    private void underTheModelLock(Runnable body) {
        modelLock.lock();
        try {
            body.run();
        } finally {
            modelLock.unlock();
        }
    }

    // Takes the model lock unless code a caller supplied holds it up as heldUpByCallersCode describes, and answers
    // whether it took it. An interrupt doesn't end the wait, and is set again once the wait is over
    private boolean lockUnlessHeldUpByCallersCode() {
        long waitingSince = System.nanoTime();
        while (!tryLockWithin(modelLock, CALLERS_CODE_POLL)) {
            if (heldUpByCallersCode(waitingSince)) {
                return false;
            }
        }
        return true;
    }

    // Whether the thread holding the model lock has been in code a caller supplied for LIFECYCLE_WAIT, counted from
    // waitingSince or from when that code began, whichever came later. That code can be waiting for the call that waits
    // here, so the call gives up rather than wait for good. A wait behind anything else has no deadline
    private boolean heldUpByCallersCode(long waitingSince) {
        long since = callersCodeUnderTheLockSince.get();
        return since != 0 && heldUpSince(since, waitingSince);
    }

    // Whether LIFECYCLE_WAIT has passed since code a caller supplied began to run at since, or since the wait began at
    // waitingSince, whichever came later
    private static boolean heldUpSince(long since, long waitingSince) {
        long from = since - waitingSince > 0 ? since : waitingSince;
        return System.nanoTime() - from >= LIFECYCLE_WAIT.toNanos();
    }

    private IllegalStateException callersCodeHeldUp(String call) {
        return callersCodeHeldUp(call, NOTHING_CHANGED);
    }

    private IllegalStateException callersCodeHeldUp(String call, String whatChanged) {
        log.error("{} waited {} ms for the model lock while code the caller supplied, such as a StartAt.dynamic function, ran under it, and gives up. {}",
                call, LIFECYCLE_WAIT.toMillis(), whatChanged);
        return new IllegalStateException("Cannot run " + call + ", because code the caller supplied, such as a StartAt.dynamic function or a CatchupListener, ran under the model lock for "
                + LIFECYCLE_WAIT.toMillis() + " ms while this call waited for the lock, and that code may be waiting for this call. Don't wait inside such code for a lifecycle call"
                + " made on another thread. " + whatChanged);
    }

    /**
     * Runs {@code code}, which a caller supplied, such as a {@link CatchupListener}. On a thread that holds the model
     * lock, as a subscribe, a resume or a {@link #start(boolean)} does on the lifecycle thread or in turn, a call from
     * another thread that waits for the lock or for the lifecycle thread meanwhile gives up after {@link #LIFECYCLE_WAIT}
     * and throws {@link IllegalStateException}, since {@code code} can be waiting for that call. The call changes
     * nothing then, apart from a {@code shutdown()}, which has ended the catch-up replays before it waited, as
     * {@link #shutDownInTurn} describes. Nested calls count from the outermost one.
     * <p>
     * On a thread that holds the handover lock of a subscription, as the replay thread does when it tells the
     * listener that a replay runs again, a lifecycle call for that subscription that waits for the lock meanwhile
     * gives up the same way, as {@link #lockHandover} describes. A {@link #stop()} that gives up so has stopped the
     * model, and the replay parks itself once {@code code} returns.
     */
    private void runCallersCode(Runnable code) {
        askCallersCode(() -> {
            code.run();
            return null;
        });
    }

    // As runCallersCode, for code that answers
    private <T extends @Nullable Object> T askCallersCode(Supplier<T> code) {
        long now = System.nanoTime();
        long since = now == 0 ? 1 : now;
        boolean markedTheModelLock = modelLock.isHeldByCurrentThread() && callersCodeUnderTheLockSince.get() == 0;
        if (markedTheModelLock) {
            callersCodeUnderTheLockSince.set(since);
        }
        // Each id whose handover lock this thread holds, and which no outer call marked yet
        List<String> markedHandoverLocks = new ArrayList<>();
        handoverLocks.forEach((subscriptionId, counted) -> {
            if (counted.lock.isHeldByCurrentThread() && callersCodeUnderTheHandoverLockSince.putIfAbsent(subscriptionId, since) == null) {
                markedHandoverLocks.add(subscriptionId);
            }
        });
        try {
            return code.get();
        } finally {
            markedHandoverLocks.forEach(callersCodeUnderTheHandoverLockSince::remove);
            if (markedTheModelLock) {
                callersCodeUnderTheLockSince.set(0);
            }
        }
    }

    // startAt, with its function asked through askCallersCode when it is dynamic, so every ask of it, by this model or by
    // the live delegate, counts as code the caller supplied
    private StartAt askedAsCallersCode(StartAt startAt) {
        return startAt.isDynamic() ? StartAt.dynamic(context -> askCallersCode(() -> startAt.get(context))) : startAt;
    }

    /**
     * The shutdowns of {@code model} that have begun so far. A {@code start(..)} of {@code model} reads it first, under
     * the model lock, and hands it to {@link #startTheLiveDelegate}.
     */
    long shutdownsBegun(Object model) {
        LiveDelegateCalls calls = liveDelegateCalls.computeIfAbsent(model, ignored -> new LiveDelegateCalls());
        synchronized (calls) {
            return calls.shutdownsBegun;
        }
    }

    /**
     * The last call that stopped or started these children, their replays or their dispatcher, numbered in the order
     * such calls came, and whether it left the replays and the live delegate stopped. A {@code stop()} leaves both
     * stopped. A {@code start(..)} leaves neither stopped, and neither does a resume of the live delegate that starts a
     * model a failed start stopped again. A {@link #resumeReplay()} leaves the replays running and doesn't change what
     * the call before left the live delegate. A {@code start(..)} that throws and stops the model again, as
     * {@link #undoStart} describes, turns its own call into one that leaves both stopped. A {@link #stopReplay()} isn't
     * recorded, since a start goes on past one made from inside it, as {@link #start(boolean)} describes.
     * <p>
     * Every {@code start(..)}, {@code stop()} and {@link #resumeReplay()} runs under the model lock, so while a start
     * holds it, such a call numbered after the start was made on the start's own thread from code the start ran, such
     * as a {@code StartAt.dynamic} function or a method of the live delegate.
     */
    record LifecycleCall(long number, boolean replaysStopped, boolean liveDelegateStopped) {
        private LifecycleCall next(boolean replaysStopped, boolean liveDelegateStopped) {
            return new LifecycleCall(number + 1, replaysStopped, liveDelegateStopped);
        }
    }

    /**
     * Records that a {@code stop()} of these children or of their dispatcher runs.
     */
    void stopBegins() {
        lastLifecycleCall.updateAndGet(last -> last.next(true, true));
    }

    /**
     * Records that a {@code start(..)} of these children or of their dispatcher begins, and returns the call to hand to
     * {@link #beginStart(LifecycleCall)} of each child it starts.
     */
    LifecycleCall startBegins() {
        return lastLifecycleCall.updateAndGet(last -> last.next(false, false));
    }


    /**
     * The last call that stopped or started these children, their replays or their dispatcher since {@code start}
     * began, read under the model lock {@code start} holds, or {@code start} itself when none came since. A start made
     * from inside {@code start} that threw and stopped the model again counts as a call that left both stopped, as
     * {@link LifecycleCall} describes, but {@code start}'s own failure doesn't, and neither does a stop that came before
     * {@code start}.
     */
    LifecycleCall lastCallSince(LifecycleCall start) {
        LifecycleCall last = lastLifecycleCall.get();
        return last.number() > start.number() ? last : start;
    }

    /**
     * Runs {@code start}, the rest of a {@code start(..)} of {@code model} from the live delegate's {@code start(..)} on,
     * under the model lock, so that a {@code stop()} made from inside the start takes effect as if it came after the
     * start, as {@link #lastCallSince} tells for {@code call}, the start's own call.
     * <p>
     * When the last such call before this left the live delegate stopped, {@code start} doesn't run. When the last
     * such call left it stopped once {@code start} has returned or thrown, the live delegate's {@code start(..)} or a
     * resume may have gone on after that call, so this runs {@code stop}, the body of {@code model}'s {@code stop()}, again before it
     * returns or rethrows, or only {@code stopTheLiveDelegate} when a {@link #resumeReplay()} came after that stop. So
     * this stops nothing when a start made from inside since started the model again, and does stop when one made there
     * threw and stopped the model again. A {@code shutdown()} under way, or begun since the start read
     * {@code shutdownsBegunBefore}, ends the live delegate itself, and nothing is stopped again then. One that returned
     * before the start began doesn't count. What that stop throws is logged at {@code ERROR} naming {@code model}, and
     * the start's own outcome stands.
     */
    void startUnlessStoppedFromInside(Object model, LifecycleCall call, long shutdownsBegunBefore, Runnable stop, Runnable stopTheLiveDelegate, Runnable start) {
        if (lastCallSince(call).liveDelegateStopped()) {
            return;
        }
        try {
            start.run();
        } finally {
            LifecycleCall last = lastCallSince(call);
            if (last.liveDelegateStopped() && !shutDownSince(model, shutdownsBegunBefore)) {
                try {
                    (last.replaysStopped() ? stop : stopTheLiveDelegate).run();
                } catch (Throwable e) {
                    // Every Throwable, since the stop it belongs to has returned, and the start's own outcome stands
                    log.error("stop() of {}, made from inside a start(..) of it and run again once that start had gone on past it, threw", model, e);
                }
            }
        }
    }

    /**
     * Whether a {@code shutdown()} of {@code model} is under way, or one has begun since a {@code start(..)} of it read
     * {@code shutdownsBegunBefore} from {@link #shutdownsBegun}. A shutdown that returned before that start began
     * doesn't count, so a start made after one starts the model as any other start does.
     */
    boolean shutDownSince(Object model, long shutdownsBegunBefore) {
        LiveDelegateCalls calls = liveDelegateCalls.computeIfAbsent(model, ignored -> new LiveDelegateCalls());
        synchronized (calls) {
            return calls.shutdowns > 0 || calls.shutdownsBegun != shutdownsBegunBefore;
        }
    }

    /**
     * Calls {@code start}, which starts the live delegate for a {@code start(..)} of {@code model}, unless a
     * {@code shutdown()} of {@code model} is under way, or one has begun since that start read
     * {@code shutdownsBegunBefore} from {@link #shutdownsBegun}. A shutdown under way has forgotten the catch-ups and
     * shuts the live delegate down once this start has returned, so not starting it ends in the same state. One that
     * began since and is no longer under way was made on this thread, from code the start ran before this, such as a
     * {@code StartAt.dynamic} function, since every other shutdown waits for the model lock the start holds. It has shut
     * the live delegate down, and starting it now would leave it running once the start has returned. A shutdown that
     * comes while {@code start} runs shuts the live delegate down without waiting for it, as {@link #shutDownInTurn}
     * describes, since a live delegate can hold its start until it is shut down. The next {@code shutdown()} of
     * {@code model} shuts the live delegate down again, also after a start that threw and may have left it running.
     * <p>
     * A shutdown made on this thread from inside {@code start}, as by a live delegate whose start calls back into code
     * that shuts {@code model} down, can't wait for this start, which may still start the live delegate after it. So once
     * {@code start} has returned or thrown, this shuts the live delegate down for it under the model lock, before it
     * returns or rethrows, and logs an {@code ERROR} naming {@code model} for what that shutdown throws, leaving the live
     * delegate to the next {@code shutdown()}.
     */
    void startTheLiveDelegate(Object model, long shutdownsBegunBefore, Runnable start) {
        LiveDelegateCalls calls = liveDelegateCalls.computeIfAbsent(model, ignored -> new LiveDelegateCalls());
        synchronized (calls) {
            if (shutDownSince(model, shutdownsBegunBefore)) {
                return;
            }
            calls.starts++;
            calls.shutDown = false;
        }
        try {
            start.run();
        } finally {
            int leftToThisStart;
            Runnable shutDownTheLiveDelegate;
            synchronized (calls) {
                calls.starts--;
                leftToThisStart = calls.starts == 0 ? calls.shutdownsLeftToTheStart : 0;
                shutDownTheLiveDelegate = calls.shutDownLeftToTheStart;
                if (leftToThisStart > 0) {
                    calls.shutdownsLeftToTheStart = 0;
                    calls.shutDownLeftToTheStart = null;
                }
            }
            if (leftToThisStart > 0 && shutDownTheLiveDelegate != null) {
                shutDownForTheShutdownsLeftToTheStart(model, calls, leftToThisStart, shutDownTheLiveDelegate);
            }
        }
    }

    // Under the model lock the start holds. The shutdowns stay counted until the live delegate's shutdown() has run, so
    // nothing in between starts the live delegate
    private void shutDownForTheShutdownsLeftToTheStart(Object model, LiveDelegateCalls calls, int shutdowns, Runnable shutDownTheLiveDelegate) {
        try {
            underTheModelLock(() -> shutDownUnlessShutDown(calls, shutDownTheLiveDelegate));
        } catch (Throwable e) {
            // Every Throwable, since the shutdown it belongs to has returned, and the start's own outcome stands
            log.error("shutdown() of {}, made from inside a start(..) of it and run once that start had returned from the live delegate, threw", model, e);
        } finally {
            synchronized (calls) {
                calls.shutdowns -= shutdowns;
            }
        }
    }

    /**
     * Runs a {@code shutdown()} of {@code model}. {@code forgetTheCatchups} forgets what the catch-ups hold and flags
     * the model as shutting down, and {@code shutDownTheLiveDelegate} shuts the live delegate down.
     * <p>
     * {@code forgetTheCatchups} runs at once on the calling thread, without the model lock, so a {@code start(..)} under
     * way resumes nothing once it finds the model flagged after the live delegate's start, and a call handed to the
     * lifecycle thread that hasn't run yet doesn't run. When a {@code start(..)} of {@code model} is in the live
     * delegate's {@code start(..)} at that moment, {@code shutDownTheLiveDelegate} runs at once too, without waiting for
     * the start, since a live delegate can hold its start until it is shut down. Then {@code shutDownTheLiveDelegate}
     * runs under the model lock, so it runs in turn with the {@link #start(boolean)}, {@link #stop()} and
     * {@code shutdown()} calls of this model and of every other child of the same dispatcher, and once a start that was
     * under way has returned. A call from another thread doesn't wait for the calls queued on the lifecycle thread,
     * handed over by an action or made from another thread behind one, which run nothing once the model is flagged.
     * While code a caller supplied holds the lock up, as {@link #runInTurn} describes, it waits for the lock up to
     * {@link #LIFECYCLE_WAIT} that way, since that code may be waiting for this shutdown, and then throws
     * {@link IllegalStateException}. By then {@code forgetTheCatchups} has run, and {@code shutDownTheLiveDelegate} has
     * run without the lock only when a start was in the live delegate's {@code start(..)}, which the exception says.
     * Before it throws, it starts a thread of its own that runs a {@code shutDownTheLiveDelegate} under the model lock.
     * That thread waits for the lock with no deadline and for nothing else, so it runs once that code has returned, with
     * no further call, also while a call queued on the lifecycle thread waits for the lock that code holds.
     * So when nobody calls {@code shutdown()} again, as when a closing Spring context made it, the live delegate is still
     * shut down. When that code never returns, the handed one waits for good. A second {@code shutdown()} held up the
     * same way before it runs hands over no second one. A {@code shutdown()} that gets the lock first shuts the live
     * delegate down itself, and the handed one then doesn't call it again. One that gets the lock after the handed one
     * returns without calling it again. A {@code start(..)}, {@code stop()} or {@code resumeSubscription(id)} made before
     * the handed one has returned changes nothing, since a {@code shutdown()} has begun. This is so on the thread that
     * holds the lock, and on another thread whether the call runs on the lifecycle thread after the calls queued there
     * or, with none queued when it was made, takes the lock once the handed one has run. A {@code start(..)} made once
     * the handed one has returned starts the live delegate again.
     * <p>
     * While the thread that holds the lock has waited {@link #CALLERS_CODE_POLL} or more for one subscription, such as a
     * {@link #stop()} waiting for the handover lock of a replay whose checkpoint storage doesn't answer, or a resume inside
     * a live delegate that reads in its {@code resumeSubscription(String)}, this runs {@code shutDownTheLiveDelegate}
     * once without the lock, and goes on waiting for the lock as above.
     * <p>
     * Called from inside the action of a replay or of a live call, or on a thread that holds a {@link #claim}, the
     * shutdown doesn't wait. This returns once {@code forgetTheCatchups} has run, and the lifecycle thread the children
     * share shuts the live delegate down as above, once the action has returned, and logs an {@code ERROR} naming
     * {@code model} for what it throws.
     * <p>
     * Called on the thread of a {@code start(..)} of {@code model}, from code that start runs, it takes the lock the start
     * holds at once, so it can't wait for that start. Made from inside the live delegate's {@code start(..)}, it lets
     * the start make the shutdown under the lock once the live delegate's {@code start(..)} has returned or thrown, and
     * it counts as under way until then, as {@link #startTheLiveDelegate} describes.
     * <p>
     * Once the live delegate's {@code shutdown()} has returned normally under the lock, a later {@code shutdown()} of
     * {@code model} doesn't call it there again, until a {@code start(..)} of {@code model} has started the live delegate
     * again. One that threw left the live delegate running, so the next {@code shutdown()} calls it again.
     */
    void shutDownInTurn(Object model, Runnable forgetTheCatchups, Runnable shutDownTheLiveDelegate) {
        forgetTheCatchups.run();
        ActionOnThread action = outermostActionOnTheCallingThread();
        if (action == null && !replaysOrClaimsOnTheCallingThread()) {
            shutDownTheLiveDelegateInTurn(model, shutDownTheLiveDelegate, true);
            return;
        }
        handToTheLifecycleThread(action, null, () -> shutDownTheLiveDelegateInTurn(model, shutDownTheLiveDelegate, false),
                e -> logHandedCallThrew(model, "shutdown()", e));
    }

    // A start that registers in the monitor before this does is ended without the lock, and one that registers after
    // this sees the shutdown under way and doesn't start the live delegate, so neither leaves this waiting for good. On a
    // thread other than the lifecycle thread, the wait for the lock ends as heldUpByCallersCode describes, and this then
    // hands the rest of the shutdown to a thread of its own and throws, since that code can be waiting for this shutdown
    // and no thread calls the live delegate for it without the lock unless a start is in the live delegate's start(..)
    private void shutDownTheLiveDelegateInTurn(Object model, Runnable shutDownTheLiveDelegate, boolean givesUpBehindCallersCode) {
        LiveDelegateCalls calls = liveDelegateCalls.computeIfAbsent(model, ignored -> new LiveDelegateCalls());
        boolean aStartIsInTheLiveDelegate;
        synchronized (calls) {
            calls.shutdowns++;
            calls.shutdownsBegun++;
            aStartIsInTheLiveDelegate = calls.starts > 0;
        }
        boolean handedOver = false;
        try {
            boolean shutDownWithoutTheLock = aStartIsInTheLiveDelegate;
            if (aStartIsInTheLiveDelegate) {
                shutDownTheLiveDelegate.run();
            }
            long waitingSince = System.nanoTime();
            while (!tryLockWithin(modelLock, CALLERS_CODE_POLL)) {
                // The thread holding the lock has waited a poll or more for a call for one subscription, which can wait for
                // good on a checkpoint storage, so the live delegate is shut down without waiting for it, as for a start in
                // its start(..)
                long perIdWaitSince = perIdWaitUnderTheLockSince.get();
                if (!shutDownWithoutTheLock && perIdWaitSince != 0 && System.nanoTime() - perIdWaitSince >= CALLERS_CODE_POLL.toNanos()) {
                    shutDownWithoutTheLock = true;
                    shutDownTheLiveDelegate.run();
                }
                if (givesUpBehindCallersCode && heldUpByCallersCode(waitingSince)) {
                    handedOver = finishOnAThreadOfItsOwn(model, calls, shutDownTheLiveDelegate);
                    throw callersCodeHeldUp("shutdown()", aStartIsInTheLiveDelegate ? SHUT_DOWN_TO_END_A_START : shutDownWithoutTheLock ? SHUT_DOWN_PAST_A_WAIT : NOT_SHUT_DOWN);
                }
            }
            try {
                handedOver = leaveToTheStartOnThisThread(calls, shutDownTheLiveDelegate);
                if (!handedOver) {
                    shutDownUnlessShutDown(calls, shutDownTheLiveDelegate);
                }
            } finally {
                modelLock.unlock();
            }
        } finally {
            if (!handedOver) {
                synchronized (calls) {
                    calls.shutdowns--;
                }
            }
        }
    }

    // Under the model lock. Every start of the live delegate runs under it, so a start of model in the live delegate's
    // start(..) now is one this thread runs and this shutdown was made from inside. That start can start the live delegate
    // again after this returns, so the last shutdown of the live delegate is left to it, as startTheLiveDelegate
    // describes, and this shutdown counts in calls.shutdowns until then. Answers whether it was left to the start
    private static boolean leaveToTheStartOnThisThread(LiveDelegateCalls calls, Runnable shutDownTheLiveDelegate) {
        synchronized (calls) {
            if (calls.starts == 0) {
                return false;
            }
            calls.shutdownsLeftToTheStart++;
            calls.shutDownLeftToTheStart = shutDownTheLiveDelegate;
            return true;
        }
    }

    // Hands a thread of its own a shutdown of the live delegate under the model lock, with no deadline on the wait for
    // the lock, unless an earlier held-up shutdown handed one over that hasn't run yet. Answers whether it handed this
    // one over, which then counts in calls.shutdowns until it has run, so a start in between doesn't start the live
    // delegate, and in handedCallsNotReturned, so a call from another thread made meanwhile runs behind it on the
    // lifecycle thread and changes nothing, and in shutdownsHandedOver, so one that found nothing pending changes
    // nothing either. Not the lifecycle thread itself, since a call queued there ahead of it can wait for the model lock
    // that the code holding this shutdown up holds. So it waits for the model lock and nothing else
    private boolean finishOnAThreadOfItsOwn(Object model, LiveDelegateCalls calls, Runnable shutDownTheLiveDelegate) {
        synchronized (calls) {
            if (calls.shutdownHandedOver) {
                return false;
            }
            calls.shutdownHandedOver = true;
        }
        shutdownsHandedOver.incrementAndGet();
        handedCallsNotReturned.incrementAndGet();
        try {
            HANDED_SHUTDOWN_THREADS.newThread(() -> {
                try {
                    underTheModelLock(() -> shutDownUnlessShutDown(calls, shutDownTheLiveDelegate));
                } catch (Throwable e) {
                    // Every Throwable, since one thrown out of here reaches nobody
                    logHandedCallThrew(model, "shutdown()", e);
                } finally {
                    synchronized (calls) {
                        calls.shutdownHandedOver = false;
                        calls.shutdowns--;
                    }
                    handedShutdownsReturned.incrementAndGet();
                    handedCallsNotReturned.decrementAndGet();
                }
            }).start();
            return true;
        } catch (Throwable e) {
            synchronized (calls) {
                calls.shutdownHandedOver = false;
            }
            handedShutdownsReturned.incrementAndGet();
            handedCallsNotReturned.decrementAndGet();
            throw e;
        }
    }

    // Under the model lock. Once the live delegate's shutdown() has returned normally, calls.shutDown keeps a later
    // shutdown from calling it again until a start has started the live delegate again
    private static void shutDownUnlessShutDown(LiveDelegateCalls calls, Runnable shutDownTheLiveDelegate) {
        if (!calls.shutDown) {
            shutDownTheLiveDelegate.run();
            calls.shutDown = true;
        }
    }

    // Body reaches the lifecycle thread once the action that handed it over has returned, after the calls that action
    // handed over before it, and at once when no action handed it over. So the lifecycle thread never waits for an
    // action, and an action that doesn't return holds up only the calls handed over from it, not those of another
    // subscription's action. Body takes the lock itself, so a call the lifecycle thread runs never waits for the thread
    // that handed it over. No thread that holds the lock waits for the lifecycle thread. concerning is the subscription
    // a per-id call is made for, and null for a call of the model
    private void handToTheLifecycleThread(@Nullable ActionOnThread action, @Nullable String concerning, Runnable body, Consumer<Throwable> whenBodyThrows) {
        handedCallsNotReturned.incrementAndGet();
        Runnable call = () -> {
            try {
                body.run();
            } catch (Throwable e) {
                // Every Throwable, since one thrown out of here reaches nobody, and a caller waiting for body would wait for good
                whenBodyThrows.accept(e);
            } finally {
                handedCallsNotReturned.decrementAndGet();
            }
        };
        if (action == null) {
            lifecycleThread.execute(call);
        } else {
            action.handOver(call, concerning);
        }
    }

    private void logHandedCallThrew(Object model, String call, Throwable e) {
        log.error("{} of {}, run on another thread once the call that made it no longer held it up, threw", call, model, e);
    }

    // The outermost action of this model, or of another child of the same dispatcher, that the calling thread runs, so a
    // call handed to the lifecycle thread waits for every action of theirs on the thread to return
    private @Nullable ActionOnThread outermostActionOnTheCallingThread() {
        ActionOnThread outermost = null;
        for (ActionOnThread call = IN_AN_ACTION.get(); call != null; call = call.outer) {
            if (call.modelLock == modelLock) {
                outermost = call;
            }
        }
        return outermost;
    }

    // An action of a live call or a replay of subscriptionId that runs on the thread, linked to the actions it runs
    // inside. Only that thread reads or writes it
    private static final class ActionOnThread {
        private final String subscriptionId;
        private final ReentrantLock modelLock;
        private final Executor lifecycleThread;
        private final Set<HandedCallsOfAnAction> actionsHoldingHandedCalls;
        private final @Nullable ActionOnThread outer;
        // The calls handed over from inside the action, in the order it made them, which reach the lifecycle thread once
        // the action returns. Made by the first of them
        private @Nullable List<Runnable> handedOver;
        // Made with handedOver, kept in actionsHoldingHandedCalls until the action returns
        private @Nullable HandedCallsOfAnAction held;

        private ActionOnThread(String subscriptionId, ReentrantLock modelLock, Executor lifecycleThread, Set<HandedCallsOfAnAction> actionsHoldingHandedCalls,
                               @Nullable ActionOnThread outer) {
            this.subscriptionId = subscriptionId;
            this.modelLock = modelLock;
            this.lifecycleThread = lifecycleThread;
            this.actionsHoldingHandedCalls = actionsHoldingHandedCalls;
            this.outer = outer;
        }

        private boolean handedACallOver() {
            return handedOver != null;
        }

        // Called on the outermost action of these children on the thread. The call concerns the subscription of every
        // action of theirs the thread runs, since it was made in the code of one of them, and concerning, the
        // subscription a per-id call is made for. A call of the model, with no concerning, concerns every subscription.
        // The concerns are recorded before a thread can find held
        private void handOver(Runnable call, @Nullable String concerning) {
            HandedCallsOfAnAction holding = held == null ? new HandedCallsOfAnAction() : held;
            for (ActionOnThread action = IN_AN_ACTION.get(); action != null; action = action.outer) {
                if (action.modelLock == modelLock) {
                    holding.concerns.add(action.subscriptionId);
                }
            }
            if (concerning != null) {
                holding.concerns.add(concerning);
            } else {
                holding.everySubscription = true;
            }
            if (handedOver == null || held == null) {
                handedOver = new ArrayList<>();
                held = holding;
                actionsHoldingHandedCalls.add(holding);
            }
            handedOver.add(call);
        }

        // The lifecycle thread gets the calls before returned completes, so a call from another thread that waited for
        // returned queues behind them
        private void actionReturned() {
            if (handedOver == null || held == null) {
                return;
            }
            try {
                handedOver.forEach(lifecycleThread::execute);
            } finally {
                actionsHoldingHandedCalls.remove(held);
                held.returned.complete(null);
            }
        }
    }

    // The calls an action that hasn't returned yet handed over, as the subscriptions they concern and a future completed
    // once they have reached the lifecycle thread. A per-id call for a subscription from another thread waits only for
    // the actions whose calls concern that subscription, so an action that handed over only per-id calls of its own and
    // doesn't return holds up the calls of no other subscription. A call of the model waits for every one of them, and a
    // call of the model handed over, such as a stop, concerns every subscription, so a later resume of any of them
    // still takes effect after it
    private static final class HandedCallsOfAnAction {
        private final Set<String> concerns = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<@Nullable Void> returned = new CompletableFuture<>();
        private volatile boolean everySubscription;

        // Whether a call for subscriptionId from another thread, or a call of the model when it is null, waits for these
        private boolean holdUp(@Nullable String subscriptionId) {
            return subscriptionId == null || everySubscription || concerns.contains(subscriptionId);
        }
    }

    // A replay thread of a child of another dispatcher may call, since nothing that call waits for runs on that thread
    private boolean replaysOrClaimsOnTheCallingThread() {
        CatchupAttempt calling = CURRENT_ATTEMPT.get();
        return (calling != null && calling.owner.modelLock == modelLock)
                || lifecycleClaims.values().stream().anyMatch(claim -> claim.lock.isHeldByCurrentThread());
    }

    /**
     * The call of a start, whether it found this model stopped, and where the lifecycle and the resumes of the live
     * delegate stood once it allowed replays to run again.
     */
    record StartAttempt(LifecycleCall call, boolean wasStopped, long generation, long liveDelegateResumes) {
    }

    /**
     * Allows the next replay on this model to run, as {@link #resumeReplay()} does, before the live delegate is
     * started for {@code call}, which {@link #startBegins()} returned. Hand the result to
     * {@link #undoStart(StartAttempt, boolean, Throwable)} when that start throws.
     */
    StartAttempt beginStart(LifecycleCall call) {
        synchronized (lifecycleLock) {
            StartAttempt attempt = new StartAttempt(call, stopped, ++lifecycleGeneration, liveDelegateResumes);
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
     * <p>
     * It parks the replays in flight as {@link #stopReplay()} does, and tells each call to the subscriber's action that
     * one of them let through that its event is no longer wanted, without waiting for the action to return.
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
            stops++;
            // Turns this start's own call into a stop, so an outer start that ran this start reads it as a stop made
            // from inside. No later call is overwritten, since each moves the generation checked above. Another child of
            // a dispatcher may have turned it first
            lastLifecycleCall.compareAndSet(attempt.call(), new LifecycleCall(attempt.call().number(), true, true));
        }
        List<ActionInFlight> admitted = new ArrayList<>();
        try {
            parkReplaysInFlight(admitted);
        } catch (RuntimeException | Error e) {
            addSuppressed(startFailure, e);
        } finally {
            // A start that failed takes nothing back that a caller asked for, so it doesn't wait for the actions. It still
            // tells each one its event is unwanted, as a stop does, since an action waiting for a lease waits for nothing else
            admitted.forEach(action -> action.unwanted().complete(null));
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
                startBegins();
            }
        }
    }

    /**
     * Delete {@code subscriptionId}'s position from the configured position storage, if any. Exposed so the
     * dispatcher can delete it exactly once when cancelling a subscription that could belong to either mode, since
     * the position storage config (and the storage instance it wraps) is shared, not owned per mode.
     */
    public void deletePositionFromStorage(String subscriptionId) {
        try {
            doIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().delete(subscriptionId));
        } finally {
            positionMayHaveMoved(subscriptionId);
        }
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
     * <p>
     * A subscribe of this model starts its replay through this method. Called on the subscribing thread, the replay
     * hands the subscription to the live delegate behind the same check as {@link #subscribeInTheWrappedModel}, so a
     * call the live delegate makes once a cancel has forgotten the subscription throws {@link AttemptNotMade}. An
     * override that calls this method on another thread starts the replay without that check, and the live delegate
     * then calls the action also after a cancel, as in 0.34.0.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused) {
        return startCatchupAsync(subscriptionId, catchup, holdPaused, false, CompletableFuture.completedFuture(null), null, recordOfThisSubscribe(subscriptionId));
    }

    // As startCatchupAsync(String, CatchupReplay, boolean), for the replay of the subscription whose record is made, so
    // the live delegate gets the action through the same gate as subscribeInTheWrappedModel gives it. The replay still
    // starts through the protected overload, so a subclass that overrides it sees it, and made goes along on this
    // thread. An override that calls the overload on another thread starts the replay without made, and the live
    // delegate then gets the action ungated, as before 0.35.0
    Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused, @Nullable Subscribed made) {
        if (made == null) {
            return startCatchupAsync(subscriptionId, catchup, holdPaused);
        }
        return withRecordOnThisThread(subscriptionId, made, () -> startCatchupAsync(subscriptionId, catchup, holdPaused));
    }

    // Runs call with made as the record of the subscription this thread subscribes, and puts back the one before
    private <T> T withRecordOnThisThread(String subscriptionId, Subscribed made, Supplier<T> call) {
        StartingFromSubscribe outer = STARTING_FROM_SUBSCRIBE.get();
        STARTING_FROM_SUBSCRIBE.set(new StartingFromSubscribe(subscriptionId, made));
        try {
            return call.get();
        } finally {
            if (outer == null) {
                STARTING_FROM_SUBSCRIBE.remove();
            } else {
                STARTING_FROM_SUBSCRIBE.set(outer);
            }
        }
    }

    // The record of the subscription this thread subscribes, read by the protected subscribeInTheWrappedModel and
    // startCatchupAsync
    private static final ThreadLocal<@Nullable StartingFromSubscribe> STARTING_FROM_SUBSCRIBE = new ThreadLocal<>();

    private record StartingFromSubscribe(String subscriptionId, Subscribed made) {
    }

    // The replay waits on its own thread for earlierAttemptsDone, so it doesn't run the action alongside an attempt
    // that is still in it
    private Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused, boolean resuming, CompletableFuture<Void> earlierAttemptsDone) {
        return startCatchupAsync(subscriptionId, catchup, holdPaused, resuming, earlierAttemptsDone, null, null);
    }

    // takenOver is the handover whose live start the live delegate lost, when a takeover starts the replay
    private Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused, boolean resuming, CompletableFuture<Void> earlierAttemptsDone,
                                                   @Nullable HandedOver takenOver, @Nullable Subscribed made) {
        CompletableFuture<Subscription> result = new CompletableFuture<>();
        ReplayState replay = new ReplayState(subscriptionId, catchup, result, resuming, takenOver, takesOverLostLiveStarts(), made);
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

    // Done once no replay of subscriptionId other than replay, that a cancel or a stop ended, or that was dropped for
    // the id, is still in the subscriber's action, and no live call of a subscription that is no longer the record for
    // the id is either. Called with the handover lock held.
    private CompletableFuture<Void> endedReplaysReturned(String subscriptionId, @Nullable ReplayState replay) {
        CompletableFuture<Void> returned = CompletableFuture.completedFuture(null);
        CatchupAttempt ended = currentAttempt.get(subscriptionId);
        if (ended != null && ended.replay != replay && ended.abandoned()) {
            returned = ended.done;
        }
        CompletableFuture<Void> dropped = droppedReplaysReturned.get(subscriptionId);
        return dropped == null ? returned : CompletableFuture.allOf(returned, dropped);
    }

    // Makes the next replay of subscriptionId, and each call the live delegate makes for the next subscribe of it, wait
    // for the calls forgotten's live delegate still has in the subscriber's action. Called once forgotten is no longer
    // the record for subscriptionId, so no call of it is let in after this
    private void waitForTheLiveCallsOf(String subscriptionId, Subscribed forgotten) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            waitForTheDroppedReplay(subscriptionId, forgotten.liveCalls().returned());
        }
    }

    // Keeps returned in droppedReplaysReturned until it completes, so registerOrPark makes the next replay of
    // subscriptionId wait for the attempts of a replay dropped for it, or for the live calls of a subscription that is
    // no longer the record for it. Called with the handover lock held.
    private void waitForTheDroppedReplay(String subscriptionId, CompletableFuture<Void> returned) {
        if (returned.isDone()) {
            return;
        }
        CompletableFuture<Void> waited = droppedReplaysReturned.merge(subscriptionId, returned, CompletableFuture::allOf);
        waited.whenComplete((ignored, alsoIgnored) -> droppedReplaysReturned.remove(subscriptionId, waited));
    }

    /**
     * Registers a fresh attempt for {@code catchup} as the running catch-up for {@code subscriptionId}, or parks
     * {@code catchup} and returns {@code null} while this model is stopped or {@code holdPaused} is set, or when the
     * {@link CatchupListener} throws as the replay starts, which is logged at {@code ERROR}. Called with the
     * handover lock held. A replay still parked for the id is dropped, and waitUntilStarted on its handle returns false.
     * The new replay waits for the attempt that a cancel ended, also once a stop has removed it, for the
     * earlier attempts of a replay it drops, and for those of a parked replay a cancel or a resume dropped, so an
     * action that runs past the cancel's or the stop's wait never runs alongside the new replay's first call to it.
     */
    private @Nullable CatchupAttempt registerOrPark(String subscriptionId, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone, boolean holdPaused) {
        earlierAttemptsDone = CompletableFuture.allOf(earlierAttemptsDone, endedReplaysReturned(subscriptionId, replay));
        ParkedReplay superseded = parkedReplays.remove(subscriptionId);
        if (superseded != null && superseded.replay() != replay) {
            superseded.replay().cancelled = true;
            superseded.replay().result.cancel(false);
            earlierAttemptsDone = CompletableFuture.allOf(earlierAttemptsDone, superseded.earlierAttemptsDone());
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
            runCallersCode(() -> listener.catchupStarted(attempt));
            return true;
        } catch (Throwable e) {
            // Throwable, since a listener written in Kotlin can throw a checked exception
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
            CatchupCannotResumeException heldPaused = null;
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
                        if (ended && endedUnwanted(attempt, failure)) {
                            // A cancel, a shutdown or a later replay ended a call to the subscription model, and the
                            // handle answers as it does when one of them finds the replay between two calls
                            attempt.replay.result.complete(new CancelledSubscription(subscriptionId));
                        } else if (ended) {
                            attempt.replay.result.completeExceptionally(failure);
                        }
                    }
                } else if (attempt.heldForLostLiveStart) {
                    // A startup that waits for the handle fails as it did before the hold, and a resume that runs the
                    // replay again gets a handle of its own
                    attempt.replay.result.completeExceptionally(failure);
                    heldPaused = (CatchupCannotResumeException) failure;
                }
            } finally {
                CURRENT_ATTEMPT.remove();
                attempt.earlierAttemptsDone.whenComplete((ignored, alsoIgnored) -> attempt.done.complete(null));
            }
            // Once this thread no longer reads as the replay's and the attempt is done, so a listener can resume or pause
            // the subscription it is told about
            if (heldPaused != null) {
                reportHeldPaused(subscriptionId, heldPaused);
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
     * Only a replay a resume started is kept to run again, since the live delegate holds its subscription paused and
     * nothing else would resume it, and so is a replay a subscribe started whose action threw {@link AttemptNotMade},
     * since the event it withdrew would otherwise never be delivered here. Such a replay runs again from the last
     * position it stored, while it is still the current attempt and nothing asked it to stop. A cancel, a resume at a
     * given position or a shutdown ends it, which the caller asked for. A stop, a pause asked for during the replay or
     * while it waits, or a {@link CatchupListener} that throws as the replay runs again, holds it until a resume or a
     * start that resumes. So does a {@link CatchupCannotResumeException}, since the replay would fail the same way if it
     * ran again now, and an {@link AttemptNotMade}, since a model that declares {@code RedeliveringSubscriptions}
     * delivers a withdrawn event again only when the subscription is resumed or started. Neither waits out a backoff.
     * Any other failure runs it again after a backoff. A resume the live delegate refuses with
     * {@link SubscriptionAlreadyRunningException} ends it too, and the log says whether the live delegate runs the
     * subscription or holds it paused by then, and what resumes it.
     * <p>
     * A replay a subscribe started that fails while a stop runs that hasn't parked it yet is parked as the stop would
     * park it, so start(true) runs it again.
     */
    private @Nullable Duration runAgainAfter(String subscriptionId, CatchupAttempt attempt, @Nullable Throwable failure) {
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            CatchupAttempt owner = currentAttempt.get(subscriptionId);
            boolean current = owner == attempt;
            if (current && !attempt.abandoned() && !shuttingDown && endedUnwanted(attempt, failure)) {
                // A call to the wrapped model threw, or answered that it can't resume, once a stop marked this model
                // stopped, and the stop hasn't parked this replay yet. Parked here as shouldKeepReplaying parks it,
                // and run again at once when a start came in between.
                attempt.backingOff = false;
                parkIfStillCurrent(subscriptionId, attempt);
                log.debug("The catch-up replay for subscription {} was parked by a stop that came during a call to the subscription model.", subscriptionId, failure);
                return null;
            }
            if (current && !attempt.abandoned() && !shuttingDown && failure != null && HistoryLossMessages.isHistoryLost(failure)) {
                holdForLostLiveStart(subscriptionId, attempt, failure);
                return null;
            }
            if (current && !attempt.abandoned() && !shuttingDown && failure instanceof AttemptNotMade) {
                attempt.withdrawn = true;
            }
            if (current && !attempt.abandoned() && !shuttingDown && failure != null && (stopped || attempt.owesAPark()) && !(failure instanceof SubscriptionAlreadyRunningException)) {
                // A failure the wrapped model didn't throw for the stop, such as a failed read of the replay or a failed
                // action, while a stop that hasn't parked this replay yet runs. Parked as the stop parks it, so
                // start(true) runs it.
                attempt.backingOff = false;
                parkIfStillCurrent(subscriptionId, attempt);
                logFailure(subscriptionId, failure, "it runs again once the subscription is resumed or start(true) is called on this model");
                return null;
            }
            if (current && (attempt.replay.resuming || attempt.withdrawn) && !attempt.abandoned() && !shuttingDown && !(failure instanceof SubscriptionAlreadyRunningException)) {
                if (stopped || attempt.owesAPark()) {
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
                if (failure instanceof CatchupCannotResumeException || failure instanceof AttemptNotMade) {
                    // Held rather than run again now. A CatchupCannotResumeException would fail the same way, and a
                    // withdrawn event is delivered again only on a resume or a start, as RedeliveringSubscriptions says
                    attempt.backingOff = false;
                    hold(subscriptionId, attempt);
                    logFailure(subscriptionId, failure, "it was held, and runs again once the subscription is resumed or start(true) is called on this model");
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
                if (!attempt.replay.resuming && !attempt.wentLive && !attempt.parked && !attempt.abandoned()) {
                    markDidNotGoLive(subscriptionId, true);
                }
            }
            // Logged as well as reported, since a caller that never calls waitUntilStarted, such as the Spring Boot
            // starter by default for a subscription that replays history, would otherwise lose the subscription
            // without a trace. A cancelled, superseded or shut down attempt is not logged at error level, since it was
            // asked to stop. An attempt whose call to the subscription model a cancel, a stop, a shutdown or a later
            // replay ended is logged at debug level.
            if (endedUnwanted(attempt, failure)) {
                log.debug("The catch-up replay for subscription {} ended once a cancel, a stop, a shutdown or a later replay of it came during a call to the subscription model.", subscriptionId, failure);
            } else if (failure != null && attempt.wentLive) {
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

    /**
     * Holds the replay of {@code attempt} paused, whether a subscribe or a resume started it, once the live delegate
     * lost the live start the replay would go live from. Running it again at once would fail the same way, and ending it
     * would leave a subscription that neither runs nor is held, with a competing-consumer lease nobody uses. When the
     * replay started after the beginning, a replay again from its origin with a live start read now would skip events,
     * so the loss is recorded. Recorded means a resume would skip events, and a resume or a {@code start(true)} refuses with {@link CatchupCannotResumeException}
     * until the subscription is cancelled or resumed at a given position. A replay that started at the beginning skips
     * nothing when it runs again, so that loss is not recorded, and a resume or a {@code start(true)} replays again.
     * Called with the handover lock held.
     */
    private void holdForLostLiveStart(String subscriptionId, CatchupAttempt attempt, Throwable failure) {
        attempt.backingOff = false;
        pauseRequestedDuringCatchup.remove(subscriptionId);
        attempt.heldForLostLiveStart = true;
        attempt.replay.lostItsLiveStart = true;
        attempt.replay.heldFor = (CatchupCannotResumeException) failure;
        hold(subscriptionId, attempt);
        Checkpoint lost = HistoryLossMessages.lostFromWhereAResumeSkipsEvents(failure);
        Subscribed made = subscribed.get(subscriptionId);
        if (lost != null && made != null && made.owner() == this && made.historyLost().recordUnlessCleared(made.historyLost().clears(), lost, true)) {
            String at = lost.asString();
            log.error(HistoryLossMessages.LIVE_START_LOST_LOG, subscriptionId, at, at, failure);
        } else {
            logFailure(subscriptionId, failure, lost == null ? "the subscription is held paused, and resuming it or start(true) on this model replays again from the beginning"
                    : "the subscription is held paused");
        }
    }

    private static void logFailure(String subscriptionId, @Nullable Throwable failure, String outcome) {
        // A withdrawn event is no failure of the replay
        if (failure instanceof AttemptNotMade) {
            log.debug("The action withdrew an event of the catch-up replay for subscription {}, so {}.", subscriptionId, outcome, failure);
        } else if (failure != null) {
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
        while (currentAttempt.get(subscriptionId) == attempt && !attempt.abandoned() && !shuttingDown && !stopped && !attempt.owesAPark()) {
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
