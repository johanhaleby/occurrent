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

package org.occurrent.subscription.mongodb.blocking.changestream.internal;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * The subscriptions of a blocking MongoDB subscription model, each read from its own change stream cursor on a thread
 * of the model's executor. {@code NativeMongoSubscriptionModel} and {@code SpringMongoSubscriptionModel} both keep
 * their subscriptions here, so a subscription is registered, opened, restarted, paused, resumed and cancelled the same
 * way in both.
 * <p>
 * Every method that changes which subscriptions are known is called with the model's monitor held, the one
 * {@link #ChangeStreamSubscriptions the constructor} is given. The threads that read the change streams never take it.
 */
@NullMarked
public final class ChangeStreamSubscriptions {

    /**
     * What this class asks of the subscription model whose subscriptions it keeps.
     */
    public interface Model {
        /**
         * Returns the change stream of the event collection for {@code pipeline}, not yet opened.
         */
        ChangeStreamIterable<Document> watch(List<Bson> pipeline);

        /**
         * Runs {@code command} against the database of the event collection and returns the reply.
         */
        Document runCommand(Document command);

        /**
         * Runs {@code task} on the executor that reads the change streams.
         */
        void execute(Runnable task);

        /**
         * Whether the executor no longer accepts tasks.
         */
        boolean executorIsShutDown();

        /**
         * Called once when the model shuts down, after every subscription is cancelled.
         */
        void shutdownExecutor();

        /**
         * Returns the handle the model gives its caller for a subscription whose change stream has opened once
         * {@code started} is released.
         */
        Subscription subscription(String subscriptionId, CountDownLatch started);

        /**
         * The model's own {@code resumeSubscription(String)}, so an override of it also runs for a subscription that
         * {@code start(true)} resumes.
         */
        Subscription resumeSubscription(String subscriptionId);

        /**
         * The model's own {@code pauseSubscription(String)}, so an override of it also runs for a subscription that
         * {@code stop()} pauses.
         */
        void pauseSubscription(String subscriptionId);

        /**
         * The model's own {@code cancelSubscription(String)}, so an override of it also runs for a subscription that
         * {@code shutdown()} cancels.
         */
        void cancelSubscription(String subscriptionId);
    }

    private final Object monitor;
    private final Model model;
    private final Class<?> modelClass;
    private final Logger log;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();
    private final List<HistoryLossListener> historyLossListeners = new CopyOnWriteArrayList<>();
    private final TimeRepresentation timeRepresentation;
    private final RetryStrategy retryStrategy;
    private final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    private final @Nullable Integer batchSize;
    private final @Nullable Duration maxAwaitTime;

    private volatile boolean shutdown = false;
    private volatile boolean running;

    private final Predicate<Throwable> NOT_SHUTDOWN = __ -> !shutdown;
    // A refused checkpoint write must never be retried, on either retry loop below. The call sites already pass
    // their own predicate, which RetryExecution combines with the strategy's own.
    private static final Predicate<Throwable> NOT_A_REFUSED_CHECKPOINT_WRITE = e -> !(e instanceof CheckpointWriteConditionNotFulfilledException);
    private final Predicate<Throwable> RETRYABLE = NOT_SHUTDOWN.and(NOT_A_REFUSED_CHECKPOINT_WRITE);

    /**
     * @param monitor    The monitor the model holds when it calls a method that changes which subscriptions are known
     * @param model      What this class asks of the model
     * @param modelClass The model's class, which a dynamic start position is told it is resolved for
     * @param log        The model's logger
     * @param running    Whether the model starts out running. A subscription made while it doesn't is held paused.
     */
    public ChangeStreamSubscriptions(Object monitor, Model model, Class<?> modelClass, Logger log, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy,
                                     boolean restartSubscriptionsOnChangeStreamHistoryLost, @Nullable Integer batchSize, @Nullable Duration maxAwaitTime, boolean running) {
        this.monitor = requireNonNull(monitor, "monitor cannot be null");
        this.model = requireNonNull(model, "model cannot be null");
        this.modelClass = requireNonNull(modelClass, "modelClass cannot be null");
        this.log = requireNonNull(log, "log cannot be null");
        this.timeRepresentation = requireNonNull(timeRepresentation, "Time representation cannot be null");
        this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
        this.batchSize = batchSize;
        this.maxAwaitTime = maxAwaitTime;
        this.running = running;
    }

    /**
     * Registers a subscription and opens its change stream, or holds it paused when {@code holdPaused} is true or the
     * model doesn't run.
     *
     * @param pipeline Builds the pipeline of the change stream, and throws when the model can't apply the filter it
     *                 builds it from. Called after the id is known to be free.
     */
    public Subscription subscribe(String subscriptionId, Supplier<List<Bson>> pipeline, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (isKnown(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }

        // Built here rather than on the executor thread, so a filter the model cannot apply is refused to the caller
        // instead of failing where nobody is listening
        List<Bson> changeStreamPipeline = pipeline.get();
        // The start position for the same reason. A checkpoint the model cannot parse would fail on the executor
        // thread, where the retry wrapper throws it again forever and the caller holds a subscription whose latch
        // never counts down. A dynamic position is a no-op in there, for a reason checkStartPosition documents.
        MongoCommons.checkStartPosition(startAt, subscriptionModelContext());

        if (shutdown || model.executorIsShutDown()) {
            throw new IllegalStateException("Cannot start subscription because the executor is shutdown or terminated.");
        }

        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
        PresentAtSubscribe presentAtSubscribe = new PresentAtSubscribe(cancelled -> recordThePresent(subscriptionId, currentStartAt, cancelled));
        InternalSubscription internalSubscription = new InternalSubscription(log, currentStartAt, action, changeStreamPipeline, presentAtSubscribe);
        // Known from here on rather than once its change stream opens, so a pause or a cancel reaches it while MongoDB
        // cannot be reached, and a wrapper asking which subscriptions the model runs gets the right answer. On a
        // running model a run that opens at the present asks for it first.
        if (running && !holdPaused) {
            runningSubscriptions.put(subscriptionId, internalSubscription);
            startSubscription(subscriptionId, internalSubscription, () -> runningSubscriptions.remove(subscriptionId, internalSubscription));
        } else {
            // Opens nothing until start() or a resume does, like a subscription that stop() paused. The present is
            // asked for on the executor, so this returns without waiting for MongoDB, and a change stream that opens
            // at the answer waits for it.
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            try {
                model.execute(presentAtSubscribe::ask);
            } catch (RuntimeException e) {
                pausedSubscriptions.remove(subscriptionId, internalSubscription);
                throw e;
            }
        }
        // Follows every run of the subscription, so it answers true once start() or a resume opens the change stream of
        // one subscribed while the model was stopped, or paused before its change stream opened
        return model.subscription(subscriptionId, internalSubscription.firstStartedLatch);
    }

    private SubscriptionModelContext subscriptionModelContext() {
        return new SubscriptionModelContext(modelClass);
    }

    /**
     * Throws when the model can't open a change stream at {@code startAt}.
     */
    public void checkStartPosition(StartAt startAt) {
        MongoCommons.checkStartPosition(startAt, subscriptionModelContext());
    }

    // Records the present for a subscription started at the present, so the events written from then on are delivered
    // to it whatever pause, stop, resume or start comes before its change stream opens. A position that isn't the
    // present is left as it is. Retried like opening a change stream, and a pause doesn't end it, so an outage that
    // ends while the subscription is paused loses nothing. A cancel or a shutdown stops it before the next attempt
    // rather than interrupting it, since the thread it runs on belongs to the executor and runs other subscriptions.
    private void recordThePresent(String subscriptionId, AtomicReference<StartAt> currentStartAt, BooleanSupplier cancelled) {
        SubscriptionModelContext subscriptionModelContext = subscriptionModelContext();
        try {
            executeWithRetry(() -> {
                MongoCommons.resolveOpeningPosition(currentStartAt, subscriptionModelContext, this::currentOperationTime);
            }, RETRYABLE.and(__ -> !cancelled.getAsBoolean() && !Thread.currentThread().isInterrupted()), retryStrategy).run();
        } catch (RuntimeException e) {
            if (cancelled.getAsBoolean() || shutdown || e instanceof MongoInterruptedException || Thread.currentThread().isInterrupted()) {
                log.debug("Stopped asking MongoDB for its operation time for subscription {} because it was cancelled or the model shut down.", subscriptionId, e);
            } else {
                log.warn("Gave up asking MongoDB for its operation time for subscription {}, as its retry strategy says. This can keep its change stream from opening, and pausing and resuming the subscription after that starts it again.", subscriptionId, e);
            }
            throw e;
        } catch (Error e) {
            log.error("Asking MongoDB for its operation time for subscription {} failed with an error, so its change stream doesn't open.", subscriptionId, e);
            throw e;
        }
    }

    private static boolean needsThePresent(StartAt position) {
        return position.isDynamic() || MongoCommons.opensAtThePresent(position);
    }

    // Called once the subscription is registered, so forget(..) on the executor thread always finds the run it removes
    private void startSubscription(String subscriptionId, InternalSubscription internalSubscription, Runnable unregister) {
        try {
            model.execute(() -> runUntilStopped(subscriptionId, internalSubscription));
        } catch (RuntimeException e) {
            unregister.run();
            throw e;
        }
    }

    // Restarts with the retry strategy's backoff until the subscription is closed or the strategy gives up. A pause,
    // cancel or shutdown closes it, and a resume runs a new one, so a restart due after the backoff opens nothing for
    // a subscription that was paused or cancelled meanwhile, and its last error is logged rather than thrown. When the
    // strategy gives up, the last error is thrown on the executor thread and the subscription stays known and
    // running, so a pause and a resume start it again.
    private void runUntilStopped(String subscriptionId, InternalSubscription internalSubscription) {
        try {
            // Even when the run is already closed, so a pause doesn't end the question. Outside the retry below, so a
            // question the strategy gave up on is thrown like an open it gave up on rather than asked again. A position
            // that is fixed and isn't the present needs no answer, such as one a resume was given, so the run doesn't
            // wait for the question or throw its give-up. A dynamic one is only known once resolved, so it waits.
            if (!needsThePresent(internalSubscription.currentStartAt.get())) {
                internalSubscription.presentAtSubscribe.passedBy();
            } else if (!internalSubscription.presentAtSubscribe.awaitedBy(internalSubscription)) {
                return;
            }
            executeWithRetry(() -> newInternalSubscription(subscriptionId, internalSubscription), RETRYABLE.and(__ -> !internalSubscription.isIntentionallyClosed()), retryStrategy).run();
        } catch (RuntimeException e) {
            if (!internalSubscription.isIntentionallyClosed()) {
                throw e;
            }
            log.debug("Stopped restarting subscription {} because it was paused, cancelled or shut down while waiting to restart after {}.", subscriptionId, e.getClass().getName(), e);
        } finally {
            internalSubscription.stoppedRestarting();
        }
    }

    // currentStartAt tracks the last change-stream document read (updated below, even without a delivered
    // CloudEvent), shared by every attempt of runUntilStopped and by a resume, so each continues gap-free from there
    // instead of the original StartAt. Before the first one it holds the operation time the stream opened at, when
    // MongoDB's reply had one, so an original StartAt of the present is not resolved again.
    // The try block spans opening the cursor too, since a change-stream error (history lost, failover) can surface
    // there just as well as while iterating.
    private void newInternalSubscription(String subscriptionId, InternalSubscription internalSubscription) {
        if (internalSubscription.isIntentionallyClosed()) {
            return;
        }
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        Consumer<CloudEvent> action = internalSubscription.action;
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = null;
        try {
            ChangeStreamIterable<Document> changeStreamDocuments = model.watch(internalSubscription.pipeline);
            if (batchSize != null) {
                changeStreamDocuments = changeStreamDocuments.batchSize(batchSize);
            }
            if (maxAwaitTime != null) {
                changeStreamDocuments = changeStreamDocuments.maxAwaitTime(maxAwaitTime.toMillis(), MILLISECONDS);
            }
            SubscriptionModelContext subscriptionModelContext = subscriptionModelContext();
            StartAt openingPosition = MongoCommons.resolveOpeningPosition(currentStartAt, subscriptionModelContext, this::currentOperationTime);
            ChangeStreamIterable<Document> changeStreamDocumentsAtPosition = MongoCommons.applyStartPosition(changeStreamDocuments, ChangeStreamIterable::startAfter, ChangeStreamIterable::startAtOperationTime, openingPosition, subscriptionModelContext);
            cursor = changeStreamDocumentsAtPosition.cursor();
            if (!internalSubscription.opened(cursor)) {
                // Closed while the change stream was opening, and the finally block closes the cursor
                return;
            }

            internalSubscription.started();

            cursor.forEachRemaining(changeStreamDocument -> {
                // A document already fetched when the subscription was closed is left to a resume, rather than
                // delivered to a subscription that is paused or cancelled
                if (internalSubscription.isIntentionallyClosed()) {
                    return;
                }
                MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(changeStreamDocument, timeRepresentation)
                        .map(cloudEvent -> new CheckpointAwareCloudEvent(cloudEvent, new MongoResumeTokenCheckpoint(changeStreamDocument.getResumeToken())))
                        .ifPresent(executeWithRetry(action, RETRYABLE.and(__ -> !internalSubscription.isIntentionallyClosed()), retryStrategy));
                currentStartAt.set(StartAt.checkpoint(new MongoResumeTokenCheckpoint(changeStreamDocument.getResumeToken())));
            });
        } catch (RuntimeException e) {
            if (internalSubscription.isIntentionallyClosed() || isCursorNoLongerOpen(e)) {
                log.debug("Caught {} (message={}) for subscription {}, this might happen when a subscription is paused or cancelled.", e.getClass().getName(), e.getMessage(), subscriptionId, e);
            } else if (e instanceof CheckpointWriteConditionNotFulfilledException) {
                // Stays known and pausable, unlike the history-lost branch below, since forgetting it here would let
                // the strategy pause a subscription the model no longer knows about. Logged at error level because
                // the exception leaves the model right after this and the outer retry won't restart on it, so
                // nothing else would say why the node went quiet.
                log.error("Checkpoint write for subscription {} was refused: {}. This node's lease has moved to another one, so delivery stops here rather than retrying. The subscription stays known and running until the next lease refresh pauses it, and a resume redelivers the event once this node holds the lease again.", subscriptionId, e.getMessage(), e);
                throw e;
            } else if (isChangeStreamHistoryLost(e)) {
                if (restartSubscriptionsOnChangeStreamHistoryLost) {
                    log.warn("There was not enough oplog to resume subscription {}, will restart subscription from current time.", subscriptionId, e);
                    currentStartAt.set(restartPositionAfterHistoryLost(subscriptionId));
                    throw e;
                } else {
                    log.error("There was not enough oplog to resume subscription {}, will not restart subscription! Consider removing the subscription from the durable storage or use a catch-up subscription to get up to speed if needed.", subscriptionId, e);
                    forget(subscriptionId, internalSubscription);
                }
            } else if (shutdown) {
                log.debug("Subscription {} is shutting down, ignoring {}.", subscriptionId, e.getClass().getName(), e);
            } else {
                log.warn("Error caught for subscription {}: {} {}. Will restart!", subscriptionId, e.getClass().getName(), e.getMessage(), e);
                throw e;
            }
        } finally {
            if (cursor != null) {
                internalSubscription.stopped();
                try {
                    cursor.close();
                } catch (Exception closeException) {
                    log.debug("Failed to close cursor for subscription {}, this can happen if the connection was already closed.", subscriptionId, closeException);
                }
            }
        }
    }

    // Tells the listeners the present before restarting from it. A listener that throws fails this attempt and
    // the retry runs it again. Without an operation time in the reply to ping, restarts from now and tells nobody
    private StartAt restartPositionAfterHistoryLost(String subscriptionId) {
        BsonTimestamp operationTime = currentOperationTime();
        if (operationTime == null) {
            return StartAt.now();
        }
        Checkpoint present = new MongoOperationTimeCheckpoint(operationTime);
        historyLossListeners.forEach(listener -> listener.restartingAfterHistoryLoss(subscriptionId, present));
        return StartAt.checkpoint(present);
    }

    public void addHistoryLossListener(HistoryLossListener listener) {
        requireNonNull(listener, HistoryLossListener.class.getSimpleName() + " cannot be null");
        historyLossListeners.add(listener);
    }

    public void removeHistoryLossListener(HistoryLossListener listener) {
        requireNonNull(listener, HistoryLossListener.class.getSimpleName() + " cannot be null");
        historyLossListeners.remove(listener);
    }

    // Only while the subscription is still on this run. A pause and a resume in the meantime started a new run, which
    // has not lost anything. Runs on the executor thread without the model's monitor, because a pause holds that
    // monitor while it waits for the run to stop.
    private void forget(String subscriptionId, InternalSubscription internalSubscription) {
        runningSubscriptions.remove(subscriptionId, internalSubscription);
    }

    private @Nullable BsonTimestamp currentOperationTime() {
        Document reply = model.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND);
        BsonTimestamp operationTime = MongoCommons.operationTimeAfter(reply);
        if (operationTime == null) {
            log.warn(MongoCommons.noOperationTimeToPinToMessage(reply));
        }
        return operationTime;
    }

    private static boolean isCursorNoLongerOpen(Throwable throwable) {
        return throwable instanceof IllegalStateException && throwable.getMessage() != null && throwable.getMessage().startsWith("Cursor") && throwable.getMessage().contains("is not longer open");
    }

    private static boolean isChangeStreamHistoryLost(Throwable throwable) {
        return throwable instanceof MongoCommandException mongoCommandException && mongoCommandException.getErrorCode() == MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE;
    }

    public void cancelSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.cancel();
        }
        InternalSubscription pausedSubscription = pausedSubscriptions.remove(subscriptionId);
        if (pausedSubscription != null) {
            pausedSubscription.cancel();
        }
    }

    public void shutdown() {
        shutdown = true;
        running = false;
        runningSubscriptions.keySet().forEach(model::cancelSubscription);
        runningSubscriptions.clear();
        // Cancelled too, so a question for the present still waiting for an executor thread isn't asked while the
        // executor shuts down below
        pausedSubscriptions.values().forEach(InternalSubscription::cancel);
        pausedSubscriptions.clear();
        model.shutdownExecutor();
    }

    public void stop() {
        if (!shutdown) {
            running = false;
            // A copy of the keys, since pauseSubscription moves each id from runningSubscriptions to
            // pausedSubscriptions as it goes, and forEach over a map that its own callback changes can visit an entry
            // that has already moved, or miss one that has not
            new ArrayList<>(runningSubscriptions.keySet()).forEach(model::pauseSubscription);
        }
    }

    /**
     * Takes the model's monitor itself while it resumes, and waits for the resumed subscriptions without it.
     */
    public void start(boolean resumeSubscriptionsAutomatically) {
        Map<String, ResumedByStart> resumed = new LinkedHashMap<>();
        List<RuntimeException> failures = new ArrayList<>();
        synchronized (monitor) {
            if (shutdown) {
                return;
            }
            running = true;
            if (resumeSubscriptionsAutomatically) {
                // A copy of the keys for the same reason as in stop(), since resumeSubscription moves each id out of
                // pausedSubscriptions as it goes
                for (String subscriptionId : new ArrayList<>(pausedSubscriptions.keySet())) {
                    // An override may already have resumed or cancelled this subscription while resuming an earlier one
                    if (!pausedSubscriptions.containsKey(subscriptionId)) {
                        continue;
                    }
                    try {
                        Subscription subscription = model.resumeSubscription(subscriptionId);
                        // Only the executor thread writes to this map without the monitor, and it only removes a run
                        // whose history was lost, so this is the run the resume started, or nothing when there is
                        // nothing to wait for
                        InternalSubscription run = runningSubscriptions.get(subscriptionId);
                        if (run != null) {
                            resumed.put(subscriptionId, new ResumedByStart(subscription, run));
                        }
                    } catch (RuntimeException e) {
                        failures.add(e);
                    }
                }
            }
        }
        // Waited for outside the monitor, so pause, cancel and subscriptionIds() answer while a change stream cannot open
        resumed.forEach((subscriptionId, resumedByStart) -> {
            try {
                waitUntilStartedOrNoLongerRunning(subscriptionId, resumedByStart);
            } catch (RuntimeException e) {
                failures.add(e);
            }
        });
        if (!failures.isEmpty()) {
            RuntimeException first = failures.getFirst();
            failures.subList(1, failures.size()).forEach(first::addSuppressed);
            throw first;
        }
    }

    private record ResumedByStart(Subscription subscription, InternalSubscription run) {
    }

    private void waitUntilStartedOrNoLongerRunning(String subscriptionId, ResumedByStart resumedByStart) {
        Subscription subscription = resumedByStart.subscription();
        InternalSubscription internalSubscription = resumedByStart.run();
        while (!subscription.waitUntilStarted(Duration.ofMillis(100))) {
            if (shutdown || runningSubscriptions.get(subscriptionId) != internalSubscription || internalSubscription.hasStoppedRestarting()) {
                return;
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public Set<String> subscriptionIds() {
        return Stream.concat(runningSubscriptions.keySet().stream(), pausedSubscriptions.keySet().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    public boolean isPaused(String subscriptionId) {
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    private boolean isKnown(String subscriptionId) {
        return runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId);
    }

    // Separates "no such subscription here" from "wrong state for this call", which a caller holding several models
    // needs in order to tell "keep looking" from "this is the owner and the answer is no".
    private void requireKnown(String subscriptionId) {
        if (!isKnown(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    /**
     * Resumes a paused subscription, at {@code repositionTo} when it is given and otherwise from the position the
     * subscription has read to.
     */
    public Subscription resumeSubscription(String subscriptionId, @Nullable StartAt repositionTo) {
        if (shutdown) {
            throw new IllegalStateException(SubscriptionModel.class.getSimpleName() + " is shutdown");
        }
        requireKnown(subscriptionId);
        if (isRunning(subscriptionId)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }

        InternalSubscription internalSubscription = pausedSubscriptions.get(subscriptionId);
        if (internalSubscription == null) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }
        if (repositionTo != null) {
            internalSubscription.currentStartAt.set(repositionTo);
        }

        running = true;

        // Shares the same currentStartAt reference so a resume continues from the last change-stream document
        // read before the subscription was paused, not the original StartAt, unless repositionTo replaced it above.
        InternalSubscription resumed = internalSubscription.resumed();
        pausedSubscriptions.remove(subscriptionId);
        runningSubscriptions.put(subscriptionId, resumed);
        startSubscription(subscriptionId, resumed, () -> {
            runningSubscriptions.remove(subscriptionId, resumed);
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        });

        return model.subscription(subscriptionId, resumed.startedLatch);
    }

    public void pauseSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(SubscriptionModel.class.getSimpleName() + " is shutdown");
        }
        requireKnown(subscriptionId);
        if (isPaused(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
        } else if (!isRunning(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }

        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.close();
            if (!internalSubscription.waitUntilStopped(Duration.ofSeconds(1))) {
                log.debug("Failed to stop internal subscription after 1 second");
            }
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
    }

    // The question to MongoDB for its operation time that fixes where a subscription started at the present opens.
    // One question shared by every run of the subscription, asked again after the retry strategy gives up on it. It's
    // asked on the executor right after subscribe(..) on a stopped model, or by the first run that gets to it. A pause
    // doesn't stop it.
    private static final class PresentAtSubscribe {
        private final Consumer<BooleanSupplier> question;
        private volatile FutureTask<Void> asking;
        private volatile boolean cancelled;
        // Set by the first run to get here, whether it waits for the answer or not
        private volatile boolean reached;

        PresentAtSubscribe(Consumer<BooleanSupplier> question) {
            this.question = question;
            this.asking = newQuestion();
        }

        void ask() {
            asking.run();
        }

        // Without an interrupt, since the question runs on an executor thread that a later task reuses, and some
        // executors don't clear the interrupt between tasks
        synchronized void cancel() {
            cancelled = true;
            asking.cancel(false);
        }

        // Asks here when nobody has yet, and otherwise waits for the answer, so the change stream never opens later
        // than the position it records. A run closed meanwhile stops waiting and frees its thread, and the question goes
        // on without it. False when the run was closed or the question cancelled first. When the retry strategy gives
        // up, its error is thrown to the open run waiting for the answer, or to the first run of all when the give-up
        // came before it, as when the strategy gives up opening the change stream. Any later run that finds the
        // question given up asks again.
        boolean awaitedBy(InternalSubscription run) {
            boolean anEarlierRunReached = reached;
            reached = true;
            FutureTask<Void> current = asking;
            if (anEarlierRunReached && current.state() == Future.State.FAILED) {
                askAgainAfter(current);
                current = asking;
            }
            current.run();
            while (true) {
                try {
                    current.get(100, MILLISECONDS);
                    return true;
                } catch (TimeoutException e) {
                    if (run.isIntentionallyClosed()) {
                        return false;
                    }
                } catch (CancellationException e) {
                    return false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    if (run.isIntentionallyClosed()) {
                        return false;
                    }
                    askAgainAfter(current);
                    Throwable gaveUpOn = e.getCause();
                    if (gaveUpOn instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    } else if (gaveUpOn instanceof Error error) {
                        throw error;
                    }
                    throw new IllegalStateException(gaveUpOn);
                }
            }
        }

        // For a run whose position needs no answer, so a give-up it never saw is asked again by a later run rather
        // than thrown to it
        void passedBy() {
            reached = true;
        }

        private synchronized void askAgainAfter(FutureTask<Void> failed) {
            if (!cancelled && asking == failed) {
                asking = newQuestion();
            }
        }

        private FutureTask<Void> newQuestion() {
            return new FutureTask<>(() -> question.accept(() -> cancelled), null);
        }
    }

    // One run of a subscription, from a subscribe or a resume until a pause, a cancel or a shutdown closes it. A
    // restart after an error reuses the same run, and a resume starts a new one, so a closed run never opens a change
    // stream again.
    private static class InternalSubscription {
        private final Logger log;
        // Kept so a resume reuses the pipeline built when subscribing, rather than deriving the same one again from the
        // same filter.
        private final List<Bson> pipeline;
        final CountDownLatch startedLatch = new CountDownLatch(1);
        // Shared by every run of the subscription, and released the first time one of them opens its change stream
        final CountDownLatch firstStartedLatch;
        private final CountDownLatch stoppedRestartingLatch = new CountDownLatch(1);
        final AtomicReference<StartAt> currentStartAt;
        // Shared by every run of the subscription
        final PresentAtSubscribe presentAtSubscribe;
        final Consumer<CloudEvent> action;
        private volatile boolean intentionallyClosed = false;
        // Read and written under this object's lock. The cursor the current attempt delivers from, and a latch released
        // once that attempt stops.
        private @Nullable MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
        private CountDownLatch stoppedLatch = new CountDownLatch(0);

        private InternalSubscription(Logger log, AtomicReference<StartAt> currentStartAt, Consumer<CloudEvent> action, List<Bson> pipeline, PresentAtSubscribe presentAtSubscribe) {
            this(log, currentStartAt, action, pipeline, presentAtSubscribe, new CountDownLatch(1));
        }

        private InternalSubscription(Logger log, AtomicReference<StartAt> currentStartAt, Consumer<CloudEvent> action, List<Bson> pipeline, PresentAtSubscribe presentAtSubscribe, CountDownLatch firstStartedLatch) {
            this.log = log;
            this.pipeline = pipeline;
            this.currentStartAt = currentStartAt;
            this.presentAtSubscribe = presentAtSubscribe;
            this.action = action;
            this.firstStartedLatch = firstStartedLatch;
        }

        InternalSubscription resumed() {
            return new InternalSubscription(log, currentStartAt, action, pipeline, presentAtSubscribe, firstStartedLatch);
        }

        // False when this was closed while the change stream opened, and the caller then closes the cursor itself
        synchronized boolean opened(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
            if (intentionallyClosed) {
                return false;
            }
            this.cursor = cursor;
            this.stoppedLatch = new CountDownLatch(1);
            return true;
        }

        void started() {
            startedLatch.countDown();
            firstStartedLatch.countDown();
        }

        synchronized void stopped() {
            cursor = null;
            stoppedLatch.countDown();
        }

        void stoppedRestarting() {
            stoppedRestartingLatch.countDown();
        }

        boolean hasStoppedRestarting() {
            return stoppedRestartingLatch.getCount() == 0;
        }

        boolean isIntentionallyClosed() {
            return intentionallyClosed;
        }

        boolean waitUntilStopped(Duration duration) {
            CountDownLatch attemptStopped;
            synchronized (this) {
                attemptStopped = stoppedLatch;
            }
            try {
                return attemptStopped.await(duration.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        // Unlike a pause, a cancel or a shutdown also stops an outstanding question for the present, since nothing
        // opens after it
        void cancel() {
            close();
            presentAtSubscribe.cancel();
        }

        // Marked before closing so the change-stream error this deliberately triggers is recognized as benign
        // (pause/cancel/shutdown) rather than an unexpected failure that should restart the subscription.
        void close() {
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> openCursor;
            synchronized (this) {
                intentionallyClosed = true;
                openCursor = cursor;
            }
            if (openCursor == null) {
                return;
            }
            try {
                openCursor.close();
            } catch (Exception e) {
                log.error("Failed to cancel subscription, this might happen if Mongo connection has been shutdown", e);
            }
        }
    }

    @Override
    public String toString() {
        return "runningSubscriptions=" + runningSubscriptions.keySet() + ", pausedSubscriptions=" + pausedSubscriptions.keySet() + ", shutdown=" + shutdown + ", running=" + running;
    }
}
