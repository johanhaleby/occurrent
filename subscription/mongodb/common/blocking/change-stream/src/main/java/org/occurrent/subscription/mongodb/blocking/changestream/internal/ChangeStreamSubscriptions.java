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
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.internal.RetryExecution.AttemptNotMade;
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
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions.QuietPositionListener;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
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
 * {@link #ChangeStreamSubscriptions the constructor} is given, except {@link #start(boolean)}, {@link #stop()} and
 * {@link #shutdown()}, which take it themselves. The threads that read the change streams never take it. Nothing
 * waits for an action to return, or for the executor to shut down, while it holds the monitor or a lock of this class,
 * since a virtual thread that waits in a monitor keeps its carrier thread on JDK 21 to 23. So a pause returns the wait
 * for the running action to the model, which runs it once it has let go of its monitor. A pause, a cancel and a
 * shutdown still close the cursor with the monitor held.
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
         * Whether the executor no longer accepts tasks. Answers {@code false} for an executor the model can't ask,
         * so a resume such an executor rejects after it was shut down is handed to it again, with a warning every
         * ten seconds, until the subscription is paused or cancelled or the model shuts down.
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
    private final List<QuietPositionListener> quietPositionListeners = new CopyOnWriteArrayList<>();
    private final TimeRepresentation timeRepresentation;
    private final RetryStrategy retryStrategy;
    private final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    private final @Nullable Integer batchSize;
    private final @Nullable Duration maxAwaitTime;

    private volatile boolean shutdown = false;
    private volatile boolean running;
    // Set by stop() while it pauses every subscription, so it waits for their running actions together once it has let
    // go of the model's monitor. Read and written under that monitor
    private @Nullable Map<String, InternalSubscription> pausedByAStop;

    private static final Duration WAIT_FOR_A_RUNNING_ACTION = Duration.ofSeconds(1);
    // How often a resume the executor rejected is handed to it again. It warns on every fifth rejection, which is every
    // ten seconds once the backoff has reached two
    private static final RetryStrategy.Retry HAND_TO_THE_EXECUTOR_AGAIN = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0);
    private static final int REJECTIONS_BETWEEN_WARNINGS = 5;

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
        try {
            executeWithRetry(() -> pinThePresent(currentStartAt), RETRYABLE.and(__ -> !cancelled.getAsBoolean() && !Thread.currentThread().isInterrupted()), retryStrategy).run();
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

    // Records the present for a dynamic position without evaluating it, so it's evaluated once, when the change stream
    // opens, and answers the recorded present if it resolves to the present then. A supplier that writes, such as
    // the one saving a durable subscription's first checkpoint, then writes once
    private void pinThePresent(AtomicReference<StartAt> currentStartAt) {
        while (true) {
            StartAt tracked = currentStartAt.get();
            if (!needsThePresent(tracked)) {
                return;
            }
            BsonTimestamp operationTime = currentOperationTime();
            if (operationTime == null || currentStartAt.compareAndSet(tracked, MongoCommons.pinnedTo(tracked, operationTime))) {
                return;
            }
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
                if (!(e instanceof CheckpointWriteConditionNotFulfilledException)) {
                    log.error("Gave up restarting subscription {}, as its retry strategy says. Pausing and resuming the subscription starts it again.", subscriptionId, e);
                }
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
    // MongoDB's reply had one, so an original StartAt of the present is not resolved again. A read that returns no
    // document moves it to the resume token MongoDB sent with that empty batch, so a subscription that matches nothing
    // doesn't keep a position the oplog drops.
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

            while (!internalSubscription.isIntentionallyClosed()) {
                // Asked before the read, so a listener decides what it may write before it knows what the read returns
                List<Consumer<Checkpoint>> quietPositionConsumers = quietPositionConsumersFor(subscriptionId);
                // Waits on the server for at most maxAwaitTime, and returns null when that batch has no document
                ChangeStreamDocument<Document> changeStreamDocument = cursor.tryNext();
                // A document already fetched when the subscription was closed is left to a resume, rather than
                // delivered to a subscription that is paused or cancelled
                if (!internalSubscription.startDelivering()) {
                    return;
                }
                try {
                    if (changeStreamDocument == null) {
                        reachedQuietPosition(internalSubscription, cursor.getResumeToken(), quietPositionConsumers);
                        continue;
                    }
                    MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(changeStreamDocument, timeRepresentation)
                            .map(cloudEvent -> new CheckpointAwareCloudEvent(cloudEvent, new MongoResumeTokenCheckpoint(changeStreamDocument.getResumeToken())))
                            .ifPresent(executeWithRetry(attemptWhileOpen(internalSubscription, action), RETRYABLE.and(__ -> !internalSubscription.isIntentionallyClosed()), retryStrategy));
                    // A run a pause closed while the action ran still moves the position, so a plain resume goes on
                    // after the event. Not once a resume has replaced it, since that resume set the position to open at
                    internalSubscription.movedUnlessReplacedTo(StartAt.checkpoint(new MongoResumeTokenCheckpoint(changeStreamDocument.getResumeToken())));
                } finally {
                    internalSubscription.stoppedDelivering();
                }
            }
        } catch (RuntimeException e) {
            if (internalSubscription.isIntentionallyClosed()) {
                log.debug("Caught {} (message={}) for subscription {}, this might happen when a subscription is paused or cancelled.", e.getClass().getName(), e.getMessage(), subscriptionId, e);
            } else if (e instanceof CheckpointWriteConditionNotFulfilledException) {
                // Stays known and pausable, unlike the history-lost branch below, since forgetting it here would let
                // the strategy pause a subscription the model no longer knows about. Logged at error level because
                // the exception leaves the model right after this and the outer retry won't restart on it, so
                // nothing else would say why the node went quiet.
                log.error("Checkpoint write for subscription {} was refused: {}. A node with a newer lease has already written this subscription's checkpoint, so delivery stops here rather than retrying. The subscription stays known and running until the next lease refresh pauses it. The refused write was for an event, or for a position reached while no event matched.", subscriptionId, e.getMessage(), e);
                throw e;
            } else if (isChangeStreamHistoryLost(e)) {
                if (restartSubscriptionsOnChangeStreamHistoryLost) {
                    log.warn("There was not enough oplog to resume subscription {}, will restart subscription from current time.", subscriptionId, e);
                    StartAt restartPosition = restartPositionAfterHistoryLost(subscriptionId, internalSubscription);
                    if (restartPosition != null) {
                        internalSubscription.movedUnlessReplacedTo(restartPosition);
                    }
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

    // Checked before every attempt, a retry included, so no attempt starts once this has read the run closed. Nothing
    // locks the check and the call together, so an attempt can still start just after a cancel returns, and one that
    // has already started can still be running when a pause or a cancel returns. The retry ends without telling the
    // strategy's listeners, since skipping the attempt is no error
    private static Consumer<CloudEvent> attemptWhileOpen(InternalSubscription internalSubscription, Consumer<CloudEvent> action) {
        return cloudEvent -> {
            if (internalSubscription.isIntentionallyClosed()) {
                throw new AttemptNotMade("The subscription was paused or cancelled before the action was called");
            }
            action.accept(cloudEvent);
        };
    }

    private List<Consumer<Checkpoint>> quietPositionConsumersFor(String subscriptionId) {
        if (quietPositionListeners.isEmpty()) {
            return List.of();
        }
        List<Consumer<Checkpoint>> consumers = new ArrayList<>(quietPositionListeners.size());
        for (QuietPositionListener listener : quietPositionListeners) {
            Consumer<Checkpoint> consumer = listener.beforeReading(subscriptionId);
            if (consumer != null) {
                consumers.add(consumer);
            }
        }
        return consumers;
    }

    // The token of an empty batch is a position every matching document before it was returned from, and the action
    // has completed for each of them, since this thread runs the action before it reads again. A run that was closed
    // moves nothing, so it cannot undo the position a resume was given. Without a token there is nothing to move to
    private void reachedQuietPosition(InternalSubscription internalSubscription, @Nullable BsonDocument resumeToken, List<Consumer<Checkpoint>> quietPositionConsumers) {
        if (resumeToken == null) {
            return;
        }
        Checkpoint quietPosition = new MongoResumeTokenCheckpoint(resumeToken);
        if (internalSubscription.movedWhileOpenTo(StartAt.checkpoint(quietPosition))) {
            quietPositionConsumers.forEach(consumer -> consumer.accept(quietPosition));
        }
    }

    // Tells the listeners the present before restarting from it. A listener that throws fails this attempt and
    // the retry runs it again. So does a reply to ping without an operation time, since restarting from a present
    // nobody stored keeps the lost position stored, and a process that stops before the next event then skips what
    // was written in between. A listener asks the run whether a resume or a cancel came while this asked for the present
    private @Nullable StartAt restartPositionAfterHistoryLost(String subscriptionId, InternalSubscription internalSubscription) {
        Document reply = model.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND);
        BsonTimestamp operationTime = MongoCommons.operationTimeAfter(reply);
        if (operationTime == null) {
            log.warn("The reply to {} had no {}, so subscription {} is not restarted from a present that cannot be recorded. Its retry strategy tries the restart again. Reply was: {}",
                    MongoCommons.CURRENT_OPERATION_TIME_COMMAND.toJson(), MongoCommons.OPERATION_TIME, subscriptionId, reply.toJson());
            return null;
        }
        Checkpoint present = new MongoOperationTimeCheckpoint(operationTime);
        historyLossListeners.forEach(listener -> listener.restartingAfterHistoryLoss(subscriptionId, present, internalSubscription::isCurrent));
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

    public void addQuietPositionListener(QuietPositionListener listener) {
        requireNonNull(listener, QuietPositionListener.class.getSimpleName() + " cannot be null");
        quietPositionListeners.add(listener);
    }

    public void removeQuietPositionListener(QuietPositionListener listener) {
        requireNonNull(listener, QuietPositionListener.class.getSimpleName() + " cannot be null");
        quietPositionListeners.remove(listener);
    }

    // Only while the subscription is still on this run. A pause and a resume in the meantime started a new run, which
    // has not lost anything. Runs on the executor thread without the model's monitor, which the threads that read the
    // change streams never take.
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

    // The causes too, since Spring Data wraps the driver's exception in one of its own
    private static boolean isChangeStreamHistoryLost(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof MongoCommandException mongoCommandException && mongoCommandException.getErrorCode() == MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE) {
                return true;
            }
        }
        return false;
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

    /**
     * Takes the model's monitor itself while it cancels every subscription, and shuts the executor down without it.
     */
    public void shutdown() {
        synchronized (monitor) {
            shutdown = true;
            running = false;
            runningSubscriptions.keySet().forEach(model::cancelSubscription);
            runningSubscriptions.clear();
            // Cancelled too, so a question for the present still waiting for an executor thread isn't asked while the
            // executor shuts down below
            pausedSubscriptions.values().forEach(InternalSubscription::cancel);
            pausedSubscriptions.clear();
        }
        model.shutdownExecutor();
    }

    /**
     * Takes the model's monitor itself while it pauses every subscription, and waits for their running actions without
     * it.
     */
    public void stop() {
        List<RuntimeException> failures = new ArrayList<>();
        Map<String, InternalSubscription> paused;
        synchronized (monitor) {
            if (shutdown) {
                return;
            }
            running = false;
            // A copy of the keys, since pauseSubscription moves each id from runningSubscriptions to
            // pausedSubscriptions as it goes, and forEach over a map that its own callback changes can visit an entry
            // that has already moved, or miss one that has not. Every id is paused even when an earlier pause throws,
            // and the first failure is thrown once they all are
            pausedByAStop = new LinkedHashMap<>();
            try {
                for (String subscriptionId : new ArrayList<>(runningSubscriptions.keySet())) {
                    try {
                        model.pauseSubscription(subscriptionId);
                    } catch (RuntimeException e) {
                        failures.add(e);
                    }
                }
            } finally {
                paused = pausedByAStop;
                pausedByAStop = null;
            }
        }
        // One second for all of them together rather than one each
        long waitsUntil = System.nanoTime() + WAIT_FOR_A_RUNNING_ACTION.toNanos();
        paused.forEach((subscriptionId, internalSubscription) -> waitForTheActionOf(subscriptionId, internalSubscription, waitsUntil));
        if (!failures.isEmpty()) {
            RuntimeException first = failures.getFirst();
            failures.subList(1, failures.size()).forEach(first::addSuppressed);
            throw first;
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
        running = true;

        // Shares the same currentStartAt reference so a resume continues from the last change-stream document
        // read before the subscription was paused, not the original StartAt, unless repositionTo replaces it.
        InternalSubscription resumed = internalSubscription.replacedBy(repositionTo);
        pausedSubscriptions.remove(subscriptionId);
        runningSubscriptions.put(subscriptionId, resumed);
        try {
            model.execute(() -> runUntilStopped(subscriptionId, resumed));
        } catch (RejectedExecutionException e) {
            if (model.executorIsShutDown()) {
                pausedAgain(subscriptionId, internalSubscription, resumed);
                throw e;
            }
            log.warn("The executor has no thread for resumed subscription {}, so it is handed to the executor again until it takes it. The subscription counts as running meanwhile.", subscriptionId, e);
            Thread.ofVirtual().name("occurrent-resume-" + subscriptionId).start(() -> handToTheExecutorAgain(subscriptionId, resumed));
        } catch (RuntimeException e) {
            pausedAgain(subscriptionId, internalSubscription, resumed);
            throw e;
        }

        return model.subscription(subscriptionId, resumed.startedLatch);
    }

    private void pausedAgain(String subscriptionId, InternalSubscription paused, InternalSubscription resumed) {
        runningSubscriptions.remove(subscriptionId, resumed);
        pausedSubscriptions.put(subscriptionId, paused);
    }

    // With a backoff, until the executor takes the run, a pause or a cancel closes it, or the model or the executor
    // shuts down. A resume can come from a lease handover or from start(true), which nothing repeats, so a rejection
    // thrown to its caller would leave the subscription paused for good. A run the executor never took stops
    // start(true) from waiting for it
    private void handToTheExecutorAgain(String subscriptionId, InternalSubscription resumed) {
        AtomicBoolean taken = new AtomicBoolean();
        try {
            HAND_TO_THE_EXECUTOR_AGAIN.onError((info, e) -> {
                if (info.getAttemptNumber() % REJECTIONS_BETWEEN_WARNINGS == 0) {
                    log.warn("The executor still has no thread for resumed subscription {} after {} tries, so it is handed to the executor again.", subscriptionId, info.getAttemptNumber(), e);
                }
            }).execute(() -> {
                if (!resumed.isIntentionallyClosed() && !shutdown) {
                    model.execute(() -> runUntilStopped(subscriptionId, resumed));
                    taken.set(true);
                }
            }, e -> e instanceof RejectedExecutionException && !resumed.isIntentionallyClosed() && !shutdown && !model.executorIsShutDown());
        } catch (RuntimeException e) {
            log.debug("Stopped handing resumed subscription {} to the executor, since it was paused or cancelled, or the model or the executor shut down.", subscriptionId, e);
        } finally {
            if (!taken.get()) {
                resumed.stoppedRestarting();
            }
        }
    }

    /**
     * Pauses a subscription, and returns the wait for its action when that is running. The model runs the wait once it
     * has let go of its monitor, so a call for another subscription doesn't wait for the paused one's action. The wait
     * returns once the action has, or after a second, and at once when called from inside the action. While
     * {@link #stop()} pauses, the wait returns at once and stop() waits for all the actions together.
     */
    public Runnable pauseSubscription(String subscriptionId) {
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
        if (internalSubscription == null) {
            return () -> {
            };
        }
        // Paused whatever closing it ends in, so the subscription is never missing from both maps
        try {
            internalSubscription.close();
        } finally {
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
        Map<String, InternalSubscription> stopping = pausedByAStop;
        if (stopping != null) {
            stopping.put(subscriptionId, internalSubscription);
            return () -> {
            };
        }
        return () -> waitForTheActionOf(subscriptionId, internalSubscription, System.nanoTime() + WAIT_FOR_A_RUNNING_ACTION.toNanos());
    }

    private void waitForTheActionOf(String subscriptionId, InternalSubscription internalSubscription, long waitsUntil) {
        if (!internalSubscription.waitUntilNotDelivering(waitsUntil)) {
            log.debug("The action of subscription {} was still running when the pause stopped waiting for it.", subscriptionId);
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
        // Not a monitor, since a pause waits on it for the action to return
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition stoppedDelivering = lock.newCondition();
        // Set under lock once a resume has created the next run of the subscription
        private boolean replaced;
        // Set under lock by a cancel or a shutdown
        private boolean cancelled;
        // Read and written under lock. The cursor the current attempt delivers from, and the thread running the action
        // or handing over a quiet position right now.
        private @Nullable MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
        private @Nullable Thread delivering;

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

        // Under the same lock as movedUnlessReplacedTo, so an action of this run that returns after the pause waited
        // for it cannot move the position once the run that replaces it exists
        InternalSubscription replacedBy(@Nullable StartAt repositionTo) {
            lock.lock();
            try {
                replaced = true;
                if (repositionTo != null) {
                    currentStartAt.set(repositionTo);
                }
                return new InternalSubscription(log, currentStartAt, action, pipeline, presentAtSubscribe, firstStartedLatch);
            } finally {
                lock.unlock();
            }
        }

        // False when this was closed while the change stream opened, and the caller then closes the cursor itself
        boolean opened(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
            lock.lock();
            try {
                if (intentionallyClosed) {
                    return false;
                }
                this.cursor = cursor;
                return true;
            } finally {
                lock.unlock();
            }
        }

        // False when this run was closed, and what the read returned is then left to a resume. Under the same lock as
        // close(), so a pause that saw nothing being delivered is never followed by a delivery on this run.
        boolean startDelivering() {
            lock.lock();
            try {
                if (intentionallyClosed) {
                    return false;
                }
                delivering = Thread.currentThread();
                return true;
            } finally {
                lock.unlock();
            }
        }

        void stoppedDelivering() {
            lock.lock();
            try {
                delivering = null;
                stoppedDelivering.signalAll();
            } finally {
                lock.unlock();
            }
        }

        // False once a resume has created the next run or a cancel has ended the subscription. A pause alone leaves it
        // true, since the subscription is still on this run until it is resumed
        boolean isCurrent() {
            lock.lock();
            try {
                return !replaced && !cancelled;
            } finally {
                lock.unlock();
            }
        }

        void movedUnlessReplacedTo(StartAt position) {
            lock.lock();
            try {
                if (!replaced) {
                    currentStartAt.set(position);
                }
            } finally {
                lock.unlock();
            }
        }

        // False when this run was closed, and the position is then left as it is
        boolean movedWhileOpenTo(StartAt position) {
            lock.lock();
            try {
                if (intentionallyClosed) {
                    return false;
                }
                currentStartAt.set(position);
                return true;
            } finally {
                lock.unlock();
            }
        }

        void started() {
            startedLatch.countDown();
            firstStartedLatch.countDown();
        }

        void stopped() {
            lock.lock();
            try {
                cursor = null;
            } finally {
                lock.unlock();
            }
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

        // Only for what is being delivered, not for a read waiting on the server, which returns on its own once
        // maxAwaitTime has passed and then delivers nothing since this run is closed. Not when the action itself
        // pauses, since it cannot return while it waits. An interrupt doesn't end the wait, since the pause must finish
        // moving the subscription, and is set again on the thread afterwards
        boolean waitUntilNotDelivering(long deadlineNanos) {
            boolean interrupted = false;
            lock.lock();
            try {
                if (delivering == Thread.currentThread()) {
                    return true;
                }
                while (delivering != null) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    try {
                        stoppedDelivering.awaitNanos(remaining);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                return true;
            } finally {
                lock.unlock();
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // Unlike a pause, a cancel or a shutdown also stops an outstanding question for the present, since nothing
        // opens after it
        void cancel() {
            lock.lock();
            try {
                cancelled = true;
            } finally {
                lock.unlock();
            }
            close();
            presentAtSubscribe.cancel();
        }

        // Marked before closing so the change-stream error this deliberately triggers is recognized as benign
        // (pause/cancel/shutdown) rather than an unexpected failure that should restart the subscription.
        void close() {
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> openCursor;
            lock.lock();
            try {
                intentionallyClosed = true;
                openCursor = cursor;
            } finally {
                lock.unlock();
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
