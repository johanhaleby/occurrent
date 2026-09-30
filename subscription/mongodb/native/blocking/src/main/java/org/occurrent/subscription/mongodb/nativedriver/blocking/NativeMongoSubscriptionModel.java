/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterbsonfilterconversion.internal.FilterToBsonFilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.DcbSubscriptionFilterConverter;
import org.occurrent.subscription.mongodb.internal.DocumentAdapter;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.match;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalCheckpointErrorMessage;

/**
 * This is a subscription that uses the "native" MongoDB Java driver (sync) to listen to changes from the event store.
 * This Subscription doesn't maintain the checkpoint, you need to store it in order to continue the stream
 * from where it's left off on application restart/crash etc. You can do this yourself or use a
 * <a href="https://occurrent.org/documentation#blocking-subscription-checkpoint-storage">checkpoint storage implementation</a>
 * or use the {@code DurableSubscriptionModel} utility from the {@code org.occurrent:durable-subscription}
 * module.
 */
@NullMarked
public class NativeMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions, RepositionableSubscriptions, HistoryRetainingSubscriptions, HistoryLossReportingSubscriptions {

    /**
     * Acknowledging costs nothing here. This model reads the event store's own change stream, so returning normally
     * advances a checkpoint and removes no event, and the store keeps its events on its own terms. An event erased
     * through {@code EventStoreOperations} is gone by that erasure rather than by the acknowledgement, so answering
     * {@code false} for it would only strand an instance on an event nobody can supply.
     */
    @Override
    public boolean retains(CloudEvent event) {
        return true;
    }

    @Override
    public boolean retainsEveryEvent() {
        return true;
    }

    private static final Logger log = LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);

    private final MongoCollection<Document> eventCollection;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions;
    private final List<HistoryLossListener> historyLossListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions;
    private final TimeRepresentation timeRepresentation;
    private final ExecutorService cloudEventDispatcher;
    private final RetryStrategy retryStrategy;
    private final boolean restartSubscriptionsOnChangeStreamHistoryLost;
    private final @Nullable Integer batchSize;
    private final @Nullable Duration maxAwaitTime;
    private final MongoDatabase database;

    private volatile boolean shutdown = false;
    private volatile boolean running = true;

    private final Predicate<Throwable> NOT_SHUTDOWN = __ -> !shutdown;
    // A refused checkpoint write must never be retried, on either retry loop below. The call sites already pass
    // their own predicate, which RetryExecution combines with the strategy's own.
    private static final Predicate<Throwable> NOT_A_REFUSED_CHECKPOINT_WRITE = e -> !(e instanceof CheckpointWriteConditionNotFulfilledException);
    private final Predicate<Throwable> RETRYABLE = NOT_SHUTDOWN.and(NOT_A_REFUSED_CHECKPOINT_WRITE);

    /**
     * Create a subscription using the native MongoDB sync driver. It will by default use a {@link RetryStrategy} for retries,
     * with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the checkpoint.
     *
     * @param database             The MongoDB database to use
     * @param eventCollectionName  The name of the collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     */
    public NativeMongoSubscriptionModel(MongoDatabase database, String eventCollectionName, TimeRepresentation timeRepresentation, ExecutorService subscriptionExecutor) {
        this(database, database.getCollection(requireNonNull(eventCollectionName, "Event collection cannot be null")), timeRepresentation, subscriptionExecutor,
                RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a subscription using the native MongoDB sync driver.
     *
     * @param database             The MongoDB database to use
     * @param eventCollectionName  The name of the collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     * @param retryStrategy        Configure how retries should be handled
     */
    public NativeMongoSubscriptionModel(MongoDatabase database, String eventCollectionName, TimeRepresentation timeRepresentation,
                                        ExecutorService subscriptionExecutor, RetryStrategy retryStrategy) {
        this(database, database.getCollection(requireNonNull(eventCollectionName, "Event collection cannot be null")), timeRepresentation, subscriptionExecutor, retryStrategy);
    }

    /**
     * Create a subscription using the native MongoDB sync driver.
     *
     * @param database             The MongoDB database to use
     * @param eventCollection      The collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     * @param retryStrategy        Configure how retries should be handled
     */
    public NativeMongoSubscriptionModel(MongoDatabase database, MongoCollection<Document> eventCollection, TimeRepresentation timeRepresentation,
                                        ExecutorService subscriptionExecutor, RetryStrategy retryStrategy) {
        this(database, eventCollection, timeRepresentation, subscriptionExecutor, NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(retryStrategy));
    }

    /**
     * Create a subscription using the native MongoDB sync driver.
     *
     * @param database             The MongoDB database to use
     * @param eventCollectionName  The name of the collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     * @param config               Configure how the subscription model should behave, for example retries and how to handle change stream history lost errors.
     */
    public NativeMongoSubscriptionModel(MongoDatabase database, String eventCollectionName, TimeRepresentation timeRepresentation,
                                        ExecutorService subscriptionExecutor, NativeMongoSubscriptionModelConfig config) {
        this(database, database.getCollection(requireNonNull(eventCollectionName, "Event collection cannot be null")), timeRepresentation, subscriptionExecutor, config);
    }

    /**
     * Create a subscription using the native MongoDB sync driver.
     *
     * @param database             The MongoDB database to use
     * @param eventCollection      The collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     * @param config               Configure how the subscription model should behave, for example retries and how to handle change stream history lost errors.
     */
    public NativeMongoSubscriptionModel(MongoDatabase database, MongoCollection<Document> eventCollection, TimeRepresentation timeRepresentation,
                                        ExecutorService subscriptionExecutor, NativeMongoSubscriptionModelConfig config) {
        requireNonNull(database, MongoDatabase.class.getSimpleName() + " cannot be null");
        requireNonNull(eventCollection, "Event collection cannot be null");
        requireNonNull(timeRepresentation, "Time representation cannot be null");
        requireNonNull(subscriptionExecutor, "subscriptionExecutor cannot be null");
        requireNonNull(config, NativeMongoSubscriptionModelConfig.class.getSimpleName() + " cannot be null");
        this.database = database;
        this.retryStrategy = config.retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = config.restartSubscriptionsOnChangeStreamHistoryLost;
        this.batchSize = config.batchSize;
        this.maxAwaitTime = config.maxAwaitTime;
        this.cloudEventDispatcher = subscriptionExecutor;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.runningSubscriptions = new ConcurrentHashMap<>();
        this.pausedSubscriptions = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, startAt, action, false);
    }

    /**
     * Holds the subscription paused as a subscription made while this model is stopped, so its change stream opens on
     * {@link #resumeSubscription(String)} or {@link #start(boolean) start(true)}. A subscription started at the present
     * is delivered the events written from this call on.
     */
    @Override
    public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, startAt, action, true);
    }

    private Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (isKnown(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }

        // Built here rather than on the dispatcher thread, so a filter this model cannot apply is refused to the caller
        // instead of failing where nobody is listening. It used to be built inside the Runnable below, which left the
        // caller holding a subscription that never delivered while the retry wrapper re-threw the same
        // IllegalArgumentException forever. SpringMongoSubscriptionModel always refused it here.
        List<Bson> pipeline = createPipeline(timeRepresentation, filter);
        // The start position, for the same reason and with the same history. A checkpoint this model cannot parse used
        // to fail down in newInternalSubscription on the dispatcher thread, where the retry wrapper re-threw it forever
        // and the caller was left holding a subscription whose latch never counted down. A dynamic position is a no-op
        // in there, for a reason checkStartPosition documents.
        MongoCommons.checkStartPosition(startAt, new SubscriptionModelContext(NativeMongoSubscriptionModel.class));

        if (shutdown || cloudEventDispatcher.isShutdown() || cloudEventDispatcher.isTerminated()) {
            throw new IllegalStateException("Cannot start subscription because the executor is shutdown or terminated.");
        }

        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(startAt);
        PresentAtSubscribe presentAtSubscribe = new PresentAtSubscribe(cancelled -> recordThePresent(subscriptionId, currentStartAt, cancelled));
        InternalSubscription internalSubscription = new InternalSubscription(currentStartAt, action, pipeline, presentAtSubscribe);
        // Known from here on rather than once its change stream opens, so a pause or a cancel reaches it while MongoDB
        // cannot be reached, and a wrapper asking which subscriptions this model runs gets the right answer. On a
        // running model a run that opens at the present asks for it first.
        if (running && !holdPaused) {
            runningSubscriptions.put(subscriptionId, internalSubscription);
            startSubscription(subscriptionId, internalSubscription, () -> runningSubscriptions.remove(subscriptionId, internalSubscription));
        } else {
            // Opens nothing until start() or a resume does, like a subscription that stop() paused. The present is
            // asked for on the dispatcher, so this returns without waiting for MongoDB, and a change stream that opens
            // at the answer waits for it.
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            try {
                cloudEventDispatcher.execute(presentAtSubscribe::ask);
            } catch (RuntimeException e) {
                pausedSubscriptions.remove(subscriptionId, internalSubscription);
                throw e;
            }
        }
        // Follows every run of the subscription, so it answers true once start() or a resume opens the change stream of
        // one subscribed while the model was stopped, or paused before its change stream opened
        return new NativeMongoSubscription(subscriptionId, internalSubscription.firstStartedLatch);
    }

    // Records the present for a subscription started at the present, so the events written from then on are delivered
    // to it whatever pause, stop, resume or start comes before its change stream opens. A position that isn't the
    // present is left as it is. Retried like opening a change stream, and a pause doesn't end it, so an outage that
    // ends while the subscription is paused loses nothing. A cancel or a shutdown stops it before the next attempt
    // rather than interrupting it, since the thread it runs on belongs to the dispatcher and runs other subscriptions.
    private void recordThePresent(String subscriptionId, AtomicReference<StartAt> currentStartAt, BooleanSupplier cancelled) {
        SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(NativeMongoSubscriptionModel.class);
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

    // Called once the subscription is registered, so forget(..) on the dispatcher thread always finds the run it removes
    private void startSubscription(String subscriptionId, InternalSubscription internalSubscription, Runnable unregister) {
        try {
            cloudEventDispatcher.execute(() -> runUntilStopped(subscriptionId, internalSubscription));
        } catch (RuntimeException e) {
            unregister.run();
            throw e;
        }
    }

    // Restarts with the retry strategy's backoff until the subscription is closed or the strategy gives up. A pause,
    // cancel or shutdown closes it, and a resume runs a new one, so a restart due after the backoff opens nothing for
    // a subscription that was paused or cancelled meanwhile, and its last error is logged rather than thrown. When the
    // strategy gives up, the last error is thrown on the dispatcher thread and the subscription stays known and
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
    // The try block spans opening the cursor too: a change-stream error (history lost, failover) can surface
    // there just as well as while iterating.
    private void newInternalSubscription(String subscriptionId, InternalSubscription internalSubscription) {
        if (internalSubscription.isIntentionallyClosed()) {
            return;
        }
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        Consumer<CloudEvent> action = internalSubscription.action;
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = null;
        try {
            ChangeStreamIterable<Document> changeStreamDocuments = eventCollection.watch(internalSubscription.pipeline, Document.class);
            if (batchSize != null) {
                changeStreamDocuments = changeStreamDocuments.batchSize(batchSize);
            }
            if (maxAwaitTime != null) {
                changeStreamDocuments = changeStreamDocuments.maxAwaitTime(maxAwaitTime.toMillis(), MILLISECONDS);
            }
            SubscriptionModelContext subscriptionModelContext = new SubscriptionModelContext(NativeMongoSubscriptionModel.class);
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
                // the strategy pause a subscription this model no longer knows about. Logged at error level because
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

    @Override
    public void addHistoryLossListener(HistoryLossListener listener) {
        requireNonNull(listener, HistoryLossListener.class.getSimpleName() + " cannot be null");
        historyLossListeners.add(listener);
    }

    @Override
    public void removeHistoryLossListener(HistoryLossListener listener) {
        requireNonNull(listener, HistoryLossListener.class.getSimpleName() + " cannot be null");
        historyLossListeners.remove(listener);
    }

    // Only while the subscription is still on this run. A pause and a resume in the meantime started a new run, which
    // has not lost anything. Runs on the dispatcher thread without this model's lock, because a pause holds that lock
    // while it waits for the dispatcher to stop.
    private void forget(String subscriptionId, InternalSubscription internalSubscription) {
        runningSubscriptions.remove(subscriptionId, internalSubscription);
    }

    private @Nullable BsonTimestamp currentOperationTime() {
        Document reply = database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND);
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

    private static List<Bson> createPipeline(TimeRepresentation timeRepresentation, @Nullable SubscriptionFilter filter) {
        final List<Bson> pipeline;
        if (filter == null) {
            pipeline = Collections.emptyList();
        } else if (filter instanceof StreamSubscriptionFilter streamSubscriptionFilter) {
            Filter streamFilter = streamSubscriptionFilter.filter();
            Bson bson = FilterToBsonFilterConverter.convertFilterToBsonFilter(MongoFilterSpecification.FULL_DOCUMENT, timeRepresentation, streamFilter);
            pipeline = Collections.singletonList(match(bson));
        } else if (filter instanceof AgnosticSubscriptionFilter agnosticSubscriptionFilter) {
            // Capability-agnostic: the change stream applies the plain Filter, the same as a stream filter. The stream
            // versus DCB scoping lives in the catch-up layer, not here.
            Filter agnosticFilter = agnosticSubscriptionFilter.filter();
            Bson bson = FilterToBsonFilterConverter.convertFilterToBsonFilter(MongoFilterSpecification.FULL_DOCUMENT, timeRepresentation, agnosticFilter);
            pipeline = Collections.singletonList(match(bson));
        } else if (filter instanceof DcbSubscriptionFilter dcbSubscriptionFilter) {
            pipeline = Collections.singletonList(DcbSubscriptionFilterConverter.toChangeStreamMatchStage(dcbSubscriptionFilter.criteria()));
        } else if (filter instanceof MongoFilterSpecification.MongoJsonFilterSpecification jsonFilterSpecification) {
            pipeline = Collections.singletonList(Document.parse(jsonFilterSpecification.getJson()));
        } else if (filter instanceof MongoFilterSpecification.MongoBsonFilterSpecification bsonFilterSpecification) {
            Bson[] aggregationStages = bsonFilterSpecification.getAggregationStages();
            DocumentAdapter documentAdapter = new DocumentAdapter(MongoClientSettings.getDefaultCodecRegistry());
            pipeline = Stream.of(aggregationStages).map(aggregationStage -> {
                return switch (aggregationStage) {
                    case Document document -> document;
                    case BsonDocument bsonDocument -> documentAdapter.fromBson(bsonDocument);
                    default -> {
                        BsonDocument bsonDocument = aggregationStage.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry());
                        yield documentAdapter.fromBson(bsonDocument);
                    }
                };
            }).collect(Collectors.toList());
        } else {
            throw new UnsupportedSubscriptionFilterException(filter.getClass());
        }
        return pipeline;
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.cancel();
        }
        InternalSubscription pausedSubscription = pausedSubscriptions.remove(subscriptionId);
        if (pausedSubscription != null) {
            pausedSubscription.cancel();
        }
    }

    @PreDestroy
    public synchronized void shutdown() {
        shutdown = true;
        running = false;
        runningSubscriptions.keySet().forEach(this::cancelSubscription);
        runningSubscriptions.clear();
        // Cancelled too, so a question for the present still waiting for a dispatcher thread isn't asked while the
        // executor shuts down below
        pausedSubscriptions.values().forEach(InternalSubscription::cancel);
        pausedSubscriptions.clear();
        ExecutorShutdown.shutdownSafely(cloudEventDispatcher, 5, TimeUnit.SECONDS);
    }

    @Override
    @Nullable
    public Checkpoint globalCheckpoint() {
        BsonTimestamp currentOperationTime;
        try {
            // Increment by 1 to avoid clashing with an existing event, preventing duplicates in rare replay cases.
            currentOperationTime = MongoCommons.getServerOperationTime(database.runCommand(new Document("hostInfo", 1)), 1);
        } catch (MongoCommandException e) {
            log.warn(cannotFindGlobalCheckpointErrorMessage(e));
            // Happens when the server prohibits "hostInfo" (e.g. shared Atlas clusters). Null is the contract's
            // answer for a problem this model cannot resolve.
            return null;
        }
        return new MongoOperationTimeCheckpoint(currentOperationTime);
    }


    @Override
    public synchronized void stop() {
        if (!shutdown) {
            running = false;
            // Snapshot the keys before iterating: pauseSubscription moves each id from runningSubscriptions to
            // pausedSubscriptions as it goes, and forEach over a map that its own callback mutates can visit an entry
            // that has already moved, or miss one that has not. Mirrors the reactor twin.
            new ArrayList<>(runningSubscriptions.keySet()).forEach(this::pauseSubscription);
        }
    }

    /**
     * Start the model and, when {@code resumeSubscriptionsAutomatically} is {@code true}, resume every paused
     * subscription through {@link #resumeSubscription(String)} and wait until the {@link Subscription} each call
     * returns has started. Pausing, cancelling and listing subscriptions keep working while it waits.
     * <p>
     * The wait for a subscription ends early when it's paused or cancelled, when the model shuts down, or when its
     * change stream won't open at all, because its {@code RetryStrategy} gave up or its change stream history was lost
     * with {@code restartSubscriptionsOnChangeStreamHistoryLost} turned off. Until one of those happens, a returned
     * {@code Subscription} that never answers that it has started keeps this method waiting. A subscription that isn't
     * running once {@code resumeSubscription(..)} returns, such as when an override doesn't call the super method,
     * isn't waited for. A subscription that an override already resumed or cancelled while resuming an earlier one is
     * left out.
     * <p>
     * When a resume or a wait throws, every other subscription is still resumed and waited for, and then this method
     * throws the first exception with the others added as suppressed.
     * <p>
     * A subscription made while the model is stopped is paused until this call or a resume starts it. For one given
     * {@link StartAt#now()}, no position, or a {@code StartAt.dynamic(..)} position that resolves to one of those,
     * {@code subscribe(..)} asks MongoDB for its operation time on the dispatcher and returns without waiting for the
     * answer. The subscription starts at that time, so its position is fixed when MongoDB answers, shortly after
     * {@code subscribe(..)} returns, and an event written before then isn't delivered to it. To be sure an event is
     * delivered, call this method and wait for the subscription's {@link Subscription#waitUntilStarted()} before
     * writing it. This method waits for the answer when it opens the change stream at it. While MongoDB can't be
     * reached, the question is retried with the model's {@code RetryStrategy}. When that strategy gives up, the
     * give-up can keep the change stream from opening, as a give-up on opening it does, and this method returns.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        Map<String, ResumedByStart> resumed = new LinkedHashMap<>();
        List<RuntimeException> failures = new ArrayList<>();
        synchronized (this) {
            if (shutdown) {
                return;
            }
            running = true;
            if (resumeSubscriptionsAutomatically) {
                // Same snapshot reasoning as stop(): resumeSubscription moves each id out of pausedSubscriptions as it
                // goes, so iterating the live map here would be exposed to the same hazard.
                for (String subscriptionId : new ArrayList<>(pausedSubscriptions.keySet())) {
                    // An override may already have resumed or cancelled this subscription while resuming an earlier one
                    if (!pausedSubscriptions.containsKey(subscriptionId)) {
                        continue;
                    }
                    try {
                        Subscription subscription = resumeSubscription(subscriptionId);
                        // Only the dispatcher writes to this map without the lock, and it only removes a run whose
                        // history was lost, so this is the run the resume started, or nothing when there is nothing
                        // to wait for
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
        // Waited for outside the lock, so pause, cancel and subscriptionIds() answer while a change stream cannot open
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

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Synchronized because a subscription moves between the two maps in two steps, so an unsynchronized reader can
     * land between them and miss an id that exists. It also keeps a caller from seeing the ids of a model that
     * {@link #shutdown()} has already flagged as shut down but not yet cleared.
     */
    @Override
    public synchronized Set<String> subscriptionIds() {
        return Stream.concat(runningSubscriptions.keySet().stream(), pausedSubscriptions.keySet().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
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
     * Resume a paused subscription from the change-stream position it had read to, so that nothing written while it
     * was paused is lost.
     * <p>
     * Delivery is <i>at least once</i> across a pause: an event whose handler had not finished when the subscription
     * was paused, and every event another consumer of the same subscription id handled in the meantime, is handed to
     * this handler again on resume. That is deliberate, since wasted work is the cheaper mistake, and it means
     * handlers must be idempotent. For a subscription started at the present, this model asks MongoDB for its
     * operation time on the dispatcher once {@code subscribe(..)} has registered it, and records the answer as that
     * subscription's position before its change stream opens. A pause doesn't stop the question, so one paused before
     * it handled any event, even before MongoDB answered, resumes from that time, and the events written since are
     * delivered too, as long as the oplog still holds that time. When it no longer does, the resume gets the handling
     * that {@code restartSubscriptionsOnChangeStreamHistoryLost} configures. When MongoDB's reply has no operation
     * time, nothing is recorded and the resume opens at the present.
     * <p>
     * A subscription whose {@code RetryStrategy} gave up opening its change stream still counts as running, and pausing
     * it and then resuming it starts it again. When the strategy gives up asking MongoDB for its operation time, the
     * give-up is logged, and it can keep a change stream from opening, thrown on the dispatcher like a give-up on
     * opening it. Pausing and resuming the subscription after that starts it again.
     * <p>
     * That is what this call does on its own. A {@code DurableSubscriptionModel} wrapping this model calls
     * {@link #resumeSubscription(String, StartAt)} with a stored checkpoint instead whenever one exists, so a
     * subscription reached that way can resume somewhere else entirely, for example the position a competing
     * consumer's other node advanced to while this one held no lease.
     *
     * @see #pauseSubscription(String)
     * @see #resumeSubscription(String, StartAt)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        return doResumeSubscription(subscriptionId, null);
    }

    /**
     * Resume a paused subscription at {@code startAt}, instead of the change-stream position it had read to.
     *
     * @see RepositionableSubscriptions#resumeSubscription(String, StartAt)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        MongoCommons.checkStartPosition(startAt, new SubscriptionModelContext(NativeMongoSubscriptionModel.class));
        return doResumeSubscription(subscriptionId, startAt);
    }

    // The shared resume path. repositionTo is the caller's explicit position from the two-arg overload, or null
    // from the one-arg overload, in which case the subscription's own currentStartAt reference is left untouched
    // and the resume continues from whatever it already holds.
    private Subscription doResumeSubscription(String subscriptionId, @Nullable StartAt repositionTo) {
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
        // read before the subscription was paused, not the original StartAt, unless repositionTo overrode it above.
        InternalSubscription resumed = internalSubscription.resumed();
        pausedSubscriptions.remove(subscriptionId);
        runningSubscriptions.put(subscriptionId, resumed);
        startSubscription(subscriptionId, resumed, () -> {
            runningSubscriptions.remove(subscriptionId, resumed);
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        });

        return new NativeMongoSubscription(subscriptionId, resumed.startedLatch);
    }

    /**
     * Pause an individual subscription. The change stream behind it is closed, but the position it has read to is
     * kept, so {@link #resumeSubscription(String)} continues from there and events written while it was paused are
     * delivered rather than skipped.
     *
     * @see #resumeSubscription(String)
     */
    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
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
    // asked on the dispatcher right after subscribe(..) on a stopped model, or by the first run that gets to it. A pause
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

        // Without an interrupt, since the question runs on a dispatcher thread that a later task reuses, and some
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

        private InternalSubscription(AtomicReference<StartAt> currentStartAt, Consumer<CloudEvent> action, List<Bson> pipeline, PresentAtSubscribe presentAtSubscribe) {
            this(currentStartAt, action, pipeline, presentAtSubscribe, new CountDownLatch(1));
        }

        private InternalSubscription(AtomicReference<StartAt> currentStartAt, Consumer<CloudEvent> action, List<Bson> pipeline, PresentAtSubscribe presentAtSubscribe, CountDownLatch firstStartedLatch) {
            this.pipeline = pipeline;
            this.currentStartAt = currentStartAt;
            this.presentAtSubscribe = presentAtSubscribe;
            this.action = action;
            this.firstStartedLatch = firstStartedLatch;
        }

        InternalSubscription resumed() {
            return new InternalSubscription(currentStartAt, action, pipeline, presentAtSubscribe, firstStartedLatch);
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

        public boolean waitUntilStopped(Duration duration) {
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
        public void close() {
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
        return new StringJoiner(", ", NativeMongoSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("eventCollection=" + eventCollection)
                .add("runningSubscriptions=" + runningSubscriptions)
                .add("pausedSubscriptions=" + pausedSubscriptions)
                .add("timeRepresentation=" + timeRepresentation)
                .add("cloudEventDispatcher=" + cloudEventDispatcher)
                .add("retryStrategy=" + retryStrategy)
                .add("database=" + database)
                .add("shutdown=" + shutdown)
                .add("running=" + running)
                .add("NOT_SHUTDOWN=" + NOT_SHUTDOWN)
                .toString();
    }
}
