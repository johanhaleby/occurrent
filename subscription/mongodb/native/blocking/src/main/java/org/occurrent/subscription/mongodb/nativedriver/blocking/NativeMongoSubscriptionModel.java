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
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterbsonfilterconversion.internal.FilterToBsonFilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;
import org.occurrent.subscription.mongodb.internal.DocumentAdapter;
import org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.match;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.cannotFindGlobalSubscriptionPositionErrorMessage;

/**
 * This is a subscription that uses the "native" MongoDB Java driver (sync) to listen to changes from the event store.
 * This Subscription doesn't maintain the subscription position, you need to store it in order to continue the stream
 * from where it's left off on application restart/crash etc. You can do this yourself or use a
 * <a href="https://occurrent.org/documentation#blocking-subscription-position-storage">subscription position storage implementation</a>
 * or use the {@code DurableSubscriptionModel} utility from the {@code org.occurrent:durable-subscription}
 * module.
 */
public class NativeMongoSubscriptionModel implements PositionAwareSubscriptionModel {
    private static final Logger log = LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);

    private final MongoCollection<Document> eventCollection;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions;
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions;
    private final TimeRepresentation timeRepresentation;
    private final ExecutorService cloudEventDispatcher;
    private final RetryStrategy retryStrategy;
    private final MongoDatabase database;

    private volatile boolean shutdown = false;
    private volatile boolean running = true;

    private final Predicate<Throwable> NOT_SHUTDOWN = __ -> !shutdown;

    /**
     * Create a subscription using the native MongoDB sync driver. It will by default use a {@link RetryStrategy} for retries,
     * with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the subscription position.
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
        requireNonNull(database, MongoDatabase.class.getSimpleName() + " cannot be null");
        requireNonNull(eventCollection, "Event collection cannot be null");
        requireNonNull(timeRepresentation, "Time representation cannot be null");
        requireNonNull(subscriptionExecutor, "CloudEventDispatcher cannot  be null");
        requireNonNull(retryStrategy, "RetryStrategy cannot be null");
        this.database = database;
        this.retryStrategy = retryStrategy;
        this.cloudEventDispatcher = subscriptionExecutor;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.runningSubscriptions = new ConcurrentHashMap<>();
        this.pausedSubscriptions = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }

        CountDownLatch subscriptionStartedLatch = new CountDownLatch(1);

        Runnable internalSubscription = () -> newInternalSubscription(subscriptionId, filter, startAt, action, subscriptionStartedLatch);

        if (shutdown || cloudEventDispatcher.isShutdown() || cloudEventDispatcher.isTerminated()) {
            throw new IllegalStateException("Cannot start subscription because the executor is shutdown or terminated.");
        }
        startSubscription(internalSubscription);
        return new NativeMongoSubscription(subscriptionId, subscriptionStartedLatch);
    }

    private void startSubscription(Runnable internalSubscription) {
        cloudEventDispatcher.execute(executeWithRetry(internalSubscription, NOT_SHUTDOWN, retryStrategy));
    }

    private void newInternalSubscription(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, CountDownLatch subscriptionStartedLatch) {
        List<Bson> pipeline = createPipeline(timeRepresentation, filter);
        ChangeStreamIterable<Document> changeStreamDocuments = eventCollection.watch(pipeline, Document.class);
        ChangeStreamIterable<Document> changeStreamDocumentsAtPosition = MongoCommons.applyStartPosition(changeStreamDocuments, ChangeStreamIterable::startAfter, ChangeStreamIterable::startAtOperationTime, startAt.get());
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamDocumentsAtPosition.cursor();

        InternalSubscription internalSubscription = new InternalSubscription(cursor, startAt, action, filter, subscriptionStartedLatch);

        if (running) {
            runningSubscriptions.put(subscriptionId, internalSubscription);
        } else {
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }

        internalSubscription.started();

        try {
            cursor.forEachRemaining(changeStreamDocument -> MongoCloudEventsToJsonDeserializer.deserializeToCloudEvent(changeStreamDocument, timeRepresentation)
                    .map(cloudEvent -> new PositionAwareCloudEvent(cloudEvent, new MongoResumeTokenSubscriptionPosition(changeStreamDocument.getResumeToken())))
                    .ifPresent(executeWithRetry(action, NOT_SHUTDOWN, retryStrategy)));
        } catch (MongoException e) {
            log.debug("Caught {} (code={}, message={}), this might happen when cursor is shutdown.", e.getClass().getName(), e.getCode(), e.getMessage(), e);
        } catch (IllegalStateException e) {
            log.debug("Caught {} (message={}), this might happen when cursor is shutdown.", e.getClass().getName(), e.getMessage(), e);
        } finally {
            internalSubscription.stopped();
        }
    }

    private static List<Bson> createPipeline(TimeRepresentation timeRepresentation, SubscriptionFilter filter) {
        final List<Bson> pipeline;
        if (filter == null) {
            pipeline = Collections.emptyList();
        } else if (filter instanceof OccurrentSubscriptionFilter) {
            Filter occurrentFilter = ((OccurrentSubscriptionFilter) filter).filter;
            Bson bson = FilterToBsonFilterConverter.convertFilterToBsonFilter(MongoFilterSpecification.FULL_DOCUMENT, timeRepresentation, occurrentFilter);
            pipeline = Collections.singletonList(match(bson));
        } else if (filter instanceof MongoFilterSpecification.MongoJsonFilterSpecification) {
            pipeline = Collections.singletonList(Document.parse(((MongoFilterSpecification.MongoJsonFilterSpecification) filter).getJson()));
        } else if (filter instanceof MongoFilterSpecification.MongoBsonFilterSpecification) {
            Bson[] aggregationStages = ((MongoFilterSpecification.MongoBsonFilterSpecification) filter).getAggregationStages();
            DocumentAdapter documentAdapter = new DocumentAdapter(MongoClientSettings.getDefaultCodecRegistry());
            pipeline = Stream.of(aggregationStages).map(aggregationStage -> {
                final Document result;
                if (aggregationStage instanceof Document) {
                    result = (Document) aggregationStage;
                } else if (aggregationStage instanceof BsonDocument) {
                    result = documentAdapter.fromBson((BsonDocument) aggregationStage);
                } else {
                    BsonDocument bsonDocument = aggregationStage.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry());
                    result = documentAdapter.fromBson(bsonDocument);
                }
                return result;
            }).collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("Invalid " + SubscriptionFilter.class.getSimpleName());
        }
        return pipeline;
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.close();
        }
        pausedSubscriptions.remove(subscriptionId);
    }

    @PreDestroy
    public synchronized void shutdown() {
        shutdown = true;
        running = false;
        runningSubscriptions.keySet().forEach(this::cancelSubscription);
        runningSubscriptions.clear();
        pausedSubscriptions.clear();
        ExecutorShutdown.shutdownSafely(cloudEventDispatcher, 5, TimeUnit.SECONDS);
    }

    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        BsonTimestamp currentOperationTime;
        try {
            // Note that we increase the "increment" by 1 in order to not clash with an existing event in the event store.
            // This is so that we can avoid duplicates in certain rare cases when replaying events.
            currentOperationTime = MongoCommons.getServerOperationTime(database.runCommand(new Document("hostInfo", 1)), 1);
        } catch (MongoCommandException e) {
            log.warn(cannotFindGlobalSubscriptionPositionErrorMessage(e));
            // This can if the server doesn't allow to get the operation time since "db.adminCommand( { "hostInfo" : 1 } )" is prohibited.
            // This is the case on for example shared Atlas clusters. If this happens we return the current time of the client instead.
            return null;
        }
        return new MongoOperationTimeSubscriptionPosition(currentOperationTime);
    }


    @Override
    public synchronized void stop() {
        if (!shutdown) {
            running = false;
            runningSubscriptions.forEach((subscriptionId, __) -> pauseSubscription(subscriptionId));
        }
    }

    @Override
    public synchronized void start() {
        if (!shutdown) {
            running = true;
            pausedSubscriptions.forEach((subscriptionId, internalSubscription) -> resumeSubscription(subscriptionId).waitUntilStarted());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(SubscriptionModel.class.getSimpleName() + " is shutdown");
        } else if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already running");
        }

        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }

        running = true;

        CountDownLatch startedLatch = new CountDownLatch(1);
        Runnable newSubscription = () -> newInternalSubscription(subscriptionId, internalSubscription.filter,
                internalSubscription.startAt, internalSubscription.action, startedLatch);
        startSubscription(newSubscription);

        return new NativeMongoSubscription(subscriptionId, startedLatch);
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(SubscriptionModel.class.getSimpleName() + " is shutdown");
        } else if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already paused");
        } else if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
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

    private static class InternalSubscription {
        private final SubscriptionFilter filter;
        final CountDownLatch startedLatch;
        final CountDownLatch stoppedLatch;
        final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
        final StartAt startAt;
        final Consumer<CloudEvent> action;

        private InternalSubscription(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor, StartAt startAtSupplier, Consumer<CloudEvent> action, SubscriptionFilter filter, CountDownLatch startedLatch) {
            this.filter = filter;
            this.startedLatch = startedLatch;
            this.cursor = cursor;
            this.startAt = startAtSupplier;
            this.action = action;
            this.stoppedLatch = new CountDownLatch(1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InternalSubscription)) return false;
            InternalSubscription that = (InternalSubscription) o;
            return Objects.equals(filter, that.filter) && Objects.equals(startedLatch, that.startedLatch) && Objects.equals(stoppedLatch, that.stoppedLatch) && Objects.equals(cursor, that.cursor) && Objects.equals(startAt, that.startAt) && Objects.equals(action, that.action);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter, startedLatch, stoppedLatch, cursor, startAt, action);
        }

        void started() {
            startedLatch.countDown();
        }

        void stopped() {
            stoppedLatch.countDown();
        }

        public boolean waitUntilStopped(Duration duration) {
            try {
                return stoppedLatch.await(duration.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            try {
                cursor.close();
            } catch (Exception e) {
                log.error("Failed to cancel subscription, this might happen if Mongo connection has been shutdown", e);
            }
        }
    }
}