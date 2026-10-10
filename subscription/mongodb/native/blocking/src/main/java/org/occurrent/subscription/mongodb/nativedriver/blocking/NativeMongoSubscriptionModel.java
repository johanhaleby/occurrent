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
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.blocking.changestream.internal.ChangeStreamSubscriptions;
import org.occurrent.subscription.mongodb.internal.DcbSubscriptionFilterConverter;
import org.occurrent.subscription.mongodb.internal.DocumentAdapter;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.match;
import static java.util.Objects.requireNonNull;
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
public class NativeMongoSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions, RepositionableSubscriptions, HistoryRetainingSubscriptions, HistoryLossReportingSubscriptions, QuietPositionReportingSubscriptions {

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
    private final TimeRepresentation timeRepresentation;
    private final ExecutorService cloudEventDispatcher;
    private final MongoDatabase database;
    private final ChangeStreamSubscriptions subscriptions;

    /**
     * Create a subscription using the native MongoDB sync driver. It will by default use a {@link RetryStrategy} for retries,
     * with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the checkpoint.
     *
     * @param database             The MongoDB database to use
     * @param eventCollectionName  The name of the collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     *                             A pause or a stop returns without waiting for a read that is waiting on the server, so the thread of a paused
     *                             subscription can stay busy for up to the change stream's {@code maxAwaitTime} after it returns, and for as long as an
     *                             action still runs once the pause has stopped waiting for it. So the model needs a thread for each running
     *                             subscription, and one more for each closed run that is still reading or still running its action. Every pause and
     *                             resume of a subscription, and every stop and start of the model, can add such a run, so no fixed number of threads
     *                             is always enough. When an executor with a fixed number of threads has no thread free for a resume or a start, the
     *                             subscription counts as running and is handed to the executor again, 100 ms and then up to 2 seconds apart, until a
     *                             thread is free, the subscription is paused or cancelled, or the model shuts down. So a smaller executor delays the
     *                             resume rather than leaving the subscription paused. A subscribe the executor has no thread for throws.
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
     *                             A pause or a stop returns without waiting for a read that is waiting on the server, so the thread of a paused
     *                             subscription can stay busy for up to the change stream's {@code maxAwaitTime} after it returns, and for as long as an
     *                             action still runs once the pause has stopped waiting for it. So the model needs a thread for each running
     *                             subscription, and one more for each closed run that is still reading or still running its action. Every pause and
     *                             resume of a subscription, and every stop and start of the model, can add such a run, so no fixed number of threads
     *                             is always enough. When an executor with a fixed number of threads has no thread free for a resume or a start, the
     *                             subscription counts as running and is handed to the executor again, 100 ms and then up to 2 seconds apart, until a
     *                             thread is free, the subscription is paused or cancelled, or the model shuts down. So a smaller executor delays the
     *                             resume rather than leaving the subscription paused. A subscribe the executor has no thread for throws.
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
     *                             A pause or a stop returns without waiting for a read that is waiting on the server, so the thread of a paused
     *                             subscription can stay busy for up to the change stream's {@code maxAwaitTime} after it returns, and for as long as an
     *                             action still runs once the pause has stopped waiting for it. So the model needs a thread for each running
     *                             subscription, and one more for each closed run that is still reading or still running its action. Every pause and
     *                             resume of a subscription, and every stop and start of the model, can add such a run, so no fixed number of threads
     *                             is always enough. When an executor with a fixed number of threads has no thread free for a resume or a start, the
     *                             subscription counts as running and is handed to the executor again, 100 ms and then up to 2 seconds apart, until a
     *                             thread is free, the subscription is paused or cancelled, or the model shuts down. So a smaller executor delays the
     *                             resume rather than leaving the subscription paused. A subscribe the executor has no thread for throws.
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
     *                             A pause or a stop returns without waiting for a read that is waiting on the server, so the thread of a paused
     *                             subscription can stay busy for up to the change stream's {@code maxAwaitTime} after it returns, and for as long as an
     *                             action still runs once the pause has stopped waiting for it. So the model needs a thread for each running
     *                             subscription, and one more for each closed run that is still reading or still running its action. Every pause and
     *                             resume of a subscription, and every stop and start of the model, can add such a run, so no fixed number of threads
     *                             is always enough. When an executor with a fixed number of threads has no thread free for a resume or a start, the
     *                             subscription counts as running and is handed to the executor again, 100 ms and then up to 2 seconds apart, until a
     *                             thread is free, the subscription is paused or cancelled, or the model shuts down. So a smaller executor delays the
     *                             resume rather than leaving the subscription paused. A subscribe the executor has no thread for throws.
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
     *                             A pause or a stop returns without waiting for a read that is waiting on the server, so the thread of a paused
     *                             subscription can stay busy for up to the change stream's {@code maxAwaitTime} after it returns, and for as long as an
     *                             action still runs once the pause has stopped waiting for it. So the model needs a thread for each running
     *                             subscription, and one more for each closed run that is still reading or still running its action. Every pause and
     *                             resume of a subscription, and every stop and start of the model, can add such a run, so no fixed number of threads
     *                             is always enough. When an executor with a fixed number of threads has no thread free for a resume or a start, the
     *                             subscription counts as running and is handed to the executor again, 100 ms and then up to 2 seconds apart, until a
     *                             thread is free, the subscription is paused or cancelled, or the model shuts down. So a smaller executor delays the
     *                             resume rather than leaving the subscription paused. A subscribe the executor has no thread for throws.
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
        this.cloudEventDispatcher = subscriptionExecutor;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.subscriptions = new ChangeStreamSubscriptions(this, new ChangeStreams(), NativeMongoSubscriptionModel.class, log, timeRepresentation, config.retryStrategy,
                config.restartSubscriptionsOnChangeStreamHistoryLost, config.batchSize, config.maxAwaitTime, true);
    }

    // What the subscriptions ask of this model. The three calls at the end go to this model's own methods, so a
    // subclass that overrides one also sees the calls start(..), stop() and shutdown() make.
    private final class ChangeStreams implements ChangeStreamSubscriptions.Model {
        @Override
        public ChangeStreamIterable<Document> watch(List<Bson> pipeline) {
            return eventCollection.watch(pipeline, Document.class);
        }

        @Override
        public Document runCommand(Document command) {
            return database.runCommand(command);
        }

        @Override
        public void execute(Runnable task) {
            cloudEventDispatcher.execute(task);
        }

        @Override
        public boolean executorIsShutDown() {
            return cloudEventDispatcher.isShutdown() || cloudEventDispatcher.isTerminated();
        }

        @Override
        public void shutdownExecutor() {
            ExecutorShutdown.shutdownSafely(cloudEventDispatcher, 5, TimeUnit.SECONDS);
        }

        @Override
        public Subscription subscription(String subscriptionId, CountDownLatch started) {
            return new NativeMongoSubscription(subscriptionId, started);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return NativeMongoSubscriptionModel.this.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            NativeMongoSubscriptionModel.this.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            NativeMongoSubscriptionModel.this.cancelSubscription(subscriptionId);
        }
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
        return subscriptions.subscribe(subscriptionId, () -> createPipeline(timeRepresentation, filter), startAt, action, holdPaused);
    }

    @Override
    public void addHistoryLossListener(HistoryLossListener listener) {
        subscriptions.addHistoryLossListener(listener);
    }

    @Override
    public void removeHistoryLossListener(HistoryLossListener listener) {
        subscriptions.removeHistoryLossListener(listener);
    }

    @Override
    public void addQuietPositionListener(QuietPositionListener listener) {
        subscriptions.addQuietPositionListener(listener);
    }

    @Override
    public void removeQuietPositionListener(QuietPositionListener listener) {
        subscriptions.removeQuietPositionListener(listener);
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
        subscriptions.cancelSubscription(subscriptionId);
    }

    // Takes the monitor itself while it cancels, and waits for the executor without it
    @PreDestroy
    public void shutdown() {
        subscriptions.shutdown();
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

    /**
     * Opens a change stream at {@code checkpoint} and closes it again. Answers {@code false} when MongoDB refuses to
     * open it because the oplog no longer reaches back to {@code checkpoint}.
     */
    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return MongoCommons.canResumeFrom(eventCollection.getNamespace().getCollectionName(), checkpoint, database::runCommand);
    }


    // Takes the monitor itself while it pauses, and waits for the running actions without it
    @Override
    public void stop() {
        subscriptions.stop();
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
        subscriptions.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return subscriptions.isRunning();
    }

    /**
     * Synchronized because a subscription moves between the two maps in two steps, so an unsynchronized reader can
     * land between them and miss an id that exists. It also keeps a caller from seeing the ids of a model that
     * {@link #shutdown()} has already flagged as shut down but not yet cleared.
     */
    @Override
    public synchronized Set<String> subscriptionIds() {
        return subscriptions.subscriptionIds();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return subscriptions.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return subscriptions.isPaused(subscriptionId);
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
        return subscriptions.resumeSubscription(subscriptionId, null);
    }

    /**
     * Resume a paused subscription at {@code startAt}, instead of the change-stream position it had read to. A
     * checkpoint the change stream can't open at, such as a catch-up's position ({@code GlobalCheckpoint}) or a time,
     * resumes from the position it had read to, as {@link #resumeSubscription(String)} does, since opening the change
     * stream at the present would skip what was written while the subscription was paused. So does a dynamic
     * {@code startAt} each time it resolves to such a checkpoint.
     *
     * @see RepositionableSubscriptions#resumeSubscription(String, StartAt)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        subscriptions.checkStartPosition(startAt);
        return subscriptions.resumeSubscription(subscriptionId, startAt);
    }

    /**
     * Pause an individual subscription. The change stream behind it is closed, but the position it has read to is
     * kept, so {@link #resumeSubscription(String)} continues from there and events written while it was paused are
     * delivered rather than skipped.
     *
     * @see #resumeSubscription(String)
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        Runnable waitForTheRunningAction;
        synchronized (this) {
            waitForTheRunningAction = subscriptions.pauseSubscription(subscriptionId);
        }
        // Without the monitor, so a call for another subscription doesn't wait for the paused one's action
        waitForTheRunningAction.run();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", NativeMongoSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("eventCollection=" + eventCollection)
                .add("timeRepresentation=" + timeRepresentation)
                .add("cloudEventDispatcher=" + cloudEventDispatcher)
                .add("database=" + database)
                .add(subscriptions.toString())
                .toString();
    }
}
