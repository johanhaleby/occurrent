package se.haleby.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.CloudEventWithSubscriptionPosition;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.Subscription;
import se.haleby.occurrent.subscription.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification;
import se.haleby.occurrent.subscription.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.internal.DocumentAdapter;
import se.haleby.occurrent.subscription.mongodb.nativedriver.blocking.RetryStrategy.Backoff;
import se.haleby.occurrent.subscription.mongodb.nativedriver.blocking.RetryStrategy.Fixed;
import se.haleby.occurrent.subscription.mongodb.nativedriver.blocking.RetryStrategy.None;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.applyStartPosition;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.getServerOperationTime;

/**
 * This is a subscription that uses the "native" MongoDB Java driver (sync) to listen to changes from the event store.
 * This Subscription doesn't maintain the stream position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 */
public class BlockingSubscriptionForMongoDB implements PositionAwareBlockingSubscription {
    private static final Logger log = LoggerFactory.getLogger(BlockingSubscriptionForMongoDB.class);

    private final MongoCollection<Document> eventCollection;
    private final ConcurrentMap<String, MongoChangeStreamCursor<ChangeStreamDocument<Document>>> subscriptions;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;
    private final Executor cloudEventDispatcher;
    private final RetryStrategy retryStrategy;
    private final MongoDatabase database;

    private volatile boolean shuttingDown = false;

    /**
     * Create a subscription using the native MongoDB sync driver.
     *
     * @param database             The MongoDB database to use
     * @param eventCollectionName  The name of the collection that contains the events
     * @param timeRepresentation   How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     * @param retryStrategy        Configure how retries should be handled
     */
    public BlockingSubscriptionForMongoDB(MongoDatabase database, String eventCollectionName, TimeRepresentation timeRepresentation,
                                            Executor subscriptionExecutor, RetryStrategy retryStrategy) {
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
    public BlockingSubscriptionForMongoDB(MongoDatabase database, MongoCollection<Document> eventCollection, TimeRepresentation timeRepresentation,
                                            Executor subscriptionExecutor, RetryStrategy retryStrategy) {
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
        this.subscriptions = new ConcurrentHashMap<>();
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter
            filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEventWithSubscriptionPosition> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAtSupplier, "Start at cannot be null");

        List<Bson> pipeline = createPipeline(filter);
        CountDownLatch subscriptionStartedLatch = new CountDownLatch(1);

        Runnable runnable = () -> {
            ChangeStreamIterable<Document> changeStreamDocuments = eventCollection.watch(pipeline, Document.class);
            ChangeStreamIterable<Document> changeStreamDocumentsAtPosition = applyStartPosition(changeStreamDocuments, ChangeStreamIterable::startAfter, ChangeStreamIterable::startAtOperationTime, startAtSupplier.get());
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamDocumentsAtPosition.cursor();

            subscriptions.put(subscriptionId, cursor);

            subscriptionStartedLatch.countDown();
            try {
                cursor.forEachRemaining(changeStreamDocument -> deserializeToCloudEvent(cloudEventSerializer, changeStreamDocument, timeRepresentation)
                        .map(cloudEvent -> new CloudEventWithSubscriptionPosition(cloudEvent, new MongoDBResumeTokenBasedSubscriptionPosition(changeStreamDocument.getResumeToken())))
                        .ifPresent(retry(action, __ -> true, convertToDelayStream(retryStrategy))));
            } catch (MongoException e) {
                log.debug("Caught {} (code={}, message={}), this might happen when cursor is shutdown.", e.getClass().getName(), e.getCode(), e.getMessage(), e);
            }
        };

        cloudEventDispatcher.execute(retry(runnable, __ -> !shuttingDown, convertToDelayStream(retryStrategy)));
        return new NativeMongoDBSubscription(subscriptionId, subscriptionStartedLatch);
    }

    private static List<Bson> createPipeline(SubscriptionFilter filter) {
        final List<Bson> pipeline;
        if (filter == null) {
            pipeline = Collections.emptyList();
        } else if (filter instanceof JsonMongoDBFilterSpecification) {
            pipeline = Collections.singletonList(Document.parse(((JsonMongoDBFilterSpecification) filter).getJson()));
        } else if (filter instanceof BsonMongoDBFilterSpecification) {
            Bson[] aggregationStages = ((BsonMongoDBFilterSpecification) filter).getAggregationStages();
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
    public void cancelSubscription(String subscriptionId) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = subscriptions.remove(subscriptionId);
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                log.error("Failed to cancel subscription, this might happen if Mongo connection has been shutdown", e);
            }
        }
    }

    public void shutdown() {
        synchronized (subscriptions) {
            shuttingDown = true;
            subscriptions.keySet().forEach(this::cancelSubscription);
        }
    }

    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        BsonTimestamp currentOperationTime = getServerOperationTime(database.runCommand(new Document("hostInfo", 1)));
        return new MongoDBOperationTimeBasedSubscriptionPosition(currentOperationTime);
    }

    private static Runnable retry(Runnable runnable, Predicate<Exception> retryPredicate, Iterator<Long> delay) {
        Consumer<Void> runnableConsumer = __ -> runnable.run();
        return () -> retry(runnableConsumer, retryPredicate, delay).accept(null);
    }

    private static <T1> Consumer<T1> retry(Consumer<T1> fn, Predicate<Exception> retryPredicate, Iterator<Long> delay) {
        return t1 -> {
            try {
                fn.accept(t1);
            } catch (Exception e) {
                if (retryPredicate.test(e) && delay != null) {
                    Long retryAfterMillis = delay.next();
                    log.error("Caught {} with message \"{}\", will retry in {} milliseconds.", e.getClass().getName(), e.getMessage(), retryAfterMillis, e);
                    try {
                        Thread.sleep(retryAfterMillis);
                    } catch (InterruptedException interruptedException) {
                        throw new RuntimeException(e);
                    }
                    retry(fn, retryPredicate, delay).accept(t1);
                } else {
                    throw e;
                }
            }
        };
    }

    private static Iterator<Long> convertToDelayStream(RetryStrategy retryStrategy) {
        final Stream<Long> delay;
        if (retryStrategy instanceof None) {
            delay = null;
        } else if (retryStrategy instanceof Fixed) {
            long millis = ((Fixed) retryStrategy).millis;
            delay = Stream.iterate(millis, __ -> millis);
        } else if (retryStrategy instanceof Backoff) {
            Backoff strategy = (Backoff) retryStrategy;
            long initialMillis = strategy.initial.toMillis();
            long maxMillis = strategy.max.toMillis();
            double multiplier = strategy.multiplier;
            delay = Stream.iterate(initialMillis, current -> Math.min(maxMillis, Math.round(current * multiplier)));
        } else {
            throw new IllegalStateException("Invalid retry strategy: " + retryStrategy.getClass().getName());
        }
        return delay == null ? null : delay.iterator();
    }
}