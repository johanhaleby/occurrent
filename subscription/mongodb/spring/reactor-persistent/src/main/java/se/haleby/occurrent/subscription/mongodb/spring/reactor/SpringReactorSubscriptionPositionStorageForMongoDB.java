package se.haleby.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.api.reactor.PositionAwareReactorSubscription;
import se.haleby.occurrent.subscription.api.reactor.ReactorSubscriptionPositionStorage;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.generateOperationTimeStreamPositionDocument;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.generateResumeTokenStreamPositionDocument;

/**
 * Wraps a {@link PositionAwareReactorSubscription} and adds persistent stream position support. It adds some convenience methods that stores the stream position
 * after an "action" (the "function" in this method {@link SpringReactorSubscriptionPositionStorageForMongoDB#subscribe(String, Function)}) has completed successfully.
 * It stores the stream position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class SpringReactorSubscriptionPositionStorageForMongoDB implements ReactorSubscriptionPositionStorage {
    private static final Logger log = LoggerFactory.getLogger(SpringReactorSubscriptionPositionStorageForMongoDB.class);

    private final PositionAwareReactorSubscription subscription;
    private final ReactiveMongoOperations mongo;
    private final String streamPositionCollection;

    /**
     * Create a new instance of {@link SpringReactorSubscriptionPositionStorageForMongoDB}
     *
     * @param subscription             The {@link PositionAwareReactorSubscription} to use when streaming events from the event store.
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting stream positions to MongoDB.
     * @param streamPositionCollection The collection that will contain the stream position for each subscriber.
     */
    public SpringReactorSubscriptionPositionStorageForMongoDB(PositionAwareReactorSubscription subscription, ReactiveMongoOperations mongo, String streamPositionCollection) {
        this.subscription = subscription;
        this.mongo = mongo;
        this.streamPositionCollection = streamPositionCollection;
    }

    /**
     * A convenience function that automatically starts from the latest persisted subscription position and saves the new position after each call to {@code action}
     * has completed successfully. If you don't want to save the position after every event then don't use this method and instead save the position yourself by calling
     * {@link #write(String, SubscriptionPosition)} when appropriate.
     * <p>
     * It's VERY important that side-effects take place within the <code>action</code> function
     * because if you perform side-effects on the returned <code>Flux<CloudEvent></code> stream then the stream position
     * has already been stored in MongoDB and the <code>action</code> will not be re-run if side-effect fails.
     * </p>
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @return A stream of {@link CloudEvent}'s. The stream position of the cloud event will already have been persisted when consumed by this stream so use <code>action</code> to perform side-effects.
     */
    public Flux<CloudEvent> subscribe(String subscriptionId, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, null, action);
    }

    /**
     * A convenience function that automatically starts from the latest persisted subscription position and saves the new position after each call to {@code action}
     * has completed successfully. If you don't want to save the position after every event then don't use this method and instead save the position yourself by calling
     * {@link #write(String, SubscriptionPosition)} when appropriate.
     *
     * <p>
     * It's VERY important that side-effects take place within the <code>action</code> function
     * because if you perform side-effects on the returned <code>Flux<CloudEvent></code> stream then the stream position
     * has already been stored in MongoDB and the <code>action</code> will not be re-run if side-effect fails.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The {@link SubscriptionFilter} to use to limit the events receive by the event store
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @return A stream of {@link CloudEvent}'s. The stream position of the cloud event will already have been persisted when consumed by this stream so use <code>action</code> to perform side-effects.
     */
    public Flux<CloudEvent> subscribe(String subscriptionId, SubscriptionFilter filter, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        return findStartAtForSubscription(subscriptionId)
                .doOnNext(startAt -> log.info("Starting subscription {} from stream position {}", subscriptionId, startAt.toString()))
                .flatMapMany(startAt -> subscription.subscribe(filter, startAt))
                .flatMap(cloudEventWithStreamPosition -> action.apply(cloudEventWithStreamPosition).thenReturn(cloudEventWithStreamPosition))
                .flatMap(cloudEventWithStreamPosition -> write(subscriptionId, cloudEventWithStreamPosition.getStreamPosition()).thenReturn(cloudEventWithStreamPosition));
    }

    /**
     * Find the start position for a given subscription.
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link StartAt} data point for the supplied subscriptionId
     */
    @Override
    public Mono<StartAt> findStartAtForSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        return read(subscriptionId)
                .doOnNext(document -> log.info("Found subscription position: {}", document))
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No stream position found for {}, will initialize a new one.", subscriptionId);
                    return subscription.globalSubscriptionPosition()
                            .flatMap(streamPosition -> write(subscriptionId, streamPosition));
                }))
                .map(StartAt::streamPosition);
    }

    /**
     * Cancel a subscription. This means that it'll no longer receive events as they are persisted to the event store.
     * The stream position that is persisted to MongoDB will also be removed.
     *
     * @param subscriptionId The subscription id to cancel
     * @return An empty {@link Mono}.
     */
    public Mono<Void> cancelSubscription(String subscriptionId) {
        return mongo.remove(query(where(ID).is(subscriptionId)), streamPositionCollection).then();
    }

    @Override
    public Mono<SubscriptionPosition> write(String subscriptionId, SubscriptionPosition changeStreamPosition) {
        Mono<?> result;
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            result = persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedSubscriptionPosition) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            result = persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedSubscriptionPosition) changeStreamPosition).operationTime);
        } else {
            String streamPositionString = changeStreamPosition.asString();
            Document document = MongoDBCommons.generateGenericStreamPositionDocument(subscriptionId, streamPositionString);
            result = persistDocumentStreamPosition(subscriptionId, document);
        }
        return result.thenReturn(changeStreamPosition);
    }

    private Mono<Document> persistResumeTokenStreamPosition(String subscriptionId, BsonDocument resumeToken) {
        Document document = generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken);
        return persistDocumentStreamPosition(subscriptionId, document).thenReturn(document);
    }

    private Mono<Document> persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp timestamp) {
        Document document = generateOperationTimeStreamPositionDocument(subscriptionId, timestamp);
        return persistDocumentStreamPosition(subscriptionId, document).thenReturn(document);
    }

    private Mono<UpdateResult> persistDocumentStreamPosition(String subscriptionId, Document document) {
        return mongo.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                streamPositionCollection);
    }

    @Override
    public Mono<SubscriptionPosition> read(String subscriptionId) {
        return mongo.findOne(query(where(ID).is(subscriptionId)), Document.class, streamPositionCollection)
                .map(MongoDBCommons::calculateSubscriptionPositionFromMongoStreamPositionDocument);
    }
}