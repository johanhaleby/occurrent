package se.haleby.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.Subscription;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.calculateSubscriptionPositionFromMongoStreamPositionDocument;

/**
 * Wraps a {@link BlockingSubscriptionForMongoDB} and adds persistent subscription position support. It stores the subscription position
 * after an "action" (the consumer in this method {@link BlockingSubscriptionForMongoDB#subscribe(String, Consumer)}) has completed successfully.
 * It stores the subscription position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the subscription position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class BlockingSubscriptionWithPositionPersistenceForMongoDB implements BlockingSubscription<CloudEvent> {

    private final MongoCollection<Document> streamPositionCollection;
    private final PositionAwareBlockingSubscription subscription;

    /**
     * Create a subscription that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscription           The subscription that will read events from the event store
     * @param database                 The database into which subscription positions will be stored
     * @param streamPositionCollection The collection into which subscription positions will be stored
     */
    public BlockingSubscriptionWithPositionPersistenceForMongoDB(PositionAwareBlockingSubscription subscription, MongoDatabase database, String streamPositionCollection) {
        this(subscription, requireNonNull(database, "Database cannot be null").getCollection(streamPositionCollection));
    }

    /**
     * Create a subscription that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscription           The subscription that will read events from the event store
     * @param streamPositionCollection The collection into which subscription positions will be stored
     */
    public BlockingSubscriptionWithPositionPersistenceForMongoDB(PositionAwareBlockingSubscription subscription, MongoCollection<Document> streamPositionCollection) {
        requireNonNull(subscription, "subscription cannot be null");
        requireNonNull(streamPositionCollection, "streamPositionCollection cannot be null");
        this.subscription = subscription;
        this.streamPositionCollection = streamPositionCollection;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        return subscription.subscribe(subscriptionId,
                filter, startAtSupplier, cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistStreamPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition());
                }
        );
    }

    @Override
    public Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, (SubscriptionFilter) null, action);
    }

    /**
     * Start streaming cloud events from the event store and persist the subscription position in MongoDB
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @return The subscription
     */
    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
            Document streamPositionDocument = streamPositionCollection.find(eq(ID, subscriptionId), Document.class).first();
            if (streamPositionDocument == null) {
                streamPositionDocument = persistStreamPosition(subscriptionId, subscription.globalSubscriptionPosition());
            }
            return StartAt.streamPosition(calculateSubscriptionPositionFromMongoStreamPositionDocument(streamPositionDocument));
        };

        return subscribe(subscriptionId, filter, startAtSupplier, action);
    }

    void pauseSubscription(String subscriptionId) {
        subscription.cancelSubscription(subscriptionId);
    }

    public void cancelSubscription(String subscriptionId) {
        pauseSubscription(subscriptionId);
        streamPositionCollection.deleteOne(eq(ID, subscriptionId));
    }

    private Document persistStreamPosition(String subscriptionId, SubscriptionPosition changeStreamPosition) {
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            return persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedSubscriptionPosition) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            return persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedSubscriptionPosition) changeStreamPosition).operationTime);
        } else {
            String streamPositionString = changeStreamPosition.asString();
            return persistDocumentStreamPosition(subscriptionId, MongoDBCommons.generateGenericStreamPositionDocument(subscriptionId, streamPositionString));
        }
    }

    private Document persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        return persistDocumentStreamPosition(subscriptionId, MongoDBCommons.generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private Document persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        return persistDocumentStreamPosition(subscriptionId, MongoDBCommons.generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private Document persistDocumentStreamPosition(String subscriptionId, Document document) {
        streamPositionCollection.replaceOne(eq(ID, subscriptionId), document, new ReplaceOptions().upsert(true));
        return document;
    }

    public void shutdown() {
        subscription.shutdown();
    }
}