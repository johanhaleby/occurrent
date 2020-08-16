package se.haleby.occurrent.changestreamer.mongodb.spring.blocking;

import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.api.blocking.BlockingChangeStreamer;
import se.haleby.occurrent.changestreamer.api.blocking.Subscription;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBOperationTimeBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons;

import javax.annotation.PreDestroy;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.*;

/**
 * Wraps a {@link BlockingChangeStreamer} (with optimized support {@link SpringBlockingChangeStreamerForMongoDB}) and adds persistent stream position support. It stores the stream position
 * after an "action" (the consumer in this method {@link SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB#stream(String, Consumer)}) has completed successfully.
 * It stores the stream position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB {

    private final MongoTemplate mongoTemplate;
    private final String streamPositionCollection;
    private final BlockingChangeStreamer changeStreamer;

    /**
     * Create a change streamer that uses the Native sync Java MongoDB driver to persists the stream position in MongoDB.
     *
     * @param changeStreamer           The change streamer that will read events from the event store
     * @param mongoTemplate            The {@link MongoTemplate} that'll be used to store the stream position
     * @param streamPositionCollection The collection into which stream positions will be stored
     */
    public SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB(BlockingChangeStreamer changeStreamer, MongoTemplate mongoTemplate, String streamPositionCollection) {
        requireNonNull(changeStreamer, "changeStreamer cannot be null");
        requireNonNull(mongoTemplate, "Mongo template cannot be null");
        requireNonNull(streamPositionCollection, "streamPositionCollection cannot be null");

        this.changeStreamer = changeStreamer;
        this.mongoTemplate = mongoTemplate;
        this.streamPositionCollection = streamPositionCollection;
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEvent> action) {
        return stream(subscriptionId, null, action);
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     */
    public Subscription stream(String subscriptionId, ChangeStreamFilter filter, Consumer<CloudEvent> action) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
            Document streamPositionDocument = mongoTemplate.findOne(query(where(ID).is(subscriptionId)), Document.class, streamPositionCollection);
            if (streamPositionDocument == null) {
                streamPositionDocument = persistStreamPosition(subscriptionId, changeStreamer.globalChangeStreamPosition());
            }
            return calculateStartAtFromStreamPositionDocument(streamPositionDocument);
        };

        return changeStreamer.stream(subscriptionId,
                filter, startAtSupplier, cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistStreamPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition());
                }
        );
    }

    void pauseSubscription(String subscriptionId) {
        changeStreamer.cancelSubscription(subscriptionId);
    }

    /**
     * Cancel a subscription. This means that it'll no longer receive events as they are persisted to the event store.
     * The stream position that is persisted to MongoDB will also be removed.
     *
     * @param subscriptionId The subscription id to cancel
     */
    public void cancelSubscription(String subscriptionId) {
        pauseSubscription(subscriptionId);
        mongoTemplate.remove(query(where(ID).is(subscriptionId)), streamPositionCollection);
    }

    private Document persistStreamPosition(String subscriptionId, ChangeStreamPosition changeStreamPosition) {
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedChangeStreamPosition) {
            return persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedChangeStreamPosition) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedChangeStreamPosition) {
            return persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedChangeStreamPosition) changeStreamPosition).operationTime);
        } else {
            String streamPositionString = changeStreamPosition.asString();
            Document document = MongoDBCommons.generateGenericStreamPositionDocument(subscriptionId, streamPositionString);
            return persistDocumentStreamPosition(subscriptionId, document);
        }
    }

    private Document persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        return persistDocumentStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private Document persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        return persistDocumentStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private Document persistDocumentStreamPosition(String subscriptionId, Document document) {
        mongoTemplate.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                streamPositionCollection);
        return document;
    }

    @PreDestroy
    public void shutdownSubscribers() {
        changeStreamer.shutdown();
    }
}