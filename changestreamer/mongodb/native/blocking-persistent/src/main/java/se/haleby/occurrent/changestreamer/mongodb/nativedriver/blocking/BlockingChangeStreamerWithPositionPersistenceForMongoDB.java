package se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.api.blocking.BlockingChangeStreamer;
import se.haleby.occurrent.changestreamer.api.blocking.Subscription;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBOperationTimeBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.calculateStartAtFromStreamPositionDocument;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.getServerOperationTime;

/**
 * Wraps a {@link BlockingChangeStreamerForMongoDB} and adds persistent stream position support. It stores the stream position
 * after an "action" (the consumer in this method {@link BlockingChangeStreamerForMongoDB#stream(String, Consumer)}) has completed successfully.
 * It stores the stream position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class BlockingChangeStreamerWithPositionPersistenceForMongoDB {

    private final MongoCollection<Document> streamPositionCollection;
    private final BlockingChangeStreamer changeStreamer;
    private final MongoDatabase database;

    /**
     * Create a change streamer that uses the Native sync Java MongoDB driver to persists the stream position in MongoDB.
     *
     * @param changeStreamer           The change streamer that will read events from the event store
     * @param database                 The database into which stream positions will be stored
     * @param streamPositionCollection The collection into which stream positions will be stored
     */
    public BlockingChangeStreamerWithPositionPersistenceForMongoDB(BlockingChangeStreamer changeStreamer, MongoDatabase database, String streamPositionCollection) {
        this(changeStreamer, database, requireNonNull(database, "Database cannot be null").getCollection(streamPositionCollection));
    }

    /**
     * Create a change streamer that uses the Native sync Java MongoDB driver to persists the stream position in MongoDB.
     *
     * @param changeStreamer           The change streamer that will read events from the event store
     * @param database                 The database into which stream positions will be stored
     * @param streamPositionCollection The collection into which stream positions will be stored
     */
    public BlockingChangeStreamerWithPositionPersistenceForMongoDB(BlockingChangeStreamer changeStreamer, MongoDatabase database, MongoCollection<Document> streamPositionCollection) {
        requireNonNull(changeStreamer, "changeStreamer cannot be null");
        requireNonNull(streamPositionCollection, "streamPositionCollection cannot be null");
        requireNonNull(database, "Database cannot be null");
        this.database = database;
        this.changeStreamer = changeStreamer;
        this.streamPositionCollection = streamPositionCollection;
    }

    /**
     * Start streaming cloud events from the event store and persist the stream position in MongoDB
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEvent> action) {
        return stream(subscriptionId, action, null);
    }

    /**
     * Start streaming cloud events from the event store and persist the stream position in MongoDB
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @return The subscription
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEvent> action, ChangeStreamFilter filter) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
            Document streamPositionDocument = streamPositionCollection.find(eq(ID, subscriptionId), Document.class).first();
            if (streamPositionDocument == null) {
                BsonTimestamp currentOperationTime = getServerOperationTime(database.runCommand(new Document("hostInfo", 1)));
                streamPositionDocument = persistOperationTimeStreamPosition(subscriptionId, currentOperationTime);
            }
            return calculateStartAtFromStreamPositionDocument(streamPositionDocument);
        };

        return changeStreamer.stream(subscriptionId,
                cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistStreamPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition());
                },
                filter,
                startAtSupplier);
    }

    void pauseSubscription(String subscriptionId) {
        changeStreamer.cancelSubscription(subscriptionId);
    }

    public void cancelSubscription(String subscriptionId) {
        pauseSubscription(subscriptionId);
        streamPositionCollection.deleteOne(eq(ID, subscriptionId));
    }

    private void persistStreamPosition(String subscriptionId, ChangeStreamPosition changeStreamPosition) {
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedChangeStreamPosition) {
            persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedChangeStreamPosition) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedChangeStreamPosition) {
            persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedChangeStreamPosition) changeStreamPosition).operationTime);
        } else {
            String streamPositionString = changeStreamPosition.asString();
            persistDocumentStreamPosition(subscriptionId, MongoDBCommons.generateGenericStreamPositionDocument(subscriptionId, streamPositionString));
        }
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, MongoDBCommons.generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private Document persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        Document document = MongoDBCommons.generateOperationTimeStreamPositionDocument(subscriptionId, operationTime);
        persistDocumentStreamPosition(subscriptionId, document);
        return document;
    }

    private void persistDocumentStreamPosition(String subscriptionId, Document document) {
        streamPositionCollection.replaceOne(eq(ID, subscriptionId), document, new ReplaceOptions().upsert(true));
    }

    public void shutdown() {
        changeStreamer.shutdown();
    }
}