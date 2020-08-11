package se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.StreamPosition;
import se.haleby.occurrent.changestreamer.StringBasedStreamPosition;
import se.haleby.occurrent.changestreamer.api.blocking.BlockingChangeStreamer;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBOperationTimeBasedStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedStreamPosition;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.*;

/**
 * Wraps a {@link BlockingChangeStreamerForMongoDB} and adds persistent stream position support. It stores the stream position
 * after an "action" (the consumer in this method {@link BlockingChangeStreamerForMongoDB#stream(String, Consumer)}) has completed successfully.
 * It stores the stream position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class BlockingChangeStreamerWithPositionPersistenceForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(BlockingChangeStreamerWithPositionPersistenceForMongoDB.class);

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
    public void stream(String subscriptionId, Consumer<CloudEvent> action) {
        stream(subscriptionId, action, null);
    }

    /**
     * Start streaming cloud events from the event store and persist the stream position in MongoDB
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     */
    public void stream(String subscriptionId, Consumer<CloudEvent> action, ChangeStreamFilter filter) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document instead the function so that we lookup the latest resume token on retry
            Document document = streamPositionCollection.find(eq(ID, subscriptionId), Document.class).first();
            final StreamPosition streamPosition;
            if (document == null) {
                log.info("Couldn't find resume token for subscription {}, will start subscribing to events at this moment in time.", subscriptionId);
                BsonTimestamp currentOperationTime = getServerOperationTime(database.runCommand(new Document("hostInfo", 1)));
                persistOperationTimeStreamPosition(subscriptionId, currentOperationTime);
                streamPosition = new MongoDBOperationTimeBasedStreamPosition(currentOperationTime);
            } else if (document.containsKey(RESUME_TOKEN)) {
                ResumeToken resumeToken = extractResumeTokenFromPersistedResumeTokenDocument(document);
                log.info("Found resume token {} for subscription {}, will resume stream.", resumeToken.asString(), subscriptionId);
                streamPosition = new MongoDBResumeTokenBasedStreamPosition(resumeToken.asBsonDocument());
            } else if (document.containsKey(OPERATION_TIME)) {
                BsonTimestamp lastOperationTime = extractOperationTimeFromPersistedPositionDocument(document);
                log.info("Found last operation time {} for subscription {}, will resume stream.", lastOperationTime.getValue(), subscriptionId);
                streamPosition = new MongoDBOperationTimeBasedStreamPosition(lastOperationTime);
            } else if (document.containsKey(GENERIC_STREAM_POSITION)) {
                String value = document.getString(GENERIC_STREAM_POSITION);
                streamPosition = new StringBasedStreamPosition(value);
            } else {
                throw new IllegalStateException("Doesn't recognize " + document + " as a valid stream position document");
            }
            return StartAt.streamPosition(streamPosition);
        };

        changeStreamer.stream(subscriptionId,
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

    private void persistStreamPosition(String subscriptionId, StreamPosition streamPosition) {
        if (streamPosition instanceof MongoDBResumeTokenBasedStreamPosition) {
            persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedStreamPosition) streamPosition).resumeToken);
        } else if (streamPosition instanceof MongoDBOperationTimeBasedStreamPosition) {
            persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedStreamPosition) streamPosition).operationTime);
        } else {
            String streamPositionString = streamPosition.asString();
            persistDocumentStreamPosition(subscriptionId, generateGenericStreamPositionDocument(subscriptionId, streamPositionString));
        }
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private void persistDocumentStreamPosition(String subscriptionId, Document document) {
        streamPositionCollection.replaceOne(eq(ID, subscriptionId), document, new ReplaceOptions().upsert(true));
    }

    public void shutdown() {
        changeStreamer.shutdown();
    }
}