package se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification;

import java.util.function.Consumer;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.*;

public class BlockingChangeStreamerWithPositionPersistenceForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(BlockingChangeStreamerWithPositionPersistenceForMongoDB.class);

    private final MongoCollection<Document> streamPositionCollection;
    private final BlockingChangeStreamerForMongoDB changeStreamer;
    private final MongoDatabase database;

    public BlockingChangeStreamerWithPositionPersistenceForMongoDB(BlockingChangeStreamerForMongoDB changeStreamer, MongoDatabase database, String streamPositionCollection) {
        this(changeStreamer, database, requireNonNull(database, "Database cannot be null").getCollection(streamPositionCollection));
    }

    public BlockingChangeStreamerWithPositionPersistenceForMongoDB(BlockingChangeStreamerForMongoDB changeStreamer, MongoDatabase database, MongoCollection<Document> streamPositionCollection) {
        requireNonNull(changeStreamer, "changeStreamer cannot be null");
        requireNonNull(streamPositionCollection, "streamPositionCollection cannot be null");
        requireNonNull(database, "Database cannot be null");
        this.database = database;
        this.changeStreamer = changeStreamer;
        this.streamPositionCollection = streamPositionCollection;
    }

    public void stream(String subscriptionId, Consumer<CloudEvent> action) {
        stream(subscriptionId, action, null);
    }

    public void stream(String subscriptionId, Consumer<CloudEvent> action, MongoDBFilterSpecification filter) {
        Function<ChangeStreamIterable<Document>, ChangeStreamIterable<Document>> changeStreamConfigurer = changeStreamIterable -> {
            // It's important that we find the document instead the function so that we lookup the latest resume token on retry
            Document document = streamPositionCollection.find(eq(ID, subscriptionId), Document.class).first();
            if (document == null) {
                log.info("Couldn't find resume token for subscription {}, will start subscribing to events at this moment in time.", subscriptionId);
                BsonTimestamp currentOperationTime = getServerOperationTime(database.runCommand(new Document("hostInfo", 1)));
                persistOperationTimeStreamPosition(subscriptionId, currentOperationTime);
                changeStreamIterable.startAtOperationTime(currentOperationTime);
            } else if (document.containsKey(RESUME_TOKEN)) {
                ResumeToken resumeToken = extractResumeTokenFromPersistedResumeTokenDocument(document);
                log.info("Found resume token {} for subscription {}, will resume stream.", resumeToken.asString(), subscriptionId);
                changeStreamIterable.startAfter(resumeToken.asBsonDocument());
            } else if (document.containsKey(OPERATION_TIME)) {
                BsonTimestamp lastOperationTime = extractOperationTimeFromPersistedPositionDocument(document);
                log.info("Found last operation time {} for subscription {}, will resume stream.", lastOperationTime.getValue(), subscriptionId);
                changeStreamIterable.startAtOperationTime(lastOperationTime);
            }
            return changeStreamIterable;
        };

        changeStreamer.stream(subscriptionId,
                cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistResumeTokenStreamPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition().resumeToken);
                },
                filter,
                changeStreamConfigurer);
    }

    void pauseSubscription(String subscriptionId) {
        changeStreamer.cancelSubscription(subscriptionId);
    }

    public void cancelSubscription(String subscriptionId) {
        pauseSubscription(subscriptionId);
        streamPositionCollection.deleteOne(eq(ID, subscriptionId));
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private void persistStreamPosition(String subscriptionId, Document document) {
        streamPositionCollection.replaceOne(eq(ID, subscriptionId), document, new ReplaceOptions().upsert(true));
    }

    public void shutdown() {
        changeStreamer.shutdown();
    }
}