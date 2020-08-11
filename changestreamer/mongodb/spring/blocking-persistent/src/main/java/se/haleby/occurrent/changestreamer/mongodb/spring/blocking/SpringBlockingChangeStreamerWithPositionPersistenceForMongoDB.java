package se.haleby.occurrent.changestreamer.mongodb.spring.blocking;

import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.Subscription;
import org.springframework.data.mongodb.core.query.Update;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedStreamPosition;

import javax.annotation.PreDestroy;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.*;

/**
 * Wraps a {@link SpringBlockingChangeStreamerForMongoDB} and adds persistent stream position support. It stores the stream position
 * after an "action" (the consumer in this method {@link SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB#stream(String, Consumer)}) has completed successfully.
 * It stores the stream position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB.class);

    private final MongoTemplate mongoTemplate;
    private final String resumeTokenCollection;
    private final SpringBlockingChangeStreamerForMongoDB changeStreamer;

    /**
     * Create a change streamer that uses the Native sync Java MongoDB driver to persists the stream position in MongoDB.
     *
     * @param changeStreamer           The change streamer that will read events from the event store
     * @param mongoTemplate            The {@link MongoTemplate} that'll be used to store the stream position
     * @param streamPositionCollection The collection into which stream positions will be stored
     */
    public SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB(SpringBlockingChangeStreamerForMongoDB changeStreamer, MongoTemplate mongoTemplate, String streamPositionCollection) {
        this.changeStreamer = changeStreamer;
        requireNonNull(changeStreamer, "changeStreamer cannot be null");
        requireNonNull(mongoTemplate, "Mongo template cannot be null");
        requireNonNull(streamPositionCollection, "streamPositionCollection cannot be null");

        this.mongoTemplate = mongoTemplate;
        this.resumeTokenCollection = streamPositionCollection;
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEvent> action) {
        return stream(subscriptionId, action, null);
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEvent> action, MongoDBFilterSpecification filter) {
        Document document = mongoTemplate.findOne(query(where(ID).is(subscriptionId)), Document.class, resumeTokenCollection);

        final ChangeStreamOptionsBuilder changeStreamOptionsBuilder = ChangeStreamOptions.builder();
        if (document == null) {
            log.info("Couldn't find resume token for subscription {}, will start subscribing to events at this moment in time.", subscriptionId);
            BsonTimestamp currentOperationTime = getServerOperationTime(mongoTemplate.executeCommand(new Document("hostInfo", 1)));
            persistOperationTimeStreamPosition(subscriptionId, currentOperationTime);
            // TODO We should change this to startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
            changeStreamOptionsBuilder.resumeAt(currentOperationTime);
        } else if (document.containsKey(RESUME_TOKEN)) {
            ResumeToken resumeToken = extractResumeTokenFromPersistedResumeTokenDocument(document);
            log.info("Found resume token {} for subscription {}, will resume stream.", resumeToken.asString(), subscriptionId);
            changeStreamOptionsBuilder.startAfter(resumeToken.asBsonDocument());
        } else if (document.containsKey(OPERATION_TIME)) {
            BsonTimestamp lastOperationTime = extractOperationTimeFromPersistedPositionDocument(document);
            log.info("Found last operation time {} for subscription {}, will resume stream.", lastOperationTime.getValue(), subscriptionId);
            // TODO We should change this to startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
            changeStreamOptionsBuilder.resumeAt(lastOperationTime);
        }

        return changeStreamer.stream(subscriptionId,
                cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedStreamPosition) cloudEventWithStreamPosition.getStreamPosition()).resumeToken);
                },
                filter,
                changeStreamOptionsBuilder);
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
        mongoTemplate.remove(query(where(ID).is(subscriptionId)), resumeTokenCollection);
    }


    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private void persistStreamPosition(String subscriptionId, Document document) {
        mongoTemplate.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                resumeTokenCollection);
    }

    @PreDestroy
    public void shutdownSubscribers() {
        changeStreamer.shutdownSubscribers();
    }
}