package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons;

import java.util.function.Function;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;

/**
 * Wraps a {@link SpringReactiveChangeStreamerForMongoDB} and adds persistent stream position support. It stores the stream position
 * after an "action" (the "function" in this method {@link SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB#stream(String, Function)}) has completed successfully.
 * It stores the stream position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB.class);

    private final SpringReactiveChangeStreamerForMongoDB changeStreamer;
    private final ReactiveMongoOperations mongo;
    private final String streamPositionCollection;

    /**
     * Create a new instance of {@link SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB}
     *
     * @param changeStreamer           The {@link SpringReactiveChangeStreamerForMongoDB} to use when streaming events from the event store.
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting stream positions to MongoDB.
     * @param streamPositionCollection The collection that will contain the stream position for each subscriber.
     */
    public SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB(SpringReactiveChangeStreamerForMongoDB changeStreamer, ReactiveMongoOperations mongo, String streamPositionCollection) {
        this.changeStreamer = changeStreamer;
        this.mongo = mongo;
        this.streamPositionCollection = streamPositionCollection;
    }

    /**
     * Subscribe the event stream and automatically persist the stream position in MongoDB after each <code>action</code>
     * has completed successfully. It's VERY important that side-effects take place within the <code>action</code> function
     * because if you perform side-effects on the returned <code>Flux<CloudEvent></code> stream then the stream position
     * has already been stored in MongoDB and the <code>action</code> will not be re-run if side-effect fails.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @return A stream of {@link CloudEvent}'s. The stream position of the cloud event will already have been persisted when consumed by this stream so use <code>action</code> to perform side-effects.
     */
    public Flux<CloudEvent> stream(String subscriptionId, Function<CloudEvent, Mono<Void>> action) {
        return changeStreamer.stream(resumeFromPersistencePosition(subscriptionId))
                .flatMap(cloudEventWithStreamPosition -> action.apply(cloudEventWithStreamPosition).thenReturn(cloudEventWithStreamPosition))
                .flatMap(cloudEventWithStreamPosition -> persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedStreamPosition) cloudEventWithStreamPosition.getStreamPosition()).resumeToken).thenReturn(cloudEventWithStreamPosition));
    }

    private Function<ChangeStreamWithFilterAndProjection<Document>, Flux<ChangeStreamEvent<Document>>> resumeFromPersistencePosition(String subscriptionId) {
        return changeStream -> mongo.find(query(where(ID).is(subscriptionId)), Document.class, streamPositionCollection)
                .flatMap(streamPositionDocument -> {
                    Flux<ChangeStreamEvent<Document>> flux;
                    // TODO Replace with startAtOperationTime when Spring adds support for it
                    if (streamPositionDocument.containsKey(MongoDBCommons.RESUME_TOKEN)) {
                        MongoDBCommons.ResumeToken resumeToken = MongoDBCommons.extractResumeTokenFromPersistedResumeTokenDocument(streamPositionDocument);
                        log.info("Found resume token {} for subscription {}, will resume stream.", resumeToken.asString(), subscriptionId);
                        flux = changeStream.startAfter(resumeToken.asBsonDocument()).listen();
                    } else if (streamPositionDocument.containsKey(MongoDBCommons.OPERATION_TIME)) {
                        BsonTimestamp lastOperationTime = MongoDBCommons.extractOperationTimeFromPersistedPositionDocument(streamPositionDocument);
                        log.info("Found last operation time {} for subscription {}, will resume stream.", lastOperationTime.getValue(), subscriptionId);
                        flux = changeStream.resumeAt(lastOperationTime).listen();
                    } else {
                        flux = Flux.error(new IllegalStateException("Couldn't identify resume token or operation time in stream position document: " + streamPositionDocument.toJson()));
                    }
                    return flux;
                })
                .switchIfEmpty(Flux.defer(() -> mongo.executeCommand(new Document("hostInfo", 1))
                        .map(MongoDBCommons::getServerOperationTime)
                        .flatMap(operationTime -> persistOperationTimeStreamPosition(subscriptionId, operationTime).thenReturn(operationTime))
                        .flatMapMany(operationTime -> {
                            log.info("Couldn't find resume token for subscriber {}, will start subscribing to events at this moment in time.", subscriptionId);
                            return changeStream.resumeAt(operationTime).listen();
                        })));
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

    private Mono<UpdateResult> persistResumeTokenStreamPosition(String subscriptionId, BsonDocument resumeToken) {
        return persistStreamPosition(subscriptionId, MongoDBCommons.generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));

    }

    private Mono<UpdateResult> persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp timestamp) {
        return persistStreamPosition(subscriptionId, MongoDBCommons.generateOperationTimeStreamPositionDocument(subscriptionId, timestamp));
    }

    private Mono<UpdateResult> persistStreamPosition(String subscriptionId, Document document) {
        return mongo.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                streamPositionCollection);
    }
}