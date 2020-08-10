package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer;

import java.util.function.Function;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.*;

public class SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB.class);

    private final SpringReactiveChangeStreamerForMongoDB changeStreamer;
    private final ReactiveMongoOperations mongo;
    private final String streamPositionCollection;

    public SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB(SpringReactiveChangeStreamerForMongoDB changeStreamer, ReactiveMongoOperations mongo, String streamPositionCollection) {
        this.changeStreamer = changeStreamer;
        this.mongo = mongo;
        this.streamPositionCollection = streamPositionCollection;
    }

    public Flux<CloudEvent> stream(String subscriptionId, Function<CloudEvent, Mono<Void>> action) {
        return changeStreamer.stream(resumeFromPersistencePosition(subscriptionId))
                .flatMap(cloudEventWithStreamPosition -> action.apply(cloudEventWithStreamPosition).thenReturn(cloudEventWithStreamPosition))
                .flatMap(cloudEventWithStreamPosition -> persistResumeTokenStreamPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition().getResumeToken()).thenReturn(cloudEventWithStreamPosition));
    }

    private Function<ChangeStreamWithFilterAndProjection<Document>, Flux<ChangeStreamEvent<Document>>> resumeFromPersistencePosition(String subscriptionId) {
        return changeStream -> mongo.find(query(where(ID).is(subscriptionId)), Document.class, streamPositionCollection)
                .flatMap(streamPositionDocument -> {
                    Flux<ChangeStreamEvent<Document>> flux;
                    if (streamPositionDocument.containsKey(RESUME_TOKEN)) {
                        ResumeToken resumeToken = extractResumeTokenFromPersistedResumeTokenDocument(streamPositionDocument);
                        log.info("Found resume token {} for subscription {}, will resume stream.", resumeToken.asString(), subscriptionId);
                        flux = changeStream.startAfter(resumeToken.asBsonDocument()).listen();
                    } else if (streamPositionDocument.containsKey(OPERATION_TIME)) {
                        BsonTimestamp lastOperationTime = extractOperationTimeFromPersistedPositionDocument(streamPositionDocument);
                        log.info("Found last operation time {} for subscription {}, will resume stream.", lastOperationTime.getValue(), subscriptionId);
                        flux = changeStream.resumeAt(lastOperationTime).listen();
                    } else {
                        flux = Flux.error(new IllegalStateException("Couldn't identify resume token or operation time in stream position document: " + streamPositionDocument.toJson()));
                    }
                    return flux;
                })
                .switchIfEmpty(Flux.defer(() -> mongo.executeCommand(new Document("hostInfo", 1))
                        .map(MongoDBCloudEventsToJsonDeserializer::getServerOperationTime)
                        .flatMap(operationTime -> persistOperationTimeStreamPosition(subscriptionId, operationTime).thenReturn(operationTime))
                        .flatMapMany(operationTime -> {
                            log.info("Couldn't find resume token for subscriber {}, will start subscribing to events at this moment in time.", subscriptionId);
                            return changeStream.resumeAt(operationTime).listen();
                        })));
    }

    public Mono<Void> cancelSubscription(String subscriptionId) {
        return mongo.remove(query(where(ID).is(subscriptionId)), streamPositionCollection).then();
    }

    private Mono<UpdateResult> persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        return persistStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));

    }

    private Mono<UpdateResult> persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp timestamp) {
        return persistStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, timestamp));
    }

    private Mono<UpdateResult> persistStreamPosition(String subscriptionId, Document document) {
        return mongo.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                streamPositionCollection);
    }
}