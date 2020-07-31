package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
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
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.generateResumeTokenDocument;

public class SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB.class);

    private final SpringReactiveChangeStreamerForMongoDB changeStreamer;
    private final ReactiveMongoOperations mongo;
    private final String resumeTokenCollection;

    public SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB(SpringReactiveChangeStreamerForMongoDB changeStreamer, ReactiveMongoOperations mongo, String resumeTokenCollection) {
        this.changeStreamer = changeStreamer;
        this.mongo = mongo;
        this.resumeTokenCollection = resumeTokenCollection;
    }

    public Flux<CloudEvent> stream(String subscriberId, Function<CloudEvent, Mono<Void>> action) {
        return changeStreamer.stream(resumeFromPersistencePosition(subscriberId))
                .flatMap(cloudEventWithStreamPosition -> action.apply(cloudEventWithStreamPosition).thenReturn(cloudEventWithStreamPosition))
                .flatMap(cloudEventWithStreamPosition -> persistResumeToken(subscriberId, cloudEventWithStreamPosition.getStreamPosition()).thenReturn(cloudEventWithStreamPosition));
    }

    private Function<ChangeStreamWithFilterAndProjection<Document>, Flux<ChangeStreamEvent<Document>>> resumeFromPersistencePosition(String subscriberId) {
        return changeStream -> mongo.find(query(where(ID).is(subscriberId)), Document.class, resumeTokenCollection)
                .map(MongoDBCloudEventsToJsonDeserializer::extractResumeTokenFromPersistedResumeTokenDocument)
                .doOnNext(resumeToken -> log.info("Found resume token {} for subscriber {}, will resume stream.", resumeToken.asString(), subscriberId))
                .flatMap(resumeToken -> changeStream.startAfter(resumeToken.asBsonDocument()).listen())
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("Couldn't find resume token for subscriber {}, will start subscribing to events at this moment in time.", subscriberId);
                    return changeStream.listen();
                }));
    }

    public Mono<Void> cancelSubscription(String subscriberId) {
        return mongo.remove(query(where(ID).is(subscriberId)), resumeTokenCollection).then();
    }

    private Mono<UpdateResult> persistResumeToken(String subscriberId, BsonValue resumeToken) {
        return mongo.upsert(query(where(ID).is(subscriberId)),
                Update.fromDocument(generateResumeTokenDocument(subscriberId, resumeToken)),
                resumeTokenCollection);
    }
}