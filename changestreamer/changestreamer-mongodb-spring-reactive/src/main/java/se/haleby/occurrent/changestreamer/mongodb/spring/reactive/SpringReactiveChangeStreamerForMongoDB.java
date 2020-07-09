package se.haleby.occurrent.changestreamer.mongodb.spring.reactive;

import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
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
import se.haleby.occurrent.changestreamer.mongodb.common.MongoDBCloudEventsToJsonDeserializer;

import java.util.List;
import java.util.function.Function;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.changestreamer.mongodb.common.MongoDBCloudEventsToJsonDeserializer.*;

public class SpringReactiveChangeStreamerForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringReactiveChangeStreamerForMongoDB.class);

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final String resumeTokenCollection;
    private final EventFormat cloudEventSerializer;

    public SpringReactiveChangeStreamerForMongoDB(ReactiveMongoOperations mongo, String eventCollection, String resumeTokenCollection) {
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.resumeTokenCollection = resumeTokenCollection;
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    public Flux<CloudEvent> subscribe(String subscriberId, Function<List<CloudEvent>, Mono<Void>> action) {
        ChangeStreamWithFilterAndProjection<String> changeStream = mongo.changeStream(String.class).watchCollection(eventCollection);

        // First try to find and use a resume token for the subscriber, if not found just listen normally.
        return mongo.find(query(where(ID).is(subscriberId)), Document.class, resumeTokenCollection)
                .map(MongoDBCloudEventsToJsonDeserializer::extractResumeTokenFromPersistedResumeTokenDocument)
                .doOnNext(resumeToken -> log.info("Found resume token {} for subscriber {}, will resume stream.", resumeToken.asString(), subscriberId))
                .flatMap(resumeToken -> changeStream.startAfter(resumeToken.asBsonDocument()).listen())
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("Couldn't find resume token for subscriber {}, will start subscribing to events at this moment in time.", subscriberId);
                    return changeStream.listen();
                }))
                .flatMap(changeEvent -> {
                    List<CloudEvent> cloudEvents = deserializeToCloudEvents(cloudEventSerializer, changeEvent.getRaw());
                    return action.apply(cloudEvents).thenReturn(new ChangeStreamEventAndCloudEvent(changeEvent, cloudEvents));
                })
                .flatMap(events -> persistResumeToken(subscriberId, events.changeStreamEvent.getResumeToken()).thenMany(Flux.fromIterable(events.cloudEvents)));
    }

    public void unsubscribe(String subscriberId) {
        mongo.remove(query(where(ID).is(subscriberId)), resumeTokenCollection).subscribe();
    }

    private Mono<UpdateResult> persistResumeToken(String subscriberId, BsonValue resumeToken) {
        return mongo.upsert(query(where(ID).is(subscriberId)),
                Update.fromDocument(generateResumeTokenDocument(subscriberId, resumeToken)),
                resumeTokenCollection);
    }

    private static class ChangeStreamEventAndCloudEvent {
        private final ChangeStreamEvent<String> changeStreamEvent;
        private final List<CloudEvent> cloudEvents;

        ChangeStreamEventAndCloudEvent(ChangeStreamEvent<String> changeStreamEvent, List<CloudEvent> cloudEvents) {
            this.changeStreamEvent = changeStreamEvent;
            this.cloudEvents = cloudEvents;
        }
    }
}