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

    public Flux<CloudEvent> subscribe(String subscriberId, Function<CloudEvent, Mono<Void>> action) {
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
                .flatMap(changeEvent ->
                        deserializeToCloudEvent(cloudEventSerializer, changeEvent.getRaw())
                                .map(cloudEvent -> action.apply(cloudEvent).thenReturn(new ChangeStreamEventAndCloudEvent(changeEvent, cloudEvent)))
                                .orElse(Mono.just(new ChangeStreamEventAndCloudEvent(changeEvent, null)))
                )
                .flatMap(event -> persistResumeToken(subscriberId, event.changeStreamEvent.getResumeToken()).thenReturn(event))
                .filter(event -> event.cloudEvent != null)
                .map(ChangeStreamEventAndCloudEvent::getCloudEvent);
    }

    public Mono<Void> cancelSubscription(String subscriberId) {
        return mongo.remove(query(where(ID).is(subscriberId)), resumeTokenCollection).then();
    }

    private Mono<UpdateResult> persistResumeToken(String subscriberId, BsonValue resumeToken) {
        return mongo.upsert(query(where(ID).is(subscriberId)),
                Update.fromDocument(generateResumeTokenDocument(subscriberId, resumeToken)),
                resumeTokenCollection);
    }

    private static class ChangeStreamEventAndCloudEvent {
        private final ChangeStreamEvent<String> changeStreamEvent;
        private final CloudEvent cloudEvent;

        ChangeStreamEventAndCloudEvent(ChangeStreamEvent<String> changeStreamEvent, CloudEvent cloudEvent) {
            this.changeStreamEvent = changeStreamEvent;
            this.cloudEvent = cloudEvent;
        }

        public CloudEvent getCloudEvent() {
            return cloudEvent;
        }
    }
}