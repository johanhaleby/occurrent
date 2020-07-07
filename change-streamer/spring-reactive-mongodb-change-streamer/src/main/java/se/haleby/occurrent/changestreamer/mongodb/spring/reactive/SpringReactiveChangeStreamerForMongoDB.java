package se.haleby.occurrent.changestreamer.mongodb.spring.reactive;

import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonString;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class SpringReactiveChangeStreamerForMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringReactiveChangeStreamerForMongoDB.class);

    private static final String ID = "_id";

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
                .map(document -> document.get("resumeToken", BsonValue.class))
                .doOnNext(resumeToken -> log.info("Found resume token {} for subscriber {}, will resume stream.", resumeToken, subscriberId))
                .flatMap(resumeToken -> changeStream.startAfter(resumeToken).listen())
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("Couldn't find resume token for subscriber {}, will start subscribing to events for this moment in time.", subscriberId);
                    return changeStream.listen();
                }))
                .flatMap(changeEvent -> {
                    List<CloudEvent> cloudEvents = extractEventsAsJson(changeEvent).stream()
                            .map(cloudEventString -> cloudEventString.getBytes(UTF_8))
                            .map(cloudEventSerializer::deserialize)
                            .collect(Collectors.toList());
                    return action.apply(cloudEvents).thenReturn(new ChangeStreamEventAndCloudEvent<>(changeEvent, cloudEvents));
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

    @SuppressWarnings("SameParameterValue")
    private static Document generateResumeTokenDocument(String subscriberId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, subscriberId);
        data.put("resumeToken", resumeToken);
        return new Document(data);
    }

    @SuppressWarnings("ConstantConditions")
    private static List<String> extractEventsAsJson(ChangeStreamEvent<String> changeStreamEvent) {
        final List<String> eventsAsJson;
        OperationType operationType = changeStreamEvent.getOperationType();
        if (operationType == INSERT) {
            // This is when the first event(s) are written to the event store for a particular stream id
            eventsAsJson = changeStreamEvent.getRaw().getFullDocument().getList("events", String.class);
        } else if (operationType == UPDATE) {
            // When events already exists for a stream id we get an update operation. To only get the events
            // that are updated we get the "updated fields" and extract only the events ("version" is also updated but
            // we don't care about it here).
            eventsAsJson = changeStreamEvent.getRaw().getUpdateDescription().getUpdatedFields().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith("events"))
                    .map(Entry::getValue)
                    .map(BsonValue::asString)
                    .map(BsonString::getValue)
                    .collect(Collectors.toList());
        } else {
            eventsAsJson = Collections.emptyList();
        }

        return eventsAsJson;
    }

    private static class ChangeStreamEventAndCloudEvent<T> {
        private final ChangeStreamEvent<String> changeStreamEvent;
        private final List<CloudEvent> cloudEvents;

        ChangeStreamEventAndCloudEvent(ChangeStreamEvent<String> changeStreamEvent, List<CloudEvent> cloudEvents) {
            this.changeStreamEvent = changeStreamEvent;
            this.cloudEvents = cloudEvents;
        }
    }
}