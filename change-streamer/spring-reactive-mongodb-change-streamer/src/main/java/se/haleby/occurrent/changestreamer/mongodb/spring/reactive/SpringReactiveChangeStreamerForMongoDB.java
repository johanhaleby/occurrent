package se.haleby.occurrent.changestreamer.mongodb.spring.reactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
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
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class SpringReactiveChangeStreamerForMongoDB<T> {
    private static final String RESUME_TOKEN_DOCUMENT_ID = "80c3cf26-0c96-4f9d-8c83-a3a2314f0776";
    private static final String ID = "_id";

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final String resumeTokenCollection;

    public SpringReactiveChangeStreamerForMongoDB(ReactiveMongoOperations mongo, String eventCollection, String resumeTokenCollection) {
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.resumeTokenCollection = resumeTokenCollection;
    }

    public Flux<CloudEventImpl<T>> subscribe(String subscriberId, Function<List<CloudEventImpl<T>>, Mono<Void>> action) {
        return mongo.changeStream(String.class)
                .watchCollection(eventCollection)
                // TODO Filter only insert and update operations??
                .listen()
                .flatMap(changeEvent -> {
                    List<CloudEventImpl<T>> cloudEvents = extractEventsAsJson(changeEvent).stream()
                            // @formatter:off
                            .map(eventAsJson -> Json.decodeValue(eventAsJson, new TypeReference<CloudEventImpl<T>>() {}))
                            // @formatter:on
                            .collect(Collectors.toList());
                    return action.apply(cloudEvents).thenReturn(new ChangeStreamEventAndCloudEvent<>(changeEvent, cloudEvents));
                })
                .flatMap(events -> persistResumeToken(subscriberId, events.changeStreamEvent.getResumeToken()).thenMany(Flux.fromIterable(events.cloudEvents)));
    }

    private Mono<UpdateResult> persistResumeToken(String subscriberId, BsonValue resumeToken) {
        return mongo.upsert(query(where(ID).is(RESUME_TOKEN_DOCUMENT_ID)),
                Update.fromDocument(latestResumeTokenDocument(subscriberId, RESUME_TOKEN_DOCUMENT_ID, resumeToken)),
                resumeTokenCollection);
    }

    @SuppressWarnings("SameParameterValue")
    private static Document latestResumeTokenDocument(String subscriberId, String resumeTokenDocumentId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, resumeTokenDocumentId);
        data.put("subscriberId", subscriberId);
        data.put("resumeToken", resumeToken);
        return new Document(data);
    }

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
        private final List<CloudEventImpl<T>> cloudEvents;

        ChangeStreamEventAndCloudEvent(ChangeStreamEvent<String> changeStreamEvent, List<CloudEventImpl<T>> cloudEvents) {
            this.changeStreamEvent = changeStreamEvent;
            this.cloudEvents = cloudEvents;
        }
    }
}