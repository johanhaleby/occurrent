package se.haleby.occurrent.changestreamer.mongodb.spring.reactive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

    public Flux<CloudEventImpl<T>> withChanges(Function<CloudEventImpl<T>, Mono<Void>> action) {
        return mongo.changeStream(String.class)
                .watchCollection(eventCollection)
                .listen()
                .flatMap(changeEvent -> {
                    Flux<String> strings = extractCloudEventString(changeEvent.getBody());
                    return strings
                            // @formatter:off
                            .map(body -> Json.decodeValue(body, new TypeReference<CloudEventImpl<T>>() {}))
                            // @formatter:on
                            .flatMap(e -> action.apply(e).thenReturn(new Object() {
                                private final ChangeStreamEvent<String> changeStreamEvent = changeEvent;
                                private final CloudEventImpl<T> cloudEvent = e;
                            }));

                })
                .flatMap(tuple ->
                        mongo.upsert(query(where(ID).is(RESUME_TOKEN_DOCUMENT_ID)),
                                Update.fromDocument(latestResumeTokenDocument(RESUME_TOKEN_DOCUMENT_ID, tuple.changeStreamEvent.getResumeToken())),
                                resumeTokenCollection).map(__ -> tuple.cloudEvent)
                );
    }

    private Flux<String> extractCloudEventString(String jsonString) {
        try {
            return Flux.fromIterable(new ObjectMapper().readTree(jsonString).path("events"))
                    .map(JsonNode::asText);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static Document latestResumeTokenDocument(String resumeTokenDocumentId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, resumeTokenDocumentId);
        data.put("resumeToken", resumeToken);
        return new Document(data);
    }
}