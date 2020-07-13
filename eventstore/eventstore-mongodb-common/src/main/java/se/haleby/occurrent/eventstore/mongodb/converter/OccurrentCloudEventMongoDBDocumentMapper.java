package se.haleby.occurrent.eventstore.mongodb.converter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.Document;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class OccurrentCloudEventMongoDBDocumentMapper {

    public static final String STREAM_ID = "streamId";
    public static final String CLOUD_EVENT = "cloudEvent";

    public static Stream<Document> convertToDocuments(EventFormat eventFormat, String streamId, Stream<CloudEvent> cloudEvents) {
        return cloudEvents.map(eventFormat::serialize)
                .map(bytes -> new String(bytes, UTF_8))
                .map(Document::parse)
                .map(cloudEvent -> {
                    Map<String, Object> data = new HashMap<>();
                    data.put(STREAM_ID, streamId);
                    data.put(CLOUD_EVENT, cloudEvent);
                    return new Document(data);
                });
    }

    public static EventStream<CloudEvent> convertToCloudEvent(EventFormat eventFormat, EventStream<Document> eventStream) {
        return requireNonNull(eventStream)
                .map(Document::toJson)
                .map(eventJsonString -> eventJsonString.getBytes(UTF_8))
                .map(eventFormat::deserialize);
    }
}