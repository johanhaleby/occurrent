package se.haleby.occurrent.eventstore.mongodb.converter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import org.bson.Document;

import java.time.OffsetDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;

public class OccurrentCloudEventMongoDBDocumentMapper {

    public static Document convertToDocument(EventFormat eventFormat, String streamId, CloudEvent cloudEvent) {
        CloudEvent fixed = fixTimestamp(cloudEvent);
        byte[] bytes = eventFormat.serialize(fixed);
        String serializedAsString = new String(bytes, UTF_8);
        Document cloudEventDocument = Document.parse(serializedAsString);
        cloudEventDocument.put(STREAM_ID, streamId);
        return cloudEventDocument;
    }

    // Creates a workaround for issue 200: https://github.com/cloudevents/sdk-java/issues/200
    // Remove when milestone 2 is released
    private static CloudEvent fixTimestamp(CloudEvent cloudEvent) {
        if (cloudEvent.getTime() == null) {
            return cloudEvent;
        }
        return CloudEventBuilder.v1(cloudEvent)
                .withTime(OffsetDateTime.from(cloudEvent.getTime()).toZonedDateTime())
                .build();
    }

    public static CloudEvent convertToCloudEvent(EventFormat eventFormat, Document cloudEventDocument) {
        Document document = new Document(cloudEventDocument);
        document.remove("_id");
        String eventJsonString = document.toJson();
        byte[] eventJsonBytes = eventJsonString.getBytes(UTF_8);
        return eventFormat.deserialize(eventJsonBytes);
    }
}