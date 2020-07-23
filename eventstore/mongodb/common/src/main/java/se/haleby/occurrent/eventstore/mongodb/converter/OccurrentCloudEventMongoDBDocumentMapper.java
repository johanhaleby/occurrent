package se.haleby.occurrent.eventstore.mongodb.converter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.Document;

import static java.nio.charset.StandardCharsets.UTF_8;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;

public class OccurrentCloudEventMongoDBDocumentMapper {

    public static Document convertToDocument(EventFormat eventFormat, String streamId, CloudEvent cloudEvent) {
        byte[] bytes = eventFormat.serialize(cloudEvent);
        String serializedAsString = new String(bytes, UTF_8);
        Document cloudEventDocument = Document.parse(serializedAsString);
        cloudEventDocument.put(STREAM_ID, streamId);
        return cloudEventDocument;
    }

    public static CloudEvent convertToCloudEvent(EventFormat eventFormat, Document cloudEventDocument) {
        String eventJsonString = cloudEventDocument.toJson();
        byte[] eventJsonBytes = eventJsonString.getBytes(UTF_8);
        return eventFormat.deserialize(eventJsonBytes);

    }
}