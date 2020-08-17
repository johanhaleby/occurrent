package se.haleby.occurrent.subscription.mongodb.internal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.Document;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper;

import java.util.Optional;

import static com.mongodb.client.model.changestream.OperationType.INSERT;

public class MongoDBCloudEventsToJsonDeserializer {

    public static final String ID = "_id";

    public static Optional<CloudEvent> deserializeToCloudEvent(EventFormat cloudEventSerializer, ChangeStreamDocument<Document> changeStreamDocument, TimeRepresentation timeRepresentation) {
        return changeStreamDocumentToCloudEventAsJson(changeStreamDocument)
                .map(document -> OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    private static Optional<Document> changeStreamDocumentToCloudEventAsJson(ChangeStreamDocument<Document> changeStreamDocument) {
        final Document eventsAsJson;
        OperationType operationType = changeStreamDocument.getOperationType();
        if (operationType == INSERT) {
            eventsAsJson = changeStreamDocument.getFullDocument();
        } else {
            eventsAsJson = null;
        }

        return Optional.ofNullable(eventsAsJson);
    }

}