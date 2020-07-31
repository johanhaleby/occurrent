package se.haleby.occurrent.changestreamer.mongodb.internal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.changestream.OperationType.INSERT;

public class MongoDBCloudEventsToJsonDeserializer {

    public static final String ID = "_id";
    public static final String RESUME_TOKEN = "resumeToken";
    private static final String RESUME_TOKEN_DATA = "_data";

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

    public static Document generateResumeTokenDocument(String subscriptionId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, subscriptionId);
        data.put(RESUME_TOKEN, resumeToken);
        return new Document(data);
    }

    public static ResumeToken extractResumeTokenFromPersistedResumeTokenDocument(Document resumeTokenDocument) {
        Document resumeTokenAsDocument = resumeTokenDocument.get(RESUME_TOKEN, Document.class);
        BsonDocument resumeToken = new BsonDocument(RESUME_TOKEN_DATA, new BsonString(resumeTokenAsDocument.getString(RESUME_TOKEN_DATA)));
        return new ResumeToken(resumeToken);
    }

    public static class ResumeToken {
        private final BsonDocument resumeToken;

        public ResumeToken(BsonDocument resumeToken) {
            this.resumeToken = resumeToken;
        }

        public BsonDocument asBsonDocument() {
            return resumeToken;
        }

        public String asString() {
            return resumeToken.getString(RESUME_TOKEN_DATA).getValue();
        }
    }
}