package se.haleby.occurrent.changestreamer.mongodb.common;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MongoDBCloudEventsToJsonDeserializer {

    public static final String ID = "_id";
    public static final String RESUME_TOKEN = "resumeToken";
    private static final String RESUME_TOKEN_DATA = "_data";
    private static final String OCCURRENT_STREAM_ID = "occurrentStreamId";

    public static Optional<CloudEvent> deserializeToCloudEvent(EventFormat cloudEventSerializer, ChangeStreamDocument<Document> changeStreamDocument) {
        return changeStreamDocumentToCloudEventAsJson(changeStreamDocument)
                .map(cloudEventString -> cloudEventString.getBytes(UTF_8))
                .map(cloudEventSerializer::deserialize);
    }

    @SuppressWarnings("ConstantConditions")
    private static Optional<String> changeStreamDocumentToCloudEventAsJson(ChangeStreamDocument<Document> changeStreamDocument) {
        final String eventsAsJson;
        OperationType operationType = changeStreamDocument.getOperationType();
        if (operationType == INSERT) {
            // This is when the first event(s) are written to the event store for a particular stream id
            Document fullDocument = changeStreamDocument.getFullDocument();
            // Remove stream id
            fullDocument.remove(OCCURRENT_STREAM_ID);
            eventsAsJson = fullDocument.toJson();
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