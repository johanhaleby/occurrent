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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MongoDBCloudEventsToJsonDeserializer {

    public static final String ID = "_id";
    public static final String RESUME_TOKEN = "resumeToken";
    private static final String RESUME_TOKEN_DATA = "_data";

    public static List<CloudEvent> deserializeToCloudEvents(EventFormat cloudEventSerializer, ChangeStreamDocument<Document> changeStreamDocument) {
        return changeStreamDocumentToCloudEventsAsJson(changeStreamDocument)
                .map(cloudEventString -> cloudEventString.getBytes(UTF_8))
                .map(cloudEventSerializer::deserialize)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("ConstantConditions")
    private static Stream<String> changeStreamDocumentToCloudEventsAsJson(ChangeStreamDocument<Document> changeStreamDocument) {
        final Stream<String> eventsAsJson;
        OperationType operationType = changeStreamDocument.getOperationType();
        if (operationType == INSERT) {
            // This is when the first event(s) are written to the event store for a particular stream id
            eventsAsJson = changeStreamDocument.getFullDocument().getList("events", Document.class).stream().map(Document::toJson);
        } else if (operationType == UPDATE) {
            // When events already exists for a stream id we get an update operation. To only get the events
            // that are updated we get the "updated fields" and extract only the events ("version" is also updated but
            // we don't care about it here).
            eventsAsJson = changeStreamDocument.getUpdateDescription().getUpdatedFields().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith("events"))
                    .map(Entry::getValue)
                    .map(BsonValue::asDocument)
                    .map(BsonDocument::toJson);
        } else {
            eventsAsJson = Stream.empty();
        }

        return eventsAsJson;
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