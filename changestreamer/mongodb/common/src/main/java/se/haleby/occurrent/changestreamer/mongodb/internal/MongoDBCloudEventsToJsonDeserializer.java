package se.haleby.occurrent.changestreamer.mongodb.internal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.*;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.changestream.OperationType.INSERT;

public class MongoDBCloudEventsToJsonDeserializer {

    public static final String ID = "_id";
    public static final String RESUME_TOKEN = "resumeToken";
    public static final String OPERATION_TIME = "operationTime";
    public static final String GENERIC_STREAM_POSITION = "streamPosition";
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

    public static Document generateResumeTokenStreamPositionDocument(String subscriptionId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, subscriptionId);
        data.put(RESUME_TOKEN, resumeToken);
        return new Document(data);
    }

    public static Document generateOperationTimeStreamPositionDocument(String subscriptionId, BsonTimestamp operationTime) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, subscriptionId);
        data.put(OPERATION_TIME, operationTime);
        return new Document(data);
    }

    public static Document generateGenericStreamPositionDocument(String subscriptionId, String streamPositionAsString) {
        Map<String, Object> data = new HashMap<>();
        data.put(ID, subscriptionId);
        data.put(GENERIC_STREAM_POSITION, streamPositionAsString);
        return new Document(data);
    }

    public static BsonTimestamp getServerOperationTime(Document hostInfoDocument) {
        return (BsonTimestamp) hostInfoDocument.get(OPERATION_TIME);
    }

    public static ResumeToken extractResumeTokenFromPersistedResumeTokenDocument(Document resumeTokenDocument) {
        Document resumeTokenAsDocument = resumeTokenDocument.get(RESUME_TOKEN, Document.class);
        BsonDocument resumeToken = new BsonDocument(RESUME_TOKEN_DATA, new BsonString(resumeTokenAsDocument.getString(RESUME_TOKEN_DATA)));
        return new ResumeToken(resumeToken);
    }

    public static BsonTimestamp extractOperationTimeFromPersistedPositionDocument(Document streamPositionDocument) {
        return streamPositionDocument.get(OPERATION_TIME, BsonTimestamp.class);
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