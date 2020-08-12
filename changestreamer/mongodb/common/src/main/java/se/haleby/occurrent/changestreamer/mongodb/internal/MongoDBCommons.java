package se.haleby.occurrent.changestreamer.mongodb.internal;

import org.bson.*;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.StartAt.StartAtStreamPosition;
import se.haleby.occurrent.changestreamer.StreamPosition;
import se.haleby.occurrent.changestreamer.StringBasedStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBOperationTimeBasedStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedStreamPosition;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MongoDBCommons {

    public static final String RESUME_TOKEN = "resumeToken";
    public static final String OPERATION_TIME = "operationTime";
    public static final String GENERIC_STREAM_POSITION = "streamPosition";
    static final String RESUME_TOKEN_DATA = "_data";

    public static Document generateResumeTokenStreamPositionDocument(String subscriptionId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoDBCloudEventsToJsonDeserializer.ID, subscriptionId);
        data.put(RESUME_TOKEN, resumeToken);
        return new Document(data);
    }

    public static Document generateOperationTimeStreamPositionDocument(String subscriptionId, BsonTimestamp operationTime) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoDBCloudEventsToJsonDeserializer.ID, subscriptionId);
        data.put(OPERATION_TIME, operationTime);
        return new Document(data);
    }

    public static Document generateGenericStreamPositionDocument(String subscriptionId, String streamPositionAsString) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoDBCloudEventsToJsonDeserializer.ID, subscriptionId);
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

    public static <T> T applyStartPosition(T t, BiFunction<T, BsonDocument, T> applyResumeToken, BiFunction<T, BsonTimestamp, T> applyOperationTime, StartAt startAt) {
        if (startAt.isNow()) {
            return t;
        }

        final T withStartPositionApplied;
        StartAtStreamPosition position = (StartAtStreamPosition) startAt;
        StreamPosition streamPosition = position.streamPosition;
        if (streamPosition instanceof MongoDBResumeTokenBasedStreamPosition) {
            BsonDocument resumeToken = ((MongoDBResumeTokenBasedStreamPosition) streamPosition).resumeToken;
            withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
        } else if (streamPosition instanceof MongoDBOperationTimeBasedStreamPosition) {
            withStartPositionApplied = applyOperationTime.apply(t, ((MongoDBOperationTimeBasedStreamPosition) streamPosition).operationTime);
        } else {
            Document document = Document.parse(streamPosition.asString());
            if (document.containsKey(RESUME_TOKEN)) {
                BsonDocument resumeToken = document.get(RESUME_TOKEN, BsonDocument.class);
                withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
            } else if (document.containsKey(OPERATION_TIME)) {
                BsonTimestamp operationTime = document.get(RESUME_TOKEN, BsonTimestamp.class);
                withStartPositionApplied = applyOperationTime.apply(t, operationTime);
            } else {
                throw new IllegalArgumentException("Doesn't recognize stream position " + streamPosition + " as a valid MongoDB stream position");
            }
        }
        return withStartPositionApplied;
    }

    public static StartAt calculateStartAtFromStreamPositionDocument(Document streamPositionDocument) {
        final StreamPosition streamPosition;
        if (streamPositionDocument.containsKey(MongoDBCommons.RESUME_TOKEN)) {
            ResumeToken resumeToken = MongoDBCommons.extractResumeTokenFromPersistedResumeTokenDocument(streamPositionDocument);
            streamPosition = new MongoDBResumeTokenBasedStreamPosition(resumeToken.asBsonDocument());
        } else if (streamPositionDocument.containsKey(MongoDBCommons.OPERATION_TIME)) {
            BsonTimestamp lastOperationTime = MongoDBCommons.extractOperationTimeFromPersistedPositionDocument(streamPositionDocument);
            streamPosition = new MongoDBOperationTimeBasedStreamPosition(lastOperationTime);
        } else if (streamPositionDocument.containsKey(MongoDBCommons.GENERIC_STREAM_POSITION)) {
            String value = streamPositionDocument.getString(MongoDBCommons.GENERIC_STREAM_POSITION);
            streamPosition = new StringBasedStreamPosition(value);
        } else {
            throw new IllegalStateException("Doesn't recognize " + streamPositionDocument + " as a valid stream position document");
        }
        return StartAt.streamPosition(streamPosition);
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
