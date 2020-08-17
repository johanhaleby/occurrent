package se.haleby.occurrent.subscription.mongodb.internal;

import org.bson.*;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.StartAt.StartAtStreamPosition;
import se.haleby.occurrent.subscription.StringBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;

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
        SubscriptionPosition changeStreamPosition = position.changeStreamPosition;
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            BsonDocument resumeToken = ((MongoDBResumeTokenBasedSubscriptionPosition) changeStreamPosition).resumeToken;
            withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            withStartPositionApplied = applyOperationTime.apply(t, ((MongoDBOperationTimeBasedSubscriptionPosition) changeStreamPosition).operationTime);
        } else {
            String changeStreamPositionString = changeStreamPosition.asString();
            if (changeStreamPositionString.contains(RESUME_TOKEN)) {
                BsonDocument bsonDocument = BsonDocument.parse(changeStreamPositionString);
                BsonDocument resumeToken = bsonDocument.getDocument(RESUME_TOKEN);
                withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
            } else if (changeStreamPositionString.contains(OPERATION_TIME)) {
                Document document = Document.parse(changeStreamPositionString);
                BsonTimestamp operationTime = document.get(OPERATION_TIME, BsonTimestamp.class);
                withStartPositionApplied = applyOperationTime.apply(t, operationTime);
            } else {
                throw new IllegalArgumentException("Doesn't recognize stream position " + changeStreamPosition + " as a valid MongoDB stream position");
            }
        }
        return withStartPositionApplied;
    }

    public static StartAt calculateStartAtFromStreamPositionDocument(Document streamPositionDocument) {
        final SubscriptionPosition changeStreamPosition;
        if (streamPositionDocument.containsKey(MongoDBCommons.RESUME_TOKEN)) {
            ResumeToken resumeToken = MongoDBCommons.extractResumeTokenFromPersistedResumeTokenDocument(streamPositionDocument);
            changeStreamPosition = new MongoDBResumeTokenBasedSubscriptionPosition(resumeToken.asBsonDocument());
        } else if (streamPositionDocument.containsKey(MongoDBCommons.OPERATION_TIME)) {
            BsonTimestamp lastOperationTime = MongoDBCommons.extractOperationTimeFromPersistedPositionDocument(streamPositionDocument);
            changeStreamPosition = new MongoDBOperationTimeBasedSubscriptionPosition(lastOperationTime);
        } else if (streamPositionDocument.containsKey(MongoDBCommons.GENERIC_STREAM_POSITION)) {
            String value = streamPositionDocument.getString(MongoDBCommons.GENERIC_STREAM_POSITION);
            changeStreamPosition = new StringBasedSubscriptionPosition(value);
        } else {
            throw new IllegalStateException("Doesn't recognize " + streamPositionDocument + " as a valid stream position document");
        }
        return StartAt.streamPosition(changeStreamPosition);
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
