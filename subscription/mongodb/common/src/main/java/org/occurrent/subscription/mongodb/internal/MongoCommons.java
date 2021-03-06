/*
 * Copyright 2021 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.mongodb.internal;

import org.bson.*;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MongoCommons {

    public static final String RESUME_TOKEN = "resumeToken";
    public static final String OPERATION_TIME = "operationTime";
    public static final String GENERIC_SUBSCRIPTION_POSITION = "subscriptionPosition";
    static final String RESUME_TOKEN_DATA = "_data";
    public static final int CHANGE_STREAM_HISTORY_LOST_ERROR_CODE = 286;

    public static Document generateResumeTokenStreamPositionDocument(String subscriptionId, BsonValue resumeToken) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoCloudEventsToJsonDeserializer.ID, subscriptionId);
        data.put(RESUME_TOKEN, resumeToken);
        return new Document(data);
    }

    public static Document generateOperationTimeStreamPositionDocument(String subscriptionId, BsonTimestamp operationTime) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoCloudEventsToJsonDeserializer.ID, subscriptionId);
        data.put(OPERATION_TIME, operationTime);
        return new Document(data);
    }

    public static Document generateGenericSubscriptionPositionDocument(String subscriptionId, String subscriptionPositionAsString) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoCloudEventsToJsonDeserializer.ID, subscriptionId);
        data.put(GENERIC_SUBSCRIPTION_POSITION, subscriptionPositionAsString);
        return new Document(data);
    }

    public static BsonTimestamp getServerOperationTime(Document hostInfoDocument) {
        return getServerOperationTime(hostInfoDocument, 0);
    }

    public static BsonTimestamp getServerOperationTime(Document hostInfoDocument, int increaseIncrementBy) {
        BsonTimestamp bsonTimestamp = (BsonTimestamp) hostInfoDocument.get(OPERATION_TIME);
        return increaseIncrementBy > 0 ? new BsonTimestamp(bsonTimestamp.getTime(), bsonTimestamp.getInc() + increaseIncrementBy) : bsonTimestamp;
    }

    public static ResumeToken extractResumeTokenFromPersistedResumeTokenDocument(Document resumeTokenDocument) {
        Document resumeTokenAsDocument = resumeTokenDocument.get(RESUME_TOKEN, Document.class);
        BsonDocument resumeToken = new BsonDocument(RESUME_TOKEN_DATA, new BsonString(resumeTokenAsDocument.getString(RESUME_TOKEN_DATA)));
        return new ResumeToken(resumeToken);
    }

    public static String cannotFindGlobalSubscriptionPositionErrorMessage(Throwable throwable) {
        return "Failed to get global subscription position from MongoDB, probably because the server doesn't allow to execute the \"hostinfo\" command. " +
                "This only affects the very first event received by the subscription. If the processing of this event fails _and_ the application is restarted " +
                "the event cannot be retried. If this is major concern, consider upgrading your MongoDB server to a non-shared environment that supports the \"hostinfo\" command. Error is:\n" + throwable.getMessage();
    }

    public static BsonTimestamp extractOperationTimeFromPersistedPositionDocument(Document subscriptionPositionDocument) {
        return subscriptionPositionDocument.get(OPERATION_TIME, BsonTimestamp.class);
    }

    public static <T> T applyStartPosition(T t, BiFunction<T, BsonDocument, T> applyResumeToken, BiFunction<T, BsonTimestamp, T> applyOperationTime, StartAt startAt) {
        StartAt startAtValue = startAt.get();
        if (startAtValue.isNow() || startAtValue.isDefault()) {
            return t;
        } else if (!(startAtValue instanceof StartAtSubscriptionPosition)) {
            throw new IllegalArgumentException("Unrecognized " + StartAt.class.getSimpleName() + " implementation: " + startAtValue.getClass().getName());
        }

        final T withStartPositionApplied;
        StartAtSubscriptionPosition position = (StartAtSubscriptionPosition) startAtValue;
        SubscriptionPosition changeStreamPosition = position.subscriptionPosition;
        if (changeStreamPosition instanceof MongoResumeTokenSubscriptionPosition) {
            BsonDocument resumeToken = ((MongoResumeTokenSubscriptionPosition) changeStreamPosition).resumeToken;
            withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
        } else if (changeStreamPosition instanceof MongoOperationTimeSubscriptionPosition) {
            withStartPositionApplied = applyOperationTime.apply(t, ((MongoOperationTimeSubscriptionPosition) changeStreamPosition).operationTime);
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
                throw new IllegalArgumentException("Doesn't recognize subscription position " + changeStreamPosition + " as a valid MongoDB subscription position");
            }
        }
        return withStartPositionApplied;
    }

    public static SubscriptionPosition calculateSubscriptionPositionFromMongoStreamPositionDocument(Document subscriptionPositionDocument) {
        final SubscriptionPosition changeStreamPosition;
        if (subscriptionPositionDocument.containsKey(MongoCommons.RESUME_TOKEN)) {
            ResumeToken resumeToken = MongoCommons.extractResumeTokenFromPersistedResumeTokenDocument(subscriptionPositionDocument);
            changeStreamPosition = new MongoResumeTokenSubscriptionPosition(resumeToken.asBsonDocument());
        } else if (subscriptionPositionDocument.containsKey(MongoCommons.OPERATION_TIME)) {
            BsonTimestamp lastOperationTime = MongoCommons.extractOperationTimeFromPersistedPositionDocument(subscriptionPositionDocument);
            changeStreamPosition = new MongoOperationTimeSubscriptionPosition(lastOperationTime);
        } else if (subscriptionPositionDocument.containsKey(MongoCommons.GENERIC_SUBSCRIPTION_POSITION)) {
            String value = subscriptionPositionDocument.getString(MongoCommons.GENERIC_SUBSCRIPTION_POSITION);
            changeStreamPosition = new StringBasedSubscriptionPosition(value);
        } else {
            throw new IllegalStateException("Doesn't recognize " + subscriptionPositionDocument + " as a valid subscription position document");
        }
        return changeStreamPosition;
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
