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
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MongoCommons {

    public static final String RESUME_TOKEN = "resumeToken";
    public static final String OPERATION_TIME = "operationTime";
    public static final String GENERIC_CHECKPOINT = "checkpoint";
    // Legacy field name used before the SubscriptionPosition -> Checkpoint rename. Kept so that documents written
    // by older versions of Occurrent can still be read. New writes never use this field, and because every adapter
    // persists the checkpoint by replacing the whole document (see the storage adapters), the legacy field does not
    // survive the first save after upgrade.
    public static final String LEGACY_GENERIC_CHECKPOINT = "subscriptionPosition";
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

    public static Document generateGenericCheckpointDocument(String subscriptionId, String checkpointAsString) {
        Map<String, Object> data = new HashMap<>();
        data.put(MongoCloudEventsToJsonDeserializer.ID, subscriptionId);
        data.put(GENERIC_CHECKPOINT, checkpointAsString);
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

    public static String cannotFindGlobalCheckpointErrorMessage(Throwable throwable) {
        return "Failed to get global checkpoint from MongoDB, probably because the server doesn't allow to execute the \"hostinfo\" command. " +
                "This only affects the very first event received by the subscription. If the processing of this event fails _and_ the application is restarted " +
                "the event cannot be retried. If this is major concern, consider upgrading your MongoDB server to a non-shared environment that supports the \"hostinfo\" command. Error is:\n" + throwable.getMessage();
    }

    public static BsonTimestamp extractOperationTimeFromPersistedPositionDocument(Document checkpointDocument) {
        return checkpointDocument.get(OPERATION_TIME, BsonTimestamp.class);
    }

    public static <T> T applyStartPosition(T t, BiFunction<T, BsonDocument, T> applyResumeToken, BiFunction<T, BsonTimestamp, T> applyOperationTime, @Nullable StartAt startAt, SubscriptionModelContext ctx) {
        StartAt startAtValue = startAt == null ? null : startAt.get(ctx);
        if (startAtValue == null || startAtValue.isNow() || startAtValue.isDefault()) {
            return t;
        }
        if (!(startAtValue instanceof StartAtCheckpoint position)) {
            throw new IllegalArgumentException("Unrecognized " + StartAt.class.getSimpleName() + " implementation: " + startAtValue.getClass().getName());
        }

        final T withStartPositionApplied;
        Checkpoint changeStreamPosition = position.checkpoint;
        if (changeStreamPosition instanceof MongoResumeTokenCheckpoint mongoResumeTokenCheckpoint) {
            BsonDocument resumeToken = mongoResumeTokenCheckpoint.resumeToken;
            withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
        } else if (changeStreamPosition instanceof MongoOperationTimeCheckpoint mongoOperationTimeCheckpoint) {
            withStartPositionApplied = applyOperationTime.apply(t, mongoOperationTimeCheckpoint.operationTime);
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
                // Unrecognized start position: return t (subscription model default/now) instead of throwing,
                // since a wrapping subscription model may understand a position this one doesn't. For example
                // CatchupSubscription's "TimeBasedCheckpoint" (written when it can't get a global position,
                // e.g. on Atlas free-tier): if no event arrives after catch-up and a restart happens first,
                // CatchupSubscription reads it back instead of replaying from the event store.
                return t;
            }
        }
        return withStartPositionApplied;
    }

    public static Checkpoint calculateCheckpointFromMongoStreamPositionDocument(Document checkpointDocument) {
        final Checkpoint changeStreamPosition;
        if (checkpointDocument.containsKey(MongoCommons.RESUME_TOKEN)) {
            ResumeToken resumeToken = MongoCommons.extractResumeTokenFromPersistedResumeTokenDocument(checkpointDocument);
            changeStreamPosition = new MongoResumeTokenCheckpoint(resumeToken.asBsonDocument());
        } else if (checkpointDocument.containsKey(MongoCommons.OPERATION_TIME)) {
            BsonTimestamp lastOperationTime = MongoCommons.extractOperationTimeFromPersistedPositionDocument(checkpointDocument);
            changeStreamPosition = new MongoOperationTimeCheckpoint(lastOperationTime);
        } else if (checkpointDocument.containsKey(MongoCommons.GENERIC_CHECKPOINT)) {
            String value = checkpointDocument.getString(MongoCommons.GENERIC_CHECKPOINT);
            changeStreamPosition = new StringBasedCheckpoint(value);
        } else if (checkpointDocument.containsKey(MongoCommons.LEGACY_GENERIC_CHECKPOINT)) {
            // One-time backward-compatible read: documents written before the SubscriptionPosition -> Checkpoint
            // rename stored the generic checkpoint value under the legacy "subscriptionPosition" field. Fall back
            // to reading it so that existing subscriptions don't lose their position. The next successful write
            // replaces the whole document under the new "checkpoint" field, so the legacy field does not survive.
            String value = checkpointDocument.getString(MongoCommons.LEGACY_GENERIC_CHECKPOINT);
            changeStreamPosition = new StringBasedCheckpoint(value);
        } else {
            throw new IllegalStateException("Doesn't recognize " + checkpointDocument + " as a valid checkpoint document");
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
