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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;

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
    /**
     * The field a {@link CheckpointWriteCondition} is evaluated against and recorded into. See ADR 116.
     */
    public static final String WRITE_VERSION = "version";

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

    /**
     * Builds the document a {@link Checkpoint} is stored as, dispatching on its recognized subtypes the same way
     * {@link #calculateCheckpointFromMongoStreamPositionDocument(Document)} reads them back.
     */
    public static Document generateCheckpointDocument(String subscriptionId, Checkpoint checkpoint) {
        final Document document;
        if (checkpoint instanceof MongoResumeTokenCheckpoint mongoResumeTokenCheckpoint) {
            document = generateResumeTokenStreamPositionDocument(subscriptionId, mongoResumeTokenCheckpoint.resumeToken);
        } else if (checkpoint instanceof MongoOperationTimeCheckpoint mongoOperationTimeCheckpoint) {
            document = generateOperationTimeStreamPositionDocument(subscriptionId, mongoOperationTimeCheckpoint.operationTime);
        } else {
            document = generateGenericCheckpointDocument(subscriptionId, checkpoint.asString());
        }
        return document;
    }

    /**
     * The single aggregation pipeline stage a conditional checkpoint write is done as. See ADR 116, "Storage
     * mechanics". Matched with a filter on {@code _id} alone against the unique index, with {@code upsert(true)}
     * and {@code returnDocument(AFTER)}, this is one round trip. The stage replaces the whole document with
     * {@code newCheckpointDocument} merged with the version the condition says to write when {@code condition}
     * allows it, and otherwise yields {@code $$ROOT}, the document unchanged.
     * <p>
     * {@code newCheckpointDocument} is wrapped in {@code $literal} because an update pipeline evaluates what it
     * writes, unlike a replace, so a subscription id or checkpoint string starting with {@code $} would otherwise be
     * read as a field path rather than a value.
     * <p>
     * The merge order, new document first and version second, is what keeps a stale {@code resumeToken} or the
     * legacy {@code subscriptionPosition} field from surviving next to a freshly written field, since only the
     * version key is added on top of a full replacement, never the previous document's other fields.
     *
     * @param newCheckpointDocument The document {@link #generateCheckpointDocument(String, Checkpoint)} built for the checkpoint being written
     * @param condition             The condition the write is subject to
     * @return The {@code $replaceWith} pipeline stage to run through {@code findOneAndUpdate}
     */
    public static Document buildConditionalCheckpointWrite(Document newCheckpointDocument, CheckpointWriteCondition condition) {
        WriteDecision decision = switch (condition) {
            // Always allowed, the stored version (if any) carries forward untouched.
            case CheckpointWriteCondition.Any any -> new WriteDecision(Boolean.TRUE, versionCarriedForwardExpr());
            case CheckpointWriteCondition.NotOlderThan notOlderThan ->
                    new WriteDecision(notOlderThanIsAllowedExpr(notOlderThan.writeVersion()), notOlderThan.writeVersion());
            case CheckpointWriteCondition.IfAbsent ifAbsent -> new WriteDecision(ifAbsentIsAllowedExpr(), versionCarriedForwardExpr());
        };

        Document newDocumentWithVersion = new Document("$mergeObjects", asList(
                new Document("$literal", newCheckpointDocument),
                new Document(WRITE_VERSION, decision.versionToWrite())));

        Document cond = new Document("$cond", new Document(Map.of(
                "if", decision.allow(),
                "then", newDocumentWithVersion,
                "else", "$$ROOT")));

        return new Document("$replaceWith", cond);
    }

    /**
     * What {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)} decided for a
     * {@link CheckpointWriteCondition}. The aggregation expression that gates the write, and the value to write into
     * {@link #WRITE_VERSION} when it fires.
     */
    private record WriteDecision(Object allow, Object versionToWrite) {
    }

    private static Document notOlderThanIsAllowedExpr(long writeVersion) {
        return new Document("$or", asList(
                fieldIsMissingExpr(WRITE_VERSION),
                new Document("$lte", asList("$" + WRITE_VERSION, writeVersion))));
    }

    /**
     * {@code ifAbsent} is gated on whether a checkpoint is stored, not on whether a version is. A checkpoint written
     * by {@code any()} before any conditional write ever happened has no version yet but is very much stored.
     */
    private static Document ifAbsentIsAllowedExpr() {
        return new Document("$not", List.of(new Document("$or", asList(
                fieldExistsExpr(RESUME_TOKEN),
                fieldExistsExpr(OPERATION_TIME),
                fieldExistsExpr(GENERIC_CHECKPOINT),
                fieldExistsExpr(LEGACY_GENERIC_CHECKPOINT)))));
    }

    private static Document fieldExistsExpr(String field) {
        return new Document("$ne", asList(new Document("$type", "$" + field), "missing"));
    }

    private static Document fieldIsMissingExpr(String field) {
        return new Document("$eq", asList(new Document("$type", "$" + field), "missing"));
    }

    /**
     * {@code any()} and a successful {@code ifAbsent()} both leave the stored version exactly as it was. Present
     * stays present, and missing stays missing rather than becoming a stored zero. {@code $$REMOVE} is what leaves a
     * field out of the document a pipeline stage writes.
     */
    private static Document versionCarriedForwardExpr() {
        return new Document("$ifNull", asList("$" + WRITE_VERSION, "$$REMOVE"));
    }

    /**
     * Reads the version {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)} recorded, or
     * empty if the document has none.
     */
    public static OptionalLong extractWriteVersion(@Nullable Document document) {
        if (document == null || !document.containsKey(WRITE_VERSION)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(document.get(WRITE_VERSION, Number.class).longValue());
    }

    /**
     * Throws {@link CheckpointWriteConditionNotFulfilledException} unless {@code afterDocument}, the document
     * {@code findOneAndUpdate} returned with {@code returnDocument(AFTER)}, shows that {@code condition} allowed the
     * write {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)} attempted.
     * <p>
     * {@code any()} never refuses, so it is not checked. {@code notOlderThan(v)} is told apart by comparing the
     * version on {@code afterDocument} to {@code v}. The pipeline stamps exactly {@code v} on success, and a refused
     * write leaves the higher stored version untouched, so the two never coincide. {@code ifAbsent()} is told apart
     * by comparing the checkpoint value on {@code afterDocument} to the one offered, since the pipeline only ever
     * leaves a different value in place when it refused the write. Two {@code ifAbsent()} writes offering the exact
     * same checkpoint value back to back are indistinguishable this way, the second is read as success rather than
     * a refusal, though the stored value ends up the same either way.
     *
     * @param subscriptionId The id of the subscription the write was for
     * @param checkpoint     The checkpoint the write offered
     * @param condition      The condition the write was subject to
     * @param afterDocument  The document {@code findOneAndUpdate} returned
     */
    public static void assertCheckpointWriteSucceeded(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition, Document afterDocument) {
        boolean succeeded = switch (condition) {
            case CheckpointWriteCondition.Any any -> true;
            case CheckpointWriteCondition.NotOlderThan notOlderThan -> {
                OptionalLong storedVersion = extractWriteVersion(afterDocument);
                yield storedVersion.isPresent() && storedVersion.getAsLong() == notOlderThan.writeVersion();
            }
            case CheckpointWriteCondition.IfAbsent ifAbsent -> {
                String storedCheckpointValue = calculateCheckpointFromMongoStreamPositionDocument(afterDocument).asString();
                yield storedCheckpointValue.equals(checkpoint.asString());
            }
        };
        if (!succeeded) {
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, extractWriteVersion(afterDocument), condition);
        }
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

    /**
     * Runs everything {@link #applyStartPosition(Object, BiFunction, BiFunction, StartAt, SubscriptionModelContext)}
     * does to work out a start position, and throws whatever that would have thrown, without applying the result to
     * anything.
     * <p>
     * It exists so a subscription model can refuse a start position it cannot make sense of from {@code subscribe},
     * rather than from a background thread or a deferred pipeline where nobody is listening and a retry re-throws it
     * forever. A {@link Checkpoint} is only a string on the way back out of storage, and a caller may write one by
     * hand, so a value this cannot parse is reachable through published API rather than hypothetical.
     * <p>
     * A dynamic {@link StartAt} is a no-op here, and that rule lives in this method rather than in a condition each
     * caller has to remember: resolving one means calling the caller's own function, the model calls it again when it
     * actually subscribes, and calling an arbitrary caller's function twice to validate it is worse than not checking
     * it. Leaving that to the call sites made it a precondition two models had to keep honouring, and a third would
     * have had to rediscover.
     */
    public static void checkStartPosition(@Nullable StartAt startAt, SubscriptionModelContext ctx) {
        if (startAt == null || startAt.isDynamic()) {
            return;
        }
        applyStartPosition(NOTHING, (nothing, resumeToken) -> nothing, (nothing, operationTime) -> nothing, startAt, ctx);
    }

    /**
     * Stands in for the object a start position would be applied to, so {@link #checkStartPosition} can reuse the
     * whole of {@code applyStartPosition} rather than growing a second copy of its parsing that could drift from it.
     */
    private static final Object NOTHING = new Object();

    public static <T> T applyStartPosition(T t, BiFunction<T, BsonDocument, T> applyResumeToken, BiFunction<T, BsonTimestamp, T> applyOperationTime, @Nullable StartAt startAt, SubscriptionModelContext ctx) {
        StartAt startAtValue = startAt == null ? null : startAt.get(ctx);
        if (startAtValue == null || startAtValue.isNow() || startAtValue.isDefault()) {
            return t;
        }
        if (!(startAtValue instanceof StartAtCheckpoint position)) {
            throw new UnsupportedStartAtException(startAtValue, "Unrecognized " + StartAt.class.getSimpleName() + " implementation: " + startAtValue.getClass().getName());
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
