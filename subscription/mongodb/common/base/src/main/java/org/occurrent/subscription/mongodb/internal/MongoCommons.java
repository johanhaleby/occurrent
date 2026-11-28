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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import org.bson.*;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.internal.SharedStorageMessages;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class MongoCommons {

    public static final String RESUME_TOKEN = "resumeToken";
    public static final String OPERATION_TIME = OperationTimeLiveStart.OPERATION_TIME;
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
    /**
     * The field a catch-up's live start is stored in next to {@link #GENERIC_CHECKPOINT}, see {@link GlobalCheckpoint#liveFrom()}
     * and {@link CatchupTimeCheckpoint#liveFrom()}.
     */
    public static final String CATCHUP_LIVE_FROM = "catchupLiveFrom";
    /**
     * The field a catch-up's replay origin is stored in next to {@link #GENERIC_CHECKPOINT}, see {@link GlobalCheckpoint#replayOrigin()}.
     * A position catch-up stores it as a number, and a time catch-up as an RFC 3339 string, see
     * {@link CatchupTimeCheckpoint#replayOrigin()}.
     */
    public static final String CATCHUP_REPLAY_ORIGIN = "catchupReplayOrigin";
    /**
     * The field a catch-up's replay end is stored in next to {@link #GENERIC_CHECKPOINT}, see {@link GlobalCheckpoint#replayTo()}.
     */
    public static final String CATCHUP_REPLAY_TO = "catchupReplayTo";
    /**
     * The field the two-argument {@link #buildFirstCheckpointRaceResolution(Document)} sets when it writes a position
     * over a stored checkpoint, and that {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)}
     * refuses a {@link CheckpointWriteCondition#notOlderThan(long)} write of a change stream position for while it is
     * stored. A node holding the version stored with the later checkpoint may already have delivered from it, and its
     * next save would skip the events between the two positions. Any write that replaces the document drops it.
     */
    public static final String POSITION_MOVED_EARLIER = "positionMovedEarlier";
    /**
     * The operation time the stored checkpoint's line of delivery began at. A write over nothing stored sets it when it
     * writes an operation time, every other conditional write keeps it, and a first-position race resolution that
     * writes its candidate sets it to the candidate's operation time. The race resolutions count a stored resume token
     * or generic checkpoint whose origin is later than the candidate as later than the candidate. A document an older
     * Occurrent version stored has none, and neither has one whose first write wasn't an operation time.
     */
    public static final String LINEAGE_ORIGIN = "lineageOrigin";

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
            GlobalCheckpoint withLiveStart = globalCheckpointWithLiveStart(checkpoint);
            if (withLiveStart != null) {
                // The live start goes in fields of its own, so a version that knows only the plain position reads
                // "checkpoint" as before and ignores them. A live start at the top level would be read as the
                // subscription's own change-stream position and skip the rest of the replay.
                document = generateGenericCheckpointDocument(subscriptionId, GlobalCheckpoint.of(withLiveStart.position()).asString());
                document.put(CATCHUP_LIVE_FROM, liveFromDocument(subscriptionId, withLiveStart.liveFrom().orElseThrow()));
                document.put(CATCHUP_REPLAY_ORIGIN, withLiveStart.replayOrigin().orElseThrow());
                document.put(CATCHUP_REPLAY_TO, withLiveStart.replayTo().orElseThrow());
            } else if (CatchupTimeCheckpoint.isCatchupTimeCheckpoint(checkpoint)) {
                // The same fields as a position, with the time in "checkpoint" and the replay origin as a time, so a
                // version that knows only the plain time reads "checkpoint" as before
                CatchupTimeCheckpoint withLiveStartAtTime = CatchupTimeCheckpoint.parse(checkpoint);
                document = generateGenericCheckpointDocument(subscriptionId, withLiveStartAtTime.time());
                document.put(CATCHUP_LIVE_FROM, liveFromDocument(subscriptionId, withLiveStartAtTime.liveFrom()));
                document.put(CATCHUP_REPLAY_ORIGIN, withLiveStartAtTime.replayOrigin());
            } else {
                document = generateGenericCheckpointDocument(subscriptionId, checkpoint.asString());
            }
        }
        return document;
    }

    private static Document liveFromDocument(String subscriptionId, Checkpoint liveFrom) {
        Document liveFromDocument = generateCheckpointDocument(subscriptionId, typedLiveStart(liveFrom));
        liveFromDocument.remove(MongoCloudEventsToJsonDeserializer.ID);
        return liveFromDocument;
    }

    private static @Nullable GlobalCheckpoint globalCheckpointWithLiveStart(Checkpoint checkpoint) {
        if (!GlobalCheckpoint.isGlobalCheckpoint(checkpoint)) {
            return null;
        }
        final GlobalCheckpoint global;
        try {
            global = GlobalCheckpoint.parse(checkpoint);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return global.liveFrom().isPresent() ? global : null;
    }

    // A live start read back from a storage that keeps strings is a StringBasedCheckpoint holding the JSON a
    // MongoResumeTokenCheckpoint or a MongoOperationTimeCheckpoint writes
    private static Checkpoint typedLiveStart(Checkpoint liveFrom) {
        if (liveFrom instanceof MongoResumeTokenCheckpoint || liveFrom instanceof MongoOperationTimeCheckpoint) {
            return liveFrom;
        }
        String value = liveFrom.asString();
        try {
            if (value.contains(RESUME_TOKEN)) {
                return new MongoResumeTokenCheckpoint(BsonDocument.parse(value).getDocument(RESUME_TOKEN));
            } else if (value.contains(OPERATION_TIME)) {
                BsonTimestamp operationTime = OperationTimeLiveStart.parse(value);
                if (operationTime != null) {
                    return new MongoOperationTimeCheckpoint(operationTime);
                }
            }
        } catch (RuntimeException e) {
            return liveFrom;
        }
        return liveFrom;
    }

    /**
     * The {@code aggregate} command that opens a change stream on {@code collectionName} at {@code checkpoint} and
     * returns at most one change, for a subscription model's {@code canResumeFrom(..)}. MongoDB answers it with
     * {@link #CHANGE_STREAM_HISTORY_LOST_ERROR_CODE} when the oplog no longer reaches back to {@code checkpoint}, once
     * the oplog has been truncated past it. Empty when {@code checkpoint} holds neither a resume token nor an operation
     * time, since a change stream then opens at the present and needs no history.
     */
    public static Optional<Document> changeStreamHistoryProbe(String collectionName, Checkpoint checkpoint) {
        Document changeStream = applyResolvedStartPosition(new Document(),
                (document, resumeToken) -> new Document("startAfter", resumeToken),
                (document, operationTime) -> new Document("startAtOperationTime", operationTime),
                StartAt.checkpoint(checkpoint));
        if (changeStream.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Document("aggregate", collectionName)
                .append("pipeline", List.of(new Document("$changeStream", changeStream)))
                .append("cursor", new Document("batchSize", 1)));
    }

    /**
     * The {@code killCursors} command for the cursor {@code reply}, the reply to a
     * {@link #changeStreamHistoryProbe(String, Checkpoint)}, left open, or empty when the reply left none open.
     */
    public static Optional<Document> killChangeStreamHistoryProbeCursor(String collectionName, Document reply) {
        Document cursor = reply.get("cursor", Document.class);
        Number cursorId = cursor == null ? null : cursor.get("id", Number.class);
        if (cursorId == null || cursorId.longValue() == 0) {
            return Optional.empty();
        }
        return Optional.of(new Document("killCursors", collectionName).append("cursors", List.of(cursorId.longValue())));
    }

    /**
     * Whether {@code throwable} is MongoDB saying that a change stream cannot open at the position it was asked to,
     * because the oplog no longer reaches back to it.
     */
    public static boolean isChangeStreamHistoryLost(Throwable throwable) {
        return hasCommandErrorCode(throwable, CHANGE_STREAM_HISTORY_LOST_ERROR_CODE);
    }

    private static boolean hasCommandErrorCode(Throwable throwable, int errorCode) {
        // Spring translates the driver's exception into one of its own and keeps the driver's as the cause
        for (Throwable cause = throwable; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause instanceof MongoCommandException mongoCommandException && mongoCommandException.getErrorCode() == errorCode) {
                return true;
            }
        }
        return false;
    }

    // The error codes the driver itself retries a read on (6, 7, 89, 91, 134, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436).
    // The driver's own check is not public.
    private static final Set<Integer> TRANSIENT_ERROR_CODES = Set.of(6, 7, 89, 91, 134, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436);

    /**
     * Whether {@code throwable} is a failure that can go away on its own, such as a lost connection, a server selection
     * timeout, a primary that stepped down, or a command that failed with an error code that the driver retries a read
     * on. Anything else, such as a refused command or a malformed resume token, stays the same on every attempt.
     */
    public static boolean isTransientFailure(Throwable throwable) {
        // Spring translates the driver's exception into one of its own and keeps the driver's as the cause
        for (Throwable cause = throwable; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause instanceof MongoSocketException || cause instanceof MongoTimeoutException || cause instanceof MongoNotPrimaryException
                    || cause instanceof MongoNodeIsRecoveringException || cause instanceof MongoConnectionPoolClearedException
                    || (cause instanceof MongoServerException serverException && TRANSIENT_ERROR_CODES.contains(serverException.getCode()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * A blocking subscription model's {@code canResumeFrom(..)}. Runs {@link #changeStreamHistoryProbe(String, Checkpoint)}
     * through {@code runCommand} and answers {@code false} only when MongoDB refuses it because the oplog no longer
     * reaches back to {@code checkpoint}. Any other failure is thrown, and the caller decides whether to retry it.
     */
    public static boolean canResumeFrom(String collectionName, Checkpoint checkpoint, Function<Document, Document> runCommand) {
        Optional<Document> probe = changeStreamHistoryProbe(collectionName, checkpoint);
        if (probe.isEmpty()) {
            return true;
        }
        final Document reply;
        try {
            reply = runCommand.apply(probe.get());
        } catch (RuntimeException e) {
            if (isChangeStreamHistoryLost(e)) {
                return false;
            }
            throw e;
        }
        killChangeStreamHistoryProbeCursor(collectionName, reply).ifPresent(killCursors -> {
            try {
                runCommand.apply(killCursors);
            } catch (RuntimeException ignored) {
                // The server closes an idle cursor on its own after a while
            }
        });
        return true;
    }

    /**
     * The single aggregation pipeline stage a conditional checkpoint write is done as. See ADR 116, "Storage
     * mechanics". Matched with a filter on {@code _id} alone against the unique index, with {@code upsert(true)}
     * and {@code returnDocument(AFTER)}, this is one round trip. The stage replaces the whole document with
     * {@code newCheckpointDocument} merged with the version the condition says to write when {@code condition}
     * allows it, and otherwise yields {@code $$ROOT}, the document unchanged. A {@code notOlderThan} write of a
     * resume token or an operation time is also refused while {@link #POSITION_MOVED_EARLIER} is stored.
     * <p>
     * {@code newCheckpointDocument} is wrapped in {@code $literal} because an update pipeline evaluates what it
     * writes, unlike a replace, so a subscription id or checkpoint string starting with {@code $} would otherwise be
     * read as a field path rather than a value.
     * <p>
     * The merge order, new document first and version second, is what keeps a stale {@code resumeToken} or the
     * legacy {@code subscriptionPosition} field from surviving next to a freshly written field, since only the
     * version key and {@link #LINEAGE_ORIGIN} are added on top of a full replacement, never the previous document's
     * other fields. A write over nothing stored takes {@link #LINEAGE_ORIGIN} from the operation time it writes, and
     * any other write keeps the stored one.
     *
     * @param newCheckpointDocument The document {@link #generateCheckpointDocument(String, Checkpoint)} built for the checkpoint being written
     * @param condition             The condition the write is subject to
     * @return The {@code $replaceWith} pipeline stage to run through {@code findOneAndUpdate}
     */
    public static Document buildConditionalCheckpointWrite(Document newCheckpointDocument, CheckpointWriteCondition condition) {
        WriteDecision decision = switch (condition) {
            // Always allowed, the stored version (if any) carries forward untouched.
            case CheckpointWriteCondition.Any any -> new WriteDecision(Boolean.TRUE, versionCarriedForwardExpr());
            case CheckpointWriteCondition.NotOlderThan notOlderThan -> new WriteDecision(isChangeStreamPosition(newCheckpointDocument)
                    ? new Document("$and", asList(notOlderThanIsAllowedExpr(notOlderThan.writeVersion()), fieldIsMissingExpr(POSITION_MOVED_EARLIER)))
                    : notOlderThanIsAllowedExpr(notOlderThan.writeVersion()), notOlderThan.writeVersion());
            case CheckpointWriteCondition.IfAbsent ifAbsent -> new WriteDecision(ifAbsentIsAllowedExpr(), versionCarriedForwardExpr());
        };

        Document newDocumentWithVersion = new Document("$mergeObjects", asList(
                new Document("$literal", newCheckpointDocument),
                new Document(WRITE_VERSION, decision.versionToWrite()),
                new Document(LINEAGE_ORIGIN, new Document("$cond", asList(ifAbsentIsAllowedExpr(),
                        originOfALineageStartingAt(newCheckpointDocument), lineageOriginCarriedForwardExpr())))));

        Document cond = new Document("$cond", new Document(Map.of(
                "if", decision.allow(),
                "then", newDocumentWithVersion,
                "else", "$$ROOT")));

        return new Document("$replaceWith", cond);
    }

    // A line of delivery that starts at an operation time begins there. One that starts at any other checkpoint has
    // no origin to compare by
    private static Object originOfALineageStartingAt(Document checkpointDocument) {
        Object operationTime = checkpointDocument.get(OPERATION_TIME);
        return operationTime == null ? "$$REMOVE" : operationTime;
    }

    // Present stays present and missing stays missing, as for the version
    private static Document lineageOriginCarriedForwardExpr() {
        return new Document("$ifNull", asList("$" + LINEAGE_ORIGIN, "$$REMOVE"));
    }

    // True when the stored checkpoint's line of delivery began after the candidate's operation time. A stored
    // operation time is compared by itself, and any other checkpoint by its origin. Without an origin, as stored by
    // an older version, nothing is later
    private static Document storedLineageIsLaterThanExpr(Object candidateOperationTime) {
        return new Document("$or", asList(
                new Document("$and", asList(
                        fieldExistsExpr(OPERATION_TIME),
                        new Document("$gt", asList("$" + OPERATION_TIME, candidateOperationTime)))),
                new Document("$and", asList(
                        fieldIsMissingExpr(OPERATION_TIME),
                        fieldExistsExpr(LINEAGE_ORIGIN),
                        new Document("$gt", asList("$" + LINEAGE_ORIGIN, candidateOperationTime))))));
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
     * The {@code $replaceWith} pipeline stage for a checkpoint storage's {@code resolveFirstCheckpointRace}, one
     * round trip like {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)}.
     * {@code candidateDocument} must carry {@link #OPERATION_TIME}, which is the caller's job to have checked, since
     * only that shape is comparable at all.
     * <p>
     * Writes {@code candidateDocument} when nothing is stored yet, the same presence check
     * {@link #ifAbsentIsAllowedExpr()} makes, when what is stored has an {@link #OPERATION_TIME} later than the one
     * {@code candidateDocument} has, or when it has {@link #RESUME_TOKEN}, {@link #GENERIC_CHECKPOINT} or
     * {@link #LEGACY_GENERIC_CHECKPOINT} with a {@link #LINEAGE_ORIGIN} later than that operation time. Changes
     * nothing otherwise, whether because the stored position or its origin is earlier than or equal to the
     * candidate's, or because the stored checkpoint has no origin, which
     * {@link #interpretFirstCheckpointRaceResolution(Document)} is what tells apart. A write stores the candidate's
     * operation time as {@link #LINEAGE_ORIGIN}, and a write over a stored checkpoint also stores
     * {@link #POSITION_MOVED_EARLIER}.
     *
     * @param candidateDocument The document {@link #generateCheckpointDocument(String, Checkpoint)} built for the
     *                          candidate position, which must carry {@link #OPERATION_TIME}
     * @return The {@code $replaceWith} pipeline stage to run through {@code findOneAndUpdate}
     */
    public static Document buildFirstCheckpointRaceResolution(Document candidateDocument) {
        Object candidateOperationTime = candidateDocument.get(OPERATION_TIME);
        Document allow = new Document("$or", asList(
                ifAbsentIsAllowedExpr(),
                storedLineageIsLaterThanExpr(candidateOperationTime)));

        // A write over nothing stored marks nothing. A write over a stored checkpoint marks it, whatever kind of
        // checkpoint it is, since the node that stored it may deliver from it
        Document newDocumentWithVersion = new Document("$mergeObjects", asList(
                new Document("$literal", candidateDocument),
                new Document(WRITE_VERSION, versionCarriedForwardExpr()),
                new Document(LINEAGE_ORIGIN, candidateOperationTime),
                new Document(POSITION_MOVED_EARLIER, new Document("$cond", asList(ifAbsentIsAllowedExpr(), "$$REMOVE", true)))));

        Document cond = new Document("$cond", new Document(Map.of(
                "if", allow,
                "then", newDocumentWithVersion,
                "else", "$$ROOT")));

        return new Document("$replaceWith", cond);
    }

    // The document stored without POSITION_MOVED_EARLIER, the one field a three-argument resolve that keeps the stored
    // checkpoint drops, and so does storing a write version with it
    private static Document storedWithoutMovedEarlierMarkExpr() {
        return new Document("$arrayToObject", List.of(new Document("$filter", new Document(Map.of(
                "input", new Document("$objectToArray", "$$ROOT"),
                "cond", new Document("$ne", asList("$$this.k", POSITION_MOVED_EARLIER)))))));
    }

    private static boolean isChangeStreamPosition(Document checkpointDocument) {
        return checkpointDocument.containsKey(RESUME_TOKEN) || checkpointDocument.containsKey(OPERATION_TIME);
    }

    /**
     * What {@code findOneAndUpdate} returned for {@link #buildFirstCheckpointRaceResolution(Document)} means, for a
     * checkpoint storage's {@code resolveFirstCheckpointRace} to answer with.
     * <p>
     * {@code afterDocument} has {@link #OPERATION_TIME} whenever the pipeline wrote the candidate or left a stored
     * operation time that proved earlier or equal in place, and {@code afterDocument} is the checkpoint that governs
     * either way. It has {@link #RESUME_TOKEN}, {@link #GENERIC_CHECKPOINT} or {@link #LEGACY_GENERIC_CHECKPOINT}
     * instead only when the pipeline didn't change a stored position of one of those kinds, because its origin is
     * earlier than or equal to the candidate's or it has none. This answers empty for it, and the caller then takes
     * its own fallback, as for a storage that can't compare.
     *
     * @param afterDocument The document {@code findOneAndUpdate} returned
     * @return The checkpoint that now governs the subscription's first position, or empty when {@code afterDocument}
     * shows a stored position this could not compare {@code candidateDocument} against
     */
    public static Optional<Checkpoint> interpretFirstCheckpointRaceResolution(Document afterDocument) {
        if (!afterDocument.containsKey(OPERATION_TIME)) {
            return Optional.empty();
        }
        return Optional.of(calculateCheckpointFromMongoStreamPositionDocument(afterDocument));
    }

    /**
     * The {@code $replaceWith} pipeline stage for a checkpoint storage's
     * {@code resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion)}, one round trip like
     * {@link #buildFirstCheckpointRaceResolution(Document)}.
     * <p>
     * Writes only when {@code notOlderThan(writeVersion)} allows it, and then always with {@code writeVersion}. It
     * writes {@code candidateDocument} when nothing is stored. For a candidate with an {@link #OPERATION_TIME}, it also
     * writes it when the stored operation time is later, or when another stored checkpoint has a
     * {@link #LINEAGE_ORIGIN} later than the candidate's, and stores the candidate's operation time as the origin. It
     * keeps a stored operation time otherwise. For any other
     * candidate it keeps a stored checkpoint equal to {@code candidateDocument}, and a generic checkpoint an older
     * Occurrent version stored in {@link #LEGACY_GENERIC_CHECKPOINT} counts as equal when it holds the value
     * {@code candidateDocument} holds in {@link #GENERIC_CHECKPOINT}. Every other stored checkpoint can't be
     * compared with {@code candidateDocument}, so the document is left as it is, and so is one with a newer stored
     * version. A stored checkpoint that is kept loses {@link #POSITION_MOVED_EARLIER}.
     * <p>
     * The three-argument {@code resolveFirstCheckpointRace} runs it with upsert, so it writes {@code candidateDocument}
     * when no checkpoint document exists. {@code resolveFirstCheckpointRaceWithoutCreatingACheckpoint} runs it without
     * upsert, so it writes nothing then and {@code findOneAndUpdate} returns {@code null}.
     *
     * @param candidateDocument The document {@link #generateCheckpointDocument(String, Checkpoint)} built for the
     *                          candidate position
     * @param writeVersion      The version to store with the checkpoint that governs
     * @return The {@code $replaceWith} pipeline stage to run through {@code findOneAndUpdate}
     */
    public static Document buildFirstCheckpointRaceResolution(Document candidateDocument, long writeVersion) {
        boolean ordered = candidateDocument.containsKey(OPERATION_TIME);
        Document takeCandidate = ordered
                ? new Document("$or", asList(
                        ifAbsentIsAllowedExpr(),
                        storedLineageIsLaterThanExpr(candidateDocument.get(OPERATION_TIME))))
                : ifAbsentIsAllowedExpr();
        Document keepStored = ordered ? fieldExistsExpr(OPERATION_TIME) : storedCheckpointIsExpr(candidateDocument);

        Document candidateWithVersion = new Document("$mergeObjects", asList(
                new Document("$literal", candidateDocument),
                new Document(WRITE_VERSION, writeVersion),
                new Document(LINEAGE_ORIGIN, originOfALineageStartingAt(candidateDocument))));
        Document storedWithVersion = new Document("$cond", new Document(Map.of(
                "if", keepStored,
                "then", new Document("$mergeObjects", asList(storedWithoutMovedEarlierMarkExpr(), new Document(WRITE_VERSION, writeVersion))),
                "else", "$$ROOT")));

        Document written = new Document("$cond", new Document(Map.of(
                "if", takeCandidate,
                "then", candidateWithVersion,
                "else", storedWithVersion)));

        Document cond = new Document("$cond", new Document(Map.of(
                "if", notOlderThanIsAllowedExpr(writeVersion),
                "then", written,
                "else", "$$ROOT")));

        return new Document("$replaceWith", cond);
    }

    // True when every field a checkpoint is stored in holds what candidateDocument holds, and each one it lacks is
    // missing from the stored document too. A generic checkpoint an older version stored in the legacy field is
    // compared as if it were stored in the current one, the way calculateCheckpointFromMongoStreamPositionDocument
    // reads it back, since candidateDocument never holds the legacy field
    private static Document storedCheckpointIsExpr(Document candidateDocument) {
        Stream<Document> generic = Stream.of(candidateDocument.containsKey(GENERIC_CHECKPOINT)
                ? new Document("$eq", asList(storedGenericCheckpointExpr(), new Document("$literal", candidateDocument.get(GENERIC_CHECKPOINT))))
                : new Document("$and", asList(fieldIsMissingExpr(GENERIC_CHECKPOINT), fieldIsMissingExpr(LEGACY_GENERIC_CHECKPOINT))));
        Stream<Document> others = Stream.of(RESUME_TOKEN, OPERATION_TIME, CATCHUP_LIVE_FROM, CATCHUP_REPLAY_ORIGIN, CATCHUP_REPLAY_TO)
                .map(field -> candidateDocument.containsKey(field)
                        ? new Document("$eq", asList("$" + field, new Document("$literal", candidateDocument.get(field))))
                        : fieldIsMissingExpr(field));
        return new Document("$and", Stream.concat(generic, others).toList());
    }

    // The generic checkpoint stored, read from the legacy field only when the current one is missing, as
    // calculateCheckpointFromMongoStreamPositionDocument reads it
    private static Document storedGenericCheckpointExpr() {
        return new Document("$cond", new Document(Map.of(
                "if", fieldExistsExpr(GENERIC_CHECKPOINT),
                "then", "$" + GENERIC_CHECKPOINT,
                "else", "$" + LEGACY_GENERIC_CHECKPOINT)));
    }

    /**
     * What {@code findOneAndUpdate} returned for {@link #buildFirstCheckpointRaceResolution(Document, long)} means.
     * <p>
     * A stored version newer than {@code writeVersion} refused the write, whatever kind of checkpoint is stored. A
     * write stores exactly {@code writeVersion}. For a {@code candidate} with an operation time, a document with
     * {@code writeVersion} and an {@link #OPERATION_TIME} is the checkpoint that governs. For any other
     * {@code candidate}, a document with {@code writeVersion} that holds {@code candidate} is. Any other document is a
     * stored checkpoint the pipeline left as it was.
     *
     * @param subscriptionId The id of the subscription the write was for
     * @param candidate      The position the write offered
     * @param afterDocument  The document {@code findOneAndUpdate} returned, {@code null} when it ran without upsert and
     *                       no checkpoint is stored
     * @param writeVersion   The version the write offered
     * @return The checkpoint that now governs the subscription's first position, or empty when the pipeline wrote
     * nothing because the stored checkpoint can't be compared with the candidate, or because none is stored
     * @throws CheckpointWriteConditionNotFulfilledException if a newer stored version refused the write
     */
    public static Optional<Checkpoint> interpretFirstCheckpointRaceResolution(String subscriptionId, Checkpoint candidate, @Nullable Document afterDocument,
                                                                             long writeVersion) {
        if (afterDocument == null) {
            return Optional.empty();
        }
        OptionalLong storedVersion = extractWriteVersion(afterDocument);
        if (storedVersion.isPresent() && storedVersion.getAsLong() > writeVersion) {
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, storedVersion, CheckpointWriteCondition.notOlderThan(writeVersion));
        }
        if (storedVersion.isEmpty() || storedVersion.getAsLong() != writeVersion) {
            return Optional.empty();
        }
        if (candidate instanceof MongoOperationTimeCheckpoint) {
            return afterDocument.containsKey(OPERATION_TIME)
                    ? Optional.of(calculateCheckpointFromMongoStreamPositionDocument(afterDocument))
                    : Optional.empty();
        }
        Checkpoint stored = calculateCheckpointFromMongoStreamPositionDocument(afterDocument);
        return stored.asString().equals(candidate.asString()) ? Optional.of(stored) : Optional.empty();
    }

    /**
     * Builds the {@code $replaceWith} stage that stores {@code writeVersion} with the checkpoint stored and leaves the
     * checkpoint as it is. It also removes {@link #POSITION_MOVED_EARLIER}, as a three-argument resolve that keeps the
     * stored checkpoint does, so a {@code notOlderThan(writeVersion)} save is allowed again. A stored version newer
     * than {@code writeVersion} leaves the whole document as it is. Run it through {@code findOneAndUpdate} without
     * upsert, so nothing is written when no checkpoint is stored.
     *
     * @param writeVersion The version to store with the checkpoint stored
     * @return The {@code $replaceWith} pipeline stage to run through {@code findOneAndUpdate}
     */
    public static Document buildWriteVersionStamp(long writeVersion) {
        Document cond = new Document("$cond", new Document(Map.of(
                "if", notOlderThanIsAllowedExpr(writeVersion),
                "then", new Document("$mergeObjects", asList(storedWithoutMovedEarlierMarkExpr(), new Document(WRITE_VERSION, writeVersion))),
                "else", "$$ROOT")));
        return new Document("$replaceWith", cond);
    }

    /**
     * What {@code findOneAndUpdate} returned for {@link #buildWriteVersionStamp(long)} means.
     *
     * @param subscriptionId The id of the subscription the write was for
     * @param afterDocument  The document {@code findOneAndUpdate} returned, {@code null} when no checkpoint is stored
     * @param writeVersion   The version the write offered
     * @return The checkpoint stored, now with {@code writeVersion}, or empty when no checkpoint is stored
     * @throws CheckpointWriteConditionNotFulfilledException if a newer stored version refused the write
     */
    public static Optional<Checkpoint> interpretWriteVersionStamp(String subscriptionId, @Nullable Document afterDocument, long writeVersion) {
        if (afterDocument == null) {
            return Optional.empty();
        }
        OptionalLong storedVersion = extractWriteVersion(afterDocument);
        if (storedVersion.isPresent() && storedVersion.getAsLong() > writeVersion) {
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, storedVersion, CheckpointWriteCondition.notOlderThan(writeVersion));
        }
        return Optional.of(calculateCheckpointFromMongoStreamPositionDocument(afterDocument));
    }

    /**
     * Reads the version {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)} recorded, or
     * empty if the document has none.
     */
    public static OptionalLong extractWriteVersion(@Nullable Document document) {
        if (document == null || !document.containsKey(WRITE_VERSION)) {
            return OptionalLong.empty();
        }
        Number version = document.get(WRITE_VERSION, Number.class);
        if (version == null) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(version.longValue());
    }

    /**
     * Throws {@link CheckpointWriteConditionNotFulfilledException} unless {@code afterDocument}, the document
     * {@code findOneAndUpdate} returned with {@code returnDocument(AFTER)}, shows that {@code condition} allowed the
     * write {@link #buildConditionalCheckpointWrite(Document, CheckpointWriteCondition)} attempted.
     * <p>
     * {@code any()} never refuses, so it is not checked. {@code notOlderThan(v)} is told apart by comparing the
     * version on {@code afterDocument} to {@code v}. The pipeline writes exactly {@code v} on success, and a refused
     * write leaves the higher stored version untouched. A write refused for {@link #POSITION_MOVED_EARLIER} can leave
     * {@code v} itself stored, so the mark is checked too, which a successful write never leaves in place. {@code ifAbsent()} is told apart
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
                // A write replaces the whole document, so the mark is still there only when the write was refused
                OptionalLong storedVersion = extractWriteVersion(afterDocument);
                yield storedVersion.isPresent() && storedVersion.getAsLong() == notOlderThan.writeVersion()
                        && !afterDocument.containsKey(POSITION_MOVED_EARLIER);
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
     * Throws when {@code startAt} is a position a catch-up subscription model stores, a {@link GlobalCheckpoint} or a
     * {@link CatchupTimeCheckpoint}. A change stream can't open at either, and
     * {@link #applyStartPosition(Object, BiFunction, BiFunction, StartAt, SubscriptionModelContext)} opens it at the
     * present for them, which skips every event written before it.
     * <p>
     * A subscription model calls it when a subscription starts, not when one resumes. A durable subscription model
     * wrapping a blocking MongoDB subscription model resumes a subscription at the position a catch-up stored on
     * purpose, and the change stream then goes on from the position the subscription has read to. A dynamic
     * {@link StartAt} is not checked here, for the reason {@link #checkStartPosition(StartAt, SubscriptionModelContext)}
     * gives. {@link #refusingCatchupPositionUntilOpened(StartAt, String, BooleanSupplier)} checks the positions it
     * answers instead.
     *
     * @throws UnsupportedStartAtException If {@code startAt} is a position a catch-up subscription model stores.
     */
    public static void refuseCatchupPosition(@Nullable StartAt startAt) {
        if (catchupPositionIn(startAt) instanceof Checkpoint checkpoint) {
            throw new UnsupportedStartAtException(startAt, "Cannot open a change stream at checkpoint \"" + checkpoint.asString()
                    + "\", " + NOT_A_CHANGE_STREAM_POSITION + "Start the subscription through the catch-up subscription model that stored the checkpoint.");
        }
    }

    /**
     * {@code startAt} itself when it isn't dynamic. A dynamic one is wrapped, so that a position its function answers
     * before a change stream of the subscription has opened is refused when it is a position a catch-up subscription
     * model stores, as {@link #refuseCatchupPosition(StartAt)} refuses a fixed one. What counts as opened is up to the
     * subscription model. A blocking one counts a subscription that has started, so an answer whose opening a pause or
     * a failure stopped doesn't end the check. Through a mongos a subscription can start only once its change stream's
     * first reply came. The reactor one counts a driver cursor that opened for a subscription
     * that reads through one. Where it has no such cursor to wait for it counts the first document the change stream
     * reads, since a subscribe doesn't say the server accepted the position, so an answer the server rejects doesn't end
     * it. The {@link UnsupportedStartAtException} is thrown where the subscription model resolves the position, as the
     * change stream opens.
     * <p>
     * An answer after that isn't checked. Once a change stream has opened, the subscription model records the position it opened
     * at with {@link #recordWhereItOpened(OpenedAt, BiPredicate)} and doesn't ask the function again. A blocking one
     * refuses to start a subscription when it can't record a position for the present, and holds one paused when that
     * happens as it restarts after its history was lost. The reactor one stops a subscription it can't find an
     * operation time to open at.
     *
     * @param subscriptionId The id the exception message names, or {@code null} for a subscription that has none
     * @param opened         Whether a change stream of the subscription has opened, as the subscription model counts it
     */
    public static StartAt refusingCatchupPositionUntilOpened(StartAt startAt, @Nullable String subscriptionId, BooleanSupplier opened) {
        if (!startAt.isDynamic()) {
            return startAt;
        }
        return StartAt.dynamic(ctx -> {
            StartAt resolved = startAt.get(ctx);
            if (!opened.getAsBoolean() && catchupPositionIn(resolved) instanceof Checkpoint checkpoint) {
                String subscription = subscriptionId == null ? "a subscription" : "subscription \"" + subscriptionId + "\"";
                throw new UnsupportedStartAtException(requireNonNull(resolved), "The dynamic start position of " + subscription + " answered checkpoint \""
                        + checkpoint.asString() + "\", " + NOT_A_CHANGE_STREAM_POSITION + "Start the subscription through a catch-up subscription model. "
                        + "When one already wraps this model, restore the start position its replay had or remove the stored checkpoint."
                        // Named only, since instances can only compete for a subscription with an id
                        + (subscriptionId == null ? "" : " " + SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT));
            }
            return resolved;
        });
    }

    private static final String NOT_A_CHANGE_STREAM_POSITION = "which is a position a catch-up subscription model stores and not a change stream position. "
            + "Opening it at the present instead would skip every event written before it. ";

    /**
     * Whether {@code startAt} is a position a catch-up subscription model stores, which a change stream can't open at.
     */
    public static boolean isCatchupPosition(@Nullable StartAt startAt) {
        return catchupPositionIn(startAt) != null;
    }

    private static @Nullable Checkpoint catchupPositionIn(@Nullable StartAt startAt) {
        return startAt instanceof StartAtCheckpoint position
                && (GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint) || CatchupTimeCheckpoint.isCatchupTimeCheckpoint(position.checkpoint))
                ? position.checkpoint : null;
    }

    /**
     * Stands in for the object a start position would be applied to, so {@link #checkStartPosition} can reuse the
     * whole of {@code applyStartPosition} rather than growing a second copy of its parsing that could drift from it.
     */
    private static final Object NOTHING = new Object();

    public static <T> T applyStartPosition(T t, BiFunction<T, BsonDocument, T> applyResumeToken, BiFunction<T, BsonTimestamp, T> applyOperationTime, @Nullable StartAt startAt, SubscriptionModelContext ctx) {
        return applyResolvedStartPosition(t, applyResumeToken, applyOperationTime, startAt == null ? null : startAt.get(ctx));
    }

    private static <T> T applyResolvedStartPosition(T t, BiFunction<T, BsonDocument, T> applyResumeToken, BiFunction<T, BsonTimestamp, T> applyOperationTime, @Nullable StartAt startAtValue) {
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
        } else if (GlobalCheckpoint.isGlobalCheckpoint(changeStreamPosition) || CatchupTimeCheckpoint.isCatchupTimeCheckpoint(changeStreamPosition)) {
            // A catch-up's position or time, whose live start would otherwise match below and fail to parse as a whole
            return t;
        } else {
            String changeStreamPositionString = changeStreamPosition.asString();
            if (changeStreamPositionString.contains(RESUME_TOKEN)) {
                BsonDocument bsonDocument = BsonDocument.parse(changeStreamPositionString);
                BsonDocument resumeToken = bsonDocument.getDocument(RESUME_TOKEN);
                withStartPositionApplied = applyResumeToken.apply(t, resumeToken);
            } else if (changeStreamPositionString.contains(OPERATION_TIME)) {
                BsonTimestamp operationTime = OperationTimeLiveStart.parse(changeStreamPositionString);
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

    /**
     * Whether a change stream opened at {@code resolved}, a start position that is already resolved, starts at the
     * present. It does for {@code StartAt.now()}, for the model default, and for a checkpoint that is not a change
     * stream position, such as a catch-up's position ({@code GlobalCheckpoint}) or a time, since
     * {@link #applyStartPosition(Object, BiFunction, BiFunction, StartAt, SubscriptionModelContext)} opens at the
     * present for that one too.
     */
    public static boolean opensAtThePresent(@Nullable StartAt resolved) {
        return applyResolvedStartPosition(Boolean.TRUE, (present, resumeToken) -> Boolean.FALSE, (present, operationTime) -> Boolean.FALSE, resolved);
    }

    /**
     * The command a subscription model sends to learn the server's current operation time, which it reads from the
     * {@link #OPERATION_TIME} field of the reply. A replica set or a sharded cluster, which change streams need,
     * includes that field in its command replies. {@code ping} needs no privilege, unlike the {@code hostInfo}
     * command a global checkpoint is read with.
     */
    public static final Document CURRENT_OPERATION_TIME_COMMAND = new Document("ping", 1);

    /**
     * The operation time just after the one in {@code reply}, a reply to {@link #CURRENT_OPERATION_TIME_COMMAND}, or
     * {@code null} when the reply has none. The reply's operation time belongs to a write made before the command
     * ran, and {@code startAtOperationTime} includes a write made at exactly the time it is given, so opening at the
     * reply's own time would deliver that earlier write too.
     */
    public static @Nullable BsonTimestamp operationTimeAfter(Document reply) {
        return reply.get(OPERATION_TIME) instanceof BsonTimestamp ? getServerOperationTime(reply, 1) : null;
    }

    /**
     * The command a subscription model sends to read the server's wall clock, from the {@code localTime} field of the
     * reply. It needs no privilege. MongoDB 4.2.0 to 4.2.9 don't know it, so send
     * {@link #LEGACY_SERVER_CLOCK_COMMAND} instead when it fails with {@link #COMMAND_NOT_FOUND_ERROR_CODE}.
     */
    public static final Document SERVER_CLOCK_COMMAND = new Document("hello", 1);

    /**
     * The name {@link #SERVER_CLOCK_COMMAND} has on MongoDB 4.2.0 to 4.2.9. Its reply has the same {@code localTime}.
     */
    public static final Document LEGACY_SERVER_CLOCK_COMMAND = new Document("isMaster", 1);

    public static final int COMMAND_NOT_FOUND_ERROR_CODE = 59;

    /**
     * Whether {@code runCommand} reaches MongoDB through a mongos, from the answer to {@link #SERVER_CLOCK_COMMAND}, or to
     * {@link #LEGACY_SERVER_CLOCK_COMMAND} on a server that doesn't know it. A mongos answers {@code msg} isdbgrid, also
     * through a load balancer. Every other failure reaches the caller.
     *
     * @param runCommand Runs a command against the database of the event collection
     */
    public static boolean isMongos(Function<Document, Document> runCommand) {
        Document answer;
        try {
            answer = runCommand.apply(SERVER_CLOCK_COMMAND);
        } catch (RuntimeException e) {
            if (!hasCommandErrorCode(e, COMMAND_NOT_FOUND_ERROR_CODE)) {
                throw e;
            }
            answer = runCommand.apply(LEGACY_SERVER_CLOCK_COMMAND);
        }
        return answer != null && "isdbgrid".equals(answer.get("msg"));
    }

    /**
     * The earliest operation time in the second the server's wall clock showed {@code elapsedNanos} before
     * {@code reply}, a reply to {@link #SERVER_CLOCK_COMMAND}, arrived. Pass the time since the moment of interest, as
     * measured with {@link System#nanoTime()} once the reply has arrived, which counts the round trip as well. A change
     * stream opened at the answer receives every write made after that moment, since MongoDB never gives a write an
     * operation time earlier than the second its wall clock shows when it writes. The time this measures
     * back from is the server's own clock, so a client clock that is ahead of or behind the server doesn't move the
     * answer. It can still be later than the moment if the server's clock is stepped forward between the moment and the
     * reply.
     *
     * @return The operation time, or {@code null} when the reply has no {@code localTime}
     */
    public static @Nullable BsonTimestamp operationTimeAsOf(Document reply, long elapsedNanos) {
        if (!(reply.get("localTime") instanceof Date localTime)) {
            return null;
        }
        // One millisecond less, since toMillis rounds the elapsed time down
        long millisAtTheMoment = localTime.getTime() - TimeUnit.NANOSECONDS.toMillis(elapsedNanos) - 1;
        return new BsonTimestamp((int) Math.floorDiv(millisAtTheMoment, 1000L), 0);
    }

    /**
     * Where a change stream that starts at a moment of interest opens. {@code helloStart} is what
     * {@link #operationTimeAsOf(Document, long)} answered for that moment, and {@code knownClusterTime} is the newest
     * cluster time the client had seen at that moment, or {@code null} when it had seen none.
     * <p>
     * MongoDB gives every write a cluster time later than any cluster time the client sent with it, and the driver
     * sends the newest one it has seen with every command. So a change stream opened just after {@code knownClusterTime}
     * receives every write the same client makes after the moment, whatever the server's clock shows. The answer is
     * that position or {@code helloStart}, whichever is earlier, so it's never later than {@code helloStart}.
     * <p>
     * A client that has sent no command for a while knows a cluster time that old, and opening there would deliver
     * everything written since. So when {@code knownClusterTime} is more than {@code maxAge} older than
     * {@code helloStart}, or {@code null}, the answer is {@code helloStart}.
     */
    public static BsonTimestamp startOf(BsonTimestamp helloStart, @Nullable BsonTimestamp knownClusterTime, Duration maxAge) {
        requireNonNull(helloStart, "helloStart cannot be null");
        requireNonNull(maxAge, "maxAge cannot be null");
        if (knownClusterTime == null || (long) helloStart.getTime() - knownClusterTime.getTime() > maxAge.toSeconds()) {
            return helloStart;
        }
        // MongoDB moves to the next second rather than let the increment pass Integer.MAX_VALUE
        BsonTimestamp afterKnown = knownClusterTime.getInc() == Integer.MAX_VALUE
                ? new BsonTimestamp(knownClusterTime.getTime() + 1, 0)
                : new BsonTimestamp(knownClusterTime.getTime(), knownClusterTime.getInc() + 1);
        return afterKnown.compareTo(helloStart) < 0 ? afterKnown : helloStart;
    }

    /**
     * The position a subscription records in place of {@code tracked} once a change stream opened from it at the
     * present, at {@code present}, a change stream position. Opening the stream again then starts at {@code present}
     * and not at a later present. A dynamic {@code tracked} stays dynamic until
     * {@link #recordWhereItOpened(OpenedAt, BiPredicate)} replaces it, so an opening before that still evaluates it,
     * and only the answers that would have opened at the present are replaced by {@code present}.
     */
    public static StartAt pinnedTo(StartAt tracked, Checkpoint present) {
        StartAt pinned = StartAt.checkpoint(requireNonNull(present, "present cannot be null"));
        if (!tracked.isDynamic()) {
            return pinned;
        }
        return StartAt.dynamic(ctx -> {
            StartAt resolved = tracked.get(ctx);
            return opensAtThePresent(resolved) ? pinned : resolved;
        });
    }

    /**
     * Resolves the position a change stream is about to open at from {@code currentStartAt}, the position a
     * subscription records. When that resolves to the present, this asks {@code currentPosition} for a change stream
     * position at the present, records it in {@code currentStartAt} with {@link #pinnedTo(StartAt, Checkpoint)}, and
     * opens the stream there. Because it is recorded before the stream opens, a pause and resume, or a restart, before
     * the first event is handled opens there too, and nothing written in between is skipped.
     * <p>
     * The position is recorded only if {@code currentStartAt} still holds the position read at the start, since the
     * checkpoint of a handled event is written from another thread and must not be overwritten. When it has changed,
     * the position is resolved again. When {@code currentPosition} throws, nothing is recorded and the exception
     * reaches the caller, so the stream never opens at a present it hasn't recorded.
     *
     * @param currentPosition Answers a change stream position at the present, a resume token or an operation time, or
     *                        throws
     * @return The position to open the change stream at, never {@code null}
     */
    public static StartAt resolveOpeningPosition(AtomicReference<StartAt> currentStartAt, SubscriptionModelContext ctx, Supplier<Checkpoint> currentPosition) {
        while (true) {
            StartAt tracked = currentStartAt.get();
            StartAt resolved = tracked.get(ctx);
            if (!opensAtThePresent(resolved)) {
                return requireNonNull(resolved);
            }
            Checkpoint present = requireNonNull(currentPosition.get(), "currentPosition answered null");
            if (currentStartAt.compareAndSet(tracked, pinnedTo(tracked, present))) {
                return StartAt.checkpoint(present);
            }
        }
    }

    /**
     * Where a change stream opened, as {@code openingPosition}, and the position the subscription tracked as it
     * opened, as {@code tracked}, before {@link MongoCommons#recordWhereItOpened(OpenedAt, BiPredicate)} replaced a
     * dynamic one. A model keeps one for each opening, so a loss of history can tell whether the
     * position it lost is the one the stream opened at.
     */
    public record OpenedAt(StartAt openingPosition, StartAt tracked) {
    }

    /**
     * Records the position a change stream opened at in place of the dynamic position the subscription tracked, once
     * the stream has opened, or as it opens when the subscription model can't tell when the server accepted the
     * position. Opening again then starts at that position and doesn't call the dynamic position's function again,
     * which could answer another position. Nothing is recorded when the tracked position isn't dynamic, when the
     * stream opened at the present, or when the tracked position has changed since {@code openedAt} was read, so a
     * checkpoint written from another thread is kept.
     *
     * @param compareAndSet Sets the tracked position to its second argument when it still holds its first
     * @return What {@code compareAndSet} answered, or {@code false} when it wasn't asked
     */
    public static boolean recordWhereItOpened(OpenedAt openedAt, BiPredicate<StartAt, StartAt> compareAndSet) {
        return openedAt.tracked().isDynamic() && !opensAtThePresent(openedAt.openingPosition()) && compareAndSet.test(openedAt.tracked(), openedAt.openingPosition());
    }

    /**
     * The checkpoint a subscription reports when MongoDB no longer has the history it reads from. While
     * {@code current} is still the position recorded right after the opening, that is the position the stream opened
     * at, taken from {@code openedAt}. Once the subscription has moved, it is the fixed position it moved to. Null
     * when neither is a checkpoint. A dynamic {@code current} is never evaluated again here, since its supplier can be
     * the caller's code.
     */
    public static @Nullable Checkpoint lostFrom(StartAt current, @Nullable OpenedAt openedAt) {
        if (openedAt != null && current == openedAt.tracked()) {
            return openedAt.openingPosition() instanceof StartAtCheckpoint position ? position.checkpoint : null;
        }
        return !current.isDynamic() && current instanceof StartAtCheckpoint position ? position.checkpoint : null;
    }

    /**
     * The warning a blocking subscription model logs when {@code reply} has no operation time to record for a change
     * stream that opens at the present.
     */
    public static String noOperationTimeToPinToMessage(Document reply) {
        return "The reply to " + CURRENT_OPERATION_TIME_COMMAND.toJson() + " carried no " + OPERATION_TIME + ", so the subscription reads the change stream once to record where the present is. " +
                "When that read returns neither an event nor a resume token, a subscription made at the present is refused and waiting for it to start throws, and a resume at the present or a restart after its history was lost holds it paused. Reply was: " + reply.toJson();
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
            Document liveFrom = checkpointDocument.get(CATCHUP_LIVE_FROM, Document.class);
            // A position catch-up stores the replay origin as a number and a time catch-up as a string
            Object replayOrigin = checkpointDocument.get(CATCHUP_REPLAY_ORIGIN);
            Object replayTo = checkpointDocument.get(CATCHUP_REPLAY_TO);
            boolean position = GlobalCheckpoint.isGlobalCheckpoint(new StringBasedCheckpoint(value));
            // Without the replay end the live start is ignored, and the catch-up resumes as from a plain position
            if (liveFrom != null && position && replayOrigin instanceof Number origin && replayTo instanceof Number to) {
                changeStreamPosition = GlobalCheckpoint.of(GlobalCheckpoint.positionOf(new StringBasedCheckpoint(value)),
                        calculateCheckpointFromMongoStreamPositionDocument(liveFrom), origin.longValue(), to.longValue());
            } else if (liveFrom != null && !position && replayOrigin instanceof String origin && replayTo == null) {
                // A value or replay origin that is not a time fails like any other document this method doesn't recognize
                try {
                    changeStreamPosition = CatchupTimeCheckpoint.of(value, calculateCheckpointFromMongoStreamPositionDocument(liveFrom), origin);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Doesn't recognize " + checkpointDocument + " as a valid checkpoint document", e);
                }
            } else {
                changeStreamPosition = new StringBasedCheckpoint(value);
            }
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
