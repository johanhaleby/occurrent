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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import jakarta.annotation.PreDestroy;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.internal.CatchupMarkerForgetWrites;
import org.occurrent.subscription.mongodb.internal.CatchupMarkerGenerationReads;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.*;

/**
 * A native sync Java MongoDB implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in MongoDB.
 * <p>
 * As a {@link CatchupMarkerStorage} it keeps the generation of a subscription id in a second document in the same
 * collection, with the {@code _id} {@code {catchupMarker: "<subscription id>"}}. Don't delete these documents. Deleting
 * one puts the generation of that id back at {@code 0}, and a marker saved earlier can then read as present again.
 * <p>
 * {@link #forgetCatchupMarker(String)} waits for a majority of the replica set to acknowledge each of its two writes,
 * whatever write concern the collection has. {@link #readCatchupMarker(String)} reads the generation with
 * {@link ReadConcern#LINEARIZABLE}, which a majority has to confirm. An attempt that gets no acknowledgement or
 * confirmation within 10 seconds fails, and the {@link RetryStrategy} decides whether to try again. A server that
 * isn't a replica set member refuses {@link ReadConcern#LINEARIZABLE}, and this storage then reads the generation
 * with {@link ReadConcern#MAJORITY} for the rest of its life.
 * <p>
 * Both writes of a forget ask for {@code j: true}, so a majority has them in its on-disk journal before acknowledging
 * them, whatever {@code writeConcernMajorityJournalDefault} says, and an acknowledged forget never rolls back. A server
 * that runs without journaling refuses {@code j: true}, and this storage then sends the forget without it for the rest
 * of its life and logs a warning the first time. That server can't journal a forget, so a crash loses an acknowledged
 * one, and the guarantee above holds only on a server with journaling.
 */
@NullMarked
public class NativeMongoCheckpointStorage implements CatchupMarkerStorage {

    private final MongoCollection<Document> checkpointCollection;
    private final MongoCollection<Document> catchupMarkerJournaledForgets;
    private final MongoCollection<Document> catchupMarkerForgetsWithoutJournal;
    private final MongoCollection<Document> catchupMarkerMajorityReads;
    private final MongoCollection<Document> catchupMarkerLinearizableReads;
    private final RetryStrategy retryStrategy;

    private final CatchupMarkerGenerationReads catchupMarkerGenerationReads = new CatchupMarkerGenerationReads();
    private final CatchupMarkerForgetWrites catchupMarkerForgetWrites = new CatchupMarkerForgetWrites();

    private volatile boolean shutdown = false;

    /**
     * Create a {@code CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in MongoDB.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the checkpoint.
     *
     * @param checkpointCollection The collection into which checkpoints will be stored
     */
    public NativeMongoCheckpointStorage(MongoDatabase database, String checkpointCollection) {
        this(database, checkpointCollection, defaultRetryStrategy());
    }

    /**
     * Create a {@code CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in MongoDB.
     *
     * @param checkpointCollection The collection into which checkpoints will be stored
     */
    public NativeMongoCheckpointStorage(MongoDatabase database, String checkpointCollection, RetryStrategy retryStrategy) {
        this(requireNonNull(database, "Database cannot be null").getCollection(checkpointCollection), retryStrategy);
    }

    /**
     * Create a {@code CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in MongoDB.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the checkpoint.
     *
     * @param checkpointCollection The collection into which checkpoints will be stored
     */
    public NativeMongoCheckpointStorage(MongoCollection<Document> checkpointCollection) {
        this(checkpointCollection, defaultRetryStrategy());
    }

    /**
     * Create a {@code CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in MongoDB.
     *
     * @param checkpointCollection The collection into which checkpoints will be stored
     * @param retryStrategy                  A custom retry strategy to use if there's a problem reading/saving/deleting the checkpoint to the MongoDB storage.
     */
    public NativeMongoCheckpointStorage(MongoCollection<Document> checkpointCollection, RetryStrategy retryStrategy) {
        requireNonNull(checkpointCollection, "checkpointCollection cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.checkpointCollection = checkpointCollection;
        this.catchupMarkerJournaledForgets = withCatchupMarkerTimeout(checkpointCollection.withWriteConcern(CATCHUP_MARKER_FORGET_WRITE_CONCERN), CATCHUP_MARKER_FORGET_WRITE_TIMEOUT);
        this.catchupMarkerForgetsWithoutJournal = withCatchupMarkerTimeout(checkpointCollection.withWriteConcern(CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL), CATCHUP_MARKER_FORGET_WRITE_TIMEOUT);
        this.catchupMarkerMajorityReads = withCatchupMarkerTimeout(checkpointCollection.withReadPreference(primary()).withReadConcern(ReadConcern.MAJORITY), CATCHUP_MARKER_READ_MAX_TIME);
        this.catchupMarkerLinearizableReads = withCatchupMarkerTimeout(checkpointCollection.withReadPreference(primary()).withReadConcern(ReadConcern.LINEARIZABLE), CATCHUP_MARKER_READ_MAX_TIME);
        this.retryStrategy = retryStrategy;
    }

    private MongoCollection<Document> catchupMarkerForgets(WriteConcern writeConcern) {
        return writeConcern == CATCHUP_MARKER_FORGET_WRITE_CONCERN ? catchupMarkerJournaledForgets : catchupMarkerForgetsWithoutJournal;
    }

    private static MongoCollection<Document> withCatchupMarkerTimeout(MongoCollection<Document> collection, Duration timeout) {
        Long timeoutMillis = catchupMarkerTimeoutMillis(collection.getTimeout(TimeUnit.MILLISECONDS), timeout);
        return timeoutMillis == null ? collection : collection.withTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }


    @Override
    @Nullable
    public Checkpoint read(String subscriptionId) {
        Supplier<@Nullable Checkpoint> read = () -> {
            Document document = checkpointCollection.find(eq(ID, subscriptionId), Document.class).first();
            final Checkpoint position;
            if (document == null) {
                position = null;
            } else {
                position = calculateCheckpointFromMongoStreamPositionDocument(document);
            }

            return position;
        };
        return executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    @NullMarked
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        Document newCheckpointDocument = generateCheckpointDocument(subscriptionId, checkpoint);
        Supplier<Document> save = () -> persistConditionalCheckpointDocument(subscriptionId, newCheckpointDocument, condition);
        // Interpreting the outcome happens outside the retried supplier, so a refusal is thrown once and never
        // retried, see ADR 116, "A refused write throws, and it must never be retried".
        Document afterDocument = requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
        assertCheckpointWriteSucceeded(subscriptionId, checkpoint, condition, afterDocument);
        return checkpoint;
    }

    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        Supplier<OptionalLong> readVersion = () -> {
            Document document = checkpointCollection.find(eq(ID, subscriptionId), Document.class).first();
            return extractWriteVersion(document);
        };
        return requireNonNull(executeWithRetry(readVersion, __ -> !shutdown, retryStrategy).get());
    }

    @Override
    public void delete(String subscriptionId) {
        Runnable delete = () -> checkpointCollection.deleteOne(eq(ID, subscriptionId));
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> checkpointCollection.find(eq(ID, subscriptionId)).first() != null;
        return requireNonNull(executeWithRetry(exists, __ -> !shutdown, retryStrategy).get());
    }

    @Override
    public CatchupMarker readCatchupMarker(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        Supplier<CatchupMarker> read = () -> {
            // The checkpoint document first and the generation second, so the answer held at one instant
            Document markerDocument = catchupMarkerMajorityReads.find(eq(ID, subscriptionId))
                    .maxTime(CATCHUP_MARKER_READ_MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                    .first();
            long generation = readCatchupMarkerGeneration(subscriptionId);
            return catchupMarkerOf(markerDocument, generation);
        };
        return requireNonNull(executeWithRetry(read, __ -> !shutdown, retryStrategy).get());
    }

    private long readCatchupMarkerGeneration(String subscriptionId) {
        return catchupMarkerGenerationReads.read(
                () -> catchupMarkerLinearizableReads.find(catchupMarkerGenerationFilter(subscriptionId))
                        .maxTime(CATCHUP_MARKER_READ_MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                        .first(),
                () -> catchupMarkerMajorityReads.find(catchupMarkerGenerationFilter(subscriptionId))
                        .maxTime(CATCHUP_MARKER_READ_MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                        .first());
    }

    @Override
    public boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(marker, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        Document markerDocument = generateCatchupMarkerDocument(subscriptionId, marker, generation);
        Supplier<Document> save = () -> persistConditionalCheckpointDocument(subscriptionId, markerDocument, condition);
        Document afterDocument = requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
        return checkpointWriteSucceeded(marker, condition, afterDocument);
    }

    @Override
    public void forgetCatchupMarker(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        // Retried apart, so a failed delete never raises the generation again, and a failed raise never sends the delete
        Runnable raiseGeneration = () -> catchupMarkerForgetWrites.write(writeConcern -> catchupMarkerForgets(writeConcern)
                .updateOne(catchupMarkerGenerationFilter(subscriptionId), raiseCatchupMarkerGeneration(), new UpdateOptions().upsert(true)));
        executeWithRetry(raiseGeneration, __ -> !shutdown, retryStrategy).run();
        Runnable delete = () -> catchupMarkerForgetWrites.write(writeConcern -> catchupMarkerForgets(writeConcern).deleteOne(eq(ID, subscriptionId)));
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    /**
     * Compares by {@link MongoOperationTimeCheckpoint#operationTime}, the one shape both a stored and an offered
     * checkpoint carry when neither has ever been advanced by real delivery, and answers empty for any other stored
     * shape or for a {@code candidate} that is not a {@link MongoOperationTimeCheckpoint} to begin with. See ADR 130.
     */
    @Override
    public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
        if (!(candidate instanceof MongoOperationTimeCheckpoint)) {
            return Optional.empty();
        }
        Document candidateDocument = generateCheckpointDocument(subscriptionId, candidate);
        Supplier<Document> resolve = () -> persistFirstCheckpointRaceResolution(subscriptionId, candidateDocument);
        Document afterDocument = requireNonNull(executeWithRetry(resolve, __ -> !shutdown, retryStrategy).get());
        return interpretFirstCheckpointRaceResolution(afterDocument);
    }

    /**
     * The single {@code findOneAndUpdate} round trip {@link #resolveFirstCheckpointRace} is, see
     * {@link org.occurrent.subscription.mongodb.internal.MongoCommons#buildFirstCheckpointRaceResolution}. Package
     * private for the same reason {@link #persistConditionalCheckpointDocument} is.
     */
    Document persistFirstCheckpointRaceResolution(String subscriptionId, Document candidateDocument) {
        return requireNonNull(checkpointCollection.findOneAndUpdate(
                eq(ID, subscriptionId),
                singletonList(buildFirstCheckpointRaceResolution(candidateDocument)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)));
    }

    /**
     * The single {@code findOneAndUpdate} round trip a conditional checkpoint write is, see
     * {@link org.occurrent.subscription.mongodb.internal.MongoCommons#buildConditionalCheckpointWrite}. Package
     * private so a test can inject a transient failure into it and prove the retry in {@link #save} survives one.
     */
    Document persistConditionalCheckpointDocument(String subscriptionId, Document newCheckpointDocument, CheckpointWriteCondition condition) {
        return requireNonNull(checkpointCollection.findOneAndUpdate(
                eq(ID, subscriptionId),
                singletonList(buildConditionalCheckpointWrite(newCheckpointDocument, condition)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)));
    }

    private static RetryStrategy defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f);
    }

    @PreDestroy
    public void shutdown() {
        this.shutdown = true;
    }
}