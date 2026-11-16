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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
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
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.springframework.data.mongodb.core.MongoOperations;

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
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * A Spring implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in MongoDB.
 * <p>
 * As a {@link CatchupMarkerStorage} it keeps the generation of a subscription id in a second document in the same
 * collection, with the {@code _id} {@code {catchupMarker: "<subscription id>"}}. Don't delete these documents. Deleting
 * one puts the generation of that id back at {@code 0}, and a marker saved earlier can then read as present again.
 * <p>
 * {@link #forgetCatchupMarker(String)} waits for a majority of the replica set to acknowledge each of its two writes,
 * whatever write concern the {@link MongoOperations} or the database has. {@link #readCatchupMarker(String)} reads the
 * generation with {@link ReadConcern#LINEARIZABLE}, which a majority has to confirm. An attempt that gets no
 * acknowledgement or confirmation within 10 seconds fails, and the {@link RetryStrategy} decides whether to try again.
 * A server that isn't a replica set member refuses {@link ReadConcern#LINEARIZABLE}, and this storage then reads the
 * generation with {@link ReadConcern#MAJORITY} for the rest of its life.
 * <p>
 * Both writes of a forget ask for {@code j: true}, so a majority has them in its on-disk journal before acknowledging
 * them, whatever {@code writeConcernMajorityJournalDefault} says, and an acknowledged forget never rolls back. A server
 * that runs without journaling refuses {@code j: true}, and this storage then sends the forget without it for the rest
 * of its life and logs a warning the first time. That server can't journal a forget, so a crash loses an acknowledged
 * one, and the guarantee above holds only on a server with journaling.
 */
@NullMarked
public class SpringMongoCheckpointStorage implements CatchupMarkerStorage {

    private final MongoOperations mongoOperations;
    private final String checkpointCollection;
    private final RetryStrategy retryStrategy;

    private final CatchupMarkerGenerationReads catchupMarkerGenerationReads = new CatchupMarkerGenerationReads();
    private final CatchupMarkerForgetWrites catchupMarkerForgetWrites = new CatchupMarkerForgetWrites();

    private volatile boolean shutdown = false;

    /**
     * Create a {@link CheckpointStorage} that uses the Spring's {@link MongoOperations} to persist checkpoints in MongoDB.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the checkpoint.
     *
     * @param mongoOperations                The {@link MongoOperations} that'll be used to store the checkpoint
     * @param checkpointCollection The collection into which checkpoints will be stored
     */
    public SpringMongoCheckpointStorage(MongoOperations mongoOperations, String checkpointCollection) {
        this(mongoOperations, checkpointCollection, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a {@link CheckpointStorage} that uses the Spring's {@link MongoOperations} to persist checkpoints in MongoDB.
     *
     * @param mongoOperations                The {@link MongoOperations} that'll be used to store the checkpoint
     * @param checkpointCollection The collection into which checkpoints will be stored
     * @param retryStrategy                  A custom retry strategy to use if there's a problem reading/saving/deleting the checkpoint to the MongoDB storage.
     */
    public SpringMongoCheckpointStorage(MongoOperations mongoOperations, String checkpointCollection, RetryStrategy retryStrategy) {
        requireNonNull(mongoOperations, "Mongo operations cannot be null");
        requireNonNull(checkpointCollection, "checkpointCollection cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.mongoOperations = mongoOperations;
        this.checkpointCollection = checkpointCollection;
        this.retryStrategy = retryStrategy;
    }

    @Nullable
    @Override
    public Checkpoint read(String subscriptionId) {
        Supplier<@Nullable Checkpoint> read = () -> {
            Document document = mongoOperations.findOne(query(where(ID).is(subscriptionId)), Document.class, checkpointCollection);
            if (document == null) {
                return null;
            }
            return MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(document);
        };

        return executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        Document newCheckpointDocument = MongoCommons.generateCheckpointDocument(subscriptionId, checkpoint);
        Supplier<Document> save = () -> persistConditionalCheckpointDocument(subscriptionId, newCheckpointDocument, condition);
        // Interpreting the outcome happens outside the retried supplier, so a refusal is thrown once and never
        // retried, see ADR 116, "A refused write throws, and it must never be retried".
        Document afterDocument = requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
        MongoCommons.assertCheckpointWriteSucceeded(subscriptionId, checkpoint, condition, afterDocument);
        return checkpoint;
    }

    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        Supplier<OptionalLong> readVersion = () -> {
            Document document = mongoOperations.findOne(query(where(ID).is(subscriptionId)), Document.class, checkpointCollection);
            return MongoCommons.extractWriteVersion(document);
        };
        return requireNonNull(executeWithRetry(readVersion, __ -> !shutdown, retryStrategy).get());
    }

    @Override
    public void delete(String subscriptionId) {
        Runnable delete = () -> mongoOperations.remove(query(where(ID).is(subscriptionId)), checkpointCollection);
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> mongoOperations.exists(query(where(ID).is(subscriptionId)), checkpointCollection);
        return Boolean.TRUE.equals(executeWithRetry(exists, __ -> !shutdown, retryStrategy).get());
    }

    // Forgetting a marker sets a write concern per operation, which MongoOperations has no method for. Reading one
    // runs on the driver collection too, with the same filter the save uses. execute(..) still translates exceptions
    @Override
    public CatchupMarker readCatchupMarker(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        Supplier<CatchupMarker> read = () -> mongoOperations.execute(checkpointCollection, collection -> {
            // The checkpoint document first and the generation second, so the answer held at one instant
            Document markerDocument = catchupMarkerMajorityReads(collection).find(eq(ID, subscriptionId))
                    .maxTime(MongoCommons.CATCHUP_MARKER_READ_MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                    .first();
            long generation = readCatchupMarkerGeneration(collection, subscriptionId);
            return MongoCommons.catchupMarkerOf(markerDocument, generation);
        });
        return requireNonNull(executeWithRetry(read, __ -> !shutdown, retryStrategy).get());
    }

    private long readCatchupMarkerGeneration(MongoCollection<Document> collection, String subscriptionId) {
        return catchupMarkerGenerationReads.read(
                () -> withCatchupMarkerTimeout(collection.withReadPreference(primary()).withReadConcern(ReadConcern.LINEARIZABLE), MongoCommons.CATCHUP_MARKER_READ_MAX_TIME)
                        .find(MongoCommons.catchupMarkerGenerationFilter(subscriptionId))
                        .maxTime(MongoCommons.CATCHUP_MARKER_READ_MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                        .first(),
                () -> catchupMarkerMajorityReads(collection).find(MongoCommons.catchupMarkerGenerationFilter(subscriptionId))
                        .maxTime(MongoCommons.CATCHUP_MARKER_READ_MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                        .first());
    }

    @Override
    public boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(marker, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        Document markerDocument = MongoCommons.generateCatchupMarkerDocument(subscriptionId, marker, generation);
        Supplier<Document> save = () -> persistConditionalCheckpointDocument(subscriptionId, markerDocument, condition);
        Document afterDocument = requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
        return MongoCommons.checkpointWriteSucceeded(marker, condition, afterDocument);
    }

    @Override
    public void forgetCatchupMarker(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        // Retried apart, so a failed delete never raises the generation again, and a failed raise never sends the delete
        Runnable raiseGeneration = () -> catchupMarkerForgetWrites.write(writeConcern -> mongoOperations.execute(checkpointCollection, collection ->
                catchupMarkerForgets(collection, writeConcern)
                        .updateOne(MongoCommons.catchupMarkerGenerationFilter(subscriptionId), MongoCommons.raiseCatchupMarkerGeneration(), new UpdateOptions().upsert(true))));
        executeWithRetry(raiseGeneration, __ -> !shutdown, retryStrategy).run();
        Runnable delete = () -> catchupMarkerForgetWrites.write(writeConcern -> mongoOperations.execute(checkpointCollection, collection ->
                catchupMarkerForgets(collection, writeConcern).deleteOne(eq(ID, subscriptionId))));
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    private static MongoCollection<Document> catchupMarkerMajorityReads(MongoCollection<Document> collection) {
        return withCatchupMarkerTimeout(collection.withReadPreference(primary()).withReadConcern(ReadConcern.MAJORITY), MongoCommons.CATCHUP_MARKER_READ_MAX_TIME);
    }

    private static MongoCollection<Document> catchupMarkerForgets(MongoCollection<Document> collection, WriteConcern writeConcern) {
        return withCatchupMarkerTimeout(collection.withWriteConcern(writeConcern), MongoCommons.CATCHUP_MARKER_FORGET_WRITE_TIMEOUT);
    }

    private static MongoCollection<Document> withCatchupMarkerTimeout(MongoCollection<Document> collection, Duration timeout) {
        Long timeoutMillis = MongoCommons.catchupMarkerTimeoutMillis(collection.getTimeout(TimeUnit.MILLISECONDS), timeout);
        return timeoutMillis == null ? collection : collection.withTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
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
        Document candidateDocument = MongoCommons.generateCheckpointDocument(subscriptionId, candidate);
        Supplier<Document> resolve = () -> persistFirstCheckpointRaceResolution(subscriptionId, candidateDocument);
        Document afterDocument = requireNonNull(executeWithRetry(resolve, __ -> !shutdown, retryStrategy).get());
        return MongoCommons.interpretFirstCheckpointRaceResolution(afterDocument);
    }

    /**
     * The single {@code findOneAndUpdate} round trip {@link #resolveFirstCheckpointRace} is, reached the same way
     * {@link #persistConditionalCheckpointDocument} is. See
     * {@link MongoCommons#buildFirstCheckpointRaceResolution}.
     */
    Document persistFirstCheckpointRaceResolution(String subscriptionId, Document candidateDocument) {
        MongoCollection<Document> collection = mongoOperations.getCollection(checkpointCollection);
        return requireNonNull(collection.findOneAndUpdate(
                eq(ID, subscriptionId),
                singletonList(MongoCommons.buildFirstCheckpointRaceResolution(candidateDocument)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)));
    }

    /**
     * The single {@code findOneAndUpdate} round trip a conditional checkpoint write is, reached through the
     * underlying driver collection because {@link MongoOperations} has no pipeline-update equivalent that returns
     * the document a write produced. See
     * {@link org.occurrent.subscription.mongodb.internal.MongoCommons#buildConditionalCheckpointWrite}. Package
     * private so a test can inject a transient failure into it and prove the retry in {@link #save} survives one.
     */
    Document persistConditionalCheckpointDocument(String subscriptionId, Document newCheckpointDocument, CheckpointWriteCondition condition) {
        MongoCollection<Document> collection = mongoOperations.getCollection(checkpointCollection);
        return requireNonNull(collection.findOneAndUpdate(
                eq(ID, subscriptionId),
                singletonList(MongoCommons.buildConditionalCheckpointWrite(newCheckpointDocument, condition)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)));
    }

    @PreDestroy
    void shutdown() {
        shutdown = true;
    }
}
