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

import jakarta.annotation.PreDestroy;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Duration;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * A Spring implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in MongoDB.
 */
@NullMarked
public class SpringMongoCheckpointStorage implements CheckpointStorage {

    private final MongoOperations mongoOperations;
    private final String checkpointCollection;
    private final RetryStrategy retryStrategy;

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
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
        Supplier<Checkpoint> save = () -> {
            if (checkpoint instanceof MongoResumeTokenCheckpoint mongoResumeTokenCheckpoint) {
                persistResumeTokenStreamPosition(subscriptionId, mongoResumeTokenCheckpoint.resumeToken);
            } else if (checkpoint instanceof MongoOperationTimeCheckpoint mongoOperationTimeCheckpoint) {
                persistOperationTimeStreamPosition(subscriptionId, mongoOperationTimeCheckpoint.operationTime);
            } else {
                String checkpointString = checkpoint.asString();
                Document document = MongoCommons.generateGenericCheckpointDocument(subscriptionId, checkpointString);
                persistDocumentStreamPosition(subscriptionId, document);
            }
            return checkpoint;
        };

        return requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
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

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, MongoCommons.generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentStreamPosition(subscriptionId, MongoCommons.generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    void persistDocumentStreamPosition(String subscriptionId, Document document) {
        // "document" has no $-prefixed operators, so Spring Data applies it as a full-document replacement,
        // not a field-level merge. That drops the legacy "subscriptionPosition" field (from before the
        // SubscriptionPosition -> Checkpoint rename) on the first save after upgrade, same as the native
        // adapter's replaceOne.
        mongoOperations.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                checkpointCollection);
    }

    @PreDestroy
    void shutdown() {
        shutdown = true;
    }
}
