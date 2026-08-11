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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.OptionalLong;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * A Spring implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in MongoDB.
 */
@NullMarked
public class ReactorCheckpointStorage implements CheckpointStorage {

    private final ReactiveMongoOperations mongo;
    private final String checkpointCollection;
    private final Retry retry;

    /**
     * Create a new instance of {@link ReactorCheckpointStorage}. Uses a default {@link Retry} with the same
     * exponential backoff interval as the blocking {@code SpringMongoCheckpointStorage}, 100 ms up to 2 seconds,
     * but bounded to 5 attempts before the original failure is rethrown, rather than retrying without limit.
     *
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting checkpoints to MongoDB.
     * @param checkpointCollection The collection that will contain the checkpoint for each subscriber.
     */
    public ReactorCheckpointStorage(ReactiveMongoOperations mongo, String checkpointCollection) {
        this(mongo, checkpointCollection, defaultRetry());
    }

    /**
     * Create a new instance of {@link ReactorCheckpointStorage}.
     *
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting checkpoints to MongoDB.
     * @param checkpointCollection The collection that will contain the checkpoint for each subscriber.
     * @param retry                       The {@link Retry} to use if there's a problem reading/saving/deleting the checkpoint in MongoDB.
     */
    public ReactorCheckpointStorage(ReactiveMongoOperations mongo, String checkpointCollection, Retry retry) {
        requireNonNull(mongo, ReactiveMongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(checkpointCollection, "checkpointCollection cannot be null");
        requireNonNull(retry, Retry.class.getSimpleName() + " cannot be null");
        this.mongo = mongo;
        this.checkpointCollection = checkpointCollection;
        this.retry = retry;
    }

    @Override
    public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        Document newCheckpointDocument = MongoCommons.generateCheckpointDocument(subscriptionId, checkpoint);
        return persistConditionalCheckpointDocument(subscriptionId, newCheckpointDocument, condition)
                .retryWhen(retry)
                // Interpreting the outcome happens outside retryWhen, so a refusal is signalled once and never
                // retried, see ADR 116, "A refused write throws, and it must never be retried".
                .map(afterDocument -> {
                    MongoCommons.assertCheckpointWriteSucceeded(subscriptionId, checkpoint, condition, afterDocument);
                    return checkpoint;
                });
    }

    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    @Override
    public Mono<Long> writeVersion(String subscriptionId) {
        return mongo.findOne(query(where(ID).is(subscriptionId)), Document.class, checkpointCollection)
                .retryWhen(retry)
                .flatMap(document -> {
                    OptionalLong version = MongoCommons.extractWriteVersion(document);
                    return version.isPresent() ? Mono.just(version.getAsLong()) : Mono.empty();
                });
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        return mongo.remove(query(where(ID).is(subscriptionId)), checkpointCollection).retryWhen(retry).then();
    }

    /**
     * The single {@code findOneAndUpdate} round trip a conditional checkpoint write is, reached through the
     * underlying reactive streams collection because {@link ReactiveMongoOperations} has no pipeline-update
     * equivalent that returns the document a write produced. See
     * {@link org.occurrent.subscription.mongodb.internal.MongoCommons#buildConditionalCheckpointWrite}.
     */
    private Mono<Document> persistConditionalCheckpointDocument(String subscriptionId, Document newCheckpointDocument, CheckpointWriteCondition condition) {
        return mongo.getCollection(checkpointCollection)
                .flatMap(collection -> Mono.from(collection.findOneAndUpdate(
                        Filters.eq(ID, subscriptionId),
                        singletonList(MongoCommons.buildConditionalCheckpointWrite(newCheckpointDocument, condition)),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true))));
    }

    @Override
    public Mono<Checkpoint> read(String subscriptionId) {
        return mongo.findOne(query(where(ID).is(subscriptionId)), Document.class, checkpointCollection)
                .retryWhen(retry)
                .map(MongoCommons::calculateCheckpointFromMongoStreamPositionDocument);
    }

    private static Retry defaultRetry() {
        return Retry.backoff(5, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
