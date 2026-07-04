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

import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.generateOperationTimeStreamPositionDocument;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.generateResumeTokenStreamPositionDocument;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * A Spring implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in MongoDB.
 */
@NullMarked
public class ReactorCheckpointStorage implements CheckpointStorage {

    private final ReactiveMongoOperations mongo;
    private final String checkpointCollection;

    /**
     * Create a new instance of {@link ReactorCheckpointStorage}
     *
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting checkpoints to MongoDB.
     * @param checkpointCollection The collection that will contain the checkpoint for each subscriber.
     */
    public ReactorCheckpointStorage(ReactiveMongoOperations mongo, String checkpointCollection) {
        requireNonNull(mongo, ReactiveMongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(checkpointCollection, "checkpointCollection cannot be null");
        this.mongo = mongo;
        this.checkpointCollection = checkpointCollection;
    }

    @Override
    public Mono<Checkpoint> save(String subscriptionId, Checkpoint changeStreamPosition) {
        Mono<?> result;
        if (changeStreamPosition instanceof MongoResumeTokenCheckpoint) {
            result = persistResumeTokenStreamPosition(subscriptionId, ((MongoResumeTokenCheckpoint) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoOperationTimeCheckpoint) {
            result = persistOperationTimeStreamPosition(subscriptionId, ((MongoOperationTimeCheckpoint) changeStreamPosition).operationTime);
        } else {
            String checkpointString = changeStreamPosition.asString();
            Document document = MongoCommons.generateGenericCheckpointDocument(subscriptionId, checkpointString);
            result = persistDocumentStreamPosition(subscriptionId, document);
        }
        return result.thenReturn(changeStreamPosition);
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        return mongo.remove(query(where(ID).is(subscriptionId)), checkpointCollection).then();
    }

    private Mono<Document> persistResumeTokenStreamPosition(String subscriptionId, BsonDocument resumeToken) {
        Document document = generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken);
        return persistDocumentStreamPosition(subscriptionId, document).thenReturn(document);
    }

    private Mono<Document> persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp timestamp) {
        Document document = generateOperationTimeStreamPositionDocument(subscriptionId, timestamp);
        return persistDocumentStreamPosition(subscriptionId, document).thenReturn(document);
    }

    private Mono<UpdateResult> persistDocumentStreamPosition(String subscriptionId, Document document) {
        // "document" carries no $-prefixed update operators, so Spring Data applies it as a full-document
        // replacement rather than a field-level merge. Any field absent from "document" is therefore dropped,
        // including the legacy "subscriptionPosition" field written before the SubscriptionPosition -> Checkpoint
        // rename: the first save after upgrade rewrites the document under the new "checkpoint" field and the legacy
        // field does not survive. This is the same replacement behaviour the native adapter gets from replaceOne.
        return mongo.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                checkpointCollection);
    }

    @Override
    public Mono<Checkpoint> read(String subscriptionId) {
        return mongo.findOne(query(where(ID).is(subscriptionId)), Document.class, checkpointCollection)
                .map(MongoCommons::calculateCheckpointFromMongoStreamPositionDocument);
    }
}