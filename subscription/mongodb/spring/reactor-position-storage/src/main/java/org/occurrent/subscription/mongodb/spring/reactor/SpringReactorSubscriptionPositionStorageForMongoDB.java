/*
 * Copyright 2020 Johan Haleby
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
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Mono;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.ReactorSubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;
import org.occurrent.subscription.mongodb.internal.MongoDBCommons;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static org.occurrent.subscription.mongodb.internal.MongoDBCommons.generateOperationTimeStreamPositionDocument;
import static org.occurrent.subscription.mongodb.internal.MongoDBCommons.generateResumeTokenStreamPositionDocument;

/**
 * A Spring implementation of {@link ReactorSubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class SpringReactorSubscriptionPositionStorageForMongoDB implements ReactorSubscriptionPositionStorage {

    private final ReactiveMongoOperations mongo;
    private final String subscriptionPositionCollection;

    /**
     * Create a new instance of {@link SpringReactorSubscriptionPositionStorageForMongoDB}
     *
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting subscription positions to MongoDB.
     * @param subscriptionPositionCollection The collection that will contain the subscription position for each subscriber.
     */
    public SpringReactorSubscriptionPositionStorageForMongoDB(ReactiveMongoOperations mongo, String subscriptionPositionCollection) {
        requireNonNull(mongo, ReactiveMongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(subscriptionPositionCollection, "subscriptionPositionCollection cannot be null");
        this.mongo = mongo;
        this.subscriptionPositionCollection = subscriptionPositionCollection;
    }

    @Override
    public Mono<SubscriptionPosition> save(String subscriptionId, SubscriptionPosition changeStreamPosition) {
        Mono<?> result;
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            result = persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedSubscriptionPosition) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            result = persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedSubscriptionPosition) changeStreamPosition).operationTime);
        } else {
            String subscriptionPositionString = changeStreamPosition.asString();
            Document document = MongoDBCommons.generateGenericStreamPositionDocument(subscriptionId, subscriptionPositionString);
            result = persistDocumentStreamPosition(subscriptionId, document);
        }
        return result.thenReturn(changeStreamPosition);
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        return mongo.remove(query(where(ID).is(subscriptionId)), subscriptionPositionCollection).then();
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
        return mongo.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                subscriptionPositionCollection);
    }

    @Override
    public Mono<SubscriptionPosition> read(String subscriptionId) {
        return mongo.findOne(query(where(ID).is(subscriptionId)), Document.class, subscriptionPositionCollection)
                .map(MongoDBCommons::calculateSubscriptionPositionFromMongoStreamPositionDocument);
    }
}