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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * A Spring implementation of {@link SubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class SpringMongoSubscriptionPositionStorage implements SubscriptionPositionStorage {

    private final MongoOperations mongoOperations;
    private final String subscriptionPositionCollection;

    /**
     * Create a {@link SubscriptionPositionStorage} that uses the Spring's {@link MongoOperations} to persist subscription positions in MongoDB.
     *
     * @param mongoOperations                The {@link MongoOperations} that'll be used to store the subscription position
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public SpringMongoSubscriptionPositionStorage(MongoOperations mongoOperations, String subscriptionPositionCollection) {
        requireNonNull(mongoOperations, "Mongo operations cannot be null");
        requireNonNull(subscriptionPositionCollection, "subscriptionPositionCollection cannot be null");

        this.mongoOperations = mongoOperations;
        this.subscriptionPositionCollection = subscriptionPositionCollection;
    }

    @Override
    public SubscriptionPosition read(String subscriptionId) {
        Document document = mongoOperations.findOne(query(where(ID).is(subscriptionId)), Document.class, subscriptionPositionCollection);
        if (document == null) {
            return null;
        }
        return MongoCommons.calculateSubscriptionPositionFromMongoStreamPositionDocument(document);
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        if (subscriptionPosition instanceof MongoResumeTokenSubscriptionPosition) {
            persistResumeTokenStreamPosition(subscriptionId, ((MongoResumeTokenSubscriptionPosition) subscriptionPosition).resumeToken);
        } else if (subscriptionPosition instanceof MongoOperationTimeSubscriptionPosition) {
            persistOperationTimeStreamPosition(subscriptionId, ((MongoOperationTimeSubscriptionPosition) subscriptionPosition).operationTime);
        } else {
            String subscriptionPositionString = subscriptionPosition.asString();
            Document document = MongoCommons.generateGenericStreamPositionDocument(subscriptionId, subscriptionPositionString);
            persistDocumentStreamPosition(subscriptionId, document);
        }
        return subscriptionPosition;
    }

    @Override
    public void delete(String subscriptionId) {
        mongoOperations.remove(query(where(ID).is(subscriptionId)), subscriptionPositionCollection);
    }

    @Override
    public boolean exists(String subscriptionId) {
        return mongoOperations.exists(query(where(ID).is(subscriptionId)), subscriptionPositionCollection);
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, MongoCommons.generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentStreamPosition(subscriptionId, MongoCommons.generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private void persistDocumentStreamPosition(String subscriptionId, Document document) {
        mongoOperations.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                subscriptionPositionCollection);
    }
}