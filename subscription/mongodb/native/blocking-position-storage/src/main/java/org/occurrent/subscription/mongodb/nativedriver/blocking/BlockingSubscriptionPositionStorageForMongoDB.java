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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static org.occurrent.subscription.mongodb.internal.MongoDBCommons.*;

/**
 * A native sync Java MongoDB implementation of {@link BlockingSubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class BlockingSubscriptionPositionStorageForMongoDB implements BlockingSubscriptionPositionStorage {

    private final MongoCollection<Document> subscriptionPositionCollection;

    /**
     * Create a {@code BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public BlockingSubscriptionPositionStorageForMongoDB(MongoDatabase database, String subscriptionPositionCollection) {
        this(requireNonNull(database, "Database cannot be null").getCollection(subscriptionPositionCollection));
    }

    /**
     * Create a {@code BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public BlockingSubscriptionPositionStorageForMongoDB(MongoCollection<Document> subscriptionPositionCollection) {
        requireNonNull(subscriptionPositionCollection, "subscriptionPositionCollection cannot be null");
        this.subscriptionPositionCollection = subscriptionPositionCollection;
    }


    @Override
    public SubscriptionPosition read(String subscriptionId) {
        Document document = subscriptionPositionCollection.find(eq(ID, subscriptionId), Document.class).first();
        if (document == null) {
            return null;
        }

        return calculateSubscriptionPositionFromMongoStreamPositionDocument(document);
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        if (subscriptionPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedSubscriptionPosition) subscriptionPosition).resumeToken);
        } else if (subscriptionPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedSubscriptionPosition) subscriptionPosition).operationTime);
        } else {
            String subscriptionPositionString = subscriptionPosition.asString();
            Document document = generateGenericStreamPositionDocument(subscriptionId, subscriptionPositionString);
            persistDocumentStreamPosition(subscriptionId, document);
        }
        return subscriptionPosition;
    }

    @Override
    public void delete(String subscriptionId) {
        subscriptionPositionCollection.deleteOne(eq(ID, subscriptionId));
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private void persistDocumentStreamPosition(String subscriptionId, Document document) {
        subscriptionPositionCollection.replaceOne(eq(ID, subscriptionId), document, new ReplaceOptions().upsert(true));
    }
}