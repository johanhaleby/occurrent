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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.*;

/**
 * A native sync Java MongoDB implementation of {@link SubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class NativeMongoSubscriptionPositionStorage implements SubscriptionPositionStorage {

    private final MongoCollection<Document> subscriptionPositionCollection;
    private final RetryStrategy retryStrategy;

    private volatile boolean shutdown = false;

    /**
     * Create a {@code BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the subscription position.
     *
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public NativeMongoSubscriptionPositionStorage(MongoDatabase database, String subscriptionPositionCollection) {
        this(database, subscriptionPositionCollection, defaultRetryStrategy());
    }

    /**
     * Create a {@code BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public NativeMongoSubscriptionPositionStorage(MongoDatabase database, String subscriptionPositionCollection, RetryStrategy retryStrategy) {
        this(requireNonNull(database, "Database cannot be null").getCollection(subscriptionPositionCollection), retryStrategy);
    }

    /**
     * Create a {@code BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the subscription position.
     *
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public NativeMongoSubscriptionPositionStorage(MongoCollection<Document> subscriptionPositionCollection) {
        this(subscriptionPositionCollection, defaultRetryStrategy());
    }

    /**
     * Create a {@code BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     * @param retryStrategy                  A custom retry strategy to use if there's a problem reading/saving/deleting the position to the MongoDB storage.
     */
    public NativeMongoSubscriptionPositionStorage(MongoCollection<Document> subscriptionPositionCollection, RetryStrategy retryStrategy) {
        requireNonNull(subscriptionPositionCollection, "subscriptionPositionCollection cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.subscriptionPositionCollection = subscriptionPositionCollection;
        this.retryStrategy = retryStrategy;
    }


    @Override
    public SubscriptionPosition read(String subscriptionId) {
        Supplier<SubscriptionPosition> read = () -> {
            Document document = subscriptionPositionCollection.find(eq(ID, subscriptionId), Document.class).first();
            if (document == null) {
                return null;
            }

            return calculateSubscriptionPositionFromMongoStreamPositionDocument(document);
        };
        return executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        Supplier<SubscriptionPosition> save = () -> {
            if (subscriptionPosition instanceof MongoResumeTokenSubscriptionPosition) {
                persistResumeTokenSubscriptionPosition(subscriptionId, ((MongoResumeTokenSubscriptionPosition) subscriptionPosition).resumeToken);
            } else if (subscriptionPosition instanceof MongoOperationTimeSubscriptionPosition) {
                persistOperationTimeSubscriptionPosition(subscriptionId, ((MongoOperationTimeSubscriptionPosition) subscriptionPosition).operationTime);
            } else {
                String subscriptionPositionString = subscriptionPosition.asString();
                Document document = generateGenericSubscriptionPositionDocument(subscriptionId, subscriptionPositionString);
                persistDocumentSubscriptionPosition(subscriptionId, document);
            }
            return subscriptionPosition;
        };

        return executeWithRetry(save, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public void delete(String subscriptionId) {
        Runnable delete = () -> subscriptionPositionCollection.deleteOne(eq(ID, subscriptionId));
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> subscriptionPositionCollection.find(eq(ID, subscriptionId)).first() != null;
        return executeWithRetry(exists, __ -> !shutdown, retryStrategy).get();
    }

    private void persistResumeTokenSubscriptionPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentSubscriptionPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeSubscriptionPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentSubscriptionPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    void persistDocumentSubscriptionPosition(String subscriptionId, Document document) {
        subscriptionPositionCollection.replaceOne(eq(ID, subscriptionId), document, new ReplaceOptions().upsert(true));
    }

    private static RetryStrategy defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f);
    }

    @PreDestroy
    public void shutdown() {
        this.shutdown = true;
    }
}