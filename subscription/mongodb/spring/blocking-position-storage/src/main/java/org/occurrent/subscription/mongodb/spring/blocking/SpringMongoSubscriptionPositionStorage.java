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

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeSubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoResumeTokenSubscriptionPosition;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.occurrent.subscription.mongodb.internal.MongoCloudEventsToJsonDeserializer.ID;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * A Spring implementation of {@link SubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class SpringMongoSubscriptionPositionStorage implements SubscriptionPositionStorage {
    private static final Logger log = LoggerFactory.getLogger(SpringMongoSubscriptionPositionStorage.class);

    private final MongoOperations mongoOperations;
    private final String subscriptionPositionCollection;
    private final RetryStrategy retryStrategy;

    private volatile boolean shutdown = false;

    /**
     * Create a {@link SubscriptionPositionStorage} that uses the Spring's {@link MongoOperations} to persist subscription positions in MongoDB.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the subscription position.
     *
     * @param mongoOperations                The {@link MongoOperations} that'll be used to store the subscription position
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public SpringMongoSubscriptionPositionStorage(MongoOperations mongoOperations, String subscriptionPositionCollection) {
        this(mongoOperations, subscriptionPositionCollection, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a {@link SubscriptionPositionStorage} that uses the Spring's {@link MongoOperations} to persist subscription positions in MongoDB.
     *
     * @param mongoOperations                The {@link MongoOperations} that'll be used to store the subscription position
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     * @param retryStrategy                  A custom retry strategy to use if there's a problem reading/saving/deleting the position to the MongoDB storage.
     */
    public SpringMongoSubscriptionPositionStorage(MongoOperations mongoOperations, String subscriptionPositionCollection, RetryStrategy retryStrategy) {
        requireNonNull(mongoOperations, "Mongo operations cannot be null");
        requireNonNull(subscriptionPositionCollection, "subscriptionPositionCollection cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.mongoOperations = mongoOperations;
        this.subscriptionPositionCollection = subscriptionPositionCollection;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public SubscriptionPosition read(String subscriptionId) {
        Supplier<SubscriptionPosition> read = () -> {
            Document document = mongoOperations.findOne(query(where(ID).is(subscriptionId)), Document.class, subscriptionPositionCollection);
            if (document == null) {
                return null;
            }
            return MongoCommons.calculateSubscriptionPositionFromMongoStreamPositionDocument(document);
        };

        return executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        Supplier<SubscriptionPosition> save = () -> {
            if (subscriptionPosition instanceof MongoResumeTokenSubscriptionPosition) {
                persistResumeTokenStreamPosition(subscriptionId, ((MongoResumeTokenSubscriptionPosition) subscriptionPosition).resumeToken);
            } else if (subscriptionPosition instanceof MongoOperationTimeSubscriptionPosition) {
                persistOperationTimeStreamPosition(subscriptionId, ((MongoOperationTimeSubscriptionPosition) subscriptionPosition).operationTime);
            } else {
                String subscriptionPositionString = subscriptionPosition.asString();
                Document document = MongoCommons.generateGenericSubscriptionPositionDocument(subscriptionId, subscriptionPositionString);
                persistDocumentStreamPosition(subscriptionId, document);
            }
            return subscriptionPosition;
        };

        return executeWithRetry(save, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public void delete(String subscriptionId) {
        Runnable delete = () -> mongoOperations.remove(query(where(ID).is(subscriptionId)), subscriptionPositionCollection);
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> mongoOperations.exists(query(where(ID).is(subscriptionId)), subscriptionPositionCollection);
        return executeWithRetry(exists, __ -> !shutdown, retryStrategy).get();
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, MongoCommons.generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentStreamPosition(subscriptionId, MongoCommons.generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    void persistDocumentStreamPosition(String subscriptionId, Document document) {
        mongoOperations.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                subscriptionPositionCollection);
    }

    @PreDestroy
    void shutdown() {
        shutdown = true;
    }
}