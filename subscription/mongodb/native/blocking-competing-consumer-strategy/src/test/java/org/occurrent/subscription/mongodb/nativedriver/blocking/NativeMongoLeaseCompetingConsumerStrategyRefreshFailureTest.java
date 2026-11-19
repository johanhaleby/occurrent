/*
 * Copyright 2026 Johan Haleby
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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.mongodb.blocking.ccs.LeaseRefreshFailure;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The strategy hands out what its lease refresh reports, which is tested where the refresh runs. This asserts that the
 * public method of the native strategy answers with it, through a refresh that fails on the schedule the strategy runs.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a native MongoDB lease strategy reporting its failing lease refresh")
@Timeout(60)
class NativeMongoLeaseCompetingConsumerStrategyRefreshFailureTest {

    private static final String DATABASE = "nativeleaserefreshfailure";
    private static final String SUBSCRIPTION = "a-subscription";
    private static final String NODE = "the-node";
    private static final Set<String> WRITES = Set.of("findOneAndUpdate", "deleteOne", "updateOne");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final AtomicBoolean mongoDbRefuses = new AtomicBoolean();
    private NativeMongoLeaseCompetingConsumerStrategy strategy;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void aStrategyHoldingALeaseWhoseRefreshIsScheduled() {
        MongoCollection<BsonDocument> locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
        strategy = new NativeMongoLeaseCompetingConsumerStrategy.Builder(refusingWhenAsked(locks))
                .leaseTime(Duration.ofSeconds(1))
                .retryStrategy(RetryStrategy.none())
                .build();
        assertThat(strategy.registerCompetingConsumer(SUBSCRIPTION, NODE)).isTrue();
    }

    @AfterEach
    void shutdown() {
        strategy.shutdown();
    }

    @Test
    void reports_no_failure_while_the_lease_refresh_works() {
        assertThat(strategy.leaseRefreshFailure()).isEmpty();
    }

    @Test
    void reports_the_failure_of_the_lease_refresh_while_it_fails_and_no_failure_once_it_works_again() {
        mongoDbRefuses.set(true);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(strategy.leaseRefreshFailure()).as("lease refresh failure").isPresent());
        LeaseRefreshFailure failure = strategy.leaseRefreshFailure().orElseThrow();
        assertThat(failure.cause()).hasMessage("MongoDB refuses the lease refresh");

        mongoDbRefuses.set(false);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(strategy.leaseRefreshFailure()).as("lease refresh failure").isEmpty());
    }

    // Refuses the calls that write the lease, which is what the scheduled refresh does, while reads work. A collection
    // derived from it refuses them too
    @SuppressWarnings("unchecked")
    private MongoCollection<BsonDocument> refusingWhenAsked(MongoCollection<BsonDocument> delegate) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(), new Class<?>[]{MongoCollection.class}, (proxy, method, args) -> {
            if (mongoDbRefuses.get() && WRITES.contains(method.getName())) {
                throw new IllegalStateException("MongoDB refuses the lease refresh");
            }
            try {
                Object result = method.invoke(delegate, args);
                return result instanceof MongoCollection<?> another ? refusingWhenAsked((MongoCollection<BsonDocument>) another) : result;
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
