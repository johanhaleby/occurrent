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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A refresh round that is retried because one lease could not be written refreshes only that lease again, and not the
 * leases it already refreshed. The refusal is a collection validator that rejects any write to the one lease
 * document, and the calls reaching MongoDB for the other lease are counted.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a retried MongoDB lease refresh round")
@Timeout(30)
class MongoLeaseRefreshRetryScopeTest {

    private static final String DATABASE = "mongoleaserefreshretryscope";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String FAILING = "failing";
    private static final String HEALTHY = "healthy";
    private static final Set<String> CALLS_TO_THE_SERVER = Set.of("findOneAndUpdate", "deleteOne", "updateOne", "find", "countDocuments");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private String collectionName;
    private MongoCollection<BsonDocument> locks;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void startWithNoLocks() {
        collectionName = "competing-consumer-locks-" + UUID.randomUUID();
        locks = database.getCollection(collectionName, BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    @Test
    void refreshes_again_only_the_lease_that_could_not_be_written() {
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
        AtomicInteger callsForHealthy = new AtomicInteger();
        MongoLeaseCompetingConsumerStrategySupport support =
                new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.retry().backoff(Backoff.fixed(10)), held)
                        .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(countingCallsFor(HEALTHY, locks, callsForHealthy)));
        assertThat(support.registerCompetingConsumer(locks, FAILING, "the-node")).isTrue();
        assertThat(support.registerCompetingConsumer(locks, HEALTHY, "the-node")).isTrue();
        scheduledRefresh.get().run();
        int callsInARoundThatSucceeds = callsForHealthy.getAndSet(0);
        assertThat(callsInARoundThatSucceeds).as("a refresh of the healthy lease reaches MongoDB").isPositive();

        database.runCommand(new Document("collMod", collectionName)
                .append("validator", new Document("$expr", new Document("$ne", List.of("$_id", FAILING))))
                .append("validationLevel", "strict").append("validationAction", "error"));
        scheduledRefresh.get().run();

        assertThat(callsForHealthy)
                .as("the retries of the round refresh only the lease MongoDB refused, not the healthy one again")
                .hasValue(callsInARoundThatSucceeds);
        assertThat(support.hasLock(HEALTHY, "the-node")).isTrue();
        support.shutdown();
    }

    /**
     * A collection that forwards every call, and counts the calls reaching the server whose arguments name
     * {@code subscriptionId}.
     */
    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> countingCallsFor(String subscriptionId, MongoCollection<BsonDocument> delegate, AtomicInteger calls) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (CALLS_TO_THE_SERVER.contains(method.getName()) && Arrays.toString(args).contains(subscriptionId)) {
                        calls.incrementAndGet();
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof MongoCollection<?> another
                                ? countingCallsFor(subscriptionId, (MongoCollection<BsonDocument>) another, calls)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
