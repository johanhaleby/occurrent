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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A refresh round retries a consumer whose refresh failed, and not only the MongoDB call that failed. Every call in a
 * round gives up after five attempts, so without the round's own retry a lease MongoDB refuses to refresh five times
 * in a row waits for the next round, half a lease time later, and expires if that round fails as well.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease refresh round facing a store that stops answering for a while")
@Timeout(30)
class MongoLeaseRefreshRetryBudgetTest {

    private static final String DATABASE = "mongoleaserefreshretrybudget";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String SUBSCRIPTION = "a-subscription";
    /**
     * More than the attempts one call in a round gets, and fewer than the attempts the round as a whole gets.
     */
    private static final int REFUSED_CALLS = 7;
    private static final Set<String> CALLS_TO_THE_SERVER = Set.of("findOneAndUpdate", "deleteOne", "updateOne");

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
    void refreshes_a_lease_whose_commit_fails_more_times_than_one_call_retries() throws InterruptedException {
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
        AtomicInteger refusedSoFar = new AtomicInteger();
        MongoLeaseCompetingConsumerStrategySupport support =
                new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.retry().backoff(Backoff.fixed(10)), held)
                        .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(refusingTheFirstCalls(locks, refusedSoFar)));
        assertThat(support.registerCompetingConsumer(locks, SUBSCRIPTION, "the-holder")).isTrue();
        Object expiresAtBefore = expiresAt();
        // expiresAt is computed on the database clock, so a refresh a few milliseconds later writes a later one
        Thread.sleep(50);

        scheduledRefresh.get().run();

        assertThat(expiresAt())
                .as("the round tries the refresh again once the call giving up on it has run out of attempts, so the "
                        + "lease is refreshed as soon as MongoDB answers again")
                .isNotEqualTo(expiresAtBefore);
        assertThat(support.hasLock(SUBSCRIPTION, "the-holder")).isTrue();
        assertThat(refusedSoFar).hasValue(REFUSED_CALLS);
        support.shutdown();
    }

    private Object expiresAt() {
        return database.getCollection(collectionName).find(eq("_id", SUBSCRIPTION)).first().get("expiresAt");
    }

    /**
     * A collection that throws for the first {@link #REFUSED_CALLS} calls that would reach the server, and forwards
     * every call after them. Everything else is forwarded throughout, since {@code commit} still calls
     * {@code withWriteConcern} before the {@code updateOne} that fails.
     */
    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> refusingTheFirstCalls(MongoCollection<BsonDocument> delegate, AtomicInteger refusedSoFar) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (CALLS_TO_THE_SERVER.contains(method.getName()) && refusedSoFar.getAndUpdate(n -> Math.min(n + 1, REFUSED_CALLS)) < REFUSED_CALLS) {
                        throw new IllegalStateException("MongoDB is not answering");
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof MongoCollection<?> another
                                ? refusingTheFirstCalls((MongoCollection<BsonDocument>) another, refusedSoFar)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
