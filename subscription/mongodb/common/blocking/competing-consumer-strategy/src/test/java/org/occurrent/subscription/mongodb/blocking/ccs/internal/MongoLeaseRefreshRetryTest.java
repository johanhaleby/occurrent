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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <a href="https://github.com/johanhaleby/occurrent/issues/691">#691</a>: a refresh round runs on a single-thread
 * scheduler that starts the next round only once the current one returns, and both the round itself and every
 * MongoDB call a consumer inside it makes used to retry without limit. A store that stayed down for the whole round
 * meant the round never returned, which meant no later round ever ran either. This is the refresh path only.
 * {@link MongoLeaseRaceTest} and {@link MongoLeaseTimingTest} cover what a lease does once a round runs. This one
 * covers whether a round facing a store that never answers runs to completion at all.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease refresh round facing a store that never answers")
// The one thing this class exists to catch is a round that never returns, so this is a backstop rather than the
// mechanism the test relies on: the round itself runs on a daemon thread joined with its own much shorter bound.
@Timeout(30)
class MongoLeaseRefreshRetryTest {

    private static final String DATABASE = "mongoleaserefreshretry";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String SUBSCRIPTION = "a-subscription";

    /**
     * Comfortably above what a handful of attempts at a 10 ms fixed backoff take against a collection that throws
     * immediately, comfortably below this class's own {@code @Timeout}, so a round that never gives up fails this
     * test in seconds instead of running out the whole suite's clock.
     */
    private static final Duration ROUND_MUST_FINISH_WITHIN = Duration.ofSeconds(5);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

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
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    @Test
    void gives_up_instead_of_retrying_a_failing_commit_forever() throws InterruptedException {
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
        // No maxAttempts configured, so this is RetryStrategy.Retry#infiniteAttempts(), the default every builder
        // ships. A fixed 10 ms backoff keeps the test fast. How many attempts the refresh path caps this at is not
        // asserted here.
        RetryStrategy retryStrategy = RetryStrategy.retry().backoff(Backoff.fixed(10));
        MongoLeaseCompetingConsumerStrategySupport support =
                new MongoLeaseCompetingConsumerStrategySupport(LEASE, retryStrategy, held)
                        .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(starving(locks)));

        // Registers against the real collection, so the consumer holds the lease before the store starts failing.
        assertThat(support.registerCompetingConsumer(locks, SUBSCRIPTION, "the-holder")).isTrue();

        Thread round = new Thread(scheduledRefresh.get());
        round.setDaemon(true);
        round.start();
        round.join(ROUND_MUST_FINISH_WITHIN.toMillis());

        assertThat(round.isAlive())
                .as("a round that never gives up on a store that never answers blocks every later round behind it, "
                        + "since the schedule starts the next execution only once this one returns")
                .isFalse();
        assertThat(support.hasLock(SUBSCRIPTION, "the-holder"))
                .as("a commit that never got through changed nothing in the lock document, so the holder keeps the "
                        + "lease it already has rather than being told it lost one")
                .isTrue();
    }

    /**
     * A collection that throws for every call that would reach the server, standing in for a MongoDB outage that
     * lasts the whole round. Everything else is forwarded, since {@code commit} still calls {@code withWriteConcern}
     * before the {@code updateOne} that actually fails.
     */
    private static final Set<String> CALLS_TO_THE_SERVER = Set.of("findOneAndUpdate", "deleteOne", "updateOne");

    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> starving(MongoCollection<BsonDocument> delegate) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (CALLS_TO_THE_SERVER.contains(method.getName())) {
                        throw new IllegalStateException("MongoDB is not answering");
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof MongoCollection<?> another
                                ? starving((MongoCollection<BsonDocument>) another)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
