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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <a href="https://github.com/johanhaleby/occurrent/issues/999">#999</a>: closing one subscription while MongoDB is
 * unreachable. Two separate things have to hold for the close to finish. Unregistering has to stop on its own, since
 * a single subscription closing is not a shutdown and nothing sets the lifecycle flag for it. And every retried call
 * has to notice a shutdown that does arrive while it is backing off, rather than after the backoff it is halfway
 * through.
 * <p>
 * {@link MongoLeaseRefreshRetryTest} covers the refresh round, which is the other path a store that never answers
 * used to hold open.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("closing a consumer while MongoDB never answers")
// Not what these tests rely on. Each one runs the call it is about on a daemon thread joined with its own much
// shorter bound, so a call that never returns fails an assertion rather than running out this timeout.
@Timeout(60)
class MongoLeaseCloseRetryTest {

    private static final String DATABASE = "mongoleasecloseretry";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String SUBSCRIPTION = "a-subscription";
    private static final String HOLDER = "the-holder";

    /**
     * Comfortably above what a handful of attempts at a 10 ms fixed backoff take against a collection that throws
     * immediately, and well below the 10 second backoff the shutdown test configures, so a call that sleeps out its
     * whole backoff before looking at the flag fails here rather than passing slowly.
     */
    private static final Duration MUST_FINISH_WITHIN = Duration.ofSeconds(5);

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
    void unregistering_gives_up_instead_of_retrying_the_removal_forever() throws InterruptedException {
        // No maxAttempts configured, so this is the default RetryStrategy.Retry#infiniteAttempts(). A fixed 10 ms
        // backoff keeps the test fast. Nothing here calls shutdown(), which is the point. One subscription closing
        // leaves the strategy running, so the only thing that can stop the removal is the cap it is given.
        MongoLeaseCompetingConsumerStrategySupport support = supportWith(RetryStrategy.retry().backoff(Backoff.fixed(10)));
        assertThat(support.registerCompetingConsumer(locks, SUBSCRIPTION, HOLDER)).isTrue();

        Thread close = runInBackground(() -> support.unregisterCompetingConsumer(starving(locks), SUBSCRIPTION, HOLDER));
        close.join(MUST_FINISH_WITHIN.toMillis());

        assertThat(close.isAlive())
                .as("an unregister that never gives up holds the whole shutdown open behind it, and the lease it is "
                        + "trying to delete expires on its own anyway")
                .isFalse();
    }

    @Test
    void shutting_down_stops_a_registration_that_is_between_attempts() throws InterruptedException {
        CountDownLatch firstAttemptFailed = new CountDownLatch(1);
        // A 10 second backoff between attempts, far longer than this test is willing to wait. Shutdown is signaled
        // while the first backoff is being slept out, so a strategy that only reads the flag between attempts sits
        // out the whole 10 seconds first.
        MongoLeaseCompetingConsumerStrategySupport support = supportWith(
                RetryStrategy.retry().backoff(Backoff.fixed(10_000)).onError(__ -> firstAttemptFailed.countDown()));

        Thread registration = runInBackground(() -> support.registerCompetingConsumer(starving(locks), SUBSCRIPTION, HOLDER));
        assertThat(firstAttemptFailed.await(MUST_FINISH_WITHIN.toMillis(), TimeUnit.MILLISECONDS))
                .as("the first attempt should have failed and the backoff started")
                .isTrue();

        support.shutdown();
        registration.join(MUST_FINISH_WITHIN.toMillis());

        assertThat(registration.isAlive())
                .as("a shutdown signaled while a registration is backing off should stop it at the next check, not "
                        + "after the rest of the backoff")
                .isFalse();
    }

    /**
     * Never schedules the refresh, so nothing runs in the background while a test drives one call by hand.
     */
    private MongoLeaseCompetingConsumerStrategySupport supportWith(RetryStrategy retryStrategy) {
        ScheduledRefresh neverScheduled = new ScheduledRefresh((lease, scheduler) -> {
        });
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, retryStrategy, neverScheduled);
    }

    /**
     * The call under test runs here rather than on the test thread, so a call that never returns leaves the test
     * free to assert that it did not, instead of hanging with it.
     */
    private static Thread runInBackground(Runnable call) {
        Thread thread = new Thread(() -> {
            try {
                call.run();
            } catch (RuntimeException e) {
                // Giving up is what these tests are about, and giving up rethrows the last failure. The caller in
                // production, SagaSubscription.close(), catches it and carries on shutting down.
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * A collection that throws for every call that would reach the server, standing in for a MongoDB outage.
     * Everything else is forwarded, since the calls under test still use {@code withWriteConcern} before the write
     * that actually fails.
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
