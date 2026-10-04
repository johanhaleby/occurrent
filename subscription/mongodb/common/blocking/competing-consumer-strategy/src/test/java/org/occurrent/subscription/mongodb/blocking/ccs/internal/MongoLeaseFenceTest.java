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
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What this node answers about a lease whose refreshes fail, timed by a clock the test moves, and what an unregister
 * that fails to release the lease does.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease whose refreshes fail")
@Timeout(30)
class MongoLeaseFenceTest {

    private static final String DATABASE = "mongoleasefence";
    // Long enough that MongoDB never expires the document during a test, so only the clock below decides
    private static final Duration LEASE = Duration.ofSeconds(20);
    private static final long HELD_FOR_NANOS = LEASE.toNanos() - LEASE.toNanos() / 4;
    private static final String SUBSCRIPTION = "a-subscription";
    private static final String SUBSCRIBER = "the-holder";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<BsonDocument> locks;
    private final AtomicLong clock = new AtomicLong(1_000_000_000L);
    private final AtomicReference<MongoCollection<BsonDocument>> roundsUse = new AtomicReference<>();
    private final AtomicReference<Runnable> round = new AtomicReference<>();
    private final List<String> told = new CopyOnWriteArrayList<>();
    private MongoLeaseCompetingConsumerStrategySupport support;

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
    void holdTheLease() {
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
        roundsUse.set(locks);
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh()));
        RetryStrategy retryStrategy = RetryStrategy.retry().backoff(Backoff.fixed(1)).maxAttempts(2);
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, retryStrategy, held, clock::get)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(roundsUse.get()));
        support.addListener(new CompetingConsumerListener() {
            @Override
            public void onConsumeGranted(String subscriptionId, String subscriberId) {
                told.add("granted");
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                told.add("prohibited");
            }
        });
        assertThat(support.registerCompetingConsumer(locks, SUBSCRIPTION, SUBSCRIBER)).isTrue();
        told.clear();
    }

    @AfterEach
    void dropTheLocks() {
        support.shutdown();
        locks.drop();
    }

    @Test
    void says_the_lease_is_not_held_once_it_could_have_expired_and_grants_it_again_when_a_refresh_gets_through() {
        roundsUse.set(starving(locks));
        round.get().run();
        clock.addAndGet(HELD_FOR_NANOS - 1);
        assertThat(support.hasLock(SUBSCRIPTION, SUBSCRIBER))
                .as("short of three quarters of the lease time since the request that set the lease, it cannot have expired")
                .isTrue();

        clock.addAndGet(1);
        round.get().run();
        assertThat(support.hasLock(SUBSCRIPTION, SUBSCRIBER))
                .as("every refresh since the lease was set failed, so by now another node may hold it")
                .isFalse();

        roundsUse.set(locks);
        round.get().run();
        assertThat(support.hasLock(SUBSCRIPTION, SUBSCRIBER))
                .as("the refresh that got through extended the lease from the time its request was sent")
                .isTrue();
        assertThat(told)
                .as("a listener told no while the lease could have expired hears that it is held again")
                .containsExactly("granted");
    }

    @Test
    void gives_up_the_lease_on_the_next_round_after_an_unregister_failed_to() {
        assertThatThrownBy(() -> support.unregisterCompetingConsumer(starving(locks), SUBSCRIPTION, SUBSCRIBER))
                .hasMessage("MongoDB is not answering");
        assertThat(support.hasLock(SUBSCRIPTION, SUBSCRIBER))
                .as("an unregistered consumer holds no lease, also while the document still names it")
                .isFalse();
        assertThat(locks.countDocuments(heldByTheHolder())).isEqualTo(1);

        round.get().run();

        assertThat(locks.countDocuments(heldByTheHolder()))
                .as("the round after the failed release releases the lease, so no other node waits for it to expire")
                .isZero();
        assertThat(told).isEmpty();
        round.get().run();
        assertThat(locks.countDocuments(heldByTheHolder()))
                .as("once the lease is released the consumer is forgotten, so no later round takes the lease for it")
                .isZero();
    }

    private static Bson heldByTheHolder() {
        return and(eq("_id", SUBSCRIPTION), eq("subscriberId", SUBSCRIBER));
    }

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
