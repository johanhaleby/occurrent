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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A lease this instance lost and then won back under a new token is not reported held until its listeners have been
 * told it was lost. A listener that has not been told yet can still let an event of the subscription through on a yes,
 * while the node that held the lease in between has delivered it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy whose listener is still being told of a lost lease")
@Timeout(30)
class MongoLeaseOwedProhibitionTest {

    private static final String DATABASE = "mongoleaseowedprohibition";
    private static final Duration LEASE = Duration.ofMinutes(10);
    /**
     * Long enough that a lock seeded this far in the past reads as expired against the database's own clock.
     */
    private static final Duration LONG_ENOUGH = Duration.ofSeconds(2);
    private static final String SUBSCRIPTION = "a-subscription";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<BsonDocument> locks;

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
    void startWithNoLocks() {
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    @Test
    void has_no_lock_while_a_prohibition_is_owed() throws InterruptedException {
        // Given
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh heldRefreshWithNotifier = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()), Executors.newSingleThreadExecutor());
        MongoLeaseCompetingConsumerStrategySupport node = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), heldRefreshWithNotifier)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        MongoLeaseCompetingConsumerStrategySupport rival = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> {
                }));
        ProhibitionThatBlocks listener = new ProhibitionThatBlocks();
        node.addListener(listener);
        try {
            assertThat(node.registerCompetingConsumer(locks, SUBSCRIPTION, "the-node")).isTrue();
            OptionalLong lost = node.fencingToken(SUBSCRIPTION);
            expireLeaseFor(SUBSCRIPTION);
            assertThat(rival.registerCompetingConsumer(locks, SUBSCRIPTION, "the-rival")).isTrue();
            scheduledRefresh.get().run();
            assertThat(listener.busy.await(10, TimeUnit.SECONDS)).as("the listener is being told the node lost the lease").isTrue();

            // When
            rival.unregisterCompetingConsumer(locks, SUBSCRIPTION, "the-rival");
            scheduledRefresh.get().run();

            // Then
            assertThat(node.fencingToken(SUBSCRIPTION)).as("the node won the lease back under a new token").isNotEqualTo(lost).isPresent();
            assertThat(node.hasLock(SUBSCRIPTION, "the-node"))
                    .as("[hasLock while the listener is still being told the node lost the lease it held before]")
                    .isFalse();
        } finally {
            listener.unblock.countDown();
            node.shutdown();
            rival.shutdown();
        }
    }

    private void expireLeaseFor(String subscriptionId) {
        locks.updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minus(LONG_ENOUGH)));
    }

    // Blocks in the first prohibition after counting down busy, until unblock is counted down
    private static final class ProhibitionThatBlocks implements CompetingConsumerListener {
        private final CountDownLatch busy = new CountDownLatch(1);
        private final CountDownLatch unblock = new CountDownLatch(1);

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            if (busy.getCount() == 0) {
                return;
            }
            busy.countDown();
            try {
                unblock.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
