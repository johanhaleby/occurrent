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
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A prohibition that reaches the listeners after this instance won the lease back is still delivered. The listener
 * pauses the subscription and gives the lease up, and a later round grants it again. Dropped instead, a subscription
 * whose delivery ended while the prohibition waited keeps a lease this instance refreshes for good, and nothing
 * delivers its events. The notifier falls behind here because the listener blocks on an earlier prohibition, the way
 * pausing a subscription whose change stream is still opening blocks it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy whose listener falls behind the refresh rounds")
@Timeout(30)
class MongoLeaseLateProhibitionTest {

    private static final String DATABASE = "mongoleaselateprohibition";
    private static final Duration LEASE = Duration.ofMinutes(10);
    /**
     * Long enough that a lock seeded this far in the past reads as expired against the database's own clock.
     */
    private static final Duration LONG_ENOUGH = Duration.ofSeconds(2);
    private static final String SLOW = "slow-to-pause";
    private static final String REGAINED = "regained";

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
    void a_prohibition_delivered_after_the_lease_was_won_back_still_reaches_the_listener() throws InterruptedException {
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh heldRefreshWithNotifier = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()), Executors.newSingleThreadExecutor());
        MongoLeaseCompetingConsumerStrategySupport node = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), heldRefreshWithNotifier)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        MongoLeaseCompetingConsumerStrategySupport rival = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> {
                }));
        BlockingListener listener = new BlockingListener();
        node.addListener(listener);
        assertThat(node.registerCompetingConsumer(locks, SLOW, "the-node")).isTrue();
        assertThat(node.registerCompetingConsumer(locks, REGAINED, "the-node")).isTrue();

        listener.recording.set(true);
        expireLeaseFor(SLOW);
        assertThat(rival.registerCompetingConsumer(locks, SLOW, "the-rival")).isTrue();
        scheduledRefresh.get().run();
        assertThat(listener.busy.await(10, TimeUnit.SECONDS)).as("the listener is busy pausing the first subscription").isTrue();

        expireLeaseFor(REGAINED);
        assertThat(rival.registerCompetingConsumer(locks, REGAINED, "the-rival")).isTrue();
        scheduledRefresh.get().run();
        assertThat(node.hasLock(REGAINED, "the-node")).as("the node found out it lost the lease").isFalse();
        rival.unregisterCompetingConsumer(locks, REGAINED, "the-rival");
        scheduledRefresh.get().run();
        assertThat(node.hasLock(REGAINED, "the-node")).as("the node won the lease back while the prohibition waited").isTrue();
        listener.unblock.countDown();

        listener.awaitCalls(3);
        assertThat(listener.calls)
                .as("the prohibition for " + REGAINED + " is delivered although the node holds that lease again, so the "
                        + "listener pauses the subscription and gives the lease up instead of keeping one nobody serves")
                .containsExactly("prohibited " + SLOW, "prohibited " + REGAINED, "granted " + REGAINED);
        node.shutdown();
        rival.shutdown();
    }

    private void expireLeaseFor(String subscriptionId) {
        locks.updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minus(LONG_ENOUGH)));
    }

    /**
     * Records every call once {@link #recording} is set, and blocks on the prohibition for {@link #SLOW} until
     * {@link #unblock} is counted down, after counting down {@link #busy}.
     */
    private static final class BlockingListener implements CompetingConsumerListener {
        private final AtomicBoolean recording = new AtomicBoolean();
        private final CountDownLatch busy = new CountDownLatch(1);
        private final CountDownLatch unblock = new CountDownLatch(1);
        private final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            record("granted " + subscriptionId);
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            record("prohibited " + subscriptionId);
        }

        private void record(String call) {
            if (!recording.get()) {
                return;
            }
            calls.add(call);
            if (call.equals("prohibited " + SLOW)) {
                busy.countDown();
                try {
                    unblock.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /**
         * Waits for {@code count} calls, and 200 milliseconds longer so that a call beyond them shows up as well. Gives
         * up after ten seconds, and the assertion after it then shows which calls are missing.
         */
        private void awaitCalls(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (calls.size() < count && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(200);
        }
    }
}
