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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR 116's rule is that {@code fencingToken} answers with a token only when exactly one consumer is registered
 * for a subscription in this instance, whatever its status, and that one consumer holds the lock. {@code Status}
 * is private, so every assertion here goes through {@code fencingToken} itself, the same call a fence built on
 * top of it would make.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy's fencing token")
@Timeout(30)
class MongoLeaseFencingTokenTest {

    private static final String DATABASE = "mongoleasefencingtoken";
    private static final Duration LEASE = Duration.ofMinutes(10);
    /**
     * Long enough that a lock seeded this far in the past reads as expired against the database's own clock.
     */
    private static final Duration LONG_ENOUGH = Duration.ofSeconds(2);

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

    /**
     * Writes {@code expiresAt} on a subscription's lock document directly, so it looks expired to the database's
     * own clock, which is what production code judges it against, without moving anything in this process.
     */
    private void expireLeaseFor(String subscriptionId) {
        locks.updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minus(LONG_ENOUGH)));
    }

    @Test
    void a_lock_released_consumer_has_no_fencing_token_even_though_it_stays_registered() {
        String subscription = "a-subscription";
        Node node = new Node("the-node");

        assertThat(node.register(subscription)).isTrue();
        assertThat(node.fencingToken(subscription))
                .as("the only consumer registered for this subscription holds the lock")
                .isPresent();

        node.release(subscription);

        assertThat(node.fencingToken(subscription))
                .as("a released consumer stays registered, but LOCK_RELEASED counts as not holding the lock, "
                        + "since its token belongs to the lease it just gave up")
                .isEmpty();
    }

    @Test
    void two_consumers_registered_for_one_subscription_have_no_fencing_token_until_one_unregisters() {
        String subscription = "a-subscription";
        MongoLeaseCompetingConsumerStrategySupport support =
                new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none());

        assertThat(support.registerCompetingConsumer(locks, subscription, "the-holder")).isTrue();
        assertThat(support.fencingToken(subscription)).isPresent();

        // Standing in for competing consumers inside a single node (ADR 116). A second subscriberId registers
        // for the same subscription id on the same strategy instance.
        assertThat(support.registerCompetingConsumer(locks, subscription, "the-rival")).isFalse();

        assertThat(support.fencingToken(subscription))
                .as("two consumers are registered for this subscription, one holding the lock and one not, and "
                        + "the rule stands down for as long as both are, whatever either one's status")
                .isEmpty();

        support.unregisterCompetingConsumer(locks, subscription, "the-rival");

        assertThat(support.fencingToken(subscription))
                .as("the holder is the only consumer registered again, so its token is back")
                .isPresent();
    }

    @Test
    void a_refresh_by_the_same_holder_leaves_the_fencing_token_unchanged() {
        String subscription = "a-subscription";
        Node node = new Node("the-node");
        assertThat(node.register(subscription)).isTrue();

        OptionalLong beforeRefresh = node.fencingToken(subscription);
        assertThat(beforeRefresh).isPresent();

        node.refresh();

        assertThat(node.fencingToken(subscription))
                .as("a refresh by the holder that still has the lease is a commit, which extends expiresAt "
                        + "without touching version, so the cached token stays exactly what it was")
                .isEqualTo(beforeRefresh);
    }

    @Test
    void a_genuine_change_of_owner_increases_the_fencing_token() {
        String subscription = "a-subscription";
        Node node = new Node("the-node");
        Node rival = new Node("the-rival");

        assertThat(node.register(subscription)).isTrue();
        long nodeToken = node.fencingToken(subscription).orElseThrow();

        // As if the node stopped refreshing, so the rival's next register is a genuine takeover rather than a
        // refresh.
        expireLeaseFor(subscription);

        assertThat(rival.register(subscription)).isTrue();

        assertThat(rival.fencingToken(subscription).orElseThrow())
                .as("a genuine change of owner increases the token, so the new holder's token is higher than the "
                        + "one the previous holder is left holding stale")
                .isGreaterThan(nodeToken);
    }

    /**
     * One competing consumer strategy with its own view of its consumers, standing in for one application
     * instance. Its refresh is held rather than scheduled, so {@link #refresh()} is the only thing that runs it.
     */
    private class Node {
        private final String subscriberId;
        private final MongoLeaseCompetingConsumerStrategySupport support;
        private final AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();

        private Node(String subscriberId) {
            this.subscriberId = subscriberId;
            ScheduledRefresh heldRefresh = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
            this.support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), heldRefresh)
                    .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        }

        private boolean register(String subscriptionId) {
            return support.registerCompetingConsumer(locks, subscriptionId, subscriberId);
        }

        private void release(String subscriptionId) {
            support.releaseCompetingConsumer(locks, subscriptionId, subscriberId);
        }

        private OptionalLong fencingToken(String subscriptionId) {
            return support.fencingToken(subscriptionId);
        }

        private void refresh() {
            scheduledRefresh.get().run();
        }
    }
}
