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

package org.occurrent.subscription.blocking.competingconsumers;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * A native subscription model that refuses a dynamic start answering a catch-up's checkpoint does so after subscribe(..)
 * returned, when the node starts the wrapped model. The node must then neither keep the lease nor the subscription id,
 * and its handle must report the refusal. Two nodes share one lease collection in MongoDB, each with its own native model.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerRefusedStartOverNativeMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration LEASE_TIME = Duration.ofSeconds(3);
    private static final Duration EVENTUALLY = Duration.ofSeconds(15);
    private static final String SUBSCRIPTION_ID = "s";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private String eventCollection;
    private NativeMongoLeaseCompetingConsumerStrategy strategyA;
    private NativeMongoLeaseCompetingConsumerStrategy strategyB;
    private CompetingConsumerSubscriptionModel nodeA;
    private CompetingConsumerSubscriptionModel nodeB;

    @BeforeEach
    void two_nodes_on_one_lease_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = requireNonNull(connectionString.getCollection());
        String locks = "locks-" + UUID.randomUUID();
        strategyA = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
        strategyB = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
        nodeA = new CompetingConsumerSubscriptionModel(nativeModel(database, eventCollection), strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(nativeModel(database, eventCollection), strategyB);
    }

    @AfterEach
    void shutdown() {
        nodeA.shutdown();
        nodeB.shutdown();
        mongoClient.close();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_node_granted_the_lease_after_it_subscribed_with_a_start_that_the_wrapped_model_refuses_reports_the_refusal_and_gives_up_the_lease_and_the_subscription_id() {
        // Given
        nodeA.subscribe("A", SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease when B subscribes").isTrue();
        Subscription subscriptionOnB = nodeB.subscribe("B", SUBSCRIPTION_ID, null, refusedStartAt(), __ -> {
        });
        assertThat(strategyB.hasLock(SUBSCRIPTION_ID, "B")).as("B waits for the lease").isFalse();

        // When
        nodeA.cancelSubscription(SUBSCRIPTION_ID);

        // Then the handle answers true while B waits for the lease, and throws once B is granted it
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThatThrownBy(() -> subscriptionOnB.waitUntilStarted(Duration.ofMillis(100)))
                .as("[waitUntilStarted on B answers although the wrapped model refused the start once B was granted the lease]")
                .isInstanceOf(UnsupportedStartAtException.class));
        assertTheNodeGaveUpTheLeaseAndTheSubscriptionId();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_node_that_wins_the_lease_while_subscribing_with_a_start_that_the_wrapped_model_refuses_reports_the_refusal_and_gives_up_the_lease_and_the_subscription_id() {
        // Given
        Subscription subscriptionOnB = nodeB.subscribe("B", SUBSCRIPTION_ID, null, refusedStartAt(), __ -> {
        });

        // When / Then
        assertThatThrownBy(() -> subscriptionOnB.waitUntilStarted(EVENTUALLY))
                .as("[waitUntilStarted on B answers although the wrapped model refused the start]")
                .isInstanceOf(UnsupportedStartAtException.class);
        assertTheNodeGaveUpTheLeaseAndTheSubscriptionId();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void every_node_subscribing_with_a_start_that_the_wrapped_model_refuses_is_refused_once_and_leaves_the_lease_free() {
        // Given three nodes on one lease collection, each counting what it asks its wrapped model to start
        String locks = "locks-" + UUID.randomUUID();
        List<Node> nodes = Stream.of("X", "Y", "Z").map(subscriberId -> new Node(subscriberId, database, eventCollection, locks)).toList();
        try {
            // When
            List<Subscription> handles = nodes.stream().map(node -> node.model.subscribe(node.subscriberId, SUBSCRIPTION_ID, null, refusedStartAt(), __ -> {
            })).toList();

            // Then
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(handles).as("[handles of the nodes]").allSatisfy(handle ->
                    assertThatThrownBy(() -> handle.waitUntilStarted(Duration.ofMillis(100))).isInstanceOf(UnsupportedStartAtException.class)));
            Map<String, Integer> startsWhenEveryNodeWasRefused = startsPerNode(nodes);
            await().during(LEASE_TIME.multipliedBy(3)).atMost(LEASE_TIME.multipliedBy(3).plus(EVENTUALLY)).untilAsserted(() -> {
                assertThat(refusalsPerNode(nodes)).as("[refusals per node]").containsOnly(Map.entry("X", 1), Map.entry("Y", 1), Map.entry("Z", 1));
                assertThat(startsPerNode(nodes)).as("[starts asked of the wrapped model per node]").isEqualTo(startsWhenEveryNodeWasRefused);
                assertThat(nodes).as("[nodes holding the lease, or running or pausing s]").noneSatisfy(node -> assertThat(
                        node.strategy.hasLock(SUBSCRIPTION_ID, node.subscriberId) || node.model.isRunning(SUBSCRIPTION_ID) || node.model.isPaused(SUBSCRIPTION_ID)).isTrue());
            });
        } finally {
            nodes.forEach(node -> node.model.shutdown());
        }
    }

    private void assertTheNodeGaveUpTheLeaseAndTheSubscriptionId() {
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(strategyB.hasLock(SUBSCRIPTION_ID, "B"))
                .as("[B still holds the lease after the wrapped model refused the start]").isFalse());
        await().atMost(EVENTUALLY).untilAsserted(() -> {
            assertThat(nodeB.isRunning(SUBSCRIPTION_ID)).as("[s still runs on B after the refusal]").isFalse();
            assertThat(nodeB.isPaused(SUBSCRIPTION_ID)).as("[s is still paused on B after the refusal]").isFalse();
        });
        Subscription again = nodeB.subscribe("B", SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        assertThat(again.waitUntilStarted(EVENTUALLY)).as("the subscription id is free to subscribe again on B").isTrue();
    }

    // A catch-up's checkpoint that a native model has no change stream position for
    private static StartAt refusedStartAt() {
        return StartAt.dynamic(context -> StartAt.checkpoint(GlobalCheckpoint.of(0)));
    }

    private static NativeMongoSubscriptionModel nativeModel(MongoDatabase database, String eventCollection) {
        return new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
    }

    private static Map<String, Integer> refusalsPerNode(List<Node> nodes) {
        return nodes.stream().collect(toMap(node -> node.subscriberId, node -> node.refusals.get()));
    }

    private static Map<String, Integer> startsPerNode(List<Node> nodes) {
        return nodes.stream().collect(toMap(node -> node.subscriberId, node -> node.wrapped.starts.get()));
    }

    // A competing consumer model over a native model, which counts the refusals the native model reports
    private static final class Node {
        private final String subscriberId;
        private final NativeMongoLeaseCompetingConsumerStrategy strategy;
        private final CountingStarts wrapped;
        private final CompetingConsumerSubscriptionModel model;
        private final AtomicInteger refusals = new AtomicInteger();

        private Node(String subscriberId, MongoDatabase database, String eventCollection, String locks) {
            this.subscriberId = subscriberId;
            NativeMongoSubscriptionModel nativeModel = nativeModel(database, eventCollection);
            nativeModel.addStartRefusalListener((subscriptionId, refusal) -> refusals.incrementAndGet());
            strategy = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
            wrapped = new CountingStarts(nativeModel);
            model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        }
    }

    // Counts each call that asks the wrapped model to start or resume a subscription
    private static final class CountingStarts implements SubscriptionModel, SubscriptionModelWrapper {
        private final SubscriptionModel delegate;
        private final AtomicInteger starts = new AtomicInteger();

        private CountingStarts(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            starts.incrementAndGet();
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            starts.incrementAndGet();
            return delegate.subscribePaused(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            starts.incrementAndGet();
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }

        // Hands the caller's position down without resolving it
        @Override
        public boolean decidesWhereTheSubscriptionStarts() {
            return false;
        }
    }
}
