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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
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
    private NativeMongoLeaseCompetingConsumerStrategy strategyA;
    private NativeMongoLeaseCompetingConsumerStrategy strategyB;
    private CompetingConsumerSubscriptionModel nodeA;
    private CompetingConsumerSubscriptionModel nodeB;

    @BeforeEach
    void two_nodes_on_one_lease_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        String eventCollection = requireNonNull(connectionString.getCollection());
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

        // Then
        assertThatThrownBy(() -> subscriptionOnB.waitUntilStarted(EVENTUALLY))
                .as("[waitUntilStarted on B answers although the wrapped model refused the start once B was granted the lease]")
                .isInstanceOf(UnsupportedStartAtException.class);
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
}
