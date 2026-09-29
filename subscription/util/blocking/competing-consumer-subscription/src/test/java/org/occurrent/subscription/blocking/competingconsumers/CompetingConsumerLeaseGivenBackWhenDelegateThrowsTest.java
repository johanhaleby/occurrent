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
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A lease won for a subscription the wrapped model then refuses to start is handed back, so another node can take the
 * subscription over. Node A's wrapped model already has a subscription with the same id, subscribed on it directly,
 * which makes every start of that id on A throw {@link DuplicateSubscriptionIdException} after the lease is won.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseGivenBackWhenDelegateThrowsTest {
    private static final String SUBSCRIPTION_ID = "subscription";

    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoTemplate template;
    private SpringMongoSubscriptionModel delegateA;
    private SpringMongoLeaseCompetingConsumerStrategy strategyA, strategyB;
    private CompetingConsumerSubscriptionModel nodeA, nodeB;

    @BeforeEach
    void create_two_nodes() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        template = new MongoTemplate(MongoClients.create(cs), requireNonNull(cs.getDatabase()));
        String locks = "locks-" + UUID.randomUUID();
        strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
        strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
        delegateA = new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
        nodeA = new CompetingConsumerSubscriptionModel(delegateA, strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING), strategyB);
    }

    @AfterEach
    void shutdown() {
        nodeA.shutdown();
        nodeB.shutdown();
    }

    @Test
    void a_subscribe_the_wrapped_model_refuses_after_the_lease_is_won_hands_the_lease_back() {
        delegateA.subscribe(SUBSCRIPTION_ID, __ -> {
        }).waitUntilStarted();

        Throwable subscribeOnA = catchThrowable(() -> nodeA.subscribe("a", SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        assertThat(subscribeOnA).isInstanceOf(DuplicateSubscriptionIdException.class);

        nodeB.subscribe("b", SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        await("node B takes over the subscription nobody on node A serves").atMost(8, SECONDS)
                .until(() -> strategyB.hasLock(SUBSCRIPTION_ID, "b"));
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "a")).isFalse();
    }

    @Test
    void a_waiting_consumer_the_wrapped_model_refuses_to_start_when_granted_hands_the_lease_back() throws InterruptedException {
        nodeB.subscribe("b", SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }).waitUntilStarted();
        nodeA.subscribe("a", SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
        delegateA.subscribe(SUBSCRIPTION_ID, __ -> {
        }).waitUntilStarted();

        CountDownLatch grantedToA = new CountDownLatch(1);
        strategyA.addListener(new CompetingConsumerListener() {
            @Override
            public void onConsumeGranted(String subscriptionId, String subscriberId) {
                grantedToA.countDown();
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            }
        });

        // B lets go, so A's refresh grants A the lease, and A's wrapped model refuses to start the subscription
        nodeB.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(grantedToA.await(8, SECONDS)).as("A is granted the lease B let go").isTrue();
        nodeB.subscribe("b", SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        await("node B takes the subscription back from the node that cannot serve it").atMost(8, SECONDS)
                .until(() -> strategyB.hasLock(SUBSCRIPTION_ID, "b") && nodeB.isRunning(SUBSCRIPTION_ID));
    }
}
