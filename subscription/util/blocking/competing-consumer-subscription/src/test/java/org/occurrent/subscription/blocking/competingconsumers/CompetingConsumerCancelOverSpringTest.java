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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * A subscription this model hands to the wrapped {@link SpringMongoSubscriptionModel} while it holds no lease is held
 * paused there. Cancelling it removes it from the wrapped model, so its id can be subscribed again.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerCancelOverSpringTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient client;
    private MongoTemplate template;
    private String locks;
    private CompetingConsumerSubscriptionModel node;
    private SpringMongoLeaseCompetingConsumerStrategy rival;

    @BeforeEach
    void connect() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        locks = "locks-" + UUID.randomUUID();
    }

    @AfterEach
    void shutdown() {
        if (node != null) node.shutdown();
        if (rival != null) rival.shutdown();
        client.close();
    }

    @Test
    void a_subscription_made_while_stopped_without_its_lease_can_be_subscribed_again_after_a_cancel() {
        rival = strategy();
        assertThat(rival.registerCompetingConsumer("X", "rival")).isTrue();
        SpringMongoSubscriptionModel spring = springModel();
        node = new CompetingConsumerSubscriptionModel(spring, strategy());
        node.stop();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), __ -> {});

        node.cancelSubscription("X");

        assertThat(spring.isPaused("X")).as("paused in the wrapped model after the cancel").isFalse();
        assertThatCode(() -> node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), __ -> {}))
                .as("subscribing X again after the cancel").doesNotThrowAnyException();
    }

    @Test
    void a_subscription_made_without_its_lease_while_the_wrapped_model_runs_can_be_subscribed_again_after_a_cancel() {
        SpringMongoSubscriptionModel spring = springModel();
        node = new CompetingConsumerSubscriptionModel(spring, strategy());
        node.subscribe("node", "A", null, StartAt.subscriptionModelDefault(), __ -> {});
        await().atMost(5, SECONDS).until(() -> node.isRunning("A"));
        node.stop();
        node.resumeSubscription("A");
        await().atMost(5, SECONDS).until(() -> node.isRunning("A"));
        rival = strategy();
        assertThat(rival.registerCompetingConsumer("X", "rival")).isTrue();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), __ -> {});

        node.cancelSubscription("X");

        assertThat(spring.isPaused("X")).as("paused in the wrapped model after the cancel").isFalse();
        assertThatCode(() -> node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), __ -> {}))
                .as("subscribing X again after the cancel").doesNotThrowAnyException();
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING));
    }

    private SpringMongoLeaseCompetingConsumerStrategy strategy() {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
    }
}
