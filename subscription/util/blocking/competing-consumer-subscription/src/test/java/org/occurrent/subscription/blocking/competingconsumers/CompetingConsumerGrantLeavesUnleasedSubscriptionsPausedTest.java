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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Granting a waiting consumer on a node whose wrapped model is stopped starts that model without resuming anything
 * else paused in it. Node A was stopped while it held X, so X sits paused in A's wrapped model after node B took X
 * over, and granting A a different subscription must leave X paused.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerGrantLeavesUnleasedSubscriptionsPausedTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private CompetingConsumerSubscriptionModel nodeA, nodeB;

    @AfterEach
    void shutdown() {
        if (nodeA != null) nodeA.shutdown();
        if (nodeB != null) nodeB.shutdown();
    }

    @Test
    void granting_a_waiting_consumer_does_not_resume_a_subscription_another_node_holds() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        MongoClient client = MongoClients.create(cs);
        MongoTemplate template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        SpringMongoEventStore eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(cs.getDatabase()))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        String locks = "locks-" + UUID.randomUUID();
        SpringMongoLeaseCompetingConsumerStrategy strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
        SpringMongoLeaseCompetingConsumerStrategy strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
        nodeA = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING), strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING), strategyB);
        CopyOnWriteArrayList<CloudEvent> xOnA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> xOnB = new CopyOnWriteArrayList<>();

        // A holds X and B holds W, and each waits for the other one's
        nodeA.subscribe("a", "X", null, StartAt.subscriptionModelDefault(), xOnA::add).waitUntilStarted();
        nodeB.subscribe("b", "W", null, StartAt.subscriptionModelDefault(), __ -> {}).waitUntilStarted();
        nodeA.subscribe("a", "W", null, StartAt.subscriptionModelDefault(), __ -> {});
        nodeB.subscribe("b", "X", null, StartAt.subscriptionModelDefault(), xOnB::add);

        // A stops, which pauses X in A's wrapped model and gives up its lease, and B takes X over
        nodeA.stop();
        await("B takes X over").atMost(6, SECONDS).until(() -> strategyB.hasLock("X", "b") && nodeB.isRunning("X"));
        // A starts again, but B holds both leases, so nothing on A may run yet
        nodeA.start(true);

        // B gives W up, and A's refresh grants W to A, which starts A's stopped wrapped model
        nodeB.cancelSubscription("W");
        await("A is granted W").atMost(6, SECONDS).until(() -> strategyA.hasLock("W", "a"));
        await().atMost(3, SECONDS).until(() -> nodeA.isRunning("W"));

        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write("stream", List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        await("B, the lease holder, handles the event").atMost(5, SECONDS).untilAsserted(() -> assertThat(xOnB).extracting(CloudEvent::getId).contains(event.eventId()));

        await().during(2, SECONDS).atMost(3, SECONDS).untilAsserted(() ->
                assertThat(xOnA).as("A holds no lease for X, so A must not handle X's event").extracting(CloudEvent::getId).doesNotContain(event.eventId()));
    }
}
