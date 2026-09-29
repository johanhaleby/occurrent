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
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A checkpoint write from a handler that was still running when its node gave the lease up is fenced with the token
 * that node last held, so it cannot move the stored checkpoint back behind the one the new holder wrote.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointFenceAfterLeaseGivenUpTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private CompetingConsumerSubscriptionModel nodeA, nodeB;
    private SpringMongoEventStore eventStore;

    @AfterEach
    void shutdown() {
        if (nodeA != null) nodeA.shutdown();
        if (nodeB != null) nodeB.shutdown();
    }

    @Test
    void a_late_write_from_the_node_that_let_the_lease_go_never_moves_the_checkpoint_back() throws Exception {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        MongoClient client = MongoClients.create(cs);
        MongoTemplate template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(cs.getDatabase()))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        String locks = "locks-" + UUID.randomUUID();
        String checkpoints = "checkpoints-" + UUID.randomUUID();
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, checkpoints);
        SpringMongoLeaseCompetingConsumerStrategy strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(30)).collectionName(locks).build();
        SpringMongoLeaseCompetingConsumerStrategy strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(30)).collectionName(locks).build();
        nodeA = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(spring(template), storage, strategyA::fencingToken), strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(spring(template), new SpringMongoCheckpointStorage(template, checkpoints), strategyB::fencingToken), strategyB);

        AtomicBoolean holdNext = new AtomicBoolean(false);
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> onA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> onB = new CopyOnWriteArrayList<>();

        nodeA.subscribe("a", "X", null, StartAt.subscriptionModelDefault(), e -> {
            onA.add(e);
            if (holdNext.compareAndSet(true, false)) {
                handlerEntered.countDown();
                try {
                    releaseHandler.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }).waitUntilStarted();
        String seed = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(onA).extracting(CloudEvent::getId).contains(seed));

        holdNext.set(true);
        String e1 = write();
        assertThat(handlerEntered.await(5, SECONDS)).isTrue();

        // A gives the lease up while its handler for e1 is still running
        nodeA.pauseSubscription("X");

        nodeB.subscribe("b", "X", null, StartAt.subscriptionModelDefault(), onB::add).waitUntilStarted();
        String e2 = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(onB).extracting(CloudEvent::getId).contains(e1, e2));
        String e2Checkpoint = ((CheckpointAwareCloudEvent) onB.stream().filter(e -> e.getId().equals(e2)).findFirst().orElseThrow()).getCheckpoint().asString();
        await("B's checkpoint for e2 lands").atMost(5, SECONDS).until(() -> e2Checkpoint.equals(storage.read("X").asString()));

        releaseHandler.countDown();

        await("the stored checkpoint stays at e2").during(2, SECONDS).atMost(4, SECONDS)
                .until(() -> e2Checkpoint.equals(storage.read("X").asString()));
    }

    private SpringMongoSubscriptionModel spring(MongoTemplate template) {
        return new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
    }

    private String write() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write("stream", List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }
}
