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
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
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
 * A subscription restarted because the change stream history for its stored checkpoint is gone records the position
 * it restarted from, so the next process start resumes from there. Otherwise that process reads the lost position
 * again, restarts from its own present, and skips every event written while nothing ran. The lost history is a
 * {@code failCommand} fail point answering the {@code aggregate} that opens the change stream with error code 286.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionHistoryLostRestartTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private DurableSubscriptionModel running;

    @AfterEach
    void shutdown() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        if (running != null) running.shutdown();
    }

    @Test
    void events_written_after_a_history_lost_restart_survive_a_process_restart() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(cs.getDatabase()))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        String checkpoints = "checkpoints-" + UUID.randomUUID();
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, checkpoints);

        // Process 1 checkpoints event e0 at token T0
        CopyOnWriteArrayList<CloudEvent> p1 = new CopyOnWriteArrayList<>();
        running = durable(storage);
        running.subscribe("X", p1::add).waitUntilStarted();
        String e0 = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(p1).extracting(CloudEvent::getId).contains(e0));
        await().pollDelay(Duration.ofMillis(300)).until(() -> true);
        String t0 = storage.read("X").asString();
        running.shutdown();

        // Process 2 starts after T0 fell off the oplog, so its change stream history is lost and it restarts
        historyLostOnNextOpen();
        running = durable(storage);
        running.subscribe("X", __ -> {
        }).waitUntilStarted();
        await("the position process 2 restarted from is stored").atMost(5, SECONDS).until(() -> !t0.equals(storage.read("X").asString()));
        running.shutdown();

        // Written while no process runs, after process 2 had already restarted past T0
        String eDown = write();

        // Process 3 starts. Only T0 is gone from the oplog, so the history is lost again only if T0 is what it reads.
        if (t0.equals(storage.read("X").asString())) {
            historyLostOnNextOpen();
        }
        CopyOnWriteArrayList<CloudEvent> p3 = new CopyOnWriteArrayList<>();
        running = durable(storage);
        running.subscribe("X", p3::add).waitUntilStarted();
        String eAfter = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(p3).extracting(CloudEvent::getId).contains(eAfter));

        assertThat(p3).as("the event written while no process ran is still in the oplog and must be delivered").extracting(CloudEvent::getId).contains(eDown);
    }

    private DurableSubscriptionModel durable(SpringMongoCheckpointStorage storage) {
        return new DurableSubscriptionModel(new SpringMongoSubscriptionModel(template,
                SpringMongoSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true)), storage);
    }

    private void historyLostOnNextOpen() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 286)));
    }

    private String write() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }
}
