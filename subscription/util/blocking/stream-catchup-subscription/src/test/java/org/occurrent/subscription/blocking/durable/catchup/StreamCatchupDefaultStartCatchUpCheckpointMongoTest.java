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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A stream catch-up over a durable model, wired as the Spring Boot starter wires them with one checkpoint storage, that
 * starts at the model default or at a dynamic start that answers the model default replays from the catch-up checkpoint
 * a catch-up stored, {@code GlobalCheckpoint.of(2, <live start>, 0, 2)}, instead of going live at the present.
 */
@Testcontainers
@Timeout(120)
@DisplayName("StreamCatchupDefaultStartCatchUpCheckpoint")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupDefaultStartCatchUpCheckpointMongoTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private String eventCollectionName;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private SpringMongoCheckpointStorage storage;
    private StreamCatchupSubscriptionModel catchup;
    private final String subscriptionId = UUID.randomUUID().toString();
    private final List<CloudEvent> received = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        eventCollectionName = requireNonNull(connectionString.getCollection());
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        catchup = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
        mongoClient.close();
    }

    @Test
    void a_start_at_the_model_default_replays_from_the_catch_up_checkpoint_in_storage_and_goes_live_from_its_live_start() {
        replaysFromTheCatchUpCheckpointInStorage(StartAt.subscriptionModelDefault());
    }

    @Test
    void a_dynamic_start_that_answers_the_model_default_replays_from_the_catch_up_checkpoint_in_storage_and_goes_live_from_its_live_start() {
        replaysFromTheCatchUpCheckpointInStorage(StartAt.dynamic(() -> StartAt.subscriptionModelDefault()));
    }

    private void replaysFromTheCatchUpCheckpointInStorage(StartAt startAt) {
        // Given both historic events were delivered, the live start was read after them and the catch-up was interrupted
        append("historic-1");
        String liveStart = liveCheckpointOf("historic-2");
        storage.save(subscriptionId, GlobalCheckpoint.of(2, new StringBasedCheckpoint(liveStart), 0, 2));
        // And an event was written while nothing delivered
        append("while-down-1");

        // When
        catchup.subscribe(subscriptionId, startAt, received::add).waitUntilStarted();
        append("live-2");

        // Then everything after the checkpoint is delivered once, in order, and nothing before it
        await("the event written after the subscribe is delivered").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(received).extracting(this::nameOf).contains("live-2"));
        assertThat(received).extracting(this::nameOf).containsExactly("while-down-1", "live-2");
    }

    // The change stream checkpoint the event named name is delivered live with
    private String liveCheckpointOf(String name) {
        SpringMongoSubscriptionModel probe = springModel();
        try {
            List<CloudEvent> seen = new CopyOnWriteArrayList<>();
            probe.subscribe("probe", null, StartAt.now(), seen::add).waitUntilStarted();
            append(name);
            await("the probe sees " + name).atMost(10, SECONDS).untilAsserted(() -> assertThat(seen).hasSize(1));
            return EventMetadata.from(seen.get(0)).getCheckpoint().orElseThrow();
        } finally {
            probe.shutdown();
        }
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    private void append(String name) {
        DomainEvent event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", name);
        eventStore.write(UUID.randomUUID().toString(), cloudEventConverter.toCloudEvents(List.of(event)));
    }
}
