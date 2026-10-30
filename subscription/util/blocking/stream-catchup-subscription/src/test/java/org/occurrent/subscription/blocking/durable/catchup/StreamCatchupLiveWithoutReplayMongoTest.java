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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A stream catch-up over a durable model that goes live without a replay hands the durable model the null its start
 * answers for it, as {@code resumeBehavior = SAME_AS_START_AT} builds it, so no checkpoint is stored for the
 * subscription. The catch-up and the durable model share one checkpoint storage, as the Spring Boot starter wires them.
 */
@Testcontainers
@Timeout(120)
@DisplayName("StreamCatchupLiveWithoutReplay")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupLiveWithoutReplayMongoTest {

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
    void a_start_at_a_live_checkpoint_that_the_durable_model_answers_null_for_stores_no_checkpoint_and_delivers_what_follows_the_checkpoint() {
        // Given a live checkpoint an event was delivered with, and an event written after it
        String liveCheckpoint = liveCheckpointOf("kept");
        append("afterKept");

        // When
        catchup.subscribe(subscriptionId, durableOffResumingAt(liveCheckpoint), received::add).waitUntilStarted();
        append("afterSubscribe");

        // Then
        await("the event written after the subscribe is delivered").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(received).extracting(this::nameOf).contains("afterSubscribe"));
        assertAll(
                () -> assertThat(received).extracting(this::nameOf).as("what was delivered").containsExactly("afterKept", "afterSubscribe"),
                () -> assertThat(storage.read(subscriptionId)).as("the checkpoint stored for the subscription").isNull());
    }

    @Test
    void a_start_at_the_model_default_that_the_durable_model_answers_null_for_stores_no_checkpoint_when_nothing_is_stored_and_delivers_what_is_written_after_it() {
        // Given a start as the default for every model but the durable one
        StartAt durableOffAtTheDefault = StartAt.dynamic(context ->
                DurableSubscriptionModel.class.isAssignableFrom(context.subscriptionModelType()) ? null : StartAt.subscriptionModelDefault());

        // When
        catchup.subscribe(subscriptionId, durableOffAtTheDefault, received::add).waitUntilStarted();
        append("first");
        append("second");

        // Then
        await("the second event is delivered").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(received).extracting(this::nameOf).contains("second"));
        assertAll(
                () -> assertThat(received).extracting(this::nameOf).as("what was delivered").containsExactly("first", "second"),
                () -> assertThat(storage.read(subscriptionId)).as("the checkpoint stored for the subscription").isNull());
    }

    // What the starter builds for startAt BEGINNING with resumeBehavior SAME_AS_START_AT and a resume point at
    // liveCheckpoint. Null for the durable model, the resume point for the catch-up and the innermost model
    private static StartAt durableOffResumingAt(String liveCheckpoint) {
        return StartAt.dynamic(context -> DurableSubscriptionModel.class.isAssignableFrom(context.subscriptionModelType())
                ? null : StartAt.checkpoint(new StringBasedCheckpoint(liveCheckpoint)));
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
