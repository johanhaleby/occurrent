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
 * A stream catch-up started from a checkpoint the application kept from a replayed event goes live from the live start
 * that checkpoint holds, also when checkpoint storage holds a later live checkpoint for the subscription. The replay
 * stores no checkpoint of its own, so only the kept checkpoint makes the handover store its live start.
 */
@Testcontainers
@Timeout(120)
@DisplayName("StreamCatchupKeptCheckpoint")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupKeptCheckpointMongoTest {

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
    private final List<StreamCatchupSubscriptionModel> catchups = new CopyOnWriteArrayList<>();
    private final String subscriptionId = UUID.randomUUID().toString();

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
    }

    @AfterEach
    void shutdown() {
        catchups.forEach(StreamCatchupSubscriptionModel::shutdown);
        mongoClient.close();
    }

    @Test
    void a_start_from_a_kept_replay_checkpoint_goes_live_from_its_live_start_and_not_from_a_later_live_checkpoint_in_storage() {
        // Given a replay that handed over, with the checkpoint its first event was delivered with kept
        append("h1");
        append("h2");
        append("h3");
        List<CloudEvent> receivedEarlier = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel earlier = catchupOverDurable();
        earlier.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), receivedEarlier::add).waitUntilStarted();
        String kept = checkpointOf(receivedEarlier, "h1");

        // And two events delivered live after it, with the live checkpoint of the later one in storage
        append("l1");
        append("l2");
        await("the earlier subscription delivers l2").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedEarlier).extracting(this::nameOf).contains("l2"));
        String later = checkpointOf(receivedEarlier, "l2");
        earlier.shutdown();
        storage.save(subscriptionId, new StringBasedCheckpoint(later));

        // When the subscription starts again from the kept checkpoint
        List<CloudEvent> received = new CopyOnWriteArrayList<>();
        catchupOverDurable().subscribe(subscriptionId, StartAt.checkpoint(new StringBasedCheckpoint(kept)), received::add).waitUntilStarted();
        append("sentinel");

        // Then
        await("the sentinel is delivered").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(received).extracting(this::nameOf).contains("sentinel"));
        assertThat(received).extracting(this::nameOf).as("what was delivered").containsExactly("h2", "h3", "l1", "l2", "sentinel");
    }

    private StreamCatchupSubscriptionModel catchupOverDurable() {
        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
        catchups.add(catchup);
        return catchup;
    }

    private String checkpointOf(List<CloudEvent> received, String name) {
        return received.stream().filter(cloudEvent -> nameOf(cloudEvent).equals(name)).findFirst()
                .flatMap(cloudEvent -> EventMetadata.from(cloudEvent).getCheckpoint()).orElseThrow();
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
