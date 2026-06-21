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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
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
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage;

/**
 * Real MongoDB integration test for {@link CatchupSubscriptionModel} in DCB mode (ADR 20). The in-memory
 * {@code DcbCatchupSubscriptionModelTest} exercises the DCB-specific replay, resume and filtering logic deterministically
 * but stubs position-awareness. This test covers the part that only a real database can: the catch-up to live handover
 * across the change stream, where DCB events written during and after the bulk replay must be delivered exactly once.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupSubscriptionModelMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final Duration AT_MOST = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                    .withReplicaSet()
                    .withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    private SpringMongoEventStore eventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private SpringMongoSubscriptionPositionStorage storage;
    private CatchupSubscriptionModel subscription;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private MongoClient mongoClient;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, requireNonNull(connectionString.getCollection()), timeRepresentation);
        storage = new SpringMongoSubscriptionPositionStorage(mongoTemplate, "storage");
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.shutdown();
        }
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void replays_dcb_history_then_delivers_live_events_across_the_handover_without_duplicates() {
        // Given a DCB history written before the subscription starts
        NameDefined historic1 = nameDefined("historic1");
        NameDefined historic2 = nameDefined("historic2");
        appendTagged("name:1", historic1);
        appendTagged("other:1", nameDefined("ignoredHistoric"));
        appendTagged("name:1", historic2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"),
                new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));

        // When the DCB-mode catch-up subscription replays from the beginning of the DCB sequence and hands over to the live change stream
        subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0)), toDomainEvents(received)).waitUntilStarted();

        // Then the matching history is delivered (non-matching events filtered out)
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(historic1, historic2));

        // And when matching and non-matching events are written after the handover
        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        appendTagged("name:1", live1);
        appendTagged("other:1", nameDefined("ignoredLive"));
        appendTagged("name:1", live2);

        // Then the matching live events arrive through the change stream, in order, with no duplicates at the seam
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() -> {
            assertThat(received).containsExactly(historic1, historic2, live1, live2);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, List.of(tag)))
                .toList();
        eventStore.append(cloudEvents);
    }
}
