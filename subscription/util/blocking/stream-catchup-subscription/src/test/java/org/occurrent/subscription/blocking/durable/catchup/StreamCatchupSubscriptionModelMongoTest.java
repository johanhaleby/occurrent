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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Covers a {@code Filter.data(..)} payload filter on the blocking stream catch-up path against a real MongoDB, during
 * the replay phase and after handover to live delivery.
 * <p>
 * Both phases answer the filter in the store rather than in process, the replay through a query and the live phase
 * through the change stream, so neither needs a way to read a payload in memory. Nothing covered that before. This
 * test is what would fail if an in-process re-check of the filter were added to this model, which is what made the
 * same filter fail on the reactive twin.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupSubscriptionModelMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(40);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "streamcatchupblocking"));

    private SpringMongoEventStore eventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> converter;
    private MongoClient mongoClient;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("streamcatchupblocking"));
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager tx = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, config);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void a_filter_on_a_data_payload_field_is_honoured_during_catchup_and_live() {
        // Both phases are asserted, because the replay answers the filter through a query and the live phase answers
        // it through the change stream, so a regression in one would go unnoticed if only the other were checked.
        appendToStream("stream-1", named("keep", "matchHistoric"));
        appendToStream("stream-1", named("drop", "ignoredHistoric"));

        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        SubscriptionHandle subscription = catchup.subscribe("subscription", StreamSubscriptionFilter.filter(Filter.data("userId", eq("keep"))),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), toNames(received));
        subscription.waitUntilStarted();

        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).containsExactly("matchHistoric"));

        appendToStream("stream-1", named("keep", "matchLive"));
        appendToStream("stream-1", named("drop", "ignoredLive"));

        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).containsExactly("matchHistoric", "matchLive"));
    }

    private Consumer<CloudEvent> toNames(List<String> target) {
        return cloudEvent -> target.add(((NameDefined) converter.toDomainEvent(cloudEvent)).name());
    }

    // Distinct userId and name, so a filter can select on one payload field while the assertion reads the other.
    private NameDefined named(String userId, String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), userId, name);
    }

    private void appendToStream(String streamId, DomainEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event));
        eventStore.write(streamId, WriteCondition.anyStreamVersion(), cloudEvents);
    }
}
