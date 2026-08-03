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
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
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

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Real MongoDB integration test proving that the blocking {@link StreamCatchupSubscriptionModel} honours a
 * {@code Filter.data(..)} payload filter both during the catch-up/replay phase and after handover to live delivery
 * (#499). {@code ReactorStreamCatchupSubscriptionModel} used to re-apply such a filter in process with a reader-less
 * matcher, so the replay succeeded server-side and the live flux then failed on the first event, for every store
 * rather than only in-memory. The blocking twin never had that in-process re-check; this test pins that it works, so
 * nobody re-introduces one here (see ADR 92).
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupSubscriptionModelMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(40);

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
            .withReplicaSet()
            .withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flush = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".streamcatchupblocking"));

    private SpringMongoEventStore eventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> converter;
    private MongoClient mongoClient;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".streamcatchupblocking");
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
        // The filter MongoDB answers when the store is queried must also be answered when it is subscribed to. This
        // pins that the blocking stream catch-up path, unlike the reactive one before ADR 92, never had an in-process
        // re-check that could reject a payload filter it cannot itself evaluate. Both phases are asserted so a
        // regression in either the replay-phase query or the live-phase change-stream filter would be caught.
        appendToStream("stream-1", named("keep", "matchHistoric"));
        appendToStream("stream-1", named("drop", "ignoredHistoric"));

        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        Subscription subscription = catchup.subscribe("subscription", StreamSubscriptionFilter.filter(Filter.data("userId", eq("keep"))),
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
