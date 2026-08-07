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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StreamSubscriptionFilter;
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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.hamcrest.Matchers.is;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.in;
import static org.occurrent.condition.Condition.ne;
import static org.occurrent.filter.Filter.time;

/**
 * A pre-upgrade event can still hold its {@code time} value in the legacy, variable-width shape written by
 * {@code OffsetDateTime.toString()} rather than the canonical fixed-width shape (ADR 79). These tests insert that
 * legacy shape directly into the underlying collection, simulating an event no rewrite has touched, and confirm
 * that a subscription filtering with {@code eq}/{@code in}/{@code ne} under {@code RFC_3339_STRING} still sees it
 * correctly through the change-stream match.
 */
@Testcontainers
class SpringMongoSubscriptionModelLegacyTimeFormatFilterTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final URI SOURCE = URI.create("http://legacy-time-format-filter-test");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private String eventCollectionName;
    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void create_spring_mongo_event_store_and_subscription_model() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, database);
        eventCollectionName = connectionString.getCollection();
        MongoTransactionManager transactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, database));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, config);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void eq_matches_an_event_delivered_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        Document legacyDocument = prepareLegacyTimeShapedEvent(instant);
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), StreamSubscriptionFilter.filter(time(eq(instant))), state::add).waitUntilStarted();

        // When
        eventCollection().insertOne(legacyDocument);

        // Then
        await().atMost(FIVE_SECONDS).until(state::size, is(1));
        assertThat(state).extracting(CloudEvent::getId).containsOnly(legacyDocument.getString("id"));
    }

    @Test
    void in_matches_an_event_delivered_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        Document legacyDocument = prepareLegacyTimeShapedEvent(instant);
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), StreamSubscriptionFilter.filter(time(in(instant, instant.plusDays(1)))), state::add).waitUntilStarted();

        // When
        eventCollection().insertOne(legacyDocument);

        // Then
        await().atMost(FIVE_SECONDS).until(state::size, is(1));
        assertThat(state).extracting(CloudEvent::getId).containsOnly(legacyDocument.getString("id"));
    }

    @Test
    void ne_excludes_an_event_delivered_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        Document legacyDocument = prepareLegacyTimeShapedEvent(instant);
        CloudEvent canaryEvent = newEvent(instant.plusDays(1));
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), StreamSubscriptionFilter.filter(time(ne(instant))), state::add).waitUntilStarted();

        // When: the legacy-shaped event at the excluded instant, and a canary event at another instant that proves
        // the subscription is alive and would have seen the legacy event too had the filter not excluded it.
        eventCollection().insertOne(legacyDocument);
        mongoEventStore.write(UUID.randomUUID().toString(), List.of(canaryEvent));

        // Then
        await().atMost(FIVE_SECONDS).until(state::size, is(1));
        assertThat(state).extracting(CloudEvent::getId).containsOnly(canaryEvent.getId());
    }

    /**
     * Writes a normal event, which stores the canonical shape, then removes it from the collection and hands back
     * the same document with its {@code time} field rewritten to the legacy {@code OffsetDateTime.toString()}
     * rendering of the same instant, ready to be inserted once a subscription is listening. Simulates a document
     * written before the upgrade to a canonical shape (ADR 79), without a subscription seeing the intermediate,
     * correctly-shaped write.
     */
    private Document prepareLegacyTimeShapedEvent(OffsetDateTime instant) {
        CloudEvent event = newEvent(instant);
        mongoEventStore.write(UUID.randomUUID().toString(), List.of(event));

        MongoCollection<Document> collection = eventCollection();
        Document document = requireNonNull(collection.find(Filters.eq("id", event.getId())).first());
        collection.deleteOne(Filters.eq("id", event.getId()));
        document.put("time", instant.toString());
        return document;
    }

    private MongoCollection<Document> eventCollection() {
        return mongoTemplate.getCollection(eventCollectionName);
    }

    private CloudEvent newEvent(OffsetDateTime instant) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("SomeEvent")
                .withTime(instant)
                .withSubject("subject")
                .withDataContentType("application/json")
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
