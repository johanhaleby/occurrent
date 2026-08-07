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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.hamcrest.Matchers.is;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.in;
import static org.occurrent.condition.Condition.ne;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.subscription.internal.ExecutorShutdown.shutdownSafely;

/**
 * A pre-upgrade event can still hold its {@code time} value in the legacy, variable-width shape written by
 * {@code OffsetDateTime.toString()} rather than the canonical fixed-width shape (ADR 79). These tests insert that
 * legacy shape directly into the underlying collection, simulating an event no rewrite has touched, and confirm
 * that a subscription filtering with {@code eq}/{@code in}/{@code ne} under {@code RFC_3339_STRING} still sees it
 * correctly through the change-stream match, the same conversion path {@link org.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling}
 * feeds.
 */
@Testcontainers
@Timeout(15)
class NativeMongoSubscriptionModelLegacyTimeFormatFilterTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true).withReplicaSet();

    private static final URI SOURCE = URI.create("http://legacy-time-format-filter-test");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoCollection<Document> eventCollection;
    private MongoEventStore mongoEventStore;
    private NativeMongoSubscriptionModel subscriptionModel;
    private ExecutorService subscriptionExecutor;

    @BeforeEach
    void create_mongo_event_store_and_subscription_model() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        EventStoreConfig config = new EventStoreConfig(TimeRepresentation.RFC_3339_STRING);
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), config);
        subscriptionExecutor = Executors.newCachedThreadPool();
        subscriptionModel = new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        subscriptionExecutor.shutdown();
        shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
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
        eventCollection.insertOne(legacyDocument);

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
        eventCollection.insertOne(legacyDocument);

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
        eventCollection.insertOne(legacyDocument);
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

        Document document = requireNonNull(eventCollection.find(Filters.eq("id", event.getId())).first());
        eventCollection.deleteOne(Filters.eq("id", event.getId()));
        document.put("time", instant.toString());
        return document;
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
