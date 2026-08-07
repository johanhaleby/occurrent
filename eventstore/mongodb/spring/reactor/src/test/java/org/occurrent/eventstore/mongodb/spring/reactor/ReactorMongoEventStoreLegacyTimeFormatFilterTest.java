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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.in;
import static org.occurrent.condition.Condition.ne;
import static org.occurrent.filter.Filter.time;

/**
 * A pre-upgrade event can still hold its {@code time} value in the legacy, variable-width shape written by
 * {@code OffsetDateTime.toString()} rather than the canonical fixed-width shape (ADR 79). These tests write that
 * legacy shape directly into the underlying collection, simulating an event no rewrite has touched, and confirm
 * that {@code eq}/{@code in}/{@code ne} filters under {@code RFC_3339_STRING} still see it correctly.
 */
@Testcontainers
class ReactorMongoEventStoreLegacyTimeFormatFilterTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final URI SOURCE = URI.create("http://legacy-time-format-filter-test");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTemplate mongoTemplate;
    private ConnectionString connectionString;
    private ReactorMongoEventStore eventStore;

    @BeforeEach
    void create_reactor_mongo_event_store() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, database);
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
    }

    @AfterEach
    void mongo_client_is_closed_after_each_test() {
        mongoClient.close();
    }

    @Test
    void eq_matches_an_event_stored_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        String eventId = persistEventWithLegacyTimeShape(instant);

        // When
        List<String> ids = eventStore.query(time(eq(instant))).map(CloudEvent::getId).collectList().block();

        // Then
        assertThat(ids).containsExactly(eventId);
    }

    @Test
    void in_matches_an_event_stored_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        String eventId = persistEventWithLegacyTimeShape(instant);

        // When
        List<String> ids = eventStore.query(time(in(instant, instant.plusDays(1)))).map(CloudEvent::getId).collectList().block();

        // Then
        assertThat(ids).containsExactly(eventId);
    }

    @Test
    void ne_excludes_an_event_stored_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        persistEventWithLegacyTimeShape(instant);

        // When
        List<CloudEvent> events = eventStore.query(time(ne(instant))).collectList().block();

        // Then
        assertThat(events).isEmpty();
    }

    /**
     * Writes an event normally, which stores the canonical shape, then overwrites its {@code time} field directly
     * in the underlying collection with the legacy {@code OffsetDateTime.toString()} rendering of the same instant,
     * simulating a document written before the upgrade to a canonical shape (ADR 79).
     */
    private String persistEventWithLegacyTimeShape(OffsetDateTime instant) {
        String eventId = UUID.randomUUID().toString();
        CloudEvent event = CloudEventBuilder.v1()
                .withId(eventId)
                .withSource(SOURCE)
                .withType("SomeEvent")
                .withTime(instant)
                .withSubject("subject")
                .withDataContentType("application/json")
                .withData("{}".getBytes(UTF_8))
                .build();
        eventStore.write("stream", Flux.just(event)).block();

        mongoTemplate.getCollection(connectionString.getCollection())
                .flatMap(collection -> Mono.from(collection.updateOne(Filters.eq("id", eventId), Updates.set("time", instant.toString()))))
                .block();
        return eventId;
    }
}
