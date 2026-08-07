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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.in;
import static org.occurrent.condition.Condition.ne;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;

/**
 * A pre-upgrade event can still hold its {@code time} value in the legacy, variable-width shape written by
 * {@code OffsetDateTime.toString()} rather than the canonical fixed-width shape (ADR 79). These tests write that
 * legacy shape directly into the underlying collection, simulating an event no rewrite has touched, and confirm
 * that {@code eq}/{@code in}/{@code ne} filters under {@code RFC_3339_STRING} still see it correctly.
 */
@Testcontainers
class MongoEventStoreLegacyTimeFormatFilterTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final URI SOURCE = URI.create("http://legacy-time-format-filter-test");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoEventStore eventStore;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        mongoClient = MongoClients.create(connectionString);
        EventStoreConfig config = new EventStoreConfig.Builder().timeRepresentation(RFC_3339_STRING).build();
        eventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), "events", config);
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
        List<String> ids = eventStore.query(time(eq(instant))).map(CloudEvent::getId).collect(Collectors.toList());

        // Then
        assertThat(ids).containsExactly(eventId);
    }

    @Test
    void in_matches_an_event_stored_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        String eventId = persistEventWithLegacyTimeShape(instant);

        // When
        List<String> ids = eventStore.query(time(in(instant, instant.plusDays(1)))).map(CloudEvent::getId).collect(Collectors.toList());

        // Then
        assertThat(ids).containsExactly(eventId);
    }

    @Test
    void ne_excludes_an_event_stored_in_the_legacy_variable_width_time_shape() {
        // Given
        OffsetDateTime instant = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, UTC);
        persistEventWithLegacyTimeShape(instant);

        // When
        Stream<CloudEvent> events = eventStore.query(time(ne(instant)));

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
        eventStore.write("stream", List.of(event));

        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName()).getCollection("events");
        collection.updateOne(Filters.eq("id", eventId), Updates.set("time", instant.toString()));
        return eventId;
    }

    private static String databaseName() {
        return Objects.requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl()).getDatabase());
    }
}
