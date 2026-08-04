/*
 * Copyright 2020 Johan Haleby
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.*;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.vavr.API.*;
import static io.vavr.Predicates.is;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.*;
import static org.occurrent.condition.Condition.*;
import static org.occurrent.domain.Composition.chain;
import static org.occurrent.eventstore.api.WriteCondition.streamVersionEq;
import static org.occurrent.filter.Filter.*;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@SuppressWarnings("SameParameterValue")
@Timeout(10)
@Testcontainers
class MongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final URI NAME_SOURCE = URI.create("http://name");
    private MongoEventStore eventStore;
    private MongoClient mongoClient;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
    private ObjectMapper objectMapper;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        mongoClient = MongoClients.create(connectionString);

        eventStore = newMongoEventStore(TimeRepresentation.RFC_3339_STRING);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void mongo_client_is_closed_after_each_test() {
        mongoClient.close();
    }


    @Test
    void can_configure_query_options_for_native_mongo_event_store() {
        // Given
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().timeRepresentation(TimeRepresentation.DATE).queryOptions(query -> query.batchSize(10).noCursorTimeout(true)).build();
        eventStore = newMongoEventStore(eventStoreConfig);

        LocalDateTime now = LocalDateTime.now();
        List<DomainEvent> events = Composition.chain(Name.defineTheName(UUID.randomUUID().toString(), now, "name", "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "name", "John Doe"));

        // When
        persist("name", WriteCondition.streamVersionEq(0), events);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(events.size()),
                () -> assertThat(readEvents).hasSize(2),
                () -> assertThat(readEvents).containsExactlyElementsOf(events)
        );
    }

    @Test
    void read_skew_is_not_allowed_for_native_implementation() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

        persist("name", streamVersionEq(0), nameDefined);
        persist("name", streamVersionEq(1), nameWasChanged1);
        // When
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        persist("name", streamVersionEq(2), nameWasChanged2);

        // Then
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(2),
                () -> assertThat(readEvents).hasSize(2),
                () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Test
    void no_events_are_inserted_when_batch_contains_duplicate_events() {
        LocalDateTime now = LocalDateTime.now();

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name4");

        // When
        Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(0), List.of(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)));

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                () -> assertThat(eventStream.version()).isEqualTo(0),
                () -> assertThat(readEvents).isEmpty()
        );
    }

    @Test
    void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted() {
        LocalDateTime now = LocalDateTime.now();

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name4");

        persist("name", streamVersionEq(0), List.of(nameDefined, nameWasChanged1));

        // When
        Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(2), List.of(nameWasChanged2, nameWasChanged1)));

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class);
        DuplicateCloudEventException duplicateCloudEventException = (DuplicateCloudEventException) throwable;
        assertAll(
                () -> assertThat(duplicateCloudEventException.getId()).isEqualTo(nameWasChanged1.eventId()),
                () -> assertThat(duplicateCloudEventException.getSource()).isEqualTo(NAME_SOURCE),
                () -> assertThat(duplicateCloudEventException.getDetails()).endsWith("Write errors: [BulkWriteError{index=1, code=11000, message='E11000 duplicate key error collection: " + eventsNamespace() + " index: id_1_source_1 dup key: { id: \"" + nameWasChanged1.eventId() + "\", source: \"http://name\" }', details={}}]."),
                () -> assertThat(throwable).hasMessageNotContaining("unknown"),
                () -> assertThat(eventStream.version()).isEqualTo(2),
                () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Test
    void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted_with_manual_unique_index() {
        LocalDateTime now = LocalDateTime.now();
        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName()).getCollection("events");
        String index = collection.createIndex(Indexes.ascending("type"), new IndexOptions().unique(true));

        try {
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged2 = new NameWasChanged(eventId2, now.plusHours(2), "name", "name4");

            // When
            Throwable throwable = catchThrowable(() -> persist("name", List.of(nameWasChanged1, nameWasChanged2)));

            // Then
            assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class);
            DuplicateCloudEventException duplicateCloudEventException = (DuplicateCloudEventException) throwable;
            assertAll(
                    () -> assertThat(duplicateCloudEventException.getId()).isNull(),
                    () -> assertThat(duplicateCloudEventException.getSource()).isNull(),
                    () -> assertThat(duplicateCloudEventException.getDetails()).endsWith("Write errors: [BulkWriteError{index=1, code=11000, message='E11000 duplicate key error collection: " + eventsNamespace() + " index: type_1 dup key: { type: \"NameWasChanged\" }', details={}}]."),
                    () -> assertThat(eventStore.count()).isZero()
            );
        } finally {
            collection.dropIndex(index);
        }
    }

    @Nested
    @DisplayName("Conditionally Write to Mongo Event Store")
    class ConditionallyWriteToMongoEventStore {

        @SuppressWarnings("ConstantConditions")
        @Nested
        @DisplayName("queries")
        class QueriesTest {

            @BeforeEach
            void create_mongo_spring_blocking_event_store() {
                eventStore = newMongoEventStore(TimeRepresentation.RFC_3339_STRING);
            }

            @Nested
            @DisplayName("when time is represented as rfc 3339 string")
            class TimeRepresentedAsRfc3339String {

                @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
                void query_filter_by_time_but_is_using_slow_string_comparision() {
                    // Given
                    LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                    // When
                    persist("name1", List.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1);
                }

            }

            @Nested
            @DisplayName("when time is represented as date")
            class TimeRepresentedAsDate {

                @BeforeEach
                void event_store_is_configured_to_using_date_as_time_representation() {
                    eventStore = newMongoEventStore(TimeRepresentation.DATE);
                }

                @Test
                void query_filter_by_time_lt() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                    // When
                    persist("name1", List.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1);
                }

            }
        }
    }

    private List<DomainEvent> deserialize(Stream<CloudEvent> events) {
        return events
                .map(CloudEvent::getData)
                // @formatter:off
                .map(unchecked(data -> objectMapper.readValue(data.toBytes(), new TypeReference<Map<String, Object>>() {})))
                // @formatter:on
                .map(event -> {
                    Instant instant = Instant.ofEpochMilli((long) event.get("time"));
                    LocalDateTime time = LocalDateTime.ofInstant(instant, UTC);
                    String eventId = (String) event.get("eventId");
                    String name = (String) event.get("name");
                    String userId = (String) event.get("userId");
                    return Match(event.get("type")).of(
                            Case($(is(NameDefined.class.getSimpleName())), e -> new NameDefined(eventId, time, userId, name)),
                            Case($(is(NameWasChanged.class.getSimpleName())), e -> new NameWasChanged(eventId, time, userId, name))
                    );
                })
                .collect(Collectors.toList());

    }

    @SuppressWarnings("unchecked")
    private <T extends DomainEvent> T deserialize(CloudEvent event) {
        return (T) deserialize(Stream.of(event)).get(0);
    }

    private void persist(String eventStreamId, CloudEvent event) {
        eventStore.write(eventStreamId, List.of(event));
    }

    private void persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        persist(eventStreamId, writeCondition, events);
    }

    private void persist(String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
        eventStore.write(eventStreamId, writeCondition, events.stream().map(convertDomainEventToCloudEvent()).collect(Collectors.toList()));
    }

    private void persist(String eventStreamId, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        persist(eventStreamId, events);
    }

    private WriteResult persist(String eventStreamId, List<DomainEvent> events) {
        return eventStore.write(eventStreamId, events.stream().map(convertDomainEventToCloudEvent()).collect(Collectors.toList()));
    }

    private Function<DomainEvent, CloudEvent> convertDomainEventToCloudEvent() {
        return e -> CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(NAME_SOURCE)
                .withType(e.getClass().getSimpleName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.getClass().getSimpleName().substring(4)) // Defined or WasChanged
                .withDataContentType("application/json")
                .withData(serializeEvent(e))
                .build();
    }

    private byte[] serializeEvent(DomainEvent e) {
        try {
            return objectMapper.writeValueAsBytes(new HashMap<String, Object>() {{
                put("type", e.getClass().getSimpleName());
                put("eventId", e.eventId());
                put("name", e.name());
                put("userId", e.userId());
                put("time", e.timestamp().getTime());
            }});
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }

    private static String databaseName() {
        return Objects.requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl()).getDatabase());
    }

    // The database is private to this container, so an error message quoting the namespace cannot be a literal.
    private static String eventsNamespace() {
        return databaseName() + ".events";
    }

    private MongoEventStore newMongoEventStore(TimeRepresentation timeRepresentation) {
        return newMongoEventStore(new EventStoreConfig(timeRepresentation));
    }

    private MongoEventStore newMongoEventStore(EventStoreConfig eventStoreConfig) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        return new MongoEventStore(mongoClient, connectionString.getDatabase(), "events", eventStoreConfig);
    }
}
