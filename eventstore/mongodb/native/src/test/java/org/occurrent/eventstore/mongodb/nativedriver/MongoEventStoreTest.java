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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.vavr.API.*;
import static io.vavr.Predicates.is;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.condition.JRE.JAVA_11;
import static org.junit.jupiter.api.condition.JRE.JAVA_8;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.*;
import static org.occurrent.condition.Condition.*;
import static org.occurrent.domain.Composition.chain;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.eventstore.api.WriteCondition.streamVersion;
import static org.occurrent.eventstore.api.WriteCondition.streamVersionEq;
import static org.occurrent.filter.Filter.*;
import static org.occurrent.time.TimeConversion.offsetDateTimeFrom;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@SuppressWarnings("SameParameterValue")
@Timeout(10)
@Testcontainers
class MongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.8");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

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
    void adds_stream_id_extension_to_each_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();

        // When
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        persist("name", Stream.of(event1, event2).collect(Collectors.toList()));

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        assertThat(eventStream.events().map(e -> e.getExtension(STREAM_ID))).containsOnly("name");
    }

    @Test
    void adds_stream_version_extension_to_each_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();

        // When
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        persist("name", Stream.of(event1, event2).collect(Collectors.toList()));

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        assertThat(eventStream.events().map(e -> e.getExtension(STREAM_VERSION))).containsExactly(1L, 2L);
    }

    @Test
    void does_not_change_event_store_content_when_writing_an_empty_stream_of_events() {
        // When
        persist("name", Stream.empty());

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");

        assertThat(eventStream.isEmpty()).isTrue();
    }

    @Test
    void can_read_and_write_single_event_to_mongo_event_store() {
        LocalDateTime now = LocalDateTime.now();

        // When
        List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
        persist("name", events);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(1),
                () -> assertThat(readEvents).hasSize(1),
                () -> assertThat(readEvents).containsExactlyElementsOf(events)
        );
    }

    @Test
    void can_read_and_write_multiple_events_at_once_to_mongo_event_store() {
        LocalDateTime now = LocalDateTime.now();
        List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

        // When
        persist("name", events);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(2),
                () -> assertThat(readEvents).hasSize(2),
                () -> assertThat(readEvents).containsExactlyElementsOf(events)
        );
    }

    @Test
    void can_read_and_write_multiple_events_at_different_occasions_to_mongo_event_store() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

        // When
        persist("name", streamVersionEq(0), nameDefined);
        persist("name", streamVersionEq(1), nameWasChanged1);
        persist("name", streamVersionEq(2), nameWasChanged2);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(3),
                () -> assertThat(readEvents).hasSize(3),
                () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2)
        );
    }

    @Test
    void can_read_events_with_skip_and_limit() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

        // When
        persist("name", streamVersionEq(0), nameDefined);
        persist("name", streamVersionEq(1), nameWasChanged1);
        persist("name", streamVersionEq(2), nameWasChanged2);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name", 1, 1);
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(3),
                () -> assertThat(readEvents).hasSize(1),
                () -> assertThat(readEvents).containsExactly(nameWasChanged1)
        );
    }

    @Test
    void read_skew_is_not_allowed_for_native_implementation() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

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

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

        // When
        Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(0), Stream.of(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)));

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

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

        persist("name", streamVersionEq(0), Stream.of(nameDefined, nameWasChanged1));

        // When
        Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(2), Stream.of(nameWasChanged2, nameWasChanged1)));

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class);
        DuplicateCloudEventException duplicateCloudEventException = (DuplicateCloudEventException) throwable;
        assertAll(
                () -> assertThat(duplicateCloudEventException.getId()).isEqualTo(nameWasChanged1.getEventId()),
                () -> assertThat(duplicateCloudEventException.getSource()).isEqualTo(NAME_SOURCE),
                () -> assertThat(duplicateCloudEventException.getDetails()).endsWith("Write errors: [BulkWriteError{index=1, code=11000, message='E11000 duplicate key error collection: test.events index: id_1_source_1 dup key: { id: \"" + nameWasChanged1.getEventId() + "\", source: \"http://name\" }', details={}}]."),
                () -> assertThat(throwable).hasMessageNotContaining("unknown"),
                () -> assertThat(eventStream.version()).isEqualTo(2),
                () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Test
    void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted_with_manual_unique_index() {
        LocalDateTime now = LocalDateTime.now();
        String databaseName = new ConnectionString(mongoDBContainer.getReplicaSetUrl()).getDatabase();
        MongoCollection<Document> collection = mongoClient.getDatabase(Objects.requireNonNull(databaseName)).getCollection("events");
        String index = collection.createIndex(Indexes.ascending("type"), new IndexOptions().unique(true));

        try {
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged2 = new NameWasChanged(eventId2, now.plusHours(2), "name4");

            // When
            Throwable throwable = catchThrowable(() -> persist("name", Stream.of(nameWasChanged1, nameWasChanged2)));

            // Then
            assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class);
            DuplicateCloudEventException duplicateCloudEventException = (DuplicateCloudEventException) throwable;
            assertAll(
                    () -> assertThat(duplicateCloudEventException.getId()).isNull(),
                    () -> assertThat(duplicateCloudEventException.getSource()).isNull(),
                    () -> assertThat(duplicateCloudEventException.getDetails()).endsWith("Write errors: [BulkWriteError{index=1, code=11000, message='E11000 duplicate key error collection: test.events index: type_1 dup key: { type: \"NameWasChanged\" }', details={}}]."),
                    () -> assertThat(eventStore.count()).isZero()
            );
        } finally {
            collection.dropIndex(index);
        }
    }

    @Nested
    @DisplayName("write result")
    class WriteResultTest {

        @Test
        void mongo_event_store_returns_the_new_stream_version_when_at_least_one_event_is_written_to_an_empty_stream() {
            // Given
            LocalDateTime now = LocalDateTime.now();

            // When
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            WriteResult writeResult = persist("name", Stream.of(event1, event2));

            // Then
            assertAll(
                    () -> assertThat(writeResult.getStreamId()).isEqualTo("name"),
                    () -> assertThat(writeResult.getStreamVersion()).isEqualTo(2L)
            );
        }

        @Test
        void mongo_event_store_returns_the_new_stream_version_when_at_least_one_event_is_written_to_an_existing_stream() {
            // Given
            LocalDateTime now = LocalDateTime.now();

            // When
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            DomainEvent event3 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe2");
            persist("name", Stream.of(event1));
            WriteResult writeResult = persist("name", Stream.of(event2, event3));

            // Then
            assertAll(
                    () -> assertThat(writeResult.getStreamId()).isEqualTo("name"),
                    () -> assertThat(writeResult.getStreamVersion()).isEqualTo(3L)
            );
        }

        @Test
        void mongo_event_store_returns_0_as_version_when_no_events_are_written_to_an_empty_stream() {
            // When
            WriteResult writeResult = persist("name", Stream.empty());

            // Then
            assertAll(
                    () -> assertThat(writeResult.getStreamId()).isEqualTo("name"),
                    () -> assertThat(writeResult.getStreamVersion()).isEqualTo(0L)
            );
        }

        @Test
        void mongo_event_store_returns_the_previous_stream_version_when_no_events_are_written_to_an_existing_stream() {
            // Given
            LocalDateTime now = LocalDateTime.now();

            // When
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            persist("name", Stream.of(event1, event2));
            WriteResult writeResult = persist("name", Stream.empty());

            // Then
            assertAll(
                    () -> assertThat(writeResult.getStreamId()).isEqualTo("name"),
                    () -> assertThat(writeResult.getStreamVersion()).isEqualTo(2L)
            );
        }
    }


    @SuppressWarnings("ConstantConditions")
    @Nested
    @DisplayName("deletion")
    class DeletionTest {

        private MongoDatabase database;

        @BeforeEach
        void createMongoDatabase() {
            ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
            database = MongoClients.create(connectionString).getDatabase(connectionString.getDatabase());
        }

        @Test
        void deleteEventStream_deletes_all_events_in_event_stream() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            eventStore.deleteEventStream("name");

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());
            assertAll(
                    () -> assertThat(eventStream.version()).isZero(),
                    () -> assertThat(readEvents).isEmpty(),
                    () -> assertThat(eventStore.exists("name")).isFalse(),
                    () -> assertThat(database.getCollection("events").countDocuments(Filters.eq(STREAM_ID, "name"))).isZero()
            );
        }

        @Test
        void deleteEvent_deletes_only_specific_event_in_event_stream() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            eventStore.deleteEvent(nameWasChanged1.getEventId(), NAME_SOURCE);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());
            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(1),
                    () -> assertThat(readEvents).containsExactly(nameDefined),
                    () -> assertThat(eventStore.exists("name")).isTrue(),
                    () -> assertThat(database.getCollection("events").countDocuments(Filters.eq(STREAM_ID, "name"))).isNotZero()
            );
        }

        @Test
        void delete_deletes_events_according_to_the_filter() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now, "name2");
            persist("name2", nameDefined2);

            // When
            eventStore.delete(streamId("name").and(time(lte(now.atOffset(UTC).plusMinutes(1)))));

            // Then
            List<DomainEvent> stream1 = deserialize(eventStore.read("name").events());
            List<DomainEvent> stream2 = deserialize(eventStore.read("name2").events());
            assertAll(
                    () -> assertThat(stream1).containsExactly(nameWasChanged1),
                    () -> assertThat(stream2).containsExactly(nameDefined2)
            );
        }
    }

    @Nested
    @DisplayName("exists")
    class Exists {

        @Test
        void returns_when_number_of_events_is_greater_than_zero() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            boolean exists = eventStore.exists("name");

            // Then
            assertThat(exists).isTrue();
        }

        @Test
        void returns_false_when_number_of_events_are_zero() {
            // When
            boolean exists = eventStore.exists("name");

            // Then
            assertThat(exists).isFalse();
        }
    }

    @Nested
    @DisplayName("count")
    class CountTest {

        @Test
        void count_without_any_filter_returns_all_the_count_of_all_events_in_the_event_store() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
            persist("name", Stream.of(event1, event2, event3).collect(Collectors.toList()));

            // When
            long count = eventStore.count();

            // Then
            assertThat(count).isEqualTo(3);
        }

        @Test
        void count_with_filter_returns_only_events_that_matches_the_filter() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
            persist("name", Stream.of(event1, event2, event3).collect(Collectors.toList()));

            // When
            long count = eventStore.count(type(NameDefined.class.getSimpleName()));

            // Then
            assertThat(count).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("exists")
    class ExistsTest {

        @Test
        void returns_false_when_there_are_no_events_in_the_event_store_and_filter_is_all() {
            // When
            boolean exists = eventStore.exists(Filter.all());

            // Then
            assertThat(exists).isFalse();
        }

        @Test
        void returns_true_when_there_are_events_in_the_event_store_and_filter_is_all() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
            persist("name", Stream.of(event1, event2, event3).collect(Collectors.toList()));

            // When
            boolean exists = eventStore.exists(Filter.all());

            // Then
            assertThat(exists).isTrue();
        }

        @Test
        void returns_false_when_there_are_no_events_in_the_event_store_and_filter_is_not_all() {
            // When
            boolean exists = eventStore.exists(type(NameDefined.class.getSimpleName()));

            // Then
            assertThat(exists).isFalse();
        }

        @Test
        void returns_true_when_there_are_matching_events_in_the_event_store_and_filter_not_all() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
            DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
            persist("name", Stream.of(event1, event2, event3).collect(Collectors.toList()));

            // When
            boolean exists = eventStore.exists(type(NameDefined.class.getSimpleName()));

            // Then
            assertThat(exists).isTrue();
        }

        @Test
        void returns_false_when_there_events_in_the_event_store_that_doesnt_match_filter() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
            DomainEvent event2 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
            persist("name", Stream.of(event1, event2).collect(Collectors.toList()));

            // When
            boolean exists = eventStore.exists(type(NameWasChanged.class.getSimpleName()));

            // Then
            assertThat(exists).isFalse();
        }
    }


    @Nested
    @DisplayName("update")
    class UpdateTest {

        @Test
        void updates_cloud_event_when_cloud_event_exists() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            eventStore.updateEvent(eventId2, NAME_SOURCE, cloudEvent -> {
                NameWasChanged e = deserialize(cloudEvent);
                NameWasChanged correctedName = new NameWasChanged(e.getEventId(), e.getTimestamp(), "name3");
                return CloudEventBuilder.v1(cloudEvent).withData(serializeEvent(correctedName)).build();
            });

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());
            assertThat(readEvents).containsExactly(nameDefined, new NameWasChanged(eventId2, now.plusHours(1), "name3"));
        }

        @Test
        void returns_updated_cloud_event_when_cloud_event_exists() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            Optional<NameWasChanged> updatedCloudEvent = eventStore.updateEvent(eventId2, NAME_SOURCE, cloudEvent -> {
                NameWasChanged e = deserialize(cloudEvent);
                NameWasChanged correctedName = new NameWasChanged(e.getEventId(), e.getTimestamp(), "name3");
                return CloudEventBuilder.v1(cloudEvent).withData(serializeEvent(correctedName)).build();
            }).map(MongoEventStoreTest.this::deserialize);
            // Then
            assertThat(updatedCloudEvent).contains(new NameWasChanged(eventId2, now.plusHours(1), "name3"));
        }

        @Test
        void returns_empty_optional_when_cloud_event_does_not_exists() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            Optional<CloudEvent> updatedCloudEvent = eventStore.updateEvent(UUID.randomUUID().toString(), NAME_SOURCE, cloudEvent -> {
                NameWasChanged e = deserialize(cloudEvent);
                NameWasChanged correctedName = new NameWasChanged(e.getEventId(), e.getTimestamp(), "name3");
                return CloudEventBuilder.v1(cloudEvent).withData(serializeEvent(correctedName)).build();
            });
            // Then
            assertThat(updatedCloudEvent).isEmpty();
        }
    }

    @Nested
    @DisplayName("Conditionally Write to Mongo Event Store")
    class ConditionallyWriteToMongoEventStore {

        LocalDateTime now = LocalDateTime.now();

        @Nested
        @DisplayName("parallel writes")
        class ParallelWritesToEventStoreReturns {

            @EnabledOnOs(MAC)
            @RepeatedIfExceptionsTest(repeats = 5, suspend = 500)
            void parallel_writes_to_event_store_throws_WriteConditionNotFulfilledException() {
                // Given
                CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
                WriteCondition writeCondition = WriteCondition.streamVersionEq(0);
                AtomicReference<Throwable> exception = new AtomicReference<>();

                // When
                new Thread(() -> {
                    NameDefined event = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                    await(cyclicBarrier);
                    exception.set(catchThrowable(() -> persist("name", writeCondition, event)));
                }).start();

                new Thread(() -> {
                    NameDefined event = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                    await(cyclicBarrier);
                    exception.set(catchThrowable(() -> persist("name", writeCondition, event)));
                }).start();

                // Then
                Awaitility.await().atMost(4, SECONDS).untilAsserted(() -> assertThat(exception).hasValue(new WriteConditionNotFulfilledException("name", 1, writeCondition, "WriteCondition was not fulfilled. Expected version to be equal to 0 but was 1.")));
            }
        }

        @Nested
        @DisplayName("eq")
        class Eq {

            @Test
            void writes_events_when_stream_version_matches_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1);

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersionEq(eventStream1.version()), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_does_not_match_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(10), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be equal to 10 but was 1.");
            }
        }

        @Nested
        @DisplayName("ne")
        class Ne {

            @Test
            void writes_events_when_stream_version_does_not_match_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(ne(20L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_match_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(ne(1L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to not be equal to 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("lt")
        class Lt {

            @Test
            void writes_events_when_stream_version_is_less_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(lt(10L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_greater_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(lt(0L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be less than 0 but was 1.");
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_equal_to_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(lt(1L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be less than 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("gt")
        class Gt {

            @Test
            void writes_events_when_stream_version_is_greater_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(gt(0L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_less_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(gt(100L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than 100 but was 1.");
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_equal_to_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(gt(1L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("lte")
        class Lte {

            @Test
            void writes_events_when_stream_version_is_less_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(lte(10L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }


            @Test
            void writes_events_when_stream_version_is_equal_to_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(lte(1L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_greater_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(lte(0L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be less than or equal to 0 but was 1.");
            }
        }

        @Nested
        @DisplayName("gte")
        class Gte {

            @Test
            void writes_events_when_stream_version_is_greater_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(gte(0L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void writes_events_when_stream_version_is_equal_to_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(gte(0L)), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_less_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(gte(100L)), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 100 but was 1.");
            }
        }

        @Nested
        @DisplayName("and")
        class And {

            @Test
            void writes_events_when_stream_version_is_when_all_conditions_match_and_expression() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(and(gte(0L), lt(100L), ne(40L))), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_any_of_the_operations_in_the_and_expression_is_not_fulfilled() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(and(gte(0L), lt(100L), ne(1L))), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 0 and to be less than 100 and to not be equal to 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("or")
        class Or {

            @Test
            void writes_events_when_stream_version_is_when_any_condition_in_or_expression_matches() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(or(gte(100L), lt(0L), ne(40L))), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_none_of_the_operations_in_the_and_expression_is_fulfilled() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(or(gte(100L), lt(1L))), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 100 or to be less than 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("not")
        class Not {

            @Test
            void writes_events_when_stream_version_is_not_matching_condition() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                EventStream<CloudEvent> eventStream1 = eventStore.read("name");
                persist(eventStream1.id(), streamVersion(not(eq(100L))), Stream.of(event2));

                // Then
                EventStream<CloudEvent> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2.events())).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_condition_is_fulfilled_but_should_not_be_so() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", Stream.of(event1));

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(not(eq(1L))), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version not to be equal to 1 but was 1.");
            }
        }

        @SuppressWarnings("ConstantConditions")
        @Nested
        @DisplayName("queries")
        class QueriesTest {

            @BeforeEach
            void create_mongo_spring_blocking_event_store() {
                eventStore = newMongoEventStore(TimeRepresentation.RFC_3339_STRING);
            }

            @Test
            void all_without_skip_and_limit_returns_all_events() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", nameDefined);
                persist("name2", nameWasChanged1);
                persist("name3", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.all();
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
            }

            @Test
            void all_with_skip_and_limit_returns_all_events_within_skip_and_limit() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", Stream.of(nameWasChanged2));

                // Then
                Stream<CloudEvent> events = eventStore.all(1, 2);
                assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameWasChanged2);
            }

            @Test
            void query_with_single_filter_without_skip_and_limit() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);
                persist("something", CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://something"))
                        .withType("something")
                        .withTime(LocalDateTime.now().atOffset(UTC))
                        .withSubject("subject")
                        .withDataContentType("application/json")
                        .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
                        .build()
                );

                // Then
                Stream<CloudEvent> events = eventStore.query(source(NAME_SOURCE));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
            }

            @Test
            void query_with_single_filter_with_skip_and_limit() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);
                persist("something", CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://something"))
                        .withType("something")
                        .withTime(LocalDateTime.now().atOffset(UTC))
                        .withSubject("subject")
                        .withDataContentType("application/json")
                        .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
                        .build()
                );

                // Then
                Stream<CloudEvent> events = eventStore.query(source(NAME_SOURCE), 1, 1);
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }

            @Test
            void compose_filters_using_and() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                UUID uuid = UUID.randomUUID();
                NameDefined nameDefined = new NameDefined(uuid.toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))).and(id(uuid.toString())));
                assertThat(deserialize(events)).containsExactly(nameDefined);
            }

            @Test
            void compose_filters_using_or() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(time(OffsetDateTime.of(now.plusHours(2), UTC)).or(source(NAME_SOURCE)));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
            }

            @Test
            void query_filter_by_data() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(data("name", eq("name2")));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }

            @Test
            void query_filter_by_subject() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(subject("WasChanged"));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameWasChanged2);
            }

            @Test
            void query_filter_by_cloud_event() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                String eventId = UUID.randomUUID().toString();
                NameWasChanged nameWasChanged1 = new NameWasChanged(eventId, now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(cloudEvent(eventId, NAME_SOURCE));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }

            @Test
            void query_filter_by_type() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(type(NameDefined.class.getSimpleName()));
                assertThat(deserialize(events)).containsExactly(nameDefined);
            }

            @Test
            void query_filter_by_data_schema() throws IOException {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);
                CloudEvent cloudEvent = CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://something"))
                        .withType("something")
                        .withTime(LocalDateTime.now().atOffset(UTC))
                        .withSubject("subject")
                        .withDataSchema(URI.create("urn:myschema"))
                        .withDataContentType("application/json")
                        .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
                        .withExtension(occurrent("something", 1L))
                        .build();
                persist("something", cloudEvent);

                // Then
                Stream<CloudEvent> events = eventStore.query(dataSchema(URI.create("urn:myschema")));
                CloudEvent expectedCloudEvent = CloudEventBuilder.v1(cloudEvent).withData(PojoCloudEventData.wrap(Document.parse(new String(cloudEvent.getData().toBytes(), UTF_8)), document -> document.toJson().getBytes(UTF_8))).build();
                assertThat(events).containsExactly(expectedCloudEvent);
            }

            @Test
            void query_filter_by_data_content_type() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);
                CloudEvent cloudEvent = CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://something"))
                        .withType("something")
                        .withTime(offsetDateTimeFrom(LocalDateTime.now(), ZoneId.of("Europe/Stockholm")))
                        .withSubject("subject")
                        .withDataSchema(URI.create("urn:myschema"))
                        .withDataContentType("text/plain")
                        .withData("text".getBytes(UTF_8))
                        .withExtension(occurrent("something", 1L))
                        .build();
                persist("something", cloudEvent);

                // Then
                Stream<CloudEvent> events = eventStore.query(dataContentType("text/plain"));
                assertThat(events).containsExactly(cloudEvent);
            }

            @Nested
            @DisplayName("sort")
            class SortTest {

                @Test
                void sort_by_natural_asc() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name3", nameWasChanged1);
                    persist("name2", nameWasChanged2);
                    persist("name1", nameDefined);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.natural(ASCENDING));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameWasChanged2, nameDefined);
                }

                @Test
                void sort_by_natural_desc() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name3", nameWasChanged1);
                    persist("name2", nameWasChanged2);
                    persist("name1", nameDefined);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.natural(DESCENDING));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged2, nameWasChanged1);
                }

                @Test
                void sort_by_time_asc() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name3", nameWasChanged1);
                    persist("name2", nameWasChanged2);
                    persist("name1", nameDefined);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.time(ASCENDING));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameDefined, nameWasChanged2);
                }

                @Test
                void sort_by_time_desc() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now.plusHours(3), "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name3", nameWasChanged1);
                    persist("name2", nameWasChanged2);
                    persist("name1", nameDefined);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.time(DESCENDING));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged2, nameWasChanged1);
                }

                @Test
                void sort_by_time_desc_and_natural_descending() {
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now, "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name1", nameDefined);
                    persist("name3", nameWasChanged1);
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.time(DESCENDING).thenNatural(DESCENDING));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged2, nameWasChanged1, nameDefined);
                }

                @Test
                void sort_by_time_desc_and_natural_ascending() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now, "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name", nameDefined);
                    persist("name", nameWasChanged1);
                    persist("name", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.time(DESCENDING).thenNatural(ASCENDING));
                    // Natural ignores indexes!
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
                }

                @Test
                void sort_by_time_desc_and_other_field_descending() {
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now, "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name", nameDefined);
                    persist("name", nameWasChanged1);
                    persist("name", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.time(DESCENDING).then(STREAM_VERSION, DESCENDING));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged2, nameWasChanged1, nameDefined);
                }

                @Test
                void sort_by_time_desc_and_other_field_ascending() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now, "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

                    // When
                    persist("name1", nameDefined);
                    persist("name3", nameWasChanged1);
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.all(SortBy.time(DESCENDING).thenStreamVersion(ASCENDING));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged2, nameDefined, nameWasChanged1);
                }

            }

            @Nested
            @DisplayName("when time is represented as rfc 3339 string")
            class TimeRepresentedAsRfc3339String {

                @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
                void query_filter_by_time_but_is_using_slow_string_comparision() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1);
                }

                @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
                void query_filter_by_time_range_is_wider_than_persisted_time_range() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now.plusMinutes(35), UTC)), lte(OffsetDateTime.of(now.plusHours(4), UTC)))));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameWasChanged2);
                }

                @EnabledOnJre(JAVA_8)
                @Test
                void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range_when_using_java_8() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                    assertThat(deserialize(events)).isNotEmpty(); // Java 8 seem to return nondeterministic results
                }

                @EnabledForJreRange(min = JAVA_11)
                @Test
                void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range_when_using_java_11_and_above() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1); // nameWasChanged2 _should_ be included but it's not due to string comparison instead of date
                }

                @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
                void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gt(OffsetDateTime.of(now.plusMinutes(50), UTC)), lt(OffsetDateTime.of(now.plusMinutes(110), UTC)))));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged1);
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
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1);
                }

                @Test
                void query_filter_by_time_range_is_wider_than_persisted_time_range() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now.plusMinutes(35), UTC)), lte(OffsetDateTime.of(now.plusHours(4), UTC)))));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameWasChanged2);
                }

                @Test
                void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                    assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
                }

                @Test
                void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

                    // When
                    persist("name1", Stream.of(nameDefined, nameWasChanged1));
                    persist("name2", nameWasChanged2);

                    // Then
                    Stream<CloudEvent> events = eventStore.query(time(and(gt(OffsetDateTime.of(now.plusMinutes(50), UTC)), lt(OffsetDateTime.of(now.plusMinutes(110), UTC)))));
                    assertThat(deserialize(events)).containsExactly(nameWasChanged1);
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
                    return Match(event.get("type")).of(
                            Case($(is(NameDefined.class.getSimpleName())), e -> new NameDefined(eventId, time, name)),
                            Case($(is(NameWasChanged.class.getSimpleName())), e -> new NameWasChanged(eventId, time, name))
                    );
                })
                .collect(Collectors.toList());

    }

    @SuppressWarnings("unchecked")
    private <T extends DomainEvent> T deserialize(CloudEvent event) {
        return (T) deserialize(Stream.of(event)).get(0);
    }

    private void persist(String eventStreamId, CloudEvent event) {
        eventStore.write(eventStreamId, Stream.of(event));
    }

    private void persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        persist(eventStreamId, writeCondition, events);
    }

    private void persist(String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
        persist(eventStreamId, writeCondition, events.stream());
    }

    private void persist(String eventStreamId, WriteCondition writeCondition, Stream<DomainEvent> events) {
        eventStore.write(eventStreamId, writeCondition, events.map(convertDomainEventToCloudEvent()));
    }

    private void persist(String eventStreamId, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        persist(eventStreamId, events);
    }

    private void persist(String eventStreamId, List<DomainEvent> events) {
        persist(eventStreamId, events.stream());
    }

    private WriteResult persist(String eventStreamId, Stream<DomainEvent> events) {
        return eventStore.write(eventStreamId, events.map(convertDomainEventToCloudEvent()));
    }

    private Function<DomainEvent, CloudEvent> convertDomainEventToCloudEvent() {
        return e -> CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(NAME_SOURCE)
                .withType(e.getClass().getSimpleName())
                .withTime(toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getClass().getSimpleName().substring(4)) // Defined or WasChanged
                .withDataContentType("application/json")
                .withData(serializeEvent(e))
                .build();
    }

    private byte[] serializeEvent(DomainEvent e) {
        try {
            return objectMapper.writeValueAsBytes(new HashMap<String, Object>() {{
                put("type", e.getClass().getSimpleName());
                put("eventId", e.getEventId());
                put("name", e.getName());
                put("time", e.getTimestamp().getTime());
            }});
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }

    private MongoEventStore newMongoEventStore(TimeRepresentation timeRepresentation) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        return new MongoEventStore(mongoClient, connectionString.getDatabase(), "events", new EventStoreConfig(timeRepresentation));
    }

    private static void await(CyclicBarrier cyclicBarrier) {
        try {
            cyclicBarrier.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}