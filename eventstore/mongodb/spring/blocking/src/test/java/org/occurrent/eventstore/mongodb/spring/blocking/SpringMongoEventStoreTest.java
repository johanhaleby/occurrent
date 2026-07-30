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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.domain.*;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.time.TimeConversion;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
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
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.condition.JRE.JAVA_11;
import static org.junit.jupiter.api.condition.JRE.JAVA_8;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.occurrent.condition.Condition.*;
import static org.occurrent.filter.Filter.*;
import static org.occurrent.time.TimeConversion.offsetDateTimeFrom;

@SuppressWarnings("SameParameterValue")
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
public class SpringMongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;
    private static final URI NAME_SOURCE = URI.create("http://name");

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    private SpringMongoEventStore eventStore;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;
    private ConnectionString connectionString;
    private MongoTransactionManager mongoTransactionManager;

    @BeforeEach
    void create_mongo_spring_blocking_event_store() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        objectMapper = new ObjectMapper();
        mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Test
    void can_configure_query_options_for_spring_blocking_event_store() {
        // Given
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.DATE)
                .queryOptions(query -> query.noCursorTimeout().allowSecondaryReads()).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);

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
    void read_skew_is_not_allowed_for_blocking_implementation() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");
        persist("name", nameDefined);
        persist("name", nameWasChanged1);

        // When
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        persist("name", nameWasChanged2);

        // Then
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(
                () -> assertThat(eventStream.version()).isEqualTo(2),
                () -> assertThat(readEvents).hasSize(2),
                () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Nested
    @DisplayName("and there are duplicate events")
    class DuplicatesTest {

        @Test
        void no_events_are_inserted_when_batch_contains_duplicate_events() {
            LocalDateTime now = LocalDateTime.now();

            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name4");

            // When
            Throwable throwable = catchThrowable(() -> persist("name", WriteCondition.streamVersionEq(0), Stream.of(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)));

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

            persist("name", WriteCondition.streamVersionEq(0), Stream.of(nameDefined, nameWasChanged1));

            // When
            Throwable throwable = catchThrowable(() -> persist("name", WriteCondition.streamVersionEq(2), Stream.of(nameWasChanged2, nameWasChanged1)));

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                    () -> assertThat(eventStream.version()).isEqualTo(2),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
            );
        }
    }

    @Nested
    @DisplayName("update")
    class Update {

        @Test
        void throw_iae_when_update_function_returns_null() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name", "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            Throwable throwable = catchThrowable(() -> eventStore.updateEvent(eventId2, NAME_SOURCE, cloudEvent -> null));

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Cloud event update function is not allowed to return null");
        }

        @Test
        void when_update_function_returns_the_same_argument_then_cloud_event_is_unchanged_in_the_database() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name", "name2");
            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            eventStore.updateEvent(eventId2, NAME_SOURCE, Function.identity());

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());
            assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1);
        }
    }

    @Nested
    @DisplayName("Conditionally Write to Blocking Spring Mongo EventStore")
    class ConditionallyWriteToSpringMongoEventStore {

        LocalDateTime now = LocalDateTime.now();

        @Nested
        @DisplayName("parallel writes")
        class ParallelWritesToEventStoreReturns {

            @EnabledOnOs(MAC)
            @RepeatedIfExceptionsTest(repeats = 5, suspend = 500)
            void parallel_writes_to_event_store_throws_WriteConditionNotFulfilledException_when_write_condition_is_not_any() {
                // Given
                CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
                WriteCondition writeCondition = WriteCondition.streamVersionEq(0);
                AtomicReference<Throwable> exception = new AtomicReference<>();

                // When
                new Thread(() -> {
                    NameDefined event = new NameDefined(UUID.randomUUID().toString(), now, "name", "John Doe");
                    await(cyclicBarrier);
                    exception.set(catchThrowable(() -> persist("name", writeCondition, event)));
                }).start();

                new Thread(() -> {
                    NameDefined event = new NameDefined(UUID.randomUUID().toString(), now, "name", "John Doe");
                    await(cyclicBarrier);
                    exception.set(catchThrowable(() -> persist("name", writeCondition, event)));
                }).start();

                // Then
                Awaitility.await().atMost(4, SECONDS).untilAsserted(() -> assertThat(exception).hasValue(new WriteConditionNotFulfilledException("name", 1, writeCondition, "WriteCondition was not fulfilled. Expected version to be equal to 0 but was 1.")));
            }

            @EnabledOnOs(MAC)
            @RepeatedIfExceptionsTest(repeats = 5, suspend = 200, minSuccess = 5)
            void parallel_writes_to_event_store_does_not_throw_WriteConditionNotFulfilledException_when_write_condition_is_any() {
                // Given
                CyclicBarrier cyclicBarrier = new CyclicBarrier(3);
                WriteCondition writeCondition = WriteCondition.anyStreamVersion();
                AtomicReference<Throwable> exception = new AtomicReference<>();

                // When
                new Thread(() -> {
                    NameWasChanged event = new NameWasChanged(UUID.randomUUID().toString(), now, "name", "Ikk Doe");
                    try {
                        await(cyclicBarrier);
                        persist("name", writeCondition, event);
                    } catch (Exception e) {
                        exception.set(e);
                    }
                }).start();

                new Thread(() -> {
                    NameWasChanged event = new NameWasChanged(UUID.randomUUID().toString(), now, "name", "Ikkster Doe");
                    try {
                        await(cyclicBarrier);
                        persist("name", writeCondition, event);
                    } catch (Exception e) {
                        exception.set(e);
                    }
                }).start();

                new Thread(() -> {
                    NameWasChanged event = new NameWasChanged(UUID.randomUUID().toString(), now, "name", "Ikkster Doe2");
                    try {
                        await(cyclicBarrier);
                        persist("name", writeCondition, event);
                    } catch (Exception e) {
                        exception.set(e);
                    }
                }).start();

                // Then
                Awaitility.await().atMost(4, SECONDS).untilAsserted(() -> {
                    EventStream<CloudEvent> eventStream = eventStore.read("name");
                    assertThat(deserialize(eventStream.events()))
                            .extracting(it -> (NameWasChanged) it)
                            .extracting(NameWasChanged::name)
                            .contains("Ikk Doe", "Ikkster Doe", "Ikkster Doe2");
                });
                assertThat(exception.get()).isNull();
            }
        }

    }

    @Nested
    @DisplayName("queries")
    class QueriesTest {

        @Test
        void query_filter_by_data() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

            // When
            persist("name1", Stream.of(nameDefined, nameWasChanged1));
            persist("name2", nameWasChanged2);

            // Then
            Stream<CloudEvent> events = eventStore.query(data("name", eq("name3")));
            assertThat(deserialize(events)).containsExactly(nameWasChanged2);
        }

        @SuppressWarnings("ConstantConditions")
        @Test
        void query_filter_by_data_schema() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                    .withExtension(OccurrentCloudEventExtension.occurrent("something", 1))
                    .build();
            persist("something", cloudEvent);

            // Then
            Stream<CloudEvent> events = eventStore.query(dataSchema(URI.create("urn:myschema")));
            // Stream position is on by default, so the store stamps a global position (the fourth event written here).
            CloudEvent expectedCloudEvent = OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1(cloudEvent).withData(PojoCloudEventData.wrap(Document.parse(new String(cloudEvent.getData().toBytes(), UTF_8)), document -> document.toJson().getBytes(UTF_8))).build(), 4);
            assertThat(events).containsExactly(expectedCloudEvent);
        }

        @Test
        void query_filter_by_data_content_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                    .withExtension(OccurrentCloudEventExtension.occurrent("something", 1))
                    .build();
            persist("something", cloudEvent);

            // Then
            Stream<CloudEvent> events = eventStore.query(dataContentType("text/plain"));
            // Stream position is on by default, so the store stamps a global position (the fourth event written here).
            assertThat(events).containsExactly(OccurrentCloudEventExtension.withPosition(cloudEvent, 4));
        }

        @Nested
        @DisplayName("when time is represented as rfc 3339 string")
        class TimeRepresentedAsRfc3339String {

            @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
            void query_filter_by_time_but_is_using_slow_string_comparison() {
                // Given
                LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).isNotEmpty(); // Java 8 seem to return undeterministic results
            }

            @EnabledForJreRange(min = JAVA_11)
            @Test
            void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range_when_using_java_11_and_above() {
                // Given
                LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
            }

            @Disabled
            @Test
            void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range_when_rfc_3339() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.DATE).build();
                eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
            }

            @Test
            void query_filter_by_time_lt() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

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
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
            }

            @Test
            void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range_when_date() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Stream.of(nameDefined, nameWasChanged1));
                persist("name2", nameWasChanged2);

                // Then
                Stream<CloudEvent> events = eventStore.query(time(and(gt(OffsetDateTime.of(now.plusMinutes(50), UTC)), lt(OffsetDateTime.of(now.plusMinutes(110), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }
        }
    }

    private List<DomainEvent> deserialize(Stream<CloudEvent> events) {
        return events
                .map(CloudEvent::getData)
                // @formatter:off
                .map(CheckedFunction.unchecked(data -> objectMapper.readValue(data.toBytes(), new TypeReference<Map<String, Object>>() {
                })))
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

    private WriteResult persist(String eventStreamId, CloudEvent event) {
        return eventStore.write(eventStreamId, List.of(event));
    }

    private WriteResult persist(String eventStreamId, DomainEvent event) {
        return eventStore.write(eventStreamId, List.of(convertDomainEventCloudEvent(event)));
    }

    private WriteResult persist(String eventStreamId, List<DomainEvent> events) {
        return persist(eventStreamId, events.stream());
    }

    private WriteResult persist(String eventStreamId, Stream<DomainEvent> events) {
        return eventStore.write(eventStreamId, events.map(this::convertDomainEventCloudEvent).collect(Collectors.toList()));
    }

    private WriteResult persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        return persist(eventStreamId, writeCondition, events);
    }

    private WriteResult persist(String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
        return persist(eventStreamId, writeCondition, events.stream());
    }

    private WriteResult persist(String eventStreamId, WriteCondition writeCondition, Stream<DomainEvent> events) {
        return eventStore.write(eventStreamId, writeCondition, events.map(this::convertDomainEventCloudEvent).collect(Collectors.toList()));
    }

    private CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
        return CloudEventBuilder.v1()
                .withId(domainEvent.eventId())
                .withSource(NAME_SOURCE)
                .withType(domainEvent.getClass().getSimpleName())
                .withTime(TimeConversion.toLocalDateTime(domainEvent.timestamp()).atOffset(UTC))
                .withSubject(domainEvent.getClass().getSimpleName().substring(4)) // Defined or WasChanged
                .withDataContentType("application/json")
                .withData(serializeEvent(domainEvent))
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

    private static void await(CyclicBarrier cyclicBarrier) {
        try {
            cyclicBarrier.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
