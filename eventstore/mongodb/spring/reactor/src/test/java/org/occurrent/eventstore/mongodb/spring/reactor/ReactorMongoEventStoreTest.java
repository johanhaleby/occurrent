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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
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
import org.occurrent.condition.Condition;
import org.occurrent.domain.*;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.time.TimeConversion;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.condition.JRE.JAVA_11;
import static org.junit.jupiter.api.condition.JRE.JAVA_8;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.occurrent.condition.Condition.*;
import static org.occurrent.filter.Filter.*;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.time.TimeConversion.offsetDateTimeFrom;

@SuppressWarnings("SameParameterValue")
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
public class ReactorMongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;
    private static final URI NAME_SOURCE = URI.create("http://name");

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true);
        mongoDBContainer.setPortBindings(ports);
    }

    private ReactorMongoEventStore eventStore;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    private ObjectMapper objectMapper;
    private ReactiveMongoTemplate mongoTemplate;
    private ConnectionString connectionString;
    private MongoClient mongoClient;
    private ReactiveMongoTransactionManager reactiveMongoTransactionManager;

    @BeforeEach
    void create_mongo_spring_reactive_event_store() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        objectMapper = new ObjectMapper();
        reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(reactiveMongoTransactionManager).timeRepresentation(RFC_3339_STRING).build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Test
    void can_configure_query_options_for_spring_blocking_event_store() {
        // Given
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.DATE)
                .queryOptions(query -> query.noCursorTimeout().allowSecondaryReads()).build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, eventStoreConfig);

        LocalDateTime now = LocalDateTime.now();
        List<DomainEvent> events = Composition.chain(Name.defineTheName(UUID.randomUUID().toString(), now, "name", "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "name", "John Doe"));

        // When
        persist("name", WriteCondition.streamVersionEq(0), events).block();

        // Then
        Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
        VersionAndEvents versionAndEvents = deserialize(eventStream);

        assertAll(
                () -> assertThat(versionAndEvents.version).isEqualTo(events.size()),
                () -> assertThat(versionAndEvents.events).hasSize(2),
                () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
        );
    }

    @Test
    void read_skew_is_avoided_and_transaction_is_started() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

        persist("name", WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1)).block();

        TransactionalOperator transactionalOperator = TransactionalOperator.create(reactiveMongoTransactionManager);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        AtomicReference<VersionAndEvents> versionAndEventsRef = new AtomicReference<>();

        // When
        transactionalOperator.execute(__ -> eventStore.read("name")
                        .flatMap(es -> es.events().collectList().map(eventList -> {
                            await(countDownLatch);
                            return new VersionAndEvents(es.version(), eventList.stream().map(deserialize()).collect(Collectors.toList()));
                        }))
                        .doOnNext(versionAndEventsRef::set))
                .subscribe();

        transactionalOperator.execute(__ -> persist("name", WriteCondition.streamVersionEq(2), nameWasChanged2)
                        .then(Mono.fromRunnable(countDownLatch::countDown)).then())
                .blockFirst();

        // Then
        VersionAndEvents versionAndEvents = Awaitility.await().untilAtomic(versionAndEventsRef, not(nullValue()));

        assertAll(
                () -> assertThat(versionAndEvents.version).describedAs("version").isEqualTo(2L),
                () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Test
    void read_skew_is_avoided_and_skip_and_limit_is_defined_even_when_no_transaction_is_started() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

        persist("name", WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1)).block();

        // When
        VersionAndEvents versionAndEvents =
                eventStore.read("name", 0, 2)
                        .flatMap(es -> persist("name", WriteCondition.streamVersionEq(2), nameWasChanged2)
                                .then(es.events().collectList())
                                .map(eventList -> new VersionAndEvents(es.version(), eventList.stream().map(deserialize()).collect(Collectors.toList()))))
                        .block();
        // Then
        assert versionAndEvents != null;
        assertAll(
                () -> assertThat(versionAndEvents.version).describedAs("version").isEqualTo(2L),
                () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Test
    void no_events_are_inserted_when_batch_contains_duplicate_events() {
        LocalDateTime now = LocalDateTime.now();

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name4");

        // When
        Throwable throwable = catchThrowable(() -> persist("name", WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)).block());

        // Then
        Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
        VersionAndEvents versionAndEvents = deserialize(eventStream);

        assertAll(
                () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                () -> assertThat(versionAndEvents.version).isEqualTo(0),
                () -> assertThat(versionAndEvents.events).isEmpty()
        );
    }

    @Test
    void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted() {
        LocalDateTime now = LocalDateTime.now();

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name4");

        persist("name", WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1)).block();

        // When
        Throwable throwable = catchThrowable(() -> persist("name", WriteCondition.streamVersionEq(2), Flux.just(nameWasChanged2, nameWasChanged1)).block());

        // Then
        Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
        VersionAndEvents versionAndEvents = deserialize(eventStream);

        assertAll(
                () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                () -> assertThat(versionAndEvents.version).isEqualTo(2),
                () -> assertThat(versionAndEvents.events).hasSize(2),
                () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1)
        );
    }

    @Nested
    @DisplayName("update when stream consistency guarantee is transactional")
    class Update {

        @Test
        void throw_iae_when_update_function_returns_null() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            String eventId2 = UUID.randomUUID().toString();
            NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name", "name2");
            persist("name", Flux.just(nameDefined, nameWasChanged1)).block();

            // When
            Throwable throwable = catchThrowable(() -> eventStore.updateEvent(eventId2, NAME_SOURCE, cloudEvent -> null).block());

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
            persist("name", Flux.just(nameDefined, nameWasChanged1)).block();

            // When
            eventStore.updateEvent(eventId2, NAME_SOURCE, Function.identity()).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);
            assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1);
        }
    }

    @Nested
    @DisplayName("Conditionally Write to Mongo Event Store")
    class ConditionallyWriteToSpringMongoEventStore {

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
                    NameDefined event = new NameDefined(UUID.randomUUID().toString(), now, "name", "John Doe");
                    await(cyclicBarrier);
                    exception.set(catchThrowable(() -> persist("name", writeCondition, event).block()));
                }).start();

                new Thread(() -> {
                    NameDefined event = new NameDefined(UUID.randomUUID().toString(), now, "name", "John Doe");
                    await(cyclicBarrier);
                    exception.set(catchThrowable(() -> persist("name", writeCondition, event).block()));
                }).start();

                // Then
                Awaitility.await().atMost(4, SECONDS).untilAsserted(() -> assertThat(exception).hasValue(new WriteConditionNotFulfilledException("name", 1, writeCondition, "WriteCondition was not fulfilled. Expected version to be equal to 0 but was 1.")));
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
            persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
            persist("name2", nameWasChanged2).block();

            // Then
            Flux<CloudEvent> events = eventStore.query(data("name", eq("name2")).or(data("name", eq("name"))));
            assertThat(deserialize(events)).containsOnly(nameWasChanged1, nameDefined);
        }

        @Test
        void query_filter_by_data_schema() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

            // When
            persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
            persist("name2", nameWasChanged2).block();
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
            persist("something", cloudEvent).block();

            // Then
            Flux<CloudEvent> events = eventStore.query(dataSchema(URI.create("urn:myschema")));
            // Stream position is on by default, so the store stamps a global position (the fourth event written here).
            CloudEvent expectedCloudEvent = OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1(cloudEvent).withData(PojoCloudEventData.wrap(Document.parse(new String(requireNonNull(cloudEvent.getData()).toBytes(), UTF_8)), document -> document.toJson().getBytes(UTF_8))).build(), 4);
            assertThat(events.toStream()).containsExactly(expectedCloudEvent);
        }

        @Test
        void query_filter_by_data_content_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

            // When
            persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
            persist("name2", nameWasChanged2).block();
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
            persist("something", cloudEvent).block();

            // Then
            Flux<CloudEvent> events = eventStore.query(dataContentType("text/plain"));
            // Stream position is on by default, so the store stamps a global position (the fourth event written here).
            assertThat(events.toStream()).containsExactly(OccurrentCloudEventExtension.withPosition(cloudEvent, 4));
        }

        @Nested
        @DisplayName("when time is represented as rfc 3339 string")
        class TimeRepresentedAsRfc3339String {

            @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
            void query_filter_by_time_but_is_using_slow_string_comparison() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
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
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now.plusMinutes(35), UTC)), lte(OffsetDateTime.of(now.plusHours(4), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1, nameWasChanged2);
            }

            @EnabledOnJre(JAVA_8)
            @Test
            void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range_when_using_java_8() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).isNotEmpty(); // Java 8 seem to return nondeterministic results
            }

            @EnabledForJreRange(min = JAVA_11)
            @Test
            void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range_when_using_java_11_and_above() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1); // nameWasChanged2 _should_ be included but it's not due to string comparison instead of date
            }

            @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
            void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gt(OffsetDateTime.of(now.plusMinutes(50), UTC)), lt(OffsetDateTime.of(now.plusMinutes(110), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }
        }

        @Nested
        @DisplayName("when time is represented as date")
        class TimeRepresentedAsDate {

            @BeforeEach
            void event_store_is_configured_to_using_date_as_time_representation() {
                eventStore = new ReactorMongoEventStore(mongoTemplate, new EventStoreConfig(connectionString.getCollection(), TransactionalOperator.create(reactiveMongoTransactionManager), TimeRepresentation.DATE));
            }

            @Test
            void query_filter_by_time_lt() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
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
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now.plusMinutes(35), UTC)), lte(OffsetDateTime.of(now.plusHours(4), UTC)))));
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
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
            }

            @Test
            void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name", "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name", "name3");

                // When
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(and(gt(OffsetDateTime.of(now.plusMinutes(50), UTC)), lt(OffsetDateTime.of(now.plusMinutes(110), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }
        }
    }

    private VersionAndEvents deserialize(Mono<EventStream<CloudEvent>> eventStreamMono) {
        return eventStreamMono
                .publishOn(Schedulers.boundedElastic())
                .map(es -> {
                    List<DomainEvent> events = es.events()
                            .map(deserialize())
                            .toStream()
                            .collect(Collectors.toList());
                    return new VersionAndEvents(es.version(), events);
                })
                .block();
    }

    private List<DomainEvent> deserialize(Flux<CloudEvent> flux) {
        return flux.map(deserialize()).toStream().collect(Collectors.toList());
    }

    private Function<CloudEvent, DomainEvent> deserialize() {
        return CheckedFunction.unchecked(this::deserialize);
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private <T extends DomainEvent> T deserialize(CloudEvent cloudEvent) {
        try {
            return (T) objectMapper.readValue(cloudEvent.getData().toBytes(), Class.forName(cloudEvent.getType()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class VersionAndEvents {
        private final long version;
        private final List<DomainEvent> events;

        VersionAndEvents(long version, List<DomainEvent> events) {
            this.version = version;
            this.events = events;
        }

        @Override
        public String toString() {
            return "VersionAndEvents{" +
                    "version=" + version +
                    ", events=" + events +
                    '}';
        }
    }

    private Mono<WriteResult> persist(String eventStreamId, CloudEvent event) {
        return eventStore.write(eventStreamId, Flux.just(event));
    }

    private Mono<WriteResult> persist(String eventStreamId, DomainEvent event) {
        return eventStore.write(eventStreamId, Flux.just(convertDomainEventCloudEvent(event)));
    }

    private Mono<WriteResult> persist(String eventStreamId, Flux<DomainEvent> events) {
        return eventStore.write(eventStreamId, events.map(this::convertDomainEventCloudEvent));
    }

    private Mono<WriteResult> persist(String eventStreamId, List<DomainEvent> events) {
        return persist(eventStreamId, Flux.fromIterable(events));
    }

    private Mono<WriteResult> persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        return persist(eventStreamId, writeCondition, events);
    }

    private Mono<WriteResult> persist(String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
        return persist(eventStreamId, writeCondition, Flux.fromIterable(events));
    }

    private Mono<WriteResult> persist(String eventStreamId, WriteCondition writeCondition, Flux<DomainEvent> events) {
        return eventStore.write(eventStreamId, writeCondition, events.map(this::convertDomainEventCloudEvent));
    }

    private CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
        return CloudEventBuilder.v1()
                .withId(domainEvent.eventId())
                .withSource(NAME_SOURCE)
                .withType(domainEvent.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(domainEvent.timestamp()).atOffset(UTC))
                .withSubject(domainEvent.getClass().getSimpleName().substring(4)) // Defined or WasChanged
                .withDataContentType("application/json")
                .withData(serializeEvent(domainEvent))
                .build();
    }

    private byte[] serializeEvent(DomainEvent domainEvent) {
        return CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(domainEvent);
    }

    private static void await(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
