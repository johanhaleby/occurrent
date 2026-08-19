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
import io.github.artsok.RepeatedIfExceptionsTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.*;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.condition.Condition.lt;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;

@SuppressWarnings("SameParameterValue")
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
public class ReactorMongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
    private static final URI NAME_SOURCE = URI.create("http://name");

    private ReactorMongoEventStore eventStore;

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));
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

    // Carried from U2's adversarial verify: the DCB append() path already has
    // ReactorMongoEventStoreDcbTest#resubscribing_the_same_append_publisher_mints_a_fresh_append_id_each_time; the
    // stream write() path had no equivalent, a coverage asymmetry rather than a known bug (write() was proven
    // correct empirically). ADR 132's AppendId is minted once per write()/append() call and must not be cached in
    // the returned lazy Mono, so resubscribing the same publisher (a mistake this library's own examples make
    // easily, since a Mono is reusable) must mint a fresh id on every execution rather than replaying the first one.
    @Test
    void resubscribing_the_same_write_publisher_mints_a_fresh_append_id_each_time() {
        CloudEvent event = convertDomainEventCloudEvent(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "reused-name"));
        Mono<WriteResult> publisher = eventStore.write("reused-stream", Flux.just(event));

        WriteResult first = requireNonNull(publisher.block());
        eventStore.deleteEvent(event.getId(), event.getSource()).block();
        WriteResult second = publisher.block();

        assertAll(
                () -> assertThat(first.appendId()).isPresent(),
                () -> assertThat(requireNonNull(second).appendId()).isPresent(),
                () -> assertThat(second.appendId())
                        .as("a reused write() publisher must mint a fresh append id on every subscription, not reuse the one from its first execution")
                        .isNotEqualTo(first.appendId())
        );
    }

    @Nested
    @DisplayName("queries")
    class QueriesTest {

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
                persist("name1", Flux.just(nameDefined, nameWasChanged1)).block();
                persist("name2", nameWasChanged2).block();

                // Then
                Flux<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1);
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
}
