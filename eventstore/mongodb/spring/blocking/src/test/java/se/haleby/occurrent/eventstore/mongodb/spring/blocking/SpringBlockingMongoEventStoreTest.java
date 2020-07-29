package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;
import se.haleby.occurrent.eventstore.api.DuplicateCloudEventException;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreQueries.SortBy;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.testsupport.mongodb.FlushMongoDBExtension;

import java.net.URI;
import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.vavr.API.*;
import static io.vavr.Predicates.is;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.condition.JRE.JAVA_11;
import static org.junit.jupiter.api.condition.JRE.JAVA_8;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.occurrent;
import static se.haleby.occurrent.domain.Composition.chain;
import static se.haleby.occurrent.eventstore.api.Condition.*;
import static se.haleby.occurrent.eventstore.api.Filter.*;
import static se.haleby.occurrent.eventstore.api.WriteCondition.streamVersion;
import static se.haleby.occurrent.eventstore.api.WriteCondition.streamVersionEq;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@SuppressWarnings("SameParameterValue")
@Testcontainers
public class SpringBlockingMongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;
    private static final URI NAME_SOURCE = URI.create("http://name");

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.setPortBindings(ports);
    }

    private SpringBlockingMongoEventStore eventStore;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;
    private ConnectionString connectionString;
    private MongoClient mongoClient;

    @BeforeEach
    void create_mongo_spring_blocking_event_store() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        objectMapper = new ObjectMapper();
    }

    @DisplayName("when using StreamConsistencyGuarantee with type None")
    @Nested
    class StreamConsistencyGuaranteeNone {

        @BeforeEach
        void create_mongo_spring_blocking_event_store_with_stream_write_consistency_guarantee_none() {
            eventStore = new SpringBlockingMongoEventStore(mongoTemplate, new EventStoreConfig(connectionString.getCollection(), StreamConsistencyGuarantee.none(), TimeRepresentation.RFC_3339_STRING));
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
            persist("name", events);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(1),
                    () -> assertThat(readEvents).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_once_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            // When
            persist("name", events);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_different_occasions_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", nameDefined);
            persist("name", nameWasChanged1);
            persist("name", nameWasChanged2);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(3),
                    () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2)
            );
        }

        @Test
        void can_read_events_with_skip_and_limit_using_mongo_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", nameDefined);
            persist("name", nameWasChanged1);
            persist("name", nameWasChanged2);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name", 1, 1);
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(1),
                    () -> assertThat(readEvents).containsExactly(nameWasChanged1)
            );
        }

        @Test
        void read_skew_does_not_happen_for_blocking_implementation_when_stream_consistency_guarantee_is_none() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");
            persist("name", nameDefined);
            persist("name", nameWasChanged1);

            // When
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            persist("name", nameWasChanged2);

            // Then
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
            );
        }

        @Test
        void any_write_condition_may_be_explicitly_specified_when_stream_consistency_guarantee_is_none() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");

            // When
            persist("name", WriteCondition.anyStreamVersion(), Stream.of(nameDefined, nameWasChanged1));

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
            );
        }

        @Test
        void events_that_are_inserted_before_duplicate_event_in_batch_is_written_when_stream_consistency_guarantee_is_none() {
            LocalDateTime now = LocalDateTime.now();

            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

            // When
            Throwable throwable = catchThrowable(() -> persist("name", Stream.of(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)));

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    // MongoDB inserts all events up until the error but ignores events after the failed events..
                    () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
            );
        }

        @Test
        void events_that_are_inserted_before_the_duplicate_event_in_batch_are_retained_when_stream_consistency_guarantee_is_none() {
            LocalDateTime now = LocalDateTime.now();

            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

            persist("name", Stream.of(nameDefined, nameWasChanged1));

            // When
            Throwable throwable = catchThrowable(() -> persist("name", Stream.of(nameWasChanged2, nameWasChanged1)));

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2)
            );
        }

        @Nested
        @DisplayName("deletion")
        class Delete {

            @Test
            void deleteAllEventsInEventStream_deletes_all_events() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                persist("name", Stream.of(nameDefined, nameWasChanged1));

                // When
                eventStore.deleteAllEventsInEventStream("name");

                // Then
                EventStream<CloudEvent> eventStream = eventStore.read("name");
                List<DomainEvent> readEvents = deserialize(eventStream.events());
                assertAll(
                        () -> assertThat(eventStream.version()).isZero(),
                        () -> assertThat(readEvents).isEmpty(),
                        () -> assertThat(mongoTemplate.count(query(where(STREAM_ID).is("name")), "events")).isZero()
                );
            }

            @Test
            void deleteEventStream_deletes_all_events_in_event_stream_when_stream_consistency_guarantee_is_none() {
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
                        () -> assertThat(mongoTemplate.count(query(where(STREAM_ID).is("name")), "events")).isZero()
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
                        () -> assertThat(eventStream.version()).isZero(),
                        () -> assertThat(readEvents).containsExactly(nameDefined),
                        () -> assertThat(eventStore.exists("name")).isTrue(),
                        () -> assertThat(mongoTemplate.count(query(where(STREAM_ID).is("name")), "events")).isEqualTo(1)
                );
            }
        }

        @Nested
        @DisplayName("exists")
        class ExistsWhenStreamConsistencyGuaranteeIsNone {

            @Test
            void returns_true_when_stream_contains_events() {
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
            void returns_false_when_all_events_have_been_removed_from_stream() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                persist("name", Stream.of(nameDefined, nameWasChanged1));
                eventStore.deleteAllEventsInEventStream("name");

                // When
                boolean exists = eventStore.exists("name");

                // Then
                assertThat(exists).isFalse();
            }

            @Test
            void returns_false_when_no_events_have_been_persisted_to_stream() {
                // When
                boolean exists = eventStore.exists("name");

                // Then
                assertThat(exists).isFalse();
            }
        }
    }

    @DisplayName("when using StreamConsistencyGuarantee with type transactional")
    @Nested
    class StreamConsistencyGuaranteeTransactional {

        @BeforeEach
        void create_mongo_spring_blocking_event_store_with_stream_write_consistency_guarantee_transactional() {
            MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
            eventStore = new SpringBlockingMongoEventStore(mongoTemplate, new EventStoreConfig(connectionString.getCollection(), StreamConsistencyGuarantee.transactional("event-stream-version", mongoTransactionManager), TimeRepresentation.RFC_3339_STRING));
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
            persist("name", streamVersionEq(0), events);

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
        void can_read_and_write_multiple_events_at_once_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            // When
            persist("name", streamVersionEq(0), events);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(1),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_different_occasions_to_mongo_spring_blocking_event_store() {
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
        void can_read_events_with_skip_and_limit_using_mongo_event_store() {
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
        void stream_version_is_not_updated_when_event_insertion_fails() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            persist("name", streamVersionEq(0), events);

            // When
            Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(1), events));

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                    () -> assertThat(eventStream.version()).isEqualTo(1),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactlyElementsOf(events)
            );
        }

        @Test
        void read_skew_is_not_allowed_for_blocking_implementation_when_stream_consistency_guarantee_is_transactional() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");
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
            void no_events_are_inserted_when_batch_contains_duplicate_events_when_stream_consistency_guarantee_is_transactional() {
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
            void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted_when_stream_consistency_guarantee_is_transactional() {
                LocalDateTime now = LocalDateTime.now();

                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

                persist("name", streamVersionEq(0), Stream.of(nameDefined, nameWasChanged1));

                // When
                Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(1), Stream.of(nameWasChanged2, nameWasChanged1)));

                // Then
                EventStream<CloudEvent> eventStream = eventStore.read("name");
                List<DomainEvent> readEvents = deserialize(eventStream.events());

                assertAll(
                        () -> assertThat(throwable).isExactlyInstanceOf(DuplicateCloudEventException.class).hasCauseExactlyInstanceOf(MongoBulkWriteException.class),
                        () -> assertThat(eventStream.version()).isEqualTo(1),
                        () -> assertThat(readEvents).hasSize(2),
                        () -> assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1)
                );
            }
        }

        @Nested
        @DisplayName("deletion")
        class Delete {

            @Test
            void deleteAllEventsInEventStream_deletes_all_events_but_retains_metadata() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                persist("name", Stream.of(nameDefined, nameWasChanged1));

                // When
                eventStore.deleteAllEventsInEventStream("name");

                // Then
                EventStream<CloudEvent> eventStream = eventStore.read("name");
                List<DomainEvent> readEvents = deserialize(eventStream.events());
                assertAll(
                        () -> assertThat(eventStream.version()).isEqualTo(1),
                        () -> assertThat(readEvents).isEmpty(),
                        () -> assertThat(mongoTemplate.count(query(where("_id").is("name")), "event-stream-version")).isNotZero()
                );
            }

            @Test
            void deleteEventStream_deletes_all_events_in_event_stream_when_stream_consistency_guarantee_is_transactional() {
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
                        () -> assertThat(mongoTemplate.count(query(where(STREAM_ID).is("name")), "events")).isZero()
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
                        () -> assertThat(mongoTemplate.count(query(where(STREAM_ID).is("name")), "events")).isEqualTo(1)
                );
            }
        }

        @Nested
        @DisplayName("exists")
        class ExistsWhenStreamConsistencyGuaranteeIsTransactional {

            @Test
            void returns_true_when_stream_exists_and_contains_events() {
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
            void returns_true_when_stream_exists_but_contains_no_events() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
                NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
                persist("name", Stream.of(nameDefined, nameWasChanged1));
                eventStore.deleteAllEventsInEventStream("name");

                // When
                boolean exists = eventStore.exists("name");

                // Then
                assertThat(exists).isTrue();
            }

            @Test
            void returns_false_when_no_events_have_been_persisted_to_stream() {
                // When
                boolean exists = eventStore.exists("name");

                // Then
                assertThat(exists).isFalse();
            }
        }
    }

    @Nested
    @DisplayName("Conditionally Write to Blocking Spring Mongo EventStore")
    class ConditionallyWriteToSpringMongoEventStore {

        LocalDateTime now = LocalDateTime.now();

        @BeforeEach
        void initialize_event_store() {
            MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
            eventStore = new SpringBlockingMongoEventStore(mongoTemplate, new EventStoreConfig(connectionString.getCollection(), StreamConsistencyGuarantee.transactional("event-stream-version", mongoTransactionManager), TimeRepresentation.RFC_3339_STRING));
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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

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
                persist("name", event1);

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(not(eq(1L))), Stream.of(event2)));

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version not to be equal to 1 but was 1.");
            }
        }
    }

    @Nested
    @DisplayName("queries")
    class QueriesTest {

        @BeforeEach
        void create_mongo_spring_blocking_event_store() {
            eventStore = new SpringBlockingMongoEventStore(mongoTemplate, new EventStoreConfig(connectionString.getCollection(), StreamConsistencyGuarantee.none(), TimeRepresentation.RFC_3339_STRING));
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
                    .withTime(LocalDateTime.now().atZone(UTC))
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
                    .withTime(LocalDateTime.now().atZone(UTC))
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
            Stream<CloudEvent> events = eventStore.query(time(lt(ZonedDateTime.of(now.plusHours(2), UTC))).and(id(uuid.toString())));
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
            Stream<CloudEvent> events = eventStore.query(time(ZonedDateTime.of(now.plusHours(2), UTC)).or(source(NAME_SOURCE)));
            assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
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
        void query_filter_by_data_schema() {
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
                    .withTime(LocalDateTime.now().atZone(UTC))
                    .withSubject("subject")
                    .withDataSchema(URI.create("urn:myschema"))
                    .withDataContentType("application/json")
                    .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
                    .withExtension(occurrent("something"))
                    .build();
            persist("something", cloudEvent);

            // Then
            Stream<CloudEvent> events = eventStore.query(dataSchema(URI.create("urn:myschema")));
            assertThat(events).containsExactly(cloudEvent);
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
                    .withTime(OffsetDateTime.from(LocalDateTime.now().atZone(ZoneId.of("Europe/Stockholm"))).toZonedDateTime())
                    .withSubject("subject")
                    .withDataSchema(URI.create("urn:myschema"))
                    .withDataContentType("text/plain")
                    .withData("text".getBytes(UTF_8))
                    .withExtension(occurrent("something"))
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
                Stream<CloudEvent> events = eventStore.all(SortBy.NATURAL_ASC);
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
                Stream<CloudEvent> events = eventStore.all(SortBy.NATURAL_DESC);
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
                Stream<CloudEvent> events = eventStore.all(SortBy.TIME_ASC);
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
                Stream<CloudEvent> events = eventStore.all(SortBy.TIME_DESC);
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged2, nameWasChanged1);
            }
        }

        @Nested
        @DisplayName("when time is represented as rfc 3339 string")
        class TimeRepresentedAsRfc3339String {

            @Test
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
                Stream<CloudEvent> events = eventStore.query(time(lt(ZonedDateTime.of(now.plusHours(2), UTC))));
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
                Stream<CloudEvent> events = eventStore.query(time(and(gte(ZonedDateTime.of(now.plusMinutes(35), UTC)), lte(ZonedDateTime.of(now.plusHours(4), UTC)))));
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
                Stream<CloudEvent> events = eventStore.query(time(and(gte(ZonedDateTime.of(now, UTC)), lte(ZonedDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).isNotEmpty(); // Java 8 seem to return undeterministic results
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
                Stream<CloudEvent> events = eventStore.query(time(and(gte(ZonedDateTime.of(now, UTC)), lte(ZonedDateTime.of(now.plusHours(2), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameDefined, nameWasChanged1); // nameWasChanged2 _should_ be included but it's not due to string comparison instead of date
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
                Stream<CloudEvent> events = eventStore.query(time(and(gt(ZonedDateTime.of(now.plusMinutes(50), UTC)), lt(ZonedDateTime.of(now.plusMinutes(110), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }
        }

        @Nested
        @DisplayName("when time is represented as date")
        class TimeRepresentedAsDate {

            @BeforeEach
            void event_store_is_configured_to_using_date_as_time_representation() {
                eventStore = new SpringBlockingMongoEventStore(mongoTemplate, new EventStoreConfig(connectionString.getCollection(), StreamConsistencyGuarantee.none(), TimeRepresentation.DATE));
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
                Stream<CloudEvent> events = eventStore.query(time(lt(ZonedDateTime.of(now.plusHours(2), UTC))));
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
                Stream<CloudEvent> events = eventStore.query(time(and(gte(ZonedDateTime.of(now.plusMinutes(35), UTC)), lte(ZonedDateTime.of(now.plusHours(4), UTC)))));
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
                Stream<CloudEvent> events = eventStore.query(time(and(gte(ZonedDateTime.of(now, UTC)), lte(ZonedDateTime.of(now.plusHours(2), UTC)))));
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
                Stream<CloudEvent> events = eventStore.query(time(and(gt(ZonedDateTime.of(now.plusMinutes(50), UTC)), lt(ZonedDateTime.of(now.plusMinutes(110), UTC)))));
                assertThat(deserialize(events)).containsExactly(nameWasChanged1);
            }
        }
    }

    private List<DomainEvent> deserialize(Stream<CloudEvent> events) {
        return events
                .map(CloudEvent::getData)
                // @formatter:off
                    .map(unchecked(data -> objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {})))
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

    private void persist(String eventStreamId, CloudEvent event) {
        eventStore.write(eventStreamId, Stream.of(event));
    }

    private void persist(String eventStreamId, DomainEvent event) {
        eventStore.write(eventStreamId, Stream.of(convertDomainEventCloudEvent(event)));
    }

    private void persist(String eventStreamId, List<DomainEvent> events) {
        persist(eventStreamId, events.stream());
    }

    private void persist(String eventStreamId, Stream<DomainEvent> events) {
        eventStore.write(eventStreamId, events.map(this::convertDomainEventCloudEvent));
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
        eventStore.write(eventStreamId, writeCondition, events.map(this::convertDomainEventCloudEvent));
    }

    @NotNull
    private CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
        return CloudEventBuilder.v1()
                .withId(domainEvent.getEventId())
                .withSource(NAME_SOURCE)
                .withType(domainEvent.getClass().getSimpleName())
                .withTime(toLocalDateTime(domainEvent.getTimestamp()).atZone(UTC))
                .withSubject(domainEvent.getClass().getSimpleName().substring(4)) // Defined or WasChanged
                .withDataContentType("application/json")
                .withData(serializeEvent(domainEvent))
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
}

