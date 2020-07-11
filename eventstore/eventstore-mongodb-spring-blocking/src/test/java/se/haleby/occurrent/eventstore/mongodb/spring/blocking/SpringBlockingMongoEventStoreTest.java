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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.testsupport.mongodb.FlushMongoDBExtension;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.vavr.API.*;
import static io.vavr.Predicates.is;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static se.haleby.occurrent.domain.Composition.chain;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class SpringBlockingMongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.setPortBindings(ports);
    }

    private EventStore eventStore;

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

    @DisplayName("common tests regardless of StreamWriteConsistencyGuarantee")
    @Nested
    class RegardlessOfStreamWriteConsistencyGuarantee {

    }

    @DisplayName("when using StreamWriteConsistencyGuarantee with type None")
    @Nested
    class StreamWriteConsistencyGuaranteeNone {

        @BeforeEach
        void create_mongo_spring_blocking_event_store_with_stream_write_consistency_guarantee_none() {
            eventStore = new SpringBlockingMongoEventStore(mongoTemplate, connectionString.getCollection(), StreamWriteConsistencyGuarantee.none());
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(now, "John Doe");
            persist(eventStore, "name", 0, events);

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
            List<DomainEvent> events = chain(Name.defineName(now, "Hello World"), es -> Name.changeName(es, now, "John Doe"));

            // When
            persist(eventStore, "name", 0, events);

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
            persist(eventStore, "name", 0, nameDefined);
            persist(eventStore, "name", 1, nameWasChanged1);
            persist(eventStore, "name", 2, nameWasChanged2);

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
            persist(eventStore, "name", 0, nameDefined);
            persist(eventStore, "name", 1, nameWasChanged1);
            persist(eventStore, "name", 2, nameWasChanged2);

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name", 1, 1);
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(eventStream.version()).isEqualTo(0),
                    () -> assertThat(readEvents).hasSize(1),
                    () -> assertThat(readEvents).containsExactly(nameWasChanged1)
            );
        }
    }

    @DisplayName("when using StreamWriteConsistencyGuarantee with type transactional")
    @Nested
    class StreamWriteConsistencyGuaranteeTransactional {

        @BeforeEach
        void create_mongo_spring_blocking_event_store_with_stream_write_consistency_guarantee_none() {
            MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
            eventStore = new SpringBlockingMongoEventStore(mongoTemplate, connectionString.getCollection(), StreamWriteConsistencyGuarantee.transactional("event-stream-version", mongoTransactionManager));
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_blocking_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(now, "John Doe");
            persist(eventStore, "name", 0, events);

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
            List<DomainEvent> events = chain(Name.defineName(now, "Hello World"), es -> Name.changeName(es, now, "John Doe"));

            // When
            persist(eventStore, "name", 0, events);

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
            persist(eventStore, "name", 0, nameDefined);
            persist(eventStore, "name", 1, nameWasChanged1);
            persist(eventStore, "name", 2, nameWasChanged2);

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
            persist(eventStore, "name", 0, nameDefined);
            persist(eventStore, "name", 1, nameWasChanged1);
            persist(eventStore, "name", 2, nameWasChanged2);

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
            List<DomainEvent> events = chain(Name.defineName(now, "Hello World"), es -> Name.changeName(es, now, "John Doe"));

            persist(eventStore, "name", 0, events);

            // When
            Throwable throwable = catchThrowable(() -> persist(eventStore, "name", 1, events));

            // Then
            EventStream<CloudEvent> eventStream = eventStore.read("name");
            List<DomainEvent> readEvents = deserialize(eventStream.events());

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(MongoBulkWriteException.class),
                    () -> assertThat(eventStream.version()).isEqualTo(1),
                    () -> assertThat(readEvents).hasSize(2),
                    () -> assertThat(readEvents).containsExactlyElementsOf(events)
            );
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

    private void persist(EventStore eventStore, String eventStreamId, long expectedStreamVersion, DomainEvent event) {
        List<DomainEvent> events = new ArrayList<>();
        events.add(event);
        persist(eventStore, eventStreamId, expectedStreamVersion, events);
    }

    private void persist(EventStore eventStore, String eventStreamId, long expectedStreamVersion, List<DomainEvent> events) {
        eventStore.write(eventStreamId, expectedStreamVersion, events.stream()
                .map(e -> CloudEventBuilder.v1()
                        .withId(e.getEventId())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(serializeEvent(e))
                        .build()
                ));
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

