package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import se.haleby.occurrent.EventStore;
import se.haleby.occurrent.EventStream;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;

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
    FlushEventsInMongoDBExtension flushEventsInMongoDBExtension = new FlushEventsInMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    private ObjectMapper objectMapper;

    @BeforeEach
    void create_mongo_spring_blocking_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoTemplate mongoTemplate = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        eventStore = new SpringBlockingMongoEventStore(mongoTemplate, connectionString.getCollection());
        objectMapper = new ObjectMapper();
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

        assertAll(() -> {
            assertThat(eventStream.version()).isEqualTo(1);
            assertThat(eventStream.events()).hasSize(1);
            assertThat(readEvents).containsExactlyElementsOf(events);
        });
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

        assertAll(() -> {
            assertThat(eventStream.version()).isEqualTo(1);
            assertThat(eventStream.events()).hasSize(2);
            assertThat(readEvents).containsExactlyElementsOf(events);
        });
    }

    @Test
    void can_read_and_write_multiple_events_at_different_occasions_to_mongo_spring_blocking_event_store() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(now.plusHours(2), "name3");

        // When
        persist(eventStore, "name", 0, nameDefined);
        persist(eventStore, "name", 1, nameWasChanged1);
        persist(eventStore, "name", 2, nameWasChanged2);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name");
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(() -> {
            assertThat(eventStream.version()).isEqualTo(3);
            assertThat(eventStream.events()).hasSize(3);
            assertThat(readEvents).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2);
        });
    }

    @Test
    void can_read_events_with_skip_and_limit_using_mongo_event_store() {
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(now, "name");
        NameWasChanged nameWasChanged1 = new NameWasChanged(now.plusHours(1), "name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged(now.plusHours(2), "name3");

        // When
        persist(eventStore, "name", 0, nameDefined);
        persist(eventStore, "name", 1, nameWasChanged1);
        persist(eventStore, "name", 2, nameWasChanged2);

        // Then
        EventStream<CloudEvent> eventStream = eventStore.read("name", 1, 1);
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(() -> {
            assertThat(eventStream.version()).isEqualTo(3);
            assertThat(eventStream.events()).hasSize(1);
            assertThat(readEvents).containsExactly(nameWasChanged1);
        });
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
                    String name = (String) event.get("name");
                    return Match(event.get("type")).of(
                            Case($(is(NameDefined.class.getSimpleName())), e -> new NameDefined(time, name)),
                            Case($(is(NameWasChanged.class.getSimpleName())), e -> new NameWasChanged(time, name))
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
                        .withId(UUID.randomUUID().toString())
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
                put("name", e.getName());
                put("time", e.getTimestamp().getTime());
            }});
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }
}
