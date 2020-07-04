package se.haleby.occurrent;

import com.mongodb.ConnectionString;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static se.haleby.occurrent.domain.Composition.chain;

@SuppressWarnings("rawtypes")
@Testcontainers
class MongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
    private MongoEventStore mongoEventStore;

    @RegisterExtension
    FlushEventsInMongoDBExtension flushEventsInMongoDBExtension = new FlushEventsInMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoEventStore = new MongoEventStore(connectionString);
    }

    @Test
    void can_read_and_write_single_event_to_mongo_event_store() {
        LocalDateTime now = LocalDateTime.now();

        // When
        List<DomainEvent> events = Name.defineName(now, "John Doe");
        persist(mongoEventStore, "name", events);

        // Then
        EventStream<Map> eventStream = mongoEventStore.read("name", Map.class);
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(() -> {
            assertThat(eventStream.version()).isEqualTo(0);
            assertThat(eventStream.events()).hasSize(1);
            assertThat(readEvents).containsExactlyElementsOf(events);
        });
    }

    @Test
    void can_read_and_write_multiple_events_to_mongo_event_store() {
        LocalDateTime now = LocalDateTime.now();
        List<DomainEvent> events = chain(Name.defineName(now, "Hello World"), es -> Name.changeName(es, now, "John Doe"));

        // When
        persist(mongoEventStore, "name", events);

        // Then
        EventStream<Map> eventStream = mongoEventStore.read("name", Map.class);
        List<DomainEvent> readEvents = deserialize(eventStream.events());

        assertAll(() -> {
            assertThat(eventStream.version()).isEqualTo(0);
            assertThat(eventStream.events()).hasSize(2);
            assertThat(readEvents).containsExactlyElementsOf(events);
        });
    }

    private List<DomainEvent> deserialize(Stream<CloudEventImpl<Map>> events) {
        return events
                .map(CloudEventImpl::getData)
                .map(Optional::get)
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

    private void persist(EventStore eventStore, String eventStreamId, List<DomainEvent> events) {
        eventStore.write(eventStreamId, 0, events.stream()
                .map(e -> CloudEventBuilder.<Map<String, Object>>builder()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(e.getTime().atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(serializeEvent(e))
                        .build()
                ));
    }

    private static Map<String, Object> serializeEvent(DomainEvent e) {
        return new HashMap<String, Object>() {{
            put("type", e.getClass().getSimpleName());
            put("name", e.getName());
            put("time", e.getTime().toInstant(UTC).toEpochMilli());
        }};
    }
}