package se.haleby.occurrent.eventstore.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@ExtendWith(SoftAssertionsExtension.class)
public class InMemoryEventStoreTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void create_object_mapper() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void read_and_write(SoftAssertions softly) {
        // Given
        InMemoryEventStore inMemoryEventStore = new InMemoryEventStore();
        String eventId = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();

        // When
        List<DomainEvent> events = Name.defineName(eventId, now, "John Doe");
        persist(inMemoryEventStore, "name", events);

        // Then
        EventStream<NameDefined> eventStream = inMemoryEventStore.read("name").map(unchecked(cloudEvent -> objectMapper.readValue(cloudEvent.getData(), NameDefined.class)));
        softly.assertThat(eventStream.version()).isEqualTo(0);
        softly.assertThat(eventStream.events()).hasSize(1);
        softly.assertThat(eventStream.events().collect(Collectors.toList())).containsExactly(new NameDefined(eventId, now, "John Doe"));
    }

    @Test
    void adds_stream_id_extension_to_each_event() {
        // Given
        InMemoryEventStore inMemoryEventStore = new InMemoryEventStore();
        LocalDateTime now = LocalDateTime.now();

        // When
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        persist(inMemoryEventStore, "name", Stream.of(event1, event2).collect(Collectors.toList()));

        // Then
        EventStream<CloudEvent> eventStream = inMemoryEventStore.read("name");
        assertThat(eventStream.events().map(e -> e.getExtension(STREAM_ID))).containsOnly("name");
    }

    private void persist(EventStore inMemoryEventStore, String eventStreamId, List<DomainEvent> events) {
        inMemoryEventStore.write(eventStreamId, 0, events.stream()
                .map(e -> CloudEventBuilder.v1()
                        .withId(e.getEventId())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build()));
    }
}