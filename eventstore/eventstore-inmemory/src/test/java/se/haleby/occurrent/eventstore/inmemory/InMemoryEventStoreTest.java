package se.haleby.occurrent.eventstore.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
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
        LocalDateTime now = LocalDateTime.now();

        // When
        List<DomainEvent> events = Name.defineName(now, "John Doe");
        persist(inMemoryEventStore, "name", events);

        // Then
        EventStream<NameDefined> eventStream = inMemoryEventStore.read("name").map(unchecked(cloudEvent -> objectMapper.readValue(cloudEvent.getData(), NameDefined.class)));
        softly.assertThat(eventStream.version()).isEqualTo(0);
        softly.assertThat(eventStream.events()).hasSize(1);
        softly.assertThat(eventStream.events().collect(Collectors.toList())).containsExactly(new NameDefined(now, "John Doe"));
    }

    private void persist(EventStore inMemoryEventStore, String eventStreamId, List<DomainEvent> events) {
        inMemoryEventStore.write(eventStreamId, 0, events.stream()
                .map(e -> CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build()));
    }
}