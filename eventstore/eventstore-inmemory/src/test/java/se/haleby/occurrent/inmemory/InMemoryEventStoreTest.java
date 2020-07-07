package se.haleby.occurrent.inmemory;

import io.cloudevents.v1.CloudEventBuilder;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import se.haleby.occurrent.EventStore;
import se.haleby.occurrent.EventStream;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;

@ExtendWith(SoftAssertionsExtension.class)
public class InMemoryEventStoreTest {

    @Test
    void read_and_write(SoftAssertions softly) {
        // Given
        InMemoryEventStore inMemoryEventStore = new InMemoryEventStore();
        LocalDateTime now = LocalDateTime.now();

        // When
        List<DomainEvent> events = Name.defineName(now, "John Doe");
        persist(inMemoryEventStore, "name", events);

        // Then
        EventStream<DomainEvent> eventStream = inMemoryEventStore.read("name", DomainEvent.class);
        softly.assertThat(eventStream.version()).isEqualTo(0);
        softly.assertThat(eventStream.events()).hasSize(1);
        softly.assertThat(eventStream.events().collect(Collectors.toList()).get(0).getData()).hasValue(new NameDefined(now, "John Doe"));
    }

    private void persist(EventStore inMemoryEventStore, String eventStreamId, List<DomainEvent> events) {
        inMemoryEventStore.write(eventStreamId, 0, events.stream()
                .map(e -> CloudEventBuilder.<DomainEvent>builder()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(e.getTimestamp().atZone(UTC))
                        .withSubject(e.getName())
                        .withData(e)
                        .build()));
    }
}