package se.haleby.occurrent.example.eventstore.mongodb.spring.transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.springframework.stereotype.Component;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@Component
public class DomainEventStore {

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

    public DomainEventStore(EventStore eventStore, ObjectMapper objectMapper) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
    }

    public void append(UUID id, long expectedVersion, List<DomainEvent> events) {
        eventStore.write(id.toString(), expectedVersion, serialize(id, events));
    }

    public EventStream<DomainEvent> loadEventStream(UUID id) {
        return eventStore.read(id.toString()).map(this::deserialize);
    }

    private Stream<CloudEvent> serialize(UUID id, List<DomainEvent> events) {
        return events.stream()
                .map(e -> CloudEventBuilder.v1()
                        .withId(id.toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }

    private DomainEvent deserialize(CloudEvent cloudEvent) {
        return unchecked((byte[] json) -> objectMapper.readValue(json, DomainEvent.class)).apply(cloudEvent.getData());
    }
}
