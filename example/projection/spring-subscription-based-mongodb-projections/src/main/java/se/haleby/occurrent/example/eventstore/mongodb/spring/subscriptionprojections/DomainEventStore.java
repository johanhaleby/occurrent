package se.haleby.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

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
    private final DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent;

    public DomainEventStore(EventStore eventStore, ObjectMapper objectMapper, DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
        this.deserializeCloudEventToDomainEvent = deserializeCloudEventToDomainEvent;
    }

    public void append(UUID id, List<DomainEvent> events) {
        eventStore.write(id.toString(), serialize(id, events));
    }

    public EventStream<DomainEvent> loadEventStream(UUID id) {
        return eventStore.read(id.toString()).map(deserializeCloudEventToDomainEvent::deserialize);
    }

    private Stream<CloudEvent> serialize(UUID id, List<DomainEvent> events) {
        return events.stream()
                .map(e -> CloudEventBuilder.v1()
                        .withId(id.toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }
}