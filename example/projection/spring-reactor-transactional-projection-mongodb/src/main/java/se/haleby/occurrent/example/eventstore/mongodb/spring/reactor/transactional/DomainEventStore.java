package se.haleby.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.eventstore.api.reactor.EventStore;
import se.haleby.occurrent.eventstore.api.reactor.EventStream;

import java.net.URI;
import java.util.List;
import java.util.UUID;

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

    public Mono<Void> append(UUID id, long expectedVersion, List<DomainEvent> events) {
        return eventStore.write(id.toString(), expectedVersion, serialize(events));
    }

    public Mono<EventStream<DomainEvent>> loadEventStream(UUID id) {
        return eventStore
                .read(id.toString())
                .map(es -> es.map(this::deserialize));
    }

    private Flux<CloudEvent> serialize(List<DomainEvent> events) {
        return Flux.fromIterable(events)
                .map(e -> CloudEventBuilder.v1()
                        .withId(e.getEventId())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }

    private DomainEvent deserialize(CloudEvent cloudEvent) {
        try {
            return (DomainEvent) objectMapper.readValue(cloudEvent.getData(), Class.forName(cloudEvent.getType()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
