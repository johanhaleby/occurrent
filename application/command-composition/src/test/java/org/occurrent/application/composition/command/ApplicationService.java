package org.occurrent.application.composition.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Disabled;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.time.TimeConversion;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.vavr.API.*;
import static io.vavr.Predicates.is;
import static java.time.ZoneOffset.UTC;

@Disabled
class ApplicationService {
    private static final URI NAME_SOURCE = URI.create("http://name");

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

    public ApplicationService(EventStore eventStore, ObjectMapper objectMapper) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
    }

    public void executeStreamCommand(String streamId, Function<Stream<DomainEvent>, Stream<DomainEvent>> functionThatCallsDomainModel) {
        EventStream<CloudEvent> eventStream = eventStore.read(streamId);
        Stream<DomainEvent> events = eventStream.events().map(this::convertCloudEventToDomainEvent);
        Stream<DomainEvent> newEvents = functionThatCallsDomainModel.apply(events);
        eventStore.write(streamId, eventStream.version(), newEvents.map(this::convertDomainEventCloudEvent));
    }

    public void executeListCommand(String streamId, Function<List<DomainEvent>, List<DomainEvent>> functionThatCallsDomainModel) {
        executeStreamCommand(streamId, CommandConversion.toStreamCommand(functionThatCallsDomainModel));
    }

    public DomainEvent convertCloudEventToDomainEvent(CloudEvent cloudEvent) {
        Map<String, Object> event = CheckedFunction.unchecked((byte[] data) -> objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {})).apply(cloudEvent.getData());


        Instant instant = Instant.ofEpochMilli((long) event.get("time"));
        LocalDateTime time = LocalDateTime.ofInstant(instant, UTC);
        String eventId = (String) event.get("eventId");
        String name = (String) event.get("name");
        return Match(event.get("type")).of(
                Case($(is(NameDefined.class.getSimpleName())), e -> new NameDefined(eventId, time, name)),
                Case($(is(NameWasChanged.class.getSimpleName())), e -> new NameWasChanged(eventId, time, name))
        );
    }

    private CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
        return CloudEventBuilder.v1()
                .withId(domainEvent.getEventId())
                .withSource(NAME_SOURCE)
                .withType(domainEvent.getClass().getSimpleName())
                .withTime(TimeConversion.toLocalDateTime(domainEvent.getTimestamp()).atOffset(UTC))
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
