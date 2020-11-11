package org.occurrent.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import java.net.URI;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

public class DomainEventConverter {

    private final ObjectMapper objectMapper;

    public DomainEventConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public CloudEvent convertToCloudEvent(DomainEvent e) {
        return CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build();
    }

    @SuppressWarnings("ConstantConditions")
    public DomainEvent convertToDomainEvent(CloudEvent cloudEvent) {
        return unchecked((CloudEvent e) -> (DomainEvent) objectMapper.readValue(e.getData().toBytes(), Class.forName(e.getType()))).apply(cloudEvent);
    }
}