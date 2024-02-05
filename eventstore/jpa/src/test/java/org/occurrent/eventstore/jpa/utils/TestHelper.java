package org.occurrent.eventstore.jpa.utils;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.eventstore.jpa.domain.TimeConversion.toLocalDateTime;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.jpa.domain.DomainEvent;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;

public record TestHelper(EventStore eventStore) {

  public WriteResult persist(String eventStreamId, Stream<DomainEvent> events) {
    return eventStore.write(eventStreamId, events.map(convertDomainEventToCloudEvent()));
  }

  public static final URI NAME_SOURCE = URI.create("http://name");

  public Function<DomainEvent, CloudEvent> convertDomainEventToCloudEvent() {
    return e ->
        CloudEventBuilder.v1()
            .withId(e.eventId())
            .withSource(NAME_SOURCE)
            .withType(e.getClass().getSimpleName())
            .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
            .withSubject(e.getClass().getSimpleName().substring(4)) // Defined or WasChanged
            .withDataContentType("application/json")
            .withData(serializeEvent(e))
            .build();
  }

  public byte[] serializeEvent(DomainEvent e) {
    try {
      var objectMapper =
          new org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper();
      return objectMapper.writeValueAsBytes(
          new HashMap<String, Object>() {
            {
              put("type", e.getClass().getSimpleName());
              put("eventId", e.eventId());
              put("name", e.name());
              put("userId", e.userId());
              put("time", e.timestamp().getTime());
            }
          });
    } catch (JsonProcessingException jsonProcessingException) {
      throw new RuntimeException(jsonProcessingException);
    }
  }
}
