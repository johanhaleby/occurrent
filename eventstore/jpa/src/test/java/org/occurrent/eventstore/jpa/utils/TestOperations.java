package org.occurrent.eventstore.jpa.utils;

import static io.vavr.API.$;
import static io.vavr.API.*;
import static io.vavr.API.Case;
import static io.vavr.Predicates.is;
import static java.time.ZoneOffset.UTC;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.vavr.API;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.jpa.CloudEventDaoTraits;
import org.occurrent.eventstore.jpa.JPAEventStore;
import org.occurrent.eventstore.jpa.batteries.SerializingOperations;

public abstract class TestOperations<
    T extends CloudEventDaoTraits<?>, E extends JPAEventStore<?, T>> {
  public static URI NAME_SOURCE = URI.create("http://name");

  protected TestDependencies<T, E> dependencies;
  protected E eventStore;
  protected ObjectMapper objectMapper;

  protected List<DomainEvent> deserialize(Stream<CloudEvent> events) {
    return events
        .map(CloudEvent::getData)
        // @formatter:off
        .map(
            unchecked(
                data ->
                    objectMapper.readValue(
                        data.toBytes(), new TypeReference<Map<String, Object>>() {})))
        // @formatter:on
        .map(
            event -> {
              Instant instant = Instant.ofEpochMilli((long) event.get("timestamp"));
              LocalDateTime time = LocalDateTime.ofInstant(instant, UTC);
              String eventId = (String) event.get("eventId");
              String name = (String) event.get("name");
              String userId = (String) event.get("userId");
              return API.Match(event.get("type"))
                  .of(
                      Case(
                          $(is(NameDefined.class.getSimpleName())),
                          e -> new NameDefined(eventId, time, userId, name)),
                      Case(
                          $(is(NameWasChanged.class.getSimpleName())),
                          e -> new NameWasChanged(eventId, time, userId, name)));
            })
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  protected <T extends DomainEvent> T deserialize(CloudEvent event) {
    return (T) deserialize(Stream.of(event)).get(0);
  }

  protected void persist(String eventStreamId, CloudEvent event) {
    eventStore.write(eventStreamId, Stream.of(event));
  }

  protected void persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
    List<DomainEvent> events = new ArrayList<>();
    events.add(event);
    persist(eventStreamId, writeCondition, events);
  }

  protected void persist(
      String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
    persist(eventStreamId, writeCondition, events.stream());
  }

  protected void persist(
      String eventStreamId, WriteCondition writeCondition, Stream<DomainEvent> events) {
    eventStore.write(eventStreamId, writeCondition, events.map(convertDomainEventToCloudEvent()));
  }

  protected void persist(String eventStreamId, DomainEvent event) {
    List<DomainEvent> events = new ArrayList<>();
    events.add(event);
    persist(eventStreamId, events);
  }

  protected void persist(String eventStreamId, List<DomainEvent> events) {
    persist(eventStreamId, events.stream());
  }

  protected WriteResult persist(String eventStreamId, Stream<DomainEvent> events) {
    return eventStore.write(eventStreamId, events.map(convertDomainEventToCloudEvent()));
  }

  protected Function<DomainEvent, CloudEvent> convertDomainEventToCloudEvent() {
    return e ->
        CloudEventBuilder.v1()
            .withId(e.eventId())
            .withSource(NAME_SOURCE)
            .withType(e.getClass().getName())
            .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
            .withSubject(e.name())
            .withData(serializeEvent(e))
            .withType(e.getClass().getSimpleName())
            .build();
  }

  protected byte[] serializeEvent(DomainEvent e) {
    return SerializingOperations.defaultInstance.domainEventToBytes(e);
    //    try {
    //      var objectMapper = new ObjectMapper();
    //      return objectMapper.writeValueAsBytes(
    //          new HashMap<String, Object>() {
    //            {
    //              put("type", e.getClass().getSimpleName());
    //              put("eventId", e.eventId());
    //              put("name", e.name());
    //              put("userId", e.userId());
    //              put("time", e.timestamp());
    //            }
    //          });
    //    } catch (JsonProcessingException jsonProcessingException) {
    //      throw new RuntimeException(jsonProcessingException);
    //    }
  }
}
