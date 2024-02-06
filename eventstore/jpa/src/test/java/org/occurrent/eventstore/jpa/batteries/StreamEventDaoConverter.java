package org.occurrent.eventstore.jpa.batteries;

import static java.time.ZoneOffset.UTC;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.occurrent.eventstore.jpa.CloudEventConverter;

@AllArgsConstructor
public class StreamEventDaoConverter implements CloudEventConverter<CloudEventDao> {
  private final ObjectMapper mapper;

  @Override
  public CloudEventDao toDao(long streamVersion, String streamId, CloudEvent e) {
    return CloudEventDao.builder()
        .streamRevision(streamVersion)
        .streamId(streamId)
        .eventUuid(UUID.fromString(e.getId()))
        .source(e.getSource())
        .type(e.toString())
        .timestamp(e.getTime().toInstant())
        .subject(e.getSubject())
        .dataContentType(e.getDataContentType())
        .dataSchema(e.getDataSchema())
        .specVersion(e.getSpecVersion())
        .data(new String(e.getData().toBytes()))
        .build();
  }

  @Override
  public CloudEvent toCloudEvent(CloudEventDao e) {
    return CloudEventBuilder.v1()
        .withId(e.eventUuid().toString())
        .withSource(e.source())
        .withType(e.getClass().getSimpleName())
        .withTime(e.timestamp().atOffset(UTC))
        .withSubject(e.subject()) // Defined or WasChanged
        .withDataContentType(e.dataContentType())
        .withData(e.data().getBytes(StandardCharsets.UTF_8))
        .build();
  }
}
