package org.occurrent.eventstore.jpa.batteries;

import static java.time.ZoneOffset.UTC;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.util.UUID;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.jpa.CloudEventConverter;

public class StreamEventDaoConverter implements CloudEventConverter<CloudEventDao> {
  private final SerializingOperations serializingOperations;

  public StreamEventDaoConverter(ObjectMapper mapper) {
    this.serializingOperations = new SerializingOperations(mapper);
  }

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
        .data(serializingOperations.bytesToJson(e.getData().toBytes()))
        .build();
  }

  @Override
  public CloudEvent toCloudEvent(CloudEventDao e) {
    return CloudEventBuilder.v1()
        .withId(e.eventUuid().toString())
        .withSource(e.source())
        .withType(e.getClass().getSimpleName())
        .withTime(e.timestamp().atOffset(UTC))
        .withExtension(OccurrentCloudEventExtension.STREAM_VERSION, e.streamRevision())
        .withExtension(OccurrentCloudEventExtension.STREAM_ID, e.streamId())
        .withSubject(e.subject()) // Defined or WasChanged
        .withDataContentType(e.dataContentType())
        .withData(serializingOperations.jsonToBytes(e.data()))
        .build();
  }
}
