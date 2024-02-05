package org.occurrent.eventstore.jpa.batteries;

import java.util.Arrays;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

@AllArgsConstructor
@Getter
@Accessors(fluent = true)
public enum FieldNames {
  STREAM_ID("StreamId", "streamId"),
  CLOUD_EVENT_ID("CloudEventId", "cloud_event_id"),
  CLOUD_EVENT_SOURCE("CloudEventSource", "cloud_event_source"),
  TIMESTAMP("Timestamp", "timestamp");

  private final String cloudEventValue;
  private final String daoValue;

  public static Optional<FieldNames> fromStringSafe(String s) {
    try {
      return Arrays.stream(FieldNames.values())
          .filter(x -> x.cloudEventValue.equals(s))
          .findFirst();
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
