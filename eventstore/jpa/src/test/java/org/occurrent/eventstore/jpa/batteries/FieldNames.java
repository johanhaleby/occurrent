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
  STREAM_ID("streamid", "streamId"),
  CLOUD_EVENT_ID("id", "eventId"),
  SUBJECT("subject", "subject"),
  CLOUD_EVENT_SOURCE("source", "source"),
  TIMESTAMP("time", "timestamp"),
  TYPE("type", "type"),
  SPEC_VERSION("specversion", "specVersion"),
;

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
