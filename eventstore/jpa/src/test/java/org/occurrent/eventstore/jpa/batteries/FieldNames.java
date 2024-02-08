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
  CLOUD_EVENT_ID("id", "eventId"),
  SUBJECT("subject", "subject"),
  CLOUD_EVENT_SOURCE("source", "source"),
  TIMESTAMP("time", "timestamp"),
  TYPE("type", "type"),
  SPEC_VERSION("specversion", "specVersion"),
//  SOURCE("source", "source"),
;
  //  String SPEC_VERSION = "specversion";
  //  String ID = "id";
  //  String TYPE = "type";
  //  String TIME = "time";
  //  String SOURCE = "source";
  //  String SUBJECT = "subject";
  //  String DATA_SCHEMA = "dataschema";
  //  String DATA_CONTENT_TYPE = "datacontenttype";
  //  String DATA = "data";

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
