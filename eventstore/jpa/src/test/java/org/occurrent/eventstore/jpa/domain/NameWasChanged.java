package org.occurrent.eventstore.jpa.domain;

import java.time.LocalDateTime;
import java.util.Date;

public record NameWasChanged(String eventId, Date timestamp, String userId, String name)
    implements DomainEvent {
  public NameWasChanged(String eventId, LocalDateTime timestamp, String userId, String name) {
    this(eventId, TimeConversion.toDate(timestamp), userId, name);
  }
}
