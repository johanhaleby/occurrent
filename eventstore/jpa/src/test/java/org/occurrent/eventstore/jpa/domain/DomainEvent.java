package org.occurrent.eventstore.jpa.domain;

import java.time.Instant;
import java.util.Date;

public sealed interface DomainEvent permits NameDefined, NameWasChanged {
  String eventId();

  Instant timestamp();

  String userId();

  String name();
}
