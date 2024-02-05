package org.occurrent.eventstore.jpa.domain;

import java.util.Date;

public sealed interface DomainEvent permits NameDefined, NameWasChanged {
  String eventId();

  Date timestamp();

  String userId();

  String name();
}
