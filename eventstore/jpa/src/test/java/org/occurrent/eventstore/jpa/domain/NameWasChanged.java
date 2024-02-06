package org.occurrent.eventstore.jpa.domain;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;

public record NameWasChanged(String eventId, Instant timestamp, String userId, String name)
    implements DomainEvent {
}
