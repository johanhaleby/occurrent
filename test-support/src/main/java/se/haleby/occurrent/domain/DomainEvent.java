package se.haleby.occurrent.domain;

import java.time.LocalDateTime;

public interface DomainEvent {
    LocalDateTime getTime();

    String getName();
}
