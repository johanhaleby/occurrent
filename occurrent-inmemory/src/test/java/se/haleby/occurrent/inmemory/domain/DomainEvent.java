package se.haleby.occurrent.inmemory.domain;

import java.time.LocalDateTime;

public interface DomainEvent {
    LocalDateTime getTime();

    String getName();
}
