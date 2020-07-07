package se.haleby.occurrent.domain;

import java.util.Date;

public interface DomainEvent {
    Date getTimestamp();

    String getName();
}
