package org.occurrent.domain;

import java.util.Date;

public interface DomainEvent {
    String getEventId();

    Date getTimestamp();

    String getName();
}
