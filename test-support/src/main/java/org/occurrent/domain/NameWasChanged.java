package org.occurrent.domain;

import org.occurrent.time.TimeConversion;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Objects;

public class NameWasChanged implements DomainEvent {

    private String eventId;
    private Date timestamp;
    private String name;

    @SuppressWarnings("unused")
    NameWasChanged() {
    }

    public NameWasChanged(String eventId, Date timestamp, String nameChangedTo) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.name = nameChangedTo;
    }

    public NameWasChanged(String eventId, LocalDateTime timestamp, String name) {
        this(eventId, TimeConversion.toDate(timestamp), name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NameWasChanged)) return false;
        NameWasChanged that = (NameWasChanged) o;
        return Objects.equals(eventId, that.eventId) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, timestamp, name);
    }

    @Override
    public String toString() {
        return "NameWasChanged{" +
                "id='" + eventId + '\'' +
                ", timestamp=" + timestamp +
                ", name='" + name + '\'' +
                '}';
    }
}